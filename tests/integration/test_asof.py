"""Temporal adversaries (05, release-blocking) and mutation checks for the causal layer."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import Engine, text
from testkit import (
    BAR_POLICY,
    World,
    aggregation_policy_id,
    ingest_minutes,
    m1_key,
    make_world,
)

from marketagg.analytics import asof
from marketagg.analytics.aggregate import derive_pending
from marketagg.analytics.asof import AgeLimits, contracts_asof, eligible_bars, eligible_options
from marketagg.calendar import SessionCalendar
from marketagg.collectors.bars import closed_writes
from marketagg.domain import AvailabilityMode, PriceBasis, Timeframe
from marketagg.providers.fake import BarRevision, FakeScenario
from marketagg.research.repository import IncompatibleQuery, ResearchRepository

CAL = SessionCalendar()
LIMITS = AgeLimits(option=timedelta(minutes=7), spot=timedelta(minutes=3))


def utc(h: int, m: int, s: int = 0, us: int = 0, day: int = 7) -> datetime:
    return datetime(2025, 3, day, h, m, s, us, tzinfo=UTC)


def stage_minute_window(world: World, start: datetime, end: datetime, *, ack_delay: float
                        ) -> datetime:
    """Fetch [start,end), stage, wait ``ack_delay`` seconds, publish; returns known_at."""
    result = world.provider.fetch_bars(world.bar_request(start, end))
    writes, _ = closed_writes(result, safe_lag=timedelta(0), calendar=CAL)
    staged = world.repo.stage_bars(result, instrument_id=world.instruments["SYNA"],
                                   policy_id=BAR_POLICY.id, scope={}, writes=writes)
    world.clock.advance(timedelta(seconds=ack_delay))
    return world.repo.publish(staged.batch_id)


def _bars(world: World, decision: datetime, start: datetime, end: datetime) -> list:
    with world.engine.connect() as conn:
        return eligible_bars(conn, m1_key(world), start=start, end=end, decision_at=decision)


# 1. Delayed publication -----------------------------------------------------------------
def test_delayed_publication_and_exact_boundaries(engine: Engine) -> None:
    world = make_world(engine, start=utc(14, 46))
    # fake latency 0.75s: fetched 14:46:00 -> 14:46:00.75, acknowledged 2.25s later
    known = stage_minute_window(world, utc(14, 45), utc(14, 46), ack_delay=2.25)
    assert known == utc(14, 46, 3)
    assert _bars(world, utc(14, 46, 2), utc(14, 45), utc(14, 46)) == []
    assert _bars(world, known - timedelta(microseconds=1), utc(14, 45), utc(14, 46)) == []
    assert len(_bars(world, known, utc(14, 45), utc(14, 46))) == 1  # <= is inclusive


# 3. Later correction ----------------------------------------------------------------------
def _correction_world(engine: Engine) -> tuple[World, datetime, datetime]:
    target = utc(15, 0)
    world = make_world(engine, FakeScenario(bar_revisions=(BarRevision(target, 2,
                                                                       Decimal("0.25")),)),
                       start=utc(15, 1))
    original = stage_minute_window(world, utc(15, 0), utc(15, 1), ack_delay=0)
    world.clock.set(utc(16, 0))
    corrected = stage_minute_window(world, utc(15, 0), utc(15, 1), ack_delay=0)
    return world, original, corrected


def test_later_correction_is_invisible_before_it_was_known(engine: Engine) -> None:
    world, original, corrected = _correction_world(engine)
    early = _bars(world, utc(15, 30), utc(15, 0), utc(15, 1))
    late = _bars(world, corrected + timedelta(seconds=1), utc(15, 0), utc(15, 1))
    assert early[0].known_at == original and late[0].known_at == corrected
    assert late[0].close - early[0].close == Decimal("0.25")


# 4. Later invalidation of an option ---------------------------------------------------------
def test_later_invalidation_hides_quote_without_fallback(engine: Engine) -> None:
    world = make_world(engine, start=utc(15, 0))
    result = world.provider.fetch_chain(world.chain_request())
    staged = world.repo.stage_chain(result, instrument_id=world.instruments["SYNA"], scope={})
    world.repo.publish(staged.batch_id)
    target = staged.written_ids[0]
    world.clock.set(utc(15, 10))
    world.repo.publish(world.repo.invalidate_snapshot(target, result.fetch_completed_at,
                                                      reason="bad print"))
    with engine.connect() as conn:
        contract = conn.execute(text("SELECT contract_id FROM option_snapshots WHERE id=:i"),
                                {"i": target}).scalar_one()
        before = eligible_options(conn, [contract], start=utc(14, 0), end=utc(16, 0),
                                  decision_at=utc(15, 5))
        after = eligible_options(conn, [contract], start=utc(14, 0), end=utc(16, 0),
                                 decision_at=utc(15, 11))
        lineage = eligible_options(conn, [contract], start=utc(14, 0), end=utc(16, 0),
                                   decision_at=utc(15, 11), include_invalidated=True)
    assert [o.id for o in before] == [target]
    assert "unknown_quote_time" not in before[0].quality_flags
    assert after == []
    assert lineage[0].invalidated and lineage[0].bid is None


# 5. Delayed option commit ---------------------------------------------------------------------
def test_delayed_option_commit_is_excluded_until_acknowledged(engine: Engine) -> None:
    world = make_world(engine, start=utc(14, 59, 59) - timedelta(milliseconds=750))
    result = world.provider.fetch_chain(world.chain_request())
    assert result.fetch_completed_at == utc(14, 59, 59)
    staged = world.repo.stage_chain(result, instrument_id=world.instruments["SYNA"], scope={})
    world.clock.set(utc(15, 0, 3))
    world.repo.publish(staged.batch_id)
    with engine.connect() as conn:
        contract = conn.execute(text("SELECT contract_id FROM option_snapshots LIMIT 1")
                                ).scalar_one()
        assert eligible_options(conn, [contract], start=utc(14, 0), end=utc(16, 0),
                                decision_at=utc(15, 0, 2)) == []
        assert len(eligible_options(conn, [contract], start=utc(14, 0), end=utc(16, 0),
                                    decision_at=utc(15, 0, 3))) == 1


# 6 + 11. Last-trade confusion and session boundary (panel) ----------------------------------
def _panel_world(engine: Engine) -> tuple[World, ResearchRepository]:
    world = make_world(engine, start=utc(20, 50))
    ingest_minutes(world, utc(20, 30), utc(20, 50))
    result = world.provider.fetch_chain(world.chain_request())
    staged = world.repo.stage_chain(result, instrument_id=world.instruments["SYNA"], scope={})
    world.repo.publish(staged.batch_id)
    world.clock.set(utc(20, 57))
    ingest_minutes(world, utc(20, 45), utc(20, 57))  # bars on both sides of option known_at
    repo = ResearchRepository(engine, CAL, {"bars_ingest": BAR_POLICY.id,
                                            "aggregation": aggregation_policy_id(world)})
    return world, repo


def _contract(engine: Engine) -> object:
    with engine.connect() as conn:
        return conn.execute(text("SELECT contract_id FROM option_snapshots LIMIT 1")).scalar_one()


def test_panel_keeps_quote_age_unknown_and_never_execution_eligible(engine: Engine) -> None:
    world, repo = _panel_world(engine)
    result = repo.panel(instrument_id=world.instruments["SYNA"], provider_id="fake",
                        timeframe=Timeframe.M1, start=utc(20, 45), end=utc(20, 56),
                        decision_at=utc(21, 0), contract_id=_contract(engine),  # type: ignore[arg-type]
                        age_limits=LIMITS)
    sampled = [p for p in result.points if p.option is not None]
    assert sampled, "option observation should be sampled after it became known"
    assert all(p.quote_age_s is None and p.freshness == "unknown" for p in sampled)
    assert not any(p.execution_eligible for p in result.points)
    assert any("freshness unknown" in w for w in result.warnings)
    # A bar ending before the option was known never uses it (no backward pull).
    early = [p for p in result.points if p.decision_at < sampled[0].option.known_at]
    assert all(p.option is None for p in early)
    # Spot cutoff is min(D, option known_at) and inside the same session.
    for point in sampled:
        assert point.spot is not None
        assert point.spot.interval_end <= min(point.decision_at, point.option.known_at)


def test_friday_option_is_not_carried_into_monday(engine: Engine) -> None:
    world, repo = _panel_world(engine)
    world.clock.set(datetime(2025, 3, 10, 13, 40, tzinfo=UTC))
    ingest_minutes(world, datetime(2025, 3, 10, 13, 30, tzinfo=UTC),
                   datetime(2025, 3, 10, 13, 38, tzinfo=UTC))
    result = repo.panel(instrument_id=world.instruments["SYNA"], provider_id="fake",
                        timeframe=Timeframe.M1, start=datetime(2025, 3, 10, 13, 30, tzinfo=UTC),
                        end=datetime(2025, 3, 10, 13, 35, tzinfo=UTC),
                        decision_at=world.clock.now(), contract_id=_contract(engine),  # type: ignore[arg-type]
                        age_limits=AgeLimits(option=timedelta(days=4), spot=LIMITS.spot))
    assert result.points and all(p.option is None for p in result.points)
    assert all("option_other_session" in p.exclusions for p in result.points)


# 8. Late feature compute (observed vs recomputed) ------------------------------------------
def test_late_derived_publication_observed_vs_recomputed(engine: Engine) -> None:
    world = make_world(engine, start=utc(15, 0, 30))
    policy = aggregation_policy_id(world)
    ingest_minutes(world, utc(14, 30), utc(15, 0))  # inputs known ~15:00:31
    world.clock.set(utc(15, 5))
    derive_pending(engine, world.repo, CAL, aggregation_policy_id=policy, now=utc(15, 5),
                   instruments={v: k for k, v in world.instruments.items()})
    repo = ResearchRepository(engine, CAL, {"bars_ingest": BAR_POLICY.id,
                                            "aggregation": policy},
                              recompute_lag=timedelta(seconds=60))
    kwargs = {"instrument_id": world.instruments["SYNA"], "provider_id": "fake",
              "timeframe": Timeframe.M15, "start": utc(14, 30), "end": utc(15, 0)}
    observed = repo.bars(decision_at=utc(15, 2), **kwargs).items  # type: ignore[arg-type]
    recomputed = repo.bars(decision_at=utc(15, 2), mode=AvailabilityMode.RECOMPUTED,
                           **kwargs).items  # type: ignore[arg-type]
    later = repo.bars(decision_at=utc(15, 6), **kwargs).items  # type: ignore[arg-type]
    assert observed == []  # indicator not yet published at 15:02
    assert len(recomputed) == 2 and all("recomputed" in b.quality_flags for b in recomputed)
    assert [b.close for b in recomputed] == [b.close for b in later]


# 9. Universe leakage ---------------------------------------------------------------------------
def test_contract_first_discovered_later_is_not_in_earlier_universe(engine: Engine) -> None:
    world = make_world(engine, start=utc(16, 0))
    result = world.provider.fetch_chain(world.chain_request())
    staged = world.repo.stage_chain(result, instrument_id=world.instruments["SYNA"], scope={})
    world.repo.publish(staged.batch_id)
    with engine.connect() as conn:
        at_ten = contracts_asof(conn, world.instruments["SYNA"], decision_at=utc(15, 0),
                                provider_id="fake")
        at_eleven = contracts_asof(conn, world.instruments["SYNA"], decision_at=utc(16, 1),
                                   provider_id="fake")
    assert at_ten == [] and len(at_eleven) == 24
    assert not any(c.has_terms for c in at_eleven)  # unknown terms: model-ineligible


# 10. Terms leakage --------------------------------------------------------------------------
def test_terms_known_later_are_not_used_earlier(engine: Engine) -> None:
    world = make_world(engine, start=utc(15, 0))
    staged = world.repo.stage_chain(world.provider.fetch_chain(world.chain_request()),
                                    instrument_id=world.instruments["SYNA"], scope={})
    world.repo.publish(staged.batch_id)
    with engine.begin() as conn:
        contract = conn.execute(text("SELECT id FROM option_contracts LIMIT 1")).scalar_one()
        conn.execute(text("""
            INSERT INTO contract_terms (id, contract_id, effective_at, known_at, multiplier,
                currency, exercise_style, settlement_style, provider_id, evidence)
            VALUES (:id, :c, :eff, :known, 100, 'USD', 'american', 'physical', 'fake',
                    '{"source":"synthetic"}')"""),
            {"id": uuid4(), "c": contract, "eff": utc(0, 0), "known": utc(18, 0)})
    with engine.connect() as conn:
        early = {c.id: c.has_terms for c in contracts_asof(
            conn, world.instruments["SYNA"], decision_at=utc(17, 0), provider_id="fake")}
        late = {c.id: c.has_terms for c in contracts_asof(
            conn, world.instruments["SYNA"], decision_at=utc(18, 1), provider_id="fake")}
    assert early[contract] is False and late[contract] is True


# 12. Mixed sources / incompatible basis ---------------------------------------------------
def test_mixed_sources_rejected_without_policy_and_basis_mismatch_409(engine: Engine) -> None:
    world, repo = _panel_world(engine)
    with pytest.raises(IncompatibleQuery, match="differ"):
        repo.panel(instrument_id=world.instruments["SYNA"], provider_id="fake",
                   option_provider_id="yahoo", timeframe=Timeframe.M1, start=utc(20, 30),
                   end=utc(20, 40), decision_at=utc(21, 0), age_limits=LIMITS)
    with pytest.raises(IncompatibleQuery, match="supplies provider_native"):
        repo.bars(instrument_id=world.instruments["SYNA"], provider_id="fake",
                  timeframe=Timeframe.M1, start=utc(20, 30), end=utc(20, 40),
                  decision_at=utc(21, 0), price_basis=PriceBasis.AS_TRADED)


# Mutation checks: the adversaries must fail if the causal predicates are weakened -----------
def test_mutation_removing_knowledge_predicate_leaks(engine: Engine,
                                                     monkeypatch: pytest.MonkeyPatch) -> None:
    world = make_world(engine, start=utc(14, 46))
    stage_minute_window(world, utc(14, 45), utc(14, 46), ack_delay=2.25)
    monkeypatch.setattr(asof, "BATCH_KNOWLEDGE_PREDICATE", "TRUE")
    leaked = _bars(world, utc(14, 46, 2), utc(14, 45), utc(14, 46))
    assert leaked, "without known_at <= D the delayed-publication adversary must fail"


def test_mutation_latest_first_selection_breaks_correction_adversary(
        engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
    world, original, _ = _correction_world(engine)
    assert _bars(world, utc(15, 30), utc(15, 0), utc(15, 1))[0].known_at == original
    monkeypatch.setattr(asof, "SELECT_LATEST_BEFORE_KNOWLEDGE", True)
    assert _bars(world, utc(15, 30), utc(15, 0), utc(15, 1)) == [], (
        "latest-then-filter must lose the then-known original revision")


def test_mutation_removing_close_predicate_leaks_open_bars(
        engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
    world = make_world(engine, start=utc(15, 5))
    stage_minute_window(world, utc(15, 0), utc(15, 4), ack_delay=0)
    decision = utc(15, 2, 30)
    honest = _bars(world, decision, utc(15, 0), utc(15, 4))
    monkeypatch.setattr(asof, "BAR_CLOSED_PREDICATE", "TRUE")
    monkeypatch.setattr(asof, "BATCH_KNOWLEDGE_PREDICATE", "TRUE")
    assert len(_bars(world, decision, utc(15, 0), utc(15, 4))) > len(honest)


def test_backfill_not_eligible_for_observed_history(engine: Engine) -> None:
    world = make_world(engine, start=datetime(2025, 3, 10, 22, 0, tzinfo=UTC))
    stage_minute_window(world, utc(14, 30), utc(15, 0), ack_delay=0)
    assert _bars(world, utc(15, 30), utc(14, 30), utc(15, 0)) == []
    assert _bars(world, world.clock.now(), utc(14, 30), utc(15, 0))
    assert date(2025, 3, 7) == _bars(world, world.clock.now(), utc(14, 30),
                                     utc(15, 0))[0].session_date
