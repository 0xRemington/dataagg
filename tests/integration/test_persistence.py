from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import Engine, text
from sqlalchemy.exc import DBAPIError
from testkit import BAR_POLICY, START, World, advance, count, make_world

from marketagg.db.errors import InvariantViolation, PartitionMissing, StaleLease
from marketagg.db.ingest import Fence
from marketagg.providers.fake import (
    PARSER_V1,
    PARSER_V2,
    BarRevision,
    FakeAction,
    FakeScenario,
    ScriptStep,
)


@pytest.fixture
def world(engine: Engine) -> World:
    return make_world(engine)


def _stage_chain(world: World, **kw: object) -> tuple[object, object]:
    result = world.provider.fetch_chain(world.chain_request())
    outcome = world.repo.stage_chain(result, instrument_id=world.instruments["SYNA"],
                                     scope={"kind": "chain_priority"}, **kw)  # type: ignore[arg-type]
    return result, outcome


def test_response_replay_is_idempotent_and_distinct_polls_persist(world: World) -> None:
    result, first = _stage_chain(world)
    replay = world.repo.stage_chain(result, instrument_id=world.instruments["SYNA"],
                                    scope={"kind": "chain_priority"})
    assert first.created and not replay.created  # type: ignore[attr-defined]
    assert replay.batch_id == first.batch_id  # type: ignore[attr-defined]
    assert count(world.engine, "SELECT count(*) FROM option_snapshots") == 24

    advance(world, 60)  # same price epoch -> identical prices, new response
    _, second = _stage_chain(world)
    assert second.created  # type: ignore[attr-defined]
    assert count(world.engine, "SELECT count(*) FROM option_snapshots") == 48
    assert count(world.engine, "SELECT count(DISTINCT observed_at) FROM option_snapshots") == 2
    assert count(world.engine, "SELECT count(*) FROM option_contracts") == 24
    assert count(world.engine, "SELECT count(*) FROM payload_objects") == 1  # same bytes


def test_partial_chain_keeps_valid_side_with_coverage(engine: Engine) -> None:
    scenario = FakeScenario(script={"chain": (ScriptStep(FakeAction.CALLS_ONLY),)})
    world = make_world(engine, scenario)
    _, outcome = _stage_chain(world)
    with engine.connect() as conn:
        coverage = conn.execute(text("SELECT coverage FROM observation_batches WHERE id=:id"),
                                {"id": outcome.batch_id}).scalar_one()  # type: ignore[attr-defined]
    assert coverage["observed_sides"] == ["C"] and coverage["scope_complete"] is False
    assert outcome.rows_written == 12  # type: ignore[attr-defined]


def test_rejected_rows_become_quality_issues(engine: Engine) -> None:
    world = make_world(engine, FakeScenario(
        script={"chain": (ScriptStep(FakeAction.NONFINITE_ROW),)}))
    _stage_chain(world)
    sql = "SELECT count(*) FROM quality_issues WHERE code='rejected_non_finite'"
    assert count(engine, sql) == 1
    assert count(engine, "SELECT count(*) FROM option_snapshots") == 23


def test_wrong_underlying_and_changed_economics_are_quarantined(world: World) -> None:
    result = world.provider.fetch_chain(world.chain_request())
    outcome = world.repo.stage_chain(result, instrument_id=world.instruments["SYNB"],
                                     scope={"kind": "chain_priority"})
    assert outcome.rows_written == 0 and outcome.rows_quarantined == 24
    assert count(world.engine, "SELECT count(*) FROM option_contracts") == 0

    world.repo.stage_chain(result.model_copy(update={"response_id": uuid4()}),
                           instrument_id=world.instruments["SYNA"], scope={})
    row = result.rows[0]
    changed = row.model_copy(update={"contract": row.contract.model_copy(
        update={"provider_symbol": "NONOCCSYMBOL", "strike": row.contract.strike})})
    other = changed.model_copy(update={"contract": changed.contract.model_copy(
        update={"strike": row.contract.strike + 1})})
    first = result.model_copy(update={"response_id": uuid4(), "rows": (changed,)})
    second = result.model_copy(update={"response_id": uuid4(), "rows": (other,)})
    world.repo.stage_chain(first, instrument_id=world.instruments["SYNA"], scope={})
    outcome = world.repo.stage_chain(second, instrument_id=world.instruments["SYNA"], scope={})
    assert outcome.rows_quarantined == 1  # same symbol, different strike: never guessed
    assert count(world.engine, "SELECT count(*) FROM quality_issues "
                               "WHERE code='ambiguous_contract'") == 1


def test_facts_are_immutable_and_published_batches_sealed(world: World) -> None:
    _, outcome = _stage_chain(world)
    with pytest.raises(DBAPIError, match="immutable"), world.engine.begin() as conn:
        conn.execute(text("UPDATE option_snapshots SET bid = 1"))
    with pytest.raises(DBAPIError, match="immutable"), world.engine.begin() as conn:
        conn.execute(text("DELETE FROM option_snapshots"))
    world.repo.publish(outcome.batch_id)  # type: ignore[attr-defined]
    with pytest.raises(DBAPIError, match="sealed"), world.engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO option_snapshots (observed_at, id, batch_id, contract_id, provider_id,
                observation_kind, field_semantics, content_sha256)
            SELECT observed_at, gen_random_uuid(), batch_id, contract_id, provider_id,
                   'checkpoint', field_semantics, content_sha256
            FROM option_snapshots LIMIT 1"""))
    with pytest.raises(DBAPIError, match="only status/known_at"), world.engine.begin() as conn:
        conn.execute(text("UPDATE observation_batches SET coverage='{}'"))
    with pytest.raises(DBAPIError, match="already published"), world.engine.begin() as conn:
        conn.execute(text("UPDATE observation_batches SET status='quarantined', known_at=NULL"))


def test_unchanged_bar_refetch_is_not_duplicated_but_changes_append(engine: Engine) -> None:
    target = datetime(2025, 3, 7, 14, 31, tzinfo=UTC)
    world = make_world(engine, FakeScenario(
        bar_revisions=(BarRevision(target, 3, Decimal("0.05")),)))
    request = world.bar_request(datetime(2025, 3, 7, 14, 30, tzinfo=UTC),
                                datetime(2025, 3, 7, 14, 45, tzinfo=UTC))
    outcomes = []
    for _ in range(3):
        result = world.provider.fetch_bars(request)
        outcomes.append(world.repo.stage_bars(result, instrument_id=world.instruments["SYNA"],
                                              policy_id=BAR_POLICY.id, scope={}))
    assert [(o.rows_written, o.rows_unchanged) for o in outcomes] == [(15, 0), (0, 15), (1, 14)]
    with engine.connect() as conn:
        revs = conn.execute(text("""
            SELECT close, supersedes_id IS NOT NULL FROM bar_observations
            WHERE interval_start = :t ORDER BY supersedes_id NULLS FIRST"""),
            {"t": target}).all()
    assert len(revs) == 2 and revs[1][0] - revs[0][0] == Decimal("0.05") and revs[1][1]


def test_missing_partition_is_a_readiness_failure(world: World) -> None:
    clock_start = datetime(2026, 2, 3, 15, 0, tzinfo=UTC)
    world.clock.set(clock_start)  # Feb 2026 partitions were not provisioned
    result = world.provider.fetch_chain(world.chain_request(
        expiration=world.provider.discover(world.discovery_request()).expirations[1]))
    with pytest.raises(PartitionMissing, match="no partition"):
        world.repo.stage_chain(result, instrument_id=world.instruments["SYNA"], scope={})
    assert count(world.engine, "SELECT count(*) FROM observation_batches") == 0


def test_stale_fence_commits_nothing(world: World) -> None:
    job_id = uuid4()
    with world.engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO collection_jobs (id, job_key, kind, provider_id, scheduled_for,
                not_before, scope, policy_id, priority, status, lease_owner, lease_token,
                lease_until)
            VALUES (:id, 'k1', 'chain_priority', 'fake', :t, :t, '{}', :pol, 1, 'running',
                    'w1', 2, :until)"""),
            {"id": job_id, "t": START, "pol": BAR_POLICY.id, "until": START + timedelta(
                minutes=3)})
    with pytest.raises(StaleLease):
        _stage_chain(world, fence=Fence(job_id, 1))
    assert count(world.engine, "SELECT count(*) FROM observation_batches") == 0
    _, ok = _stage_chain(world, fence=Fence(job_id, 2))
    assert ok.created  # type: ignore[attr-defined]


def test_parser_correction_appends_linked_revision(engine: Engine) -> None:
    world = make_world(engine, FakeScenario(parser_version=PARSER_V1))
    request = world.chain_request()
    original = world.provider.fetch_chain(request)
    staged = world.repo.stage_chain(original, instrument_id=world.instruments["SYNA"], scope={})
    world.repo.publish(staged.batch_id)
    corrected = world.provider.reparse_chain(original, request, PARSER_V2)
    outcome = world.repo.stage_chain_correction(staged.batch_id, corrected,
                                                instrument_id=world.instruments["SYNA"])
    again = world.repo.stage_chain_correction(staged.batch_id, corrected,
                                              instrument_id=world.instruments["SYNA"])
    assert outcome.created and not again.created
    assert outcome.rows_written == 24  # in_the_money now mapped on every row
    with engine.connect() as conn:
        revs = conn.execute(text("""
            SELECT record_revision, count(*), count(DISTINCT observed_at)
            FROM option_snapshots GROUP BY record_revision ORDER BY 1""")).all()
    assert [tuple(r) for r in revs] == [(1, 24, 1), (2, 24, 1)]
    with pytest.raises(InvariantViolation):
        world.repo.stage_chain_correction(
            staged.batch_id, corrected.model_copy(update={"response_id": uuid4()}),
            instrument_id=world.instruments["SYNA"])


def test_invalidation_appends_revision_without_prices(world: World) -> None:
    _, outcome = _stage_chain(world)
    world.repo.publish(outcome.batch_id)  # type: ignore[attr-defined]
    with world.engine.connect() as conn:
        target = conn.execute(text(
            "SELECT id, observed_at FROM option_snapshots LIMIT 1")).one()
    batch = world.repo.invalidate_snapshot(target.id, target.observed_at, reason="test")
    assert world.repo.invalidate_snapshot(target.id, target.observed_at, reason="test") == batch
    with world.engine.connect() as conn:
        row = conn.execute(text("""
            SELECT bid, ask, quality_flags, record_revision FROM option_snapshots
            WHERE supersedes_id = :id"""), {"id": target.id}).one()
    assert row.bid is None and row.ask is None and "invalidated" in row.quality_flags
    assert row.record_revision == 2


def test_symbol_change_resolution_is_bitemporal(world: World) -> None:
    iid = world.instruments["SYNB"]
    before = world.clock.now()
    advance(world, 3600)
    world.repo.supersede_instrument_identifier(
        instrument_id=iid, provider_id="fake", symbol="SYNB", valid_to=date(2025, 3, 10),
        status="inactive", evidence={"action": "symbol_change"}, new_symbol="SYNC",
        new_valid_from=date(2025, 3, 10))
    after = world.clock.now()
    resolve = world.repo.resolve_instrument
    assert resolve("fake", "SYNB", known_by=after, effective=date(2025, 3, 7)) == iid
    assert resolve("fake", "SYNB", known_by=after, effective=date(2025, 3, 11)) is None
    assert resolve("fake", "SYNC", known_by=after, effective=date(2025, 3, 11)) == iid
    assert resolve("fake", "SYNC", known_by=before, effective=date(2025, 3, 11)) is None
    # Before the change was known, the old mapping still looked open-ended.
    assert resolve("fake", "SYNB", known_by=before, effective=date(2025, 3, 11)) == iid
