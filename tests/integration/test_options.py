from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, text
from testkit import count

from marketagg.config import load_policies, load_universe
from marketagg.providers.fake import FakeAction, FakeClock, FakeProvider, FakeScenario, ScriptStep
from marketagg.runtime import Runtime, bootstrap, make_worker
from marketagg.scheduler.leases import JobIntent

ROOT = Path(__file__).resolve().parents[2]
UNIVERSE = load_universe(ROOT / "config" / "universe.yaml")
POLICIES = load_policies(ROOT / "config" / "policies.yaml")
START = datetime(2025, 3, 7, 14, 15, tzinfo=UTC)  # 09:15 ET discovery time


def _runtime(engine: Engine, scenario: FakeScenario | None = None,
             start: datetime = START) -> Runtime:
    clock = FakeClock(start)
    return bootstrap(engine, FakeProvider(scenario or FakeScenario(), clock), UNIVERSE,
                     POLICIES, clock=clock, requests_per_minute=600)


def _job(rt: Runtime, kind: str, deadline: int = 120, **scope: Any) -> None:
    at = rt.clock.now()
    rt.queue.enqueue(JobIntent(kind=kind, provider_id="fake",
                               scope={"symbol": "SYNA", **scope}, scheduled_for=at,
                               not_before=at, deadline_at=at + timedelta(seconds=deadline),
                               policy_id=rt.policy_ids["collection"], priority=1))


def _run(rt: Runtime) -> None:
    worker = make_worker(rt, sleep=lambda s: rt.clock.advance(timedelta(seconds=s)))  # type: ignore[attr-defined]
    while worker.run_once() is not None:
        pass


def _discover(rt: Runtime) -> None:
    _job(rt, "discovery", deadline=3600)
    _run(rt)
    rt.clock.advance(timedelta(minutes=20))  # 09:35 ET slot


def _runs(engine: Engine) -> list[Any]:
    with engine.connect() as conn:
        return conn.execute(text("""
            SELECT j.kind, r.status, r.endpoint_requests, r.rows_written, r.diagnostics
            FROM ingestion_runs r JOIN collection_jobs j ON j.id = r.job_id
            ORDER BY r.started_at""")).all()


def test_priority_slot_collects_selected_expiries_with_exact_request_cost(
        engine: Engine) -> None:
    rt = _runtime(engine)
    _discover(rt)
    _job(rt, "chain_priority", tier="priority", slot=rt.clock.now().isoformat())
    _run(rt)
    discovery, chain = _runs(engine)
    assert discovery.endpoint_requests == 1
    # Two fake expiries fall in 30-90 DTE: exactly one request per expiry, both sides each.
    assert chain.status == "succeeded" and chain.endpoint_requests == 2
    assert chain.rows_written == 48
    expiries = chain.diagnostics["expiries"]
    assert sorted(expiries) == ["2025-04-11", "2025-05-09"]
    assert set(chain.diagnostics["omitted"].values()) == {"outside_dte_band(7)",
                                                         "outside_dte_band(126)"}
    fetched = sorted(v["fetch_completed_at"] for v in expiries.values())
    assert fetched[0] != fetched[1] and chain.diagnostics["cycle_skew_seconds"] > 0
    assert count(engine, "SELECT count(DISTINCT batch_id) FROM option_snapshots") == 2


def test_equal_price_polls_are_two_observations(engine: Engine) -> None:
    rt = _runtime(engine)
    _discover(rt)
    for _ in range(2):
        _job(rt, "chain_priority", tier="priority", slot=rt.clock.now().isoformat())
        _run(rt)
        rt.clock.advance(timedelta(seconds=30))  # same five-minute price epoch
    with engine.connect() as conn:
        per_contract = conn.execute(text("""
            SELECT count(*), count(DISTINCT observed_at), count(DISTINCT (bid, ask))
            FROM option_snapshots GROUP BY contract_id""")).all()
    assert {tuple(r) for r in per_contract} == {(2, 2, 1)}


def test_partial_response_preserves_calls_and_flags_scope(engine: Engine) -> None:
    rt = _runtime(engine, FakeScenario(script={"chain": (ScriptStep(FakeAction.CALLS_ONLY),)}))
    _discover(rt)
    _job(rt, "chain_priority", tier="priority", slot=rt.clock.now().isoformat())
    _run(rt)
    chain = _runs(engine)[-1]
    assert chain.status == "partial"
    first = chain.diagnostics["expiries"]["2025-04-11"]
    assert first["missing_sides"] == ["P"] and first["rows"] == 12
    assert count(engine, "SELECT count(*) FROM option_snapshots "
                         "WHERE 'partial_scope' = ANY(quality_flags)") == 12


def test_flags_and_nulls_are_stored_as_observed(engine: Engine) -> None:
    rt = _runtime(engine)
    _discover(rt)
    _job(rt, "chain_priority", tier="priority", slot=rt.clock.now().isoformat())
    _run(rt)
    with engine.connect() as conn:
        flagged = dict(conn.execute(text("""
            SELECT f, count(*) FROM option_snapshots, unnest(quality_flags) f
            GROUP BY f""")).all())
        nulls = conn.execute(text("""
            SELECT count(*) FILTER (WHERE reported_open_interest IS NULL),
                   count(*) FILTER (WHERE reported_open_interest = 0),
                   count(*) FILTER (WHERE bid < 0)
            FROM option_snapshots""")).one()
    assert flagged["unknown_quote_time"] == 48
    for flag in ("zero_bid", "missing_bid", "locked", "crossed", "negative_price",
                 "invalid_iv", "old_last_trade", "unknown_oi_date"):
        assert flagged.get(flag, 0) >= 1, flag
    assert tuple(nulls) == (2, 2, 2)


def test_no_discovery_known_before_slot_skips_with_gap(engine: Engine) -> None:
    rt = _runtime(engine)
    _job(rt, "chain_priority", tier="priority", slot=rt.clock.now().isoformat())
    _run(rt)
    assert _runs(engine)[-1].status == "skipped"
    assert count(engine, "SELECT count(*) FROM data_gaps "
                         "WHERE kind='missing_chain_snapshot'") == 1
    assert count(engine, "SELECT count(*) FROM option_snapshots") == 0


def test_expiry_roll_appends_membership_changes(engine: Engine) -> None:
    rt = _runtime(engine)
    _discover(rt)
    _job(rt, "chain_priority", tier="priority", slot=rt.clock.now().isoformat())
    _run(rt)
    first = count(engine, "SELECT count(*) FROM universe_memberships")
    _job(rt, "chain_priority", tier="priority", slot=(rt.clock.now() + timedelta(
        seconds=1)).isoformat())
    _run(rt)
    assert count(engine, "SELECT count(*) FROM universe_memberships") == first  # unchanged
    rt.clock.set(datetime(2025, 3, 14, 14, 15, tzinfo=UTC))  # one week later
    _discover(rt)
    _job(rt, "chain_priority", tier="priority", slot=rt.clock.now().isoformat())
    _run(rt)
    with engine.connect() as conn:
        selected = conn.execute(text("""
            SELECT DISTINCT reason FROM universe_memberships
            WHERE selected AND known_at = (SELECT max(known_at) FROM universe_memberships)
        """)).scalars().all()
    assert count(engine, "SELECT count(*) FROM universe_memberships") > first
    assert any("2025-04-18" in r or "2025-05-16" in r for r in selected)


@pytest.mark.parametrize("action", [FakeAction.AMBIGUOUS_ROW, FakeAction.NONFINITE_ROW])
def test_bad_rows_quarantined_others_kept(engine: Engine, action: FakeAction) -> None:
    rt = _runtime(engine, FakeScenario(script={"chain": (ScriptStep(action),)}))
    _discover(rt)
    _job(rt, "chain_priority", tier="priority", slot=rt.clock.now().isoformat())
    _run(rt)
    assert count(engine, "SELECT count(*) FROM option_snapshots") == 47
    assert count(engine, "SELECT count(*) FROM quality_issues") == 1
