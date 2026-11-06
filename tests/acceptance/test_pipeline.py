"""End-to-end fixture acceptance scenarios A01-A06 and A09 (05).

Every scenario runs the real components (migrations, scheduler queue, worker,
collectors, publication, aggregation, research API, exports, archive) against a
disposable PostgreSQL 18 database with the deterministic fake provider.
"""

from __future__ import annotations

import time
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, text
from testkit import count

from marketagg.api.app import ApiServices, create_app
from marketagg.calendar import SessionCalendar
from marketagg.config import load_policies, load_universe
from marketagg.db.errors import StaleLease
from marketagg.db.ingest import IngestRepository
from marketagg.domain import AvailabilityMode, PriceBasis, Timeframe
from marketagg.ops.status import allowed_kinds
from marketagg.providers.fake import (
    BarRevision,
    FakeAction,
    FakeClock,
    FakeProvider,
    FakeScenario,
    ScriptStep,
)
from marketagg.research.datasets import DatasetRequest, DatasetService
from marketagg.research.repository import ResearchRepository
from marketagg.runtime import Runtime, bootstrap, make_planner, make_scheduler, make_worker
from marketagg.scheduler.leases import JobIntent, JobQueue, RunCounts
from marketagg.storage.archive import ArchiveManager, DirectoryOffsite, UnifiedBarReader

pytestmark = [pytest.mark.acceptance, pytest.mark.integration]
ROOT = Path(__file__).resolve().parents[2]
UNIVERSE = load_universe(ROOT / "config" / "universe.yaml")
POLICIES = load_policies(ROOT / "config" / "policies.yaml")
DAY = date(2025, 3, 7)
OPEN = datetime(2025, 3, 7, 14, 30, tzinfo=UTC)
CLOSE = datetime(2025, 3, 7, 21, 0, tzinfo=UTC)
TOKENS = ("read-0123456789", "export-0123456789")
CAL = SessionCalendar()


def _rt(engine: Engine, scenario: FakeScenario | None = None,
        start: datetime = datetime(2025, 3, 7, 14, 10, tzinfo=UTC)) -> Runtime:
    clock = FakeClock(start)
    return bootstrap(engine, FakeProvider(scenario or FakeScenario(), clock), UNIVERSE,
                     POLICIES, clock=clock, requests_per_minute=600)


def _sleep(rt: Runtime) -> Any:
    return lambda seconds: rt.clock.advance(timedelta(seconds=seconds))  # type: ignore[attr-defined]


def _drive(rt: Runtime, until: datetime, step: timedelta = timedelta(minutes=1),
           kinds: set[str] | None = None) -> None:
    """Advance the fake clock, planning and working like the real services do.

    Ticks follow a fixed grid: pacing sleeps move the clock during work, and stepping from
    ``now`` would let that drift accumulate until polls miss their deadlines.
    """
    planner = make_planner(rt)
    worker = make_worker(rt, "w1", sleep=_sleep(rt))
    tick = rt.clock.now()
    while rt.clock.now() < until:
        planner.enqueue_due(rt.queue, rt.clock.now(), horizon=step)
        while worker.run_once(sorted(kinds) if kinds else None) is not None:
            pass
        tick = min(tick + step, until)
        rt.clock.set(max(tick, rt.clock.now()))  # type: ignore[attr-defined]


def _research(rt: Runtime) -> ResearchRepository:
    return ResearchRepository(rt.engine, rt.calendar, rt.policy_ids)


def _api(rt: Runtime, tmp_path: Path) -> TestClient:
    research = _research(rt)
    services = ApiServices(engine=rt.engine, research=research, calendar=rt.calendar,
                           policies=POLICIES, read_tokens=(TOKENS[0],),
                           export_tokens=(TOKENS[1],), clock=rt.clock,
                           datasets=DatasetService(rt.engine, research, tmp_path / "exports",
                                                   signing_key=b"k" * 32, clock=rt.clock))
    return TestClient(create_app(services), headers={"Authorization": f"Bearer {TOKENS[0]}"})


# A01 -------------------------------------------------------------------------------------
def test_a01_normal_fixture_session(engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
    rt = _rt(engine)
    recorded: list[Any] = []
    fetch = rt.provider.fetch_chain

    def recording(request: Any) -> Any:
        result = fetch(request)
        recorded.append(result)
        return result

    monkeypatch.setattr(rt.provider, "fetch_chain", recording)
    # Past 09:40 ET so that slot's jobs (due shortly after the slot) finish; before 09:45.
    _drive(rt, datetime(2025, 3, 7, 14, 43, tzinfo=UTC),
           kinds={"discovery", "chain_priority", "bars_1m"})
    with engine.connect() as conn:
        per_contract = conn.execute(text("""
            SELECT count(*) AS polls, count(DISTINCT (bid, ask)) AS prices
            FROM option_snapshots GROUP BY contract_id""")).all()
        coverage = conn.execute(text("""
            SELECT scope->>'expiration', coverage->'observed_sides', fetch_completed_at
            FROM observation_batches WHERE batch_kind='chain'
            ORDER BY fetch_completed_at""")).all()
        unknown = conn.execute(text("""SELECT bool_and(provider_event_at IS NULL)
            FROM option_snapshots""")).scalar_one()
    # 09:35 and 09:40 slots: two observations per selected contract even if prices are equal.
    assert {r.polls for r in per_contract} == {2}
    assert {e for e, _, _ in coverage} == {"2025-04-11", "2025-05-09"}
    assert all(sorted(sides) == ["C", "P"] for _, sides, _ in coverage)
    assert unknown is True
    batches_before = count(engine, "SELECT count(*) FROM observation_batches")
    rows_before = count(engine, "SELECT count(*) FROM option_snapshots")
    for result in recorded:  # reprocessing any stored response changes no counts
        symbol = result.rows[0].contract.underlying_symbol
        outcome = rt.repo.stage_chain(result, instrument_id=rt.instruments[symbol], scope={})
        assert not outcome.created
    assert len(recorded) == 8  # two symbols x two slots x two selected expiries
    assert count(engine, "SELECT count(*) FROM observation_batches") == batches_before
    assert count(engine, "SELECT count(*) FROM option_snapshots") == rows_before


# A02 -------------------------------------------------------------------------------------
@pytest.mark.parametrize(("day", "expected"), [
    (date(2025, 3, 7), (78, 26, [240, 150])),
    (date(2025, 11, 28), (42, 14, [210])),
])
def test_a02_candles_and_synchronised_panels(engine: Engine, tmp_path: Path, day: date,
                                             expected: tuple[int, int, list[int]]) -> None:
    session = CAL.session(day)
    assert session is not None
    rt = _rt(engine, start=session.open_at - timedelta(minutes=20))
    _drive(rt, session.close_at + timedelta(minutes=12),
           kinds={"discovery", "chain_priority", "bars_1m", "derive_bars"},
           step=timedelta(minutes=1))
    client = _api(rt, tmp_path)
    instrument = str(rt.instruments["SYNA"])

    def bars(tf: str) -> list[dict[str, Any]]:
        body = client.get("/v1/bars", params={
            "instrument_id": instrument, "provider_id": "fake", "timeframe": tf,
            "start": session.open_at.isoformat(), "end": session.close_at.isoformat()}).json()
        return list(body["items"])

    m5, m15, h4 = bars("5m"), bars("15m"), bars("4h")
    assert (len(m5), len(m15), [b["duration_minutes"] for b in h4]) == expected
    minutes = bars("1m")
    assert len(minutes) == session.minutes_count
    first = minutes[:5]
    assert m5[0]["open"] == first[0]["open"] and m5[0]["close"] == first[-1]["close"]
    assert Decimal(m5[0]["high"]) == max(Decimal(m["high"]) for m in first)
    with engine.connect() as conn:
        contract = conn.execute(text("SELECT contract_id FROM option_snapshots LIMIT 1")
                                ).scalar_one()
    panel = client.get("/v1/panel", params={
        "instrument_id": instrument, "provider_id": "fake", "timeframe": "15m",
        "start": session.open_at.isoformat(), "end": session.close_at.isoformat(),
        "contract_id": str(contract)}).json()
    sampled = [p for p in panel["items"] if p["option"]]
    assert sampled and all(p["quote_age_seconds"] is None for p in sampled)
    assert all(p["option"]["known_at"] <= p["decision_at"] for p in sampled)


def test_a02_missing_minute_marks_buckets_incomplete(engine: Engine, tmp_path: Path) -> None:
    gap = datetime(2025, 3, 7, 15, 7, tzinfo=UTC)
    rt = _rt(engine, FakeScenario(omitted_minutes=frozenset({gap})),
             start=datetime(2025, 3, 7, 14, 30, tzinfo=UTC))
    _drive(rt, datetime(2025, 3, 7, 15, 40, tzinfo=UTC), kinds={"bars_1m", "derive_bars"})
    client = _api(rt, tmp_path)
    params = {"instrument_id": str(rt.instruments["SYNA"]), "provider_id": "fake",
              "timeframe": "15m", "start": OPEN.isoformat(),
              "end": datetime(2025, 3, 7, 15, 30, tzinfo=UTC).isoformat()}
    research = client.get("/v1/bars", params=params).json()["items"]
    display = client.get("/v1/bars", params=params | {"include_partial": "true"}).json()
    # The 15:00 bucket contains the missing 15:07 minute; every other bucket is complete.
    assert [b["interval_start"][11:16] for b in research] == ["14:30", "14:45", "15:15"]
    partial = [b for b in display["items"] if not b["complete"]]
    assert partial and partial[0]["observed_minutes"] == 14
    assert any("partial" in w for w in display["warnings"])


# A03 -------------------------------------------------------------------------------------
def test_a03_correction_and_causal_export(engine: Engine, tmp_path: Path) -> None:
    corrected_minute = datetime(2025, 3, 7, 14, 50, tzinfo=UTC)
    rt = _rt(engine, start=datetime(2025, 3, 7, 14, 30, tzinfo=UTC))
    _drive(rt, datetime(2025, 3, 7, 15, 30, tzinfo=UTC), kinds={"bars_1m", "derive_bars"})
    research = _research(rt)
    service = DatasetService(engine, research, tmp_path / "exports", signing_key=b"k" * 32,
                             clock=rt.clock)
    frozen = datetime(2025, 3, 7, 15, 30, tzinfo=UTC)
    request = DatasetRequest(instrument_id=rt.instruments["SYNA"], provider_id="fake",
                             timeframe=Timeframe.M15, start=OPEN, end=frozen,
                             decision_at=frozen)
    before = service.run(service.submit(request))
    # After the cutoff the source revises one minute; the scheduled close sweep (16:10 ET)
    # picks it up and derivation republishes the containing buckets.
    rt.provider = FakeProvider(FakeScenario(bar_revisions=(
        BarRevision(corrected_minute, 1, Decimal("0.5")),)), rt.clock)  # type: ignore[arg-type]
    rt.clock.set(datetime(2025, 3, 7, 21, 5, tzinfo=UTC))  # type: ignore[attr-defined]
    _drive(rt, datetime(2025, 3, 7, 21, 15, tzinfo=UTC), kinds={"minute_sweep", "derive_bars"})
    pinned = service.run(service.submit(request))
    current = research.bars(instrument_id=rt.instruments["SYNA"], provider_id="fake",
                            timeframe=Timeframe.M15, start=OPEN, end=frozen,
                            decision_at=rt.clock.now()).items
    old = research.bars(instrument_id=rt.instruments["SYNA"], provider_id="fake",
                        timeframe=Timeframe.M15, start=OPEN, end=frozen,
                        decision_at=frozen).items
    assert pinned.manifest["output_sha256"] == before.manifest["output_sha256"]
    # The current view also has buckets published after the cutoff; compare shared ones.
    now_by_start = {c.interval_start: c for c in current}
    assert {o.interval_start for o in old} <= set(now_by_start)
    changed = [(o.close, now_by_start[o.interval_start].close) for o in old
               if o.content_sha256 != now_by_start[o.interval_start].content_sha256]
    assert changed, "current view must show the correction"
    recomputed = research.bars(instrument_id=rt.instruments["SYNA"], provider_id="fake",
                               timeframe=Timeframe.M15, start=OPEN, end=frozen,
                               decision_at=frozen, mode=AvailabilityMode.RECOMPUTED).items
    assert all("recomputed" in b.quality_flags for b in recomputed)


# A04 -------------------------------------------------------------------------------------
class Crash(BaseException):
    """Simulated process termination (not caught by worker error handling)."""


def test_a04_scheduler_contention_and_restart(engine: Engine,
                                              monkeypatch: pytest.MonkeyPatch) -> None:
    rt = _rt(engine)
    first, second = make_scheduler(rt, "s1"), make_scheduler(rt, "s2")
    rt.clock.set(datetime(2025, 3, 7, 14, 34, tzinfo=UTC))  # type: ignore[attr-defined]
    assert first.tick() is not None and second.tick() is None
    jobs = count(engine, "SELECT count(*) FROM collection_jobs")
    make_planner(rt).enqueue_due(rt.queue, rt.clock.now())
    assert count(engine, "SELECT count(*) FROM collection_jobs") == jobs  # one logical set
    first.release()
    second.release()
    worker_a = make_worker(rt, "a", sleep=_sleep(rt))
    while worker_a.run_once(["discovery"]) is not None:
        pass
    rt.clock.set(datetime(2025, 3, 7, 14, 35, tzinfo=UTC))  # type: ignore[attr-defined]
    make_planner(rt).enqueue_due(rt.queue, rt.clock.now())
    original_publish = IngestRepository.publish

    def crash_publish(self: IngestRepository, batch_id: Any) -> Any:
        raise Crash("killed after staging commit, before publication")

    monkeypatch.setattr(IngestRepository, "publish", crash_publish)
    with pytest.raises(Crash):
        worker_a.run_once(["chain_priority"])
    monkeypatch.setattr(IngestRepository, "publish", original_publish)
    staged = count(engine, "SELECT count(*) FROM observation_batches WHERE status='staged'")
    assert staged == 1
    # Restart 10 minutes later: recovery publishes with a conservative (late) known time.
    rt.clock.set(datetime(2025, 3, 7, 14, 45, tzinfo=UTC))  # type: ignore[attr-defined]
    recovered = IngestRepository(engine, clock=rt.clock).finalize_staged()
    assert len(recovered) == 1
    with engine.connect() as conn:
        known, fetched = conn.execute(text(
            "SELECT known_at, fetch_completed_at FROM observation_batches WHERE id=:i"),
            {"i": recovered[0]}).one()
    assert known - fetched >= timedelta(minutes=9)
    # The crashed worker's job lease expired; the 09:35 slot is past its deadline:
    # it is skipped and recorded, never re-fetched as a 09:35 observation.
    worker_b = make_worker(rt, "b", sleep=_sleep(rt))
    while worker_b.run_once(["chain_priority"]) is not None:
        pass
    with engine.connect() as conn:
        skipped = conn.execute(text("""
            SELECT count(*) FROM collection_jobs WHERE kind='chain_priority'
              AND status='skipped' AND last_error_code='deadline_passed'""")).scalar_one()
        gaps = conn.execute(text("SELECT count(*) FROM data_gaps "
                                 "WHERE kind='missing_chain_snapshot'")).scalar_one()
    assert skipped >= 1 and gaps >= 1
    # No response was replayed twice: every chain batch has a unique response identity.
    assert count(engine, "SELECT count(*) FROM observation_batches") == count(
        engine, "SELECT count(DISTINCT response_identity) FROM observation_batches")


def test_a04_stale_worker_cannot_complete(engine: Engine) -> None:
    rt = _rt(engine, start=datetime(2025, 3, 7, 14, 34, tzinfo=UTC))
    queue = JobQueue(engine, clock=rt.clock, lease=timedelta(seconds=180))
    queue.enqueue(JobIntent(kind="bars_1m", provider_id="fake", scope={"symbol": "SYNA"},
                            scheduled_for=rt.clock.now(), not_before=rt.clock.now(),
                            deadline_at=rt.clock.now() + timedelta(hours=1),
                            policy_id=rt.policy_ids["collection"], priority=1))
    stale = queue.claim("old")
    rt.clock.advance(timedelta(minutes=4))  # type: ignore[attr-defined]
    fresh = queue.claim("new")
    assert stale is not None and fresh is not None and fresh.fence.lease_token == 2
    with pytest.raises(StaleLease):
        queue.complete(stale, "succeeded", RunCounts())


# A05 -------------------------------------------------------------------------------------
def _chain_runs(engine: Engine) -> dict[str, int]:
    with engine.connect() as conn:
        return dict(conn.execute(text("""
            SELECT coalesce(r.error_code, r.status), count(*) FROM ingestion_runs r
            JOIN collection_jobs j ON j.id = r.job_id WHERE j.kind='chain_priority'
            GROUP BY 1""")).all())


def test_a05_rate_limit_cooldown_is_respected(engine: Engine) -> None:
    rt = _rt(engine, FakeScenario(script={"chain": (
        ScriptStep(FakeAction.RATE_LIMITED, retry_after=120.0),)}))
    _drive(rt, datetime(2025, 3, 7, 14, 49, tzinfo=UTC),
           kinds={"discovery", "chain_priority", "bars_1m"})
    # One rate-limited call, then a 15-minute provider-wide cooldown: no chain requests.
    assert rt.provider.call_counts["chain"] == 1  # type: ignore[attr-defined]
    assert _chain_runs(engine).get("rate_limited") == 1
    # The same provider cools for every endpoint: bars fetched before the 429 are kept,
    # and every later run in the window is refused locally without a request.
    with engine.connect() as conn:
        limited_at = conn.execute(text("SELECT started_at FROM ingestion_runs "
                                       "WHERE error_code='rate_limited'")).scalar_one()
        later = conn.execute(text("""SELECT DISTINCT coalesce(error_code, status)
            FROM ingestion_runs WHERE started_at > :t"""), {"t": limited_at}).scalars().all()
        last_bar = conn.execute(text("""SELECT max(x.known_at) FROM bar_observations b
            JOIN observation_batches x ON x.id = b.batch_id""")).scalar_one()
    assert later == ["circuit_open"]
    assert last_bar is not None and last_bar <= limited_at
    _drive(rt, datetime(2025, 3, 7, 14, 56, tzinfo=UTC),
           kinds={"discovery", "chain_priority", "bars_1m"})
    assert rt.provider.call_counts["chain"] > 1  # type: ignore[attr-defined]


def test_a05_partial_ambiguous_then_access_denied(engine: Engine) -> None:
    rt = _rt(engine, FakeScenario(script={"chain": (
        ScriptStep(FakeAction.CALLS_ONLY), ScriptStep(FakeAction.AMBIGUOUS_ROW),
        ScriptStep(FakeAction.UNAUTHORIZED))}))
    _drive(rt, datetime(2025, 3, 7, 14, 50, tzinfo=UTC),
           kinds={"discovery", "chain_priority", "bars_1m"})
    with engine.connect() as conn:
        partial_rows = conn.execute(text("""SELECT count(*) FROM option_snapshots
            WHERE 'partial_scope' = ANY(quality_flags)""")).scalar_one()
        quarantined = conn.execute(text("""SELECT count(*) FROM quality_issues
            WHERE code = 'rejected_ambiguous_contract'""")).scalar_one()
        circuit = conn.execute(text("SELECT state FROM provider_circuits "
                                    "WHERE endpoint='*'")).scalar_one()
        incident = conn.execute(text("SELECT count(*) FROM operational_incidents "
                                     "WHERE kind='unauthorized'")).scalar_one()
    # Valid rows kept, nothing guessed: 12 calls-only rows plus the 23 unambiguous rows of
    # the response whose one ambiguous row was quarantined (that scope is partial too).
    assert partial_rows == 12 + 23 and quarantined == 1
    assert circuit == "disabled" and incident == 1
    calls = rt.provider.call_counts["chain"]  # type: ignore[attr-defined]
    _drive(rt, datetime(2025, 3, 7, 15, 30, tzinfo=UTC),
           kinds={"discovery", "chain_priority", "bars_1m"})
    assert rt.provider.call_counts["chain"] == calls  # type: ignore[attr-defined]
    assert _chain_runs(engine).get("provider_disabled", 0) > 0


def test_a05_schema_drift_opens_incident_without_aggressive_retry(engine: Engine) -> None:
    rt = _rt(engine, FakeScenario(script={"chain": (ScriptStep(FakeAction.SCHEMA_CHANGED),)}))
    _drive(rt, datetime(2025, 3, 7, 14, 50, tzinfo=UTC),
           kinds={"discovery", "chain_priority", "bars_1m"})
    with engine.connect() as conn:
        incident = conn.execute(text("SELECT details FROM operational_incidents "
                                     "WHERE kind='schema_changed'")).scalar_one()
    assert incident["missing"] == "bid" and incident["fingerprint"].startswith("fake-chain/1:")
    assert _chain_runs(engine).get("schema_changed") == 1
    assert rt.provider.call_counts["chain"] == 1  # type: ignore[attr-defined]
    assert count(engine, "SELECT count(*) FROM bar_observations") > 0  # bars continue


# A06 -------------------------------------------------------------------------------------
def test_a06_archive_round_trip(engine: Engine, tmp_path: Path) -> None:
    rt = _rt(engine, start=datetime(2025, 3, 7, 14, 30, tzinfo=UTC))
    _drive(rt, datetime(2025, 3, 7, 15, 30, tzinfo=UTC), kinds={"bars_1m", "derive_bars"})
    research = _research(rt)
    service = DatasetService(engine, research, tmp_path / "exports", signing_key=b"k" * 32,
                             clock=rt.clock)
    request = DatasetRequest(instrument_id=rt.instruments["SYNA"], provider_id="fake",
                             timeframe=Timeframe.M1, start=OPEN,
                             end=datetime(2025, 3, 7, 15, 30, tzinfo=UTC),
                             decision_at=rt.clock.now())
    before = service.run(service.submit(request)).manifest["output_sha256"]
    archive = ArchiveManager(engine, tmp_path / "archive", DirectoryOffsite(tmp_path / "off"),
                             clock=rt.clock)
    archive.archive_and_activate("bar_observations", "fake", date(2025, 3, 1))
    reader = UnifiedBarReader(engine, archive)
    key = research.bar_key(rt.instruments["SYNA"], "fake", Timeframe.M1,
                           PriceBasis.PROVIDER_NATIVE)
    mixed = reader.eligible_bars(key, start=OPEN, end=request.end, decision_at=rt.clock.now())
    rt.clock.advance(timedelta(days=8))  # type: ignore[attr-defined]
    archive.prune_hot_partition("bar_observations", "fake", date(2025, 3, 1),
                                restore_verified=True)
    cold = reader.eligible_bars(key, start=OPEN, end=request.end,
                                decision_at=request.decision_at)
    assert [(b.id, b.close) for b in cold] == [(b.id, b.close) for b in mixed]
    assert len(cold) > 50 and before


# A09 -------------------------------------------------------------------------------------
def test_a09_scaled_resource_pressure(engine: Engine, tmp_path: Path,
                                      evidence: dict[str, Any]) -> None:
    """Scaled (5 sessions, 2 symbols) load and a 3x request burst against the fake.

    This measures the small-fixture envelope only; it is not pilot capacity evidence.
    """
    rt = _rt(engine, start=datetime(2025, 3, 3, 14, 10, tzinfo=UTC))
    started = time.monotonic()
    for day in (3, 4, 5, 6, 7):
        session = CAL.session(date(2025, 3, day))
        assert session is not None
        rt.clock.set(session.open_at - timedelta(minutes=20))  # type: ignore[attr-defined]
        _drive(rt, session.close_at + timedelta(minutes=6),
               kinds={"discovery", "chain_priority", "bars_1m"})
    ingest_seconds = time.monotonic() - started
    burst_queue = JobQueue(engine, clock=rt.clock)
    for i in range(3 * 78):
        burst_queue.enqueue(JobIntent(kind="gap_scan", provider_id="fake",
                                      scope={"burst": i}, scheduled_for=rt.clock.now(),
                                      not_before=rt.clock.now(),
                                      deadline_at=rt.clock.now() + timedelta(hours=1),
                                      policy_id=rt.policy_ids["collection"], priority=70))
    summary = make_planner(rt).enqueue_due(rt.queue, datetime(2025, 3, 10, 13, 34, tzinfo=UTC))
    assert summary.shed > 0  # backlog sheds broad/hourly scopes first, recorded as gaps
    assert allowed_kinds("stop_bulk", ["daily_backfill", "bars_1m"]) == ["bars_1m"]
    client = _api(rt, tmp_path)
    t0 = time.monotonic()
    response = client.get("/v1/bars", params={
        "instrument_id": str(rt.instruments["SYNA"]), "provider_id": "fake", "timeframe": "1m",
        "start": "2025-03-03T00:00:00Z", "end": "2025-03-08T00:00:00Z"})
    api_seconds = time.monotonic() - t0
    assert response.status_code == 200 and len(response.json()["items"]) == 5 * 390
    rows = count(engine, "SELECT count(*) FROM option_snapshots")
    evidence.update({"a09_option_rows": rows, "a09_ingest_seconds": round(ingest_seconds, 1),
                     "a09_api_1m_week_seconds": round(api_seconds, 3)})
    assert rows > 5 * 78 * 48 * 0.9

