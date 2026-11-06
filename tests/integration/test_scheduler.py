from __future__ import annotations

import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import Engine, text
from testkit import EXPIRY_35, FAR, World, count, make_world

from marketagg.calendar import SessionCalendar
from marketagg.config import load_policies, load_universe, policy_version
from marketagg.db.errors import StaleLease
from marketagg.domain import ChainRequest, InstrumentRef
from marketagg.providers.fake import FakeAction, FakeScenario, ScriptStep
from marketagg.scheduler.leases import JobIntent, JobQueue, RunCounts
from marketagg.scheduler.pacing import CircuitBreaker, PacingGate
from marketagg.scheduler.planner import SchedulePlanner, SchedulerService
from marketagg.worker import JobContext, JobOutcome, Worker

ROOT = Path(__file__).resolve().parents[2]
UNIVERSE = load_universe(ROOT / "config" / "universe.yaml")
POLICIES = load_policies(ROOT / "config" / "policies.yaml")
COLLECTION = policy_version("collection", UNIVERSE)


def _setup(engine: Engine, scenario: FakeScenario | None = None) -> tuple[World, JobQueue]:
    world = make_world(engine, scenario)
    world.repo.register_policy(COLLECTION)
    return world, JobQueue(engine, clock=world.clock)


def _intent(world: World, kind: str = "chain_priority", deadline_s: int = 120,
            slot: datetime | None = None) -> JobIntent:
    at = slot or world.clock.now()
    return JobIntent(kind=kind, provider_id="fake",
                     scope={"symbol": "SYNA", "slot": at.isoformat()}, scheduled_for=at,
                     not_before=at, deadline_at=at + timedelta(seconds=deadline_s),
                     policy_id=COLLECTION.id, priority=20)


def _chain_handler(ctx: JobContext) -> JobOutcome:
    request = ChainRequest(provider_id="fake", policy_id=COLLECTION.id, deadline_at=FAR,
                           correlation_id=str(ctx.job.id),
                           instrument=InstrumentRef(provider_id="fake", symbol="SYNA"),
                           expiration_date=EXPIRY_35)
    result = ctx.gate.call("chain", lambda: ctx.provider.fetch_chain(request))
    staged = ctx.repo.stage_chain(result, instrument_id=ctx.instruments["SYNA"],
                                  scope=dict(ctx.job.scope), fence=ctx.job.fence,
                                  run_id=ctx.job.run_id)
    ctx.repo.publish(staged.batch_id)
    ctx.counts.rows_received += len(result.rows)
    ctx.counts.rows_written += staged.rows_written
    return JobOutcome("succeeded" if result.status.value == "complete" else "partial")


def _worker(world: World, queue: JobQueue, worker_id: str = "w1") -> Worker:
    return Worker(
        engine=world.engine, queue=queue, repo=world.repo, provider=world.provider,
        calendar=SessionCalendar(), universe=UNIVERSE, policies=POLICIES,
        handlers={"chain_priority": _chain_handler},
        pacing=PacingGate(world.engine, "fake", 10, clock=world.clock),
        circuits=CircuitBreaker(world.engine, POLICIES.retry, clock=world.clock),
        instruments=world.instruments, worker_id=worker_id, clock=world.clock,
        sleep=lambda s: world.clock.advance(timedelta(seconds=s)),
    )


def test_two_schedulers_produce_one_logical_job_set(engine: Engine) -> None:
    world, queue = _setup(engine)
    planner = SchedulePlanner(SessionCalendar(), UNIVERSE, POLICIES, COLLECTION.id)
    first = SchedulerService(engine, planner, queue, "s1", clock=world.clock)
    second = SchedulerService(engine, planner, queue, "s2", clock=world.clock)
    summary = first.tick()
    assert summary is not None and summary.created > 0
    assert second.tick() is None  # advisory lock held by s1
    jobs = count(engine, "SELECT count(*) FROM collection_jobs")
    # Even without the lock, job keys make planning idempotent.
    planner.enqueue_due(queue, world.clock.now())
    assert count(engine, "SELECT count(*) FROM collection_jobs") == jobs
    first.release()
    assert second.tick() is not None
    second.release()


def test_competing_workers_claim_each_job_once(engine: Engine) -> None:
    world, queue = _setup(engine)
    for i in range(20):
        queue.enqueue(_intent(world, slot=world.clock.now() - timedelta(microseconds=i)))
    claimed: list[str] = []
    lock = threading.Lock()

    def drain(name: str) -> None:
        while (job := queue.claim(name)) is not None:
            with lock:
                claimed.append(str(job.id))
            queue.complete(job, "succeeded", RunCounts())

    threads = [threading.Thread(target=drain, args=(f"w{i}",)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(claimed) == 20 == len(set(claimed))


def test_stale_worker_cannot_commit_after_lease_loss(engine: Engine) -> None:
    world, queue = _setup(engine)
    queue.enqueue(_intent(world, deadline_s=3600))
    stale = queue.claim("old")
    assert stale is not None
    world.clock.advance(timedelta(seconds=181))  # lease expired
    fresh = queue.claim("new")
    assert fresh is not None and fresh.id == stale.id
    assert fresh.fence.lease_token == stale.fence.lease_token + 1
    assert not queue.heartbeat(stale)
    result = world.provider.fetch_chain(world.chain_request())
    with pytest.raises(StaleLease):
        world.repo.stage_chain(result, instrument_id=world.instruments["SYNA"], scope={},
                               fence=stale.fence)
    with pytest.raises(StaleLease):
        queue.complete(stale, "succeeded", RunCounts())
    world.repo.stage_chain(result, instrument_id=world.instruments["SYNA"], scope={},
                           fence=fresh.fence)
    queue.complete(fresh, "succeeded", RunCounts())
    with engine.connect() as conn:
        runs = conn.execute(text("SELECT status FROM ingestion_runs ORDER BY attempt")).scalars()
        assert list(runs) == ["abandoned", "succeeded"]


def test_overdue_live_slot_is_skipped_with_unrecoverable_gap(engine: Engine) -> None:
    world, queue = _setup(engine)
    queue.enqueue(_intent(world))
    world.clock.advance(timedelta(minutes=10))  # restart long after the slot
    assert queue.claim("w1") is None
    with engine.connect() as conn:
        status = conn.execute(text("SELECT status, last_error_code FROM collection_jobs")).one()
        gap = conn.execute(text("SELECT kind, status FROM data_gaps")).one()
    assert tuple(status) == ("skipped", "deadline_passed")
    assert tuple(gap) == ("missing_chain_snapshot", "unrecoverable")
    assert count(engine, "SELECT count(*) FROM observation_batches") == 0  # no late fetch


def test_worker_records_actual_requests_and_publishes(engine: Engine) -> None:
    world, queue = _setup(engine)
    queue.enqueue(_intent(world))
    assert _worker(world, queue).run_once() is not None
    with engine.connect() as conn:
        run = conn.execute(text("SELECT status, endpoint_requests, transport_requests, "
                                "rows_written FROM ingestion_runs")).one()
    assert tuple(run) == ("succeeded", 1, 1, 24)
    assert count(engine, "SELECT count(*) FROM observation_batches "
                         "WHERE status='published'") == 1


def test_rate_limit_opens_circuit_and_retries_after_retry_after(engine: Engine) -> None:
    scenario = FakeScenario(script={"chain": (ScriptStep(FakeAction.RATE_LIMITED,
                                                         retry_after=30.0),)})
    world, queue = _setup(engine, scenario)
    queue.enqueue(_intent(world, deadline_s=3600))
    worker = _worker(world, queue)
    worker.run_once()
    with engine.connect() as conn:
        job = conn.execute(text("SELECT status, not_before, last_error_code "
                                "FROM collection_jobs")).one()
        circuit = conn.execute(text("SELECT state, open_until FROM provider_circuits")).one()
        run = conn.execute(text("SELECT endpoint_requests FROM ingestion_runs")).scalar_one()
    assert job.status == "pending" and job.last_error_code == "rate_limited"
    assert circuit.state == "open"
    assert circuit.open_until - world.clock.now() >= timedelta(seconds=899)  # 15 min cooldown
    assert job.not_before - world.clock.now() >= timedelta(seconds=30)
    assert run == 1  # the failed call is counted


def test_access_denied_disables_provider(engine: Engine) -> None:
    scenario = FakeScenario(script={"chain": (ScriptStep(FakeAction.UNAUTHORIZED),)})
    world, queue = _setup(engine, scenario)
    queue.enqueue(_intent(world))
    worker = _worker(world, queue)
    worker.run_once()
    breaker = CircuitBreaker(engine, POLICIES.retry, clock=world.clock)
    assert breaker.state("fake", "chain").state == "disabled"
    world.clock.advance(timedelta(days=1))
    assert not breaker.allows("fake", "chain")  # stays disabled until an operator acts
    queue.enqueue(_intent(world, slot=world.clock.now()))
    worker.run_once()
    assert world.provider.call_counts["chain"] == 1  # no further provider calls
    assert count(engine, "SELECT count(*) FROM data_gaps WHERE status='unrecoverable'") == 2


def test_shared_pacing_spaces_requests(engine: Engine) -> None:
    world, _ = _setup(engine)
    a = PacingGate(engine, "fake", 10, clock=world.clock)
    b = PacingGate(engine, "fake", 10, clock=world.clock)  # second worker, same token clock
    assert a.try_acquire() == 0.0
    wait = b.try_acquire()
    assert 5.9 < wait <= 6.0
    world.clock.advance(timedelta(seconds=6))
    assert b.try_acquire() == 0.0


def test_backlog_sheds_broad_work_first(engine: Engine) -> None:
    world, queue = _setup(engine)
    start = datetime(2025, 3, 7, 14, 30, tzinfo=UTC)
    for i in range(POLICIES.schedule.backlog_shed_threshold + 1):
        queue.enqueue(_intent(world, kind="gap_scan", deadline_s=7200,
                              slot=start - timedelta(seconds=i + 1)))
    planner = SchedulePlanner(SessionCalendar(), UNIVERSE, POLICIES, COLLECTION.id)
    world.clock.set(datetime(2025, 3, 7, 14, 34, tzinfo=UTC))
    summary = planner.enqueue_due(queue, world.clock.now())
    assert summary.shed >= 2  # both symbols' 09:35 'other' scope
    with engine.connect() as conn:
        kinds = set(conn.execute(text(
            "SELECT kind FROM collection_jobs WHERE last_error_code='shed_backlog'")).scalars())
        priority = conn.execute(text(
            "SELECT count(*) FROM collection_jobs WHERE kind='chain_priority' "
            "AND status='pending'")).scalar_one()
    assert kinds == {"chain_other"} and priority > 0
