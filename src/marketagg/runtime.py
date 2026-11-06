"""Process wiring for scheduler/worker/API services and tests.

``bootstrap`` is idempotent: it registers the provider capabilities, versioned
policies and universe instruments, and provisions partitions for the current
and next two months (06). It never enables a live source on its own.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy import Engine, text

from marketagg.analytics.aggregate import aggregation_policy
from marketagg.calendar import SessionCalendar
from marketagg.clock import SYSTEM_CLOCK, Clock
from marketagg.collectors.bars import bar_ingest_policy
from marketagg.config import PoliciesConfig, UniverseConfig, policy_version
from marketagg.db.ingest import IngestRepository
from marketagg.db.partitions import ensure_partitions
from marketagg.providers.base import MarketDataProvider
from marketagg.providers.fake import FakeProvider
from marketagg.scheduler.leases import JobQueue
from marketagg.scheduler.pacing import CircuitBreaker, PacingGate
from marketagg.scheduler.planner import SchedulePlanner, SchedulerService
from marketagg.settings import ProviderName, Settings
from marketagg.storage.payloads import PayloadStore
from marketagg.worker import Handler, Worker


@dataclass
class Runtime:
    engine: Engine
    clock: Clock
    calendar: SessionCalendar
    universe: UniverseConfig
    policies: PoliciesConfig
    provider: MarketDataProvider
    repo: IngestRepository
    queue: JobQueue
    pacing: PacingGate
    circuits: CircuitBreaker
    instruments: dict[str, UUID]
    policy_ids: dict[str, str]
    payloads: PayloadStore | None = None
    extra: dict[str, Any] = field(default_factory=dict)


def build_provider(settings: Settings, calendar: SessionCalendar,
                   clock: Clock = SYSTEM_CLOCK) -> MarketDataProvider:
    if settings.provider is ProviderName.FAKE:
        return FakeProvider()
    if settings.provider is ProviderName.YAHOO:
        from marketagg.providers.yahoo import LiveAuthorization, YahooProvider  # noqa: PLC0415

        return YahooProvider(calendar=calendar,
                             authorization=LiveAuthorization.from_settings(settings),
                             clock=clock)
    raise NotImplementedError("licensed provider adapter is planned in T24")


def bootstrap(
    engine: Engine,
    provider: MarketDataProvider,
    universe: UniverseConfig,
    policies: PoliciesConfig,
    *,
    clock: Clock = SYSTEM_CLOCK,
    requests_per_minute: int = 10,
    payload_root: Path | None = None,
    code_sha: str = "dev",
    provision_partitions: bool = True,
) -> Runtime:
    if universe.provider != provider.provider_id:
        raise ValueError(f"universe expects provider {universe.provider!r}, "
                         f"got {provider.provider_id!r}")
    calendar = SessionCalendar()
    repo = IngestRepository(engine, clock=clock)
    repo.register_provider(provider.capabilities())
    versions = {
        "collection": policy_version("collection", universe, code_sha),
        "policies": policy_version("research_policies", policies, code_sha),
        "calendar": calendar.policy.policy_version(code_sha),
        "bars_ingest": bar_ingest_policy(universe, calendar.policy_id),
        "aggregation": aggregation_policy(calendar.policy_id),
    }
    for version in versions.values():
        repo.register_policy(version)
    now = clock.now()
    if provision_partitions:  # read-only roles (API) leave this to writers
        ensure_partitions(engine, now, now + timedelta(days=62))
    instruments = {
        s.symbol: repo.register_instrument(
            provider_id=provider.provider_id, symbol=s.symbol, asset_class=s.asset_class,
            currency=s.currency, calendar_id=s.calendar_id,
            valid_from=calendar.local_date(now), evidence={"source": "universe config",
                                                           "version": universe.version})
        for s in universe.symbols
    }
    return Runtime(
        engine=engine, clock=clock, calendar=calendar, universe=universe, policies=policies,
        provider=provider, repo=repo,
        queue=JobQueue(engine, clock=clock,
                       lease=timedelta(seconds=policies.schedule.lease_seconds)),
        pacing=PacingGate(engine, provider.provider_id, requests_per_minute, clock=clock),
        circuits=CircuitBreaker(engine, policies.retry, clock=clock),
        instruments=instruments, policy_ids={k: v.id for k, v in versions.items()},
        payloads=PayloadStore(payload_root) if payload_root is not None else None,
    )


def make_worker(rt: Runtime, worker_id: str = "worker-1",
                handlers: dict[str, Handler] | None = None,
                sleep: Any = None) -> Worker:
    from marketagg.collectors.registry import HANDLERS  # noqa: PLC0415 - avoids import cycle

    return Worker(engine=rt.engine, queue=rt.queue, repo=rt.repo, provider=rt.provider,
                  calendar=rt.calendar, universe=rt.universe, policies=rt.policies,
                  handlers=handlers or HANDLERS, pacing=rt.pacing, circuits=rt.circuits,
                  instruments=rt.instruments, policy_ids=rt.policy_ids, worker_id=worker_id,
                  clock=rt.clock, sleep=sleep, payloads=rt.payloads)


def make_planner(rt: Runtime) -> SchedulePlanner:
    return SchedulePlanner(rt.calendar, rt.universe, rt.policies, rt.policy_ids["collection"])


def make_scheduler(rt: Runtime, holder: str = "scheduler-1") -> SchedulerService:
    return SchedulerService(rt.engine, make_planner(rt), rt.queue, holder, clock=rt.clock)


def enqueue_history_bootstrap(rt: Runtime) -> int:
    """Enqueue initial history once per collection policy; returns jobs created."""
    with rt.engine.connect() as conn:
        done = set(conn.execute(text("""
            SELECT kind || ':' || (scope->>'symbol') FROM collection_jobs
            WHERE scope->>'phase' = 'bootstrap' AND policy_id = :p
              AND status NOT IN ('failed', 'cancelled')"""),
            {"p": rt.policy_ids["collection"]}).scalars())
    created = 0
    for intent in make_planner(rt).history_bootstrap_intents(rt.clock.now()):
        if f"{intent.kind}:{intent.scope['symbol']}" not in done:
            created += int(rt.queue.enqueue(intent))
    return created
