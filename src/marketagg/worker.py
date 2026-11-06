"""Bounded worker: claim -> gate provider calls -> handler -> complete (T05).

Provider calls go through ``RequestGate``: circuit check, shared pacing token,
request accounting (failed calls included) and circuit updates. Error policy (03):

- ``RateLimited``: circuit opens (>= Retry-After); job retried only within deadline.
- ``Unauthorized``: provider disabled; job failed; never retried or circumvented.
- ``Unavailable``: full-jitter retry, at most ``max_attempts``, within deadline.
- ``SchemaChanged``: endpoint circuit opens; job failed; evidence kept in the run.
- ``StaleLease``: the job belongs to someone else now; nothing is completed.

A live chain slot that ends without data is recorded as an unrecoverable gap.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, TypeVar
from uuid import uuid4

from sqlalchemy import Engine, text

from marketagg.calendar import SessionCalendar
from marketagg.clock import SYSTEM_CLOCK, Clock
from marketagg.config import PoliciesConfig, UniverseConfig
from marketagg.db.errors import StaleLease
from marketagg.db.ingest import IngestRepository
from marketagg.domain.errors import (
    ProviderError,
    RateLimited,
    RequestCounts,
    SchemaChanged,
    Unauthorized,
    Unavailable,
    UnsupportedCapability,
)
from marketagg.domain.primitives import canonical_dumps
from marketagg.logging import get_logger
from marketagg.ops.status import allowed_kinds, disk_level
from marketagg.providers.base import MarketDataProvider
from marketagg.scheduler.budgets import next_retry_at
from marketagg.scheduler.leases import (
    LIVE_SNAPSHOT_KINDS,
    ClaimedJob,
    JobQueue,
    RunCounts,
    record_gap,
)
from marketagg.scheduler.pacing import CircuitBreaker, PacingGate
from marketagg.storage.payloads import PayloadStore

T = TypeVar("T")
log = get_logger("worker")


class CircuitOpen(Exception):
    def __init__(self, endpoint: str, until: datetime | None, disabled: bool) -> None:
        super().__init__(f"circuit {'disabled' if disabled else 'open'} for {endpoint}")
        self.until = until
        self.disabled = disabled


class RequestGate:
    def __init__(self, provider_id: str, pacing: PacingGate, circuits: CircuitBreaker,
                 counts: RunCounts, sleep: Callable[[float], None],
                 heartbeat: Callable[[], bool]) -> None:
        self.provider_id = provider_id
        self.pacing = pacing
        self.circuits = circuits
        self.counts = counts
        self.sleep = sleep
        self.heartbeat = heartbeat

    def call(self, endpoint: str, fn: Callable[[], T]) -> T:
        state = self.circuits.state(self.provider_id, endpoint)
        if not state.allows(self.circuits.clock.now()):
            raise CircuitOpen(endpoint, state.open_until, state.state == "disabled")
        while (wait := self.pacing.try_acquire()) > 0:
            self.sleep(wait)
        if not self.heartbeat():
            raise StaleLease("lease lost before provider call")
        try:
            result = fn()
        except ProviderError as exc:
            self.counts.add_requests(exc.request_counts.endpoint_requests,
                                     exc.request_counts.transport_requests)
            if not isinstance(exc, UnsupportedCapability):
                self.circuits.record_failure(self.provider_id, endpoint, exc)
            raise
        counts: RequestCounts | None = getattr(result, "request_counts", None)
        if counts is not None:
            self.counts.add_requests(counts.endpoint_requests, counts.transport_requests)
        self.circuits.record_success(self.provider_id, endpoint)
        return result


@dataclass
class JobContext:
    job: ClaimedJob
    engine: Engine
    repo: IngestRepository
    provider: MarketDataProvider
    calendar: SessionCalendar
    universe: UniverseConfig
    policies: PoliciesConfig
    clock: Clock
    gate: RequestGate
    counts: RunCounts
    payloads: PayloadStore | None
    instruments: Mapping[str, Any]
    policy_ids: Mapping[str, str] = field(default_factory=dict)

    def spool(self, provider_id: str, body: bytes | None) -> str | None:
        if body is None or self.payloads is None:
            return None
        return self.payloads.put(provider_id, body)


@dataclass(frozen=True)
class JobOutcome:
    status: str = "succeeded"
    diagnostics: Mapping[str, Any] = field(default_factory=dict)


Handler = Callable[[JobContext], JobOutcome]


class Worker:
    def __init__(
        self,
        *,
        engine: Engine,
        queue: JobQueue,
        repo: IngestRepository,
        provider: MarketDataProvider,
        calendar: SessionCalendar,
        universe: UniverseConfig,
        policies: PoliciesConfig,
        handlers: Mapping[str, Handler],
        pacing: PacingGate,
        circuits: CircuitBreaker,
        instruments: Mapping[str, Any],
        policy_ids: Mapping[str, str] | None = None,
        worker_id: str = "worker-1",
        clock: Clock = SYSTEM_CLOCK,
        sleep: Callable[[float], None] | None = None,
        payloads: PayloadStore | None = None,
        rng: random.Random | None = None,
        disk_paths: Sequence[Path] = (),
    ) -> None:
        self.engine = engine
        self.queue = queue
        self.repo = repo
        self.provider = provider
        self.calendar = calendar
        self.universe = universe
        self.policies = policies
        self.handlers = handlers
        self.pacing = pacing
        self.circuits = circuits
        self.instruments = instruments
        self.policy_ids = dict(policy_ids or {})
        self.worker_id = worker_id
        self.clock = clock
        self.sleep = sleep or _real_sleep
        self.payloads = payloads
        self.rng = rng or random.Random(0)  # noqa: S311 - jitter, not security
        self.disk_paths = tuple(disk_paths)

    def run_once(self, kinds: list[str] | None = None) -> ClaimedJob | None:
        level, usage = disk_level(self.disk_paths)
        claimable = allowed_kinds(level, kinds or sorted(self.handlers))
        if not claimable:
            log.error("disk pressure: collectors suspended", extra={"usage": usage})
            return None
        job = self.queue.claim(self.worker_id, claimable)
        if job is None:
            return None
        counts = RunCounts()
        gate = RequestGate(self.provider.provider_id, self.pacing, self.circuits, counts,
                           self.sleep, lambda: self.queue.heartbeat(job))
        ctx = JobContext(job, self.engine, self.repo, self.provider, self.calendar,
                         self.universe, self.policies, self.clock, gate, counts,
                         self.payloads, self.instruments, self.policy_ids)
        try:
            outcome = self.handlers[job.kind](ctx)
            counts.diagnostics.update(outcome.diagnostics)
            self.queue.complete(job, outcome.status, counts)
        except StaleLease:
            log.warning("lease lost; abandoning job", extra={"job_id": str(job.id)})
        except CircuitOpen as exc:
            if exc.disabled:  # access denied earlier: no retries until an operator acts
                self._terminal(job, counts, "skipped", "provider_disabled")
                return job
            self._retry_or_fail(job, counts, "circuit_open", retry_after=_seconds_until(
                exc.until, self.clock.now()))
        except RateLimited as exc:
            self._retry_or_fail(job, counts, "rate_limited", retry_after=exc.retry_after)
        except Unavailable:
            self._retry_or_fail(job, counts, "unavailable")
        except (Unauthorized, SchemaChanged, UnsupportedCapability) as exc:
            counts.diagnostics["error"] = exc.to_failure().model_dump(mode="json")
            self._terminal(job, counts, "failed", exc.code.value)
            if isinstance(exc, SchemaChanged | Unauthorized):
                self._incident(exc.code.value, {"job_id": str(job.id), "kind": job.kind,
                                                **exc.details})
        except Exception as exc:
            # A defect or host problem in one job (e.g. an unwritable volume) must not
            # crash-loop the worker and stop every other scope: bounded retries, then fail.
            log.exception("job handler error", extra={"job_id": str(job.id),
                                                       "kind": job.kind})
            counts.diagnostics["error"] = {"type": type(exc).__name__,
                                           "message": str(exc)[:300]}
            self._incident("internal_error", {"job_id": str(job.id), "kind": job.kind,
                                              "type": type(exc).__name__})
            self._retry_or_fail(job, counts, "internal_error")
        return job

    def run_until_idle(self, max_jobs: int = 10_000) -> int:
        done = 0
        while done < max_jobs and self.run_once() is not None:
            done += 1
        return done

    def _incident(self, kind: str, details: Mapping[str, Any]) -> None:
        with self.engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO operational_incidents (id, kind, severity, details, opened_at) "
                "VALUES (:id, :k, 'critical', CAST(:d AS jsonb), :now)"),
                {"id": uuid4(), "k": kind, "d": canonical_dumps(dict(details)),
                 "now": self.clock.now()})

    def _retry_or_fail(self, job: ClaimedJob, counts: RunCounts, code: str,
                       retry_after: float | None = None) -> None:
        retry = self.policies.retry
        when = next_retry_at(attempt=job.attempt, max_attempts=retry.max_attempts,
                             now=self.clock.now(), deadline=job.deadline_at, rng=self.rng,
                             retry_after=retry_after, base=retry.base_seconds,
                             cap=retry.cap_seconds)
        try:
            if when is None:
                self._terminal(job, counts, "failed" if code != "circuit_open" else "skipped",
                               code)
            else:
                self.queue.retry(job, when, counts, code)
        except StaleLease:
            log.warning("lease lost while recording failure", extra={"job_id": str(job.id)})

    def _terminal(self, job: ClaimedJob, counts: RunCounts, status: str, code: str) -> None:
        self.queue.complete(job, status, counts, error_code=code)
        if job.kind in LIVE_SNAPSHOT_KINDS:
            now = self.clock.now()
            with self.engine.begin() as conn:
                record_gap(conn, provider_id=job.provider_id or self.provider.provider_id,
                           instrument_id=self.instruments.get(str(job.scope.get("symbol"))),
                           kind="missing_chain_snapshot", start_at=job.scheduled_for,
                           end_at=max(now, job.scheduled_for + timedelta(seconds=1)),
                           scope=job.scope, status="unrecoverable", now=now,
                           evidence={"job_id": str(job.id), "error": code})


def _seconds_until(until: datetime | None, now: datetime) -> float | None:
    return None if until is None else max((until - now).total_seconds(), 0.0)


def _real_sleep(seconds: float) -> None:
    time.sleep(min(seconds, 60.0))
