"""Durable job queue with leases and fencing tokens (03 "Claims, leases, and publication").

- ``enqueue`` is idempotent on the deterministic ``job_key``.
- ``claim`` uses ``FOR UPDATE SKIP LOCKED`` in a short transaction, increments the
  fencing token and creates the attempt row atomically. Expired leases are
  reclaimable; the stale holder's fence no longer matches, so its writes fail.
- Live chain slots past their deadline are skipped (never fetched late as if on
  time) and recorded as unrecoverable missing-snapshot gaps.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Connection, Engine, text

from marketagg.clock import SYSTEM_CLOCK, Clock
from marketagg.db.errors import StaleLease
from marketagg.db.ingest import Fence
from marketagg.domain.primitives import canonical_dumps

# Live snapshots cannot be repaired later: fetching now would be a different observation.
LIVE_SNAPSHOT_KINDS = frozenset({"chain_priority", "chain_other", "chain_broad"})
SHEDDABLE_KINDS = frozenset({"chain_other", "chain_broad"})


def job_key(kind: str, provider_id: str | None, scope: Mapping[str, Any],
            scheduled_for: datetime, policy_id: str) -> str:
    material = canonical_dumps({
        "kind": kind, "provider": provider_id, "scope": scope,
        "scheduled_for": scheduled_for.isoformat(), "policy": policy_id,
    })
    return hashlib.sha256(material.encode()).hexdigest()


@dataclass(frozen=True)
class JobIntent:
    kind: str
    provider_id: str | None
    scope: Mapping[str, Any]
    scheduled_for: datetime
    not_before: datetime
    deadline_at: datetime | None
    policy_id: str
    priority: int

    @property
    def key(self) -> str:
        return job_key(self.kind, self.provider_id, self.scope, self.scheduled_for,
                       self.policy_id)


@dataclass(frozen=True)
class ClaimedJob:
    id: UUID
    kind: str
    provider_id: str | None
    scope: Mapping[str, Any]
    scheduled_for: datetime
    deadline_at: datetime | None
    policy_id: str
    attempt: int
    run_id: UUID
    fence: Fence


@dataclass
class RunCounts:
    endpoint_requests: int = 0
    transport_requests: int | None = 0
    rows_received: int = 0
    rows_written: int = 0
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def add_requests(self, endpoint: int, transport: int | None) -> None:
        self.endpoint_requests += endpoint
        if transport is None or self.transport_requests is None:
            self.transport_requests = None  # hidden calls: never claim a strict count
        else:
            self.transport_requests += transport


class JobQueue:
    def __init__(self, engine: Engine, clock: Clock = SYSTEM_CLOCK,
                 lease: timedelta = timedelta(seconds=180)) -> None:
        self.engine = engine
        self.clock = clock
        self.lease = lease

    def enqueue(self, intent: JobIntent, *, status: str = "pending",
                error_code: str | None = None) -> bool:
        with self.engine.begin() as conn:
            created = conn.execute(
                text("""
                    INSERT INTO collection_jobs (id, job_key, kind, provider_id, scheduled_for,
                        not_before, deadline_at, scope, policy_id, priority, status,
                        last_error_code, created_at)
                    VALUES (:id, :key, :kind, :p, :sched, :nb, :dl, CAST(:scope AS jsonb),
                            :pol, :prio, :status, :err, :now)
                    ON CONFLICT (job_key) DO NOTHING RETURNING id
                """),
                {"id": uuid4(), "key": intent.key, "kind": intent.kind,
                 "p": intent.provider_id, "sched": intent.scheduled_for,
                 "nb": intent.not_before, "dl": intent.deadline_at,
                 "scope": canonical_dumps(intent.scope), "pol": intent.policy_id,
                 "prio": intent.priority, "status": status, "err": error_code,
                 "now": self.clock.now()},
            ).scalar_one_or_none()
        return created is not None

    def backlog(self) -> int:
        with self.engine.connect() as conn:
            return int(conn.execute(
                text("SELECT count(*) FROM collection_jobs WHERE status='pending' "
                     "AND not_before <= :now"),
                {"now": self.clock.now()},
            ).scalar_one())

    def claim(self, worker_id: str, kinds: Sequence[str] | None = None) -> ClaimedJob | None:
        """Claim the most urgent ready job, skipping overdue live slots on the way."""
        for _ in range(100):
            now = self.clock.now()
            with self.engine.begin() as conn:
                row = conn.execute(
                    text("""
                        SELECT * FROM collection_jobs
                        WHERE ((status = 'pending' AND not_before <= :now)
                               OR (status = 'running' AND lease_until < :now))
                          AND (CAST(:kinds AS text[]) IS NULL OR kind = ANY(:kinds))
                        ORDER BY priority, not_before, id
                        FOR UPDATE SKIP LOCKED LIMIT 1
                    """),
                    {"now": now, "kinds": list(kinds) if kinds else None},
                ).mappings().one_or_none()
                if row is None:
                    return None
                if row["deadline_at"] is not None and now >= row["deadline_at"]:
                    # Never spend a provider request on an overdue job; live snapshot
                    # slots additionally become unrecoverable gaps.
                    self._skip_overdue(conn, dict(row), now)
                    continue
                token = row["lease_token"] + 1
                attempt = row["attempts"] + 1
                if row["status"] == "running":  # expired lease: close the abandoned attempt
                    conn.execute(
                        text("UPDATE ingestion_runs SET status='abandoned', finished_at=:now, "
                             "error_code='lease_expired' WHERE job_id=:j AND finished_at IS NULL"),
                        {"now": now, "j": row["id"]},
                    )
                conn.execute(
                    text("""
                        UPDATE collection_jobs SET status='running', lease_owner=:w,
                            lease_token=:t, lease_until=:until, attempts=:a
                        WHERE id=:id
                    """),
                    {"w": worker_id, "t": token, "until": now + self.lease, "a": attempt,
                     "id": row["id"]},
                )
                run_id = uuid4()
                conn.execute(
                    text("""
                        INSERT INTO ingestion_runs (id, job_id, attempt, lease_token,
                                                    started_at, status)
                        VALUES (:id, :j, :a, :t, :now, 'running')
                    """),
                    {"id": run_id, "j": row["id"], "a": attempt, "t": token, "now": now},
                )
                return ClaimedJob(
                    id=UUID(str(row["id"])), kind=row["kind"], provider_id=row["provider_id"],
                    scope=row["scope"], scheduled_for=row["scheduled_for"],
                    deadline_at=row["deadline_at"], policy_id=row["policy_id"],
                    attempt=attempt, run_id=run_id, fence=Fence(UUID(str(row["id"])), token),
                )
        return None

    def heartbeat(self, job: ClaimedJob) -> bool:
        with self.engine.begin() as conn:
            updated = conn.execute(
                text("UPDATE collection_jobs SET lease_until=:until WHERE id=:id "
                     "AND lease_token=:t AND status='running' AND lease_until > :now"),
                {"until": self.clock.now() + self.lease, "id": job.id,
                 "t": job.fence.lease_token, "now": self.clock.now()},
            ).rowcount
        return bool(updated)

    def complete(self, job: ClaimedJob, status: str, counts: RunCounts,
                 error_code: str | None = None) -> None:
        if status not in {"succeeded", "partial", "failed", "skipped"}:
            raise ValueError(f"invalid terminal status {status}")
        self._finish(job, status, counts, error_code, job_status=status)

    def retry(self, job: ClaimedJob, not_before: datetime, counts: RunCounts,
              error_code: str) -> None:
        self._finish(job, "retry", counts, error_code, job_status="pending",
                     not_before=not_before)

    def _finish(self, job: ClaimedJob, run_status: str, counts: RunCounts,
                error_code: str | None, *, job_status: str,
                not_before: datetime | None = None) -> None:
        now = self.clock.now()
        with self.engine.begin() as conn:
            updated = conn.execute(
                text("""
                    UPDATE collection_jobs SET status=:s, last_error_code=:err,
                        lease_owner=NULL, lease_until=NULL,
                        not_before=COALESCE(:nb, not_before)
                    WHERE id=:id AND lease_token=:t AND status='running'
                """),
                {"s": job_status, "err": error_code, "nb": not_before, "id": job.id,
                 "t": job.fence.lease_token},
            ).rowcount
            if not updated:
                raise StaleLease(f"job {job.id} fence {job.fence.lease_token} lost")
            conn.execute(
                text("""
                    UPDATE ingestion_runs SET status=:s, finished_at=:now, error_code=:err,
                        endpoint_requests=:er, transport_requests=:tr, rows_received=:rr,
                        rows_written=:rw, diagnostics=CAST(:diag AS jsonb)
                    WHERE id=:id
                """),
                {"s": run_status, "now": now, "err": error_code,
                 "er": counts.endpoint_requests, "tr": counts.transport_requests,
                 "rr": counts.rows_received, "rw": counts.rows_written,
                 "diag": canonical_dumps(counts.diagnostics), "id": job.run_id},
            )

    def _skip_overdue(self, conn: Connection, row: Mapping[str, Any], now: datetime) -> None:
        conn.execute(
            text("UPDATE collection_jobs SET status='skipped', last_error_code='deadline_passed',"
                 " lease_owner=NULL, lease_until=NULL WHERE id=:id"),
            {"id": row["id"]},
        )
        if row["status"] == "running":
            conn.execute(
                text("UPDATE ingestion_runs SET status='abandoned', finished_at=:now, "
                     "error_code='lease_expired' WHERE job_id=:j AND finished_at IS NULL"),
                {"now": now, "j": row["id"]},
            )
        if row["kind"] not in LIVE_SNAPSHOT_KINDS:
            return  # rolling/maintenance work is covered by the next run or a repair
        record_gap(conn, provider_id=row["provider_id"], instrument_id=None,
                   kind="missing_chain_snapshot", start_at=row["scheduled_for"],
                   end_at=row["deadline_at"], scope=row["scope"], status="unrecoverable",
                   now=now, evidence={"job_id": str(row["id"]),
                                      "reason": "live slot passed its deadline; a later fetch "
                                                "is a different observation"})


def record_gap(conn: Connection, *, provider_id: str, instrument_id: UUID | None, kind: str,
               start_at: datetime, end_at: datetime, scope: Mapping[str, Any], status: str,
               now: datetime, evidence: Mapping[str, Any],
               recoverable_until: datetime | None = None) -> UUID:
    gap_id = uuid4()
    conn.execute(
        text("""
            INSERT INTO data_gaps (id, provider_id, instrument_id, kind, start_at, end_at,
                scope, status, first_detected_at, updated_at, recoverable_until, evidence)
            VALUES (:id, :p, :i, :k, :s, :e, CAST(:scope AS jsonb), :st, :now, :now, :ru,
                    CAST(:ev AS jsonb))
        """),
        {"id": gap_id, "p": provider_id, "i": instrument_id, "k": kind, "s": start_at,
         "e": end_at, "scope": canonical_dumps(scope), "st": status, "now": now,
         "ru": recoverable_until, "ev": canonical_dumps(evidence)},
    )
    return gap_id
