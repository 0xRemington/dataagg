"""Calendar-driven job intents (03 "Authoritative schedule").

Schedules derive from the session calendar, never from cron strings, so holidays
create no jobs and early closes move close-relative jobs. ``due_intents`` only
returns intents whose deadline has not passed: a restarted scheduler resumes the
next live slot instead of recreating the past.
"""

from __future__ import annotations

import math
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Any

from sqlalchemy import Connection, Engine

from marketagg.calendar import Session, SessionCalendar
from marketagg.calendar.sessions import at_local
from marketagg.clock import SYSTEM_CLOCK, Clock
from marketagg.config import PoliciesConfig, UniverseConfig
from marketagg.scheduler.leases import SHEDDABLE_KINDS, JobIntent, JobQueue, record_gap

M = timedelta(minutes=1)
# The broad sweep runs as small chunks spread over the last BROAD_WINDOW before the
# close. One 24-expiry job held the single worker for ~2.5 minutes at 10 requests/minute
# and starved 1m bars and the closing priority slot; chunks interleave with them.
BROAD_CHUNK_EXPIRIES = 3
BROAD_WINDOW = 20 * M

PRIORITY = {
    "bars_1m": 10,
    "discovery": 15,
    "chain_priority": 20,
    "derive_bars": 25,
    "chain_other": 40,
    "chain_broad": 50,
    "daily_refresh": 60,
    "minute_sweep": 60,
    "gap_scan": 70,
    "daily_backfill": 80,  # bulk history must never consume live headroom (03 step 4)
}


@dataclass(frozen=True)
class PlanSummary:
    created: int
    shed: int
    considered: int


class SchedulePlanner:
    def __init__(self, calendar: SessionCalendar, universe: UniverseConfig,
                 policies: PoliciesConfig, policy_id: str) -> None:
        self.calendar = calendar
        self.universe = universe
        self.policies = policies
        self.policy_id = policy_id
        self._cache: dict[tuple[Any, ...], list[JobIntent]] = {}

    def intents_for_session(self, session: Session) -> list[JobIntent]:
        """Deterministic per session; memoised because the planner ticks every 5s."""
        key = (session.session_date, session.open_at, session.close_at)
        if key not in self._cache:
            if len(self._cache) > 16:
                self._cache.clear()
            self._cache[key] = sorted(self._session_intents(session),
                                      key=lambda i: (i.not_before, i.priority, i.kind,
                                                     str(i.scope)))
        return self._cache[key]

    def due_intents(self, now: datetime, horizon: timedelta = timedelta(minutes=10)
                    ) -> list[JobIntent]:
        sessions = self.calendar.sessions(self.calendar.local_date(now) - timedelta(days=1),
                                          self.calendar.local_date(now) + timedelta(days=1))
        due: list[JobIntent] = []
        for session in sessions:
            for intent in self.intents_for_session(session):
                if intent.not_before > now + horizon:
                    continue
                expiry = intent.deadline_at or intent.not_before + timedelta(hours=6)
                if expiry <= now:
                    continue  # never recreate the past as live work
                due.append(intent)
        return sorted(due, key=lambda i: (i.not_before, i.priority))

    def enqueue_due(self, queue: JobQueue, now: datetime,
                    horizon: timedelta = timedelta(minutes=10)) -> PlanSummary:
        intents = self.due_intents(now, horizon)
        shedding = queue.backlog() > self.policies.schedule.backlog_shed_threshold
        created = shed = 0
        for intent in intents:
            if shedding and intent.kind in SHEDDABLE_KINDS:
                if queue.enqueue(intent, status="skipped", error_code="shed_backlog"):
                    shed += 1
                    with queue.engine.begin() as conn:
                        record_gap(conn, provider_id=self.universe.provider, instrument_id=None,
                                   kind="shed_chain_snapshot", start_at=intent.scheduled_for,
                                   end_at=intent.deadline_at or intent.scheduled_for + M,
                                   scope=intent.scope, status="unrecoverable", now=now,
                                   evidence={"reason": "backlog shed lower-priority scope"})
                continue
            created += queue.enqueue(intent)
        return PlanSummary(created, shed, len(intents))

    # -- Per-session schedule ------------------------------------------------------------

    def _session_intents(self, session: Session) -> Iterator[JobIntent]:
        symbols = [s.symbol for s in self.universe.symbols]
        tz = self.calendar.timezone
        open_at, close_at = session.open_at, session.close_at
        day = session.session_date
        overlap = self.universe.bars.minute_overlap_minutes
        chain_deadline = timedelta(seconds=self.policies.schedule.priority_chain_deadline_seconds)

        for i, symbol in enumerate(symbols):
            stagger = timedelta(seconds=15 * i)
            discovery_at = min(at_local(day, time(9, 15), tz), open_at - 15 * M)
            yield self._intent("discovery", {"symbol": symbol}, discovery_at + stagger,
                               open_at + 30 * M)

            minute = open_at + 2 * M
            while minute <= close_at + 5 * M:
                at = minute + timedelta(seconds=20)
                yield self._intent("bars_1m", {"symbol": symbol, "window_minutes": overlap},
                                   at, at + timedelta(seconds=55))
                minute += M

            slot = open_at + 5 * M
            while slot <= close_at:
                yield self._intent("chain_priority",
                                   {"symbol": symbol, "tier": "priority",
                                    "slot": slot.isoformat()},
                                   slot, slot + chain_deadline, not_before=slot + stagger)
                slot += 5 * M

            slot = open_at + 5 * M
            while slot <= close_at - 25 * M:
                yield self._intent("chain_other",
                                   {"symbol": symbol, "tier": "other", "slot": slot.isoformat()},
                                   slot, slot + 10 * M, not_before=slot + stagger)
                slot += 60 * M

            broad = close_at - BROAD_WINDOW
            chunks = max(1, math.ceil(self.universe.options.broad_expiries_max
                                      / BROAD_CHUNK_EXPIRIES))
            for chunk in range(chunks):
                at = broad + (BROAD_WINDOW - 5 * M) * chunk / chunks
                yield self._intent("chain_broad",
                                   {"symbol": symbol, "tier": "broad", "slot": broad.isoformat(),
                                    "chunk": chunk, "chunks": chunks},
                                   at, close_at, not_before=at + stagger)

            for label, at in (("preopen", open_at - 30 * M), ("close", close_at + 30 * M)):
                yield self._intent("daily_refresh",
                                   {"symbol": symbol, "sessions": 10, "phase": label},
                                   at, at + 120 * M)
            for label, at in (("preopen", open_at - 25 * M), ("close", close_at + 10 * M)):
                yield self._intent("minute_sweep",
                                   {"symbol": symbol, "sessions": 2, "phase": label},
                                   at, at + 60 * M)

        scan = open_at + 15 * M
        while scan <= close_at + 20 * M:
            yield self._intent("gap_scan", {"session": day.isoformat()}, scan, scan + 15 * M)
            scan += 15 * M
        minute = open_at + M
        while minute <= close_at + 10 * M:
            at = minute + timedelta(seconds=40)
            yield self._intent("derive_bars", {"session": day.isoformat()}, at,
                               at + timedelta(seconds=55))
            minute += M

    def history_bootstrap_intents(self, now: datetime) -> list[JobIntent]:
        """Initial history (03 steps 2-3): bounded daily backfill and recent 1m sweep."""
        today = self.calendar.local_date(now)
        days = self.universe.bars.minute_bootstrap_days
        sessions = [s for s in self.calendar.sessions(today - timedelta(days=days), today)
                    if s.close_at <= now]
        intents = []
        for symbol in (s.symbol for s in self.universe.symbols):
            intents.append(self._intent(
                "daily_backfill", {"symbol": symbol, "phase": "bootstrap",
                                   "years": self.universe.bars.daily_history_years},
                now, None))
            if sessions:
                intents.append(self._intent(
                    "minute_sweep", {"symbol": symbol, "phase": "bootstrap",
                                     "sessions": len(sessions)}, now, None))
        if sessions:  # after the sweeps: derive 5m/15m/4h for the backfilled sessions
            intents.append(self._intent("derive_bars", {"symbol": "*", "phase": "bootstrap"},
                                        now, None, priority=PRIORITY["daily_backfill"] + 5))
        return intents

    def _intent(self, kind: str, scope: dict[str, object], scheduled_for: datetime,
                deadline: datetime | None, *, not_before: datetime | None = None,
                priority: int | None = None) -> JobIntent:
        return JobIntent(
            kind=kind, provider_id=self.universe.provider, scope=scope,
            scheduled_for=scheduled_for, not_before=not_before or scheduled_for,
            deadline_at=deadline, policy_id=self.policy_id,
            priority=PRIORITY[kind] if priority is None else priority,
        )


class SchedulerService:
    """Single authoritative planner guarded by a session-level advisory lock."""

    LOCK_KEY = 0x6D61726B65746167  # "marketag"

    def __init__(self, engine: Engine, planner: SchedulePlanner, queue: JobQueue,
                 holder: str, clock: Clock = SYSTEM_CLOCK) -> None:
        self.engine = engine
        self.planner = planner
        self.queue = queue
        self.holder = holder
        self.clock = clock
        self._lock_conn: Connection | None = None

    def acquire(self) -> bool:
        if self._lock_conn is not None:
            return True
        conn = self.engine.connect()
        acquired = bool(conn.exec_driver_sql(
            f"SELECT pg_try_advisory_lock({self.LOCK_KEY})").scalar_one())
        conn.commit()
        if not acquired:
            conn.close()
            return False
        conn.detach()  # dedicated session; never returned to the pool
        self._lock_conn = conn
        return True

    def holds_lock(self) -> bool:
        if self._lock_conn is None:
            return False
        try:
            held = self._lock_conn.exec_driver_sql(
                "SELECT count(*) FROM pg_locks WHERE locktype='advisory' "
                "AND pid=pg_backend_pid() AND granted").scalar_one()
            self._lock_conn.commit()
            return bool(held)
        except Exception:  # connection lost => lock lost; stop planning immediately
            self._lock_conn = None
            return False

    def tick(self) -> PlanSummary | None:
        if not self.holds_lock() and not self.acquire():
            return None
        now = self.clock.now()
        summary = self.planner.enqueue_due(self.queue, now)
        with self.engine.begin() as conn:
            conn.exec_driver_sql(
                "INSERT INTO scheduler_state (singleton, holder, heartbeat_at, last_planned_at) "
                "VALUES (true, %(h)s, %(now)s, %(now)s) ON CONFLICT (singleton) DO UPDATE SET "
                "holder=EXCLUDED.holder, heartbeat_at=EXCLUDED.heartbeat_at, "
                "last_planned_at=EXCLUDED.last_planned_at",
                {"h": self.holder, "now": now},
            )
        return summary

    def release(self) -> None:
        if self._lock_conn is not None:
            # Detached session: closing really closes it, which releases the lock.
            try:
                self._lock_conn.exec_driver_sql("SELECT pg_advisory_unlock_all()")
            finally:
                self._lock_conn.close()
            self._lock_conn = None
