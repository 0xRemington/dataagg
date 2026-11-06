"""Underlying bar collection: daily backfill (T07), rolling minutes and sweeps (T08).

- Only *closed* bars are persisted: a minute is eligible once
  ``fetch_started_at >= interval_end + (safe_lag - 60s)``; the in-progress bar a
  source returns is dropped and counted.
- Overlap re-fetches append a revision only when content changed (repository
  hashing) and dirty the containing 5m/15m/4h buckets.
- Daily history is fetched in bounded annual chunks with a small overlap and a
  durable resume cursor. Arrival time is today's knowledge time: backfilled
  history is never eligible for old observed-mode decisions.
- An empty history never implies delisting.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import Engine, text

from marketagg.analytics.aggregate import dirty_rows
from marketagg.calendar import SessionCalendar
from marketagg.clock import Clock
from marketagg.config import UniverseConfig
from marketagg.db.ingest import BarWrite, Fence, IngestRepository
from marketagg.db.partitions import ensure_partitions
from marketagg.domain import (
    ActionRequest,
    BarObservation,
    BarRequest,
    FetchResult,
    InstrumentRef,
    PolicyVersion,
    QualityFlag,
    Timeframe,
)
from marketagg.domain.primitives import canonical_dumps
from marketagg.providers.base import MarketDataProvider, SupportsActions
from marketagg.worker import JobContext, JobOutcome

CallFn = Callable[[str, Callable[[], Any]], Any]
SpoolFn = Callable[[str, bytes | None], str | None]


def bar_ingest_policy(universe: UniverseConfig, calendar_policy_id: str) -> PolicyVersion:
    return PolicyVersion.create("bars_ingest", {
        "version": 1, "provider": universe.provider, "closed_only": True,
        "safe_lag_seconds": universe.bars.minute_safe_lag_seconds,
        "session": "RTH", "calendar_policy": calendar_policy_id,
    }, "bars-v1")


def closed_writes(result: FetchResult[BarObservation], *, safe_lag: timedelta,
                  calendar: SessionCalendar) -> tuple[list[BarWrite], int]:
    """Closed, in-session bars as complete writes; returns (writes, dropped_unclosed)."""
    writes: list[BarWrite] = []
    dropped = 0
    margin = safe_lag - timedelta(minutes=1)
    for bar in result.rows:
        if bar.timeframe is Timeframe.M1:
            if bar.interval_end + margin > result.fetch_started_at:
                dropped += 1
                continue
            if calendar.session_at(bar.interval_start) is None:
                continue
            writes.append(BarWrite(bar, complete=True, expected_minutes=1, observed_minutes=1))
        else:
            if bar.interval_end > result.fetch_started_at:
                dropped += 1  # today's daily bar is not final at the bell (03)
                continue
            if calendar.session(bar.session_date) is None:
                continue  # a "daily bar" for a non-session day is a source anomaly
            writes.append(BarWrite(bar, complete=True))
    return _dedupe(writes), dropped


def _dedupe(writes: list[BarWrite]) -> list[BarWrite]:
    """Identical duplicates collapse; conflicting duplicates in one response are dropped
    (quarantined in the payload) rather than guessed between."""
    by_start: dict[datetime, list[BarWrite]] = {}
    for write in writes:
        by_start.setdefault(write.observation.interval_start, []).append(write)
    kept: list[BarWrite] = []
    for group in by_start.values():
        if len({w.content_sha256() for w in group}) == 1:
            kept.append(group[0])
    return sorted(kept, key=lambda w: w.observation.interval_start)


def ingest_bars(
    *,
    provider: MarketDataProvider,
    repo: IngestRepository,
    calendar: SessionCalendar,
    request: BarRequest,
    instrument_id: UUID,
    policy_id: str,
    safe_lag: timedelta,
    call: CallFn | None = None,
    fence: Fence | None = None,
    run_id: UUID | None = None,
    spool: SpoolFn | None = None,
    extra_flags: tuple[QualityFlag, ...] = (),
) -> dict[str, int]:
    """Fetch, filter, stage, publish and dirty one bar window."""
    def fetch() -> FetchResult[BarObservation]:
        return provider.fetch_bars(request)

    result: FetchResult[BarObservation] = (
        call(f"bars_{request.timeframe.value}", fetch) if call is not None else fetch())
    writes, dropped = closed_writes(result, safe_lag=safe_lag, calendar=calendar)
    if extra_flags:
        writes = [BarWrite(w.observation, w.complete, w.expected_minutes, w.observed_minutes,
                           extra_flags=extra_flags) for w in writes]
    uri = spool(result.provider_id, result.payload_body) if spool is not None else None
    outcome = repo.stage_bars(
        result, instrument_id=instrument_id, policy_id=policy_id,
        scope={"kind": f"bars_{request.timeframe.value}", "symbol": request.instrument.symbol,
               "start": request.start.isoformat(), "end": request.end.isoformat()},
        writes=writes, payload_uri=uri, fence=fence, run_id=run_id)
    if outcome.created:
        repo.publish(outcome.batch_id)
    changed: list[datetime] = []
    if outcome.written_ids and request.timeframe is Timeframe.M1:
        with repo.engine.connect() as conn:
            changed = list(conn.execute(text(
                "SELECT interval_start FROM bar_observations WHERE id = ANY(:ids)"),
                {"ids": list(outcome.written_ids)}).scalars())
        repo.mark_dirty(dirty_rows(calendar, changed, instrument_id=instrument_id,
                                   provider_id=result.provider_id,
                                   price_basis=writes[0].observation.price_basis,
                                   source_policy_id=policy_id))
    return {"received": len(result.rows), "written": outcome.rows_written,
            "unchanged": outcome.rows_unchanged, "unclosed_dropped": dropped,
            "rejected": len(result.rejected), "dirty_minutes": len(changed)}


def _request(ctx: JobContext, symbol: str, timeframe: Timeframe, start: datetime,
             end: datetime) -> BarRequest:
    deadline = ctx.job.deadline_at or (ctx.clock.now() + timedelta(minutes=30))
    return BarRequest(provider_id=ctx.provider.provider_id,
                      policy_id=ctx.policy_ids["bars_ingest"], deadline_at=deadline,
                      correlation_id=str(ctx.job.run_id),
                      instrument=InstrumentRef(provider_id=ctx.provider.provider_id,
                                               symbol=symbol),
                      timeframe=timeframe, start=start, end=end)


def collect_minutes(ctx: JobContext) -> JobOutcome:
    """Rolling 1m job: trailing overlap window, closed bars only."""
    symbol = str(ctx.job.scope["symbol"])
    now = ctx.clock.now().replace(second=0, microsecond=0)
    window = timedelta(minutes=int(ctx.job.scope.get("window_minutes", 20)))
    request = _request(ctx, symbol, Timeframe.M1, now - window, now)
    stats = ingest_bars(provider=ctx.provider, repo=ctx.repo, calendar=ctx.calendar,
                        request=request, instrument_id=ctx.instruments[symbol],
                        policy_id=ctx.policy_ids["bars_ingest"],
                        safe_lag=timedelta(seconds=ctx.universe.bars.minute_safe_lag_seconds),
                        call=ctx.gate.call, fence=ctx.job.fence, run_id=ctx.job.run_id,
                        spool=ctx.spool)
    ctx.counts.rows_received += stats["received"]
    ctx.counts.rows_written += stats["written"]
    return JobOutcome("succeeded", stats)


def minute_sweep(ctx: JobContext) -> JobOutcome:
    """Correction sweep over the last N sessions, one session per request."""
    symbol = str(ctx.job.scope["symbol"])
    sessions = int(ctx.job.scope.get("sessions", 2))
    today = ctx.calendar.local_date(ctx.clock.now())
    recent = [s for s in ctx.calendar.sessions(today - timedelta(days=10), today)
              if s.close_at <= ctx.clock.now()][-sessions:]
    totals: dict[str, int] = {}
    for session in recent:
        request = _request(ctx, symbol, Timeframe.M1, session.open_at, session.close_at)
        stats = ingest_bars(provider=ctx.provider, repo=ctx.repo, calendar=ctx.calendar,
                            request=request, instrument_id=ctx.instruments[symbol],
                            policy_id=ctx.policy_ids["bars_ingest"],
                            safe_lag=timedelta(seconds=ctx.universe.bars.minute_safe_lag_seconds),
                            call=ctx.gate.call, fence=ctx.job.fence, run_id=ctx.job.run_id,
                            spool=ctx.spool)
        for k, v in stats.items():
            totals[k] = totals.get(k, 0) + v
    ctx.counts.rows_written += totals.get("written", 0)
    return JobOutcome("succeeded", {"sessions": len(recent), **totals})


# -- Daily history and actions (T07) ------------------------------------------------------


def annual_chunks(start: date, end: date, overlap_days: int) -> list[tuple[date, date]]:
    """Half-open ``[start, end)`` chunks of at most one year, overlapping by N days."""
    chunks: list[tuple[date, date]] = []
    cursor = start
    while cursor < end:
        stop = min(date(cursor.year + 1, cursor.month, min(cursor.day, 28)), end)
        chunks.append((max(start, cursor - timedelta(days=overlap_days)), stop))
        cursor = stop
    return chunks


def cursor_key(provider_id: str, symbol: str, policy_id: str) -> str:
    return f"daily:{provider_id}:{symbol}:{policy_id}"


def read_cursor(engine: Engine, key: str) -> dict[str, Any] | None:
    with engine.connect() as conn:
        value = conn.execute(text("SELECT cursor FROM backfill_cursors WHERE scope_key=:k"),
                             {"k": key}).scalar_one_or_none()
    return None if value is None else dict(value)


def write_cursor(engine: Engine, key: str, cursor: Mapping[str, Any], now: datetime) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO backfill_cursors (scope_key, cursor, updated_at)
            VALUES (:k, CAST(:c AS jsonb), :now)
            ON CONFLICT (scope_key) DO UPDATE SET cursor=EXCLUDED.cursor,
                                                  updated_at=EXCLUDED.updated_at
        """), {"k": key, "c": canonical_dumps(dict(cursor)), "now": now})


def backfill_daily(
    *,
    provider: MarketDataProvider,
    repo: IngestRepository,
    calendar: SessionCalendar,
    clock: Clock,
    symbol: str,
    instrument_id: UUID,
    policy_id: str,
    years: int,
    overlap_days: int,
    call: CallFn | None = None,
    fence: Fence | None = None,
    run_id: UUID | None = None,
    max_chunks: int | None = None,
) -> dict[str, Any]:
    """Resumable daily backfill; the cursor records the last durably published chunk."""
    today = calendar.local_date(clock.now())
    start = date(today.year - years, today.month, min(today.day, 28))
    key = cursor_key(provider.provider_id, symbol, policy_id)
    cursor = read_cursor(repo.engine, key)
    resume_from = date.fromisoformat(cursor["next_start"]) if cursor else start
    chunks = annual_chunks(start, today + timedelta(days=1), overlap_days)
    pending = [c for c in chunks if c[1] > resume_from]
    if max_chunks is not None:
        pending = pending[:max_chunks]
    ensure_partitions(repo.engine, start, today)
    ref = InstrumentRef(provider_id=provider.provider_id, symbol=symbol)
    totals = {"chunks": 0, "written": 0, "unchanged": 0, "actions_written": 0, "rows": 0}
    for planned_start, chunk_end in pending:
        chunk_start = max(planned_start, resume_from - timedelta(days=overlap_days), start)
        request = BarRequest(
            provider_id=provider.provider_id, policy_id=policy_id,
            deadline_at=clock.now() + timedelta(minutes=30), correlation_id=key,
            instrument=ref, timeframe=Timeframe.D1,
            start=calendar_midnight(calendar, chunk_start),
            end=calendar_midnight(calendar, chunk_end))
        flags = (QualityFlag.BASIS_UNVERIFIED,) if _split_known(
            repo, instrument_id, provider.provider_id, clock.now()) else ()
        stats = ingest_bars(provider=provider, repo=repo, calendar=calendar, request=request,
                            instrument_id=instrument_id, policy_id=policy_id,
                            safe_lag=timedelta(0), call=call, fence=fence, run_id=run_id,
                            extra_flags=flags)
        totals["written"] += stats["written"]
        totals["unchanged"] += stats["unchanged"]
        totals["rows"] += stats["received"]
        if isinstance(provider, SupportsActions):
            totals["actions_written"] += ingest_actions(
                provider=provider, repo=repo, clock=clock, symbol=symbol,
                instrument_id=instrument_id, start=chunk_start, end=chunk_end,
                policy_id=policy_id, call=call, fence=fence, run_id=run_id)
        totals["chunks"] += 1
        write_cursor(repo.engine, key, {"next_start": chunk_end.isoformat(),
                                        "completed_chunks": totals["chunks"]}, clock.now())
    totals["resumed_from"] = resume_from.isoformat()  # type: ignore[assignment]
    return totals


def calendar_midnight(calendar: SessionCalendar, day: date) -> datetime:
    """Local exchange midnight of ``day`` as an aware UTC instant."""
    return datetime.combine(day, time(0), calendar.timezone).astimezone(UTC)


def ingest_actions(*, provider: MarketDataProvider, repo: IngestRepository, clock: Clock,
                   symbol: str, instrument_id: UUID, start: date, end: date, policy_id: str,
                   call: CallFn | None = None, fence: Fence | None = None,
                   run_id: UUID | None = None) -> int:
    if not isinstance(provider, SupportsActions):
        return 0
    provider_id = provider.provider_id
    request = ActionRequest(provider_id=provider_id, policy_id=policy_id,
                            deadline_at=clock.now() + timedelta(minutes=30),
                            correlation_id=f"actions:{symbol}",
                            instrument=InstrumentRef(provider_id=provider_id, symbol=symbol),
                            start_date=start, end_date=end)
    def fetch() -> FetchResult[Any]:
        return provider.fetch_actions(request)

    result = call("actions", fetch) if call is not None else fetch()
    outcome = repo.stage_actions(result, instrument_id=instrument_id,
                                 scope={"kind": "actions", "symbol": symbol,
                                        "start": start.isoformat(), "end": end.isoformat()},
                                 fence=fence, run_id=run_id)
    if outcome.created:
        repo.publish(outcome.batch_id)
    return outcome.rows_written


def _split_known(repo: IngestRepository, instrument_id: UUID, provider_id: str,
                 now: datetime) -> bool:
    with repo.engine.connect() as conn:
        return bool(conn.execute(text("""
            SELECT EXISTS (SELECT 1 FROM corporate_actions a
                           JOIN observation_batches x ON x.id = a.batch_id
                           WHERE a.instrument_id=:i AND a.provider_id=:p
                             AND a.action_type='split' AND x.status='published'
                             AND x.known_at <= :now)
        """), {"i": instrument_id, "p": provider_id, "now": now}).scalar_one())


def daily_refresh(ctx: JobContext) -> JobOutcome:
    """Re-fetch the last N sessions of daily bars (03: close+30m and preopen)."""
    symbol = str(ctx.job.scope["symbol"])
    sessions = int(ctx.job.scope.get("sessions", 10))
    today = ctx.calendar.local_date(ctx.clock.now())
    recent = ctx.calendar.sessions(today - timedelta(days=sessions * 2 + 7), today)[-sessions:]
    if not recent:
        return JobOutcome("skipped", {"reason": "no sessions"})
    request = _request(ctx, symbol, Timeframe.D1, recent[0].open_at,
                       recent[-1].close_at + timedelta(hours=1))
    stats = ingest_bars(provider=ctx.provider, repo=ctx.repo, calendar=ctx.calendar,
                        request=request, instrument_id=ctx.instruments[symbol],
                        policy_id=ctx.policy_ids["bars_ingest"], safe_lag=timedelta(0),
                        call=ctx.gate.call, fence=ctx.job.fence, run_id=ctx.job.run_id)
    ctx.counts.rows_written += stats["written"]
    return JobOutcome("succeeded", stats)


def daily_backfill_job(ctx: JobContext) -> JobOutcome:
    symbol = str(ctx.job.scope["symbol"])
    totals = backfill_daily(
        provider=ctx.provider, repo=ctx.repo, calendar=ctx.calendar, clock=ctx.clock,
        symbol=symbol, instrument_id=ctx.instruments[symbol],
        policy_id=ctx.policy_ids["bars_ingest"],
        years=int(ctx.job.scope.get("years", ctx.universe.bars.daily_history_years)),
        overlap_days=ctx.universe.bars.daily_chunk_overlap_days, call=ctx.gate.call,
        fence=ctx.job.fence, run_id=ctx.job.run_id)
    ctx.counts.rows_written += int(totals["written"])
    return JobOutcome("succeeded", totals)


__all__: Sequence[str] = ["annual_chunks", "backfill_daily", "closed_writes", "collect_minutes",
                          "daily_backfill_job", "daily_refresh", "ingest_actions", "ingest_bars",
                          "minute_sweep"]
