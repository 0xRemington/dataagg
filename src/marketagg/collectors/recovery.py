"""Minute gap detection and repair (T08, 03 "Quality and gaps").

- Expected minutes come from the session calendar, only for minutes whose
  publication grace has passed; weekends/holidays never create gaps.
- Missing minutes are never synthesized. A missing quote/bar does not prove a
  halt: ``halt_confirmed`` requires explicit evidence (``confirm_halt``).
- Repairs run earliest-expiring first within the source horizon. Past the horizon
  a gap becomes ``unrecoverable``; repeated failures escalate as quality issues.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import Engine, text

from marketagg.analytics.aggregate import derive_pending, dirty_rows
from marketagg.analytics.asof import BarKey, eligible_bars
from marketagg.calendar import SessionCalendar
from marketagg.collectors.bars import CallFn, ingest_bars
from marketagg.db.ingest import Fence, IngestRepository
from marketagg.domain import (
    BarRequest,
    InstrumentRef,
    PriceBasis,
    ProviderCapabilities,
    SessionKind,
    Timeframe,
)
from marketagg.domain.primitives import canonical_dumps
from marketagg.providers.base import MarketDataProvider
from marketagg.scheduler.leases import record_gap
from marketagg.worker import JobContext, JobOutcome

ONE = timedelta(minutes=1)


def minute_horizon(capabilities: ProviderCapabilities, fallback_days: int) -> tuple[timedelta,
                                                                                   str]:
    """Oldest retrievable 1m age: measured capability, else a labelled assumption."""
    cap = capabilities.bars.get(Timeframe.M1)
    if cap is not None and cap.oldest_history_seconds:
        return timedelta(seconds=cap.oldest_history_seconds), "capability"
    return timedelta(days=fallback_days), "assumed (unprobed)"


def _ranges(missing: Sequence[datetime]) -> list[tuple[datetime, datetime]]:
    ranges: list[tuple[datetime, datetime]] = []
    for minute in sorted(missing):
        if ranges and ranges[-1][1] == minute:
            ranges[-1] = (ranges[-1][0], minute + ONE)
        else:
            ranges.append((minute, minute + ONE))
    return ranges


@dataclass(frozen=True)
class ScanResult:
    expected: int
    present: int
    opened: int
    repaired: int


def scan_minute_gaps(
    engine: Engine,
    calendar: SessionCalendar,
    *,
    key: BarKey,
    start: datetime,
    now: datetime,
    grace: timedelta,
    horizon: timedelta,
    horizon_source: str,
) -> ScanResult:
    end = now - grace
    expected = calendar.expected_minutes(start, end)
    with engine.connect() as conn:
        present = {b.interval_start for b in eligible_bars(conn, key, start=start, end=end,
                                                           decision_at=now)}
        existing = conn.execute(text("""
            SELECT id, start_at, end_at, status FROM data_gaps
            WHERE provider_id=:p AND instrument_id=:i AND kind='missing_minutes'
              AND start_at < :end AND end_at > :start
        """), {"p": key.provider_id, "i": key.instrument_id, "start": start,
               "end": end}).all()
    missing = [m for m in expected if m not in present]
    opened = repaired = 0
    with engine.begin() as conn:
        for gap in existing:
            if gap.status in ("open", "retrying") and all(
                    m in present for m in calendar.expected_minutes(gap.start_at, gap.end_at)):
                conn.execute(text("UPDATE data_gaps SET status='repaired', updated_at=:now "
                                  "WHERE id=:id"), {"now": now, "id": gap.id})
                repaired += 1
        known = {(g.start_at, g.end_at) for g in existing if g.status != "repaired"}
        for gap_start, gap_end in _ranges(missing):
            if (gap_start, gap_end) in known:
                continue
            recoverable_until = gap_start + horizon
            record_gap(conn, provider_id=key.provider_id, instrument_id=key.instrument_id,
                       kind="missing_minutes", start_at=gap_start, end_at=gap_end,
                       scope={"timeframe": "1m", "policy": key.policy_id},
                       status="unrecoverable" if now >= recoverable_until else "open",
                       now=now, recoverable_until=recoverable_until,
                       evidence={"expected_minutes": int((gap_end - gap_start) / ONE),
                                 "cause": "unknown (missing is not proof of halt/no-trade)",
                                 "horizon": horizon_source})
            opened += 1
    return ScanResult(len(expected), len(present & set(expected)), opened, repaired)


def repair_gaps(
    *,
    engine: Engine,
    provider: MarketDataProvider,
    repo: IngestRepository,
    calendar: SessionCalendar,
    key: BarKey,
    symbol: str,
    now: datetime,
    max_cycles: int,
    call: CallFn | None = None,
    fence: Fence | None = None,
    limit: int = 20,
) -> dict[str, int]:
    """Repair open gaps earliest-expiring first; never fabricate data."""
    with engine.connect() as conn:
        gaps = conn.execute(text("""
            SELECT * FROM data_gaps
            WHERE provider_id=:p AND instrument_id=:i AND kind='missing_minutes'
              AND status IN ('open','retrying')
            ORDER BY recoverable_until NULLS LAST, start_at LIMIT :n
        """), {"p": key.provider_id, "i": key.instrument_id, "n": limit}).mappings().all()
    stats = {"attempted": 0, "repaired": 0, "unrecoverable": 0, "escalated": 0}
    for gap in gaps:
        if gap["recoverable_until"] is not None and now >= gap["recoverable_until"]:
            _set_gap(engine, gap["id"], "unrecoverable", now, gap["attempt_count"])
            stats["unrecoverable"] += 1
            continue
        stats["attempted"] += 1
        request = BarRequest(provider_id=key.provider_id, policy_id=key.policy_id,
                             deadline_at=now + timedelta(minutes=10),
                             correlation_id=f"repair:{gap['id']}",
                             instrument=InstrumentRef(provider_id=key.provider_id,
                                                      symbol=symbol),
                             timeframe=Timeframe.M1, start=gap["start_at"], end=gap["end_at"])
        ingest_bars(provider=provider, repo=repo, calendar=calendar, request=request,
                    instrument_id=key.instrument_id, policy_id=key.policy_id,
                    safe_lag=timedelta(0), call=call, fence=fence)
        with engine.connect() as conn:
            present = {b.interval_start for b in eligible_bars(
                conn, key, start=gap["start_at"], end=gap["end_at"],
                decision_at=repo.clock.now())}
        if all(m in present for m in calendar.expected_minutes(gap["start_at"], gap["end_at"])):
            _set_gap(engine, gap["id"], "repaired", now, gap["attempt_count"] + 1)
            stats["repaired"] += 1
            continue
        attempts = gap["attempt_count"] + 1
        _set_gap(engine, gap["id"], "retrying", now, attempts)
        if attempts >= max_cycles:
            repo.record_quality_issue(None, {"gap": str(gap["id"])}, "gap_repair_escalated",
                                      "error", {"attempts": attempts})
            stats["escalated"] += 1
    return stats


def _set_gap(engine: Engine, gap_id: UUID, status: str, now: datetime, attempts: int) -> None:
    with engine.begin() as conn:
        conn.execute(text("UPDATE data_gaps SET status=:s, updated_at=:now, attempt_count=:a "
                          "WHERE id=:id"), {"s": status, "now": now, "a": attempts, "id": gap_id})


def confirm_halt(engine: Engine, gap_id: UUID, evidence: Mapping[str, Any],
                 now: datetime) -> None:
    """Mark a gap as an exchange-confirmed halt; requires explicit authoritative evidence."""
    if not evidence.get("source") or not evidence.get("reference"):
        raise ValueError("halt confirmation needs an authoritative source and reference")
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE data_gaps SET status='halt_confirmed', updated_at=:now,
                   evidence = evidence || CAST(:ev AS jsonb)
            WHERE id=:id
        """), {"now": now, "id": gap_id, "ev": canonical_dumps({"halt": dict(evidence)})})


def invalidate_minute(repo: IngestRepository, calendar: SessionCalendar, *, bar_id: UUID,
                      interval_start: datetime, key: BarKey, reason: str) -> UUID:
    """Invalidate a published minute and dirty every dependent derived bucket."""
    batch = repo.invalidate_bar(bar_id, interval_start, reason=reason)
    repo.publish(batch)
    repo.mark_dirty(dirty_rows(calendar, [interval_start], instrument_id=key.instrument_id,
                               provider_id=key.provider_id, price_basis=key.price_basis,
                               source_policy_id=key.policy_id))
    return batch


def _key(ctx: JobContext, symbol: str) -> BarKey:
    return BarKey(ctx.instruments[symbol], ctx.provider.provider_id, Timeframe.M1,
                  PriceBasis.PROVIDER_NATIVE, SessionKind.RTH, ctx.policy_ids["bars_ingest"])


def gap_scan_job(ctx: JobContext) -> JobOutcome:
    now = ctx.clock.now()
    horizon, source = minute_horizon(ctx.provider.capabilities(),
                                     ctx.universe.bars.minute_bootstrap_days)
    grace = timedelta(minutes=ctx.universe.bars.publication_grace_minutes)
    diagnostics: dict[str, Any] = {}
    for symbol in (s.symbol for s in ctx.universe.symbols):
        key = _key(ctx, symbol)
        session = ctx.calendar.previous_session(ctx.calendar.local_date(now))
        result = scan_minute_gaps(ctx.engine, ctx.calendar, key=key, start=session.open_at,
                                  now=now, grace=grace, horizon=horizon, horizon_source=source)
        repaired = repair_gaps(engine=ctx.engine, provider=ctx.provider, repo=ctx.repo,
                               calendar=ctx.calendar, key=key, symbol=symbol, now=now,
                               max_cycles=ctx.universe.bars.max_repair_cycles,
                               call=ctx.gate.call, fence=ctx.job.fence)
        diagnostics[symbol] = {"scan": result.__dict__, "repair": repaired}
    return JobOutcome("succeeded", diagnostics)


def derive_job(ctx: JobContext, max_passes: int = 20) -> JobOutcome:
    """Drain closed dirty windows in bounded passes (a history bootstrap leaves ~1k)."""
    totals: dict[str, int] = {}
    for _ in range(max_passes):
        summary = derive_pending(
            ctx.engine, ctx.repo, ctx.calendar,
            aggregation_policy_id=ctx.policy_ids["aggregation"], now=ctx.clock.now(),
            instruments={v: k for k, v in ctx.instruments.items()})
        ctx.counts.rows_written += summary.written
        for key, value in summary.__dict__.items():
            totals[key] = totals.get(key, 0) + value
        if summary.processed_windows == 0:
            break
    return JobOutcome("succeeded", totals)


__all__ = ["confirm_halt", "derive_job", "gap_scan_job", "invalidate_minute", "repair_gaps",
           "scan_minute_gaps"]
