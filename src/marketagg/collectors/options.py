"""Bounded options chain collection (T10).

Each selected expiry is one provider request and one batch with its *own* fetch
window (cycle skew stays visible). Every contract returned for a selected expiry
is retained (retention policy v1), including unchanged prices: two polls are two
observations. Quality flags are applied before staging; partial responses keep
valid rows and record missing sides in coverage.
"""

from __future__ import annotations

import functools
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Engine, text

from marketagg.collectors.discovery import (
    ExpirySelection,
    latest_discovery,
    other_expiries,
    select_expiries,
)
from marketagg.collectors.quality import with_flags
from marketagg.domain import (
    ChainRequest,
    FetchResult,
    FetchStatus,
    InstrumentRef,
    OptionObservation,
)
from marketagg.scheduler.leases import record_gap
from marketagg.worker import JobContext, JobOutcome


def scope_selection(ctx: JobContext, tier: str, expirations: tuple[Any, ...],
                    session_date: Any) -> ExpirySelection:
    options = ctx.universe.options
    priority = select_expiries(expirations, session_date, targets=options.dte_targets,
                               dte_min=options.dte_min, dte_max=options.dte_max,
                               k=options.priority_expiries)
    if tier == "priority":
        return priority
    if tier == "other":
        return other_expiries(expirations, session_date, exclude=priority.selected,
                              limit=options.other_expiries_max)
    return other_expiries(expirations, session_date, exclude=(),
                          limit=options.broad_expiries_max)


def record_membership(engine: Engine, *, instrument_id: UUID, policy_id: str, tier: str,
                      selection: ExpirySelection, now: datetime) -> bool:
    """Append expiry-level membership decisions only when the selection changed."""
    with engine.begin() as conn:
        latest = conn.execute(text("""
            SELECT reason, selected FROM universe_memberships
            WHERE instrument_id=:i AND policy_id=:p AND contract_id IS NULL
              AND reason LIKE :prefix
              AND known_at = (SELECT max(known_at) FROM universe_memberships
                              WHERE instrument_id=:i AND policy_id=:p AND reason LIKE :prefix)
        """), {"i": instrument_id, "p": policy_id, "prefix": f"tier={tier} %"}).all()
        current = {(_reason(tier, e, r), True) for e, r in selection.reasons.items()} | {
            (_reason(tier, e, r), False) for e, r in selection.omitted.items()}
        if {(r.reason, r.selected) for r in latest} == current:
            return False
        for reason, selected in sorted(current):
            conn.execute(text("""
                INSERT INTO universe_memberships (id, instrument_id, contract_id, policy_id,
                    effective_from, known_at, selected, reason)
                VALUES (:id, :i, NULL, :p, :now, :now, :sel, :reason)
            """), {"id": uuid4(), "i": instrument_id, "p": policy_id, "now": now,
                   "sel": selected, "reason": reason})
    return True


def _reason(tier: str, expiry: Any, why: str) -> str:
    return f"tier={tier} expiry={expiry.isoformat()} {why}"


def collect_chain(ctx: JobContext) -> JobOutcome:
    symbol = str(ctx.job.scope["symbol"])
    tier = str(ctx.job.scope.get("tier", "priority"))
    now = ctx.clock.now()
    instrument_id: UUID = ctx.instruments[symbol]
    known = latest_discovery(ctx.engine, ctx.provider.provider_id, symbol, now)
    if known is None:
        with ctx.engine.begin() as conn:
            record_gap(conn, provider_id=ctx.provider.provider_id, instrument_id=instrument_id,
                       kind="missing_chain_snapshot", start_at=ctx.job.scheduled_for,
                       end_at=max(now, ctx.job.scheduled_for + timedelta(seconds=1)),
                       scope=ctx.job.scope, status="unrecoverable", now=now,
                       evidence={"reason": "no discovery known before the slot"})
        return JobOutcome("skipped", {"reason": "no_discovery"})
    session_date = ctx.calendar.local_date(now)
    selection = scope_selection(ctx, tier, known.expirations, session_date)
    record_membership(ctx.engine, instrument_id=instrument_id,
                      policy_id=ctx.policy_ids["collection"], tier=tier, selection=selection,
                      now=now)
    expiries = selection.selected
    if "chunk" in ctx.job.scope:  # broad sweeps run as interleaved chunks (planner)
        expiries = expiries[int(ctx.job.scope["chunk"])::int(ctx.job.scope["chunks"])]
        if not expiries:
            return JobOutcome("succeeded", {"tier": tier, "expiries": {},
                                            "reason": "chunk_empty"})
    options = ctx.universe.options
    per_expiry: dict[str, Mapping[str, Any]] = {}
    statuses: list[FetchStatus] = []
    for expiry in expiries:
        request = ChainRequest(
            provider_id=ctx.provider.provider_id, policy_id=ctx.job.policy_id,
            deadline_at=ctx.job.deadline_at or now + timedelta(minutes=10),
            correlation_id=str(ctx.job.run_id),
            instrument=InstrumentRef(provider_id=ctx.provider.provider_id, symbol=symbol),
            expiration_date=expiry)
        result: FetchResult[OptionObservation] = ctx.gate.call(
            "chain", functools.partial(ctx.provider.fetch_chain, request))
        partial = result.status is not FetchStatus.COMPLETE
        rows = [with_flags(r, partial_scope=partial,
                           old_trade_after=timedelta(minutes=options.old_last_trade_minutes),
                           iv_max=options.iv_plausible_max) for r in result.rows]
        outcome = ctx.repo.stage_chain(
            result, instrument_id=instrument_id, rows=rows,
            scope={"kind": f"chain_{tier}", "symbol": symbol, "tier": tier,
                   "expiration": expiry.isoformat(), "slot": ctx.job.scope.get("slot"),
                   "discovery_batch": known.batch_id,
                   "selection_reason": selection.reasons.get(expiry)},
            payload_uri=ctx.spool(result.provider_id, result.payload_body),
            fence=ctx.job.fence, run_id=ctx.job.run_id)
        if outcome.created:
            ctx.repo.publish(outcome.batch_id)
        statuses.append(result.status)
        ctx.counts.rows_received += len(result.rows)
        ctx.counts.rows_written += outcome.rows_written
        per_expiry[expiry.isoformat()] = {
            "status": result.status.value, "rows": outcome.rows_written,
            "quarantined": outcome.rows_quarantined,
            "fetch_completed_at": result.fetch_completed_at.isoformat(),
            "missing_sides": [s.value for s in result.coverage.missing_sides],
        }
    completions = [datetime.fromisoformat(str(v["fetch_completed_at"]))
                   for v in per_expiry.values()]
    skew = (max(completions) - min(completions)).total_seconds() if completions else 0.0
    status = "succeeded" if statuses and all(
        s is FetchStatus.COMPLETE for s in statuses) else "partial"
    return JobOutcome(status, {
        "tier": tier, "expiries": per_expiry, "cycle_skew_seconds": skew,
        "omitted": {e.isoformat(): r for e, r in selection.omitted.items()},
        "discovery_known_at": known.known_at.isoformat(),
    })
