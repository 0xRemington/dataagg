"""Expiry discovery and bounded scope selection (T10, 03 "honest strike tiers").

Priority scope: distinct expiries nearest each calendar-DTE target, only inside
``[dte_min, dte_max]``, ties to the earlier expiration, at most ``k``. Every
omitted expiry is reported with a reason so research knows what was sampled.
Discovery used for planning must have been *known* before the slot (no hindsight).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from sqlalchemy import Engine, text

from marketagg.calendar import calendar_dte
from marketagg.domain import DiscoveryRequest, InstrumentRef
from marketagg.worker import JobContext, JobOutcome


@dataclass(frozen=True)
class ExpirySelection:
    selected: tuple[date, ...]
    reasons: dict[date, str] = field(default_factory=dict)
    omitted: dict[date, str] = field(default_factory=dict)


def select_expiries(expirations: Sequence[date], session_date: date, *,
                    targets: Sequence[int], dte_min: int, dte_max: int,
                    k: int) -> ExpirySelection:
    omitted: dict[date, str] = {}
    in_band: list[date] = []
    for expiry in sorted(set(expirations)):
        dte = calendar_dte(expiry, session_date)
        if dte < 0:
            omitted[expiry] = "expired"
        elif not dte_min <= dte <= dte_max:
            omitted[expiry] = f"outside_dte_band({dte})"
        else:
            in_band.append(expiry)
    selected: list[date] = []
    reasons: dict[date, str] = {}
    for target in targets:
        if len(selected) >= k:
            break
        candidates = [e for e in in_band if e not in selected]
        if not candidates:
            break
        best = min(candidates, key=lambda e: (abs(calendar_dte(e, session_date) - target), e))
        selected.append(best)
        reasons[best] = f"nearest_to_{target}d"
    for expiry in in_band:
        if expiry not in selected:
            omitted[expiry] = "priority_scope_ceiling"
    return ExpirySelection(tuple(sorted(selected)), reasons, omitted)


def other_expiries(expirations: Sequence[date], session_date: date, *,
                   exclude: Sequence[date], limit: int) -> ExpirySelection:
    live = [e for e in sorted(set(expirations)) if e >= session_date and e not in exclude]
    chosen = sorted(live, key=lambda e: (calendar_dte(e, session_date), e))[:limit]
    omitted = {e: "other_scope_ceiling" for e in live if e not in chosen}
    return ExpirySelection(tuple(sorted(chosen)), {e: "other_tier" for e in chosen}, omitted)


@dataclass(frozen=True)
class KnownDiscovery:
    batch_id: str
    expirations: tuple[date, ...]
    known_at: datetime
    fetched_at: datetime


def latest_discovery(engine: Engine, provider_id: str, symbol: str,
                     decision_at: datetime) -> KnownDiscovery | None:
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT id, scope, known_at, fetch_completed_at FROM observation_batches
            WHERE provider_id = :p AND batch_kind = 'reference' AND status = 'published'
              AND scope->>'kind' = 'discovery' AND scope->>'symbol' = :s
              AND known_at <= :d
            ORDER BY known_at DESC LIMIT 1
        """), {"p": provider_id, "s": symbol, "d": decision_at}).one_or_none()
    if row is None:
        return None
    return KnownDiscovery(str(row.id), tuple(date.fromisoformat(d)
                                             for d in row.scope["expirations"]),
                          row.known_at, row.fetch_completed_at)


def discovery_job(ctx: JobContext) -> JobOutcome:
    symbol = str(ctx.job.scope["symbol"])
    request = DiscoveryRequest(
        provider_id=ctx.provider.provider_id, policy_id=ctx.job.policy_id,
        deadline_at=ctx.job.deadline_at or ctx.clock.now() + timedelta(minutes=10),
        correlation_id=str(ctx.job.run_id),
        instrument=InstrumentRef(provider_id=ctx.provider.provider_id, symbol=symbol))
    result = ctx.gate.call("discovery", lambda: ctx.provider.discover(request))
    outcome = ctx.repo.stage_reference(result, scope={"job": str(ctx.job.id)},
                                       payload_uri=ctx.spool(result.provider_id,
                                                             result.payload_body),
                                       fence=ctx.job.fence, run_id=ctx.job.run_id)
    if outcome.created:
        ctx.repo.publish(outcome.batch_id)
    return JobOutcome("succeeded" if result.expirations else "partial",
                      {"expirations": len(result.expirations)})
