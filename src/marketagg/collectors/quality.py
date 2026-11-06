"""Option quality flags (03 "Quality and gaps"). Flags describe; they never repair.

Anomalous values (negative/crossed/zero) remain stored as observed. A stale
last-trade flag applies to the trade field only, never to bid/ask freshness.
"""

from __future__ import annotations

from datetime import timedelta

from marketagg.domain import OptionObservation, QualityFlag


def option_flags(obs: OptionObservation, *, partial_scope: bool, old_trade_after: timedelta,
                 iv_max: float) -> tuple[QualityFlag, ...]:
    flags = set(obs.quality_flags)
    bid, ask, last = obs.bid, obs.ask, obs.last
    if bid is None:
        flags.add(QualityFlag.MISSING_BID)
    if ask is None:
        flags.add(QualityFlag.MISSING_ASK)
    if bid is not None and bid == 0:
        flags.add(QualityFlag.ZERO_BID)
    if any(v is not None and v < 0 for v in (bid, ask, last)):
        flags.add(QualityFlag.NEGATIVE_PRICE)
    if bid is not None and ask is not None:
        if bid > ask:
            flags.add(QualityFlag.CROSSED)
        elif bid == ask and ask > 0:
            flags.add(QualityFlag.LOCKED)
    if obs.provider_event_at is None:
        flags.add(QualityFlag.UNKNOWN_QUOTE_TIME)
    if obs.last_trade_at is not None and obs.observed_at - obs.last_trade_at > old_trade_after:
        flags.add(QualityFlag.OLD_LAST_TRADE)
    if obs.reported_iv is not None and not 0 < obs.reported_iv <= iv_max:
        flags.add(QualityFlag.INVALID_IV)
    if obs.reported_open_interest is not None and obs.open_interest_asof_date is None:
        flags.add(QualityFlag.UNKNOWN_OI_DATE)
    if partial_scope:
        flags.add(QualityFlag.PARTIAL_SCOPE)
    return tuple(sorted(flags))


def with_flags(obs: OptionObservation, **kwargs: object) -> OptionObservation:
    return obs.model_copy(update={"quality_flags": option_flags(obs, **kwargs)})  # type: ignore[arg-type]


def midpoint_eligible(obs: OptionObservation) -> bool:
    """Descriptive midpoint: finite 0 <= bid <= ask and ask > 0 (03)."""
    return (obs.bid is not None and obs.ask is not None and obs.ask > 0
            and 0 <= obs.bid <= obs.ask)
