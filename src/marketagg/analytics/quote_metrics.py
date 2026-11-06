"""Descriptive quote metrics (T12). No Greeks, no fair value, no model IV.

- Midpoint requires finite ``0 <= bid <= ask`` and ``ask > 0``; crossed/locked/zero
  cases are excluded or flagged, never repaired (03).
- Relative spread divides by the midpoint only when it is positive.
- IV here is *provider-reported* (origin ``provider_reported``); its calculation
  assumptions are unknown, so values are descriptive only.
- ATM v1: strike closest to a compatible spot, ties to the lower strike; not 50-delta.
- Near-ATM: ``|ln(K/S)| <= 0.10``. Put-call skew compares the put nearest
  0.95·S with the call nearest 1.05·S within one cycle (<= max skew seconds).
- Without provider event times every output is labelled temporally uncertain.
- Cumulative volume and reported OI are reported as latest values, never summed.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from marketagg.analytics.asof import OptionRecord

METRICS_VERSION = "quote-metrics/1"
NEAR_ATM_LOG_MONEYNESS = 0.10
SKEW_PUT_MONEYNESS = 0.95
SKEW_CALL_MONEYNESS = 1.05


@dataclass(frozen=True)
class Spot:
    price: Decimal
    price_basis: str
    session_date: date
    known_at: datetime


@dataclass(frozen=True)
class QuoteContext:
    option_type: str
    strike: Decimal
    expiration_date: date


@dataclass(frozen=True)
class MetricResult:
    values: dict[str, Any]
    diagnostics: dict[str, Any] = field(default_factory=dict)


def quote_metrics(obs: OptionRecord) -> MetricResult:
    bid, ask = obs.bid, obs.ask
    diagnostics: dict[str, Any] = {"version": METRICS_VERSION,
                                   "temporal": _temporal(obs), "flags": list(obs.quality_flags)}
    midpoint: Decimal | None = None
    if bid is None or ask is None:
        diagnostics["midpoint_unavailable"] = "missing_side"
    elif bid > ask:
        diagnostics["midpoint_unavailable"] = "crossed"
    elif ask <= 0:
        diagnostics["midpoint_unavailable"] = "non_positive_ask"
    elif bid < 0:
        diagnostics["midpoint_unavailable"] = "negative_bid"
    else:
        midpoint = (bid + ask) / 2
    spread = None if bid is None or ask is None else ask - bid
    relative = None
    if spread is not None and midpoint is not None and midpoint > 0:
        relative = float(spread / midpoint)
    iv = obs.reported_iv
    if iv is not None and (not math.isfinite(iv) or iv <= 0 or "invalid_iv" in obs.quality_flags):
        diagnostics["reported_iv_excluded"] = "invalid_iv"
        iv = None
    return MetricResult({
        "midpoint": midpoint,
        "spread_abs": spread,
        "spread_rel": relative,
        "reported_iv": iv,
        "reported_iv_origin": "provider_reported" if iv is not None else None,
        "units": {"midpoint": "currency", "spread_rel": "fraction_of_mid",
                  "reported_iv": "decimal_annualized"},
    }, diagnostics)


def _temporal(obs: OptionRecord) -> str:
    return "event_time_known" if obs.provider_event_at is not None else "temporally_uncertain"


def compatible_spot(spot: Spot | None, *, option_session: date,
                    required_basis: str | None) -> tuple[Spot | None, str | None]:
    if spot is None:
        return None, "spot_unavailable"
    if spot.session_date != option_session:
        return None, "spot_other_session"
    if required_basis is not None and spot.price_basis != required_basis:
        return None, f"spot_basis_{spot.price_basis}_incompatible"
    return spot, None


def atm_strike(strikes: Sequence[Decimal], spot: Decimal) -> Decimal | None:
    if not strikes:
        return None
    return min(sorted(set(strikes)), key=lambda k: (abs(k - spot), k))


def term_structure(
    cycle: Sequence[tuple[QuoteContext, OptionRecord]],
    *,
    spot: Spot | None,
    session_date: date,
    requested_expiries: Sequence[date],
    required_basis: str | None = None,
    max_skew_seconds: float = 120.0,
) -> MetricResult:
    """ATM reported IV per expiry; absent expiries are listed, not interpolated."""
    usable, reason = compatible_spot(spot, option_session=session_date,
                                     required_basis=required_basis)
    points: list[dict[str, Any]] = []
    for expiry in sorted(set(requested_expiries)):
        rows = [(c, o) for c, o in cycle if c.expiration_date == expiry]
        if usable is None:
            points.append({"expiry": expiry, "status": "unavailable", "reason": reason})
            continue
        if not rows:
            points.append({"expiry": expiry, "status": "unavailable", "reason": "no_observation"})
            continue
        strike = atm_strike([c.strike for c, _ in rows], usable.price)
        ivs = {c.option_type: quote_metrics(o).values["reported_iv"]
               for c, o in rows if c.strike == strike}
        times = [o.fetch_completed_at for _, o in rows]
        points.append({
            "expiry": expiry, "dte": (expiry - session_date).days, "atm_strike": strike,
            "call_reported_iv": ivs.get("C"), "put_reported_iv": ivs.get("P"),
            "status": "ok" if any(v is not None for v in ivs.values()) else "unavailable",
            "observation_skew_seconds": (max(times) - min(times)).total_seconds(),
        })
    times_all = [o.fetch_completed_at for _, o in cycle]
    cycle_skew = (max(times_all) - min(times_all)).total_seconds() if times_all else 0.0
    return MetricResult({"points": points, "method": "atm_closest_strike_v1"}, {
        "version": METRICS_VERSION, "cycle_skew_seconds": cycle_skew,
        "cycle_skew_exceeded": cycle_skew > max_skew_seconds,
        "temporal": ("temporally_uncertain" if any(o.provider_event_at is None
                                                   for _, o in cycle) else "event_time_known"),
        "iv_origin": "provider_reported", "interpolation": "none",
    })


def moneyness_skew(
    rows: Sequence[tuple[QuoteContext, OptionRecord]],
    *,
    spot: Spot | None,
    session_date: date,
    required_basis: str | None = None,
    max_skew_seconds: float = 120.0,
) -> MetricResult:
    usable, reason = compatible_spot(spot, option_session=session_date,
                                     required_basis=required_basis)
    if usable is None:
        return MetricResult({"points": [], "put_call_skew": None},
                            {"unavailable": reason, "version": METRICS_VERSION})
    s = float(usable.price)
    points = []
    excluded: dict[str, int] = {}
    for ctx, obs in rows:
        iv = quote_metrics(obs).values["reported_iv"]
        lm = math.log(float(ctx.strike) / s)
        if abs(lm) > NEAR_ATM_LOG_MONEYNESS:
            excluded["outside_near_atm"] = excluded.get("outside_near_atm", 0) + 1
            continue
        if iv is None:
            excluded["no_valid_iv"] = excluded.get("no_valid_iv", 0) + 1
            continue
        points.append({"strike": ctx.strike, "type": ctx.option_type,
                       "log_moneyness": round(lm, 6), "reported_iv": iv})
    times = [o.fetch_completed_at for _, o in rows]
    skew_seconds = (max(times) - min(times)).total_seconds() if times else 0.0
    put_call = None
    puts = [p for p in points if p["type"] == "P"]
    calls = [p for p in points if p["type"] == "C"]
    if puts and calls and skew_seconds <= max_skew_seconds:
        put = min(puts, key=lambda p: (abs(float(p["strike"]) / s - SKEW_PUT_MONEYNESS),
                                       p["strike"]))
        call = min(calls, key=lambda p: (abs(float(p["strike"]) / s - SKEW_CALL_MONEYNESS),
                                         p["strike"]))
        put_call = round((put["reported_iv"] - call["reported_iv"]) * 100, 6)
    return MetricResult(
        {"points": sorted(points, key=lambda p: (p["log_moneyness"], p["type"])),
         "put_call_skew": put_call,
         "put_call_skew_units": "volatility_points (IV(0.95S put) - IV(1.05S call))"},
        {"version": METRICS_VERSION, "excluded": excluded,
         "observation_skew_seconds": skew_seconds,
         "skew_bound_exceeded": skew_seconds > max_skew_seconds,
         "near_atm_definition": f"|ln(K/S)| <= {NEAR_ATM_LOG_MONEYNESS}",
         "iv_origin": "provider_reported", "greeks": "not computed (T20)"})


def latest_activity(history: Sequence[OptionRecord]) -> dict[str, Any]:
    """Latest reported volume/OI; repeated cumulative values are never summed."""
    if not history:
        return {"reported_volume": None, "reported_open_interest": None, "as_of": None}
    last = max(history, key=lambda o: o.observed_at)
    return {"reported_volume": last.reported_volume,
            "reported_open_interest": last.reported_open_interest,
            "as_of": last.observed_at, "volume_semantics": last.field_semantics.get(
                "volume_kind", "unknown")}
