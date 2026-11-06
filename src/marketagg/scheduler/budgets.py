"""Request budget planning (03 "Request accounting").

``chain_endpoint_calls = sum_s(E_p(s)*P + E_o(s)*H)`` plus broad extras,
discovery, bars, daily and repair. Each expiry response contains both sides, so
sides are never double-counted. The project budget (default 10 requests/minute,
70% planned utilisation) is self-imposed pacing, not a provider allowance; hidden
transport calls make every estimate a lower bound.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta


@dataclass(frozen=True)
class BudgetInputs:
    symbols: int
    priority_expiries: int
    other_expiries: int
    priority_polls: int = 78
    hourly_polls: int = 7
    minute_polls: int = 390
    discovery_calls_per_symbol: int = 1
    daily_calls_per_symbol: int = 2
    repair_calls: int = 16
    broad_extra_calls: int = 12
    session_minutes: int = 390
    requests_per_minute: int = 10
    planned_utilization: float = 0.70


@dataclass(frozen=True)
class BudgetReport:
    inputs: BudgetInputs
    chain_calls: int
    bar_calls: int
    other_calls: int
    total_calls: int
    mean_per_minute: float
    planned_capacity: int
    utilization: float
    boundary_tokens: int
    boundary_spread_seconds: float
    feasible: bool
    reasons: tuple[str, ...] = field(default_factory=tuple)
    note: str = ("Estimate excludes hidden transport/cookie/retry overhead; it is a lower "
                 "bound, not a measured or provider-approved rate.")

    def as_dict(self) -> dict[str, object]:
        return {
            "inputs": self.inputs.__dict__,
            "chain_calls": self.chain_calls,
            "bar_calls": self.bar_calls,
            "other_calls": self.other_calls,
            "total_calls": self.total_calls,
            "mean_per_minute": round(self.mean_per_minute, 3),
            "planned_capacity": self.planned_capacity,
            "utilization": round(self.utilization, 4),
            "boundary_tokens": self.boundary_tokens,
            "boundary_spread_seconds": self.boundary_spread_seconds,
            "feasible": self.feasible,
            "reasons": list(self.reasons),
            "note": self.note,
        }


def plan_budget(inputs: BudgetInputs) -> BudgetReport:
    s = inputs.symbols
    chain = s * (inputs.priority_expiries * inputs.priority_polls
                 + inputs.other_expiries * inputs.hourly_polls)
    bars = s * inputs.minute_polls
    other = (s * inputs.discovery_calls_per_symbol + s * inputs.daily_calls_per_symbol
             + inputs.repair_calls + inputs.broad_extra_calls)
    total = chain + bars + other
    capacity = math.floor(inputs.requests_per_minute * inputs.session_minutes
                          * inputs.planned_utilization)
    utilization = total / (inputs.requests_per_minute * inputs.session_minutes)
    # At a five-minute boundary every priority expiry plus one bar fetch per symbol fall due.
    boundary = s * inputs.priority_expiries + s
    spread = 60.0 * boundary / inputs.requests_per_minute
    reasons: list[str] = []
    if total > capacity:
        reasons.append(f"{total} calls/session exceed planned capacity {capacity}")
    if spread > 120:
        reasons.append(
            f"boundary burst of {boundary} requests needs {spread:.0f}s at the cap, "
            "beyond the 120s priority-chain deadline")
    return BudgetReport(inputs, chain, bars, other, total, total / inputs.session_minutes,
                        capacity, utilization, boundary, spread, not reasons, tuple(reasons))


def full_jitter_delay(attempt: int, rng: random.Random, base: float = 2.0,
                      cap: float = 60.0) -> float:
    """Uniform ``[0, min(cap, base * 2**attempt)]`` (03 retry policy)."""
    return rng.uniform(0.0, min(cap, base * 2**attempt))


def next_retry_at(
    *,
    attempt: int,
    max_attempts: int,
    now: datetime,
    deadline: datetime | None,
    rng: random.Random,
    retry_after: float | None = None,
    base: float = 2.0,
    cap: float = 60.0,
) -> datetime | None:
    """When to retry, or ``None`` when attempts or the deadline are exhausted."""
    if attempt >= max_attempts:
        return None
    delay = full_jitter_delay(attempt, rng, base, cap)
    if retry_after is not None:
        delay = max(delay, retry_after)
    when = now + timedelta(seconds=delay)
    if deadline is not None and when >= deadline:
        return None
    return when
