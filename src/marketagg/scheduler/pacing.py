"""Provider-wide pacing and circuit breakers, persisted in PostgreSQL.

Pacing state lives in ``provider_pacing`` and is updated under a row lock, so any
number of workers share one token clock (independent in-memory buckets are
forbidden, 03). Circuits (``provider_circuits``) record rate-limit cooldowns,
schema-drift opens and access-denied disablement; a disabled provider stays
disabled until an operator resolves configuration/permission.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from sqlalchemy import Engine, text

from marketagg.clock import SYSTEM_CLOCK, Clock
from marketagg.config import RetryPolicy
from marketagg.domain.errors import (
    ProviderError,
    RateLimited,
    SchemaChanged,
    Unauthorized,
    Unavailable,
)


class PacingGate:
    def __init__(self, engine: Engine, provider_id: str, requests_per_minute: int,
                 burst: int = 1, clock: Clock = SYSTEM_CLOCK) -> None:
        if requests_per_minute <= 0 or burst <= 0:
            raise ValueError("requests_per_minute and burst must be positive")
        self.engine = engine
        self.provider_id = provider_id
        self.capacity = Decimal(burst)
        self.rate = Decimal(requests_per_minute) / Decimal(60)
        self.clock = clock

    def try_acquire(self) -> float:
        """Consume one token. Returns 0.0 on success, else seconds until one is available."""
        now = self.clock.now()
        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO provider_pacing (provider_id, capacity, tokens,
                                                 refill_per_second, updated_at)
                    VALUES (:p, :cap, :cap, :rate, :now)
                    ON CONFLICT (provider_id) DO UPDATE
                      SET capacity = EXCLUDED.capacity,
                          refill_per_second = EXCLUDED.refill_per_second
                """),
                {"p": self.provider_id, "cap": self.capacity, "rate": self.rate, "now": now},
            )
            row = conn.execute(
                text("SELECT tokens, updated_at FROM provider_pacing "
                     "WHERE provider_id = :p FOR UPDATE"),
                {"p": self.provider_id},
            ).one()
            elapsed = max(Decimal((now - row.updated_at).total_seconds()), Decimal(0))
            tokens = min(self.capacity, row.tokens + elapsed * self.rate)
            if tokens >= 1:
                tokens -= 1
                wait = 0.0
            else:
                wait = float((Decimal(1) - tokens) / self.rate)
            conn.execute(
                text("UPDATE provider_pacing SET tokens = :t, updated_at = :now "
                     "WHERE provider_id = :p"),
                {"t": tokens, "now": max(now, row.updated_at), "p": self.provider_id},
            )
        return wait


@dataclass(frozen=True)
class CircuitState:
    state: str
    open_until: datetime | None
    consecutive_failures: int
    reason: str | None

    def allows(self, now: datetime) -> bool:
        if self.state == "disabled":
            return False
        if self.state == "open":
            return self.open_until is not None and now >= self.open_until  # probe
        return True


CLOSED = CircuitState("closed", None, 0, None)


class CircuitBreaker:
    def __init__(self, engine: Engine, policy: RetryPolicy, clock: Clock = SYSTEM_CLOCK):
        self.engine = engine
        self.policy = policy
        self.clock = clock

    def state(self, provider_id: str, endpoint: str) -> CircuitState:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT state, open_until, consecutive_failures, reason "
                     "FROM provider_circuits WHERE provider_id=:p AND endpoint IN (:e, '*') "
                     "ORDER BY (state = 'disabled') DESC, open_until DESC NULLS LAST LIMIT 1"),
                {"p": provider_id, "e": endpoint},
            ).one_or_none()
        if row is None:
            return CLOSED
        return CircuitState(row.state, row.open_until, row.consecutive_failures, row.reason)

    def allows(self, provider_id: str, endpoint: str) -> bool:
        return self.state(provider_id, endpoint).allows(self.clock.now())

    def record_success(self, provider_id: str, endpoint: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text("UPDATE provider_circuits SET state='closed', open_until=NULL, "
                     "consecutive_failures=0, reason=NULL, updated_at=:now "
                     "WHERE provider_id=:p AND endpoint=:e AND state='open'"),
                {"p": provider_id, "e": endpoint, "now": self.clock.now()},
            )

    def record_failure(self, provider_id: str, endpoint: str, error: ProviderError) -> None:
        now = self.clock.now()
        current = self.state(provider_id, endpoint)
        failures = current.consecutive_failures + 1
        state, until, scope = "closed", None, endpoint
        if isinstance(error, Unauthorized):
            state, scope = "disabled", "*"  # whole provider; never retried automatically
        elif isinstance(error, RateLimited):
            opens = failures if current.state == "open" else 1
            cooldown = min(self.policy.rate_limit_cooldown_seconds * 2 ** (opens - 1),
                           self.policy.rate_limit_cooldown_max_seconds)
            cooldown = max(cooldown, int(error.retry_after or 0))
            state, until, scope = "open", now + timedelta(seconds=cooldown), "*"
        elif isinstance(error, SchemaChanged):
            state = "open"
            until = now + timedelta(seconds=self.policy.schema_change_cooldown_seconds)
        elif isinstance(error, Unavailable) and (
                failures >= self.policy.unavailable_failures_to_open):
            state = "open"
            until = now + timedelta(seconds=self.policy.unavailable_cooldown_seconds)
        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO provider_circuits (provider_id, endpoint, state, opened_at,
                        open_until, consecutive_failures, reason, updated_at)
                    VALUES (:p, :e, :s, :opened, :until, :n, :reason, :now)
                    ON CONFLICT (provider_id, endpoint) DO UPDATE SET
                        state = CASE WHEN provider_circuits.state = 'disabled'
                                     THEN 'disabled' ELSE EXCLUDED.state END,
                        opened_at = COALESCE(EXCLUDED.opened_at, provider_circuits.opened_at),
                        open_until = EXCLUDED.open_until,
                        consecutive_failures = EXCLUDED.consecutive_failures,
                        reason = EXCLUDED.reason,
                        updated_at = EXCLUDED.updated_at
                """),
                {"p": provider_id, "e": scope, "s": state,
                 "opened": now if state != "closed" else None, "until": until, "n": failures,
                 "reason": f"{error.code.value}: {error.message}"[:500], "now": now},
            )

    def enable(self, provider_id: str) -> None:
        """Operator action after resolving the access/permission problem."""
        with self.engine.begin() as conn:
            conn.execute(
                text("UPDATE provider_circuits SET state='closed', open_until=NULL, "
                     "consecutive_failures=0, reason='operator re-enabled', updated_at=:now "
                     "WHERE provider_id=:p"),
                {"p": provider_id, "now": self.clock.now()},
            )
