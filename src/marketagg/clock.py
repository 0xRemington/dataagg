"""Injectable clocks. Production uses ``SystemClock``; tests use ``FakeClock``."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    def now(self) -> datetime: ...


class SystemClock:
    """Wall-clock UTC. ``is_system`` enables DB clock-skew checks at publication."""

    is_system = True

    def now(self) -> datetime:
        return datetime.now(UTC)


SYSTEM_CLOCK = SystemClock()


def is_system_clock(clock: Clock) -> bool:
    return bool(getattr(clock, "is_system", False))
