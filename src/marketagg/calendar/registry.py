"""Versioned session policies (T04).

A policy names the exchange calendar, the session kind, any exceptional
closures/early closes not yet in the pinned calendar package, and *metadata-only*
product overrides (e.g. an ETF option series that trades until 16:15). Product
overrides never change the canonical underlying RTH session; consumers ask for
them explicitly and treat absence as "unknown", not as 16:00.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, time
from typing import Any

from marketagg.domain.enums import SessionKind
from marketagg.domain.policies import PolicyVersion


@dataclass(frozen=True)
class ProductSessionOverride:
    symbol: str
    close_time: time
    evidence: str


@dataclass(frozen=True)
class CalendarPolicy:
    calendar_id: str = "XNYS"
    session_kind: SessionKind = SessionKind.RTH
    version: int = 1
    # Closures/early closes announced after the pinned calendar package release.
    extra_closures: frozenset[date] = frozenset()
    extra_early_closes: dict[date, time] = field(default_factory=dict)
    product_overrides: tuple[ProductSessionOverride, ...] = ()
    history_start: date = date(2015, 1, 1)

    @property
    def policy_id(self) -> str:
        kind = self.session_kind.value.lower()
        return f"calendar-{self.calendar_id.lower()}-{kind}/{self.version}"

    def as_config(self) -> dict[str, Any]:
        return {
            "calendar_id": self.calendar_id,
            "session_kind": self.session_kind.value,
            "version": self.version,
            "extra_closures": sorted(d.isoformat() for d in self.extra_closures),
            "extra_early_closes": {
                d.isoformat(): t.isoformat() for d, t in sorted(self.extra_early_closes.items())
            },
            "product_overrides": [
                {"symbol": o.symbol, "close_time": o.close_time.isoformat(), "evidence": o.evidence}
                for o in self.product_overrides
            ],
            "minute_labels": "left",
            "four_hour_convention": "session_open_anchored",
        }

    def policy_version(self, code_sha: str) -> PolicyVersion:
        return PolicyVersion.create("calendar", self.as_config(), code_sha)


DEFAULT_POLICY = CalendarPolicy()
