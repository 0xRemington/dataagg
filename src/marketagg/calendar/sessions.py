"""Exchange sessions, left-labeled minutes and session-anchored buckets (T04).

Conventions (01 "Bar details"):

- Intervals are half-open ``[start, end)``; a minute is labeled by its start.
- Normal US RTH: 390 minutes, 09:30 through 15:59 starts.
- 4h buckets are session-open anchored: ``[open, open+4h)`` and the remainder
  ``[open+4h, close)``. An early close shorter than 4h gives one short bucket.
- DTE is calendar days ``(expiration_date - session_date).days``. Valuation
  cut-offs are NOT derived from the expiration date (00).

pandas/exchange_calendars types stay inside this module; callers see ``datetime``.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from functools import lru_cache
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import pandas as pd

from marketagg.calendar.registry import DEFAULT_POLICY, CalendarPolicy
from marketagg.domain.enums import Timeframe
from marketagg.domain.identifiers import ContractTermsObservation

ONE_MINUTE = timedelta(minutes=1)
FOUR_HOURS = timedelta(hours=4)
_FIXED_WIDTH = {Timeframe.M1: 1, Timeframe.M5: 5, Timeframe.M15: 15}


@dataclass(frozen=True)
class Session:
    session_date: date
    open_at: datetime
    close_at: datetime
    early_close: bool
    policy_id: str

    @property
    def minutes_count(self) -> int:
        return int((self.close_at - self.open_at) / ONE_MINUTE)

    def minutes(self) -> list[datetime]:
        return [self.open_at + i * ONE_MINUTE for i in range(self.minutes_count)]

    def contains(self, at: datetime) -> bool:
        return self.open_at <= at < self.close_at


@dataclass(frozen=True)
class Bucket:
    timeframe: Timeframe
    start: datetime
    end: datetime
    session_date: date
    expected_minutes: int
    is_short_session_bucket: bool
    label: str

    @property
    def duration(self) -> timedelta:
        return self.end - self.start


class SessionCalendar:
    def __init__(self, policy: CalendarPolicy = DEFAULT_POLICY) -> None:
        self.policy = policy
        self._calendar = _exchange_calendar(policy.calendar_id, policy.history_start)
        self._tz = ZoneInfo(str(self._calendar.tz))

    @property
    def policy_id(self) -> str:
        return self.policy.policy_id

    @property
    def timezone(self) -> ZoneInfo:
        return self._tz

    def local_date(self, at: datetime) -> date:
        return at.astimezone(self._tz).date()

    def session(self, day: date) -> Session | None:
        if day in self.policy.extra_closures:
            return None
        self._check_range(day)
        label = pd.Timestamp(day)
        if not self._calendar.is_session(label):
            return None
        open_at = _to_utc(self._calendar.session_open(label))
        close_at = _to_utc(self._calendar.session_close(label))
        early = bool(label in self._calendar.early_closes)
        if day in self.policy.extra_early_closes:
            override = datetime.combine(day, self.policy.extra_early_closes[day], self._tz)
            close_at, early = min(close_at, override.astimezone(UTC)), True
        return Session(day, open_at, close_at, early, self.policy_id)

    def sessions(self, first: date, last: date) -> list[Session]:
        """Sessions with ``first <= session_date <= last``."""
        out: list[Session] = []
        day = first
        while day <= last:
            found = self.session(day)
            if found is not None:
                out.append(found)
            day += timedelta(days=1)
        return out

    def sessions_overlapping(self, start: datetime, end: datetime) -> list[Session]:
        first = self.local_date(start) - timedelta(days=1)
        last = self.local_date(end)
        return [s for s in self.sessions(first, last) if s.open_at < end and s.close_at > start]

    def session_at(self, at: datetime) -> Session | None:
        found = self.session(self.local_date(at))
        return found if found is not None and found.contains(at) else None

    def is_open(self, at: datetime) -> bool:
        return self.session_at(at) is not None

    def next_session(self, after: date) -> Session:
        day = after + timedelta(days=1)
        for _ in range(30):
            found = self.session(day)
            if found is not None:
                return found
            day += timedelta(days=1)
        raise LookupError(f"no session within 30 days after {after}")

    def previous_session(self, before: date) -> Session:
        day = before - timedelta(days=1)
        for _ in range(30):
            found = self.session(day)
            if found is not None:
                return found
            day -= timedelta(days=1)
        raise LookupError(f"no session within 30 days before {before}")

    def expected_minutes(self, start: datetime, end: datetime) -> list[datetime]:
        """Left-labeled minute starts inside sessions and inside ``[start, end)``."""
        return [
            m
            for s in self.sessions_overlapping(start, end)
            for m in s.minutes()
            if start <= m < end
        ]

    def buckets(self, session: Session, timeframe: Timeframe) -> list[Bucket]:
        if timeframe is Timeframe.D1:
            return [Bucket(timeframe, session.open_at, session.close_at, session.session_date,
                           session.minutes_count, False, "daily session")]
        if timeframe is Timeframe.H4:
            return _four_hour_buckets(session)
        width = _FIXED_WIDTH[timeframe]
        out: list[Bucket] = []
        start = session.open_at
        while start < session.close_at:
            end = min(start + width * ONE_MINUTE, session.close_at)
            minutes = int((end - start) / ONE_MINUTE)
            out.append(Bucket(timeframe, start, end, session.session_date, minutes,
                              minutes < width, f"{timeframe.value}"))
            start = end
        return out

    def bucket_for(self, at: datetime, timeframe: Timeframe) -> Bucket | None:
        session = self.session_at(at)
        if session is None:
            return None
        return next(b for b in self.buckets(session, timeframe) if b.start <= at < b.end)

    def product_close(self, symbol: str, session: Session) -> datetime | None:
        """Product-specific close if documented in policy; ``None`` means unknown."""
        for override in self.policy.product_overrides:
            if override.symbol == symbol:
                local = datetime.combine(session.session_date, override.close_time, self._tz)
                return local.astimezone(UTC)
        return None

    def _check_range(self, day: date) -> None:
        first = self._calendar.first_session.date()
        last = self._calendar.last_session.date()
        if not first <= day <= last:
            raise ValueError(
                f"{day} outside calendar range {first}..{last}; refusing to guess sessions"
            )


def _four_hour_buckets(session: Session) -> list[Bucket]:
    first_end = session.open_at + FOUR_HOURS
    if first_end >= session.close_at:
        minutes = session.minutes_count
        label = (
            "4h session bucket"
            if minutes == 240
            else f"4h session bucket ({_hm(minutes)} early close)"
        )
        return [Bucket(Timeframe.H4, session.open_at, session.close_at, session.session_date,
                       minutes, minutes < 240, label)]
    remainder = int((session.close_at - first_end) / ONE_MINUTE)
    return [
        Bucket(Timeframe.H4, session.open_at, first_end, session.session_date, 240, False,
               "4h session bucket"),
        Bucket(Timeframe.H4, first_end, session.close_at, session.session_date, remainder,
               remainder < 240, f"4h session bucket ({_hm(remainder)} remainder)"),
    ]


def _hm(minutes: int) -> str:
    return f"{minutes // 60}h{minutes % 60:02d}"


def _to_utc(value: pd.Timestamp) -> datetime:
    result: datetime = value.tz_convert("UTC").to_pydatetime()
    return result.astimezone(UTC)


@lru_cache(maxsize=4)
def _exchange_calendar(calendar_id: str, start: date) -> xcals.ExchangeCalendar:
    # exchange_calendars 4.13 triggers a pandas-3 deprecation (Series.values on tz
    # data) inside its constructor; results are verified by tests/unit/test_calendar.py.
    # Re-check when upgrading either package.
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Series.values returning an ndarray",
                                category=Warning)
        return xcals.get_calendar(calendar_id, start=start.isoformat(), side="left")


def calendar_dte(expiration_date: date, session_date: date) -> int:
    """Calendar days to expiration (00); not business days, not a year fraction."""
    return (expiration_date - session_date).days


def verified_expiry_cutoff(terms: ContractTermsObservation | None) -> datetime | None:
    """Valuation cut-off only when terms state it; never guessed from the date."""
    return None if terms is None else terms.expiry_at


def at_local(day: date, clock: time, tz: ZoneInfo) -> datetime:
    return datetime.combine(day, clock, tz).astimezone(UTC)
