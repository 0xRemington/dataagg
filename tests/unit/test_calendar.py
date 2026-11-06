from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta

import pytest

from marketagg.calendar import (
    CalendarPolicy,
    ProductSessionOverride,
    SessionCalendar,
    calendar_dte,
    verified_expiry_cutoff,
)
from marketagg.domain.enums import Timeframe
from marketagg.domain.identifiers import ContractTermsObservation


@pytest.fixture(scope="module")
def cal() -> SessionCalendar:
    return SessionCalendar()


def utc(*args: int) -> datetime:
    return datetime(*args, tzinfo=UTC)


def test_normal_session_has_390_left_labeled_minutes(cal: SessionCalendar) -> None:
    s = cal.session(date(2025, 3, 7))
    assert s is not None and not s.early_close
    assert (s.open_at, s.close_at) == (utc(2025, 3, 7, 14, 30), utc(2025, 3, 7, 21, 0))
    minutes = s.minutes()
    assert len(minutes) == 390
    assert minutes[0] == s.open_at and minutes[-1] == utc(2025, 3, 7, 20, 59)  # 15:59 ET


def test_dst_transition_moves_utc_open(cal: SessionCalendar) -> None:
    s = cal.session(date(2025, 3, 10))  # first session after 2025-03-09 DST change
    assert s is not None and s.open_at == utc(2025, 3, 10, 13, 30)
    assert s.minutes_count == 390


def test_early_close_is_210_minutes_with_one_short_4h_bucket(cal: SessionCalendar) -> None:
    s = cal.session(date(2025, 11, 28))
    assert s is not None and s.early_close and s.minutes_count == 210
    buckets = cal.buckets(s, Timeframe.H4)
    assert [(b.expected_minutes, b.is_short_session_bucket) for b in buckets] == [(210, True)]
    assert buckets[0].label == "4h session bucket (3h30 early close)"
    assert len(cal.buckets(s, Timeframe.M5)) == 42
    assert len(cal.buckets(s, Timeframe.M15)) == 14


def test_normal_4h_buckets_are_240_and_150(cal: SessionCalendar) -> None:
    s = cal.session(date(2025, 3, 7))
    assert s is not None
    buckets = cal.buckets(s, Timeframe.H4)
    assert [b.expected_minutes for b in buckets] == [240, 150]
    assert buckets[1].start == utc(2025, 3, 7, 18, 30)  # 13:30 ET
    assert buckets[1].label == "4h session bucket (2h30 remainder)"
    assert [b.is_short_session_bucket for b in buckets] == [False, True]
    assert len(cal.buckets(s, Timeframe.M5)) == 78
    assert len(cal.buckets(s, Timeframe.M15)) == 26
    assert all(b.expected_minutes == 5 for b in cal.buckets(s, Timeframe.M5))


@pytest.mark.parametrize("day", [date(2025, 7, 4), date(2025, 12, 25), date(2025, 3, 8)])
def test_holidays_and_weekends_have_no_session(cal: SessionCalendar, day: date) -> None:
    assert cal.session(day) is None


def test_no_phantom_overnight_or_weekend_minutes(cal: SessionCalendar) -> None:
    friday_close = utc(2025, 3, 7, 21, 0)
    monday_open = utc(2025, 3, 10, 13, 30)
    assert cal.expected_minutes(friday_close, monday_open) == []
    week = cal.expected_minutes(utc(2025, 3, 3), utc(2025, 3, 8))
    assert len(week) == 5 * 390
    assert not cal.is_open(friday_close)  # half-open: close instant is outside
    assert cal.is_open(friday_close - timedelta(minutes=1))


def test_synthetic_exceptional_closure_and_early_close() -> None:
    policy = CalendarPolicy(
        extra_closures=frozenset({date(2025, 3, 5)}),
        extra_early_closes={date(2025, 3, 6): time(13, 0)},
        version=2,
    )
    cal = SessionCalendar(policy)
    assert cal.session(date(2025, 3, 5)) is None
    s = cal.session(date(2025, 3, 6))
    assert s is not None and s.early_close and s.minutes_count == 210
    assert cal.policy_id == "calendar-xnys-rth/2"
    assert policy.policy_version("test").id != CalendarPolicy().policy_version("test").id


def test_calendar_refuses_dates_outside_its_range(cal: SessionCalendar) -> None:
    with pytest.raises(ValueError, match="refusing to guess"):
        cal.session(date(1990, 1, 2))


def test_dte_is_calendar_days_and_cutoff_is_never_guessed() -> None:
    assert calendar_dte(date(2025, 4, 11), date(2025, 3, 7)) == 35
    assert verified_expiry_cutoff(None) is None
    assert verified_expiry_cutoff(ContractTermsObservation()) is None


def test_product_override_is_metadata_only() -> None:
    policy = CalendarPolicy(product_overrides=(
        ProductSessionOverride("SYNA", time(16, 15), "synthetic fixture"),))
    cal = SessionCalendar(policy)
    s = cal.session(date(2025, 3, 7))
    assert s is not None and s.close_at == utc(2025, 3, 7, 21, 0)  # underlying RTH unchanged
    assert cal.product_close("SYNA", s) == utc(2025, 3, 7, 21, 15)
    assert cal.product_close("SYNB", s) is None  # unknown, not 16:00


def test_bucket_lookup_and_session_navigation(cal: SessionCalendar) -> None:
    bucket = cal.bucket_for(utc(2025, 3, 7, 14, 47), Timeframe.M15)
    assert bucket is not None and bucket.start == utc(2025, 3, 7, 14, 45)
    assert cal.bucket_for(utc(2025, 3, 8, 15, 0), Timeframe.M5) is None
    assert cal.next_session(date(2025, 3, 7)).session_date == date(2025, 3, 10)
    assert cal.previous_session(date(2025, 7, 7)).session_date == date(2025, 7, 3)
