from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

from marketagg.analytics.aggregate import aggregate_bucket, reconcile_daily
from marketagg.analytics.asof import BarRecord
from marketagg.calendar import SessionCalendar
from marketagg.domain import InstrumentRef, PriceBasis, Timeframe

CAL = SessionCalendar()
REF = InstrumentRef(provider_id="fake", symbol="SYNA")


def minute(start: datetime, i: int, volume: int | None = None) -> BarRecord:
    base = Decimal(100) + Decimal(i) / 100
    return BarRecord(
        id=uuid4(), batch_id=uuid4(), interval_start=start, interval_end=start + timedelta(
            minutes=1), session_date=start.date(), open=base, high=base + Decimal("0.5"),
        low=base - Decimal("0.5") - (Decimal(1) if i == 7 else 0), close=base + Decimal("0.1"),
        volume=volume if volume is not None else 100 + i, complete=True, expected_minutes=1,
        observed_minutes=1, is_short_session_bucket=False, quality_flags=(),
        known_at=start + timedelta(minutes=2), content_sha256=f"{i:064x}", timeframe="1m",
        price_basis="provider_native", policy_id="p", provider_id="fake",
    )


def session_minutes(day: date, skip: set[int] | None = None) -> list[BarRecord]:
    session = CAL.session(day)
    assert session is not None
    return [minute(m, i) for i, m in enumerate(session.minutes()) if i not in (skip or set())]


def _aggregate(day: date, timeframe: Timeframe, minutes: list[BarRecord]) -> list:
    session = CAL.session(day)
    assert session is not None
    return [aggregate_bucket(b, minutes, REF, PriceBasis.PROVIDER_NATIVE)
            for b in CAL.buckets(session, timeframe)]


@pytest.mark.parametrize(("day", "counts"), [
    (date(2025, 3, 7), {Timeframe.M5: 78, Timeframe.M15: 26, Timeframe.H4: 2}),
    (date(2025, 11, 28), {Timeframe.M5: 42, Timeframe.M15: 14, Timeframe.H4: 1}),
])
def test_bucket_counts_and_ohlcv(day: date, counts: dict[Timeframe, int]) -> None:
    minutes = session_minutes(day)
    for timeframe, expected in counts.items():
        writes = _aggregate(day, timeframe, minutes)
        assert len(writes) == expected and all(w is not None and w.complete for w in writes)
    first_five = minutes[:5]
    bar = _aggregate(day, Timeframe.M5, minutes)[0].observation
    assert bar.open == first_five[0].open
    assert bar.high == max(m.high for m in first_five)
    assert bar.low == min(m.low for m in first_five)
    assert bar.close == first_five[-1].close
    assert bar.volume == sum(m.volume or 0 for m in first_five)


def test_session_4h_durations_and_flags() -> None:
    normal = _aggregate(date(2025, 3, 7), Timeframe.H4, session_minutes(date(2025, 3, 7)))
    assert [(w.expected_minutes, w.is_short_session_bucket) for w in normal] == [
        (240, False), (150, True)]
    early = _aggregate(date(2025, 11, 28), Timeframe.H4, session_minutes(date(2025, 11, 28)))
    assert [(w.expected_minutes, w.observed_minutes) for w in early] == [(210, 210)]


def test_missing_minute_makes_containing_buckets_incomplete() -> None:
    minutes = session_minutes(date(2025, 3, 7), skip={7})
    m5 = _aggregate(date(2025, 3, 7), Timeframe.M5, minutes)
    assert [w.complete for w in m5[:3]] == [True, False, True]
    assert m5[1].observed_minutes == 4 and m5[1].expected_minutes == 5
    h4 = _aggregate(date(2025, 3, 7), Timeframe.H4, minutes)
    assert [w.complete for w in h4] == [False, True]


def test_no_constituents_publishes_nothing_and_null_volume_propagates() -> None:
    minutes = session_minutes(date(2025, 3, 7))
    assert _aggregate(date(2025, 3, 7), Timeframe.M5, minutes[:5])[1] is None
    minutes[2] = minute(minutes[2].interval_start, 2).__class__(
        **{**minutes[2].__dict__, "volume": None})
    assert _aggregate(date(2025, 3, 7), Timeframe.M5, minutes)[0].observation.volume is None


def test_lineage_references_every_input() -> None:
    minutes = session_minutes(date(2025, 3, 7))
    write = _aggregate(date(2025, 3, 7), Timeframe.M15, minutes)[0]
    assert [r["id"] for r in write.input_refs] == [str(m.id) for m in minutes[:15]]


def test_duplicate_minutes_are_rejected() -> None:
    minutes = session_minutes(date(2025, 3, 7))
    with pytest.raises(ValueError, match="duplicate"):
        _aggregate(date(2025, 3, 7), Timeframe.M5, [*minutes, minutes[0]])


def test_daily_reconciliation_reports_without_overwriting() -> None:
    minutes = session_minutes(date(2025, 3, 7))
    session = CAL.session(date(2025, 3, 7))
    assert session is not None
    daily = minutes[0].__class__(**{
        **minutes[0].__dict__, "interval_start": session.open_at,
        "interval_end": session.close_at, "timeframe": "1d", "close": minutes[-1].close,
        "high": max(m.high for m in minutes), "low": min(m.low for m in minutes),
        "volume": sum(m.volume or 0 for m in minutes) * 2})
    report = reconcile_daily(daily, minutes, price_abs=Decimal("0.01"), price_rel=1e-5,
                             volume_rel=0.01)
    assert report["comparable"] and report["price_differences"] == {}
    assert report["volume_warning"] is True
    other_basis = minutes[0].__class__(**{**daily.__dict__, "price_basis": "split_adjusted"})
    assert reconcile_daily(other_basis, minutes, price_abs=Decimal("0.01"), price_rel=1e-5,
                           volume_rel=0.01)["comparable"] is False


def test_utc_inputs() -> None:
    assert session_minutes(date(2025, 3, 7))[0].interval_start == datetime(
        2025, 3, 7, 14, 30, tzinfo=UTC)
