"""Binance kline mapping (UPDATE2): closed-only, UTC buckets, quote volume."""

from datetime import UTC, datetime, timedelta

from marketagg.crypto.binance import (
    BARS_POLICY,
    capabilities,
    rest_rows_to_bars,
    stream_kline_to_bar,
    stream_url,
)
from marketagg.domain import SessionKind, Timeframe

T0 = 1793750400000  # 2026-11-04T00:00:00Z


def _rest(start_ms: int, minutes: int) -> list[object]:
    end = start_ms + minutes * 60_000 - 1
    return [start_ms, "100.5", "101.0", "100.0", "100.75", "1.5", end, "150.6", 10, "0.7",
            "70.2", "0"]


def test_rest_drops_forming_kline_and_maps_fields() -> None:
    rows = [_rest(T0, 240), _rest(T0 + 240 * 60_000, 240)]
    now = datetime(2026, 11, 4, 5, 0, tzinfo=UTC)  # second kline (04:00-08:00) still forming
    bars = rest_rows_to_bars(Timeframe.H4, rows, now)
    assert len(bars) == 1
    bar = bars[0]
    assert bar.interval_start == datetime(2026, 11, 4, tzinfo=UTC)
    assert bar.interval_end - bar.interval_start == timedelta(hours=4)
    assert bar.session_date.isoformat() == "2026-11-04"
    assert bar.session_kind is SessionKind.RTH
    assert str(bar.close) == "100.75"
    assert bar.volume == 151  # quote volume, rounded


def test_one_minute_bars_span_one_minute() -> None:
    now = datetime(2026, 11, 4, 1, tzinfo=UTC)
    (bar,) = rest_rows_to_bars(Timeframe.M1, [_rest(T0, 1)], now)
    assert bar.interval_end - bar.interval_start == timedelta(minutes=1)


def test_stream_only_closed_klines() -> None:
    k = {"t": T0, "T": T0 + 59_999, "i": "1m", "o": "1", "h": "2", "l": "0.5", "c": "1.5",
         "v": "3", "q": "4.4", "x": False}
    assert stream_kline_to_bar({"stream": "btcusdt@kline_1m", "data": {"k": k}}) is None
    bar = stream_kline_to_bar({"stream": "btcusdt@kline_1m", "data": {"k": {**k, "x": True}}})
    assert bar is not None
    assert bar.timeframe is Timeframe.M1
    assert bar.volume == 4


def test_stream_and_rest_give_identical_bars() -> None:
    k = {"t": T0, "i": "5m", "o": "100.5", "h": "101.0", "l": "100.0", "c": "100.75",
         "q": "150.6", "x": True}
    from_ws = stream_kline_to_bar({"data": {"k": k}})
    (from_rest,) = rest_rows_to_bars(Timeframe.M5, [_rest(T0, 5)],
                                     datetime(2026, 11, 4, 1, tzinfo=UTC))
    assert from_ws == from_rest


def test_stream_url_covers_all_timeframes() -> None:
    url = stream_url()
    for tf in ("1m", "5m", "15m", "4h", "1d"):
        assert f"btcusdt@kline_{tf}" in url


def test_capabilities_and_policy_are_valid() -> None:
    caps = capabilities()
    assert caps.provider_id == "binance"
    assert all(caps.supports_timeframe(tf) for tf in Timeframe)
    assert BARS_POLICY.id.startswith("bars_ingest:")
