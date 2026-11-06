"""Offline Yahoo adapter tests with synthetic frames shaped like yfinance 1.7.0 output.

No fixture here is real Yahoo data. The single ``live`` test is skipped unless
``--run-live`` is given *and* settings pass the source-review gate.
"""

from __future__ import annotations

import math
import os
from collections import namedtuple
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import numpy as np
import pandas as pd
import pytest

from marketagg.calendar import SessionCalendar
from marketagg.domain import (
    ActionRequest,
    BarRequest,
    ChainRequest,
    DiscoveryRequest,
    FetchStatus,
    InstrumentRef,
    OptionType,
    PriceBasis,
    QualityFlag,
    QuoteTimeBasis,
    RateLimited,
    RejectReason,
    RequestCounts,
    SchemaChanged,
    Timeframe,
    Unauthorized,
    Unavailable,
    UnsupportedCapability,
)
from marketagg.providers.base import MarketDataProvider
from marketagg.providers.yahoo import (
    LiveAuthorization,
    LiveSourceBlocked,
    YahooProvider,
    map_chain,
    yahoo_capabilities,
)
from marketagg.settings import load_settings

SPY = InstrumentRef(provider_id="yahoo", symbol="SPY")
FAR = datetime(2035, 1, 1, tzinfo=UTC)
FETCHED = datetime(2025, 3, 7, 15, 0, 1, tzinfo=UTC)
CAL = SessionCalendar()
Options = namedtuple("Options", ["calls", "puts", "underlying"])


def chain_frame(side: str, n: int = 3, **overrides: list[Any]) -> pd.DataFrame:
    strikes = [580.0 + 5 * i for i in range(n)]
    data: dict[str, list[Any]] = {
        "contractSymbol": [f"SPY250411{side}{int(k * 1000):08d}" for k in strikes],
        "lastTradeDate": pd.to_datetime([1741355000 - 60 * i for i in range(n)], unit="s",
                                        utc=True),
        "strike": strikes,
        "lastPrice": [12.5 + i for i in range(n)],
        "bid": [12.4 + i for i in range(n)],
        "ask": [12.6 + i for i in range(n)],
        "change": [0.1] * n,
        "percentChange": [0.5] * n,
        "volume": [100.0 + i for i in range(n)],
        "openInterest": [1000.0 + i for i in range(n)],
        "impliedVolatility": [0.18 + 0.01 * i for i in range(n)],
        "inTheMoney": [False] * n,
        "contractSize": ["REGULAR"] * n,
        "currency": ["USD"] * n,
    }
    data.update(overrides)
    return pd.DataFrame(data)


def chain_request() -> ChainRequest:
    return ChainRequest(provider_id="yahoo", policy_id="p", deadline_at=FAR,
                        correlation_id="c", instrument=SPY, expiration_date=date(2025, 4, 11))


def _map(calls: pd.DataFrame | None, puts: pd.DataFrame | None) -> Any:
    return map_chain(calls, puts, request=chain_request(), fetch_started_at=FETCHED,
                     fetch_completed_at=FETCHED, response_id="00000000-0000-4000-8000-000000000001",
                     request_counts=RequestCounts(endpoint_requests=1, transport_requests=None))


def test_chain_mapping_keeps_unknown_timing_null() -> None:
    result = _map(chain_frame("C"), chain_frame("P"))
    assert result.status is FetchStatus.COMPLETE
    row = result.rows[0]
    assert row.provider_event_at is None and row.bid_size is None and row.ask_size is None
    assert row.field_semantics.quote_time_basis is QuoteTimeBasis.UNKNOWN
    assert row.terms is None  # contractSize 'REGULAR' never becomes multiplier=100
    assert row.bid == Decimal("12.4")
    assert row.contract.strike == Decimal("580.0")
    assert row.last_trade_at == datetime.fromtimestamp(1741355000, tz=UTC)
    assert row.reported_volume == 100 and row.reported_open_interest == 1000
    assert result.coverage.scope_complete is False  # never claimed for Yahoo
    assert result.advertised_delay_seconds == 900
    assert result.request_counts.transport_requests is None
    assert result.payload is not None
    assert result.payload.representation.value == "adapter_result"


def test_nan_means_not_reported_zero_stays_zero() -> None:
    calls = chain_frame("C", bid=[math.nan, 0.0, 1.0], volume=[math.nan, 0.0, 5.0],
                        openInterest=[0.0, math.nan, 7.0])
    result = _map(calls, chain_frame("P"))
    bids = [r.bid for r in result.rows[:3]]
    assert bids == [None, Decimal("0.0"), Decimal("1.0")]
    assert [r.reported_volume for r in result.rows[:3]] == [None, 0, 5]
    assert [r.reported_open_interest for r in result.rows[:3]] == [0, None, 7]


def test_infinite_values_and_bad_symbols_are_rejected_per_row() -> None:
    calls = chain_frame("C", ask=[math.inf, 12.7, 12.8])
    puts = chain_frame("P")
    puts.loc[1, "contractSymbol"] = "SPY250411P00999000"  # strike disagrees with symbol
    puts.loc[2, "contractSymbol"] = None
    result = _map(calls, puts)
    reasons = sorted(r.reason for r in result.rejected)
    assert reasons == sorted([RejectReason.NON_FINITE, RejectReason.AMBIGUOUS_CONTRACT,
                              RejectReason.MISSING_IDENTITY])
    assert result.status is FetchStatus.PARTIAL and len(result.rows) == 3


def test_adjusted_root_is_kept_and_flagged() -> None:
    calls = chain_frame("C", n=1)
    calls.loc[0, "contractSymbol"] = "SPY1250411C00580000"  # adjusted series root
    result = _map(calls, chain_frame("P", n=1))
    flagged = [r for r in result.rows if QualityFlag.AMBIGUOUS_CONTRACT in r.quality_flags]
    assert len(flagged) == 1 and flagged[0].contract.provider_symbol.startswith("SPY1")


def test_missing_side_and_empty_chain() -> None:
    partial = _map(chain_frame("C"), None)
    assert partial.status is FetchStatus.PARTIAL
    assert partial.coverage.missing_sides == (OptionType.PUT,)
    empty = _map(chain_frame("C", n=0), chain_frame("P", n=0))
    assert empty.status is FetchStatus.EMPTY and empty.coverage.empty_reason


def test_schema_drift_renamed_or_retyped_column() -> None:
    with pytest.raises(SchemaChanged) as renamed:
        _map(chain_frame("C").rename(columns={"bid": "bidPrice"}), chain_frame("P"))
    assert "bid" in renamed.value.details["missing"]
    with pytest.raises(SchemaChanged) as retyped:
        _map(chain_frame("C", strike=["580", "585", "590"]), chain_frame("P"))
    assert retyped.value.details["retyped"] == "strike"


# -- Provider-level behaviour with a fake ticker (no network) ----------------------------


class FakeTicker:
    def __init__(self, symbol: str, *, error: Exception | None = None) -> None:
        self.symbol = symbol
        self.error = error
        self.history_kwargs: dict[str, Any] = {}

    @property
    def options(self) -> tuple[str, ...]:
        return ("2025-04-11", "2025-03-14")

    def option_chain(self, when: str) -> Options:
        if self.error is not None:
            raise self.error
        if when != "2025-04-11":
            raise ValueError(f"Expiration `{when}` cannot be found. Available ...")
        return Options(chain_frame("C"), chain_frame("P"), {})

    def history(self, **kwargs: Any) -> pd.DataFrame:
        self.history_kwargs = kwargs
        if kwargs["interval"] == "1d":
            index = pd.DatetimeIndex(pd.to_datetime(["2025-03-06", "2025-03-07"]).tz_localize(
                "America/New_York"))
            return pd.DataFrame({"Open": [570.0, 571.0], "High": [575.0, 576.0],
                                 "Low": [565.0, 566.0], "Close": [572.0, 573.0],
                                 "Volume": [1e6, 2e6], "Dividends": [0.0, 1.75],
                                 "Stock Splits": [0.0, 0.0]}, index=index)
        if self.error is not None:
            raise self.error
        index = pd.date_range("2025-03-07 09:29", periods=4, freq="1min", tz="America/New_York")
        # Yahoo intraday prices are float32 values widened to float64 by yfinance.
        f32 = lambda v: float(np.float32(v))  # noqa: E731
        return pd.DataFrame({"Open": [f32(570.23)] * 4, "High": [f32(571.07)] * 4,
                             "Low": [f32(569.955)] * 4, "Close": [f32(570.61)] * 4,
                             "Volume": [10.0, 20.0, 30.0, 40.0],
                             "Dividends": [0.0] * 4, "Stock Splits": [0.0] * 4}, index=index)


class _Clock:
    def __init__(self) -> None:
        self.t = FETCHED

    def now(self) -> datetime:
        self.t += timedelta(milliseconds=500)
        return self.t


def provider(error: Exception | None = None) -> tuple[YahooProvider, dict[str, FakeTicker]]:
    made: dict[str, FakeTicker] = {}

    def factory(symbol: str) -> FakeTicker:
        made[symbol] = FakeTicker(symbol, error=error)
        return made[symbol]

    return YahooProvider(calendar=CAL, ticker_factory=factory, clock=_Clock()), made


def test_provider_satisfies_protocol_and_counts_cold_discovery() -> None:
    yahoo, _ = provider()
    assert isinstance(yahoo, MarketDataProvider)
    first = yahoo.fetch_chain(chain_request())
    second = yahoo.fetch_chain(chain_request())
    assert first.request_counts.endpoint_requests == 2  # hidden expiry discovery
    assert second.request_counts.endpoint_requests == 1
    assert first.response_id != second.response_id
    discovered = yahoo.discover(DiscoveryRequest(provider_id="yahoo", policy_id="p",
                                                 deadline_at=FAR, correlation_id="c",
                                                 instrument=SPY))
    assert discovered.expirations == (date(2025, 3, 14), date(2025, 4, 11))
    assert yahoo.fetch_chain(chain_request().model_copy(
        update={"expiration_date": date(2025, 5, 2)})).status is FetchStatus.EMPTY


def test_history_requests_are_unadjusted_and_flag_basis() -> None:
    yahoo, made = provider()
    request = BarRequest(provider_id="yahoo", policy_id="p", deadline_at=FAR,
                         correlation_id="c", instrument=SPY, timeframe=Timeframe.M1,
                         start=datetime(2025, 3, 7, 14, 29, tzinfo=UTC),
                         end=datetime(2025, 3, 7, 14, 33, tzinfo=UTC))
    result = yahoo.fetch_bars(request)
    kwargs = made["SPY"].history_kwargs
    assert kwargs["auto_adjust"] is False and kwargs["prepost"] is False
    assert kwargs["repair"] is False and kwargs["back_adjust"] is False
    assert [b.interval_start.minute for b in result.rows] == [30, 31, 32]  # 09:29 rejected
    # 570.2300109863281 (float32 widened) is stored as the 570.23 Yahoo sent.
    assert (result.rows[0].open, result.rows[0].low) == (Decimal("570.23"), Decimal("569.955"))
    assert result.rejected[0].reason is RejectReason.OUT_OF_WINDOW
    assert all(b.price_basis is PriceBasis.PROVIDER_NATIVE for b in result.rows)
    assert all(QualityFlag.BASIS_UNVERIFIED in b.quality_flags for b in result.rows)
    daily = yahoo.fetch_bars(request.model_copy(update={
        "timeframe": Timeframe.D1, "start": datetime(2025, 3, 6, tzinfo=UTC),
        "end": datetime(2025, 3, 8, tzinfo=UTC)}))
    assert daily.rows[1].interval_start == datetime(2025, 3, 7, 14, 30, tzinfo=UTC)
    assert daily.rows[1].session_date == date(2025, 3, 7)
    with pytest.raises(UnsupportedCapability):
        yahoo.fetch_bars(request.model_copy(update={"timeframe": Timeframe.H4}))


def test_dividend_mapping() -> None:
    yahoo, _ = provider()
    result = yahoo.fetch_actions(ActionRequest(
        provider_id="yahoo", policy_id="p", deadline_at=FAR, correlation_id="c",
        instrument=SPY, start_date=date(2025, 3, 1), end_date=date(2025, 3, 10)))
    assert [(a.action_type, a.cash_amount) for a in result.rows] == [
        ("dividend", Decimal("1.75"))]


class _HTTPError(Exception):
    def __init__(self, status: int, headers: dict[str, str] | None = None) -> None:
        super().__init__(f"HTTP {status}")
        self.response = type("R", (), {"status_code": status, "headers": headers or {}})()


class YFRateLimitError(Exception):
    pass


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (YFRateLimitError("Too Many Requests"), RateLimited),
        (_HTTPError(429, {"Retry-After": "120"}), RateLimited),
        (_HTTPError(403), Unauthorized),
        (_HTTPError(503), Unavailable),
        (TimeoutError("timed out"), Unavailable),
        (KeyError("optionChain"), SchemaChanged),
    ],
)
def test_exceptions_map_to_typed_failures(error: Exception, expected: type) -> None:
    yahoo, _ = provider(error)
    with pytest.raises(expected) as excinfo:
        yahoo.fetch_chain(chain_request())
    assert excinfo.value.request_counts.endpoint_requests >= 1
    if isinstance(error, _HTTPError) and error.response.status_code == 429:
        assert excinfo.value.retry_after == 120.0


class YFPricesMissingError(Exception):
    pass


def test_window_without_bars_is_empty_not_unavailable() -> None:
    yahoo, _ = provider(YFPricesMissingError("$SPY: possibly delisted; no price data found"))
    request = BarRequest(provider_id="yahoo", policy_id="p", deadline_at=FAR,
                         correlation_id="c", instrument=SPY, timeframe=Timeframe.M1,
                         start=datetime(2025, 3, 8, 14, 0, tzinfo=UTC),
                         end=datetime(2025, 3, 9, 14, 0, tzinfo=UTC))
    result = yahoo.fetch_bars(request)
    assert result.status is FetchStatus.EMPTY and result.rows == ()
    assert result.coverage.empty_reason is not None  # inconclusive, never "delisted"
    yahoo, _ = provider(_HTTPError(503))
    with pytest.raises(Unavailable):
        yahoo.fetch_bars(request)


def test_real_client_requires_live_gate() -> None:
    with pytest.raises(LiveSourceBlocked):
        YahooProvider(calendar=CAL)
    with pytest.raises(LiveSourceBlocked):
        LiveAuthorization.from_settings(load_settings("development", {}))
    caps = yahoo_capabilities()
    assert not caps.quotes.event_time.supported and not caps.quotes.sizes.supported
    assert not caps.options.historical_snapshots.supported
    assert caps.entitlements.reviewed is False
    assert caps.timing.advertised_delay_seconds == 900


@pytest.mark.live
def test_live_probe_requires_review() -> None:  # pragma: no cover - opt-in only
    settings = load_settings(os.environ.get("MARKETAGG_ENV"))
    auth = LiveAuthorization.from_settings(settings)
    yahoo = YahooProvider(calendar=CAL, authorization=auth)
    result = yahoo.discover(DiscoveryRequest(provider_id="yahoo", policy_id="probe",
                                             deadline_at=FAR, correlation_id="live",
                                             instrument=SPY))
    assert result.expirations
