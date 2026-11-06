"""Yahoo Finance adapter via the unofficial ``yfinance`` package (T06).

Source facts (pinned yfinance 1.7.0, read from its source, not live experiments):

- ``Ticker.option_chain(date)`` returns calls/puts frames with ``contractSymbol,
  lastTradeDate, strike, lastPrice, bid, ask, change, percentChange, volume,
  openInterest, impliedVolatility, inTheMoney, contractSize, currency``. There is
  no quote event time and no bid/ask size, so those stay null.
- A cold ``Ticker`` performs an extra expiry-discovery request inside
  ``option_chain(date)``; cookie/crumb calls are hidden. Transport requests are
  therefore reported as ``None`` (uncountable), never as a strict number.
- ``history`` defaults to ``auto_adjust=True``; this adapter always passes
  ``auto_adjust=False, back_adjust=False, repair=False, prepost=False`` and labels
  prices ``provider_native`` with ``basis_unverified`` (00: ``auto_adjust=False``
  does not prove as-traded prices).
- Only processed DataFrames are exposed, so retained payloads are
  ``adapter_result`` representations, not wire bytes.

Live use is gated: constructing the adapter around the real ``yfinance.Ticker``
requires a ``LiveAuthorization`` derived from settings with a recorded source
review. Mapping functions are pure and tested offline with synthetic frames.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import uuid4

import numpy as np
import pandas as pd
from pydantic import ValidationError

from marketagg.calendar import SessionCalendar
from marketagg.clock import SYSTEM_CLOCK, Clock
from marketagg.domain.capabilities import (
    CapabilityFact,
    CapabilityStatus,
    Confidence,
    DelayKind,
    Entitlements,
    IntervalCapability,
    OptionsCapabilities,
    ProviderCapabilities,
    QuoteCapabilities,
    SemanticsCapabilities,
    TimestampPrecision,
    TimingCapabilities,
)
from marketagg.domain.enums import (
    CompletenessConfidence,
    FetchStatus,
    OptionType,
    PayloadRepresentation,
    PriceBasis,
    QualityFlag,
    RejectReason,
    SessionKind,
    Timeframe,
)
from marketagg.domain.errors import (
    FailureCode,
    ProviderError,
    ProviderFailure,
    RateLimited,
    RequestCounts,
    SchemaChanged,
    Unauthorized,
    Unavailable,
    UnsupportedCapability,
)
from marketagg.domain.identifiers import ContractRef, occ_mismatches
from marketagg.domain.observations import (
    BarObservation,
    CorporateActionObservation,
    Coverage,
    DiscoveryResult,
    FetchResult,
    FieldSemantics,
    OptionObservation,
    PayloadRef,
    RejectedRow,
)
from marketagg.domain.requests import ActionRequest, BarRequest, ChainRequest, DiscoveryRequest
from marketagg.settings import ProviderName, Settings

YAHOO_PROVIDER_ID = "yahoo"
MAPPING_VERSION = "yahoo-map/2"  # 2: float32 history prices recovered exactly
CAPABILITY_VERSION = "yahoo-capabilities/1"
PINNED_YFINANCE = "1.7.0"
EVIDENCE_CHECKED = datetime(2026, 10, 24, tzinfo=UTC)  # planning documentation review
CHAIN_REQUIRED = ("contractSymbol", "strike", "bid", "ask")
HISTORY_REQUIRED = ("Open", "High", "Low", "Close", "Volume")
_EVIDENCE = {
    "chain": "yfinance 1.7.0 ticker.py option_chain/_options2df (source read)",
    "history": "yfinance 1.7.0 scrapers/history.py; intervals per yfinance reference",
    "delay": "https://help.yahoo.com/kb/SLN2310.html (OPRA listed as 15-minute delay)",
}
_INTERVALS = {Timeframe.M1: "1m", Timeframe.D1: "1d"}


class LiveSourceBlocked(RuntimeError):
    """Live Yahoo access requested without the configured review gate."""


@dataclass(frozen=True)
class LiveAuthorization:
    """Proof that settings passed the live-source gate. It records a review; it does
    not create data-use permission (00)."""

    source_review_id: str

    @classmethod
    def from_settings(cls, settings: Settings) -> LiveAuthorization:
        if (settings.provider is not ProviderName.YAHOO or not settings.live_data_enabled
                or not settings.source_review_id):
            raise LiveSourceBlocked(
                "Yahoo live access requires PROVIDER=yahoo, LIVE_DATA_ENABLED=true and a "
                "recorded SOURCE_REVIEW_ID")
        return cls(settings.source_review_id)


def _doc(status: CapabilityStatus, evidence: str) -> CapabilityFact:
    return CapabilityFact(status=status, confidence=Confidence.DOCUMENTED, evidence=evidence,
                          checked_at=EVIDENCE_CHECKED)


def yahoo_capabilities(review_id: str | None = None) -> ProviderCapabilities:
    yes, no = CapabilityStatus.SUPPORTED, CapabilityStatus.UNSUPPORTED
    return ProviderCapabilities(
        provider_id=YAHOO_PROVIDER_ID,
        capability_version=CAPABILITY_VERSION,
        reliability_tier="unofficial_delayed",
        synthetic=False,
        bars={
            # Request span / oldest history are separate and remain unknown until probed.
            Timeframe.M1: IntervalCapability(available=_doc(yes, _EVIDENCE["history"])),
            Timeframe.D1: IntervalCapability(available=_doc(yes, _EVIDENCE["history"])),
            Timeframe.M5: IntervalCapability(available=_doc(no, "derived locally (ADR-006)")),
            Timeframe.M15: IntervalCapability(available=_doc(no, "derived locally (ADR-006)")),
            Timeframe.H4: IntervalCapability(available=_doc(no, "no 4h interval documented")),
        },
        options=OptionsCapabilities(
            current_chain=_doc(yes, _EVIDENCE["chain"]),
            historical_snapshots=_doc(no, "no historical chain endpoint in yfinance"),
            streaming_quotes=_doc(no, "chain polling adapter only"),
        ),
        actions=_doc(yes, _EVIDENCE["history"]),
        quotes=QuoteCapabilities(
            event_time=_doc(no, _EVIDENCE["chain"] + ": no quote timestamp column"),
            sizes=_doc(no, _EVIDENCE["chain"] + ": no bid/ask size columns"),
            sequence=_doc(no, _EVIDENCE["chain"]),
            replay=_doc(no, _EVIDENCE["chain"]),
        ),
        timing=TimingCapabilities(
            delay_kind=DelayKind.DELAYED,
            advertised_delay_seconds=900,
            timestamp_precision=TimestampPrecision.UNKNOWN,
            evidence=_doc(yes, _EVIDENCE["delay"]),
        ),
        semantics=SemanticsCapabilities(
            adjustment_basis=PriceBasis.PROVIDER_NATIVE,
            session_coverage=(SessionKind.RTH,),
            evidence=_doc(yes, "auto_adjust=False, prepost=False passed explicitly"),
        ),
        entitlements=Entitlements(
            reviewed=review_id is not None, review_id=review_id,
            notes="Unofficial package; Yahoo terms/automation/retention rights unverified "
                  "(planning could not retrieve terms). A review id records a decision; "
                  "it does not grant rights.",
        ),
    )


TickerFactory = Callable[[str], Any]


class YahooProvider:
    def __init__(
        self,
        *,
        calendar: SessionCalendar,
        authorization: LiveAuthorization | None = None,
        ticker_factory: TickerFactory | None = None,
        clock: Clock = SYSTEM_CLOCK,
        timeout_seconds: int = 30,
    ) -> None:
        if ticker_factory is None:
            if authorization is None:
                raise LiveSourceBlocked("real yfinance access requires LiveAuthorization")
            import yfinance as yf  # noqa: PLC0415 - optional live dependency

            if yf.__version__ != PINNED_YFINANCE:
                raise LiveSourceBlocked(f"yfinance {yf.__version__} is not the reviewed "
                                        f"{PINNED_YFINANCE}; re-run source qualification")
            ticker_factory = yf.Ticker
        self.calendar = calendar
        self.authorization = authorization
        self._factory = ticker_factory
        self._tickers: dict[str, Any] = {}
        self.clock = clock
        self.timeout = timeout_seconds
        self._capabilities = yahoo_capabilities(
            authorization.source_review_id if authorization else None)

    @property
    def provider_id(self) -> str:
        return YAHOO_PROVIDER_ID

    def capabilities(self) -> ProviderCapabilities:
        return self._capabilities

    def _ticker(self, symbol: str) -> tuple[Any, bool]:
        # Cache per worker for efficiency only; correctness never depends on it.
        cold = symbol not in self._tickers
        if cold:
            self._tickers[symbol] = self._factory(symbol)
        return self._tickers[symbol], cold

    def discover(self, request: DiscoveryRequest) -> DiscoveryResult:
        ticker, _ = self._ticker(request.instrument.symbol)
        started = self.clock.now()
        expirations_raw = _call(lambda: tuple(ticker.options), RequestCounts(endpoint_requests=1))
        completed = self.clock.now()
        expirations = tuple(sorted({date.fromisoformat(str(d)) for d in expirations_raw}))
        body = _json_bytes({"expirations": [d.isoformat() for d in expirations]})
        return DiscoveryResult(
            provider_id=YAHOO_PROVIDER_ID, response_id=uuid4(),
            correlation_id=request.correlation_id, instrument=request.instrument,
            fetch_started_at=started, fetch_completed_at=completed, expirations=expirations,
            payload=_payload_ref(body), payload_body=body,
            request_counts=RequestCounts(endpoint_requests=1, transport_requests=None),
        )

    def fetch_chain(self, request: ChainRequest) -> FetchResult[OptionObservation]:
        ticker, cold = self._ticker(request.instrument.symbol)
        endpoint = 2 if cold else 1  # cold client discovers expiries first
        counts = RequestCounts(endpoint_requests=endpoint, transport_requests=None)
        started = self.clock.now()
        try:
            chain = _call(lambda: ticker.option_chain(request.expiration_date.isoformat()),
                          counts)
            calls, puts = chain.calls, chain.puts
        except _UnlistedExpiration:
            calls = puts = None
        completed = self.clock.now()
        return map_chain(calls, puts, request=request, fetch_started_at=started,
                         fetch_completed_at=completed, response_id=uuid4(),
                         request_counts=counts)

    def fetch_bars(self, request: BarRequest) -> FetchResult[BarObservation]:
        interval = _INTERVALS.get(request.timeframe)
        if interval is None or request.session_kind is not SessionKind.RTH:
            raise UnsupportedCapability(f"bars.intervals.{request.timeframe}")
        ticker, _ = self._ticker(request.instrument.symbol)
        counts = RequestCounts(endpoint_requests=1, transport_requests=None)
        started = self.clock.now()
        try:
            frame = _call(lambda: ticker.history(
                interval=interval, start=request.start, end=request.end, prepost=False,
                actions=True, auto_adjust=False, back_adjust=False, repair=False,
                keepna=False, rounding=False, timeout=self.timeout, raise_errors=True), counts)
        except Unavailable as exc:
            # yfinance raises "possibly delisted; no price data found" for any window
            # without bars (weekends, holidays). That is an inconclusive empty response,
            # not a transport failure that should trip the provider circuit.
            if exc.details.get("kind") != "YFPricesMissingError":
                raise
            frame = None
        completed = self.clock.now()
        return map_history(frame, request=request, calendar=self.calendar,
                           fetch_started_at=started, fetch_completed_at=completed,
                           response_id=uuid4(), request_counts=counts)

    def fetch_actions(self, request: ActionRequest) -> FetchResult[CorporateActionObservation]:
        ticker, _ = self._ticker(request.instrument.symbol)
        counts = RequestCounts(endpoint_requests=1, transport_requests=None)
        started = self.clock.now()
        frame = _call(lambda: ticker.history(
            interval="1d", start=request.start_date.isoformat(),
            end=request.end_date.isoformat(), actions=True, auto_adjust=False,
            back_adjust=False, repair=False, timeout=self.timeout, raise_errors=True), counts)
        completed = self.clock.now()
        return map_actions(frame, request=request, fetch_started_at=started,
                           fetch_completed_at=completed, response_id=uuid4(),
                           request_counts=counts)


# -- Error mapping -----------------------------------------------------------------------


class _UnlistedExpiration(Exception):
    pass


def _call(fn: Callable[[], Any], counts: RequestCounts) -> Any:
    try:
        return fn()
    except ProviderError:
        raise
    except Exception as exc:  # map third-party failures into typed provider errors
        raise _map_exception(exc, counts) from exc


def _map_exception(exc: Exception, counts: RequestCounts) -> Exception:  # noqa: PLR0911
    name = type(exc).__name__
    message = str(exc)[:300]
    if name == "YFRateLimitError":
        return RateLimited(message, request_counts=counts)
    status = getattr(getattr(exc, "response", None), "status_code", None)
    if status == 429:
        headers = getattr(getattr(exc, "response", None), "headers", {}) or {}
        retry_after = _float_or_none(headers.get("Retry-After"))
        return RateLimited(message, retry_after=retry_after, request_counts=counts)
    if status in (401, 403):
        return Unauthorized(message, request_counts=counts)
    if isinstance(exc, ValueError) and "cannot be found" in message:
        return _UnlistedExpiration(message)
    if name in {"YFTickerMissingError", "YFPricesMissingError", "YFTzMissingError"}:
        return Unavailable(message, request_counts=counts, details={"kind": name})
    if isinstance(exc, KeyError | TypeError | AttributeError):
        return SchemaChanged(f"unexpected response shape: {name}: {message}",
                             request_counts=counts)
    return Unavailable(f"{name}: {message}", request_counts=counts)


# -- Pure mapping ------------------------------------------------------------------------


def _json_bytes(doc: Mapping[str, Any]) -> bytes:
    return json.dumps(doc, sort_keys=True, separators=(",", ":"), default=str,
                      allow_nan=True).encode()


def _payload_ref(body: bytes) -> PayloadRef:
    return PayloadRef(sha256=hashlib.sha256(body).hexdigest(), byte_count=len(body),
                      representation=PayloadRepresentation.ADAPTER_RESULT,
                      parser_version=MAPPING_VERSION)


def _frame_records(frame: pd.DataFrame | None) -> list[dict[str, Any]]:
    if frame is None:
        return []
    records: list[dict[str, Any]] = json.loads(frame.to_json(orient="records",
                                                              date_format="iso"))
    return records


class _NonFiniteValue(ValueError):
    pass


def _is_missing(value: Any) -> bool:
    if value is None or value is pd.NaT:
        return True
    return isinstance(value, float) and math.isnan(value)


def _decimal(value: Any) -> Decimal | None:
    if _is_missing(value):
        return None
    if isinstance(value, bool):
        raise ValueError("bool is not a price")
    if isinstance(value, float | int) and not math.isfinite(float(value)):
        raise _NonFiniteValue("non-finite price")
    try:
        result = Decimal(str(value))  # shortest repr of the float Yahoo returned
    except InvalidOperation as exc:
        raise ValueError(f"not numeric: {value!r}") from exc
    if not result.is_finite():
        raise _NonFiniteValue("non-finite price")
    return result


def _int(value: Any) -> int | None:
    if _is_missing(value):
        return None
    number = float(value)
    if not math.isfinite(number):
        raise _NonFiniteValue("non-finite count")
    if number != int(number):
        raise ValueError(f"non-integral count {value!r}")
    return int(number)


def _float_or_none(value: Any) -> float | None:
    if _is_missing(value):
        return None
    number = float(value)
    if not math.isfinite(number):
        raise _NonFiniteValue("non-finite float")
    return number


def _timestamp(value: Any) -> datetime | None:
    if _is_missing(value):
        return None
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        raise ValueError("naive timestamp from source")
    result: datetime = stamp.tz_convert("UTC").to_pydatetime()
    return result


def _schema_check(frame: pd.DataFrame, required: tuple[str, ...], numeric: tuple[str, ...],
                  counts: RequestCounts, what: str) -> None:
    missing = [c for c in required if c not in frame.columns]
    retyped = [c for c in numeric if c in frame.columns and len(frame)
               and not pd.api.types.is_numeric_dtype(frame[c])]
    if missing or retyped:
        raise SchemaChanged(
            f"{what} schema drift", request_counts=counts,
            details={"missing": ",".join(missing), "retyped": ",".join(retyped),
                     "columns": ",".join(map(str, frame.columns))[:500]})


def _fingerprint(frames: list[pd.DataFrame | None]) -> str:
    parts = sorted(f"{c}:{f[c].dtype}" for f in frames if f is not None for c in f.columns)
    return "yahoo:" + hashlib.sha256(",".join(parts).encode()).hexdigest()[:16]


def map_chain(
    calls: pd.DataFrame | None,
    puts: pd.DataFrame | None,
    *,
    request: ChainRequest,
    fetch_started_at: datetime,
    fetch_completed_at: datetime,
    response_id: Any,
    request_counts: RequestCounts,
) -> FetchResult[OptionObservation]:
    sides = {OptionType.CALL: calls, OptionType.PUT: puts}
    for frame in sides.values():
        if frame is not None:
            _schema_check(frame, CHAIN_REQUIRED, ("strike", "bid", "ask"), request_counts,
                          "option chain")
    semantics = FieldSemantics(mapping_version=MAPPING_VERSION)  # all timing unknown
    rows: list[OptionObservation] = []
    rejected: list[RejectedRow] = []
    position = 0
    for side, frame in sides.items():
        for raw in (frame.to_dict(orient="records") if frame is not None else []):
            try:
                rows.append(_option_row(raw, side, request, fetch_completed_at, semantics))
            except _NonFiniteValue as exc:
                rejected.append(_reject(RejectReason.NON_FINITE, exc, position, raw))
            except _AmbiguousRow as exc:
                rejected.append(_reject(RejectReason.AMBIGUOUS_CONTRACT, exc, position, raw))
            except (ValueError, ValidationError, TypeError) as exc:
                symbol = raw.get("contractSymbol")
                reason = (RejectReason.MISSING_IDENTITY
                          if not isinstance(symbol, str) or not symbol
                          else RejectReason.MALFORMED)
                rejected.append(_reject(reason, exc, position, raw))
            position += 1
    observed = tuple(s for s in sides if any(r.contract.option_type is s for r in rows))
    total = sum(len(f) for f in sides.values() if f is not None)
    counts_by_reason: dict[str, int] = {}
    for r in rejected:
        counts_by_reason[r.reason.value] = counts_by_reason.get(r.reason.value, 0) + 1
    fingerprint = _fingerprint([calls, puts])
    coverage = Coverage(
        requested_expiries=(request.expiration_date,),
        returned_expiries=(request.expiration_date,) if rows else (),
        expected_sides=request.expected_sides, observed_sides=observed,
        rows_before_filters=total, rows_after_filters=len(rows),
        exclusions_by_reason=counts_by_reason,
        scope_complete=False,  # an HTTP success never proves chain completeness
        completeness_confidence=CompletenessConfidence.UNKNOWN,
        empty_reason=None if total else "no rows returned for expiration",
        source_schema_fingerprint=fingerprint,
    )
    status, failures = _status(rows, rejected, coverage.missing_sides)
    body = _json_bytes({"calls": _frame_records(calls), "puts": _frame_records(puts)})
    return FetchResult[OptionObservation](
        provider_id=YAHOO_PROVIDER_ID, response_id=response_id,
        correlation_id=request.correlation_id, fetch_started_at=fetch_started_at,
        fetch_completed_at=fetch_completed_at, status=status, rows=tuple(rows),
        rejected=tuple(rejected), coverage=coverage, payload=_payload_ref(body),
        payload_body=body, source_fingerprint=fingerprint, request_counts=request_counts,
        advertised_delay_seconds=900, failures=failures,
    )


class _AmbiguousRow(ValueError):
    pass


def _option_row(raw: Mapping[str, Any], side: OptionType, request: ChainRequest,
                observed_at: datetime, semantics: FieldSemantics) -> OptionObservation:
    symbol = raw.get("contractSymbol")
    if not isinstance(symbol, str) or not symbol:
        raise ValueError("contractSymbol missing")
    strike = _decimal(raw.get("strike"))
    if strike is None:
        raise ValueError("strike missing")
    contract = ContractRef(provider_id=YAHOO_PROVIDER_ID, provider_symbol=symbol,
                           underlying_symbol=request.instrument.symbol, option_type=side,
                           strike=strike, expiration_date=request.expiration_date)
    mismatch = occ_mismatches(contract)
    if mismatch and mismatch != ["root"]:
        raise _AmbiguousRow(f"symbol disagrees on {','.join(mismatch)}")
    iv = _float_or_none(raw.get("impliedVolatility"))
    flags = (QualityFlag.AMBIGUOUS_CONTRACT,) if mismatch else ()  # e.g. adjusted root
    return OptionObservation(
        contract=contract, observed_at=observed_at,
        last_trade_at=_timestamp(raw.get("lastTradeDate")),
        bid=_decimal(raw.get("bid")), ask=_decimal(raw.get("ask")),
        last=_decimal(raw.get("lastPrice")), reported_iv=iv,
        reported_volume=_int(raw.get("volume")),
        reported_open_interest=_int(raw.get("openInterest")),
        in_the_money=None if _is_missing(raw.get("inTheMoney")) else bool(raw["inTheMoney"]),
        field_semantics=semantics, quality_flags=flags,
    )


def map_history(
    frame: pd.DataFrame | None,
    *,
    request: BarRequest,
    calendar: SessionCalendar,
    fetch_started_at: datetime,
    fetch_completed_at: datetime,
    response_id: Any,
    request_counts: RequestCounts,
) -> FetchResult[BarObservation]:
    if frame is not None and len(frame):
        _schema_check(frame, HISTORY_REQUIRED, HISTORY_REQUIRED, request_counts, "history")
        if not isinstance(frame.index, pd.DatetimeIndex) or frame.index.tz is None:
            raise SchemaChanged("history index is not a timezone-aware DatetimeIndex",
                                request_counts=request_counts)
    bars: list[BarObservation] = []
    rejected: list[RejectedRow] = []
    records = [] if frame is None else list(zip(frame.index, frame.to_dict(orient="records"),
                                                strict=True))
    for position, (stamp, raw) in enumerate(records):
        try:
            bars.append(_bar_row(stamp, raw, request, calendar))
        except _NonFiniteValue as exc:
            rejected.append(_reject(RejectReason.NON_FINITE, exc, position, raw))
        except _OutOfSession as exc:
            rejected.append(_reject(RejectReason.OUT_OF_WINDOW, exc, position, raw))
        except ValidationError as exc:
            reason = (RejectReason.INVALID_OHLC if "OHLC" in str(exc)
                      else RejectReason.MALFORMED)
            rejected.append(_reject(reason, exc, position, raw))
        except (ValueError, TypeError) as exc:
            rejected.append(_reject(RejectReason.MALFORMED, exc, position, raw))
    counts_by_reason: dict[str, int] = {}
    for r in rejected:
        counts_by_reason[r.reason.value] = counts_by_reason.get(r.reason.value, 0) + 1
    coverage = Coverage(
        requested_start=request.start, requested_end=request.end,
        rows_before_filters=len(records), rows_after_filters=len(bars),
        exclusions_by_reason=counts_by_reason,
        empty_reason=None if records else "no rows returned (inconclusive until checked)",
        source_schema_fingerprint=_fingerprint([frame]),
    )
    status, failures = _status(bars, rejected, ())
    body = _json_bytes({"index": [str(s) for s, _ in records],
                        "rows": _frame_records(frame)})
    return FetchResult[BarObservation](
        provider_id=YAHOO_PROVIDER_ID, response_id=response_id,
        correlation_id=request.correlation_id, fetch_started_at=fetch_started_at,
        fetch_completed_at=fetch_completed_at, status=status, rows=tuple(bars),
        rejected=tuple(rejected), coverage=coverage, payload=_payload_ref(body),
        payload_body=body, source_fingerprint=_fingerprint([frame]),
        request_counts=request_counts, failures=failures,
    )


class _OutOfSession(ValueError):
    pass


def _history_decimal(value: Any) -> Decimal | None:
    """Yahoo's chart API carries prices as float32, widened to float64 by yfinance.

    767.23 arrives as 767.22998046875, which exceeds numeric(20,8). When the value is
    exactly a float32, its shortest float32 representation is the value Yahoo sent.
    """
    if isinstance(value, float) and math.isfinite(value):
        single = np.float32(value)
        if float(single) == value:
            return _decimal(np.format_float_positional(single, unique=True, trim="-"))
    return _decimal(value)


def _bar_row(stamp: Any, raw: Mapping[str, Any], request: BarRequest,
             calendar: SessionCalendar) -> BarObservation:
    values: dict[str, Decimal] = {}
    for column in ("Open", "High", "Low", "Close"):
        value = _history_decimal(raw.get(column))
        if value is None:
            raise ValueError(f"missing {column}")
        values[column] = value
    when = pd.Timestamp(stamp)
    if request.timeframe is Timeframe.D1:
        session = calendar.session(when.date())
        if session is None:
            raise _OutOfSession(f"{when.date()} is not a session")
        start, end = session.open_at, session.close_at
    else:
        start = when.tz_convert("UTC").to_pydatetime()
        session = calendar.session_at(start)
        if session is None:
            raise _OutOfSession(f"{start} is outside the RTH session")
        end = start + timedelta(minutes=1)
    return BarObservation(
        instrument=request.instrument, timeframe=request.timeframe, interval_start=start,
        interval_end=end, session_date=session.session_date, session_kind=SessionKind.RTH,
        price_basis=PriceBasis.PROVIDER_NATIVE, open=values["Open"], high=values["High"],
        low=values["Low"], close=values["Close"], volume=_int(raw.get("Volume")),
        quality_flags=(QualityFlag.BASIS_UNVERIFIED,),
    )


def map_actions(
    frame: pd.DataFrame | None,
    *,
    request: ActionRequest,
    fetch_started_at: datetime,
    fetch_completed_at: datetime,
    response_id: Any,
    request_counts: RequestCounts,
) -> FetchResult[CorporateActionObservation]:
    rows: list[CorporateActionObservation] = []
    if frame is not None and len(frame):
        for stamp, raw in zip(frame.index, frame.to_dict(orient="records"), strict=True):
            day = pd.Timestamp(stamp).date()
            split = _float_or_none(raw.get("Stock Splits"))
            dividend = _float_or_none(raw.get("Dividends"))
            if split:
                rows.append(CorporateActionObservation(
                    instrument=request.instrument, action_type="split", effective_date=day,
                    ratio=Decimal(str(split)), details={"source_column": "Stock Splits"}))
            if dividend:
                rows.append(CorporateActionObservation(
                    instrument=request.instrument, action_type="dividend", effective_date=day,
                    cash_amount=Decimal(str(dividend)), currency="USD",
                    details={"source_column": "Dividends", "currency_assumed": "true"}))
    body = _json_bytes({"actions": [r.model_dump(mode="json") for r in rows]})
    return FetchResult[CorporateActionObservation](
        provider_id=YAHOO_PROVIDER_ID, response_id=response_id,
        correlation_id=request.correlation_id, fetch_started_at=fetch_started_at,
        fetch_completed_at=fetch_completed_at,
        status=FetchStatus.COMPLETE if rows else FetchStatus.EMPTY, rows=tuple(rows),
        coverage=Coverage(rows_before_filters=len(rows), rows_after_filters=len(rows),
                          empty_reason=None if rows else "no actions reported"),
        payload=_payload_ref(body), payload_body=body, request_counts=request_counts,
    )


def _reject(reason: RejectReason, exc: Exception, position: int,
            raw: Mapping[str, Any]) -> RejectedRow:
    key = raw.get("contractSymbol")
    return RejectedRow(reason=reason, detail=str(exc).splitlines()[0][:200] or reason.value,
                       row_index=position, source_key=key if isinstance(key, str) else None)


def _status(rows: list[Any], rejected: list[RejectedRow], missing: tuple[OptionType, ...]
            ) -> tuple[FetchStatus, tuple[ProviderFailure, ...]]:
    if not rows:
        return FetchStatus.EMPTY, ()
    details: dict[str, str] = {}
    if missing:
        details["missing_sides"] = ",".join(s.value for s in missing)
    if rejected:
        details["rejected_rows"] = str(len(rejected))
    if not details:
        return FetchStatus.COMPLETE, ()
    return FetchStatus.PARTIAL, (ProviderFailure(code=FailureCode.PARTIAL_RESULT,
                                                 message="partial response", retryable=False,
                                                 details=details),)
