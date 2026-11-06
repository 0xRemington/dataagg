"""Deterministic fake provider for offline development and tests (T02).

Everything is a pure function of ``FakeScenario`` plus the injected ``FakeClock``:
two providers built from equal inputs replay byte-identical payloads, response
IDs and observations. Each fetch renders a synthetic JSON payload and then parses
it with a versioned parser, so malformed rows, non-finite values, schema drift and
parser corrections exercise the same path a real adapter uses.

Synthetic symbols (``SYNA``, ``SYNB``) avoid any resemblance to licensed data.
Sessions are naive weekday 09:30-16:00 America/New_York; this is NOT an exchange
calendar (T04 owns holidays and early closes). Quote event times are absent by
default, like the planned Yahoo adapter, so unknown freshness stays unknown.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any, cast
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import ValidationError

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
    RequestLimits,
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
    QuoteTimeBasis,
    RejectReason,
    SessionKind,
    SizeUnits,
    Timeframe,
    VolumeDateBasis,
    VolumeKind,
)
from marketagg.domain.errors import (
    FailureCode,
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

FAKE_PROVIDER_ID = "fake"
PARSER_V1 = "fake-parser/1"
PARSER_V2 = "fake-parser/2"  # v1 dropped ``inTheMoney``; v2 maps it (a parser correction)
PARSER_VERSIONS = (PARSER_V1, PARSER_V2)
CAPABILITY_VERSION = "fake-capabilities/1"
# Fixed so capability declarations are deterministic.
CAPABILITY_CHECKED_AT = datetime(2026, 10, 24, tzinfo=UTC)

NEW_YORK = ZoneInfo("America/New_York")
SESSION_OPEN = time(9, 30)
SESSION_CLOSE = time(16, 0)
CENT = Decimal("0.01")

_CHAIN_SCHEMA = "fake-chain/1"
_BARS_SCHEMA = "fake-bars/1"
_DISCOVERY_SCHEMA = "fake-discovery/1"
_CHAIN_REQUIRED = frozenset({"contractSymbol", "strike", "bid", "ask"})
_BAR_REQUIRED = frozenset({"t", "o", "h", "l", "c", "v"})


class FakeClock:
    """Injectable UTC clock; the fake advances it by the scripted latency."""

    def __init__(self, start: datetime) -> None:
        if start.tzinfo is None:
            raise ValueError("FakeClock requires an aware datetime")
        self._now = start.astimezone(UTC)

    def now(self) -> datetime:
        return self._now

    def advance(self, delta: timedelta) -> datetime:
        if delta < timedelta(0):
            raise ValueError("FakeClock cannot move backwards; use set() for clock jumps")
        self._now += delta
        return self._now

    def set(self, value: datetime) -> None:
        if value.tzinfo is None:
            raise ValueError("FakeClock requires an aware datetime")
        self._now = value.astimezone(UTC)


class FakeAction(StrEnum):
    OK = "ok"
    RATE_LIMITED = "rate_limited"
    UNAUTHORIZED = "unauthorized"
    UNAVAILABLE = "unavailable"
    SCHEMA_CHANGED = "schema_changed"
    CALLS_ONLY = "calls_only"
    PUTS_ONLY = "puts_only"
    EMPTY = "empty"
    MALFORMED_ROW = "malformed_row"
    NONFINITE_ROW = "nonfinite_row"
    AMBIGUOUS_ROW = "ambiguous_row"
    INVALID_OHLC = "invalid_ohlc"


@dataclass(frozen=True)
class ScriptStep:
    action: FakeAction
    retry_after: float | None = None
    extra_latency: timedelta = timedelta(0)


@dataclass(frozen=True)
class BarRevision:
    """From the ``from_call``-th (1-based) bars fetch on, this bar's close moves."""

    interval_start: datetime
    from_call: int
    close_delta: Decimal


@dataclass(frozen=True)
class ActionSpec:
    """A synthetic corporate action the provider reports from ``visible_from`` on."""

    symbol: str
    action_type: str
    effective_date: date
    visible_from: datetime
    ratio: Decimal | None = None
    cash_amount: Decimal | None = None


@dataclass(frozen=True)
class FakeScenario:
    seed: int = 7
    symbols: tuple[str, ...] = ("SYNA", "SYNB")
    latency: timedelta = timedelta(milliseconds=750)
    strikes_per_side: int = 12
    # Option prices are constant within one epoch, so two polls in the same epoch
    # are equal-price observations with distinct response IDs.
    price_epoch: timedelta = timedelta(minutes=5)
    quote_event_times: bool = False
    completeness_known: bool = True
    anomalies: bool = True
    parser_version: str = PARSER_V2
    # Keys: "discover", "bars", "chain". Steps are consumed in order, then OK.
    script: Mapping[str, tuple[ScriptStep, ...]] = field(default_factory=dict)
    bar_revisions: tuple[BarRevision, ...] = ()
    omitted_minutes: frozenset[datetime] = frozenset()
    actions: tuple[ActionSpec, ...] = ()

    def __post_init__(self) -> None:
        if self.parser_version not in PARSER_VERSIONS:
            raise ValueError(f"unknown parser_version {self.parser_version!r}")
        if self.strikes_per_side < 1:
            raise ValueError("strikes_per_side must be positive")
        unknown = set(self.script) - {"discover", "bars", "chain", "actions"}
        if unknown:
            raise ValueError(f"unknown script operations: {sorted(unknown)}")


class _NonFinite:
    def __init__(self, token: str) -> None:
        self.token = token


def _fact(status: CapabilityStatus, evidence: str) -> CapabilityFact:
    return CapabilityFact(
        status=status,
        confidence=Confidence.SYNTHETIC,
        evidence=evidence,
        checked_at=CAPABILITY_CHECKED_AT,
    )


_YES = CapabilityStatus.SUPPORTED
_NO = CapabilityStatus.UNSUPPORTED


def fake_capabilities(scenario: FakeScenario) -> ProviderCapabilities:
    src = "marketagg.providers.fake (synthetic)"
    event = _YES if scenario.quote_event_times else _NO
    return ProviderCapabilities(
        provider_id=FAKE_PROVIDER_ID,
        capability_version=CAPABILITY_VERSION,
        reliability_tier="synthetic",
        synthetic=True,
        bars={
            Timeframe.M1: IntervalCapability(available=_fact(_YES, src)),
            Timeframe.D1: IntervalCapability(available=_fact(_YES, src)),
            # Derived locally from canonical 1m (01 ADR-006), as with Yahoo.
            Timeframe.M5: IntervalCapability(available=_fact(_NO, src)),
            Timeframe.M15: IntervalCapability(available=_fact(_NO, src)),
            Timeframe.H4: IntervalCapability(available=_fact(_NO, src)),
        },
        options=OptionsCapabilities(
            current_chain=_fact(_YES, src),
            historical_snapshots=_fact(_NO, src),
            streaming_quotes=_fact(_NO, src),
        ),
        actions=_fact(_YES, src),
        quotes=QuoteCapabilities(
            event_time=_fact(event, src),
            sizes=_fact(_NO, src),
            sequence=_fact(_NO, src),
            replay=_fact(_NO, src),
        ),
        timing=TimingCapabilities(
            delay_kind=DelayKind.SYNTHETIC,
            timestamp_precision=(
                TimestampPrecision.NANOSECOND
                if scenario.quote_event_times
                else TimestampPrecision.UNKNOWN
            ),
            evidence=_fact(_YES, src),
        ),
        semantics=SemanticsCapabilities(
            adjustment_basis=PriceBasis.PROVIDER_NATIVE,
            session_coverage=(SessionKind.RTH,),
            volume_semantics=VolumeKind.SESSION_CUMULATIVE,
            evidence=_fact(_YES, src),
        ),
        entitlements=Entitlements(
            notes="Synthetic data only; confers no market-data rights.",
        ),
        request_limits=RequestLimits(),
    )


class FakeProvider:
    """Implements ``MarketDataProvider`` deterministically and offline."""

    def __init__(self, scenario: FakeScenario | None = None, clock: FakeClock | None = None):
        self.scenario = scenario or FakeScenario()
        self.clock = clock or FakeClock(datetime(2025, 3, 7, 15, 0, tzinfo=UTC))
        self._calls: dict[str, int] = {"discover": 0, "bars": 0, "chain": 0, "actions": 0}
        self._responses = 0
        self._capabilities = fake_capabilities(self.scenario)

    @property
    def provider_id(self) -> str:
        return FAKE_PROVIDER_ID

    @property
    def call_counts(self) -> Mapping[str, int]:
        return dict(self._calls)

    def capabilities(self) -> ProviderCapabilities:
        return self._capabilities

    # -- MarketDataProvider -------------------------------------------------

    def discover(self, request: DiscoveryRequest) -> DiscoveryResult:
        self._check_provider(request.provider_id)
        started, completed, step = self._begin("discover", request.deadline_at)
        symbol = request.instrument.symbol
        expirations: list[str] = []
        price: float | None = None
        if symbol in self.scenario.symbols and step.action is not FakeAction.EMPTY:
            expirations = [d.isoformat() for d in self._expirations(_ny_date(completed))]
            price = float(self._spot(symbol, completed))
        body = _dumps({"schema": _DISCOVERY_SCHEMA, "symbol": symbol,
                       "expirations": expirations, "price": price})
        doc = _loads(body)
        parsed_price = doc["price"]
        return DiscoveryResult(
            provider_id=FAKE_PROVIDER_ID,
            response_id=self._response_id(),
            correlation_id=request.correlation_id,
            instrument=request.instrument,
            fetch_started_at=started,
            fetch_completed_at=completed,
            expirations=tuple(date.fromisoformat(d) for d in doc["expirations"]),
            underlying_reference_price=parsed_price,
            payload=self._payload_ref(body, self.scenario.parser_version),
            payload_body=body,
            source_fingerprint=_fingerprint(_DISCOVERY_SCHEMA, doc.keys()),
            request_counts=_ONE_REQUEST,
        )

    def fetch_bars(self, request: BarRequest) -> FetchResult[BarObservation]:
        self._check_provider(request.provider_id)
        if request.session_kind is not SessionKind.RTH:
            raise UnsupportedCapability("bars.session_kind.ETH")
        if not self._capabilities.supports_timeframe(request.timeframe):
            raise UnsupportedCapability(f"bars.intervals.{request.timeframe}")
        started, completed, step = self._begin("bars", request.deadline_at)
        call_index = self._calls["bars"]
        rows: list[dict[str, Any]] = []
        listed = request.instrument.symbol in self.scenario.symbols
        if listed and step.action is not FakeAction.EMPTY:
            rows = list(self._bar_rows(request, completed, call_index))
        if step.action is FakeAction.INVALID_OHLC and rows:
            rows[0] = {**rows[0], "h": rows[0]["l"] - 1}
        if step.action is FakeAction.NONFINITE_ROW and rows:
            rows[0] = {**rows[0], "c": math.nan}
        if step.action is FakeAction.MALFORMED_ROW and rows:
            rows[0] = {**rows[0], "o": "n/a"}
        if step.action is FakeAction.SCHEMA_CHANGED and rows:
            rows = [{("close" if k == "c" else k): v for k, v in r.items()} for r in rows]
        body = _dumps({"schema": _BARS_SCHEMA, "symbol": request.instrument.symbol,
                       "interval": request.timeframe.value, "rows": rows})
        return self._parse_bars(request, body, started, completed, self._response_id(),
                                self.scenario.parser_version)

    def fetch_chain(self, request: ChainRequest) -> FetchResult[OptionObservation]:
        self._check_provider(request.provider_id)
        started, completed, step = self._begin("chain", request.deadline_at)
        symbol = request.instrument.symbol
        listed = self._expirations(_ny_date(completed))
        calls: list[dict[str, Any]] = []
        puts: list[dict[str, Any]] = []
        if (
            symbol in self.scenario.symbols
            and request.expiration_date in listed
            and step.action is not FakeAction.EMPTY
        ):
            calls, puts = self._chain_rows(symbol, request.expiration_date, completed)
        if step.action is FakeAction.CALLS_ONLY:
            puts = []
        elif step.action is FakeAction.PUTS_ONLY:
            calls = []
        elif step.action is FakeAction.MALFORMED_ROW and calls:
            calls[0] = {**calls[0], "strike": "n/a"}
        elif step.action is FakeAction.NONFINITE_ROW and calls:
            calls[0] = {**calls[0], "bid": math.nan}
        elif step.action is FakeAction.AMBIGUOUS_ROW and puts:
            puts[0] = {**puts[0], "strike": puts[0]["strike"] + 1}
        elif step.action is FakeAction.SCHEMA_CHANGED:
            calls = [_rename(r, "bid", "bidPrice") for r in calls]
            puts = [_rename(r, "bid", "bidPrice") for r in puts]
        body = _dumps({"schema": _CHAIN_SCHEMA, "underlying": symbol,
                       "expiration": request.expiration_date.isoformat(),
                       "calls": calls, "puts": puts})
        return self._parse_chain(request, body, started, completed, self._response_id(),
                                 self.scenario.parser_version)

    def fetch_actions(self, request: ActionRequest) -> FetchResult[CorporateActionObservation]:
        self._check_provider(request.provider_id)
        started, completed, step = self._begin("actions", request.deadline_at)
        rows: list[CorporateActionObservation] = []
        if step.action is not FakeAction.EMPTY:
            for spec in self.scenario.actions:
                if (spec.symbol == request.instrument.symbol
                        and request.start_date <= spec.effective_date < request.end_date
                        and completed >= spec.visible_from):
                    rows.append(CorporateActionObservation(
                        instrument=request.instrument,
                        action_type=spec.action_type,  # type: ignore[arg-type]
                        effective_date=spec.effective_date,
                        ratio=spec.ratio,
                        cash_amount=spec.cash_amount,
                        currency="USD" if spec.cash_amount is not None else None,
                    ))
        body = _dumps({"schema": "fake-actions/1", "symbol": request.instrument.symbol,
                       "rows": [r.model_dump(mode="json") for r in rows]})
        return FetchResult[CorporateActionObservation](
            provider_id=FAKE_PROVIDER_ID,
            response_id=self._response_id(),
            correlation_id=request.correlation_id,
            fetch_started_at=started,
            fetch_completed_at=completed,
            status=FetchStatus.COMPLETE if rows else FetchStatus.EMPTY,
            rows=tuple(rows),
            coverage=Coverage(rows_before_filters=len(rows), rows_after_filters=len(rows),
                              empty_reason=None if rows else "no actions in window"),
            payload=self._payload_ref(body, self.scenario.parser_version),
            payload_body=body,
            request_counts=_ONE_REQUEST,
        )

    # -- Parser corrections ---------------------------------------------------

    def reparse_chain(
        self,
        original: FetchResult[OptionObservation],
        request: ChainRequest,
        parser_version: str,
    ) -> FetchResult[OptionObservation]:
        """Re-run a different parser over the *same* retained payload.

        Response identity and fetch times are unchanged: this is a parser
        correction of one response, not a new observation. No request is made.
        """
        if original.payload_body is None or original.payload is None:
            raise ValueError("original result has no retained payload body")
        if parser_version not in PARSER_VERSIONS:
            raise ValueError(f"unknown parser_version {parser_version!r}")
        result = self._parse_chain(request, original.payload_body, original.fetch_started_at,
                                   original.fetch_completed_at, original.response_id,
                                   parser_version)
        return result.model_copy(update={"request_counts": RequestCounts(
            endpoint_requests=0, transport_requests=0)})

    # -- Internals: scheduling/script ------------------------------------------

    def _check_provider(self, provider_id: str) -> None:
        if provider_id != FAKE_PROVIDER_ID:
            raise ValueError(f"request for provider {provider_id!r} sent to fake provider")

    def _begin(self, op: str, deadline: datetime) -> tuple[datetime, datetime, ScriptStep]:
        steps = self.scenario.script.get(op, ())
        index = self._calls[op]
        self._calls[op] += 1
        step = steps[index] if index < len(steps) else ScriptStep(FakeAction.OK)
        started = self.clock.now()
        completed = self.clock.advance(self.scenario.latency + step.extra_latency)
        counts = _ONE_REQUEST
        if completed > deadline:
            raise Unavailable("deadline exceeded", request_counts=counts,
                              details={"deadline_at": deadline.isoformat()})
        if step.action is FakeAction.RATE_LIMITED:
            raise RateLimited(retry_after=step.retry_after, request_counts=counts)
        if step.action is FakeAction.UNAUTHORIZED:
            raise Unauthorized("access denied (scripted)", request_counts=counts)
        if step.action is FakeAction.UNAVAILABLE:
            raise Unavailable("upstream unavailable (scripted)", request_counts=counts)
        return started, completed, step

    def _response_id(self) -> UUID:
        # Includes the clock so a restarted fake never reissues an earlier response's ID
        # (that would make a new response look like a replay). Still deterministic.
        self._responses += 1
        digest = _digest(self.scenario.seed, "response", self._responses,
                         self.clock.now().isoformat(), size=16)
        return UUID(bytes=digest, version=4)

    def _payload_ref(self, body: bytes, parser_version: str) -> PayloadRef:
        return PayloadRef(
            sha256=hashlib.sha256(body).hexdigest(),
            byte_count=len(body),
            representation=PayloadRepresentation.SYNTHETIC,
            parser_version=parser_version,
        )

    # -- Internals: synthetic market ------------------------------------------

    def _unit(self, *parts: object) -> float:
        return int.from_bytes(_digest(self.scenario.seed, *parts, size=8)) / 2**64

    def _base(self, symbol: str) -> float:
        return 80.0 + 400.0 * self._unit("base", symbol)

    def _level(self, symbol: str, at: datetime) -> float:
        minutes = at.timestamp() / 60.0
        base = self._base(symbol)
        drift = 0.01 * math.sin(minutes / 390.0 * 2 * math.pi * 0.37)
        wave = 0.003 * math.sin(minutes / 37.0)
        noise = 0.0008 * (self._unit("noise", symbol, math.floor(minutes)) - 0.5)
        return base * (1.0 + drift + wave + noise)

    def _spot(self, symbol: str, at: datetime) -> Decimal:
        epoch = self.scenario.price_epoch.total_seconds()
        epoch_start = datetime.fromtimestamp(math.floor(at.timestamp() / epoch) * epoch, tz=UTC)
        return _cents(self._level(symbol, epoch_start))

    def _expirations(self, session: date) -> tuple[date, ...]:
        # Two inside 30-90 DTE plus one shorter and one longer to exercise selection.
        return tuple(sorted({_next_friday(session + timedelta(days=d)) for d in (7, 35, 63, 120)}))

    def _bar_rows(
        self, request: BarRequest, completed: datetime, call_index: int
    ) -> Iterator[dict[str, Any]]:
        symbol = request.instrument.symbol
        revisions = {
            r.interval_start.astimezone(UTC): r.close_delta
            for r in self.scenario.bar_revisions
            if call_index >= r.from_call
        }
        for start, end in _intervals(request.timeframe, request.start, request.end):
            if end > completed or start in self.scenario.omitted_minutes:
                continue  # unclosed bars are never returned by the fake
            minutes = list(_minute_starts(start, end))
            levels = [self._level(symbol, m) for m in minutes] + [self._level(symbol, end)]
            o, c = _cents(levels[0]), _cents(levels[-1])
            span = self._base(symbol) * 0.0004
            h = max(_cents(max(levels) + span * self._unit("h", symbol, start)), o, c)
            low = min(_cents(min(levels) - span * self._unit("l", symbol, start)), o, c)
            volume = sum(1000 + int(5000 * self._unit("v", symbol, m)) for m in minutes)
            if start in revisions:
                c += revisions[start]
                h, low = max(h, c), min(low, c)
            yield {"t": int(start.timestamp()), "o": float(o), "h": float(h), "l": float(low),
                   "c": float(c), "v": volume}

    def _chain_rows(
        self, symbol: str, expiration: date, completed: datetime
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        spot = self._spot(symbol, completed)
        step = Decimal(1) if spot < 200 else Decimal(5)
        atm = (spot / step).to_integral_value() * step
        n = self.scenario.strikes_per_side
        strikes = [atm + step * (k - n // 2) for k in range(n)]
        epoch = math.floor(completed.timestamp() / self.scenario.price_epoch.total_seconds())
        epoch_start = datetime.fromtimestamp(
            epoch * self.scenario.price_epoch.total_seconds(), tz=UTC)
        years = max((expiration - _ny_date(completed)).days, 1) / 365.0
        calls: list[dict[str, Any]] = []
        puts: list[dict[str, Any]] = []
        for index, strike in enumerate(strikes):
            for side, out in ((OptionType.CALL, calls), (OptionType.PUT, puts)):
                out.append(self._contract_row(symbol, expiration, side, strike, spot, years,
                                              epoch, epoch_start, index))
        return calls, puts

    def _contract_row(
        self, symbol: str, expiration: date, side: OptionType, strike: Decimal, spot: Decimal,
        years: float, epoch: int, epoch_start: datetime, index: int,
    ) -> dict[str, Any]:
        s, k = float(spot), float(strike)
        moneyness = math.log(k / s)
        vol = 0.18 + 0.08 * abs(moneyness) + 0.01 * self._unit("iv", symbol, expiration, side,
                                                                strike, epoch)
        intrinsic = max(s - k, 0.0) if side is OptionType.CALL else max(k - s, 0.0)
        time_value = s * vol * math.sqrt(years) * 0.4 * math.exp(
            -(moneyness**2) / (2 * vol * vol * years))
        mid = intrinsic + time_value
        half = max(0.01, mid * 0.02)
        bid: float | None = float(_cents(max(mid - half, 0.0)))
        ask: float | None = float(_cents(mid + half))
        oi: int | None = 100 + int(5000 * self._unit("oi", symbol, expiration, side, strike))
        volume: int | None = int(800 * self._unit("vol", symbol, expiration, side, strike, epoch))
        traded = epoch_start - timedelta(minutes=1 + int(30 * self._unit("lt", symbol, strike)))
        last_trade: int | None = int(traded.timestamp())
        iv = round(vol, 6)
        if self.scenario.anomalies:
            put = side is OptionType.PUT
            if index == 0 and put:
                bid = 0.0  # zero bid
            if index == self.scenario.strikes_per_side - 1 and not put:
                bid = None  # missing bid
            if index == 1 and not put and ask is not None:
                bid = ask  # locked
            if index == 2 and put and ask is not None:
                bid = round(ask + 0.05, 2)  # crossed
            if index == 6 and put:
                bid = -0.05  # negative quote stored as observed
            if index == 3 and not put:
                iv = 12.5  # implausible IV (1250%)
            if index == 4:
                volume = None if put else 0  # null vs zero volume
            if index == 5:
                oi = None if put else 0  # null vs zero open interest
            if index == 7 and not put:
                last_trade = int((epoch_start - timedelta(days=10)).timestamp())  # stale trade
            if index == 7 and put:
                last_trade = None
        row: dict[str, Any] = {
            "contractSymbol": _occ(symbol, expiration, side, strike),
            "strike": float(strike),
            "bid": bid,
            "ask": ask,
            "lastPrice": float(_cents(mid)),
            "lastTradeDate": last_trade,
            "impliedVolatility": iv,
            "volume": volume,
            "openInterest": oi,
            "inTheMoney": intrinsic > 0,
        }
        if self.scenario.quote_event_times:
            row["quoteTimeNs"] = int(epoch_start.timestamp()) * 1_000_000_000 + 123_456_789
        return row

    # -- Internals: parsers -----------------------------------------------------

    def _parse_chain(
        self, request: ChainRequest, body: bytes, started: datetime, completed: datetime,
        response_id: UUID, parser_version: str,
    ) -> FetchResult[OptionObservation]:
        doc = _loads(body)
        sides = {OptionType.CALL: doc["calls"], OptionType.PUT: doc["puts"]}
        all_rows = [r for rows in sides.values() for r in rows]
        if all_rows and not all(set(r) >= _CHAIN_REQUIRED for r in all_rows):
            seen = sorted({k for r in all_rows for k in r})
            raise SchemaChanged(
                "chain rows missing required fields",
                request_counts=_ONE_REQUEST,
                details={"fingerprint": _fingerprint(_CHAIN_SCHEMA, seen),
                         "missing": ",".join(sorted(_CHAIN_REQUIRED - set(seen)))},
            )
        semantics = FieldSemantics(
            quote_time_basis=(QuoteTimeBasis.PROVIDER_EVENT if self.scenario.quote_event_times
                              else QuoteTimeBasis.UNKNOWN),
            volume_kind=VolumeKind.SESSION_CUMULATIVE,
            volume_date_basis=VolumeDateBasis.INFERRED_SESSION,
            size_units=SizeUnits.UNKNOWN,
            mapping_version=parser_version,
        )
        observations: list[OptionObservation] = []
        rejected: list[RejectedRow] = []
        position = 0
        for side, rows in sides.items():
            for raw in rows:
                try:
                    observations.append(self._chain_observation(
                        request, side, raw, completed, semantics, parser_version))
                except _Reject as rej:
                    rejected.append(RejectedRow(reason=rej.reason, detail=rej.detail,
                                                row_index=position,
                                                source_key=_source_key(raw)))
                position += 1
        observed = tuple(s for s in sides if any(o.contract.option_type is s for o in observations))
        fingerprint = _fingerprint(_CHAIN_SCHEMA, sorted({k for r in all_rows for k in r}))
        coverage = Coverage(
            requested_expiries=(request.expiration_date,),
            returned_expiries=(request.expiration_date,) if observations else (),
            expected_sides=request.expected_sides,
            observed_sides=observed,
            rows_before_filters=len(all_rows),
            rows_after_filters=len(observations),
            exclusions_by_reason=_count_reasons(rejected),
            scope_complete=(self.scenario.completeness_known and not rejected
                            and set(request.expected_sides) <= set(observed)),
            completeness_confidence=(CompletenessConfidence.KNOWN
                                     if self.scenario.completeness_known
                                     else CompletenessConfidence.UNKNOWN),
            empty_reason=None if all_rows else "provider returned no rows for expiration",
            source_schema_fingerprint=fingerprint,
        )
        status, failures = _status(bool(observations), rejected, coverage.missing_sides)
        return FetchResult[OptionObservation](
            provider_id=FAKE_PROVIDER_ID,
            response_id=response_id,
            correlation_id=request.correlation_id,
            fetch_started_at=started,
            fetch_completed_at=completed,
            status=status,
            rows=tuple(observations),
            rejected=tuple(rejected),
            coverage=coverage,
            payload=self._payload_ref(body, parser_version),
            payload_body=body,
            source_fingerprint=fingerprint,
            request_counts=_ONE_REQUEST,
            failures=failures,
        )

    def _chain_observation(
        self, request: ChainRequest, side: OptionType, raw: Mapping[str, Any],
        completed: datetime, semantics: FieldSemantics, parser_version: str,
    ) -> OptionObservation:
        if any(isinstance(v, _NonFinite) for v in raw.values()):
            raise _Reject(RejectReason.NON_FINITE, "non-finite numeric value")
        symbol = raw.get("contractSymbol")
        if not isinstance(symbol, str) or not symbol:
            raise _Reject(RejectReason.MISSING_IDENTITY, "contractSymbol missing")
        event_at: datetime | None = None
        event_ns: int | None = None
        if self.scenario.quote_event_times and raw.get("quoteTimeNs") is not None:
            event_ns = int(raw["quoteTimeNs"])
            event_at = _EPOCH + timedelta(microseconds=event_ns // 1_000)
        try:
            contract = ContractRef(
                provider_id=FAKE_PROVIDER_ID,
                provider_symbol=symbol,
                underlying_symbol=request.instrument.symbol,
                option_type=side,
                strike=_decimal(raw["strike"]),
                expiration_date=request.expiration_date,
            )
            if occ_mismatches(contract):
                raise _Reject(RejectReason.AMBIGUOUS_CONTRACT,
                              f"symbol disagrees on {','.join(occ_mismatches(contract))}")
            last_trade = raw.get("lastTradeDate")
            return OptionObservation(
                contract=contract,
                observed_at=completed,
                provider_event_at=event_at,
                provider_event_ns=event_ns,
                last_trade_at=(datetime.fromtimestamp(last_trade, tz=UTC)
                               if last_trade is not None else None),
                bid=_decimal_or_none(raw.get("bid")),
                ask=_decimal_or_none(raw.get("ask")),
                last=_decimal_or_none(raw.get("lastPrice")),
                reported_iv=_float_or_none(raw.get("impliedVolatility")),
                reported_volume=raw.get("volume"),
                reported_open_interest=raw.get("openInterest"),
                volume_session_date=_ny_date(completed),
                in_the_money=raw.get("inTheMoney") if parser_version != PARSER_V1 else None,
                field_semantics=semantics,
            )
        except (ValidationError, InvalidOperation, TypeError, ValueError) as exc:
            raise _Reject(RejectReason.MALFORMED, _short(exc)) from None

    def _parse_bars(
        self, request: BarRequest, body: bytes, started: datetime, completed: datetime,
        response_id: UUID, parser_version: str,
    ) -> FetchResult[BarObservation]:
        doc = _loads(body)
        raw_rows: list[dict[str, Any]] = doc["rows"]
        if raw_rows and not all(set(r) >= _BAR_REQUIRED for r in raw_rows):
            seen = sorted({k for r in raw_rows for k in r})
            raise SchemaChanged(
                "bar rows missing required fields",
                request_counts=_ONE_REQUEST,
                details={"fingerprint": _fingerprint(_BARS_SCHEMA, seen),
                         "missing": ",".join(sorted(_BAR_REQUIRED - set(seen)))},
            )
        bars: list[BarObservation] = []
        rejected: list[RejectedRow] = []
        width = _TIMEFRAME_WIDTH.get(request.timeframe)
        for position, raw in enumerate(raw_rows):
            try:
                bars.append(self._bar_observation(request, raw, width))
            except _Reject as rej:
                rejected.append(RejectedRow(reason=rej.reason, detail=rej.detail,
                                            row_index=position, source_key=str(raw.get("t"))))
        fingerprint = _fingerprint(_BARS_SCHEMA, sorted({k for r in raw_rows for k in r}))
        coverage = Coverage(
            requested_start=request.start,
            requested_end=request.end,
            rows_before_filters=len(raw_rows),
            rows_after_filters=len(bars),
            exclusions_by_reason=_count_reasons(rejected),
            # Without an exchange calendar the fake cannot prove minute completeness.
            completeness_confidence=CompletenessConfidence.UNKNOWN,
            empty_reason=None if raw_rows else "no closed synthetic session bars in window",
            source_schema_fingerprint=fingerprint,
        )
        status, failures = _status(bool(bars), rejected, ())
        return FetchResult[BarObservation](
            provider_id=FAKE_PROVIDER_ID,
            response_id=response_id,
            correlation_id=request.correlation_id,
            fetch_started_at=started,
            fetch_completed_at=completed,
            status=status,
            rows=tuple(bars),
            rejected=tuple(rejected),
            coverage=coverage,
            payload=self._payload_ref(body, parser_version),
            payload_body=body,
            source_fingerprint=fingerprint,
            request_counts=_ONE_REQUEST,
            failures=failures,
        )

    def _bar_observation(
        self, request: BarRequest, raw: Mapping[str, Any], width: timedelta | None
    ) -> BarObservation:
        if any(isinstance(v, _NonFinite) for v in raw.values()):
            raise _Reject(RejectReason.NON_FINITE, "non-finite numeric value")
        try:
            start = datetime.fromtimestamp(raw["t"], tz=UTC)
            o, h, low, c = (_decimal(raw[k]) for k in ("o", "h", "l", "c"))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise _Reject(RejectReason.MALFORMED, _short(exc)) from None
        if not (low <= h and low <= o <= h and low <= c <= h):
            raise _Reject(RejectReason.INVALID_OHLC, "OHLC invariant violated")
        if width is None:
            session = _ny_date(start)
            end = datetime.combine(session, SESSION_CLOSE, NEW_YORK).astimezone(UTC)
        else:
            end = start + width
        try:
            return BarObservation(
                instrument=request.instrument,
                timeframe=request.timeframe,
                interval_start=start,
                interval_end=end,
                session_date=_ny_date(start),
                session_kind=SessionKind.RTH,
                price_basis=PriceBasis.PROVIDER_NATIVE,
                open=o, high=h, low=low, close=c,
                volume=raw["v"],
            )
        except ValidationError as exc:
            raise _Reject(RejectReason.MALFORMED, _short(exc)) from None


# -- Module helpers ---------------------------------------------------------------

_ONE_REQUEST = RequestCounts(endpoint_requests=1, transport_requests=1)
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_TIMEFRAME_WIDTH = {Timeframe.M1: timedelta(minutes=1)}


class _Reject(Exception):
    def __init__(self, reason: RejectReason, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


def _digest(seed: int, *parts: object, size: int) -> bytes:
    key = "|".join([str(seed), *map(str, parts)]).encode()
    return hashlib.blake2b(key, digest_size=size).digest()


def _dumps(doc: Mapping[str, Any]) -> bytes:
    # allow_nan=True on purpose: the fake must be able to emit NaN for parser tests.
    return json.dumps(doc, sort_keys=True, separators=(",", ":")).encode()


def _loads(body: bytes) -> dict[str, Any]:
    loaded = json.loads(body, parse_float=Decimal, parse_constant=_NonFinite)
    return cast(dict[str, Any], loaded)


def _fingerprint(schema: str, keys: Any) -> str:
    joined = ",".join(sorted(str(k) for k in keys))
    return f"{schema}:{hashlib.sha256(joined.encode()).hexdigest()[:16]}"


def _rename(row: Mapping[str, Any], old: str, new: str) -> dict[str, Any]:
    return {(new if k == old else k): v for k, v in row.items()}


def _cents(value: float) -> Decimal:
    return Decimal(f"{value:.2f}")


def _decimal(value: Any) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, Decimal | int):
        raise ValueError(f"expected a JSON number, got {type(value).__name__}")
    return Decimal(value)


def _decimal_or_none(value: Any) -> Decimal | None:
    return None if value is None else _decimal(value)


def _float_or_none(value: Any) -> float | None:
    return None if value is None else float(_decimal(value))


def _short(exc: BaseException) -> str:
    return str(exc).splitlines()[0][:200] if str(exc) else type(exc).__name__


def _source_key(raw: Mapping[str, Any]) -> str | None:
    key = raw.get("contractSymbol")
    return key if isinstance(key, str) else None


def _count_reasons(rejected: list[RejectedRow]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rejected:
        counts[row.reason.value] = counts.get(row.reason.value, 0) + 1
    return counts


def _status(
    has_rows: bool, rejected: list[RejectedRow], missing_sides: tuple[OptionType, ...]
) -> tuple[FetchStatus, tuple[ProviderFailure, ...]]:
    if not has_rows:
        return FetchStatus.EMPTY, ()
    details: dict[str, str] = {}
    if missing_sides:
        details["missing_sides"] = ",".join(s.value for s in missing_sides)
    if rejected:
        details["rejected_rows"] = str(len(rejected))
    if not details:
        return FetchStatus.COMPLETE, ()
    failure = ProviderFailure(code=FailureCode.PARTIAL_RESULT, message="partial response",
                              retryable=False, details=details)
    return FetchStatus.PARTIAL, (failure,)


def _ny_date(at: datetime) -> date:
    return at.astimezone(NEW_YORK).date()


def _next_friday(day: date) -> date:
    return day + timedelta(days=(4 - day.weekday()) % 7)


def _occ(symbol: str, expiration: date, side: OptionType, strike: Decimal) -> str:
    return f"{symbol}{expiration:%y%m%d}{side.value}{int(strike * 1000):08d}"


def _sessions(start: datetime, end: datetime) -> Iterator[tuple[datetime, datetime]]:
    day = _ny_date(start)
    while day <= _ny_date(end):
        if day.weekday() < 5:
            yield (datetime.combine(day, SESSION_OPEN, NEW_YORK).astimezone(UTC),
                   datetime.combine(day, SESSION_CLOSE, NEW_YORK).astimezone(UTC))
        day += timedelta(days=1)


def _minute_starts(start: datetime, end: datetime) -> Iterator[datetime]:
    current = start
    while current < end:
        yield current
        current += timedelta(minutes=1)


def _intervals(
    timeframe: Timeframe, start: datetime, end: datetime
) -> Iterator[tuple[datetime, datetime]]:
    for open_at, close_at in _sessions(start, end):
        if timeframe is Timeframe.D1:
            if start <= open_at < end:
                yield open_at, close_at
            continue
        for minute in _minute_starts(max(open_at, _ceil_minute(start)), min(close_at, end)):
            yield minute, minute + timedelta(minutes=1)


def _ceil_minute(at: datetime) -> datetime:
    floored = at.replace(second=0, microsecond=0)
    return floored if floored == at else floored + timedelta(minutes=1)


__all__ = [
    "FAKE_PROVIDER_ID",
    "PARSER_V1",
    "PARSER_V2",
    "ActionSpec",
    "BarRevision",
    "FakeAction",
    "FakeClock",
    "FakeProvider",
    "FakeScenario",
    "ScriptStep",
    "fake_capabilities",
]
