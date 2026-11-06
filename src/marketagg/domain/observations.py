"""Canonical observations and fetch results (02 "Timestamp and publication contract").

Time fields keep their distinct meanings:

- ``fetch_started_at`` / ``fetch_completed_at``: server UTC around the operation.
- ``observed_at``: poll response completion (stream: receive time). Never a quote time.
- ``provider_event_at`` / ``provider_event_ns``: actual source quote time, if the
  source supplies one. Null otherwise; never derived from last-trade time or an
  advertised delay.
- ``last_trade_at``: reported last trade only.

``known_at`` and ``recorded_at`` are persistence facts (T03) and are deliberately
absent: an adapter cannot know when its output became durable.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, timedelta
from typing import Literal
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from marketagg.domain.enums import (
    CompletenessConfidence,
    FetchStatus,
    ObservationKind,
    OpenInterestDateBasis,
    OpenInterestKind,
    OptionType,
    PayloadRepresentation,
    PriceBasis,
    QualityFlag,
    QuoteTimeBasis,
    RejectReason,
    SessionKind,
    SizeUnits,
    Timeframe,
    VolumeDateBasis,
    VolumeKind,
)
from marketagg.domain.errors import FailureCode, ProviderFailure, RequestCounts
from marketagg.domain.identifiers import ContractRef, ContractTermsObservation, InstrumentRef
from marketagg.domain.primitives import (
    SCHEMA_VERSION,
    CanonicalModel,
    FiniteFloat,
    NonNegativeInt,
    PositivePrice,
    ProviderId,
    Sha256Hex,
    SignedPrice,
    StrictInt,
    UtcDateTime,
)

_NS_PER_US = 1_000
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _sorted_flags(flags: tuple[QualityFlag, ...]) -> tuple[QualityFlag, ...]:
    return tuple(sorted(set(flags)))


class FieldSemantics(CanonicalModel):
    quote_time_basis: QuoteTimeBasis = QuoteTimeBasis.UNKNOWN
    volume_kind: VolumeKind = VolumeKind.UNKNOWN
    oi_kind: OpenInterestKind = OpenInterestKind.REPORTED_TOTAL
    volume_date_basis: VolumeDateBasis = VolumeDateBasis.UNKNOWN
    oi_date_basis: OpenInterestDateBasis = OpenInterestDateBasis.UNKNOWN
    size_units: SizeUnits = SizeUnits.UNKNOWN
    iv_units: Literal["decimal_annualized"] = "decimal_annualized"
    mapping_version: str = Field(min_length=1)


class OptionObservation(CanonicalModel):
    """One contract row in one poll/event/checkpoint. Nullable means "not reported"."""

    contract: ContractRef
    terms: ContractTermsObservation | None = None
    observation_kind: ObservationKind = ObservationKind.POLL
    observed_at: UtcDateTime
    provider_event_at: UtcDateTime | None = None
    provider_event_ns: StrictInt | None = None
    provider_sequence: str | None = None
    last_trade_at: UtcDateTime | None = None
    bid: SignedPrice | None = None
    ask: SignedPrice | None = None
    last: SignedPrice | None = None
    bid_size: StrictInt | None = None
    ask_size: StrictInt | None = None
    reported_iv: FiniteFloat | None = None
    reported_volume: StrictInt | None = None
    reported_open_interest: StrictInt | None = None
    volume_session_date: date | None = None
    open_interest_asof_date: date | None = None
    in_the_money: bool | None = None
    field_semantics: FieldSemantics
    quality_flags: tuple[QualityFlag, ...] = ()

    _sort_flags = field_validator("quality_flags")(_sorted_flags)

    @model_validator(mode="after")
    def _event_time_consistency(self) -> OptionObservation:
        has_event = self.provider_event_at is not None
        basis_event = self.field_semantics.quote_time_basis is QuoteTimeBasis.PROVIDER_EVENT
        if has_event != basis_event:
            raise ValueError(
                "quote_time_basis=provider_event iff provider_event_at is present"
            )
        if self.provider_event_ns is not None:
            if self.provider_event_at is None:
                raise ValueError("provider_event_ns requires provider_event_at")
            epoch_us = (self.provider_event_at - _EPOCH) // timedelta(microseconds=1)
            if self.provider_event_ns // _NS_PER_US != epoch_us:
                raise ValueError("provider_event_ns disagrees with provider_event_at")
        if self.observation_kind is ObservationKind.EVENT and not has_event:
            raise ValueError("event observations require provider_event_at")
        return self


class BarObservation(CanonicalModel):
    """A provider bar. ``complete``/revision state is decided by collectors (T08/T09)."""

    instrument: InstrumentRef
    timeframe: Timeframe
    interval_start: UtcDateTime
    interval_end: UtcDateTime
    session_date: date
    session_kind: SessionKind
    price_basis: PriceBasis
    open: PositivePrice
    high: PositivePrice
    low: PositivePrice
    close: PositivePrice
    volume: NonNegativeInt | None = None
    quality_flags: tuple[QualityFlag, ...] = ()

    _sort_flags = field_validator("quality_flags")(_sorted_flags)

    @model_validator(mode="after")
    def _invariants(self) -> BarObservation:
        if self.interval_end <= self.interval_start:
            raise ValueError("interval_end must be after interval_start")
        if self.timeframe is Timeframe.M1 and (
            self.interval_end - self.interval_start != timedelta(minutes=1)
        ):
            raise ValueError("1m bars must span exactly one minute")
        if not (
            self.low <= self.high
            and self.low <= self.open <= self.high
            and self.low <= self.close <= self.high
        ):
            raise ValueError("OHLC invariant violated (low <= open/close <= high)")
        return self


class CorporateActionObservation(CanonicalModel):
    """A split/dividend/etc. as reported by a source; corrections supersede (02)."""

    instrument: InstrumentRef
    action_type: Literal["split", "dividend", "symbol_change", "delisting",
                         "deliverable_change"]
    effective_date: date
    announced_at: UtcDateTime | None = None
    ratio: PositivePrice | None = None  # split: new shares per old share
    cash_amount: PositivePrice | None = None
    currency: str | None = None
    details: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _fields_match_type(self) -> CorporateActionObservation:
        if self.action_type == "split" and self.ratio is None:
            raise ValueError("split requires ratio")
        if self.action_type == "dividend" and (self.cash_amount is None or not self.currency):
            raise ValueError("dividend requires cash_amount and currency")
        return self


class RejectedRow(CanonicalModel):
    """A source row that could not become canonical. Kept for quality incidents."""

    reason: RejectReason
    detail: str = Field(max_length=500)
    row_index: NonNegativeInt | None = None
    source_key: str | None = Field(default=None, max_length=128)


class Coverage(CanonicalModel):
    """What a response covered. HTTP success never proves full completeness (02)."""

    requested_expiries: tuple[date, ...] = ()
    returned_expiries: tuple[date, ...] = ()
    expected_sides: tuple[OptionType, ...] = ()
    observed_sides: tuple[OptionType, ...] = ()
    requested_start: UtcDateTime | None = None
    requested_end: UtcDateTime | None = None
    rows_before_filters: NonNegativeInt = 0
    rows_after_filters: NonNegativeInt = 0
    exclusions_by_reason: dict[str, NonNegativeInt] = Field(default_factory=dict)
    discovery_at: UtcDateTime | None = None
    scope_complete: bool = False
    completeness_confidence: CompletenessConfidence = CompletenessConfidence.UNKNOWN
    empty_reason: str | None = None
    source_schema_fingerprint: str | None = None

    @model_validator(mode="after")
    def _consistency(self) -> Coverage:
        if self.rows_after_filters > self.rows_before_filters:
            raise ValueError("rows_after_filters exceeds rows_before_filters")
        if self.scope_complete:
            if self.completeness_confidence is not CompletenessConfidence.KNOWN:
                raise ValueError("scope_complete requires completeness_confidence=known")
            if set(self.expected_sides) - set(self.observed_sides):
                raise ValueError("scope_complete with missing sides")
        return self

    @property
    def missing_sides(self) -> tuple[OptionType, ...]:
        return tuple(s for s in self.expected_sides if s not in self.observed_sides)


class PayloadRef(CanonicalModel):
    """Retained payload identity. The checksum is never the identity of a poll."""

    sha256: Sha256Hex
    byte_count: NonNegativeInt
    representation: PayloadRepresentation
    parser_version: str = Field(min_length=1)


def _check_payload_body(ref: PayloadRef | None, body: bytes | None) -> None:
    if body is None:
        return
    if ref is None:
        raise ValueError("payload_body requires a payload reference")
    if len(body) != ref.byte_count or hashlib.sha256(body).hexdigest() != ref.sha256:
        raise ValueError("payload_body does not match payload sha256/byte_count")


class FetchResult[RowT: (BarObservation, OptionObservation, CorporateActionObservation)](
    CanonicalModel
):
    schema_version: int = SCHEMA_VERSION
    provider_id: ProviderId
    response_id: UUID
    correlation_id: str
    fetch_started_at: UtcDateTime
    fetch_completed_at: UtcDateTime
    status: FetchStatus
    rows: tuple[RowT, ...]
    rejected: tuple[RejectedRow, ...] = ()
    coverage: Coverage
    payload: PayloadRef | None = None
    # Retained bytes for the worker's private spool (03 step 3); never serialised.
    payload_body: bytes | None = Field(default=None, exclude=True, repr=False)
    source_fingerprint: str | None = None
    request_counts: RequestCounts
    advertised_delay_seconds: NonNegativeInt | None = None
    failures: tuple[ProviderFailure, ...] = ()

    @model_validator(mode="after")
    def _consistency(self) -> FetchResult[RowT]:
        if self.fetch_completed_at < self.fetch_started_at:
            raise ValueError("fetch_completed_at precedes fetch_started_at")
        _check_payload_body(self.payload, self.payload_body)
        if (self.status is FetchStatus.EMPTY) != (not self.rows):
            raise ValueError("status=empty iff there are no rows")
        has_partial = any(f.code is FailureCode.PARTIAL_RESULT for f in self.failures)
        if self.status is FetchStatus.PARTIAL and not has_partial:
            raise ValueError("partial results must carry a partial_result failure")
        if self.status is FetchStatus.COMPLETE and (
            self.failures or self.rejected or self.coverage.missing_sides
        ):
            raise ValueError("complete results cannot have failures, rejects or missing sides")
        for row in self.rows:
            if isinstance(row, OptionObservation):
                if row.contract.provider_id != self.provider_id:
                    raise ValueError("row contract belongs to a different provider")
                if (
                    row.observation_kind is ObservationKind.POLL
                    and row.observed_at != self.fetch_completed_at
                ):
                    raise ValueError("poll observed_at must equal fetch_completed_at")
            elif isinstance(row, CorporateActionObservation | BarObservation) and (
                    row.instrument.provider_id != self.provider_id):
                raise ValueError("row instrument belongs to a different provider")
        return self


class DiscoveryResult(CanonicalModel):
    schema_version: int = SCHEMA_VERSION
    provider_id: ProviderId
    response_id: UUID
    correlation_id: str
    instrument: InstrumentRef
    fetch_started_at: UtcDateTime
    fetch_completed_at: UtcDateTime
    expirations: tuple[date, ...]
    underlying_reference_price: PositivePrice | None = None
    payload: PayloadRef | None = None
    payload_body: bytes | None = Field(default=None, exclude=True, repr=False)
    source_fingerprint: str | None = None
    request_counts: RequestCounts

    @model_validator(mode="after")
    def _consistency(self) -> DiscoveryResult:
        if self.fetch_completed_at < self.fetch_started_at:
            raise ValueError("fetch_completed_at precedes fetch_started_at")
        _check_payload_body(self.payload, self.payload_body)
        if list(self.expirations) != sorted(set(self.expirations)):
            raise ValueError("expirations must be sorted and distinct")
        return self

