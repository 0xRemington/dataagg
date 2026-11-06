"""HTTP API v1 response schemas (02 "HTTP API v1").

Decimals serialise as strings, timestamps as ISO 8601 UTC ``Z``; nulls never
become zero. Every data response carries schema version, request ID, source and
policy IDs, availability semantics, coverage and warnings.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from marketagg.domain.primitives import SignedPrice, UtcDateTime

API_SCHEMA_VERSION = 1


class _Out(BaseModel):
    model_config = ConfigDict(frozen=True)


class ErrorBody(_Out):
    code: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)
    request_id: str


class Availability(_Out):
    mode: Literal["observed", "recomputed"]
    decision_at: UtcDateTime
    label: str


class Source(_Out):
    provider_id: str
    policy_id: str | None = None
    price_basis: str | None = None
    session_kind: str | None = None


class Envelope(_Out):
    schema_version: int = API_SCHEMA_VERSION
    request_id: str
    source: Source
    availability: Availability
    coverage: dict[str, Any] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)
    next_cursor: str | None = None


class InstrumentOut(_Out):
    id: UUID
    symbol: str
    provider_id: str
    asset_class: str
    currency: str
    calendar_id: str
    status: str
    valid_from: date
    valid_to: date | None
    identity_known_at: UtcDateTime


class InstrumentsResponse(_Out):
    schema_version: int = API_SCHEMA_VERSION
    request_id: str
    items: list[InstrumentOut]


class ContractOut(_Out):
    id: UUID
    provider_symbol: str
    option_type: Literal["C", "P"]
    strike: SignedPrice
    expiration_date: date
    first_known_at: UtcDateTime
    terms_known: bool
    model_eligible: bool


class ContractsResponse(Envelope):
    items: list[ContractOut]


class BarOut(_Out):
    interval_start: UtcDateTime
    interval_end: UtcDateTime
    session_date: date
    duration_minutes: int
    label: str
    open: SignedPrice
    high: SignedPrice
    low: SignedPrice
    close: SignedPrice
    volume: int | None
    complete: bool
    expected_minutes: int | None
    observed_minutes: int | None
    is_short_session_bucket: bool
    quality_flags: list[str]
    known_at: UtcDateTime


class BarsResponse(Envelope):
    timeframe: str
    items: list[BarOut]


class OptionOut(_Out):
    observed_at: UtcDateTime
    known_at: UtcDateTime
    fetch_completed_at: UtcDateTime
    provider_event_at: UtcDateTime | None
    quote_time_basis: str
    last_trade_at: UtcDateTime | None
    bid: SignedPrice | None
    ask: SignedPrice | None
    midpoint: SignedPrice | None
    last: SignedPrice | None
    bid_size: int | None
    ask_size: int | None
    reported_iv: float | None
    reported_iv_origin: str | None
    reported_volume: int | None
    reported_open_interest: int | None
    quality_flags: list[str]
    record_revision: int


class OptionHistoryResponse(Envelope):
    contract_id: UUID
    items: list[OptionOut]


class PanelPointOut(_Out):
    bar: BarOut
    decision_at: UtcDateTime
    option: OptionOut | None
    spot_close: SignedPrice | None
    spot_interval_end: UtcDateTime | None
    option_observation_age_seconds: float | None
    quote_age_seconds: float | None
    spot_age_seconds: float | None
    freshness: Literal["known", "unknown"]
    execution_eligible: bool
    exclusions: list[str]


class PanelResponse(Envelope):
    timeframe: str
    contract_id: UUID | None
    sampling: str
    items: list[PanelPointOut]


class SurfaceResponse(Envelope):
    value: float | None
    diagnostics: dict[str, Any]


class QualityResponse(_Out):
    schema_version: int = API_SCHEMA_VERSION
    request_id: str
    report: dict[str, Any]


class DatasetRequestIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    instrument_id: UUID
    provider_id: str
    timeframe: Literal["1m", "5m", "15m", "4h", "1d"]
    start: UtcDateTime
    end: UtcDateTime
    decision_at: UtcDateTime
    mode: Literal["observed", "recomputed"] = "observed"
    contract_ids: list[UUID] = Field(default_factory=list, max_length=500)
    formats: list[Literal["csv", "parquet"]] = Field(default=["parquet"], min_length=1)


class DatasetJobOut(_Out):
    schema_version: int = API_SCHEMA_VERSION
    request_id: str
    id: UUID
    state: str
    manifest: dict[str, Any] | None = None
    downloads: dict[str, str] = Field(default_factory=dict)
    error: dict[str, Any] | None = None
