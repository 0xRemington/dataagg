"""Versioned provider capability declarations (07 "Capability negotiation").

Every capability is a ``CapabilityFact`` with status, confidence, evidence and
check time. The default is *unknown*: an adapter must cite evidence before it may
claim support or non-support. Nothing here grants data-use permission.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import Field, model_validator

from marketagg.domain.enums import (
    OpenInterestKind,
    PriceBasis,
    SessionKind,
    Timeframe,
    VolumeKind,
)
from marketagg.domain.primitives import (
    SCHEMA_VERSION,
    CanonicalModel,
    NonNegativeInt,
    ProviderId,
    UtcDateTime,
)


class CapabilityStatus(StrEnum):
    SUPPORTED = "supported"
    UNSUPPORTED = "unsupported"
    UNKNOWN = "unknown"


class Confidence(StrEnum):
    VERIFIED = "verified"  # contractual/entitlement confirmation
    OBSERVED = "observed"  # measured by a recorded probe
    DOCUMENTED = "documented"  # provider documentation or pinned source code
    SYNTHETIC = "synthetic"  # property of a fake/fixture provider only
    UNKNOWN = "unknown"


class CapabilityFact(CanonicalModel):
    status: CapabilityStatus = CapabilityStatus.UNKNOWN
    confidence: Confidence = Confidence.UNKNOWN
    evidence: str | None = Field(default=None, max_length=2000)
    checked_at: UtcDateTime | None = None

    @model_validator(mode="after")
    def _claims_need_evidence(self) -> CapabilityFact:
        if self.status is not CapabilityStatus.UNKNOWN and (
            self.confidence is Confidence.UNKNOWN or not self.evidence or self.checked_at is None
        ):
            raise ValueError(
                f"status={self.status} requires confidence, evidence and checked_at"
            )
        return self

    @property
    def supported(self) -> bool:
        return self.status is CapabilityStatus.SUPPORTED


UNKNOWN_FACT = CapabilityFact()


class IntervalCapability(CanonicalModel):
    """Maximum request span and oldest retrievable age are separate facts (00)."""

    available: CapabilityFact = UNKNOWN_FACT
    max_request_span_seconds: NonNegativeInt | None = None
    oldest_history_seconds: NonNegativeInt | None = None
    limits: CapabilityFact = UNKNOWN_FACT


class OptionsCapabilities(CanonicalModel):
    current_chain: CapabilityFact = UNKNOWN_FACT
    historical_snapshots: CapabilityFact = UNKNOWN_FACT
    streaming_quotes: CapabilityFact = UNKNOWN_FACT


class QuoteCapabilities(CanonicalModel):
    event_time: CapabilityFact = UNKNOWN_FACT
    sizes: CapabilityFact = UNKNOWN_FACT
    sequence: CapabilityFact = UNKNOWN_FACT
    replay: CapabilityFact = UNKNOWN_FACT


class DelayKind(StrEnum):
    REALTIME = "realtime"
    DELAYED = "delayed"
    INDICATIVE = "indicative"
    SYNTHETIC = "synthetic"
    UNKNOWN = "unknown"


class TimestampPrecision(StrEnum):
    NANOSECOND = "ns"
    MICROSECOND = "us"
    MILLISECOND = "ms"
    SECOND = "s"
    UNKNOWN = "unknown"


class TimingCapabilities(CanonicalModel):
    """Advertised delay is metadata only; it never becomes a quote event time."""

    delay_kind: DelayKind = DelayKind.UNKNOWN
    advertised_delay_seconds: NonNegativeInt | None = None
    timestamp_precision: TimestampPrecision = TimestampPrecision.UNKNOWN
    evidence: CapabilityFact = UNKNOWN_FACT


class SemanticsCapabilities(CanonicalModel):
    adjustment_basis: PriceBasis | None = None
    session_coverage: tuple[SessionKind, ...] = ()
    volume_semantics: VolumeKind = VolumeKind.UNKNOWN
    oi_semantics: OpenInterestKind | None = None
    evidence: CapabilityFact = UNKNOWN_FACT


class ContractReferenceCapabilities(CanonicalModel):
    multiplier: CapabilityFact = UNKNOWN_FACT
    exercise_style: CapabilityFact = UNKNOWN_FACT
    deliverable: CapabilityFact = UNKNOWN_FACT
    expiry_cutoff: CapabilityFact = UNKNOWN_FACT


class Entitlements(CanonicalModel):
    """Recorded review of data-use rights. ``reviewed=False`` blocks live use."""

    reviewed: bool = False
    review_id: str | None = None
    review_expires_at: UtcDateTime | None = None
    display_allowed: bool | None = None
    export_allowed: bool | None = None
    automated_collection_allowed: bool | None = None
    max_retention_days: NonNegativeInt | None = None
    notes: str | None = Field(default=None, max_length=2000)

    @model_validator(mode="after")
    def _review_needs_id(self) -> Entitlements:
        if self.reviewed and not self.review_id:
            raise ValueError("reviewed entitlements require review_id")
        return self


class RequestLimits(CanonicalModel):
    """Provider-approved or measured limits; never invented global defaults."""

    requests_per_minute: NonNegativeInt | None = None
    max_concurrency: NonNegativeInt | None = None
    max_subscriptions: NonNegativeInt | None = None
    evidence: CapabilityFact = UNKNOWN_FACT


class ProviderCapabilities(CanonicalModel):
    schema_version: int = SCHEMA_VERSION
    provider_id: ProviderId
    capability_version: str = Field(min_length=1)
    reliability_tier: str = Field(min_length=1)
    synthetic: bool
    bars: dict[Timeframe, IntervalCapability] = Field(default_factory=dict)
    options: OptionsCapabilities = OptionsCapabilities()
    actions: CapabilityFact = UNKNOWN_FACT
    quotes: QuoteCapabilities = QuoteCapabilities()
    timing: TimingCapabilities = TimingCapabilities()
    semantics: SemanticsCapabilities = SemanticsCapabilities()
    contract_reference: ContractReferenceCapabilities = ContractReferenceCapabilities()
    entitlements: Entitlements = Entitlements()
    request_limits: RequestLimits = RequestLimits()

    def supports_timeframe(self, timeframe: Timeframe) -> bool:
        capability = self.bars.get(timeframe)
        return capability is not None and capability.available.supported
