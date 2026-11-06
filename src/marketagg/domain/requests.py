"""Provider request contracts. Windows are half-open ``[start, end)``."""

from __future__ import annotations

from datetime import date

from pydantic import Field, model_validator

from marketagg.domain.enums import OptionType, SessionKind, Timeframe
from marketagg.domain.identifiers import InstrumentRef
from marketagg.domain.primitives import CanonicalModel, ProviderId, UtcDateTime


class ProviderRequest(CanonicalModel):
    provider_id: ProviderId
    policy_id: str = Field(min_length=1)
    deadline_at: UtcDateTime
    correlation_id: str = Field(min_length=1, max_length=128)
    instrument: InstrumentRef

    @model_validator(mode="after")
    def _same_provider(self) -> ProviderRequest:
        if self.instrument.provider_id != self.provider_id:
            raise ValueError("instrument reference belongs to a different provider")
        return self


class DiscoveryRequest(ProviderRequest):
    pass


class BarRequest(ProviderRequest):
    timeframe: Timeframe
    start: UtcDateTime
    end: UtcDateTime
    session_kind: SessionKind = SessionKind.RTH

    @model_validator(mode="after")
    def _window(self) -> BarRequest:
        if self.end <= self.start:
            raise ValueError("bar window must satisfy start < end (half-open)")
        return self


class ChainRequest(ProviderRequest):
    expiration_date: date
    expected_sides: tuple[OptionType, ...] = (OptionType.CALL, OptionType.PUT)

    @model_validator(mode="after")
    def _sides(self) -> ChainRequest:
        if not self.expected_sides or len(set(self.expected_sides)) != len(self.expected_sides):
            raise ValueError("expected_sides must be non-empty and distinct")
        return self


class ActionRequest(ProviderRequest):
    start_date: date
    end_date: date

    @model_validator(mode="after")
    def _window(self) -> ActionRequest:
        if self.end_date <= self.start_date:
            raise ValueError("action window must satisfy start_date < end_date")
        return self
