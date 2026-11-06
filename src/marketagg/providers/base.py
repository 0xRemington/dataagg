"""Provider boundary (02). Adapters return canonical types or raise ``ProviderError``.

Adapters never write SQL, assign internal IDs, decide research eligibility, or
retry beyond what their transport documents; scheduling and publication belong
to the worker.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from marketagg.domain.capabilities import ProviderCapabilities
from marketagg.domain.observations import (
    BarObservation,
    CorporateActionObservation,
    DiscoveryResult,
    FetchResult,
    OptionObservation,
)
from marketagg.domain.requests import ActionRequest, BarRequest, ChainRequest, DiscoveryRequest


@runtime_checkable
class MarketDataProvider(Protocol):
    @property
    def provider_id(self) -> str: ...

    def capabilities(self) -> ProviderCapabilities: ...

    def discover(self, request: DiscoveryRequest) -> DiscoveryResult: ...

    def fetch_bars(self, request: BarRequest) -> FetchResult[BarObservation]: ...

    def fetch_chain(self, request: ChainRequest) -> FetchResult[OptionObservation]: ...


@runtime_checkable
class SupportsActions(Protocol):
    """Optional: corporate actions (splits/dividends) as reported by the source."""

    def fetch_actions(self, request: ActionRequest) -> FetchResult[CorporateActionObservation]: ...
