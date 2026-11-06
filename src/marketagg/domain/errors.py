"""Typed provider failures (02 "Provider and repository Python interfaces").

Operations that produce no usable rows raise a ``ProviderError`` subclass. A
response with some valid rows is never an exception: it is a ``FetchResult`` with
``status=partial`` and a ``ProviderFailure(code=PARTIAL_RESULT)`` entry. Every
error carries the requests it consumed so budgets count failed calls too.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import Field

from marketagg.domain.primitives import CanonicalModel, NonNegativeInt


class FailureCode(StrEnum):
    RATE_LIMITED = "rate_limited"
    UNAUTHORIZED = "unauthorized"
    UNAVAILABLE = "unavailable"
    UNSUPPORTED_CAPABILITY = "unsupported_capability"
    SCHEMA_CHANGED = "schema_changed"
    PARTIAL_RESULT = "partial_result"


class RequestCounts(CanonicalModel):
    """``transport_requests=None`` means hidden calls could not be counted (03)."""

    endpoint_requests: NonNegativeInt = 0
    transport_requests: NonNegativeInt | None = None


class ProviderFailure(CanonicalModel):
    code: FailureCode
    message: str = Field(max_length=2000)
    retryable: bool
    retry_after_seconds: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    details: dict[str, str] = Field(default_factory=dict)


class ProviderError(Exception):
    code: FailureCode
    retryable: bool = False

    def __init__(
        self,
        message: str,
        *,
        request_counts: RequestCounts | None = None,
        details: dict[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.request_counts = request_counts or RequestCounts()
        self.details = details or {}

    @property
    def retry_after_seconds(self) -> float | None:
        return None

    def to_failure(self) -> ProviderFailure:
        return ProviderFailure(
            code=self.code,
            message=self.message,
            retryable=self.retryable,
            retry_after_seconds=self.retry_after_seconds,
            details=self.details,
        )


class RateLimited(ProviderError):
    code = FailureCode.RATE_LIMITED
    retryable = True

    def __init__(
        self,
        message: str = "rate limited",
        *,
        retry_after: float | None = None,
        request_counts: RequestCounts | None = None,
        details: dict[str, str] | None = None,
    ) -> None:
        super().__init__(message, request_counts=request_counts, details=details)
        self.retry_after = retry_after

    @property
    def retry_after_seconds(self) -> float | None:
        return self.retry_after


class Unauthorized(ProviderError):
    """Access denied. Disables the provider; never retried or circumvented."""

    code = FailureCode.UNAUTHORIZED


class Unavailable(ProviderError):
    """Transient transport/5xx/timeout failure."""

    code = FailureCode.UNAVAILABLE
    retryable = True


class UnsupportedCapability(ProviderError):
    code = FailureCode.UNSUPPORTED_CAPABILITY

    def __init__(
        self,
        capability: str,
        message: str | None = None,
        *,
        request_counts: RequestCounts | None = None,
    ) -> None:
        super().__init__(
            message or f"capability not supported: {capability}",
            request_counts=request_counts,
            details={"capability": capability},
        )
        self.capability = capability


class SchemaChanged(ProviderError):
    """Response shape drifted. Opens the endpoint circuit; no aggressive retry."""

    code = FailureCode.SCHEMA_CHANGED
