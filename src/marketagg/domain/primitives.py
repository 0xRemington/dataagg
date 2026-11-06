"""Value rules shared by every canonical model (02 "Migration contract").

- Datetimes must be timezone-aware; they are normalised to UTC and serialised as
  ISO 8601 with ``Z``. Naive input is rejected, never assumed to be UTC.
- Prices/strikes are ``Decimal`` within ``numeric(20,8)``. Binary floats are
  rejected so adapters must convert deliberately (e.g. ``Decimal(str(x))``).
- Floats (IV, model outputs) must be finite. NaN/inf are rejected, not nulled.
- Counts are strict ints (``True`` is not a volume).
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    PlainSerializer,
    Strict,
)

SCHEMA_VERSION = 1

PRICE_MAX_DIGITS = 20
PRICE_DECIMAL_PLACES = 8


def _require_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("naive datetime rejected; timezone-aware UTC is required")
    return value.astimezone(UTC)


def format_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


UtcDateTime = Annotated[
    datetime,
    AfterValidator(_require_aware_utc),
    PlainSerializer(format_utc, return_type=str, when_used="json"),
]


def _reject_float(value: Any) -> Any:
    if isinstance(value, float):
        raise ValueError("binary float rejected for a decimal price; pass Decimal or str")
    if isinstance(value, bool):
        raise ValueError("bool is not a price")
    return value


def _require_finite_decimal(value: Decimal) -> Decimal:
    if not value.is_finite():
        raise ValueError("non-finite decimal rejected")
    return value


def format_decimal(value: Decimal) -> str:
    """Canonical text: no exponent, no trailing zeros (``1.50`` -> ``1.5``)."""
    text = format(value.normalize(), "f")
    return "0" if text in {"-0", "0"} else text


_DecimalBase = Annotated[
    Decimal,
    BeforeValidator(_reject_float),
    Field(max_digits=PRICE_MAX_DIGITS, decimal_places=PRICE_DECIMAL_PLACES, allow_inf_nan=False),
    AfterValidator(_require_finite_decimal),
    PlainSerializer(format_decimal, return_type=str, when_used="json"),
]

# Signed: anomalous source values (e.g. negative bids) are stored and flagged, not clamped.
SignedPrice = _DecimalBase
PositivePrice = Annotated[_DecimalBase, Field(gt=0)]

FiniteFloat = Annotated[float, Field(allow_inf_nan=False)]
StrictInt = Annotated[int, Strict()]
NonNegativeInt = Annotated[int, Strict(), Field(ge=0)]

Sha256Hex = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
ProviderId = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_.-]{1,63}$")]
Symbol = Annotated[str, Field(min_length=1, max_length=64, pattern=r"^\S+$")]


class CanonicalModel(BaseModel):
    """Frozen, closed, versioned canonical data. No pandas/vendor types allowed."""

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        strict=False,
        allow_inf_nan=False,
        use_enum_values=False,
        validate_default=True,
    )

    def canonical_json(self) -> str:
        """Deterministic JSON: sorted keys, compact separators, canonical decimals."""
        return canonical_dumps(self.model_dump(mode="json"))

    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


def canonical_dumps(data: Any) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False)
