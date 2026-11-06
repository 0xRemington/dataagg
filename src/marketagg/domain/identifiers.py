"""Source-level identity as reported by a provider.

Adapters never assign internal UUIDs; persistence (T03) maps these references to
stable instrument/contract IDs with effective and knowledge intervals. OCC-style
parsing is a *consistency check*, never authoritative identity (01).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import Field, model_validator

from marketagg.domain.enums import ExerciseStyle, OptionType, SettlementStyle
from marketagg.domain.primitives import (
    SCHEMA_VERSION,
    CanonicalModel,
    PositivePrice,
    ProviderId,
    Symbol,
    UtcDateTime,
)


class InstrumentRef(CanonicalModel):
    provider_id: ProviderId
    symbol: Symbol
    exchange_mic: str | None = None


class ContractRef(CanonicalModel):
    """A provider's identification of one listed option, plus reported economics."""

    provider_id: ProviderId
    provider_symbol: Symbol
    underlying_symbol: Symbol
    option_type: OptionType
    strike: PositivePrice
    expiration_date: date


class DeliverableStatus(StrEnum):
    VERIFIED = "verified"
    UNVERIFIED = "unverified"
    UNKNOWN = "unknown"


class DeliverableComponent(CanonicalModel):
    symbol: Symbol
    units: PositivePrice


class Deliverable(CanonicalModel):
    """Contract deliverable. A null deliverable does NOT mean 100 shares (02)."""

    schema_version: int = SCHEMA_VERSION
    securities: tuple[DeliverableComponent, ...] = ()
    cash_amount: PositivePrice | None = None
    cash_currency: str | None = None
    adjustment_memo_ref: str | None = None
    verification_status: DeliverableStatus = DeliverableStatus.UNKNOWN

    @model_validator(mode="after")
    def _cash_needs_currency(self) -> Deliverable:
        if self.cash_amount is not None and not self.cash_currency:
            raise ValueError("cash_amount requires cash_currency")
        return self


class ContractTermsObservation(CanonicalModel):
    """Terms as reported by a source. Every unverified field stays null/unknown;
    a multiplier is never inferred from the symbol alone."""

    multiplier: PositivePrice | None = None
    currency: str | None = None
    exercise_style: ExerciseStyle = ExerciseStyle.UNKNOWN
    settlement_style: SettlementStyle = SettlementStyle.UNKNOWN
    expiry_at: UtcDateTime | None = None
    last_trading_at: UtcDateTime | None = None
    deliverable: Deliverable | None = None
    standard_contract: bool | None = None
    evidence: str | None = Field(default=None, max_length=2000)


_OCC = re.compile(r"^(?P<root>[A-Z][A-Z0-9.]{0,5})(?P<ymd>\d{6})(?P<cp>[CP])(?P<strike>\d{8})$")


@dataclass(frozen=True)
class OccParts:
    root: str
    expiration_date: date
    option_type: OptionType
    strike: Decimal


def parse_occ_symbol(symbol: str) -> OccParts | None:
    """Parse ``ROOTyymmdd[C|P]strike*1000``; ``None`` if not OCC-shaped."""
    match = _OCC.match(symbol)
    if match is None:
        return None
    try:
        expiration = datetime.strptime(match["ymd"], "%y%m%d").date()  # noqa: DTZ007
    except ValueError:
        return None
    return OccParts(
        root=match["root"],
        expiration_date=expiration,
        option_type=OptionType(match["cp"]),
        strike=Decimal(match["strike"]) / 1000,
    )


def occ_mismatches(ref: ContractRef) -> list[str]:
    """Fields where an OCC-shaped symbol disagrees with reported fields.

    Empty when consistent *or* when the symbol is not OCC-shaped (nonstandard
    symbols are legal; they simply cannot be cross-checked this way). A root that
    differs from the underlying (e.g. adjusted ``SPY1``) is reported, not guessed.
    """
    parts = parse_occ_symbol(ref.provider_symbol)
    if parts is None:
        return []
    problems: list[str] = []
    if parts.root != ref.underlying_symbol:
        problems.append("root")
    if parts.expiration_date != ref.expiration_date:
        problems.append("expiration_date")
    if parts.option_type is not ref.option_type:
        problems.append("option_type")
    if parts.strike != ref.strike:
        problems.append("strike")
    return problems
