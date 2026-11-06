"""Shared enums. Values match the CHECK constraints in schema/001_reference.sql.

Owned by the foundation/contract owner (04): other agents request additions.
"""

from __future__ import annotations

from enum import StrEnum


class OptionType(StrEnum):
    CALL = "C"
    PUT = "P"


class Timeframe(StrEnum):
    M1 = "1m"
    M5 = "5m"
    M15 = "15m"
    H4 = "4h"
    D1 = "1d"


class SessionKind(StrEnum):
    RTH = "RTH"
    ETH = "ETH"


class PriceBasis(StrEnum):
    PROVIDER_NATIVE = "provider_native"
    AS_TRADED = "as_traded"
    SPLIT_ADJUSTED = "split_adjusted"
    TOTAL_RETURN_ADJUSTED = "total_return_adjusted"


class BatchKind(StrEnum):
    CHAIN = "chain"
    BARS = "bars"
    MARK = "mark"
    ACTION = "action"
    DERIVED = "derived"
    REFERENCE = "reference"
    STREAM = "stream"


class ObservationKind(StrEnum):
    POLL = "poll"
    EVENT = "event"
    CHECKPOINT = "checkpoint"


class PayloadRepresentation(StrEnum):
    PROVIDER_WIRE = "provider_wire"
    ADAPTER_RESULT = "adapter_result"
    SYNTHETIC = "synthetic"


class ExerciseStyle(StrEnum):
    AMERICAN = "american"
    EUROPEAN = "european"
    UNKNOWN = "unknown"


class SettlementStyle(StrEnum):
    PHYSICAL = "physical"
    CASH = "cash"
    UNKNOWN = "unknown"


class AvailabilityMode(StrEnum):
    OBSERVED = "observed"
    RECOMPUTED = "recomputed"


class QuoteTimeBasis(StrEnum):
    PROVIDER_EVENT = "provider_event"
    UNKNOWN = "unknown"


class VolumeKind(StrEnum):
    SESSION_CUMULATIVE = "session_cumulative"
    UNKNOWN = "unknown"


class OpenInterestKind(StrEnum):
    REPORTED_TOTAL = "reported_total"


class VolumeDateBasis(StrEnum):
    SOURCE = "source"
    INFERRED_SESSION = "inferred_session"
    UNKNOWN = "unknown"


class OpenInterestDateBasis(StrEnum):
    SOURCE = "source"
    UNKNOWN = "unknown"


class SizeUnits(StrEnum):
    CONTRACTS = "contracts"
    UNKNOWN = "unknown"


class CompletenessConfidence(StrEnum):
    KNOWN = "known"
    UNKNOWN = "unknown"


class FetchStatus(StrEnum):
    COMPLETE = "complete"
    PARTIAL = "partial"
    EMPTY = "empty"


class QualityFlag(StrEnum):
    """Row-level flags (03 "Quality and gaps") plus revision markers (02)."""

    MISSING_BID = "missing_bid"
    MISSING_ASK = "missing_ask"
    ZERO_BID = "zero_bid"
    CROSSED = "crossed"
    LOCKED = "locked"
    NEGATIVE_PRICE = "negative_price"
    UNKNOWN_QUOTE_TIME = "unknown_quote_time"
    OLD_LAST_TRADE = "old_last_trade"
    INVALID_IV = "invalid_iv"
    UNKNOWN_OI_DATE = "unknown_oi_date"
    PARTIAL_SCOPE = "partial_scope"
    AMBIGUOUS_CONTRACT = "ambiguous_contract"
    BASIS_UNVERIFIED = "basis_unverified"
    INVALIDATED = "invalidated"


class RejectReason(StrEnum):
    """Why a source row could not become a canonical observation."""

    NON_FINITE = "non_finite"
    MALFORMED = "malformed"
    MISSING_IDENTITY = "missing_identity"
    AMBIGUOUS_CONTRACT = "ambiguous_contract"
    INVALID_OHLC = "invalid_ohlc"
    OUT_OF_WINDOW = "out_of_window"
