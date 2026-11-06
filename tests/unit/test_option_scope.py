from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from marketagg.collectors.discovery import other_expiries, select_expiries
from marketagg.collectors.quality import midpoint_eligible, option_flags
from marketagg.domain import ContractRef, FieldSemantics, OptionObservation, QualityFlag

SESSION = date(2025, 3, 7)


def _expiries(*dtes: int) -> list[date]:
    return [SESSION + timedelta(days=d) for d in dtes]


def test_selects_distinct_expiries_nearest_targets_within_band() -> None:
    # Fifteen in-band expiries: only four are collected; all others are reported.
    expirations = _expiries(*range(30, 91, 4), 7, 120, -1)
    selection = select_expiries(expirations, SESSION, targets=(30, 45, 60, 90), dte_min=30,
                                dte_max=90, k=4)
    assert [(e - SESSION).days for e in selection.selected] == [30, 46, 58, 90]
    assert len(selection.omitted) == len(set(expirations)) - 4
    reasons = {(e - SESSION).days: r for e, r in selection.omitted.items()}
    assert reasons[7].startswith("outside_dte_band") and reasons[120].startswith("outside")
    assert reasons[-1] == "expired" and reasons[34] == "priority_scope_ceiling"


def test_ties_go_to_the_earlier_expiry_and_targets_never_double_book() -> None:
    selection = select_expiries(_expiries(43, 47, 50), SESSION, targets=(45, 45),
                                dte_min=30, dte_max=90, k=4)
    assert [(e - SESSION).days for e in selection.selected] == [43, 47]


def test_sparse_band_selects_fewer_rather_than_leaving_band() -> None:
    selection = select_expiries(_expiries(14, 35, 100), SESSION, targets=(30, 45, 60, 90),
                                dte_min=30, dte_max=90, k=4)
    assert [(e - SESSION).days for e in selection.selected] == [35]


def test_other_tier_excludes_priority_and_expired() -> None:
    expirations = _expiries(-3, 2, 9, 35, 63, 200)
    other = other_expiries(expirations, SESSION, exclude=_expiries(35, 63), limit=2)
    assert [(e - SESSION).days for e in other.selected] == [2, 9]
    assert [(e - SESSION).days for e in other.omitted] == [200]


def _obs(**fields: object) -> OptionObservation:
    base: dict[str, object] = {
        "contract": ContractRef(provider_id="fake", provider_symbol="SYNA250411C00100000",
                                underlying_symbol="SYNA", option_type="C", strike="100",
                                expiration_date=date(2025, 4, 11)),
        "observed_at": datetime(2025, 3, 7, 15, tzinfo=UTC),
        "field_semantics": FieldSemantics(mapping_version="t"),
        "bid": Decimal("1.0"), "ask": Decimal("1.2"),
    }
    return OptionObservation(**(base | fields))  # type: ignore[arg-type]


def _flags(obs: OptionObservation, partial: bool = False) -> set[QualityFlag]:
    return set(option_flags(obs, partial_scope=partial, old_trade_after=timedelta(minutes=60),
                            iv_max=5.0))


def test_flags_describe_without_repairing() -> None:
    assert _flags(_obs()) == {QualityFlag.UNKNOWN_QUOTE_TIME}
    assert QualityFlag.ZERO_BID in _flags(_obs(bid=Decimal(0)))
    assert QualityFlag.CROSSED in _flags(_obs(bid=Decimal("1.3")))
    assert QualityFlag.LOCKED in _flags(_obs(bid=Decimal("1.2")))
    assert {QualityFlag.NEGATIVE_PRICE} <= _flags(_obs(bid=Decimal("-0.05")))
    assert {QualityFlag.MISSING_BID, QualityFlag.MISSING_ASK} <= _flags(_obs(bid=None, ask=None))
    assert QualityFlag.INVALID_IV in _flags(_obs(reported_iv=12.5))
    assert QualityFlag.INVALID_IV in _flags(_obs(reported_iv=0.0))
    assert QualityFlag.UNKNOWN_OI_DATE in _flags(_obs(reported_open_interest=10))
    assert QualityFlag.OLD_LAST_TRADE in _flags(_obs(
        last_trade_at=datetime(2025, 3, 6, 15, tzinfo=UTC)))
    assert QualityFlag.PARTIAL_SCOPE in _flags(_obs(), partial=True)
    assert _obs(bid=Decimal("-0.05")).bid == Decimal("-0.05")  # value kept as observed


def test_midpoint_eligibility() -> None:
    assert midpoint_eligible(_obs())
    assert midpoint_eligible(_obs(bid=Decimal(0)))  # zero bid allowed for descriptive mid
    assert not midpoint_eligible(_obs(bid=Decimal("1.3")))
    assert not midpoint_eligible(_obs(ask=Decimal(0), bid=Decimal(0)))
    assert not midpoint_eligible(_obs(bid=None))
