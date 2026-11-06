from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

from marketagg.analytics.asof import OptionRecord
from marketagg.analytics.quote_metrics import (
    QuoteContext,
    Spot,
    atm_strike,
    latest_activity,
    moneyness_skew,
    quote_metrics,
    term_structure,
)

T0 = datetime(2025, 3, 7, 15, 0, tzinfo=UTC)
SESSION = date(2025, 3, 7)
SPOT = Spot(Decimal("100"), "provider_native", SESSION, T0)


def rec(bid: str | None = "1.0", ask: str | None = "1.2", iv: float | None = 0.2,
        at: datetime = T0, **kw: Any) -> OptionRecord:
    base: dict[str, Any] = dict(
        id=uuid4(), batch_id=uuid4(), contract_id=uuid4(), provider_id="fake",
        observed_at=at, known_at=at + timedelta(seconds=1), fetch_completed_at=at,
        provider_event_at=None, last_trade_at=None,
        bid=None if bid is None else Decimal(bid), ask=None if ask is None else Decimal(ask),
        last=None, bid_size=None, ask_size=None, reported_iv=iv, reported_volume=10,
        reported_open_interest=100, in_the_money=False, quality_flags=(), record_revision=1,
        content_sha256="0" * 64, field_semantics={"volume_kind": "unknown"})
    return OptionRecord(**(base | kw))


def test_midpoint_and_spreads() -> None:
    values = quote_metrics(rec()).values
    assert values["midpoint"] == Decimal("1.1") and values["spread_abs"] == Decimal("0.2")
    assert round(values["spread_rel"], 6) == round(0.2 / 1.1, 6)
    assert values["reported_iv_origin"] == "provider_reported"


def test_zero_denominator_and_bad_quotes_produce_no_midpoint() -> None:
    zero = quote_metrics(rec(bid="0", ask="0"))
    assert zero.values["midpoint"] is None and zero.values["spread_rel"] is None
    assert zero.diagnostics["midpoint_unavailable"] == "non_positive_ask"
    crossed = quote_metrics(rec(bid="1.3", ask="1.2"))
    assert crossed.values["midpoint"] is None
    assert crossed.diagnostics["midpoint_unavailable"] == "crossed"
    assert quote_metrics(rec(bid=None)).diagnostics["midpoint_unavailable"] == "missing_side"
    zero_bid = quote_metrics(rec(bid="0", ask="0.05")).values
    assert zero_bid["midpoint"] == Decimal("0.025") and zero_bid["spread_rel"] == 2.0


def test_impossible_iv_is_excluded_and_temporal_uncertainty_labelled() -> None:
    result = quote_metrics(rec(iv=12.5, quality_flags=("invalid_iv",)))
    assert result.values["reported_iv"] is None
    assert result.diagnostics["reported_iv_excluded"] == "invalid_iv"
    assert quote_metrics(rec(iv=-0.1)).values["reported_iv"] is None
    assert result.diagnostics["temporal"] == "temporally_uncertain"


def test_wide_spread_reported_not_hidden() -> None:
    wide = quote_metrics(rec(bid="0.10", ask="2.00")).values
    assert wide["spread_rel"] > 1.5 and wide["midpoint"] == Decimal("1.05")


def test_atm_ties_go_to_lower_strike() -> None:
    assert atm_strike([Decimal(95), Decimal(105)], Decimal(100)) == Decimal(95)
    assert atm_strike([], Decimal(100)) is None


def _cycle(expiry: date, at: datetime = T0) -> list[tuple[QuoteContext, OptionRecord]]:
    rows = []
    for k in (90, 95, 100, 105, 110):
        for side, iv in (("C", 0.20 + (k - 100) * 0.001), ("P", 0.22 - (k - 100) * 0.001)):
            rows.append((QuoteContext(side, Decimal(k), expiry), rec(iv=iv, at=at)))
    return rows


def test_term_structure_lists_absent_expiries_and_rejects_mismatched_basis() -> None:
    e1, e2, missing = date(2025, 4, 11), date(2025, 5, 9), date(2025, 6, 6)
    cycle = _cycle(e1) + _cycle(e2, T0 + timedelta(seconds=30))
    result = term_structure(cycle, spot=SPOT, session_date=SESSION,
                            requested_expiries=[e1, e2, missing])
    points = {p["expiry"]: p for p in result.values["points"]}
    assert points[e1]["atm_strike"] == Decimal(100) and points[e1]["call_reported_iv"] == 0.2
    assert points[missing]["status"] == "unavailable"
    assert result.diagnostics["cycle_skew_seconds"] == 30.0
    assert result.diagnostics["temporal"] == "temporally_uncertain"
    mismatched = term_structure(cycle, spot=SPOT, session_date=SESSION,
                                requested_expiries=[e1], required_basis="as_traded")
    assert mismatched.values["points"][0]["reason"] == "spot_basis_provider_native_incompatible"
    stale = term_structure(cycle, spot=Spot(SPOT.price, SPOT.price_basis, date(2025, 3, 6), T0),
                           session_date=SESSION, requested_expiries=[e1])
    assert stale.values["points"][0]["reason"] == "spot_other_session"


def test_skew_bounded_by_cycle_skew_and_near_atm_definition() -> None:
    expiry = date(2025, 4, 11)
    rows = [*_cycle(expiry), (QuoteContext("C", Decimal(150), expiry), rec(iv=0.5))]
    result = moneyness_skew(rows, spot=SPOT, session_date=SESSION)
    # ln(90/100) = -0.105 and the 150 strike fall outside |ln(K/S)| <= 0.10
    assert result.diagnostics["excluded"] == {"outside_near_atm": 3}
    assert result.values["put_call_skew"] == round((0.22 + 0.005 - 0.205) * 100, 6)
    late = [(c, rec(iv=o.reported_iv, at=T0 + timedelta(seconds=300) if c.option_type == "C"
                    else T0)) for c, o in _cycle(expiry)]
    bounded = moneyness_skew(late, spot=SPOT, session_date=SESSION)
    assert bounded.values["put_call_skew"] is None and bounded.diagnostics["skew_bound_exceeded"]


def test_cumulative_activity_is_not_summed() -> None:
    history = [rec(at=T0 + timedelta(minutes=5 * i), reported_volume=100 * (i + 1))
               for i in range(3)]
    activity = latest_activity(history)
    assert activity["reported_volume"] == 300 and activity["reported_open_interest"] == 100
