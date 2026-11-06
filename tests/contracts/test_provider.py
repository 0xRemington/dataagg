"""Provider boundary contract tests, run against the deterministic fake provider.

Golden files live in tests/fixtures/synthetic/. Regenerate deliberately with
``MARKETAGG_UPDATE_GOLDEN=1 uv run pytest tests/contracts/test_provider.py`` and
review the diff: a changed golden file is a contract change (01).
"""

from __future__ import annotations

import ast
import json
import os
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from marketagg.domain import (
    BarObservation,
    BarRequest,
    CapabilityFact,
    CapabilityStatus,
    ChainRequest,
    Confidence,
    ContractRef,
    Coverage,
    DiscoveryRequest,
    DiscoveryResult,
    FailureCode,
    FetchResult,
    FetchStatus,
    FieldSemantics,
    InstrumentRef,
    OptionObservation,
    OptionType,
    PolicyVersion,
    ProviderCapabilities,
    ProviderError,
    QuoteTimeBasis,
    RateLimited,
    RejectReason,
    RequestCounts,
    SchemaChanged,
    Timeframe,
    Unauthorized,
    Unavailable,
    UnsupportedCapability,
)
from marketagg.domain.identifiers import parse_occ_symbol
from marketagg.domain.primitives import CanonicalModel
from marketagg.providers.base import MarketDataProvider
from marketagg.providers.fake import (
    PARSER_V1,
    PARSER_V2,
    BarRevision,
    FakeAction,
    FakeClock,
    FakeProvider,
    FakeScenario,
    ScriptStep,
)

ROOT = Path(__file__).resolve().parents[2]
GOLDEN = ROOT / "tests" / "fixtures" / "synthetic"
UPDATE_GOLDEN = os.environ.get("MARKETAGG_UPDATE_GOLDEN") == "1"

START = datetime(2025, 3, 7, 15, 0, tzinfo=UTC)  # Friday 10:00 America/New_York
DEADLINE = datetime(2030, 1, 1, tzinfo=UTC)
SYNA = InstrumentRef(provider_id="fake", symbol="SYNA")
EXPIRY_35 = date(2025, 4, 11)  # 35 calendar DTE from 2025-03-07


def make(scenario: FakeScenario | None = None, start: datetime = START) -> FakeProvider:
    return FakeProvider(scenario or FakeScenario(), FakeClock(start))


def discovery_request() -> DiscoveryRequest:
    return DiscoveryRequest(provider_id="fake", policy_id="test", deadline_at=DEADLINE,
                            correlation_id="corr-discover", instrument=SYNA)


def chain_request(expiration: date = EXPIRY_35, deadline: datetime = DEADLINE) -> ChainRequest:
    return ChainRequest(provider_id="fake", policy_id="test", deadline_at=deadline,
                        correlation_id="corr-chain", instrument=SYNA,
                        expiration_date=expiration)


def bar_request(
    start: datetime = datetime(2025, 3, 7, 14, 30, tzinfo=UTC),
    end: datetime = datetime(2025, 3, 7, 14, 45, tzinfo=UTC),
    timeframe: Timeframe = Timeframe.M1,
) -> BarRequest:
    return BarRequest(provider_id="fake", policy_id="test", deadline_at=DEADLINE,
                      correlation_id="corr-bars", instrument=SYNA, timeframe=timeframe,
                      start=start, end=end)


def by_symbol(result: FetchResult[OptionObservation]) -> dict[str, OptionObservation]:
    return {row.contract.provider_symbol: row for row in result.rows}


def row_at(
    result: FetchResult[OptionObservation], side: OptionType, index: int
) -> OptionObservation:
    rows = sorted((r for r in result.rows if r.contract.option_type is side),
                  key=lambda r: r.contract.strike)
    return rows[index]


# -- Protocol and determinism -----------------------------------------------------


def test_fake_satisfies_provider_protocol() -> None:
    assert isinstance(make(), MarketDataProvider)


def test_replay_is_deterministic() -> None:
    def run() -> list[str]:
        provider = make()
        results: list[CanonicalModel] = [
            provider.discover(discovery_request()),
            provider.fetch_chain(chain_request()),
            provider.fetch_bars(bar_request()),
        ]
        return [r.canonical_json() for r in results]

    assert run() == run()


def test_different_seed_changes_synthetic_market() -> None:
    a = make().fetch_chain(chain_request())
    b = make(FakeScenario(seed=8)).fetch_chain(chain_request())
    assert a.response_id != b.response_id
    assert [r.bid for r in a.rows] != [r.bid for r in b.rows]


# -- Golden JSON contract -----------------------------------------------------------


def _golden_cases() -> dict[str, tuple[type[CanonicalModel], CanonicalModel]]:
    provider = make()
    events = make(FakeScenario(quote_event_times=True))
    return {
        "capabilities.json": (ProviderCapabilities, provider.capabilities()),
        "discovery_SYNA.json": (DiscoveryResult, provider.discover(discovery_request())),
        "chain_SYNA_20250411.json": (FetchResult[OptionObservation],
                                     provider.fetch_chain(chain_request())),
        "bars_SYNA_1m.json": (FetchResult[BarObservation], provider.fetch_bars(bar_request())),
        "chain_SYNA_20250411_event_times.json": (FetchResult[OptionObservation],
                                                 events.fetch_chain(chain_request())),
    }


@pytest.mark.parametrize("name", sorted(_golden_cases()))
def test_golden_serialization_round_trip_is_stable(name: str) -> None:
    model_type, value = _golden_cases()[name]
    rendered = json.dumps(value.model_dump(mode="json"), indent=2, sort_keys=True) + "\n"
    path = GOLDEN / name
    if UPDATE_GOLDEN:
        path.write_text(rendered, encoding="utf-8")
    assert path.read_text(encoding="utf-8") == rendered, f"golden drift in {name}"

    reparsed = model_type.model_validate_json(path.read_text(encoding="utf-8"))
    assert reparsed.canonical_json() == value.canonical_json()


# -- Timestamps and unknown freshness ------------------------------------------------


def test_poll_timestamps_are_fetch_times_and_quote_time_stays_unknown() -> None:
    provider = make()
    result = provider.fetch_chain(chain_request())
    assert result.fetch_started_at == START
    assert result.fetch_completed_at == START + provider.scenario.latency
    assert not provider.capabilities().quotes.event_time.supported
    for row in result.rows:
        assert row.observed_at == result.fetch_completed_at
        assert row.provider_event_at is None and row.provider_event_ns is None
        assert row.field_semantics.quote_time_basis is QuoteTimeBasis.UNKNOWN
        assert row.bid_size is None and row.ask_size is None
        assert row.terms is None  # multiplier/deliverable are never guessed
        assert row.open_interest_asof_date is None
    # A last-trade time is reported but never promoted to a quote time.
    assert any(row.last_trade_at is not None for row in result.rows)


def test_event_time_scenario_preserves_nanoseconds() -> None:
    provider = make(FakeScenario(quote_event_times=True))
    row = provider.fetch_chain(chain_request()).rows[0]
    assert row.provider_event_ns is not None and row.provider_event_at is not None
    assert row.provider_event_ns % 1_000 == 789  # sub-microsecond digits retained
    assert row.field_semantics.quote_time_basis is QuoteTimeBasis.PROVIDER_EVENT
    assert provider.capabilities().quotes.event_time.supported


def test_equal_price_polls_are_distinct_observations() -> None:
    provider = make()
    first = provider.fetch_chain(chain_request())
    provider.clock.advance(timedelta(seconds=60))  # same five-minute price epoch
    second = provider.fetch_chain(chain_request())
    assert first.response_id != second.response_id
    assert first.fetch_completed_at < second.fetch_completed_at
    assert [(r.bid, r.ask) for r in first.rows] == [(r.bid, r.ask) for r in second.rows]
    assert first.payload is not None and second.payload is not None
    assert first.payload.sha256 == second.payload.sha256  # checksum is not poll identity


# -- Nullable and anomalous values ----------------------------------------------------


def test_null_zero_and_anomalous_values_are_preserved_not_repaired() -> None:
    result = make().fetch_chain(chain_request())
    last = make().scenario.strikes_per_side - 1
    assert row_at(result, OptionType.PUT, 0).bid == Decimal(0)
    assert row_at(result, OptionType.CALL, last).bid is None
    locked = row_at(result, OptionType.CALL, 1)
    assert locked.bid == locked.ask
    crossed = row_at(result, OptionType.PUT, 2)
    assert crossed.bid is not None and crossed.ask is not None and crossed.bid > crossed.ask
    assert row_at(result, OptionType.PUT, 6).bid == Decimal("-0.05")
    assert row_at(result, OptionType.CALL, 3).reported_iv == 12.5
    assert row_at(result, OptionType.CALL, 4).reported_volume == 0
    assert row_at(result, OptionType.PUT, 4).reported_volume is None
    assert row_at(result, OptionType.CALL, 5).reported_open_interest == 0
    assert row_at(result, OptionType.PUT, 5).reported_open_interest is None
    stale = row_at(result, OptionType.CALL, 7).last_trade_at
    assert stale is not None and result.fetch_completed_at - stale > timedelta(days=9)
    assert row_at(result, OptionType.PUT, 7).last_trade_at is None
    assert result.status is FetchStatus.COMPLETE  # anomalies are data, not failures


# -- Partial, empty and rejected responses -------------------------------------------


def _scripted(op: str, *actions: FakeAction, **kw: Any) -> FakeProvider:
    return make(FakeScenario(script={op: tuple(ScriptStep(a, **kw) for a in actions)}))


@pytest.mark.parametrize(
    ("action", "missing"), [(FakeAction.CALLS_ONLY, "P"), (FakeAction.PUTS_ONLY, "C")]
)
def test_missing_side_is_partial_not_success(action: FakeAction, missing: str) -> None:
    result = _scripted("chain", action).fetch_chain(chain_request())
    assert result.status is FetchStatus.PARTIAL
    assert result.coverage.missing_sides == (OptionType(missing),)
    assert result.coverage.scope_complete is False
    assert result.failures[0].code is FailureCode.PARTIAL_RESULT
    assert result.failures[0].details["missing_sides"] == missing
    assert len(result.rows) == 12


def test_empty_chain_is_empty_with_reason() -> None:
    result = _scripted("chain", FakeAction.EMPTY).fetch_chain(chain_request())
    assert result.status is FetchStatus.EMPTY and result.rows == ()
    assert result.coverage.empty_reason
    assert result.coverage.returned_expiries == ()


def test_unlisted_expiration_returns_empty_not_guessed_rows() -> None:
    result = make().fetch_chain(chain_request(expiration=date(2025, 4, 12)))
    assert result.status is FetchStatus.EMPTY


@pytest.mark.parametrize(
    ("action", "reason"),
    [
        (FakeAction.MALFORMED_ROW, RejectReason.MALFORMED),
        (FakeAction.NONFINITE_ROW, RejectReason.NON_FINITE),
        (FakeAction.AMBIGUOUS_ROW, RejectReason.AMBIGUOUS_CONTRACT),
    ],
)
def test_bad_rows_are_rejected_and_valid_rows_kept(
    action: FakeAction, reason: RejectReason
) -> None:
    result = _scripted("chain", action).fetch_chain(chain_request())
    assert result.status is FetchStatus.PARTIAL
    assert [r.reason for r in result.rejected] == [reason]
    assert len(result.rows) == 23
    assert result.coverage.rows_before_filters == 24
    assert result.coverage.exclusions_by_reason == {reason.value: 1}
    assert not result.coverage.scope_complete


def test_schema_drift_raises_typed_error_with_fingerprint() -> None:
    with pytest.raises(SchemaChanged) as excinfo:
        _scripted("chain", FakeAction.SCHEMA_CHANGED).fetch_chain(chain_request())
    failure = excinfo.value.to_failure()
    assert failure.code is FailureCode.SCHEMA_CHANGED and not failure.retryable
    assert failure.details["missing"] == "bid"
    assert failure.details["fingerprint"].startswith("fake-chain/1:")
    assert excinfo.value.request_counts.endpoint_requests == 1


# -- Typed failures and request accounting -------------------------------------------


def test_rate_limit_carries_retry_after_and_counts_the_request() -> None:
    provider = _scripted("chain", FakeAction.RATE_LIMITED, retry_after=42.0)
    with pytest.raises(RateLimited) as excinfo:
        provider.fetch_chain(chain_request())
    assert excinfo.value.retry_after == 42.0
    assert excinfo.value.to_failure().retry_after_seconds == 42.0
    assert excinfo.value.request_counts == RequestCounts(endpoint_requests=1,
                                                         transport_requests=1)
    assert provider.fetch_chain(chain_request()).status is FetchStatus.COMPLETE  # script consumed


@pytest.mark.parametrize(
    ("action", "error", "retryable"),
    [(FakeAction.UNAUTHORIZED, Unauthorized, False), (FakeAction.UNAVAILABLE, Unavailable, True)],
)
def test_access_and_transport_failures_are_typed(
    action: FakeAction, error: type[ProviderError], retryable: bool
) -> None:
    with pytest.raises(error) as excinfo:
        _scripted("discover", action).discover(discovery_request())
    assert excinfo.value.retryable is retryable
    assert excinfo.value.to_failure().code is error.code


def test_deadline_exceeded_is_unavailable() -> None:
    provider = make()
    with pytest.raises(Unavailable, match="deadline"):
        provider.fetch_chain(chain_request(deadline=START + timedelta(milliseconds=100)))


def test_unsupported_timeframe_fails_without_a_request() -> None:
    provider = make()
    with pytest.raises(UnsupportedCapability) as excinfo:
        provider.fetch_bars(bar_request(timeframe=Timeframe.H4))
    assert excinfo.value.capability == "bars.intervals.4h"
    assert excinfo.value.request_counts.endpoint_requests == 0
    assert provider.call_counts["bars"] == 0
    assert not provider.capabilities().options.historical_snapshots.supported


# -- Bars ----------------------------------------------------------------------------


def test_bars_are_half_open_closed_and_session_bounded() -> None:
    provider = make(start=datetime(2025, 3, 7, 14, 40, 30, tzinfo=UTC))
    result = provider.fetch_bars(bar_request(start=datetime(2025, 3, 7, 14, 0, tzinfo=UTC)))
    starts = [b.interval_start for b in result.rows]
    assert starts[0] == datetime(2025, 3, 7, 14, 30, tzinfo=UTC)  # nothing before the open
    assert starts[-1] == datetime(2025, 3, 7, 14, 39, tzinfo=UTC)  # 14:40 bar not closed yet
    assert all(b.interval_end <= result.fetch_completed_at for b in result.rows)
    assert all(b.interval_end - b.interval_start == timedelta(minutes=1) for b in result.rows)


def test_omitted_minute_is_absent_not_synthesized() -> None:
    gap = datetime(2025, 3, 7, 14, 35, tzinfo=UTC)
    result = make(FakeScenario(omitted_minutes=frozenset({gap}))).fetch_bars(bar_request())
    assert gap not in {b.interval_start for b in result.rows}
    assert len(result.rows) == 14


def test_bar_revision_appears_only_in_later_fetches() -> None:
    target = datetime(2025, 3, 7, 14, 31, tzinfo=UTC)
    provider = make(FakeScenario(bar_revisions=(BarRevision(target, 2, Decimal("0.07")),)))
    first = {b.interval_start: b for b in provider.fetch_bars(bar_request()).rows}
    second = {b.interval_start: b for b in provider.fetch_bars(bar_request()).rows}
    assert second[target].close - first[target].close == Decimal("0.07")
    assert second[target].canonical_sha256() != first[target].canonical_sha256()
    unchanged = datetime(2025, 3, 7, 14, 32, tzinfo=UTC)
    assert second[unchanged].canonical_sha256() == first[unchanged].canonical_sha256()


def test_invalid_ohlc_row_is_quarantined() -> None:
    result = _scripted("bars", FakeAction.INVALID_OHLC).fetch_bars(bar_request())
    assert [r.reason for r in result.rejected] == [RejectReason.INVALID_OHLC]
    assert result.status is FetchStatus.PARTIAL


def test_daily_bars_cover_whole_sessions() -> None:
    provider = make(start=datetime(2025, 3, 10, 22, 0, tzinfo=UTC))
    result = provider.fetch_bars(bar_request(start=datetime(2025, 3, 3, tzinfo=UTC),
                                             end=datetime(2025, 3, 11, tzinfo=UTC),
                                             timeframe=Timeframe.D1))
    assert [b.session_date for b in result.rows] == [
        date(2025, 3, 3), date(2025, 3, 4), date(2025, 3, 5), date(2025, 3, 6),
        date(2025, 3, 7), date(2025, 3, 10),
    ]
    # Session open shifts in UTC across the 2025-03-09 DST change.
    assert result.rows[-2].interval_start.hour == 14
    assert result.rows[-1].interval_start.hour == 13


# -- Parser corrections ---------------------------------------------------------------


def test_reparse_is_a_correction_of_the_same_response() -> None:
    provider = make(FakeScenario(parser_version=PARSER_V1))
    original = provider.fetch_chain(chain_request())
    corrected = provider.reparse_chain(original, chain_request(), PARSER_V2)
    assert corrected.response_id == original.response_id
    assert corrected.fetch_completed_at == original.fetch_completed_at
    assert corrected.request_counts.endpoint_requests == 0
    assert original.payload is not None and corrected.payload is not None
    assert corrected.payload.sha256 == original.payload.sha256
    assert (original.payload.parser_version, corrected.payload.parser_version) == (
        PARSER_V1, PARSER_V2)
    assert all(r.in_the_money is None for r in original.rows)
    assert any(r.in_the_money for r in corrected.rows)
    assert {r.observed_at for r in corrected.rows} == {original.fetch_completed_at}


# -- Canonical value rules ------------------------------------------------------------


def _contract(**overrides: Any) -> ContractRef:
    fields: dict[str, Any] = {
        "provider_id": "fake", "provider_symbol": "SYNA250411C00100000",
        "underlying_symbol": "SYNA", "option_type": "C", "strike": "100",
        "expiration_date": date(2025, 4, 11),
    }
    return ContractRef(**(fields | overrides))


def _option(**overrides: Any) -> OptionObservation:
    fields: dict[str, Any] = {
        "contract": _contract(), "observed_at": START,
        "field_semantics": FieldSemantics(mapping_version="test"),
    }
    return OptionObservation(**(fields | overrides))


@pytest.mark.parametrize(
    "overrides",
    [
        {"observed_at": datetime(2025, 3, 7, 15, 0)},  # noqa: DTZ001 - naive on purpose
        {"bid": 1.25},  # binary float price
        {"bid": "NaN"},
        {"ask": Decimal("Infinity")},
        {"bid": "0.000000001"},  # beyond numeric(20,8)
        {"reported_iv": float("nan")},
        {"reported_iv": float("inf")},
        {"reported_volume": True},
        {"unexpected_field": 1},
        {"provider_event_ns": 1},  # nanoseconds without an event time
        {"provider_event_at": START},  # event time while semantics say unknown
    ],
)
def test_canonical_option_rejects_invalid_values(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        _option(**overrides)


def test_canonical_values_normalise_utc_and_decimals() -> None:
    eastern = timezone(timedelta(hours=-5))
    a = _option(observed_at=datetime(2025, 3, 7, 10, 0, tzinfo=eastern), bid="1.50")
    b = _option(observed_at=START, bid=Decimal("1.5"))
    assert a.observed_at == START and a.observed_at.tzinfo is UTC
    assert a.canonical_json() == b.canonical_json()
    assert json.loads(a.canonical_json())["bid"] == "1.5"
    assert json.loads(a.canonical_json())["observed_at"] == "2025-03-07T15:00:00Z"


def test_canonical_models_are_frozen() -> None:
    with pytest.raises(ValidationError):
        _option().bid = Decimal(1)  # type: ignore[misc]


def test_bar_requires_ohlc_invariants() -> None:
    fields: dict[str, Any] = {
        "instrument": SYNA, "timeframe": "1m", "interval_start": START,
        "interval_end": START + timedelta(minutes=1), "session_date": date(2025, 3, 7),
        "session_kind": "RTH", "price_basis": "provider_native",
        "open": "10", "high": "11", "low": "9", "close": "10.5",
    }
    BarObservation(**fields)
    with pytest.raises(ValidationError, match="OHLC"):
        BarObservation(**(fields | {"close": "12"}))
    with pytest.raises(ValidationError, match="one minute"):
        BarObservation(**(fields | {"interval_end": START + timedelta(minutes=2)}))


def test_fetch_result_enforces_publication_invariants() -> None:
    good = make().fetch_chain(chain_request())
    data = good.model_dump()
    with pytest.raises(ValidationError, match="observed_at must equal"):
        FetchResult[OptionObservation].model_validate(
            data | {"fetch_completed_at": good.fetch_completed_at + timedelta(seconds=1)})
    with pytest.raises(ValidationError, match="complete results"):
        FetchResult[OptionObservation].model_validate(
            data | {"coverage": data["coverage"] | {"observed_sides": ("C",),
                                                     "scope_complete": False}})
    with pytest.raises(ValidationError, match="payload_body"):
        FetchResult[OptionObservation].model_validate(data | {"payload_body": b"tampered"})


def test_coverage_cannot_claim_completeness_without_confidence() -> None:
    with pytest.raises(ValidationError, match="completeness_confidence"):
        Coverage(scope_complete=True)


def test_capability_claims_require_evidence() -> None:
    assert CapabilityFact().status is CapabilityStatus.UNKNOWN
    with pytest.raises(ValidationError, match="evidence"):
        CapabilityFact(status=CapabilityStatus.SUPPORTED)
    with pytest.raises(ValidationError, match="evidence"):
        CapabilityFact(status=CapabilityStatus.UNSUPPORTED, confidence=Confidence.OBSERVED,
                       evidence="probe")


def test_fake_capabilities_grant_no_data_rights() -> None:
    caps = make().capabilities()
    assert caps.synthetic is True
    assert caps.entitlements.reviewed is False
    assert caps.request_limits.requests_per_minute is None  # never invented


def test_occ_parser_is_a_consistency_check_only() -> None:
    parts = parse_occ_symbol("SYNA250411P00102500")
    assert parts is not None and parts.strike == Decimal("102.5")
    assert parts.option_type is OptionType.PUT
    assert parse_occ_symbol("not-an-occ-symbol") is None
    assert parse_occ_symbol("SYNA251399C00100000") is None  # invalid date


def test_policy_id_tracks_content() -> None:
    a = PolicyVersion.create("expiry_selection", {"targets": [30, 45, 60, 90]}, "abc")
    b = PolicyVersion.create("expiry_selection", {"targets": [30, 45, 60]}, "abc")
    assert a.id != b.id
    with pytest.raises(ValidationError, match="content_sha256"):
        PolicyVersion(id=a.id, kind=a.kind, config={"x": 1}, code_sha="abc",
                      content_sha256=a.content_sha256)


# -- Import boundary ---------------------------------------------------------------


FORBIDDEN_MODULES = {"pandas", "numpy", "yfinance", "sqlalchemy", "psycopg", "requests"}


@pytest.mark.parametrize(
    "path",
    [
        *sorted((ROOT / "src" / "marketagg" / "domain").glob("*.py")),
        ROOT / "src" / "marketagg" / "providers" / "base.py",
        ROOT / "src" / "marketagg" / "providers" / "fake.py",
    ],
    ids=lambda p: p.name,
)
def test_contracts_do_not_import_dataframe_or_io_libraries(path: Path) -> None:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    assert not imported & FORBIDDEN_MODULES
