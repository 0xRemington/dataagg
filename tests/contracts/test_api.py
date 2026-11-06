"""API contract tests against a seeded disposable database (needs PostgreSQL 18)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, text
from testkit import BAR_POLICY, aggregation_policy_id, ingest_minutes, make_world

from marketagg.analytics.aggregate import derive_pending
from marketagg.api.app import ApiServices, create_app
from marketagg.calendar import SessionCalendar
from marketagg.config import load_policies
from marketagg.research.datasets import DatasetService
from marketagg.research.repository import ResearchRepository

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]
READ, EXPORT = "read-token-0123456789", "export-token-0123456789"
CAL = SessionCalendar()
NOW = datetime(2025, 3, 7, 21, 10, tzinfo=UTC)
OPEN = datetime(2025, 3, 7, 14, 30, tzinfo=UTC)


@pytest.fixture
def seeded(engine: Engine, tmp_path: Path) -> dict[str, Any]:
    world = make_world(engine, start=NOW - timedelta(minutes=5))
    ingest_minutes(world, OPEN, datetime(2025, 3, 7, 21, 0, tzinfo=UTC))
    policy = aggregation_policy_id(world)
    world.clock.set(NOW)
    derive_pending(engine, world.repo, CAL, aggregation_policy_id=policy, now=NOW,
                   instruments={v: k for k, v in world.instruments.items()})
    world.clock.set(datetime(2025, 3, 7, 20, 0, tzinfo=UTC))
    chain = world.provider.fetch_chain(world.chain_request())
    staged = world.repo.stage_chain(chain, instrument_id=world.instruments["SYNA"], scope={})
    world.repo.publish(staged.batch_id)
    world.clock.set(NOW + timedelta(minutes=1))
    research = ResearchRepository(engine, CAL, {"bars_ingest": BAR_POLICY.id,
                                                "aggregation": policy})
    datasets = DatasetService(engine, research, tmp_path / "exports", signing_key=b"k" * 32,
                              clock=world.clock)
    services = ApiServices(engine=engine, research=research, calendar=CAL,
                           policies=load_policies(ROOT / "config" / "policies.yaml"),
                           read_tokens=(READ,), export_tokens=(EXPORT,), datasets=datasets,
                           clock=world.clock)
    with engine.connect() as conn:
        contract = conn.execute(text("SELECT contract_id FROM option_snapshots LIMIT 1")
                                ).scalar_one()
    return {"client": TestClient(create_app(services)), "world": world,
            "instrument": str(world.instruments["SYNA"]), "contract": str(contract)}


def _get(seeded: dict[str, Any], path: str, token: str | None = READ, **params: Any) -> Any:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return seeded["client"].get(path, params=params, headers=headers)


def _bars(seeded: dict[str, Any], **params: Any) -> Any:
    base = {"instrument_id": seeded["instrument"], "provider_id": "fake", "timeframe": "15m",
            "start": OPEN.isoformat(), "end": "2025-03-07T21:00:00Z"}
    return _get(seeded, "/v1/bars", **(base | params))


def test_auth_required_and_errors_use_envelope(seeded: dict[str, Any]) -> None:
    missing = _bars(seeded, token=None)
    assert missing.status_code == 401
    body = missing.json()
    assert set(body) == {"code", "message", "details", "request_id"}
    assert body["request_id"] == missing.headers["X-Request-ID"]
    wrong = _get(seeded, "/v1/instruments", token="nope")
    assert wrong.status_code == 403 and "nope" not in wrong.text


def test_health_has_no_secrets_and_readiness_checks_schema(seeded: dict[str, Any]) -> None:
    live = seeded["client"].get("/health/live")
    assert live.status_code == 200 and live.json() == {"status": "alive"}
    ready = seeded["client"].get("/health/ready")
    payload = ready.text
    assert READ not in payload and EXPORT not in payload and "postgres" not in payload
    checks = ready.json()["checks"]
    assert checks["database"] == "ok" and checks["migration"] == "ok"


def test_bars_serialise_decimals_as_strings_and_utc_z(seeded: dict[str, Any]) -> None:
    response = _bars(seeded)
    assert response.status_code == 200
    body = response.json()
    assert body["schema_version"] == 1 and body["timeframe"] == "15m"
    assert body["availability"]["mode"] == "observed"
    assert body["source"]["price_basis"] == "provider_native"
    first = body["items"][0]
    assert isinstance(first["open"], str) and first["interval_start"].endswith("Z")
    assert len(body["items"]) == 26
    h4 = _bars(seeded, timeframe="4h").json()
    assert [i["label"] for i in h4["items"]] == ["4h session bucket",
                                                 "4h session bucket (2h30 remainder)"]
    assert any("session-anchored" in w for w in h4["warnings"])


def test_cursor_pagination_is_stable_while_ingestion_continues(seeded: dict[str, Any]) -> None:
    first = _bars(seeded, timeframe="1m", limit=100).json()
    assert len(first["items"]) == 100 and first["next_cursor"]
    decision = first["availability"]["decision_at"]
    world = seeded["world"]
    world.clock.advance(timedelta(minutes=5))
    # An invalidation published after the cutoff must not change later pages.
    world.repo.publish(world.repo.invalidate_bar(
        *_bar_id(world.engine, "2025-03-07T17:00:00Z"), reason="late invalidation"))
    second = _bars(seeded, timeframe="1m", limit=100, cursor=first["next_cursor"]).json()
    assert second["availability"]["decision_at"] == decision  # cutoff pinned by cursor
    starts = [i["interval_start"] for i in first["items"] + second["items"]]
    assert len(starts) == len(set(starts)) == 200
    assert "2025-03-07T17:00:00Z" in starts  # invalidation was not known at the cutoff
    tampered = _bars(seeded, timeframe="15m", cursor=first["next_cursor"])
    assert tampered.status_code == 422


def _bar_id(engine: Engine, start: str) -> tuple[Any, Any]:
    with engine.connect() as conn:
        row = conn.execute(text("SELECT id, interval_start FROM bar_observations "
                                "WHERE timeframe='1m' AND interval_start=:s"),
                           {"s": datetime.fromisoformat(start.replace("Z", "+00:00"))}).one()
    return row.id, row.interval_start


def test_query_limits_and_validation(seeded: dict[str, Any]) -> None:
    too_long = _bars(seeded, timeframe="1m", start="2025-01-01T00:00:00Z")
    assert too_long.status_code == 422 and too_long.json()["code"] == "invalid_query"
    assert _bars(seeded, limit=10_001).status_code == 422
    future = _bars(seeded, decision_at="2030-01-01T00:00:00Z")
    assert future.status_code == 422
    injection = _get(seeded, "/v1/bars", instrument_id="1; DROP TABLE x", provider_id="fake",
                     timeframe="1m", start=OPEN.isoformat(), end="2025-03-07T15:00:00Z")
    assert injection.status_code == 422


def test_incompatible_basis_is_409(seeded: dict[str, Any]) -> None:
    response = _bars(seeded, price_basis="as_traded")
    assert response.status_code == 409 and response.json()["code"] == "incompatible_query"


def test_option_history_preserves_nulls_and_unknown_freshness(seeded: dict[str, Any]) -> None:
    response = _get(seeded, "/v1/options/history", contract_id=seeded["contract"],
                    start="2025-03-07T14:00:00Z", end="2025-03-07T21:00:00Z")
    body = response.json()
    item = body["items"][0]
    assert item["provider_event_at"] is None and item["quote_time_basis"] == "unknown"
    assert item["bid_size"] is None and item["ask_size"] is None
    assert body["coverage"]["unknown_quote_time"] == len(body["items"])
    assert any("no option candles" in w for w in body["warnings"])
    early = _get(seeded, "/v1/options/history", contract_id=seeded["contract"],
                 start="2025-03-07T14:00:00Z", end="2025-03-07T21:00:00Z",
                 decision_at="2025-03-07T19:59:00Z").json()
    assert early["items"] == []


def test_panel_and_surfaces_contracts(seeded: dict[str, Any]) -> None:
    panel = _get(seeded, "/v1/panel", instrument_id=seeded["instrument"], provider_id="fake",
                 timeframe="5m", start="2025-03-07T19:50:00Z", end="2025-03-07T20:30:00Z",
                 contract_id=seeded["contract"]).json()
    with_option = [p for p in panel["items"] if p["option"] is not None]
    assert with_option and all(p["freshness"] == "unknown" for p in with_option)
    assert not any(p["execution_eligible"] for p in panel["items"])
    assert all(p["quote_age_seconds"] is None for p in panel["items"])
    mixed = _get(seeded, "/v1/panel", instrument_id=seeded["instrument"], provider_id="fake",
                 option_provider_id="yahoo", timeframe="5m", start="2025-03-07T19:50:00Z",
                 end="2025-03-07T20:30:00Z")
    assert mixed.status_code == 409
    surface = _get(seeded, "/v1/surfaces", instrument_id=seeded["instrument"],
                   provider_id="fake", target_dte=30).json()
    assert surface["value"] is None and surface["diagnostics"]["extrapolation"] is False


def test_reference_and_quality_routes(seeded: dict[str, Any]) -> None:
    instruments = _get(seeded, "/v1/instruments").json()["items"]
    assert {i["symbol"] for i in instruments} == {"SYNA", "SYNB"}
    contracts = _get(seeded, "/v1/contracts", instrument_id=seeded["instrument"],
                     provider_id="fake", asof="2025-03-07T21:00:00Z").json()
    assert len(contracts["items"]) == 24
    assert not any(c["model_eligible"] for c in contracts["items"])
    assert contracts["warnings"]
    assert "gaps" in _get(seeded, "/v1/quality").json()["report"]


def test_dataset_lifecycle_requires_export_scope(seeded: dict[str, Any]) -> None:
    body = {"instrument_id": seeded["instrument"], "provider_id": "fake", "timeframe": "15m",
            "start": "2025-03-07T14:30:00Z", "end": "2025-03-07T21:00:00Z",
            "decision_at": "2025-03-07T21:05:00Z", "formats": ["csv", "parquet"],
            "contract_ids": [seeded["contract"]]}
    client = seeded["client"]
    denied = client.post("/v1/datasets", json=body, headers={"Authorization": f"Bearer {READ}"})
    assert denied.status_code == 403
    accepted = client.post("/v1/datasets", json=body,
                           headers={"Authorization": f"Bearer {EXPORT}"})
    assert accepted.status_code == 202
    job = _get(seeded, f"/v1/datasets/{accepted.json()['id']}").json()
    assert job["state"] == "succeeded" and job["manifest"]["row_count"] > 0
    url = job["downloads"]["bars.csv"]
    assert client.get(url).status_code == 200
    assert client.get(url.replace("signature=", "signature=0")).status_code == 403
    unbounded = client.post("/v1/datasets", json=body | {"start": "2015-01-01T00:00:00Z"},
                            headers={"Authorization": f"Bearer {EXPORT}"})
    assert unbounded.status_code == 422
    traversal = client.get(url.replace("bars.csv", "..%2F..%2Fetc%2Fpasswd"))
    assert traversal.status_code in (403, 404)
