"""Dashboard acceptance against fixture API responses (no database, no network).

Fixtures are validated with the API's own response models, so the UI is tested
against the real contract shape. Screenshot review (05 A08) needs a browser and
is reported separately.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from streamlit.testing.v1 import AppTest

from marketagg.api import schemas as s

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ui"))

from views import FOUR_HOUR_NOTE, banners, build_figure, with_breaks  # noqa: E402

pytestmark = pytest.mark.acceptance
INSTRUMENT = str(uuid4())
CONTRACT = str(uuid4())
DAY = datetime(2025, 3, 7, 14, 30, tzinfo=UTC)


def _bar(start: datetime, minutes: int, complete: bool = True, label: str = "5m") -> dict:
    return {"interval_start": start, "interval_end": start + timedelta(minutes=minutes),
            "session_date": start.date(), "duration_minutes": minutes, "label": label,
            "open": "100", "high": "101", "low": "99", "close": "100.5", "volume": 10,
            "complete": complete, "expected_minutes": minutes,
            "observed_minutes": minutes if complete else minutes - 1,
            "is_short_session_bucket": minutes < 240 and label.startswith("4h"),
            "quality_flags": [], "known_at": start + timedelta(minutes=minutes + 1)}


def _envelope(timeframe: str, items: list[dict], **extra: Any) -> dict:
    return {"request_id": "r", "timeframe": timeframe,
            "source": {"provider_id": "fake", "policy_id": "p", "price_basis": "provider_native",
                       "session_kind": "RTH"},
            "availability": {"mode": "observed", "decision_at": DAY + timedelta(hours=7),
                             "label": "observed: outputs actually published by the decision "
                                      "time"},
            "items": items, **extra}


def _option(at: datetime) -> dict:
    return {"observed_at": at, "known_at": at + timedelta(seconds=2), "fetch_completed_at": at,
            "provider_event_at": None, "quote_time_basis": "unknown", "last_trade_at": None,
            "bid": "1.0", "ask": "1.2", "midpoint": "1.1", "last": None, "bid_size": None,
            "ask_size": None, "reported_iv": 0.2, "reported_iv_origin": "provider_reported",
            "reported_volume": None, "reported_open_interest": 5, "quality_flags": [],
            "record_revision": 1}


def _panel_items() -> list[dict]:
    items = []
    for i in range(6):
        if i == 3:
            continue  # a missing bar must break the line, not be bridged
        bar = _bar(DAY + timedelta(minutes=5 * i), 5, complete=i != 5)
        option = _option(bar["interval_end"] - timedelta(seconds=30)) if i > 0 else None
        items.append({"bar": bar, "decision_at": bar["interval_end"], "option": option,
                      "spot_close": "100.5" if option else None,
                      "spot_interval_end": bar["interval_end"] if option else None,
                      "option_observation_age_seconds": 30.0 if option else None,
                      "quote_age_seconds": None, "spot_age_seconds": 60.0 if option else None,
                      "freshness": "unknown", "execution_eligible": False,
                      "exclusions": [] if option else ["option_unavailable"]})
    return items


def _write(directory: Path, **routes: Any) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    for name, model in routes.items():
        body = model.model_dump(mode="json") if hasattr(model, "model_dump") else model
        (directory / f"{name}.json").write_text(json.dumps(body))
    return directory


def _fixtures(tmp_path: Path, *, with_contract: bool = True, timeframe: str = "5m",
              items: list[dict] | None = None) -> Path:
    instruments = s.InstrumentsResponse(request_id="r", items=[s.InstrumentOut(
        id=INSTRUMENT, symbol="SYNA", provider_id="fake", asset_class="etf", currency="USD",
        calendar_id="XNYS", status="active", valid_from=DAY.date(), valid_to=None,
        identity_known_at=DAY)])
    contracts = s.ContractsResponse.model_validate(_envelope("5m", [
        {"id": CONTRACT, "provider_symbol": "SYNA250411C00100000", "option_type": "C",
         "strike": "100", "expiration_date": "2025-04-11", "first_known_at": DAY,
         "terms_known": False, "model_eligible": False}] if with_contract else []))
    panel = s.PanelResponse.model_validate(_envelope(
        timeframe, items if items is not None else _panel_items(), contract_id=CONTRACT,
        sampling="latest known at bar end",
        warnings=["option samples are periodic polls; no option candles are implied"]))
    bars = s.BarsResponse.model_validate(_envelope(timeframe, items if items is not None else [
        _bar(DAY, 240, label="4h session bucket"),
        _bar(DAY + timedelta(hours=4), 150, label="4h session bucket (2h30 remainder)")]))
    quality = s.QualityResponse(request_id="r", report={"gaps": []})
    return _write(tmp_path, v1_instruments=instruments, v1_contracts=contracts, v1_panel=panel,
                  v1_bars=bars, v1_quality=quality)


def _run(directory: Path, monkeypatch: pytest.MonkeyPatch) -> AppTest:
    monkeypatch.setenv("MARKETAGG_UI_FIXTURE_DIR", str(directory))
    app = AppTest.from_file(str(ROOT / "ui" / "app.py"), default_timeout=60)
    app.run()
    assert not app.exception, app.exception
    return app


def _texts(app: AppTest) -> str:
    parts = [m.value for m in [*app.warning, *app.info, *app.error, *app.caption, *app.markdown]]
    return "\n".join(str(p) for p in parts)


def test_contract_browser_and_aligned_panels(tmp_path: Path,
                                             monkeypatch: pytest.MonkeyPatch) -> None:
    app = _run(_fixtures(tmp_path), monkeypatch)
    assert {sb.label for sb in app.selectbox} == {"Instrument", "Expiration", "Strike"}
    interval = next(r for r in app.radio if r.label == "Interval")
    assert list(interval.options) == ["1m", "5m", "15m", "4h", "1d"]
    text = _texts(app)
    assert "Quote event time UNKNOWN" in text
    assert "partial bar(s) shown for display only" in text
    assert "price basis provider_native" in text and "browse only" in text


def test_shortened_4h_explained_and_empty_states(tmp_path: Path,
                                                monkeypatch: pytest.MonkeyPatch) -> None:
    app = _run(_fixtures(tmp_path / "a", with_contract=False, timeframe="4h"), monkeypatch)
    assert FOUR_HOUR_NOTE in _texts(app)
    empty = _run(_fixtures(tmp_path / "b", with_contract=False, items=[]), monkeypatch)
    text = _texts(empty)
    assert "No eligible bars" in text and "No contracts observed" in text


def test_api_error_is_readable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    directory = _write(tmp_path, v1_instruments={
        "code": "dependency_unavailable", "message": "database unavailable", "details": {},
        "request_id": "r"})
    app = _run(directory, monkeypatch)
    assert "Research API unavailable: dependency_unavailable" in _texts(app)


def test_figure_breaks_lines_at_gaps_and_keeps_utc_axis() -> None:
    response = s.PanelResponse.model_validate(_envelope("5m", _panel_items(),
                                                        contract_id=CONTRACT,
                                                        sampling="x")).model_dump(mode="json")
    figure = build_figure(response)
    mid = next(t for t in figure.data if t.name == "option midpoint")
    assert None in list(mid.y)  # missing 15:45 bar is a visible break
    assert mid.connectgaps is False
    assert figure.layout.xaxis2.title.text == "UTC"
    xs, ys = with_breaks([(DAY, 1.0), (DAY + timedelta(minutes=5), 2.0),
                          (DAY + timedelta(days=3), 3.0)], timedelta(minutes=5))
    assert ys == [1.0, 2.0, None, 3.0] and xs[2] is None  # no weekend bridging
    levels = [lvl for lvl, _ in banners(response, has_contract=True)]
    assert "warning" in levels
