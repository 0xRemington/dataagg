from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

import pyarrow.parquet as pq
import pytest

ROOT = Path(__file__).resolve().parents[2]


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("probe_provider",
                                                  ROOT / "scripts" / "probe_provider.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # dataclasses resolve annotations via sys.modules
    spec.loader.exec_module(module)
    return module


probe = _load()
FILES = ("environment.json", "capabilities.json", "request-budget.json", "source-review.md",
         "probe-results.parquet")


def test_fake_probe_writes_complete_deterministic_report(tmp_path: Path) -> None:
    a, b = tmp_path / "a", tmp_path / "b"
    assert probe.main(["--provider", "fake", "--output", str(a)]) == 0
    assert probe.main(["--provider", "fake", "--output", str(b)]) == 0
    for name in FILES:
        assert (a / name).is_file()
        if name.endswith((".json", ".md")):
            assert (a / name).read_text() == (b / name).read_text(), name
    assert pq.read_table(a / "probe-results.parquet").equals(
        pq.read_table(b / "probe-results.parquet"))


def test_probe_separates_span_from_age_and_claims_no_history(tmp_path: Path) -> None:
    probe.main(["--provider", "fake", "--output", str(tmp_path)])
    caps = json.loads((tmp_path / "capabilities.json").read_text())
    assert caps["options"]["historical_snapshots"]["status"] == "unsupported"
    m1 = caps["bars"]["1m"]
    assert "max_request_span_seconds" in m1 and "oldest_history_seconds" in m1
    for fact in (m1["available"], caps["quotes"]["event_time"]):
        assert {"status", "confidence", "evidence", "checked_at"} <= set(fact)
    env = json.loads((tmp_path / "environment.json").read_text())
    assert env["evidence_kind"] == "synthetic dry run (no network)"
    rows = pq.read_table(tmp_path / "probe-results.parquet").to_pylist()
    details = {r["detail"] for r in rows if r["probe"] == "bars"}
    assert {"1m span 1d", "1m span 7d", "daily 30d"} <= details
    assert any(r["probe"] == "chain" and r["detail"].startswith("warm") for r in rows)
    review = (tmp_path / "source-review.md").read_text()
    assert "UNRESOLVED" in review and "Live probe executed | no" in review


def test_live_probe_is_blocked_without_review(tmp_path: Path,
                                              monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("PROVIDER", "LIVE_DATA_ENABLED", "SOURCE_REVIEW_ID", "MARKETAGG_ENV"):
        monkeypatch.delenv(name, raising=False)
    assert probe.main(["--provider", "yahoo", "--output",
                       str(tmp_path / "live" / "yahoo")]) == probe.EXIT_BLOCKED
    assert probe.main(["--provider", "yahoo", "--output",
                       str(tmp_path / "yahoo")]) == probe.EXIT_BLOCKED
    assert not any(tmp_path.rglob("*.json"))
