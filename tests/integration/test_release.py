"""T19: pilot measurement from recorded facts, and a release gate that fails closed."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine

from marketagg.calendar import SessionCalendar
from marketagg.config import load_policies, load_universe
from marketagg.providers.fake import FakeClock, FakeProvider, FakeScenario
from marketagg.release import CAPACITY_FILE, evaluate_release, measure_capacity
from marketagg.runtime import bootstrap, make_planner, make_worker

ROOT = Path(__file__).resolve().parents[2]
CAL = SessionCalendar()


def _run_sessions(engine: Engine, days: list[date]) -> None:
    clock = FakeClock(datetime(2025, 3, 7, 14, 0, tzinfo=UTC))
    rt = bootstrap(engine, FakeProvider(FakeScenario(), clock),
                   load_universe(ROOT / "config" / "universe.yaml"),
                   load_policies(ROOT / "config" / "policies.yaml"), clock=clock,
                   requests_per_minute=600)
    planner = make_planner(rt)
    worker = make_worker(rt, "w", sleep=lambda s: clock.advance(timedelta(seconds=s)))
    for day in days:
        session = CAL.session(day)
        assert session is not None
        tick = session.open_at - timedelta(minutes=20)
        clock.set(tick)
        while clock.now() < session.close_at + timedelta(minutes=5):
            planner.enqueue_due(rt.queue, clock.now(), horizon=timedelta(minutes=1))
            while worker.run_once(["discovery", "chain_priority", "bars_1m"]) is not None:
                pass
            tick += timedelta(minutes=1)
            clock.set(max(tick, clock.now()))


def test_measurement_counts_every_slot_and_fixture_never_releases(engine: Engine,
                                                                   tmp_path: Path) -> None:
    _run_sessions(engine, [date(2025, 3, 7)])
    report = measure_capacity(engine, CAL, provider_id="fake", sessions=5,
                              requests_per_minute=600, disk_paths=[tmp_path],
                              max_local_gib=100)
    summary = report["summary"]
    # Only one session has data; the other four are reported, with zero requests.
    assert summary["sessions_measured"] == 5
    assert sum(1 for s in report["sessions"] if s["requests"]["endpoint"] > 0) == 1
    measured = [s for s in report["sessions"] if s["requests"]["endpoint"] > 0]
    for session in measured:
        assert session["minute_coverage"]["SYNA"]["expected"] == 390
        assert session["minute_coverage"]["SYNA"]["coverage"] >= 0.99
        scopes = session["chain_scopes"]
        assert scopes and all(s["intended"] == 78 for s in scopes)  # every 5-min slot
        assert session["quote_event_time_known_ratio"] == 0.0
    assert summary["freshness_claim"] == "collection_cadence_only"
    assert report["storage"]["bytes_per_row_with_indexes"]["option_snapshots"] > 0
    assert report["headroom"]["projected_headroom_days"] is not None
    pilot = tmp_path / "reports" / "pilot"
    pilot.mkdir(parents=True)
    (pilot / CAPACITY_FILE).write_text(json.dumps(report, default=str))
    result = evaluate_release(pilot)
    gates = {g["gate"]: g["passed"] for g in result["gates"]}
    assert result["release_allowed"] is False
    assert gates["live_source_measured"] is False and gates["five_sessions"] is False
    assert gates["acceptance_signed_off"] is False  # no acceptance manifest here
    assert (pilot / "release-decision.json").is_file()


def test_missing_evidence_fails_closed(tmp_path: Path) -> None:
    result = evaluate_release(tmp_path / "nothing-here")
    assert result["release_allowed"] is False
    assert all(not g["passed"] for g in result["gates"])


def _complete_evidence(root: Path) -> Path:
    """Hand-built evidence for a hypothetical live provider (gate logic only)."""
    sessions = [{"session_date": f"2026-01-0{i}", "requests": {"endpoint": 500},
                 "skipped_slots": 1, "chain_scopes": []} for i in range(5, 10)]
    capacity: dict[str, Any] = {
        "provider_id": "yahoo", "live_source": True, "sessions": sessions,
        "requests_per_minute_configured": 10,
        "headroom": {"projected_headroom_days": 45.0},
        "summary": {"min_scope_chain_coverage": 0.97, "skipped_slots": 5,
                    "min_minute_coverage": 0.995, "max_fetch_to_known_p95_seconds": 1.2,
                    "request_budget_utilization": 1.0, "planned_request_utilization": 0.4,
                    "peak_requests_per_minute": 10.0, "quote_event_time_known_ratio": 0.0,
                    "freshness_claim": "collection_cadence_only",
                    "verified_backups_during_sessions": 1}}
    pilot = root / "pilot"
    pilot.mkdir(parents=True)
    (pilot / CAPACITY_FILE).write_text(json.dumps(capacity))
    review = root / "feasibility" / "live" / "yahoo"
    review.mkdir(parents=True)
    (review / "source-review.md").write_text("# Review\n\nRelease decision: APPROVED\n")
    run = root / "acceptance" / "20260110T000000Z-fixture"
    run.mkdir(parents=True)
    manifest = {"signed_off": True,
                "scenarios": {s: {"status": "passed"} for s in (
                    *(f"A0{i}" for i in range(1, 10)), "temporal_adversaries")},
                "measurements": {"test_a07": {"a07_rto_seconds": 12.0}}}
    body = json.dumps(manifest).encode()
    (run / "manifest.json").write_bytes(body)
    (run / "manifest.sha256").write_text(hashlib.sha256(body).hexdigest() + "  manifest.json")
    return pilot


def test_complete_evidence_passes_and_each_gate_blocks(tmp_path: Path) -> None:
    pilot = _complete_evidence(tmp_path)
    assert evaluate_release(pilot)["release_allowed"] is True
    capacity = json.loads((pilot / CAPACITY_FILE).read_text())
    capacity["summary"]["min_scope_chain_coverage"] = 0.90
    (pilot / CAPACITY_FILE).write_text(json.dumps(capacity))
    blocked = evaluate_release(pilot)
    assert blocked["release_allowed"] is False
    assert [g["gate"] for g in blocked["gates"] if not g["passed"]] == [
        "priority_chain_coverage"]
    # Publishing a measured slower cadence (never relabelled as 5-minute) is the alternative.
    (pilot / "cadence-limitation.json").write_text(json.dumps(
        {"measured_cadence_minutes": 10, "scope": "SPY/QQQ priority expiries"}))
    assert evaluate_release(pilot)["release_allowed"] is True


@pytest.mark.parametrize(("path", "mutate"), [
    ("feasibility/live/yahoo/source-review.md",
     lambda p: p.write_text("Release decision: APPROVED\n| Retention | UNRESOLVED |\n")),
    ("acceptance/20260110T000000Z-fixture/manifest.json",
     lambda p: p.write_bytes(p.read_bytes().replace(b"true", b"false"))),
])
def test_unresolved_review_or_tampered_manifest_blocks(tmp_path: Path, path: str,
                                                       mutate: Any) -> None:
    pilot = _complete_evidence(tmp_path)
    mutate(tmp_path / path)
    assert evaluate_release(pilot)["release_allowed"] is False


@pytest.mark.parametrize(("field", "value"), [
    ("planned_request_utilization", 0.75),  # session mean above the 70% planned ceiling
    ("peak_requests_per_minute", 12.0),  # pacing exceeded the cap plus burst one
])
def test_request_budget_blocks_on_planned_use_or_broken_pacing(
    tmp_path: Path, field: str, value: float,
) -> None:
    pilot = _complete_evidence(tmp_path)
    capacity = json.loads((pilot / CAPACITY_FILE).read_text())
    capacity["summary"][field] = value
    (pilot / CAPACITY_FILE).write_text(json.dumps(capacity))
    blocked = evaluate_release(pilot)
    assert [g["gate"] for g in blocked["gates"] if not g["passed"]] == ["request_budget"]
