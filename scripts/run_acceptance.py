"""Run the end-to-end acceptance scenarios and write an evidence manifest (T18, doc 05).

Run: uv run python scripts/run_acceptance.py --profile fixture [--signing-key-file PATH]

Needs MARKETAGG_TEST_ADMIN_DSN (disposable PostgreSQL 18; databases named
marketagg_test_*). Writes reports/acceptance/<UTC stamp>-<profile>/ with:

- junit.xml: raw pytest results;
- measurements/*.json: values measured by individual scenarios (RPO/RTO, latencies);
- manifest.json: versions, image digests, seed, fixture checksums, per-scenario
  results, measurements, elapsed time and documented limits;
- manifest.sha256, plus manifest.hmac when --signing-key-file is given.

Exit status is 0 only when every MVP scenario (A01-A09) and every temporal adversary
passed with nothing skipped. Manual review items are listed, never marked as passed.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import platform
import re
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
ADMIN_ENV = "MARKETAGG_TEST_ADMIN_DSN"
OUT_ENV = "MARKETAGG_ACCEPTANCE_OUT"

TEST_PATHS = [
    "tests/acceptance/test_pipeline.py",
    "tests/acceptance/test_restore.py",
    "tests/acceptance/test_ui.py",
    "tests/contracts/test_api.py",
    "tests/integration/test_asof.py",
    "tests/integration/test_exports.py",
]

# Scenario -> (description, node-id patterns). A test may support several scenarios.
SCENARIOS: dict[str, tuple[str, list[str]]] = {
    "A01": ("Normal fixture session", [r"test_pipeline\.py::test_a01_"]),
    "A02": ("Candles and synchronised panels", [r"test_pipeline\.py::test_a02_"]),
    "A03": ("Correction and causal export", [
        r"test_pipeline\.py::test_a03_", r"test_exports\.py::test_later_correction",
        r"test_exports\.py::test_same_pinned_request", r"test_asof\.py::test_backfill_"]),
    "A04": ("Scheduler contention and restart", [r"test_pipeline\.py::test_a04_",
                                                 r"test_exports\.py::test_restart_recovery"]),
    "A05": ("Provider failures and source drift", [r"test_pipeline\.py::test_a05_"]),
    "A06": ("Archive round trip", [r"test_pipeline\.py::test_a06_"]),
    "A07": ("Clean disaster recovery", [r"test_restore\.py::"]),
    "A08": ("Private API and UI", [
        r"test_ui\.py::", r"test_api\.py::", r"test_exports\.py::test_csv_formula",
        r"test_exports\.py::test_signed_downloads"]),
    "A09": ("Resource pressure (scaled fixture)", [r"test_pipeline\.py::test_a09_"]),
    "temporal_adversaries": ("Release-blocking temporal adversaries and mutation checks",
                             [r"test_asof\.py::"]),
}
NOT_IN_SCOPE = {"A10": "Licensed provider substitution is Phase 8 (T24), not an MVP gate."}
MANUAL_REVIEW = [
    "A08: review UI screenshots at desktop and narrow widths (not automated here).",
    "A09: fixture timings are a scaled small-fixture envelope, not pilot capacity evidence.",
]
LIMITS = [
    "Fixture profile only: synthetic fake-provider data; no live source was contacted.",
    "A09 loads 5 sessions x 2 symbols; the 90-day pilot-equivalent load is a T19 measurement.",
    "A07 RPO is that of a nightly logical backup (time since last verified backup); "
    "a <=15 min RPO needs WAL archiving and is not claimed.",
    "docker compose config validation is skipped when the docker CLI is absent.",
]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _versions() -> dict[str, Any]:
    packages = {}
    for name in ("marketagg", "sqlalchemy", "psycopg", "fastapi", "pyarrow", "pydantic",
                 "exchange-calendars", "pytest", "streamlit", "cryptography"):
        try:
            packages[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            packages[name] = None
    server = None
    dsn = os.environ.get(ADMIN_ENV)
    if dsn:
        from sqlalchemy import create_engine, text

        from marketagg.db.engine import normalize_url

        engine = create_engine(normalize_url(dsn))
        try:
            with engine.connect() as conn:
                server = conn.execute(text("SHOW server_version")).scalar_one()
        finally:
            engine.dispose()
    return {"python": platform.python_version(), "platform": platform.platform(),
            "postgresql_server": server, "packages": packages,
            "uv_lock_sha256": _sha256(ROOT / "uv.lock")}


def _image_digests() -> dict[str, list[str]]:
    found: dict[str, list[str]] = {}
    for rel in ("deploy/compose.yaml", "deploy/Dockerfile"):
        found[rel] = sorted(set(re.findall(r"[\w./:-]+@sha256:[0-9a-f]{64}",
                                           (ROOT / rel).read_text())))
    return found


def _fixture_checksums() -> dict[str, str]:
    files = sorted((ROOT / "tests" / "fixtures").rglob("*.json"))
    files += [ROOT / "config" / "universe.yaml", ROOT / "config" / "policies.yaml"]
    return {str(p.relative_to(ROOT)): _sha256(p) for p in files}


def _seed() -> int:
    from marketagg.providers.fake import FakeScenario

    return FakeScenario().seed


def _junit_cases(path: Path) -> list[dict[str, Any]]:
    cases = []
    for case in ET.parse(path).getroot().iter("testcase"):
        classname, name = case.get("classname", ""), case.get("name", "")
        module = classname.replace(".", "/") + ".py"
        outcome = "passed"
        for tag in ("failure", "error", "skipped"):
            child = case.find(tag)
            if child is not None:
                outcome = {"failure": "failed", "error": "error"}.get(tag, "skipped")
                message = (child.get("message") or "")[:300]
                break
        else:
            message = ""
        cases.append({"nodeid": f"{module}::{name}", "outcome": outcome,
                      "seconds": float(case.get("time", 0)), "message": message})
    return cases


def _scenario_results(cases: list[dict[str, Any]]) -> dict[str, Any]:
    results: dict[str, Any] = {}
    for scenario, (title, patterns) in SCENARIOS.items():
        matched = [c for c in cases if any(re.search(p, c["nodeid"]) for p in patterns)]
        outcomes = {c["outcome"] for c in matched}
        if not matched:
            status = "missing"
        elif outcomes <= {"passed"}:
            status = "passed"
        elif outcomes & {"failed", "error"}:
            status = "failed"
        else:
            status = "incomplete"  # something skipped: never reported as passed
        results[scenario] = {"title": title, "status": status, "tests": len(matched),
                             "not_passed": [c for c in matched if c["outcome"] != "passed"]}
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--profile", choices=["fixture"], required=True,
                        help="only the fixture profile exists; live acceptance is T19")
    parser.add_argument("--output", type=Path, default=ROOT / "reports" / "acceptance")
    parser.add_argument("--signing-key-file", type=Path,
                        help="HMAC-SHA256 key used to sign manifest.json")
    parser.add_argument("pytest_args", nargs="*", help="extra arguments passed to pytest")
    args = parser.parse_args()
    if not os.environ.get(ADMIN_ENV):
        print(f"error: {ADMIN_ENV} must point at a disposable PostgreSQL 18 server; "
              "acceptance cannot pass without it", file=sys.stderr)
        return 2
    started_at = datetime.now(UTC)
    out = args.output / f"{started_at:%Y%m%dT%H%M%SZ}-{args.profile}"
    out.mkdir(parents=True)
    junit = out / "junit.xml"
    t0 = time.monotonic()
    command = [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
               "-W", "ignore::ResourceWarning", f"--junitxml={junit}", *TEST_PATHS,
               *args.pytest_args]
    proc = subprocess.run(command, cwd=ROOT, env={**os.environ, OUT_ENV: str(out)},
                          check=False)
    elapsed = time.monotonic() - t0
    cases = _junit_cases(junit) if junit.is_file() else []
    scenarios = _scenario_results(cases)
    measurements = {p.stem: json.loads(p.read_text())
                    for p in sorted((out / "measurements").glob("*.json"))}
    all_passed = proc.returncode == 0 and all(r["status"] == "passed"
                                               for r in scenarios.values())
    manifest = {
        "format": "marketagg-acceptance/1",
        "profile": args.profile,
        "started_at": started_at.isoformat(),
        "elapsed_seconds": round(elapsed, 1),
        "command": " ".join(command[1:]),
        "pytest_exit_code": proc.returncode,
        "versions": _versions(),
        "image_digests": _image_digests(),
        "fake_provider_seed": _seed(),
        "fixture_checksums": _fixture_checksums(),
        "totals": {o: sum(1 for c in cases if c["outcome"] == o)
                   for o in ("passed", "failed", "error", "skipped")},
        "scenarios": scenarios,
        "not_in_scope": NOT_IN_SCOPE,
        "measurements": measurements,
        "manual_review_required": MANUAL_REVIEW,
        "documented_limits": LIMITS,
        "signed_off": all_passed,
        "sign_off_rule": "all MVP scenarios and temporal adversaries passed, none skipped",
    }
    body = json.dumps(manifest, indent=2, sort_keys=True, default=str).encode()
    (out / "manifest.json").write_bytes(body)
    (out / "manifest.sha256").write_text(f"{hashlib.sha256(body).hexdigest()}  manifest.json\n")
    if args.signing_key_file:
        key = args.signing_key_file.read_bytes().strip()
        (out / "manifest.hmac").write_text(hmac.new(key, body, hashlib.sha256).hexdigest())
    print(f"\nacceptance evidence: {out}")
    for name, result in scenarios.items():
        print(f"  {name:<22} {result['status']:<10} ({result['tests']} tests)")
    print(f"  signed off: {all_passed}")
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
