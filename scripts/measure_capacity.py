"""Measure the five-session pilot from recorded database facts (T19); see ``marketagg.release``.

    uv run python scripts/measure_capacity.py --sessions 5 --output reports/pilot \
        [--profile production] [--env-file PATH] [--last-session YYYY-MM-DD]

Writes ``capacity.json`` and a readable ``capacity.md``. Measurements come only from
what the pilot stored (runs, jobs, batches, relation sizes, backups); nothing is
extrapolated beyond the stated headroom projection. Then run
``uv run marketagg release check --report reports/pilot``.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path
from typing import Any

from marketagg.calendar import SessionCalendar
from marketagg.release import CAPACITY_FILE, measure_capacity
from marketagg.services import engine_for
from marketagg.settings import load_settings, parse_env_file


def _markdown(report: dict[str, Any]) -> str:
    s = report["summary"]
    lines = [
        f"# Pilot capacity report: {report['provider_id']}",
        "",
        f"Measured at {report['measured_at']}. Live source: **{report['live_source']}**.",
        "",
        "| Measure | Value |",
        "|---|---|",
        *(f"| {k} | {v} |" for k, v in s.items()),
        f"| projected_headroom_days | {report['headroom']['projected_headroom_days']} |",
        f"| bytes_per_session | {report['headroom']['bytes_per_session']} |",
        "",
        "## Per session",
        "",
        "| Session | Requests | Peak/min | Skipped slots | Option rows | Errors |",
        "|---|---|---|---|---|---|",
        *(f"| {x['session_date']} | {x['requests']['endpoint']} | "
          f"{x['requests']['peak_per_minute']} | {x['skipped_slots']} | {x['option_rows']} | "
          f"{x['errors'] or '-'} |" for x in report["sessions"]),
        "",
        "## Uncertainty",
        "",
        *(f"- {u}" for u in report["uncertainty"]),
        "",
    ]
    if not report["live_source"]:
        lines[2:2] = ["", "> Fixture data: this report exercises the tooling only and "
                      "cannot satisfy the release gate.", ""]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--sessions", type=int, default=5)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--profile", default=None)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--last-session", type=date.fromisoformat)
    args = parser.parse_args()
    environ = dict(parse_env_file(args.env_file)) if args.env_file else {}
    settings = load_settings(args.profile, {**environ, **os.environ})
    engine = engine_for(settings, "marketagg-capacity", statement_timeout_ms=None)
    try:
        report = measure_capacity(
            engine, SessionCalendar(), provider_id=settings.provider.value,
            sessions=args.sessions, last=args.last_session,
            requests_per_minute=settings.requests_per_minute,
            disk_paths=[p for p in (settings.data_root,) if p.exists()],
            max_local_gib=settings.max_local_data_gib)
    finally:
        engine.dispose()
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / CAPACITY_FILE).write_text(json.dumps(report, indent=2, default=str))
    (args.output / "capacity.md").write_text(_markdown(report))
    print(json.dumps(report["summary"], indent=2, default=str))
    measured = report["summary"]["sessions_measured"]
    if measured < args.sessions:
        print(f"warning: only {measured} of {args.sessions} sessions found", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
