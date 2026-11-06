"""Operator status: stalled scopes, unknown freshness, backlog, disk, backups (T17, 06).

Thresholds are the 06 pilot objectives (project defaults, not provider SLAs).
Contract IDs and exception strings stay out of metric-style labels; details live
in DB tables and logs.
"""

from __future__ import annotations

import shutil
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, text

from marketagg.calendar import SessionCalendar

DISK_WARN, DISK_STOP_BULK, DISK_SUSPEND = 0.70, 0.80, 0.90
BULK_KINDS = frozenset({"daily_backfill", "minute_sweep", "chain_broad", "chain_other"})
CADENCE = {"chain_priority": timedelta(minutes=5), "bars_1m": timedelta(minutes=1)}


def disk_level(paths: Sequence[Path]) -> tuple[str, float]:
    usage = 0.0
    for path in paths:
        if path.exists():
            total, used, _ = shutil.disk_usage(path)
            usage = max(usage, used / total if total else 0.0)
    if usage >= DISK_SUSPEND:
        return "suspend", usage
    if usage >= DISK_STOP_BULK:
        return "stop_bulk", usage
    if usage >= DISK_WARN:
        return "warn", usage
    return "ok", usage


def allowed_kinds(level: str, kinds: Sequence[str]) -> list[str]:
    """Jobs a worker may claim under disk pressure (no unverified deletion is attempted)."""
    if level == "suspend":
        return []
    if level == "stop_bulk":
        return [k for k in kinds if k not in BULK_KINDS]
    return list(kinds)


@dataclass(frozen=True)
class StatusReport:
    severity: str
    sections: dict[str, Any]


def operator_status(engine: Engine, calendar: SessionCalendar, now: datetime, *,
                    data_paths: Sequence[Path] = ()) -> StatusReport:
    alerts: list[dict[str, str]] = []
    session = calendar.session_at(now)
    with engine.connect() as conn:
        scopes = [dict(r) for r in conn.execute(text("""
            SELECT x.provider_id, x.scope->>'symbol' AS symbol, x.scope->>'kind' AS kind,
                   x.scope->>'expiration' AS expiration, max(x.fetch_completed_at) AS last_fetch
            FROM observation_batches x
            WHERE x.status='published' AND x.batch_kind IN ('chain','bars')
              AND x.scope ? 'symbol'
            GROUP BY 1, 2, 3, 4""")).mappings()]
        backlog = conn.execute(text("SELECT count(*) FROM collection_jobs "
                                    "WHERE status='pending' AND not_before <= :now"),
                               {"now": now}).scalar_one()
        failed: dict[str, int] = dict(conn.execute(text("""
            SELECT kind, count(*) FROM collection_jobs
            WHERE status IN ('failed','skipped') AND scheduled_for > :since GROUP BY kind"""),
            {"since": now - timedelta(days=1)}).all())
        circuits = [dict(r) for r in conn.execute(text(
            "SELECT provider_id, endpoint, state, open_until FROM provider_circuits "
            "WHERE state <> 'closed'")).mappings()]
        freshness = conn.execute(text("""
            SELECT count(*) FILTER (WHERE provider_event_at IS NULL), count(*)
            FROM option_snapshots WHERE observed_at > :since"""),
            {"since": now - timedelta(days=1)}).one()
        backup = conn.execute(text("""
            SELECT max(finished_at) FROM backup_runs WHERE state='verified'""")).scalar_one()
        incidents = [dict(r) for r in conn.execute(text(
            "SELECT kind, severity, count(*) AS n FROM operational_incidents "
            "WHERE resolved_at IS NULL GROUP BY kind, severity")).mappings()]
        gaps: dict[str, int] = dict(conn.execute(text(
            "SELECT status, count(*) FROM data_gaps GROUP BY status")).all())
        scheduler = conn.execute(text("SELECT holder, heartbeat_at FROM scheduler_state"
                                      )).one_or_none()
    for scope in scopes:
        cadence = CADENCE.get(scope["kind"] or "")
        if session is None or cadence is None or scope["last_fetch"] is None:
            continue
        age = now - scope["last_fetch"]
        scope["age_seconds"] = age.total_seconds()
        if age > 3 * cadence:
            alerts.append({"severity": "critical", "alert": "ingestion_stalled",
                           "scope": f"{scope['symbol']}:{scope['kind']}"})
        elif age > 2 * cadence:
            alerts.append({"severity": "warning", "alert": "ingestion_late",
                           "scope": f"{scope['symbol']}:{scope['kind']}"})
    if scheduler is not None and session is not None and \
            now - scheduler.heartbeat_at > timedelta(minutes=5):
        alerts.append({"severity": "critical", "alert": "scheduler_lag", "scope": "scheduler"})
    level, usage = disk_level(data_paths)
    if level != "ok":
        alerts.append({"severity": "critical" if level == "suspend" else "warning",
                       "alert": f"disk_{level}", "scope": "disk"})
    if backup is None or now - backup > timedelta(hours=48):
        alerts.append({"severity": "critical", "alert": "backup_stale", "scope": "backup"})
    elif now - backup > timedelta(hours=26):
        alerts.append({"severity": "warning", "alert": "backup_stale", "scope": "backup"})
    for circuit in circuits:
        alerts.append({"severity": "critical" if circuit["state"] == "disabled" else "warning",
                       "alert": f"provider_{circuit['state']}", "scope": circuit["provider_id"]})
    unknown, total = freshness
    severity = ("critical" if any(a["severity"] == "critical" for a in alerts)
                else "warning" if alerts else "ok")
    return StatusReport(severity, {
        "now": now.isoformat(), "in_session": session is not None,
        "scopes": scopes, "backlog_ready_jobs": backlog, "failed_or_skipped_24h": failed,
        "circuits": circuits,
        "quote_freshness": {"unknown": unknown, "total": total,
                            "unknown_ratio": (unknown / total) if total else None},
        "disk": {"level": level, "max_usage": round(usage, 4)},
        "backup_last_verified": backup.isoformat() if backup else None,
        "incidents": incidents, "gaps": gaps, "alerts": alerts,
        "notifications": "disabled until an authorized destination is configured",
    })
