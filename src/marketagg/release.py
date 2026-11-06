"""Five-session pilot measurement and the MVP release gate (T19, docs 05/06).

``measure_capacity`` turns what the database actually recorded during the pilot into
a capacity report: per-session request rate, per-scope chain coverage (every skipped
or shed slot counted), minute coverage, fetch-to-knowledge lag, row counts, measured
bytes per row, projected disk headroom and backups taken during ingestion.

``evaluate_release`` checks that report plus the acceptance manifest and the source
review against the mandatory gates. It fails closed: missing or fixture-only evidence
never allows a release.
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, text

from marketagg.calendar import Session, SessionCalendar

CAPACITY_FILE = "capacity.json"
LIMITATION_FILE = "cadence-limitation.json"
DECISION_FILE = "release-decision.json"
APPROVAL = re.compile(r"^Release decision:\s*APPROVED\b", re.MULTILINE)
MVP_SCENARIOS = ("A01", "A02", "A03", "A04", "A05", "A06", "A07", "A08", "A09",
                 "temporal_adversaries")

# Pilot objectives (06). Thresholds are gates, not tunables to get a green check.
CHAIN_COVERAGE_MIN = 0.95
CHAIN_ON_TIME = timedelta(seconds=120)
MINUTE_COVERAGE_MIN = 0.99
LAG_P95_MAX_SECONDS = 5.0
HEADROOM_DAYS_MIN = 30.0
UTILIZATION_MAX = 0.70  # planned (session mean) use of the self-imposed budget (03)
PACING_BURST = 1  # token-bucket burst (03): a measured minute may reach the cap plus this
STOP_BULK_FRACTION = 0.80  # disk policy: bulk work stops at 80%
WINDOW_PAD = timedelta(minutes=30)
OBSERVATION_TABLES = ("bar_observations", "option_snapshots", "underlying_marks")


# -- Measurement -------------------------------------------------------------------------

def pilot_sessions(engine: Engine, calendar: SessionCalendar, provider_id: str, count: int,
                   last: date | None = None) -> list[Session]:
    """The ``count`` calendar sessions ending at ``last`` (default: latest with 1m data)."""
    if last is None:
        with engine.connect() as conn:
            last = conn.execute(text(
                "SELECT max(session_date) FROM bar_observations "
                "WHERE provider_id=:p AND timeframe='1m'"), {"p": provider_id}).scalar_one()
        if last is None:
            return []
    return calendar.sessions(last - timedelta(days=count * 2 + 10), last)[-count:]


def _session_metrics(engine: Engine, provider_id: str, session: Session) -> dict[str, Any]:
    window: dict[str, Any] = {
        "p": provider_id, "start": session.open_at - WINDOW_PAD,
        "end": session.close_at + WINDOW_PAD, "open": session.open_at,
        "close": session.close_at, "on_time": CHAIN_ON_TIME, "day": session.session_date}
    with engine.connect() as conn:
        requests = conn.execute(text("""
            SELECT coalesce(sum(r.endpoint_requests), 0) AS endpoint,
                   sum(r.transport_requests) AS transport, count(*) AS runs
            FROM ingestion_runs r JOIN collection_jobs j ON j.id = r.job_id
            WHERE j.provider_id=:p AND r.started_at >= :start AND r.started_at < :end"""),
            window).one()
        # Runs record a request count, not per-request times. Spread each run's requests
        # evenly over the minutes it actually ran: crediting a whole 24-request run to
        # its start minute reported 34/min while pacing held 10/min.
        peak = conn.execute(text("""
            SELECT coalesce(max(n), 0) FROM (
              SELECT m.minute, sum(CASE WHEN r.dur > 0 THEN r.endpoint_requests
                  * extract(epoch FROM least(r.done, m.minute + interval '1 minute')
                                       - greatest(r.started_at, m.minute)) / r.dur
                  ELSE r.endpoint_requests END) AS n
              FROM (SELECT r.started_at, r.endpoint_requests,
                           coalesce(r.finished_at, r.started_at) AS done,
                           extract(epoch FROM coalesce(r.finished_at, r.started_at)
                                              - r.started_at) AS dur
                    FROM ingestion_runs r JOIN collection_jobs j ON j.id = r.job_id
                    WHERE j.provider_id=:p AND r.started_at >= :start AND r.started_at < :end
                      AND r.endpoint_requests > 0) r
              CROSS JOIN LATERAL generate_series(date_trunc('minute', r.started_at),
                                                 date_trunc('minute', r.done),
                                                 interval '1 minute') AS m(minute)
              GROUP BY 1) m"""), window).scalar_one()
        errors: dict[str, int] = dict(conn.execute(text("""
            SELECT r.error_code, count(*) FROM ingestion_runs r
            JOIN collection_jobs j ON j.id = r.job_id
            WHERE j.provider_id=:p AND r.started_at >= :start AND r.started_at < :end
              AND r.error_code IS NOT NULL GROUP BY 1"""), window).all())
        scopes = conn.execute(text("""
            SELECT j.scope->>'symbol' AS symbol, j.scope->>'expiration' AS expiration,
                   count(*) AS intended,
                   count(*) FILTER (WHERE j.status='skipped') AS skipped,
                   count(*) FILTER (WHERE j.status='partial') AS partial,
                   count(*) FILTER (WHERE j.status='failed') AS failed,
                   count(*) FILTER (WHERE j.status='succeeded' AND EXISTS (
                       SELECT 1 FROM ingestion_runs r WHERE r.job_id = j.id
                         AND r.status='succeeded'
                         AND r.finished_at - j.scheduled_for <= :on_time)) AS on_time
            FROM collection_jobs j
            WHERE j.provider_id=:p AND j.kind='chain_priority'
              AND j.scheduled_for >= :open AND j.scheduled_for <= :close
            GROUP BY 1, 2 ORDER BY 1, 2"""), window).mappings().all()
        minutes = conn.execute(text("""
            SELECT i.symbol, count(DISTINCT b.interval_start) AS observed
            FROM instrument_identifiers i LEFT JOIN bar_observations b
              ON b.instrument_id = i.instrument_id AND b.provider_id = :p
             AND b.timeframe = '1m' AND b.session_date = :day
            WHERE i.provider_id = :p AND i.status = 'active' AND i.valid_from <= :day
              AND (i.valid_to IS NULL OR i.valid_to > :day)
            GROUP BY 1 ORDER BY 1"""), window).all()
        lag = conn.execute(text("""
            SELECT batch_kind, count(*) AS n,
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM
                       known_at - fetch_completed_at)) AS p50,
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM
                       known_at - fetch_completed_at)) AS p95
            FROM observation_batches
            WHERE provider_id=:p AND status='published'
              AND fetch_completed_at >= :start AND fetch_completed_at < :end
            GROUP BY 1 ORDER BY 1"""), window).mappings().all()
        options = conn.execute(text("""
            SELECT count(*) AS rows, avg((provider_event_at IS NOT NULL)::int) AS event_known
            FROM option_snapshots WHERE provider_id=:p
              AND observed_at >= :start AND observed_at < :end"""), window).one()
    scope_rows = [dict(s) | {"coverage": (s["on_time"] / s["intended"]) if s["intended"]
                             else None} for s in scopes]
    window_minutes = (window["end"] - window["start"]).total_seconds() / 60
    session_minutes = (session.close_at - session.open_at).total_seconds() / 60
    return {
        "session_date": session.session_date.isoformat(),
        "open_at": session.open_at.isoformat(), "close_at": session.close_at.isoformat(),
        "requests": {"endpoint": int(requests.endpoint),
                     "transport": None if requests.transport is None
                     else int(requests.transport),
                     "runs": int(requests.runs),
                     "effective_per_minute": round(requests.endpoint / window_minutes, 3),
                     "session_per_minute": round(requests.endpoint / session_minutes, 3),
                     "peak_per_minute": round(float(peak), 2)},
        "errors": {k: int(v) for k, v in errors.items()},
        "chain_scopes": scope_rows,
        "skipped_slots": sum(s["skipped"] for s in scope_rows),
        "minute_coverage": {sym: {"observed": int(n), "expected": session.minutes_count,
                                  "coverage": n / session.minutes_count}
                            for sym, n in minutes},
        "fetch_to_known_lag_seconds": {r["batch_kind"]: {
            "batches": int(r["n"]), "p50": float(r["p50"]), "p95": float(r["p95"])}
            for r in lag},
        "option_rows": int(options.rows),
        "quote_event_time_known_ratio": None if options.event_known is None
        else float(options.event_known),
    }


def _storage(engine: Engine, provider_id: str) -> dict[str, Any]:
    with engine.connect() as conn:
        tables = {t: int(conn.execute(text("""
            SELECT coalesce(sum(pg_total_relation_size(relid)), 0)
            FROM pg_partition_tree(CAST(:t AS regclass))"""), {"t": t}).scalar_one())
            for t in OBSERVATION_TABLES}
        rows = {t: int(conn.execute(text(
            f"SELECT count(*) FROM {t} WHERE provider_id=:p"),  # noqa: S608 - fixed names
            {"p": provider_id}).scalar_one()) for t in OBSERVATION_TABLES}
        database = int(conn.execute(text(
            "SELECT pg_database_size(current_database())")).scalar_one())
        payload = int(conn.execute(text(
            "SELECT coalesce(sum(byte_count), 0) FROM payload_objects "
            "WHERE provider_id=:p AND body_deleted_at IS NULL"), {"p": provider_id}
        ).scalar_one())
        archive = int(conn.execute(text(
            "SELECT coalesce(sum(byte_count), 0) FROM archive_manifest "
            "WHERE state IN ('active','verified')")).scalar_one())
        wal = conn.execute(text("SELECT wal_bytes, stats_reset FROM pg_stat_wal")).one()
        sessions_stored = int(conn.execute(text(
            "SELECT count(DISTINCT session_date) FROM bar_observations "
            "WHERE provider_id=:p AND timeframe='1m'"), {"p": provider_id}).scalar_one())
    per_row = {t: (tables[t] / rows[t]) if rows[t] else None for t in OBSERVATION_TABLES}
    return {"table_bytes": tables, "rows": rows, "bytes_per_row_with_indexes": per_row,
            "database_bytes": database, "payload_bytes": payload, "archive_bytes": archive,
            "wal_bytes_since_stats_reset": int(wal.wal_bytes),
            "wal_stats_reset": wal.stats_reset, "sessions_stored": sessions_stored}


def _headroom(storage: dict[str, Any], disk_paths: Sequence[Path],
              max_local_gib: float | None) -> dict[str, Any]:
    local = storage["database_bytes"] + storage["payload_bytes"] + storage["archive_bytes"]
    sessions = storage["sessions_stored"]
    per_session = local / sessions if sessions else None
    limits: dict[str, float] = {}
    for path in disk_paths:
        usage = shutil.disk_usage(path)
        limits[f"disk:{path}"] = usage.total * STOP_BULK_FRACTION - usage.used
    if max_local_gib is not None:
        limits["MAX_LOCAL_DATA_GIB"] = max_local_gib * 2**30 - local
    available = min(limits.values()) if limits else None
    days = None
    if per_session and available is not None:
        days = max(available, 0) / per_session * 7 / 5  # trading sessions -> calendar days
    return {"local_bytes": local, "bytes_per_session": per_session,
            "available_bytes_by_limit": limits, "projected_headroom_days": days,
            "basis": "local DB+payload+archive bytes / sessions stored; stop-bulk at 80% disk"}


def measure_capacity(engine: Engine, calendar: SessionCalendar, *, provider_id: str,
                     sessions: int, last: date | None = None,
                     requests_per_minute: int, disk_paths: Sequence[Path] = (),
                     max_local_gib: float | None = None,
                     now: datetime | None = None) -> dict[str, Any]:
    chosen = pilot_sessions(engine, calendar, provider_id, sessions, last)
    per_session = [_session_metrics(engine, provider_id, s) for s in chosen]
    storage = _storage(engine, provider_id)
    with engine.connect() as conn:
        backups = conn.execute(text("""
            SELECT started_at, finished_at, state FROM backup_runs
            WHERE state='verified' ORDER BY started_at""")).all()
    during = [b for b in backups for s in chosen
              if b.started_at < s.close_at and (b.finished_at or b.started_at) > s.open_at]
    scopes = [sc for s in per_session for sc in s["chain_scopes"]]
    coverages = [sc["coverage"] for sc in scopes if sc["coverage"] is not None]
    minute = [m["coverage"] for s in per_session for m in s["minute_coverage"].values()]
    lag95 = [v["p95"] for s in per_session for v in s["fetch_to_known_lag_seconds"].values()]
    known = [s["quote_event_time_known_ratio"] for s in per_session
             if s["quote_event_time_known_ratio"] is not None]
    peak = max((s["requests"]["peak_per_minute"] for s in per_session), default=0)
    planned = max((s["requests"]["session_per_minute"] for s in per_session), default=0)
    summary = {
        "sessions_measured": len(per_session),
        "min_scope_chain_coverage": min(coverages) if coverages else None,
        "skipped_slots": sum(s["skipped_slots"] for s in per_session),
        "min_minute_coverage": min(minute) if minute else None,
        "max_fetch_to_known_p95_seconds": max(lag95) if lag95 else None,
        "peak_requests_per_minute": peak,
        "request_budget_utilization": peak / requests_per_minute if requests_per_minute
        else None,
        "planned_request_utilization": planned / requests_per_minute if requests_per_minute
        else None,
        "quote_event_time_known_ratio": min(known) if known else None,
        "freshness_claim": "provider_event_timestamps" if known and min(known) == 1.0
        else "collection_cadence_only",
        "verified_backups_during_sessions": len(during),
    }
    return {
        "format": "marketagg-capacity/1",
        "measured_at": (now or datetime.now(UTC)).isoformat(),
        "provider_id": provider_id,
        "live_source": provider_id != "fake",
        "requests_per_minute_configured": requests_per_minute,
        "sessions": per_session,
        "storage": storage,
        "headroom": _headroom(storage, disk_paths, max_local_gib),
        "summary": summary,
        "uncertainty": [
            "One measured week may be unrepresentative (events, expiries, volatility).",
            "Provider throttling behaviour is observed, not documented; limits may change.",
            "Quote event times are unknown unless the provider supplies them.",
        ],
    }


# -- Release gate ------------------------------------------------------------------------

def _gate(name: str, passed: bool, detail: str) -> dict[str, Any]:
    return {"gate": name, "passed": passed, "detail": detail}


def _latest_acceptance(root: Path) -> tuple[Path | None, dict[str, Any] | None, str]:
    manifests = sorted(root.glob("*/manifest.json"))
    if not manifests:
        return None, None, f"no acceptance manifest under {root}"
    path = manifests[-1]
    body = path.read_bytes()
    recorded = (path.parent / "manifest.sha256").read_text().split()[0] \
        if (path.parent / "manifest.sha256").is_file() else None
    if recorded != hashlib.sha256(body).hexdigest():
        return path, None, f"{path}: checksum missing or mismatched"
    return path, json.loads(body), str(path)


def evaluate_release(report: Path, *, acceptance_root: Path | None = None,
                     feasibility_root: Path | None = None) -> dict[str, Any]:
    """Evaluate every mandatory gate; ``release_allowed`` only if all pass."""
    acceptance_root = acceptance_root or report.parent / "acceptance"
    feasibility_root = feasibility_root or report.parent / "feasibility"
    gates: list[dict[str, Any]] = []
    capacity_path = report / CAPACITY_FILE
    capacity: dict[str, Any] | None = None
    if capacity_path.is_file():
        capacity = json.loads(capacity_path.read_text())
    gates.append(_gate("capacity_report_present", capacity is not None, str(capacity_path)))
    cap = capacity or {}
    summary: dict[str, Any] = cap.get("summary", {})
    provider = cap.get("provider_id")

    gates.append(_gate("live_source_measured", bool(cap.get("live_source")) and
                       provider not in (None, "fake"),
                       f"provider={provider!r}; fixture data cannot release the live MVP"))
    sessions = cap.get("sessions", [])
    with_data = [s for s in sessions if s.get("requests", {}).get("endpoint", 0) > 0]
    gates.append(_gate("five_sessions", len(with_data) >= 5,
                       f"{len(with_data)} sessions with recorded requests"))

    review = feasibility_root / "live" / str(provider) / "source-review.md"  # live evidence
    review_text = review.read_text() if review.is_file() else ""
    approved = bool(APPROVAL.search(review_text)) and "UNRESOLVED" not in review_text
    gates.append(_gate("source_review_approved", approved,
                       f"{review}: needs 'Release decision: APPROVED' and no UNRESOLVED item"))

    path, manifest, detail = _latest_acceptance(acceptance_root)
    scenarios = (manifest or {}).get("scenarios", {})
    failing = [s for s in MVP_SCENARIOS if scenarios.get(s, {}).get("status") != "passed"]
    gates.append(_gate("acceptance_signed_off",
                       manifest is not None and manifest.get("signed_off") is True
                       and not failing,
                       detail + (f"; not passed: {failing}" if failing else "")))
    measured_rto = any("a07_rto_seconds" in m for m in
                       (manifest or {}).get("measurements", {}).values())
    gates.append(_gate("offsite_restore_drill",
                       measured_rto and summary.get("verified_backups_during_sessions", 0) >= 1,
                       "A07 restore measured in acceptance and a verified backup taken "
                       "during a pilot session"))

    coverage = summary.get("min_scope_chain_coverage")
    limitation = report / LIMITATION_FILE
    declared = limitation.is_file() and json.loads(limitation.read_text()).get(
        "measured_cadence_minutes", 0) > 5
    coverage_ok = coverage is not None and coverage >= CHAIN_COVERAGE_MIN
    gates.append(_gate("priority_chain_coverage", coverage_ok or declared,
                       f"min per-scope on-time coverage {coverage} (>= {CHAIN_COVERAGE_MIN}"
                       f"{' or published measured cadence limitation' if not coverage_ok else ''}"
                       f"); skipped slots counted: {summary.get('skipped_slots')}"))
    gates.append(_gate("skipped_scopes_counted",
                       bool(sessions) and all("skipped_slots" in s for s in sessions),
                       "every session reports its skipped/shed slots"))
    minute = summary.get("min_minute_coverage")
    gates.append(_gate("minute_coverage", minute is not None and minute >= MINUTE_COVERAGE_MIN,
                       f"min {minute} (>= {MINUTE_COVERAGE_MIN})"))
    lag = summary.get("max_fetch_to_known_p95_seconds")
    gates.append(_gate("processing_lag", lag is not None and lag <= LAG_P95_MAX_SECONDS,
                       f"worst p95 {lag}s (<= {LAG_P95_MAX_SECONDS}s)"))
    headroom = cap.get("headroom", {}).get("projected_headroom_days")
    gates.append(_gate("disk_headroom", headroom is not None and headroom >= HEADROOM_DAYS_MIN,
                       f"projected {headroom} days (>= {HEADROOM_DAYS_MIN})"))
    # 03: a 70% planned-utilisation ceiling on the session, and pacing (burst one) holding
    # the cap in every minute. Boundary minutes run at the cap by design.
    planned = summary.get("planned_request_utilization")
    peak = summary.get("peak_requests_per_minute")
    rpm = cap.get("requests_per_minute_configured")
    gates.append(_gate("request_budget",
                       planned is not None and planned <= UTILIZATION_MAX and peak is not None
                       and rpm is not None and peak <= rpm + PACING_BURST,
                       f"planned utilisation {planned} (<= {UTILIZATION_MAX}); peak {peak}/min "
                       f"(<= cap {rpm} + burst {PACING_BURST})"))
    known = summary.get("quote_event_time_known_ratio")
    honest = summary.get("freshness_claim") == "collection_cadence_only" or known == 1.0
    gates.append(_gate("no_execution_grade_freshness_claim", bool(summary) and honest,
                       f"event-time known ratio {known}; claim {summary.get('freshness_claim')}"))

    result = {"release_allowed": all(g["passed"] for g in gates),
              "checked_at": datetime.now(UTC).isoformat(), "report": str(report),
              "acceptance_manifest": str(path) if path else None, "gates": gates}
    if report.is_dir():
        (report / DECISION_FILE).write_text(json.dumps(result, indent=2, default=str))
    return result
