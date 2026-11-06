"""Reproducible point-in-time exports (T15).

A dataset request pins the decision cutoff, availability mode, source, policies
and universe. Rows are selected through the causal research repository, sorted
deterministically and written to a temporary directory; only after every file is
written and checksummed is the directory atomically renamed and the manifest
appended. Failed/cancelled jobs never leave a published partial file. The
semantic hash depends only on content, so the same pinned request reproduces it
regardless of Parquet byte-level metadata.
"""

from __future__ import annotations

import hashlib
import hmac
import shutil
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import Engine, text

from marketagg.analytics.asof import eligible_options
from marketagg.analytics.quote_metrics import quote_metrics
from marketagg.clock import SYSTEM_CLOCK, Clock
from marketagg.domain import AvailabilityMode, PriceBasis, Timeframe
from marketagg.domain.primitives import UtcDateTime, canonical_dumps
from marketagg.research.manifests import file_sha256, semantic_hash, write_csv
from marketagg.research.repository import ResearchRepository

DATASET_SCHEMA_VERSION = 1
BAR_COLUMNS = ("interval_start", "interval_end", "session_date", "open", "high", "low",
               "close", "volume", "complete", "expected_minutes", "observed_minutes",
               "is_short_session_bucket", "quality_flags", "known_at", "bar_id",
               "content_sha256")
OPTION_COLUMNS = ("contract_id", "observed_at", "known_at", "provider_event_at",
                  "quote_time_basis", "last_trade_at", "bid", "ask", "midpoint", "last",
                  "reported_iv", "reported_volume", "reported_open_interest", "quality_flags",
                  "record_revision", "invalidated", "snapshot_id", "content_sha256")
TEXT_COLUMNS = {"quality_flags", "quote_time_basis", "contract_id", "bar_id", "snapshot_id",
                "content_sha256", "session_date"}


class DatasetRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    instrument_id: UUID
    provider_id: str
    timeframe: Timeframe
    start: UtcDateTime
    end: UtcDateTime
    decision_at: UtcDateTime
    mode: AvailabilityMode = AvailabilityMode.OBSERVED
    price_basis: PriceBasis = PriceBasis.PROVIDER_NATIVE
    contract_ids: tuple[UUID, ...] = Field(default=(), max_length=500)
    formats: tuple[str, ...] = ("parquet",)

    @model_validator(mode="after")
    def _bounds(self) -> DatasetRequest:
        if self.end <= self.start:
            raise ValueError("end must be after start")
        if self.decision_at < self.start:
            raise ValueError("decision_at precedes the window")
        if not set(self.formats) <= {"csv", "parquet"} or not self.formats:
            raise ValueError("formats must be a non-empty subset of csv, parquet")
        return self

    def sha256(self) -> str:
        return hashlib.sha256(canonical_dumps(self.model_dump(mode="json")).encode()).hexdigest()


class ExportTooLarge(ValueError):
    pass


@dataclass(frozen=True)
class JobView:
    id: UUID
    state: str
    manifest: dict[str, Any] | None
    error: dict[str, Any] | None
    files: dict[str, str]


class DatasetService:
    def __init__(self, engine: Engine, research: ResearchRepository, root: Path, *,
                 signing_key: bytes, clock: Clock = SYSTEM_CLOCK, max_rows: int = 5_000_000,
                 code_sha: str = "dev", expire_after: timedelta = timedelta(days=7),
                 page_size: int = 10_000) -> None:
        self.engine = engine
        self.research = research
        self.root = root
        self.signing_key = signing_key
        self.clock = clock
        self.max_rows = max_rows
        self.code_sha = code_sha
        self.expire_after = expire_after
        self.page_size = page_size

    # -- Job lifecycle ---------------------------------------------------------------

    def submit(self, request: DatasetRequest) -> UUID:
        job_id = uuid4()
        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO dataset_jobs (id, request, request_sha256, state, created_at,
                                          expires_at)
                VALUES (:id, CAST(:req AS jsonb), :sha, 'queued', :now, :exp)
            """), {"id": job_id, "req": canonical_dumps(request.model_dump(mode="json")),
                   "sha": request.sha256(), "now": self.clock.now(),
                   "exp": self.clock.now() + self.expire_after})
        return job_id

    def cancel(self, job_id: UUID) -> bool:
        with self.engine.begin() as conn:
            return bool(conn.execute(text(
                "UPDATE dataset_jobs SET state='cancelled', finished_at=:now "
                "WHERE id=:id AND state IN ('queued','running')"),
                {"id": job_id, "now": self.clock.now()}).rowcount)

    def recover(self) -> dict[str, int]:
        """Startup recovery: re-queue interrupted jobs and remove unpublished staging dirs."""
        with self.engine.begin() as conn:
            requeued = conn.execute(text(
                "UPDATE dataset_jobs SET state='queued', started_at=NULL "
                "WHERE state='running'")).rowcount
        removed = 0
        if self.root.exists():
            for staging in self.root.glob(".*-*"):
                if staging.is_dir():
                    shutil.rmtree(staging, ignore_errors=True)
                    removed += 1
        return {"requeued": int(requeued), "staging_removed": removed}

    def run_pending(self, limit: int = 5) -> list[UUID]:
        with self.engine.connect() as conn:
            queued = [UUID(str(r)) for r in conn.execute(text(
                "SELECT id FROM dataset_jobs WHERE state='queued' ORDER BY created_at "
                "LIMIT :n"), {"n": limit}).scalars()]
        for job_id in queued:
            self.run(job_id)
        return queued

    def run(self, job_id: UUID) -> JobView:
        with self.engine.begin() as conn:
            row = conn.execute(text(
                "UPDATE dataset_jobs SET state='running', started_at=:now "
                "WHERE id=:id AND state='queued' RETURNING request"),
                {"id": job_id, "now": self.clock.now()}).scalar_one_or_none()
        if row is None:
            return self.view(job_id)
        request = DatasetRequest.model_validate(row)
        staging = Path(tempfile.mkdtemp(prefix=f".{job_id}-", dir=self._ensure_root()))
        try:
            manifest = self._build(job_id, request, staging)
            if self._state(job_id) == "cancelled":
                raise _Cancelled
            final = self.root / str(job_id)
            staging.rename(final)  # atomic publication of the complete file set
            manifest_id = self._record_manifest(manifest, final)
            with self.engine.begin() as conn:
                conn.execute(text(
                    "UPDATE dataset_jobs SET state='succeeded', finished_at=:now, "
                    "manifest_id=:m WHERE id=:id AND state='running'"),
                    {"now": self.clock.now(), "m": manifest_id, "id": job_id})
        except _Cancelled:
            shutil.rmtree(staging, ignore_errors=True)
        except Exception as exc:
            shutil.rmtree(staging, ignore_errors=True)
            with self.engine.begin() as conn:
                conn.execute(text(
                    "UPDATE dataset_jobs SET state='failed', finished_at=:now, "
                    "error=CAST(:err AS jsonb) WHERE id=:id"),
                    {"now": self.clock.now(), "id": job_id, "err": canonical_dumps(
                        {"type": type(exc).__name__, "message": str(exc)[:500]})})
        return self.view(job_id)

    def view(self, job_id: UUID) -> JobView:
        with self.engine.connect() as conn:
            job = conn.execute(text("SELECT * FROM dataset_jobs WHERE id=:id"),
                               {"id": job_id}).mappings().one_or_none()
            if job is None:
                raise KeyError(str(job_id))
            manifest = None
            if job["manifest_id"] is not None:
                manifest = dict(conn.execute(text(
                    "SELECT * FROM dataset_manifests WHERE id=:id"),
                    {"id": job["manifest_id"]}).mappings().one())
        files = {}
        if manifest is not None:
            files = dict(manifest["eligibility_summary"].get("files", {}))
        return JobView(job_id, job["state"], manifest, job["error"], files)

    # -- Authorised downloads --------------------------------------------------------

    def sign(self, job_id: UUID, filename: str, expires: datetime) -> str:
        message = f"{job_id}|{filename}|{int(expires.timestamp())}".encode()
        return hmac.new(self.signing_key, message, hashlib.sha256).hexdigest()

    def resolve_download(self, job_id: UUID, filename: str, expires: int,
                         signature: str) -> Path:
        """Path for a signed download; never derived from client-supplied paths."""
        if self.clock.now().timestamp() > expires:
            raise PermissionError("download link expired")
        expected = hmac.new(self.signing_key, f"{job_id}|{filename}|{expires}".encode(),
                            hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, signature):
            raise PermissionError("invalid download signature")
        view = self.view(job_id)
        if view.state != "succeeded" or filename not in view.files:
            raise FileNotFoundError(filename)
        path = (self.root / str(job_id) / filename).resolve()
        if path.parent != (self.root / str(job_id)).resolve():
            raise PermissionError("path escapes export root")
        return path

    # -- Build -------------------------------------------------------------------------

    def _build(self, job_id: UUID, request: DatasetRequest, staging: Path) -> dict[str, Any]:
        bars = self._bars(request)
        options = self._options(request)
        if len(bars) + len(options) > self.max_rows:
            raise ExportTooLarge(f"{len(bars) + len(options)} rows exceed {self.max_rows}")
        bar_rows = [_bar_row(b) for b in bars]
        option_rows = [_option_row(o) for o in options]
        files: dict[str, str] = {}
        if "csv" in request.formats:
            write_csv(staging / "bars.csv", BAR_COLUMNS, bar_rows, TEXT_COLUMNS)
            write_csv(staging / "options.csv", OPTION_COLUMNS, option_rows, TEXT_COLUMNS)
            files["bars.csv"] = file_sha256(staging / "bars.csv")
            files["options.csv"] = file_sha256(staging / "options.csv")
        if "parquet" in request.formats:
            _write_parquet(staging / "bars.parquet", BAR_COLUMNS, bar_rows)
            _write_parquet(staging / "options.parquet", OPTION_COLUMNS, option_rows)
            files["bars.parquet"] = file_sha256(staging / "bars.parquet")
            files["options.parquet"] = file_sha256(staging / "options.parquet")
        lineage = [{"dataset": "bar_observations", "id": r["bar_id"],
                    "partition_time": r["interval_start"], "content_sha256": r["content_sha256"]}
                   for r in bar_rows] + [
                   {"dataset": "option_snapshots", "id": r["snapshot_id"],
                    "partition_time": r["observed_at"], "content_sha256": r["content_sha256"]}
                   for r in option_rows]
        _write_parquet(staging / "lineage.parquet",
                       ("dataset", "id", "partition_time", "content_sha256"), lineage)
        files["lineage.parquet"] = file_sha256(staging / "lineage.parquet")
        spec = {"request": request.model_dump(mode="json"),
                "schema_version": DATASET_SCHEMA_VERSION}
        content_hash = semantic_hash([*bar_rows, *option_rows], spec)
        with_partial = self.research.bars(
            instrument_id=request.instrument_id, provider_id=request.provider_id,
            timeframe=request.timeframe, start=request.start, end=request.end,
            decision_at=request.decision_at, price_basis=request.price_basis,
            mode=request.mode, include_partial=True)
        with self.engine.connect() as conn:
            caps = conn.execute(text("SELECT capabilities FROM providers WHERE id=:p"),
                                {"p": request.provider_id}).scalar_one()
            gaps = conn.execute(text("""
                SELECT kind, status, count(*) FROM data_gaps
                WHERE provider_id=:p AND (instrument_id=:i OR instrument_id IS NULL)
                  AND start_at < :end AND end_at > :start AND first_detected_at <= :d
                GROUP BY kind, status"""), {"p": request.provider_id, "i": request.instrument_id,
                                             "start": request.start, "end": request.end,
                                             "d": request.decision_at}).all()
        return {
            "job_id": str(job_id), "spec": spec, "semantic_sha256": content_hash,
            "files": files, "row_count": len(bar_rows) + len(option_rows),
            "eligibility": {
                "bars": len(bar_rows),
                "bars_excluded_incomplete": len(with_partial.items) - len(bar_rows),
                "options": len(option_rows),
                "options_invalidated": sum(1 for r in option_rows if r["invalidated"]),
                "options_unknown_quote_time": sum(1 for r in option_rows
                                                  if r["provider_event_at"] is None),
                "known_gaps": [{"kind": g[0], "status": g[1], "count": g[2]} for g in gaps],
            },
            "availability_mode": request.mode.value,
            "source_capabilities": caps,
            "policies": dict(self.research.policy_ids),
        }

    def _bars(self, request: DatasetRequest) -> list[Any]:
        items: list[Any] = []
        cursor = None
        while True:
            page = self.research.bars(
                instrument_id=request.instrument_id, provider_id=request.provider_id,
                timeframe=request.timeframe, start=request.start, end=request.end,
                decision_at=request.decision_at, price_basis=request.price_basis,
                mode=request.mode, limit=self.page_size, cursor=cursor)
            items.extend(page.items)
            if page.next_cursor is None or len(items) > self.max_rows:
                return items
            cursor = page.next_cursor

    def _options(self, request: DatasetRequest) -> list[Any]:
        if not request.contract_ids:
            return []
        with self.engine.connect() as conn:
            return eligible_options(conn, list(request.contract_ids), start=request.start,
                                    end=request.end, decision_at=request.decision_at,
                                    provider_id=request.provider_id, include_invalidated=True,
                                    limit=self.max_rows + 1)

    def _record_manifest(self, manifest: Mapping[str, Any], final: Path) -> UUID:
        manifest_id = uuid4()
        lineage_sha = manifest["files"]["lineage.parquet"]
        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO dataset_manifests (id, created_at, schema_version, query_spec,
                    input_refs, policy_versions, code_sha, output_uri, output_sha256, row_count,
                    eligibility_summary)
                VALUES (:id, :now, :sv, CAST(:spec AS jsonb), CAST(:refs AS jsonb),
                        CAST(:pol AS jsonb), :code, :uri, :sha, :rows, CAST(:elig AS jsonb))
            """), {"id": manifest_id, "now": self.clock.now(), "sv": DATASET_SCHEMA_VERSION,
                   "spec": canonical_dumps(manifest["spec"]),
                   "refs": canonical_dumps([{"lineage": "lineage.parquet",
                                             "sha256": lineage_sha}]),
                   "pol": canonical_dumps(manifest["policies"]), "code": self.code_sha,
                   "uri": f"export://{final.name}", "sha": manifest["semantic_sha256"],
                   "rows": manifest["row_count"],
                   "elig": canonical_dumps({**manifest["eligibility"],
                                            "files": manifest["files"],
                                            "availability_mode": manifest["availability_mode"],
                                            "source_capabilities":
                                                manifest["source_capabilities"]})})
        return manifest_id

    def _ensure_root(self) -> Path:
        self.root.mkdir(parents=True, exist_ok=True)
        return self.root

    def _state(self, job_id: UUID) -> str:
        with self.engine.connect() as conn:
            return str(conn.execute(text("SELECT state FROM dataset_jobs WHERE id=:id"),
                                    {"id": job_id}).scalar_one())


class _Cancelled(Exception):
    pass


def _bar_row(b: Any) -> dict[str, Any]:
    return {"interval_start": b.interval_start, "interval_end": b.interval_end,
            "session_date": b.session_date, "open": b.open, "high": b.high, "low": b.low,
            "close": b.close, "volume": b.volume, "complete": b.complete,
            "expected_minutes": b.expected_minutes, "observed_minutes": b.observed_minutes,
            "is_short_session_bucket": b.is_short_session_bucket,
            "quality_flags": list(b.quality_flags), "known_at": b.known_at,
            "bar_id": str(b.id), "content_sha256": b.content_sha256}


def _option_row(o: Any) -> dict[str, Any]:
    metrics = quote_metrics(o).values
    return {"contract_id": str(o.contract_id), "observed_at": o.observed_at,
            "known_at": o.known_at, "provider_event_at": o.provider_event_at,
            "quote_time_basis": o.field_semantics.get("quote_time_basis", "unknown"),
            "last_trade_at": o.last_trade_at, "bid": o.bid, "ask": o.ask,
            "midpoint": metrics["midpoint"], "last": o.last, "reported_iv": o.reported_iv,
            "reported_volume": o.reported_volume,
            "reported_open_interest": o.reported_open_interest,
            "quality_flags": list(o.quality_flags), "record_revision": o.record_revision,
            "invalidated": o.invalidated, "snapshot_id": str(o.id),
            "content_sha256": o.content_sha256}


_ARROW_TYPES: dict[str, pa.DataType] = {
    "interval_start": pa.timestamp("us", tz="UTC"), "interval_end": pa.timestamp("us", tz="UTC"),
    "observed_at": pa.timestamp("us", tz="UTC"), "known_at": pa.timestamp("us", tz="UTC"),
    "provider_event_at": pa.timestamp("us", tz="UTC"),
    "last_trade_at": pa.timestamp("us", tz="UTC"),
    "partition_time": pa.timestamp("us", tz="UTC"),
    "session_date": pa.date32(), "open": pa.decimal128(20, 8), "high": pa.decimal128(20, 8),
    "low": pa.decimal128(20, 8), "close": pa.decimal128(20, 8), "bid": pa.decimal128(20, 8),
    "ask": pa.decimal128(20, 8), "midpoint": pa.decimal128(28, 9), "last": pa.decimal128(20, 8),
    "volume": pa.int64(), "reported_volume": pa.int64(), "reported_open_interest": pa.int64(),
    "expected_minutes": pa.int32(), "observed_minutes": pa.int32(), "record_revision": pa.int32(),
    "complete": pa.bool_(), "is_short_session_bucket": pa.bool_(), "invalidated": pa.bool_(),
    "reported_iv": pa.float64(), "quality_flags": pa.list_(pa.string()),
}


def _write_parquet(path: Path, columns: tuple[str, ...], rows: list[dict[str, Any]]) -> None:
    schema = pa.schema([(c, _ARROW_TYPES.get(c, pa.string())) for c in columns],
                       metadata={b"marketagg.schema_version": str(DATASET_SCHEMA_VERSION).encode()})
    table = pa.Table.from_pylist([{c: r.get(c) for c in columns} for r in rows], schema=schema)
    pq.write_table(table, path, compression="zstd")
