"""Parquet archives with a checksummed catalog and unified hot/cold reads (T16).

Lifecycle per (dataset, month): write temp file -> verify (row count, identity
set, types, checksum) -> ``verified`` -> copy off-host and verify -> atomic switch
to ``active`` (previous generation ``superseded``). Late corrections produce a new
generation; files are never rewritten in place. Rows embed their batch's
``known_at``/``fetch_completed_at`` so cold reads apply the same causal rules.

Pruning hot partitions requires an active, off-host-verified generation older
than the grace period plus explicit restore proof, and only ever detaches/drops
the validated monthly partition (06).
"""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID, uuid4

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import Engine, text

from marketagg.analytics.asof import BarKey, BarRecord, _bar
from marketagg.clock import SYSTEM_CLOCK, Clock
from marketagg.db.partitions import month_starts
from marketagg.domain.primitives import canonical_dumps

ARCHIVE_SCHEMA_VERSION = 1
DATASETS = ("bar_observations", "option_snapshots")
GRACE = timedelta(days=7)


class ArchiveError(RuntimeError):
    pass


class ArchiveCorrupt(ArchiveError):
    pass


class OffsiteStore(Protocol):
    def put(self, relative: str, source: Path) -> str: ...

    def sha256(self, relative: str) -> str: ...

    def get(self, relative: str, destination: Path) -> None: ...


class DirectoryOffsite:
    """Off-host store backed by a directory (tests, or a mounted remote volume)."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def _path(self, relative: str) -> Path:
        path = (self.root / relative).resolve()
        if self.root.resolve() not in path.parents:
            raise ArchiveError("offsite path escapes root")
        return path

    def put(self, relative: str, source: Path) -> str:
        target = self._path(relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        return f"offsite://{relative}"

    def sha256(self, relative: str) -> str:
        return _sha256(self._path(relative))

    def get(self, relative: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(self._path(relative), destination)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


_SELECT = {
    "bar_observations": """
        SELECT b.*, x.known_at, x.fetch_completed_at, x.status AS batch_status
        FROM bar_observations b JOIN observation_batches x ON x.id = b.batch_id
        WHERE b.interval_start >= :lo AND b.interval_start < :hi
          AND b.provider_id = :p AND x.status = 'published'
        ORDER BY b.interval_start, b.id""",
    "option_snapshots": """
        SELECT s.*, x.known_at, x.fetch_completed_at, x.status AS batch_status
        FROM option_snapshots s JOIN observation_batches x ON x.id = s.batch_id
        WHERE s.observed_at >= :lo AND s.observed_at < :hi
          AND s.provider_id = :p AND x.status = 'published'
        ORDER BY s.observed_at, s.id""",
}
_DECIMAL_COLUMNS = {"open", "high", "low", "close", "bid", "ask", "last", "strike"}
_JSON_COLUMNS = {"input_refs", "field_semantics"}


@dataclass(frozen=True)
class Generation:
    id: UUID
    dataset: str
    partition_key: str
    generation: int
    state: str
    uri: str
    sha256: str
    row_count: int


def partition_key(provider_id: str, month: date) -> str:
    return f"source={provider_id}/year={month.year:04d}/month={month.month:02d}"


class ArchiveManager:
    def __init__(self, engine: Engine, root: Path, offsite: OffsiteStore | None = None,
                 clock: Clock = SYSTEM_CLOCK) -> None:
        self.engine = engine
        self.root = root
        self.offsite = offsite
        self.clock = clock

    # -- Write / verify / activate -----------------------------------------------------

    def archive_month(self, dataset: str, provider_id: str, month: date) -> Generation:
        if dataset not in DATASETS:
            raise ArchiveError(f"unsupported dataset {dataset}")
        lo = datetime(month.year, month.month, 1, tzinfo=UTC)
        hi = datetime(month.year + month.month // 12, month.month % 12 + 1, 1, tzinfo=UTC)
        key = partition_key(provider_id, month)
        with self.engine.connect() as conn:
            rows = [dict(r) for r in conn.execute(text(_SELECT[dataset]),
                                                  {"lo": lo, "hi": hi, "p": provider_id})
                    .mappings()]
            number = int(conn.execute(text(
                "SELECT coalesce(max(generation), 0) + 1 FROM archive_manifest "
                "WHERE dataset=:d AND partition_key=:k"), {"d": dataset, "k": key}).scalar_one())
        if not rows:
            raise ArchiveError(f"no published rows for {dataset} {key}")
        relative = (f"dataset={dataset}/schema={ARCHIVE_SCHEMA_VERSION}/{key}/"
                    f"generation={number}/part-00000.parquet")
        gen_id = uuid4()
        now = self.clock.now()
        coverage_col = "interval_start" if dataset == "bar_observations" else "observed_at"
        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO archive_manifest (id, dataset, partition_key, generation,
                    schema_version, state, coverage_start, coverage_end, min_known_at,
                    max_known_at, uri, sha256, row_count, byte_count, provenance, created_at)
                VALUES (:id, :d, :k, :g, :sv, 'writing', :lo, :hi, :mink, :maxk, :uri, '',
                        0, 0, CAST(:prov AS jsonb), :now)
            """), {"id": gen_id, "d": dataset, "k": key, "g": number,
                   "sv": ARCHIVE_SCHEMA_VERSION, "lo": lo, "hi": hi,
                   "mink": min(r["known_at"] for r in rows),
                   "maxk": max(r["known_at"] for r in rows), "uri": f"archive://{relative}",
                   "prov": canonical_dumps({"provider": provider_id,
                                            "coverage_column": coverage_col,
                                            "rows_identity": "id"}), "now": now})
        final = self.root / relative
        final.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=".archive-", dir=final.parent)
        os.close(fd)
        tmp = Path(tmp_name)
        try:
            table = _to_table(rows)
            pq.write_table(table, tmp, compression="zstd")
            self._verify_file(tmp, rows)
            os.replace(tmp, final)
            sha = _sha256(final)
        except Exception:
            tmp.unlink(missing_ok=True)
            self._set_state(gen_id, "failed")
            raise
        with self.engine.begin() as conn:
            conn.execute(text("""
                UPDATE archive_manifest SET state='verified', sha256=:sha, row_count=:n,
                       byte_count=:b, verified_at=:now WHERE id=:id
            """), {"sha": sha, "n": len(rows), "b": final.stat().st_size,
                   "now": self.clock.now(), "id": gen_id})
        return self.generation(gen_id)

    def _verify_file(self, path: Path, rows: list[dict[str, Any]]) -> None:
        table = pq.read_table(path)
        if table.num_rows != len(rows):
            raise ArchiveCorrupt("row count mismatch after write")
        written = sorted(str(v) for v in table.column("id").to_pylist())
        expected = sorted(str(r["id"]) for r in rows)
        if written != expected:
            raise ArchiveCorrupt("row identity mismatch after write")
        hashes = dict(zip(table.column("id").to_pylist(),
                          table.column("content_sha256").to_pylist(), strict=True))
        if any(hashes[str(r["id"])] != r["content_sha256"] for r in rows):
            raise ArchiveCorrupt("content hash mismatch after write")

    def copy_offsite(self, gen_id: UUID) -> None:
        if self.offsite is None:
            raise ArchiveError("no off-host store configured")
        generation = self.generation(gen_id)
        relative = generation.uri.removeprefix("archive://")
        self.offsite.put(relative, self.root / relative)
        if self.offsite.sha256(relative) != generation.sha256:
            raise ArchiveCorrupt("off-host copy checksum mismatch")
        with self.engine.begin() as conn:
            conn.execute(text("UPDATE archive_manifest SET offsite_verified_at=:now "
                              "WHERE id=:id"), {"now": self.clock.now(), "id": gen_id})

    def activate(self, gen_id: UUID) -> None:
        generation = self.generation(gen_id)
        if generation.state != "verified":
            raise ArchiveError(f"generation is {generation.state}, not verified")
        with self.engine.begin() as conn:
            conn.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:k, 0))"),
                         {"k": f"archive:{generation.dataset}:{generation.partition_key}"})
            offsite = conn.execute(text("SELECT offsite_verified_at FROM archive_manifest "
                                        "WHERE id=:id"), {"id": gen_id}).scalar_one()
            if offsite is None:
                raise ArchiveError("activation requires a verified off-host copy")
            conn.execute(text("""
                UPDATE archive_manifest SET state='superseded'
                WHERE dataset=:d AND partition_key=:k AND state='active'
            """), {"d": generation.dataset, "k": generation.partition_key})
            conn.execute(text("UPDATE archive_manifest SET state='active' WHERE id=:id"),
                         {"id": gen_id})

    def archive_and_activate(self, dataset: str, provider_id: str, month: date) -> Generation:
        generation = self.archive_month(dataset, provider_id, month)
        self.copy_offsite(generation.id)
        self.activate(generation.id)
        return self.generation(generation.id)

    def recover(self) -> int:
        """Mark interrupted 'writing' generations failed and drop their temp files."""
        with self.engine.begin() as conn:
            stale = conn.execute(text("UPDATE archive_manifest SET state='failed' "
                                      "WHERE state='writing' RETURNING id")).all()
        for temp in self.root.rglob(".archive-*") if self.root.exists() else []:
            temp.unlink(missing_ok=True)
        return len(stale)

    # -- Pruning ------------------------------------------------------------------------

    def prune_hot_partition(self, dataset: str, provider_id: str, month: date, *,
                            restore_verified: bool, grace: timedelta = GRACE) -> str:
        """Drop a hot monthly partition only after verified, off-host, restorable archive."""
        if dataset not in DATASETS:
            raise ArchiveError(f"unsupported dataset {dataset}")
        if not restore_verified:
            raise ArchiveError("pruning requires a successful restore drill first")
        key = partition_key(provider_id, month)
        with self.engine.connect() as conn:
            active = conn.execute(text("""
                SELECT * FROM archive_manifest WHERE dataset=:d AND partition_key=:k
                  AND state='active'"""), {"d": dataset, "k": key}).mappings().one_or_none()
            other_sources = conn.execute(text(f"""
                SELECT count(*) FROM {dataset}_{month:%Y_%m}
                WHERE provider_id <> :p"""), {"p": provider_id}).scalar_one()
            hot_rows = conn.execute(text(f"""
                SELECT count(*) FROM {dataset}_{month:%Y_%m} t
                JOIN observation_batches x ON x.id = t.batch_id
                WHERE x.status='published'""")).scalar_one()
        if active is None or active["offsite_verified_at"] is None:
            raise ArchiveError("no active off-host-verified generation")
        if self.clock.now() - active["verified_at"] < grace:
            raise ArchiveError("deletion grace period has not elapsed")
        if other_sources:
            raise ArchiveError("partition holds other providers' rows; archive them first")
        if hot_rows != active["row_count"]:
            raise ArchiveError("hot rows changed since archiving; archive a new generation")
        self.verify_generation(UUID(str(active["id"])))
        partition = f"{dataset}_{month:%Y_%m}"
        with self.engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE {dataset} DETACH PARTITION {partition}"))
            conn.execute(text(f"DROP TABLE {partition}"))
        return partition

    # -- Catalog --------------------------------------------------------------------------

    def generation(self, gen_id: UUID) -> Generation:
        with self.engine.connect() as conn:
            row = conn.execute(text("SELECT * FROM archive_manifest WHERE id=:id"),
                               {"id": gen_id}).mappings().one()
        return Generation(row["id"], row["dataset"], row["partition_key"], row["generation"],
                          row["state"], row["uri"], row["sha256"], row["row_count"])

    def pin(self, dataset: str, provider_id: str, start: datetime, end: datetime
            ) -> list[Generation]:
        """Active generations overlapping a window, pinned for one query/export."""
        keys = [partition_key(provider_id, m) for m in month_starts(start, end)]
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id FROM archive_manifest
                WHERE dataset=:d AND state='active' AND partition_key = ANY(:keys)
                ORDER BY partition_key"""), {"d": dataset, "keys": keys}).scalars().all()
        return [self.generation(UUID(str(r))) for r in rows]

    def verify_generation(self, gen_id: UUID) -> Path:
        generation = self.generation(gen_id)
        path = self.root / generation.uri.removeprefix("archive://")
        if not path.is_file():
            raise ArchiveCorrupt(f"archive object missing: {generation.uri}")
        if _sha256(path) != generation.sha256:
            raise ArchiveCorrupt(f"archive checksum mismatch: {generation.uri}")
        return path

    def _set_state(self, gen_id: UUID, state: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(text("UPDATE archive_manifest SET state=:s WHERE id=:id"),
                         {"s": state, "id": gen_id})


def _to_table(rows: list[dict[str, Any]]) -> pa.Table:
    columns: dict[str, list[Any]] = {k: [] for k in rows[0]}
    for row in rows:
        for k, column in columns.items():
            value = row[k]
            if k in _JSON_COLUMNS:
                value = canonical_dumps(value)
            elif isinstance(value, UUID):
                value = str(value)
            column.append(value)
    fields = []
    for name, values in columns.items():
        if name in _DECIMAL_COLUMNS:
            fields.append(pa.field(name, pa.decimal128(20, 8)))
        elif name in {"interval_start", "interval_end", "observed_at", "known_at",
                      "fetch_completed_at", "provider_event_at", "last_trade_at",
                      "supersedes_observed_at"}:
            fields.append(pa.field(name, pa.timestamp("us", tz="UTC")))
        elif name == "quality_flags":
            fields.append(pa.field(name, pa.list_(pa.string())))
        else:
            fields.append(pa.field(name, _infer_type(values)))
    schema = pa.schema(fields, metadata={b"marketagg.archive_schema":
                                         str(ARCHIVE_SCHEMA_VERSION).encode()})
    return pa.Table.from_pydict(columns, schema=schema)


def _infer_type(values: list[Any]) -> pa.DataType:
    """Arrow type from the first non-null value; all-null columns become strings."""
    sample = next((v for v in values if v is not None), None)
    return pa.string() if sample is None else pa.array([sample]).type


class UnifiedBarReader:
    """Causal bar selection over hot rows plus pinned archive generations.

    Rows are de-duplicated by immutable ``id`` (an archived row still present hot
    counts once), then the same knowledge-first revision selection as
    ``analytics.asof.eligible_bars`` is applied.
    """

    def __init__(self, engine: Engine, archive: ArchiveManager) -> None:
        self.engine = engine
        self.archive = archive

    def eligible_bars(self, key: BarKey, *, start: datetime, end: datetime,
                      decision_at: datetime, include_partial: bool = False) -> list[BarRecord]:
        pinned = self.archive.pin("bar_observations", key.provider_id, start, end)
        files = [str(self.archive.verify_generation(g.id)) for g in pinned]
        with self.engine.connect() as conn:
            hot = [dict(r) for r in conn.execute(text("""
                SELECT b.*, x.known_at FROM bar_observations b
                JOIN observation_batches x ON x.id = b.batch_id
                WHERE x.status='published' AND b.instrument_id=:i AND b.provider_id=:p
                  AND b.timeframe=:tf AND b.price_basis=:pb AND b.session_kind=:sk
                  AND b.policy_id=:pol AND b.interval_start >= :s AND b.interval_start < :e
            """), {"i": key.instrument_id, "p": key.provider_id, "tf": key.timeframe.value,
                   "pb": key.price_basis.value, "sk": key.session_kind.value,
                   "pol": key.policy_id, "s": start, "e": end}).mappings()]
        db = duckdb.connect()
        try:
            cold: list[dict[str, Any]] = []
            if files:
                cold_rel = db.execute("""
                    SELECT * EXCLUDE (fetch_completed_at, batch_status)
                    FROM read_parquet(?)
                    WHERE instrument_id = ? AND provider_id = ? AND timeframe = ?
                      AND price_basis = ? AND session_kind = ? AND policy_id = ?
                      AND interval_start >= ? AND interval_start < ?
                """, [files, str(key.instrument_id), key.provider_id, key.timeframe.value,
                      key.price_basis.value, key.session_kind.value, key.policy_id, start,
                      end])
                names = [d[0] for d in cold_rel.description]
                cold = [dict(zip(names, row, strict=True)) for row in cold_rel.fetchall()]
        finally:
            db.close()
        merged: dict[str, dict[str, Any]] = {}
        for row in [*cold, *hot]:
            merged[str(row["id"])] = row  # immutable identity: duplicates collapse
        latest: dict[datetime, dict[str, Any]] = {}
        for row in merged.values():
            known = _aware(row["known_at"])
            if known > decision_at or _aware(row["interval_end"]) > decision_at:
                continue
            slot = _aware(row["interval_start"])
            current = latest.get(slot)
            if current is None or (_aware(current["known_at"]), str(current["id"])) < (
                    known, str(row["id"])):
                latest[slot] = row
        out = []
        for slot in sorted(latest):
            row = latest[slot]
            flags = list(row["quality_flags"] or [])
            if "invalidated" in flags or not (row["complete"] or include_partial):
                continue
            out.append(_bar(_normalise(row)))
        return out


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def _normalise(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    for k in ("id", "batch_id"):
        out[k] = UUID(str(out[k]))
    for k in ("interval_start", "interval_end", "known_at"):
        out[k] = _aware(out[k])
    out["quality_flags"] = list(out["quality_flags"] or [])
    return out
