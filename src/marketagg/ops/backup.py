"""Encrypted, catalog-consistent backups and verified restores (T17, 06).

A backup is complete only when the database dump *and* every external object the
catalog references (active/verified archive generations, retained payloads) are
copied, encrypted and verified at the destination. The manifest records
plaintext and ciphertext checksums and is authenticated with an HMAC.

Restores always target a *different*, freshly created database and storage root;
restoring over the source is refused.

Encryption: AES-256-GCM in 1 MiB chunks (fresh random nonce per chunk, chunk index
and final flag bound as associated data), so large files stream with bounded
memory and truncation/reordering is detected.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import shutil
import struct
import subprocess
import tempfile
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from sqlalchemy import Engine, create_engine, make_url, text

from marketagg.clock import SYSTEM_CLOCK, Clock
from marketagg.db.engine import normalize_url
from marketagg.domain.primitives import canonical_dumps

MAGIC = b"MAGBK1\n"
CHUNK = 1 << 20
PG_BIN = Path(os.environ.get("MARKETAGG_PG_BIN", "/usr/lib/postgresql/18/bin"))


class BackupError(RuntimeError):
    pass


def load_key(path: Path) -> bytes:
    raw = path.read_bytes().strip()
    key = bytes.fromhex(raw.decode()) if len(raw) == 64 else raw
    if len(key) != 32:
        raise BackupError("backup key must be 32 bytes (or 64 hex characters)")
    return key


_HEADER = struct.Struct(">12s?I")  # nonce, final-chunk flag, sealed length


def encrypt_file(source: Path, target: Path, key: bytes) -> None:
    aes = AESGCM(key)
    with source.open("rb") as src, target.open("wb") as dst:
        dst.write(MAGIC)
        index = 0
        chunk = src.read(CHUNK)
        while True:
            following = src.read(CHUNK)
            final = not following
            nonce = os.urandom(12)
            # Index and final flag are authenticated: reordering/truncation is detected.
            sealed = aes.encrypt(nonce, chunk, struct.pack(">Q?", index, final))
            dst.write(_HEADER.pack(nonce, final, len(sealed)) + sealed)
            if final:
                return
            chunk, index = following, index + 1


def decrypt_file(source: Path, target: Path, key: bytes) -> None:
    aes = AESGCM(key)
    target.parent.mkdir(parents=True, exist_ok=True)
    with source.open("rb") as src, target.open("wb") as dst:
        if src.read(len(MAGIC)) != MAGIC:
            raise BackupError(f"{source.name}: not a marketagg backup object")
        index = 0
        while True:
            header = src.read(_HEADER.size)
            if len(header) != _HEADER.size:
                raise BackupError(f"{source.name}: truncated (no final chunk)")
            nonce, final, length = _HEADER.unpack(header)
            try:
                dst.write(aes.decrypt(nonce, src.read(length), struct.pack(">Q?", index, final)))
            except InvalidTag as exc:
                raise BackupError(f"{source.name}: authentication failed at chunk {index}") \
                    from exc
            if final:
                if src.read(1):
                    raise BackupError(f"{source.name}: data after final chunk")
                return
            index += 1


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(CHUNK), b""):
            digest.update(block)
    return digest.hexdigest()


@dataclass(frozen=True)
class BackupResult:
    id: UUID
    path: Path
    manifest: dict[str, Any]


def _libpq_url(url: str) -> str:
    """Plain libpq URL for pg_dump/pg_restore (no SQLAlchemy driver suffix)."""
    return make_url(normalize_url(url)).set(drivername="postgresql").render_as_string(
        hide_password=False)


def _objects(engine: Engine, archive_root: Path | None, payload_root: Path | None
             ) -> Iterator[tuple[str, Path]]:
    with engine.connect() as conn:
        archives = conn.execute(text(
            "SELECT uri FROM archive_manifest WHERE state IN ('active','verified')")).scalars()
        archive_list = list(archives)
        payloads = list(conn.execute(text(
            "SELECT DISTINCT uri FROM payload_objects WHERE uri LIKE 'payload://%' "
            "AND body_deleted_at IS NULL")).scalars())
    for uri in archive_list:
        if archive_root is None:
            raise BackupError("archive objects referenced but no archive root configured")
        yield f"archive/{uri.removeprefix('archive://')}", archive_root / uri.removeprefix(
            "archive://")
    for uri in payloads:
        if payload_root is None:
            raise BackupError("payload objects referenced but no payload root configured")
        provider, sha = uri.removeprefix("payload://").split("/", 1)
        yield f"payload/{provider}/{sha[:2]}/{sha}.bin", payload_root / provider / sha[:2] / (
            f"{sha}.bin")


def create_backup(engine: Engine, database_url: str, destination: Path, key: bytes, *,
                  archive_root: Path | None = None, payload_root: Path | None = None,
                  clock: Clock = SYSTEM_CLOCK) -> BackupResult:
    backup_id = uuid4()
    started = clock.now()
    folder = destination / f"{started:%Y%m%dT%H%M%SZ}-{str(backup_id)[:8]}"
    _record(engine, backup_id, started, "running", str(folder))
    staging = Path(tempfile.mkdtemp(prefix=".backup-", dir=_mkdir(destination)))
    try:
        with tempfile.TemporaryDirectory() as plain_dir:
            dump = Path(plain_dir) / "db.dump"
            _pg_dump(database_url, dump)
            entries: list[dict[str, Any]] = [{"name": "db.dump", "kind": "database",
                                              "plain_sha256": _sha256(dump),
                                              "bytes": dump.stat().st_size}]
            encrypt_file(dump, staging / "db.dump.enc", key)
            entries[0]["cipher_sha256"] = _sha256(staging / "db.dump.enc")
            for relative, source in _objects(engine, archive_root, payload_root):
                if not source.is_file():
                    raise BackupError(f"referenced object missing: {relative}")
                target = staging / "objects" / (relative + ".enc")
                target.parent.mkdir(parents=True, exist_ok=True)
                encrypt_file(source, target, key)
                entries.append({"name": relative, "kind": relative.split("/", 1)[0],
                                "plain_sha256": _sha256(source),
                                "cipher_sha256": _sha256(target),
                                "bytes": source.stat().st_size})
        with engine.connect() as conn:
            known_max = conn.execute(text(
                "SELECT max(known_at) FROM observation_batches WHERE status='published'")
            ).scalar_one()
        manifest = {"id": str(backup_id), "created_at": started.isoformat(),
                    "rpo_known_at_max": known_max.isoformat() if known_max else None,
                    "entries": entries, "format": "marketagg-backup/1",
                    "encryption": "AES-256-GCM, 1 MiB chunks"}
        body = canonical_dumps(manifest).encode()
        (staging / "manifest.json").write_bytes(body)
        (staging / "manifest.hmac").write_text(hmac.new(key, body, hashlib.sha256).hexdigest())
        staging.rename(folder)
        verify_backup(folder, key)
        _finish(engine, backup_id, clock.now(), "verified", hashlib.sha256(body).hexdigest(),
                known_max, len(entries), sum(int(e["bytes"]) for e in entries))
        return BackupResult(backup_id, folder, manifest)
    except Exception as exc:
        shutil.rmtree(staging, ignore_errors=True)
        _finish(engine, backup_id, clock.now(), "failed", None, None, None, None,
                error=str(exc)[:500])
        raise


def _pg_dump(database_url: str, target: Path) -> None:
    env = {**os.environ, "PGOPTIONS": "-c statement_timeout=0"}
    result = subprocess.run(  # noqa: S603 - fixed binary path, arguments not shell-parsed
        [str(PG_BIN / "pg_dump"), "--format=custom", "--no-owner", "--no-privileges",
         f"--file={target}", _libpq_url(database_url)],
        capture_output=True, env=env, check=False)
    if result.returncode != 0:
        raise BackupError(f"pg_dump failed: {result.stderr.decode()[-400:]}")


def verify_backup(folder: Path, key: bytes) -> dict[str, Any]:
    body = (folder / "manifest.json").read_bytes()
    expected = hmac.new(key, body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, (folder / "manifest.hmac").read_text().strip()):
        raise BackupError("manifest authentication failed")
    manifest: dict[str, Any] = json.loads(body)
    with tempfile.TemporaryDirectory() as plain_dir:
        for entry in manifest["entries"]:
            cipher = (folder / "db.dump.enc" if entry["kind"] == "database"
                      else folder / "objects" / (entry["name"] + ".enc"))
            if not cipher.is_file() or _sha256(cipher) != entry["cipher_sha256"]:
                raise BackupError(f"backup object missing or altered: {entry['name']}")
            plain = Path(plain_dir) / "check"
            decrypt_file(cipher, plain, key)
            if _sha256(plain) != entry["plain_sha256"]:
                raise BackupError(f"decrypted checksum mismatch: {entry['name']}")
            if entry["kind"] == "database":
                listing = subprocess.run(  # noqa: S603
                    [str(PG_BIN / "pg_restore"), "--list", str(plain)],
                    capture_output=True, check=False)
                if listing.returncode != 0:
                    raise BackupError("pg_restore cannot read the database dump")
            plain.unlink()
    return manifest


@dataclass(frozen=True)
class RestoreReport:
    database: str
    rpo_known_at_max: datetime | None
    rto_seconds: float
    objects_restored: int
    archive_generations_verified: int


def restore_backup(folder: Path, key: bytes, *, admin_url: str, target_database: str,
                   source_database: str, archive_root: Path, payload_root: Path
                   ) -> RestoreReport:
    """Restore into a new database and new storage roots, then validate references."""
    if target_database == source_database:
        raise BackupError("refusing to restore over the source database")
    for root in (archive_root, payload_root):
        if root.exists() and any(root.iterdir()):
            raise BackupError(f"restore target {root} is not empty")
    started = time.monotonic()
    manifest = verify_backup(folder, key)
    admin = create_engine(normalize_url(admin_url), isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        exists = conn.execute(text("SELECT 1 FROM pg_database WHERE datname=:d"),
                              {"d": target_database}).scalar()
        if exists:
            raise BackupError(f"target database {target_database} already exists")
        conn.execute(text(f'CREATE DATABASE "{target_database}"'))
    admin.dispose()
    target_url = make_url(normalize_url(admin_url)).set(database=target_database)
    restored = 0
    with tempfile.TemporaryDirectory() as plain_dir:
        dump = Path(plain_dir) / "db.dump"
        decrypt_file(folder / "db.dump.enc", dump, key)
        result = subprocess.run(  # noqa: S603
            [str(PG_BIN / "pg_restore"), "--no-owner", "--no-privileges", "--exit-on-error",
             f"--dbname={_libpq_url(target_url.render_as_string(hide_password=False))}",
             str(dump)], capture_output=True, check=False)
        if result.returncode != 0:
            raise BackupError(f"pg_restore failed: {result.stderr.decode()[-400:]}")
        for entry in manifest["entries"]:
            if entry["kind"] == "database":
                continue
            kind, relative = entry["name"].split("/", 1)
            root = archive_root if kind == "archive" else payload_root
            decrypt_file(folder / "objects" / (entry["name"] + ".enc"), root / relative, key)
            if _sha256(root / relative) != entry["plain_sha256"]:
                raise BackupError(f"restored object checksum mismatch: {entry['name']}")
            restored += 1
    engine = create_engine(target_url, pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            generations = conn.execute(text(
                "SELECT uri, sha256 FROM archive_manifest WHERE state IN ('active','verified')")
            ).all()
            for uri, sha in generations:
                path = archive_root / uri.removeprefix("archive://")
                if not path.is_file() or _sha256(path) != sha:
                    raise BackupError(f"restored catalog reference unresolved: {uri}")
            rpo = conn.execute(text("SELECT max(known_at) FROM observation_batches "
                                    "WHERE status='published'")).scalar_one()
    finally:
        engine.dispose()
    return RestoreReport(target_database, rpo, time.monotonic() - started, restored,
                         len(generations))


def _mkdir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _record(engine: Engine, backup_id: UUID, started: datetime, state: str,
            destination: str) -> None:
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO backup_runs (id, started_at, state, destination) "
                          "VALUES (:id, :s, :st, :d)"),
                     {"id": backup_id, "s": started, "st": state, "d": destination})


def _finish(engine: Engine, backup_id: UUID, finished: datetime, state: str,
            manifest_sha: str | None, known_max: datetime | None, objects: int | None,
            size: int | None, error: str | None = None) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE backup_runs SET finished_at=:f, state=:s, manifest_sha256=:m,
                   db_known_at_max=:k, object_count=:o, byte_count=:b, error=:e WHERE id=:id
        """), {"f": finished, "s": state, "m": manifest_sha, "k": known_max, "o": objects,
               "b": size, "e": error, "id": backup_id})
