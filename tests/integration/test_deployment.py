from __future__ import annotations

import os
import shutil
import subprocess
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import yaml
from sqlalchemy import Engine, create_engine, make_url, text
from testkit import ingest_minutes, make_world

from marketagg.calendar import SessionCalendar
from marketagg.ops.backup import (
    BackupError,
    create_backup,
    decrypt_file,
    encrypt_file,
    restore_backup,
    verify_backup,
)
from marketagg.ops.status import allowed_kinds, operator_status
from marketagg.storage.archive import ArchiveManager, DirectoryOffsite
from marketagg.storage.payloads import PayloadStore

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = yaml.safe_load((ROOT / "deploy" / "compose.yaml").read_text())
KEY = bytes(range(32))


def test_compose_is_private_pinned_and_least_privilege() -> None:
    services: dict[str, Any] = COMPOSE["services"]
    assert "ports" not in services["db"], "database must not publish a host port"
    assert COMPOSE["networks"]["internal"]["internal"] is True
    assert "@sha256:" in services["db"]["image"]
    # Only the authenticating proxy publishes a port (operator decision: public IPv4 :8000).
    published = {name for name, svc in services.items() if svc.get("ports")}
    assert published == {"proxy"}
    assert services["proxy"]["ports"] == ["0.0.0.0:${MARKETAGG_PUBLIC_PORT:-8000}:8000"]
    assert "@sha256:" in services["proxy"]["image"]
    assert services["proxy"]["cap_add"] == ["NET_BIND_SERVICE"]
    caddy = (ROOT / "deploy" / "Caddyfile.template").read_text()
    assert "basic_auth" in caddy and "@api path /v1/* /health/*" in caddy
    # Only the worker (provider fetches), the Binance collector (UPDATE2) and the proxy can
    # leave the internal network.
    egress = {n for n, svc in services.items() if "egress" in svc.get("networks", [])}
    assert egress == {"worker", "binance", "proxy"}
    assert services["binance"]["read_only"] is True and services["binance"]["cap_drop"] == ["ALL"]
    assert "database_url_owner" not in services["binance"]["secrets"]  # ingest role only
    assert services["migrate"]["depends_on"]["db"]["condition"] == "service_healthy"
    for name in ("scheduler", "worker", "api"):
        assert services[name]["depends_on"]["migrate"]["condition"] == (
            "service_completed_successfully")
        assert services[name]["read_only"] is True and services[name]["cap_drop"] == ["ALL"]
    assert "healthcheck" in services["db"] and "healthcheck" in services["api"]
    assert set(services["ui"]["secrets"]) == {"api_read_token"}  # no DB credentials in UI
    text_all = (ROOT / "deploy" / "compose.yaml").read_text()
    assert "docker.sock" not in text_all and "PASSWORD:" not in text_all.replace(
        "PASSWORD_FILE", "")
    dockerfile = (ROOT / "deploy" / "Dockerfile").read_text()
    assert dockerfile.splitlines()[2].startswith("FROM ") and "@sha256:" in dockerfile
    assert "USER 10001" in dockerfile


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker CLI not installed")
def test_docker_compose_config_validates() -> None:  # pragma: no cover - needs docker
    subprocess.run(["docker", "compose", "-f",  # noqa: S603, S607 - fixed arguments
                    str(ROOT / "deploy" / "compose.yaml"),
                    "config", "--quiet"], check=True)


def test_chunked_encryption_detects_tampering(tmp_path: Path) -> None:
    plain = tmp_path / "plain.bin"
    plain.write_bytes(os.urandom((1 << 20) * 2 + 123))
    enc, out = tmp_path / "x.enc", tmp_path / "x.out"
    encrypt_file(plain, enc, KEY)
    decrypt_file(enc, out, KEY)
    assert out.read_bytes() == plain.read_bytes()
    data = bytearray(enc.read_bytes())
    data[100] ^= 1
    enc.write_bytes(bytes(data))
    with pytest.raises(BackupError, match="authentication failed"):
        decrypt_file(enc, out, KEY)
    encrypt_file(plain, enc, KEY)
    enc.write_bytes(enc.read_bytes()[: (1 << 20) + 40])  # truncated after first chunk
    with pytest.raises(BackupError):
        decrypt_file(enc, out, KEY)


@pytest.fixture
def populated(engine: Engine, db_name: str, tmp_path: Path) -> dict[str, Any]:
    world = make_world(engine, start=datetime(2025, 3, 7, 15, 5, tzinfo=UTC))
    ingest_minutes(world, datetime(2025, 3, 7, 14, 30, tzinfo=UTC),
                   datetime(2025, 3, 7, 15, 0, tzinfo=UTC))
    payloads = PayloadStore(tmp_path / "data" / "payloads")
    result = world.provider.fetch_chain(world.chain_request())
    uri = payloads.put("fake", result.payload_body or b"")
    world.repo.publish(world.repo.stage_chain(result, instrument_id=world.instruments["SYNA"],
                                              scope={}, payload_uri=uri).batch_id)
    archive = ArchiveManager(engine, tmp_path / "data" / "archives",
                             DirectoryOffsite(tmp_path / "offsite"), clock=world.clock)
    archive.archive_and_activate("bar_observations", "fake", date(2025, 3, 1))
    url = engine.url.render_as_string(hide_password=False)
    return {"world": world, "url": url, "db": db_name, "root": tmp_path}


def test_backup_restore_round_trip_into_new_database(populated: dict[str, Any],
                                                     engine: Engine) -> None:
    root: Path = populated["root"]
    result = create_backup(engine, populated["url"], root / "backups", KEY,
                           archive_root=root / "data" / "archives",
                           payload_root=root / "data" / "payloads",
                           clock=populated["world"].clock)
    kinds = sorted(e["kind"] for e in result.manifest["entries"])
    assert kinds == ["archive", "database", "payload"]
    target = f"marketagg_test_restore_{uuid.uuid4().hex[:8]}"
    admin = make_url(populated["url"]).set(database="postgres").render_as_string(
        hide_password=False)
    with pytest.raises(BackupError, match="over the source"):
        restore_backup(result.path, KEY, admin_url=admin, target_database=populated["db"],
                       source_database=populated["db"], archive_root=root / "r1",
                       payload_root=root / "r2")
    try:
        report = restore_backup(result.path, KEY, admin_url=admin, target_database=target,
                                source_database=populated["db"],
                                archive_root=root / "restore" / "archives",
                                payload_root=root / "restore" / "payloads")
        assert report.objects_restored == 2 and report.archive_generations_verified == 1
        assert report.rpo_known_at_max is not None and report.rto_seconds >= 0
        restored = create_engine(make_url(admin).set(database=target))
        with restored.connect() as conn, engine.connect() as src:
            for table in ("bar_observations", "option_snapshots", "archive_manifest"):
                query = text(f"SELECT count(*) FROM {table}")  # noqa: S608 - fixed names
                assert conn.execute(query).scalar_one() == src.execute(query).scalar_one()
        restored.dispose()
    finally:
        cleanup = create_engine(admin, isolation_level="AUTOCOMMIT")
        with cleanup.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{target}" WITH (FORCE)'))
        cleanup.dispose()
    with engine.connect() as conn:
        assert conn.execute(text("SELECT state FROM backup_runs")).scalar_one() == "verified"


def test_tampered_or_incomplete_backup_fails_verification(populated: dict[str, Any],
                                                          engine: Engine) -> None:
    root: Path = populated["root"]
    result = create_backup(engine, populated["url"], root / "backups", KEY,
                           archive_root=root / "data" / "archives",
                           payload_root=root / "data" / "payloads")
    victim = next((result.path / "objects").rglob("*.enc"))
    victim.unlink()
    with pytest.raises(BackupError, match="missing or altered"):
        verify_backup(result.path, KEY)
    with pytest.raises(BackupError, match="authentication"):
        verify_backup(result.path, bytes(32))
    # A referenced object missing at backup time aborts the backup (no partial success).
    for archive in (root / "data" / "archives").rglob("*.parquet"):
        archive.unlink()
    with pytest.raises(BackupError, match="referenced object missing"):
        create_backup(engine, populated["url"], root / "backups2", KEY,
                      archive_root=root / "data" / "archives",
                      payload_root=root / "data" / "payloads")
    assert not any((root / "backups2").glob("2*"))


def test_operator_status_reports_stalls_backups_and_disk(engine: Engine,
                                                         tmp_path: Path) -> None:
    world = make_world(engine, start=datetime(2025, 3, 7, 15, 0, tzinfo=UTC))
    ingest_minutes(world, datetime(2025, 3, 7, 14, 30, tzinfo=UTC),
                   datetime(2025, 3, 7, 14, 59, tzinfo=UTC))
    later = datetime(2025, 3, 7, 15, 10, tzinfo=UTC)
    report = operator_status(engine, SessionCalendar(), later, data_paths=(tmp_path,))
    alerts = {(a["alert"], a["scope"]) for a in report.sections["alerts"]}
    assert ("ingestion_stalled", "SYNA:bars_1m") in alerts
    assert ("backup_stale", "backup") in alerts and report.severity == "critical"
    assert report.sections["notifications"].startswith("disabled")
    calm = operator_status(engine, SessionCalendar(), datetime(2025, 3, 8, 15, tzinfo=UTC))
    assert not any(a["alert"].startswith("ingestion") for a in calm.sections["alerts"])
    assert allowed_kinds("stop_bulk", ["bars_1m", "daily_backfill"]) == ["bars_1m"]
    assert allowed_kinds("suspend", ["bars_1m"]) == []
    assert timedelta(0) == timedelta(0)
