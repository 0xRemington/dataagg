"""A07 clean disaster recovery (05) and corrupt-backup refusal.

The drill populates a disposable stack, takes an encrypted off-host backup, then
*destroys* that stack (database dropped, data root deleted) before restoring into a
differently named database and storage root with outbound network limited to the
local PostgreSQL server. Every query after the restore runs against the restored
instance only; the original database no longer exists to be used by mistake.
"""

from __future__ import annotations

import shutil
import socket
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, create_engine, make_url, text
from test_pipeline import OPEN, POLICIES, TOKENS, UNIVERSE, _drive

from marketagg.api.app import ApiServices, create_app
from marketagg.calendar import SessionCalendar
from marketagg.db.engine import make_engine
from marketagg.domain import PriceBasis, Timeframe
from marketagg.ops.backup import BackupError, create_backup, restore_backup
from marketagg.providers.fake import FakeClock, FakeProvider, FakeScenario
from marketagg.research.datasets import DatasetRequest, DatasetService
from marketagg.research.repository import ResearchRepository
from marketagg.runtime import bootstrap
from marketagg.storage.archive import ArchiveManager, DirectoryOffsite, UnifiedBarReader

pytestmark = [pytest.mark.acceptance, pytest.mark.integration]
KEY = bytes(range(32))
CUTOFF = datetime(2025, 3, 7, 15, 30, tzinfo=UTC)
RTO_TARGET = timedelta(hours=4)  # provisional objective (06)


def _admin(engine: Engine) -> str:
    return engine.url.set(database="postgres").render_as_string(hide_password=False)


def _drop(admin_url: str, name: str) -> None:
    admin = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
    admin.dispose()


@pytest.fixture
def local_db_only(engine: Engine, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Refuse every outbound connection except the disposable PostgreSQL server."""
    allowed = {(engine.url.host, engine.url.port or 5432), ("localhost", engine.url.port)}
    original = socket.socket.connect

    def guarded(self: socket.socket, address: Any) -> None:
        if self.family != socket.AF_UNIX and tuple(address[:2]) not in allowed:
            raise ConnectionRefusedError(f"live network disabled during restore: {address!r}")
        original(self, address)

    monkeypatch.setattr(socket.socket, "connect", guarded)
    yield


def _populate(engine: Engine, root: Path) -> dict[str, Any]:
    clock = FakeClock(datetime(2025, 3, 7, 14, 10, tzinfo=UTC))
    rt = bootstrap(engine, FakeProvider(FakeScenario(), clock), UNIVERSE, POLICIES,
                   clock=clock, requests_per_minute=600, payload_root=root / "payloads")
    _drive(rt, CUTOFF, kinds={"discovery", "chain_priority", "bars_1m", "derive_bars"})
    research = ResearchRepository(engine, rt.calendar, rt.policy_ids)
    exports = DatasetService(engine, research, root / "exports", signing_key=b"k" * 32,
                             clock=clock)
    request = DatasetRequest(instrument_id=rt.instruments["SYNA"], provider_id="fake",
                             timeframe=Timeframe.M15, start=OPEN, end=CUTOFF,
                             decision_at=CUTOFF)
    pinned = exports.run(exports.submit(request))
    assert pinned.state == "succeeded" and pinned.manifest is not None
    archive = ArchiveManager(engine, root / "archives", DirectoryOffsite(root.parent / "off"),
                             clock=clock)
    archive.archive_and_activate("bar_observations", "fake", date(2025, 3, 1))
    key = research.bar_key(rt.instruments["SYNA"], "fake", Timeframe.M1,
                           PriceBasis.PROVIDER_NATIVE)
    m1 = UnifiedBarReader(engine, archive).eligible_bars(key, start=OPEN, end=CUTOFF,
                                                         decision_at=CUTOFF)
    with engine.connect() as conn:
        known_max = conn.execute(text("SELECT max(known_at) FROM observation_batches "
                                      "WHERE status='published'")).scalar_one()
        payloads = conn.execute(text("SELECT count(DISTINCT uri) FROM payload_objects "
                                     "WHERE uri LIKE 'payload://%'")).scalar_one()
    assert payloads > 0, "drill must cover retained payload objects"
    return {"rt": rt, "request": request, "manifest": pinned.manifest, "m1": m1,
            "known_max": known_max, "clock": clock}


def test_a07_clean_disaster_recovery(engine: Engine, db_name: str, tmp_path: Path,
                                     local_db_only: None, evidence: dict[str, Any]) -> None:
    data = tmp_path / "stack" / "data"
    state = _populate(engine, data)
    offsite = tmp_path / "offsite-backups"  # independent of the stack's data root
    backup = create_backup(engine, engine.url.render_as_string(hide_password=False), offsite,
                           KEY, archive_root=data / "archives", payload_root=data / "payloads",
                           clock=state["clock"])
    admin = _admin(engine)
    # Discard only the designated disposable stack.
    engine.dispose()
    _drop(admin, db_name)
    shutil.rmtree(tmp_path / "stack")
    target = f"marketagg_test_dr_{uuid.uuid4().hex[:8]}"
    restored_root = tmp_path / "restored"
    started = time.monotonic()
    try:
        report = restore_backup(backup.path, KEY, admin_url=admin, target_database=target,
                                source_database=db_name,
                                archive_root=restored_root / "archives",
                                payload_root=restored_root / "payloads")
        restored = make_engine(make_url(admin).set(database=target).render_as_string(
            hide_password=False), statement_timeout_ms=30_000)
        try:
            rt = state["rt"]
            research = ResearchRepository(restored, SessionCalendar(), rt.policy_ids)
            services = ApiServices(engine=restored, research=research,
                                   calendar=research.calendar, policies=POLICIES,
                                   read_tokens=(TOKENS[0],), export_tokens=(TOKENS[1],),
                                   clock=state["clock"])
            client = TestClient(create_app(services),
                                headers={"Authorization": f"Bearer {TOKENS[0]}"})
            ready = client.get("/health/ready")
            assert ready.status_code == 200, ready.json()
            rto = time.monotonic() - started
            # Every pinned manifest resolves, and A03's dataset reproduces bit-for-bit.
            with restored.connect() as conn:
                manifests = conn.execute(text(
                    "SELECT output_sha256, eligibility_summary FROM dataset_manifests")).all()
            original = state["manifest"]["output_sha256"]
            assert [m.output_sha256 for m in manifests] == [original]
            exports = DatasetService(restored, research, restored_root / "exports",
                                     signing_key=b"k" * 32, clock=state["clock"])
            again = exports.run(exports.submit(state["request"]))
            assert again.manifest is not None and again.manifest["output_sha256"] == original
            assert again.files == manifests[0].eligibility_summary["files"]
            # Archived generations resolve from the restored storage root.
            archive = ArchiveManager(restored, restored_root / "archives")
            key = research.bar_key(rt.instruments["SYNA"], "fake", Timeframe.M1,
                                   PriceBasis.PROVIDER_NATIVE)
            m1 = UnifiedBarReader(restored, archive).eligible_bars(
                key, start=OPEN, end=CUTOFF, decision_at=CUTOFF)
            assert [(b.id, b.close) for b in m1] == [(b.id, b.close) for b in state["m1"]]
            # RPO: the latest recoverable knowledge time is exactly the backup's snapshot.
            assert report.rpo_known_at_max == state["known_max"]
            assert report.archive_generations_verified == 1 and report.objects_restored >= 2
            assert timedelta(seconds=rto) < RTO_TARGET
            evidence.update({
                "a07_rpo_known_at_max": report.rpo_known_at_max,
                "a07_rpo_note": "logical nightly backup; RPO equals time since last backup",
                "a07_rto_seconds": round(rto, 3),
                "a07_restore_seconds": round(report.rto_seconds, 3),
                "a07_objects_restored": report.objects_restored,
                "a07_dataset_sha256": original,
                "a07_restored_database": target,
            })
        finally:
            restored.dispose()
    finally:
        _drop(admin, target)


def test_a07_corrupt_backup_is_refused_before_any_restore(engine: Engine, db_name: str,
                                                          tmp_path: Path) -> None:
    data = tmp_path / "data"
    _populate(engine, data)
    backup = create_backup(engine, engine.url.render_as_string(hide_password=False),
                           tmp_path / "offsite", KEY, archive_root=data / "archives",
                           payload_root=data / "payloads")
    victim = next((backup.path / "objects" / "archive").rglob("*.enc"))
    raw = bytearray(victim.read_bytes())
    raw[-5] ^= 0xFF
    victim.write_bytes(bytes(raw))
    target = f"marketagg_test_dr_{uuid.uuid4().hex[:8]}"
    admin = _admin(engine)
    with pytest.raises(BackupError, match="missing or altered"):
        restore_backup(backup.path, KEY, admin_url=admin, target_database=target,
                       source_database=db_name, archive_root=tmp_path / "r" / "archives",
                       payload_root=tmp_path / "r" / "payloads")
    with engine.connect() as conn:
        exists = conn.execute(text("SELECT 1 FROM pg_database WHERE datname=:d"),
                              {"d": target}).scalar()
    assert exists is None, "verification must fail before a target database is created"
