from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from sqlalchemy import Engine, text
from testkit import World, count, ingest_minutes, m1_key, make_world

from marketagg.analytics.asof import eligible_bars
from marketagg.providers.fake import BarRevision, FakeScenario
from marketagg.storage.archive import (
    ArchiveCorrupt,
    ArchiveError,
    ArchiveManager,
    DirectoryOffsite,
    UnifiedBarReader,
)

OPEN = datetime(2025, 3, 7, 14, 30, tzinfo=UTC)
MARCH = date(2025, 3, 1)


def _world(engine: Engine, scenario: FakeScenario | None = None) -> World:
    world = make_world(engine, scenario, start=datetime(2025, 3, 7, 15, 5, tzinfo=UTC))
    ingest_minutes(world, OPEN, OPEN + timedelta(minutes=30))
    return world


def _manager(engine: Engine, tmp_path: Path, world: World) -> ArchiveManager:
    return ArchiveManager(engine, tmp_path / "archive", DirectoryOffsite(tmp_path / "offsite"),
                          clock=world.clock)


def _view(world: World, reader: UnifiedBarReader, decision: datetime) -> list:
    return reader.eligible_bars(m1_key(world), start=OPEN, end=OPEN + timedelta(hours=1),
                                decision_at=decision)


def test_round_trip_preserves_types_identity_and_hashes(engine: Engine, tmp_path: Path) -> None:
    world = _world(engine)
    manager = _manager(engine, tmp_path, world)
    generation = manager.archive_and_activate("bar_observations", "fake", MARCH)
    assert generation.state == "active" and generation.row_count == 30
    table = pq.read_table(manager.verify_generation(generation.id))
    assert str(table.schema.field("open").type) == "decimal128(20, 8)"
    assert str(table.schema.field("known_at").type) == "timestamp[us, tz=UTC]"
    with engine.connect() as conn:
        hot = {str(r.id): r for r in conn.execute(text(
            "SELECT id, open, content_sha256 FROM bar_observations"))}
    for row in table.to_pylist():
        assert hot[row["id"]].open == row["open"]
        assert hot[row["id"]].content_sha256 == row["content_sha256"]


def test_hot_mixed_and_cold_reads_agree(engine: Engine, tmp_path: Path) -> None:
    world = _world(engine)
    manager = _manager(engine, tmp_path, world)
    reader = UnifiedBarReader(engine, manager)
    decision = world.clock.now()
    with engine.connect() as conn:
        hot_only = eligible_bars(conn, m1_key(world), start=OPEN,
                                 end=OPEN + timedelta(hours=1), decision_at=decision)
    manager.archive_and_activate("bar_observations", "fake", MARCH)
    mixed = _view(world, reader, decision)  # archive + still-present hot rows
    assert [b.id for b in mixed] == [b.id for b in hot_only]
    world.clock.advance(timedelta(days=8))
    manager.prune_hot_partition("bar_observations", "fake", MARCH, restore_verified=True)
    assert count(engine, "SELECT count(*) FROM bar_observations") == 0
    cold = _view(world, reader, decision)
    assert [(b.id, b.close, b.known_at) for b in cold] == [
        (b.id, b.close, b.known_at) for b in hot_only]
    early = _view(world, reader, OPEN)  # causal rules still apply to cold rows
    assert early == []


def test_late_correction_creates_new_generation_old_pins_unchanged(engine: Engine,
                                                                   tmp_path: Path) -> None:
    target = OPEN + timedelta(minutes=5)
    world = _world(engine, FakeScenario(bar_revisions=(BarRevision(target, 2,
                                                                   Decimal("0.3")),)))
    manager = _manager(engine, tmp_path, world)
    first = manager.archive_and_activate("bar_observations", "fake", MARCH)
    before = world.clock.now()
    world.clock.advance(timedelta(hours=1))
    ingest_minutes(world, OPEN, OPEN + timedelta(minutes=30))
    second = manager.archive_and_activate("bar_observations", "fake", MARCH)
    assert second.generation == first.generation + 1
    assert manager.generation(first.id).state == "superseded"
    assert (tmp_path / "archive" / first.uri.removeprefix("archive://")).is_file()
    reader = UnifiedBarReader(engine, manager)
    old = {b.interval_start: b.close for b in _view(world, reader, before)}
    new = {b.interval_start: b.close for b in _view(world, reader, world.clock.now())}
    assert new[target] - old[target] == Decimal("0.3")


def test_missing_or_corrupt_objects_fail_visibly(engine: Engine, tmp_path: Path) -> None:
    world = _world(engine)
    manager = _manager(engine, tmp_path, world)
    generation = manager.archive_and_activate("bar_observations", "fake", MARCH)
    path = tmp_path / "archive" / generation.uri.removeprefix("archive://")
    path.write_bytes(path.read_bytes()[:-10] + b"corrupted!")
    reader = UnifiedBarReader(engine, manager)
    with pytest.raises(ArchiveCorrupt, match="checksum"):
        _view(world, reader, world.clock.now())
    path.unlink()
    with pytest.raises(ArchiveCorrupt, match="missing"):
        _view(world, reader, world.clock.now())


def test_interrupted_archive_loses_nothing(engine: Engine, tmp_path: Path,
                                           monkeypatch: pytest.MonkeyPatch) -> None:
    world = _world(engine)
    manager = _manager(engine, tmp_path, world)

    def crash(*args: object, **kwargs: object) -> None:
        raise OSError("killed mid-write")

    monkeypatch.setattr("marketagg.storage.archive.pq.write_table", crash)
    with pytest.raises(OSError):
        manager.archive_month("bar_observations", "fake", MARCH)
    monkeypatch.undo()
    assert count(engine, "SELECT count(*) FROM archive_manifest WHERE state='failed'") == 1
    assert count(engine, "SELECT count(*) FROM bar_observations") == 30  # hot data intact
    assert not list((tmp_path / "archive").rglob(".archive-*"))
    assert manager.archive_and_activate("bar_observations", "fake", MARCH).state == "active"


def test_pruning_requires_offsite_grace_and_restore_proof(engine: Engine,
                                                          tmp_path: Path) -> None:
    world = _world(engine)
    manager = _manager(engine, tmp_path, world)
    generation = manager.archive_month("bar_observations", "fake", MARCH)
    with pytest.raises(ArchiveError, match="off-host"):
        manager.activate(generation.id)
    manager.copy_offsite(generation.id)
    manager.activate(generation.id)
    with pytest.raises(ArchiveError, match="restore drill"):
        manager.prune_hot_partition("bar_observations", "fake", MARCH, restore_verified=False)
    with pytest.raises(ArchiveError, match="grace"):
        manager.prune_hot_partition("bar_observations", "fake", MARCH, restore_verified=True)
    world.clock.advance(timedelta(days=8))
    ingest_minutes(world, OPEN + timedelta(minutes=30), OPEN + timedelta(minutes=40))
    with pytest.raises(ArchiveError, match="hot rows changed"):
        manager.prune_hot_partition("bar_observations", "fake", MARCH, restore_verified=True)
    assert count(engine, "SELECT count(*) FROM bar_observations") == 40
