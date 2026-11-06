"""Services start under their least-privilege roles once the owner has bootstrapped.

Production regression (2026-10-26): every service upserted reference rows at startup,
which the ingest and reader roles may not do. Other integration tests connect as a
superuser and could not see it.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import Engine, create_engine, text

from marketagg.config import load_policies, load_universe
from marketagg.db.engine import make_engine
from marketagg.db.errors import InvariantViolation
from marketagg.providers.fake import FakeClock, FakeProvider, FakeScenario
from marketagg.runtime import bootstrap

ROOT = Path(__file__).resolve().parents[2]
UNIVERSE = load_universe(ROOT / "config" / "universe.yaml")
POLICIES = load_policies(ROOT / "config" / "policies.yaml")


@pytest.fixture
def role_engine(engine: Engine) -> Iterator[object]:
    created: list[str] = []
    engines: list[Engine] = []
    admin = create_engine(engine.url, isolation_level="AUTOCOMMIT")

    def connect(group: str) -> Engine:
        name = f"marketagg_test_login_{uuid.uuid4().hex[:8]}"
        with admin.connect() as conn:
            conn.execute(text(f'CREATE ROLE "{name}" LOGIN IN ROLE {group}'))
        created.append(name)
        eng = make_engine(engine.url.set(username=name, password=None).render_as_string(
            hide_password=False), statement_timeout_ms=30_000)
        engines.append(eng)
        return eng

    yield connect
    for eng in engines:
        eng.dispose()
    with admin.connect() as conn:
        for name in created:
            conn.execute(text(f'DROP OWNED BY "{name}"'))
            conn.execute(text(f'DROP ROLE "{name}"'))
    admin.dispose()


def _boot(engine: Engine, **kwargs: object) -> object:
    clock = FakeClock(datetime(2025, 3, 7, 14, 0, tzinfo=UTC))
    return bootstrap(engine, FakeProvider(FakeScenario(), clock), UNIVERSE, POLICIES,
                     clock=clock, **kwargs)  # type: ignore[arg-type]


def test_services_boot_read_only_after_owner_bootstrap(engine: Engine, role_engine: object
                                                       ) -> None:
    connect = role_engine  # type: ignore[assignment]
    ingest, reader = connect("marketagg_ingest"), connect("marketagg_reader")  # type: ignore[operator]
    with pytest.raises(InvariantViolation, match="migrate --bootstrap"):
        _boot(reader, provision_partitions=False)  # nothing registered yet
    owner_rt = _boot(engine)  # the migrate --bootstrap step (owner)
    ingest_rt = _boot(ingest)
    reader_rt = _boot(reader, provision_partitions=False)
    assert owner_rt.instruments == ingest_rt.instruments == reader_rt.instruments  # type: ignore[attr-defined]
    assert owner_rt.policy_ids == reader_rt.policy_ids  # type: ignore[attr-defined]


def test_backup_role_can_dump_partitions_created_after_migration(
        engine: Engine, role_engine: object, tmp_path: Path) -> None:
    from marketagg.ops.backup import create_backup  # noqa: PLC0415

    _boot(engine)  # owner bootstrap creates new monthly partitions via the definer function
    backup_engine = role_engine("marketagg_backup")  # type: ignore[operator]
    result = create_backup(backup_engine,
                           backup_engine.url.render_as_string(hide_password=False),
                           tmp_path / "backups", bytes(range(32)))
    assert result.manifest["entries"][0]["kind"] == "database"
