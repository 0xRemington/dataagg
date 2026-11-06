"""Programmatic migration entry points and downgrade guard."""

from __future__ import annotations

import os
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import Connection, Engine, text

ROOT = Path(__file__).resolve().parents[3]
DISPOSABLE_PREFIX = "marketagg_test_"
DOWNGRADE_ENV = "MARKETAGG_ALLOW_DISPOSABLE_DOWNGRADE"


class DestructiveDowngradeRefused(RuntimeError):
    pass


def alembic_config() -> Config:
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "migrations"))
    return config


def head_revision() -> str:
    head = ScriptDirectory.from_config(alembic_config()).get_current_head()
    if head is None:
        raise RuntimeError("no migration head")
    return head


def upgrade(engine: Engine, revision: str = "head") -> None:
    config = alembic_config()
    with engine.begin() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, revision)


def downgrade(engine: Engine, revision: str) -> None:
    config = alembic_config()
    with engine.begin() as connection:
        config.attributes["connection"] = connection
        command.downgrade(config, revision)


def current_revision(connection: Connection) -> str | None:
    exists = connection.execute(text("SELECT to_regclass('public.alembic_version')")).scalar()
    if exists is None:
        return None
    value = connection.execute(text("SELECT version_num FROM alembic_version")).scalar()
    return None if value is None else str(value)


def require_disposable_for_downgrade(connection: Connection) -> None:
    """Downgrades drop fact tables: allowed only on explicit disposable test DBs."""
    name = str(connection.execute(text("SELECT current_database()")).scalar())
    if not name.startswith(DISPOSABLE_PREFIX) or os.environ.get(DOWNGRADE_ENV) != "1":
        raise DestructiveDowngradeRefused(
            f"downgrade of {name!r} refused: populated fact tables are never dropped outside "
            f"a disposable '{DISPOSABLE_PREFIX}*' database with {DOWNGRADE_ENV}=1"
        )
