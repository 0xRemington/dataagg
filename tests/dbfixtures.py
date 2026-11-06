"""Disposable PostgreSQL 18 database fixtures (registered from tests/conftest.py).

Set ``MARKETAGG_TEST_ADMIN_DSN`` (e.g. ``postgresql://postgres@127.0.0.1:54318/postgres``).
Without it, integration tests are skipped (reported as skipped, never as passed).
Every database created here is named ``marketagg_test_*``; the fixture refuses to
touch a DSN equal to the configured production ``DATABASE_URL_FILE``.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Engine, create_engine, make_url, text

from marketagg.db.engine import make_engine, normalize_url
from marketagg.db.migrate import DISPOSABLE_PREFIX, upgrade

ADMIN_ENV = "MARKETAGG_TEST_ADMIN_DSN"


def _admin_url() -> str:
    dsn = os.environ.get(ADMIN_ENV)
    if not dsn:
        pytest.skip(f"{ADMIN_ENV} not set: no disposable PostgreSQL 18 available")
    production = os.environ.get("DATABASE_URL_FILE")
    if production and Path(production).is_file():
        prod = make_url(normalize_url(Path(production).read_text().strip()))
        admin = make_url(normalize_url(dsn))
        if (prod.host, prod.port) == (admin.host, admin.port):
            pytest.exit("refusing to run integration tests against the production server")
    return normalize_url(dsn)


def _admin_engine() -> Engine:
    return create_engine(_admin_url(), isolation_level="AUTOCOMMIT")


def _db_url(name: str) -> str:
    assert name.startswith(DISPOSABLE_PREFIX)
    return make_url(_admin_url()).set(database=name).render_as_string(hide_password=False)


@pytest.fixture(scope="session")
def template_db() -> Iterator[str]:
    admin = _admin_engine()
    with admin.connect() as conn:
        version = conn.execute(text("SHOW server_version_num")).scalar_one()
        if int(version) < 180000:
            pytest.skip(f"PostgreSQL 18 required, server is {version}")
    name = f"{DISPOSABLE_PREFIX}tpl_{uuid.uuid4().hex[:10]}"
    with admin.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{name}"'))
    engine = make_engine(_db_url(name), statement_timeout_ms=None)
    upgrade(engine)
    engine.dispose()
    yield name
    with admin.connect() as conn:
        conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
    admin.dispose()


@pytest.fixture
def db_name(template_db: str) -> Iterator[str]:
    admin = _admin_engine()
    name = f"{DISPOSABLE_PREFIX}{uuid.uuid4().hex[:12]}"
    with admin.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{name}" TEMPLATE "{template_db}"'))
    yield name
    with admin.connect() as conn:
        conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
    admin.dispose()


@pytest.fixture
def engine(db_name: str) -> Iterator[Engine]:
    eng = make_engine(_db_url(db_name), pool_size=8, statement_timeout_ms=30_000)
    yield eng
    eng.dispose()


@pytest.fixture
def empty_db_url() -> Iterator[str]:
    admin = _admin_engine()
    name = f"{DISPOSABLE_PREFIX}empty_{uuid.uuid4().hex[:10]}"
    with admin.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{name}"'))
    yield _db_url(name)
    with admin.connect() as conn:
        conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
    admin.dispose()
