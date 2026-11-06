"""Alembic environment. Migrations are raw SQL; there is no ORM metadata.

Connection source, in order:
1. ``config.attributes["connection"]`` injected by tests/tools (``marketagg.db.migrate``).
2. ``DATABASE_URL_FILE`` resolved through typed settings (secret file, never inline).
"""

from __future__ import annotations

from alembic import context
from sqlalchemy import Connection, create_engine

from marketagg.db.engine import database_url_from_settings


def _run(connection: Connection) -> None:
    context.configure(connection=connection, transaction_per_migration=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    injected = context.config.attributes.get("connection")
    if injected is not None:
        _run(injected)
        return
    engine = create_engine(database_url_from_settings(), pool_pre_ping=True)
    with engine.connect() as connection:
        _run(connection)
    engine.dispose()


if context.is_offline_mode():
    raise SystemExit("offline SQL generation is not supported; run against a database")
run_migrations_online()
