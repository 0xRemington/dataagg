"""Monthly partition provisioning through the validated SQL function (0004)."""

from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import Connection, Engine, text

PARTITIONED = ("bar_observations", "option_snapshots")


def month_starts(start: date | datetime, end: date | datetime) -> list[date]:
    """First days of every month touching ``[start, end]``."""
    current = date(start.year, start.month, 1)
    last = date(end.year, end.month, 1)
    months: list[date] = []
    while current <= last:
        months.append(current)
        current = date(current.year + current.month // 12, current.month % 12 + 1, 1)
    return months


def ensure_partitions(
    engine: Engine, start: date | datetime, end: date | datetime,
    tables: tuple[str, ...] = PARTITIONED,
) -> list[str]:
    with engine.begin() as conn:
        return ensure_partitions_conn(conn, start, end, tables)


def ensure_partitions_conn(
    conn: Connection, start: date | datetime, end: date | datetime,
    tables: tuple[str, ...] = PARTITIONED,
) -> list[str]:
    names: list[str] = []
    for table in tables:
        for month in month_starts(start, end):
            names.append(str(conn.execute(
                text("SELECT marketagg_ensure_month_partition(:t, :m)"),
                {"t": table, "m": month},
            ).scalar_one()))
    return names


def missing_partitions(
    engine: Engine, start: date | datetime, end: date | datetime,
    tables: tuple[str, ...] = PARTITIONED,
) -> list[str]:
    """Readiness check: expected partition names that do not exist."""
    expected = [f"{t}_{m:%Y_%m}" for t in tables for m in month_starts(start, end)]
    with engine.connect() as conn:
        existing = {
            row[0] for row in conn.execute(
                text("SELECT tablename FROM pg_tables WHERE schemaname='public'"))
        }
    return [name for name in expected if name not in existing]
