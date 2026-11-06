from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
from sqlalchemy import Engine, text
from sqlalchemy.exc import DBAPIError

from marketagg.db.engine import make_engine
from marketagg.db.migrate import (
    DOWNGRADE_ENV,
    DestructiveDowngradeRefused,
    current_revision,
    downgrade,
    head_revision,
    upgrade,
)
from marketagg.db.partitions import ensure_partitions, missing_partitions

REFERENCE_TABLES = {
    "providers", "policy_versions", "instruments", "instrument_identifiers", "option_contracts",
    "contract_identifiers", "contract_terms", "universe_memberships", "collection_jobs",
    "ingestion_runs", "payload_objects", "observation_batches", "corporate_actions",
    "bar_observations", "option_snapshots", "underlying_marks", "data_gaps", "quality_issues",
    "calculation_runs", "derived_option_metrics", "surfaces", "archive_manifest",
    "dataset_manifests",
}


def _tables(engine: Engine) -> set[str]:
    with engine.connect() as conn:
        return set(conn.execute(text(
            "SELECT tablename FROM pg_tables WHERE schemaname='public'")).scalars())


def test_empty_database_upgrades_to_single_head(empty_db_url: str) -> None:
    engine = make_engine(empty_db_url)
    upgrade(engine)
    with engine.connect() as conn:
        assert current_revision(conn) == head_revision() == "0008_crypto_asset_class"
        views = set(conn.execute(text(
            "SELECT viewname FROM pg_views WHERE schemaname='public'")).scalars())
    assert _tables(engine) >= REFERENCE_TABLES
    assert {"ohlcv_1m", "ohlcv_daily", "ohlcv_derived"} <= views
    now = datetime.now(UTC)
    assert missing_partitions(engine, now, now.replace(day=1)) == []
    upgrade(engine)  # idempotent at head
    engine.dispose()


def test_downgrade_refused_without_disposable_opt_in(
    engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv(DOWNGRADE_ENV, raising=False)
    with pytest.raises(DestructiveDowngradeRefused):
        downgrade(engine, "0004_roles_and_partitions")
    with engine.connect() as conn:
        assert current_revision(conn) == head_revision()


def test_disposable_round_trip(engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(DOWNGRADE_ENV, "1")
    downgrade(engine, "base")
    assert REFERENCE_TABLES.isdisjoint(_tables(engine))
    upgrade(engine)
    assert _tables(engine) >= REFERENCE_TABLES


def test_partition_function_validates_input_and_has_no_default(engine: Engine) -> None:
    names = ensure_partitions(engine, date(2024, 11, 15), date(2025, 1, 2))
    assert "bar_observations_2024_12" in names and "option_snapshots_2025_01" in names
    ensure_partitions(engine, date(2024, 12, 1), date(2024, 12, 1))  # idempotent
    with pytest.raises(DBAPIError, match="first day"), engine.begin() as conn:
        conn.execute(text("SELECT marketagg_ensure_month_partition('bar_observations', "
                          "'2024-12-05')"))
    with pytest.raises(DBAPIError, match="unsupported"), engine.begin() as conn:
        conn.execute(text("SELECT marketagg_ensure_month_partition('providers', '2024-12-01')"))
    with engine.connect() as conn:
        defaults = conn.execute(text("""
            SELECT count(*) FROM pg_partitioned_table WHERE partdefid <> 0""")).scalar_one()
    assert defaults == 0


def test_roles_follow_least_privilege(engine: Engine) -> None:
    def priv(role: str, table: str, mode: str) -> bool:
        with engine.connect() as conn:
            return bool(conn.execute(text("SELECT has_table_privilege(:r, :t, :m)"),
                                     {"r": role, "t": table, "m": mode}).scalar_one())

    assert priv("marketagg_ingest", "option_snapshots", "INSERT")
    assert not priv("marketagg_ingest", "option_snapshots", "UPDATE")
    assert not priv("marketagg_ingest", "option_snapshots", "DELETE")
    assert not priv("marketagg_ingest", "archive_manifest", "INSERT")
    assert priv("marketagg_reader", "bar_observations", "SELECT")
    assert not priv("marketagg_reader", "bar_observations", "INSERT")
    assert priv("marketagg_export", "dataset_manifests", "INSERT")
    assert priv("marketagg_maintenance", "archive_manifest", "UPDATE")
    with engine.connect() as conn:
        allowed = conn.execute(text(
            "SELECT has_column_privilege('marketagg_ingest', 'observation_batches', "
            "'coverage', 'UPDATE')")).scalar_one()
    assert allowed is False
