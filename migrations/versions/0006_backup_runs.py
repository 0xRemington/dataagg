"""Backup run records for operator status (additive).

Revision ID: 0006_backup_runs
Revises: 0005_integrity_and_operations
"""

from alembic import op

from marketagg.db.migrate import require_disposable_for_downgrade

revision = "0006_backup_runs"
down_revision = "0005_integrity_and_operations"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE backup_runs (
            id uuid PRIMARY KEY,
            started_at timestamptz NOT NULL,
            finished_at timestamptz,
            state text NOT NULL CHECK (state IN ('running', 'verified', 'failed')),
            destination text NOT NULL,
            manifest_sha256 text,
            db_known_at_max timestamptz,
            object_count integer,
            byte_count bigint,
            error text
        );
        GRANT SELECT ON backup_runs TO marketagg_ingest, marketagg_reader, marketagg_export,
            marketagg_backup, marketagg_maintenance;
        GRANT INSERT, UPDATE ON backup_runs TO marketagg_backup;
    """)


def downgrade() -> None:
    require_disposable_for_downgrade(op.get_bind())
    op.execute("DROP TABLE IF EXISTS backup_runs")
