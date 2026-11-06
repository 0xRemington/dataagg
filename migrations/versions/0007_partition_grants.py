"""Backup role can read partitions created after 0004 (additive).

pg_dump locks every partition directly, but partitions created later by
marketagg_ensure_month_partition (SECURITY DEFINER, runs as the owner) had no
grants, so a least-privilege backup failed with "permission denied". Default
privileges make every future owner-created table readable by the backup role.

Revision ID: 0007_partition_grants
Revises: 0006_backup_runs
"""

from alembic import op

from marketagg.db.migrate import require_disposable_for_downgrade

revision = "0007_partition_grants"
down_revision = "0006_backup_runs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("GRANT SELECT ON ALL TABLES IN SCHEMA public TO marketagg_backup")
    op.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES "
               "TO marketagg_backup")


def downgrade() -> None:
    require_disposable_for_downgrade(op.get_bind())
    op.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE SELECT ON TABLES "
               "FROM marketagg_backup")
