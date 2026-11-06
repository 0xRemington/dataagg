"""Roles, grants, and monthly partition provisioning.

Partition names are derived from validated dates inside a SECURITY DEFINER
function; there is no default catch-all partition, so a missing month fails
loudly. Group roles are cluster-wide and created only if absent; login users
for services are created by deployment (T17) as members of these roles.

Revision ID: 0004_roles_and_partitions
Revises: 0003_research_and_archives
"""

from alembic import op

from marketagg.db.migrate import require_disposable_for_downgrade

revision = "0004_roles_and_partitions"
down_revision = "0003_research_and_archives"
branch_labels = None
depends_on = None

PARTITION_FUNCTION = """
CREATE FUNCTION marketagg_ensure_month_partition(parent text, month_start date)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
DECLARE
    part text;
    lower_ts timestamptz;
    upper_ts timestamptz;
BEGIN
    IF parent NOT IN ('bar_observations', 'option_snapshots') THEN
        RAISE EXCEPTION 'unsupported partitioned table %', parent;
    END IF;
    IF month_start IS NULL OR month_start <> date_trunc('month', month_start)::date THEN
        RAISE EXCEPTION 'month_start must be the first day of a month: %', month_start;
    END IF;
    IF month_start < date '1990-01-01' OR month_start >= date '2200-01-01' THEN
        RAISE EXCEPTION 'month_start out of supported range: %', month_start;
    END IF;
    part := parent || '_' || to_char(month_start, 'YYYY_MM');
    PERFORM pg_advisory_xact_lock(hashtextextended('partition:' || part, 0));
    IF to_regclass(format('public.%I', part)) IS NULL THEN
        lower_ts := month_start::timestamp AT TIME ZONE 'UTC';
        upper_ts := (month_start + interval '1 month')::timestamp AT TIME ZONE 'UTC';
        EXECUTE format(
            'CREATE TABLE public.%I PARTITION OF public.%I FOR VALUES FROM (%L) TO (%L)',
            part, parent, lower_ts, upper_ts
        );
    END IF;
    RETURN part;
END;
$$;
REVOKE ALL ON FUNCTION marketagg_ensure_month_partition(text, date) FROM PUBLIC;
"""

INITIAL_PARTITIONS = """
SELECT marketagg_ensure_month_partition(p.parent, (date_trunc('month', now() AT TIME ZONE 'UTC')
                                                   + make_interval(months => m))::date)
FROM (VALUES ('bar_observations'), ('option_snapshots')) AS p(parent)
CROSS JOIN generate_series(0, 2) AS m;
"""

ROLES = """
DO $$
DECLARE
    r text;
BEGIN
    FOREACH r IN ARRAY ARRAY['marketagg_ingest', 'marketagg_reader', 'marketagg_export',
                             'marketagg_backup', 'marketagg_maintenance']
    LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
            EXECUTE format('CREATE ROLE %I NOLOGIN', r);
        END IF;
    END LOOP;
END;
$$;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO marketagg_ingest, marketagg_reader, marketagg_export,
    marketagg_backup, marketagg_maintenance;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO marketagg_ingest, marketagg_reader,
    marketagg_export, marketagg_backup, marketagg_maintenance;

-- Ingest appends facts; it may update only operational state.
GRANT INSERT ON ALL TABLES IN SCHEMA public TO marketagg_ingest;
REVOKE INSERT ON archive_manifest, dataset_manifests FROM marketagg_ingest;
GRANT UPDATE ON collection_jobs, ingestion_runs, data_gaps, quality_issues, calculation_runs
    TO marketagg_ingest;
GRANT UPDATE (status, known_at) ON observation_batches TO marketagg_ingest;
GRANT UPDATE (retained_until) ON payload_objects TO marketagg_ingest;

GRANT INSERT ON dataset_manifests TO marketagg_export;
GRANT INSERT, UPDATE ON archive_manifest TO marketagg_maintenance;

GRANT EXECUTE ON FUNCTION marketagg_ensure_month_partition(text, date)
    TO marketagg_ingest, marketagg_maintenance;
"""


def upgrade() -> None:
    op.execute(PARTITION_FUNCTION)
    op.execute(INITIAL_PARTITIONS)
    op.execute(ROLES)


def downgrade() -> None:
    require_disposable_for_downgrade(op.get_bind())
    op.execute("DROP FUNCTION IF EXISTS marketagg_ensure_month_partition(text, date)")
