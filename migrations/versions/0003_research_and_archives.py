"""Calculation runs, metrics, surfaces, archive and dataset manifests.

Statements are verbatim from docs/build-guide/schema/001_reference.sql.

Revision ID: 0003_research_and_archives
Revises: 0002_observations
"""

from alembic import op

from marketagg.db.migrate import require_disposable_for_downgrade

revision = "0003_research_and_archives"
down_revision = '0002_observations'
branch_labels = None
depends_on = None

DDL = """
CREATE TABLE calculation_runs (
    id uuid PRIMARY KEY,
    policy_id text NOT NULL REFERENCES policy_versions,
    asof_at timestamptz NOT NULL,
    input_cutoff_at timestamptz NOT NULL,
    computed_at timestamptz NOT NULL,
    published_at timestamptz,
    availability_mode text NOT NULL CHECK (availability_mode IN ('observed','recomputed')),
    input_refs jsonb NOT NULL,
    input_sha256 text NOT NULL,
    assumptions jsonb NOT NULL,
    status text NOT NULL,
    UNIQUE(policy_id,asof_at,input_sha256,availability_mode)
);

CREATE TABLE derived_option_metrics (
    id uuid PRIMARY KEY,
    calculation_id uuid NOT NULL REFERENCES calculation_runs,
    snapshot_observed_at timestamptz NOT NULL,
    snapshot_id uuid NOT NULL,
    contract_id uuid NOT NULL REFERENCES option_contracts,
    metric_schema_version integer NOT NULL,
    values_json jsonb NOT NULL,
    diagnostics jsonb NOT NULL,
    UNIQUE(calculation_id,snapshot_observed_at,snapshot_id)
);

CREATE TABLE surfaces (
    id uuid PRIMARY KEY,
    calculation_id uuid NOT NULL REFERENCES calculation_runs,
    instrument_id uuid NOT NULL REFERENCES instruments,
    target_dte integer,
    target_delta numeric(8,6),
    option_type char(1) CHECK (option_type IN ('C','P')),
    points jsonb NOT NULL,
    diagnostics jsonb NOT NULL,
    UNIQUE NULLS NOT DISTINCT(calculation_id,instrument_id,target_dte,target_delta,option_type)
);

CREATE TABLE archive_manifest (
    id uuid PRIMARY KEY,
    dataset text NOT NULL,
    partition_key text NOT NULL,
    generation integer NOT NULL CHECK (generation > 0),
    schema_version integer NOT NULL,
    state text NOT NULL CHECK (state IN ('writing','verified','active','superseded','failed')),
    coverage_start timestamptz NOT NULL,
    coverage_end timestamptz NOT NULL,
    min_known_at timestamptz,
    max_known_at timestamptz,
    uri text NOT NULL,
    sha256 text NOT NULL,
    row_count bigint NOT NULL,
    byte_count bigint NOT NULL,
    provenance jsonb NOT NULL,
    created_at timestamptz NOT NULL,
    verified_at timestamptz,
    offsite_verified_at timestamptz,
    UNIQUE(dataset,partition_key,generation),
    CHECK (coverage_end > coverage_start)
);

CREATE UNIQUE INDEX one_active_archive ON archive_manifest(dataset,partition_key) WHERE state = 'active';

CREATE TABLE dataset_manifests (
    id uuid PRIMARY KEY,
    created_at timestamptz NOT NULL,
    schema_version integer NOT NULL,
    query_spec jsonb NOT NULL,
    input_refs jsonb NOT NULL,
    policy_versions jsonb NOT NULL,
    code_sha text NOT NULL,
    output_uri text NOT NULL,
    output_sha256 text NOT NULL,
    row_count bigint NOT NULL,
    eligibility_summary jsonb NOT NULL
);
"""


def upgrade() -> None:
    op.execute(DDL)


def downgrade() -> None:
    require_disposable_for_downgrade(op.get_bind())
    op.execute("DROP TABLE IF EXISTS dataset_manifests")
    op.execute("DROP TABLE IF EXISTS archive_manifest")
    op.execute("DROP TABLE IF EXISTS surfaces")
    op.execute("DROP TABLE IF EXISTS derived_option_metrics")
    op.execute("DROP TABLE IF EXISTS calculation_runs")
