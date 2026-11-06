"""Providers, policies, identities, terms, memberships, jobs and runs.

Statements are verbatim from docs/build-guide/schema/001_reference.sql.

Revision ID: 0001_reference_and_jobs
Revises: None
"""

from alembic import op

from marketagg.db.migrate import require_disposable_for_downgrade

revision = "0001_reference_and_jobs"
down_revision = None
branch_labels = None
depends_on = None

DDL = """
CREATE TABLE providers (
    id text PRIMARY KEY,
    reliability_tier text NOT NULL,
    capabilities jsonb NOT NULL,
    terms_review jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE policy_versions (
    id text PRIMARY KEY,
    kind text NOT NULL,
    config jsonb NOT NULL,
    code_sha text NOT NULL,
    content_sha256 text NOT NULL CHECK (length(content_sha256) = 64),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE instruments (
    id uuid PRIMARY KEY,
    asset_class text NOT NULL CHECK (asset_class IN ('equity','etf')),
    currency text NOT NULL,
    calendar_id text NOT NULL,
    exchange_timezone text NOT NULL DEFAULT 'America/New_York',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE instrument_identifiers (
    id uuid PRIMARY KEY,
    instrument_id uuid NOT NULL REFERENCES instruments,
    provider_id text NOT NULL REFERENCES providers,
    symbol text NOT NULL,
    exchange_mic text,
    valid_from date NOT NULL,
    valid_to date,
    known_at timestamptz NOT NULL,
    status text NOT NULL CHECK (status IN ('active','inactive','unknown')),
    evidence jsonb NOT NULL,
    supersedes_id uuid REFERENCES instrument_identifiers,
    CHECK (valid_to IS NULL OR valid_to > valid_from)
);

CREATE INDEX instrument_symbol_lookup ON instrument_identifiers(provider_id,symbol,known_at DESC);

CREATE TABLE option_contracts (
    id uuid PRIMARY KEY,
    underlying_id uuid NOT NULL REFERENCES instruments,
    option_type char(1) NOT NULL CHECK (option_type IN ('C','P')),
    strike numeric(20,8) NOT NULL CHECK (strike > 0),
    expiration_date date NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX contract_browser ON option_contracts(underlying_id,expiration_date,option_type,strike);

CREATE TABLE contract_identifiers (
    id uuid PRIMARY KEY,
    contract_id uuid NOT NULL REFERENCES option_contracts,
    provider_id text NOT NULL REFERENCES providers,
    symbol text NOT NULL,
    valid_from date NOT NULL,
    valid_to date,
    known_at timestamptz NOT NULL,
    evidence jsonb NOT NULL,
    supersedes_id uuid REFERENCES contract_identifiers,
    CHECK (valid_to IS NULL OR valid_to > valid_from)
);

CREATE INDEX contract_symbol_lookup ON contract_identifiers(provider_id,symbol,known_at DESC);

CREATE TABLE contract_terms (
    id uuid PRIMARY KEY,
    contract_id uuid NOT NULL REFERENCES option_contracts,
    effective_at timestamptz NOT NULL,
    known_at timestamptz NOT NULL,
    multiplier numeric(20,8) CHECK (multiplier > 0),
    currency text NOT NULL,
    exercise_style text NOT NULL CHECK (exercise_style IN ('american','european','unknown')),
    settlement_style text NOT NULL CHECK (settlement_style IN ('physical','cash','unknown')),
    expiry_at timestamptz,
    last_trading_at timestamptz,
    deliverable jsonb,
    standard_contract boolean,
    provider_id text NOT NULL REFERENCES providers,
    evidence jsonb NOT NULL,
    supersedes_id uuid REFERENCES contract_terms
);

CREATE INDEX terms_asof ON contract_terms(contract_id,effective_at,known_at DESC);

CREATE TABLE universe_memberships (
    id uuid PRIMARY KEY,
    instrument_id uuid NOT NULL REFERENCES instruments,
    contract_id uuid REFERENCES option_contracts,
    policy_id text NOT NULL REFERENCES policy_versions,
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    known_at timestamptz NOT NULL,
    selected boolean NOT NULL,
    reason text NOT NULL,
    CHECK (effective_to IS NULL OR effective_to > effective_from)
);

CREATE TABLE collection_jobs (
    id uuid PRIMARY KEY,
    job_key text NOT NULL UNIQUE,
    kind text NOT NULL,
    provider_id text REFERENCES providers,
    scheduled_for timestamptz NOT NULL,
    not_before timestamptz NOT NULL,
    deadline_at timestamptz,
    scope jsonb NOT NULL,
    policy_id text NOT NULL REFERENCES policy_versions,
    priority integer NOT NULL,
    status text NOT NULL CHECK (status IN ('pending','running','succeeded','partial','failed','skipped','cancelled')),
    lease_owner text,
    lease_token bigint NOT NULL DEFAULT 0,
    lease_until timestamptz,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX jobs_claim ON collection_jobs(priority,not_before) WHERE status IN ('pending','running');

CREATE TABLE ingestion_runs (
    id uuid PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES collection_jobs,
    attempt integer NOT NULL CHECK (attempt > 0),
    lease_token bigint NOT NULL,
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    status text NOT NULL,
    endpoint_requests integer NOT NULL DEFAULT 0,
    transport_requests integer,
    rows_received bigint NOT NULL DEFAULT 0,
    rows_written bigint NOT NULL DEFAULT 0,
    error_code text,
    diagnostics jsonb NOT NULL DEFAULT '{}',
    UNIQUE(job_id,attempt),
    CHECK (finished_at IS NULL OR finished_at >= started_at)
);
"""


def upgrade() -> None:
    op.execute(DDL)


def downgrade() -> None:
    require_disposable_for_downgrade(op.get_bind())
    op.execute("DROP TABLE IF EXISTS ingestion_runs")
    op.execute("DROP TABLE IF EXISTS collection_jobs")
    op.execute("DROP TABLE IF EXISTS universe_memberships")
    op.execute("DROP TABLE IF EXISTS contract_terms")
    op.execute("DROP TABLE IF EXISTS contract_identifiers")
    op.execute("DROP TABLE IF EXISTS option_contracts")
    op.execute("DROP TABLE IF EXISTS instrument_identifiers")
    op.execute("DROP TABLE IF EXISTS instruments")
    op.execute("DROP TABLE IF EXISTS policy_versions")
    op.execute("DROP TABLE IF EXISTS providers")
