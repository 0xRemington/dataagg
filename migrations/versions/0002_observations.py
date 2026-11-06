"""Payloads, batches, actions, bars, snapshots, marks, gaps and quality.

Statements are verbatim from docs/build-guide/schema/001_reference.sql.

Revision ID: 0002_observations
Revises: 0001_reference_and_jobs
"""

from alembic import op

from marketagg.db.migrate import require_disposable_for_downgrade

revision = "0002_observations"
down_revision = '0001_reference_and_jobs'
branch_labels = None
depends_on = None

DDL = """
CREATE TABLE payload_objects (
    id uuid PRIMARY KEY,
    provider_id text NOT NULL REFERENCES providers,
    sha256 text NOT NULL CHECK (length(sha256) = 64),
    uri text NOT NULL,
    representation text NOT NULL CHECK (representation IN ('provider_wire','adapter_result','synthetic')),
    parser_version text NOT NULL,
    byte_count bigint NOT NULL CHECK (byte_count >= 0),
    retained_until timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(provider_id,sha256,representation,parser_version)
);

CREATE TABLE observation_batches (
    id uuid PRIMARY KEY,
    run_id uuid REFERENCES ingestion_runs,
    provider_id text NOT NULL REFERENCES providers,
    response_identity text NOT NULL,
    payload_id uuid REFERENCES payload_objects,
    batch_kind text NOT NULL CHECK (batch_kind IN ('chain','bars','mark','action','derived','reference','stream')),
    scope jsonb NOT NULL,
    fetch_started_at timestamptz NOT NULL,
    fetch_completed_at timestamptz NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    known_at timestamptz,
    status text NOT NULL CHECK (status IN ('staged','published','quarantined')),
    advertised_delay_seconds integer CHECK (advertised_delay_seconds >= 0),
    coverage jsonb NOT NULL,
    UNIQUE(provider_id,response_identity),
    CHECK (fetch_completed_at >= fetch_started_at),
    CHECK (known_at IS NULL OR known_at >= fetch_completed_at),
    CHECK ((status = 'published') = (known_at IS NOT NULL))
);

CREATE INDEX batch_known ON observation_batches(provider_id,known_at);

CREATE TABLE corporate_actions (
    id uuid PRIMARY KEY,
    instrument_id uuid NOT NULL REFERENCES instruments,
    provider_id text NOT NULL REFERENCES providers,
    batch_id uuid NOT NULL REFERENCES observation_batches,
    action_type text NOT NULL CHECK (action_type IN ('split','dividend','symbol_change','delisting','deliverable_change')),
    effective_date date NOT NULL,
    announced_at timestamptz,
    details jsonb NOT NULL,
    supersedes_id uuid REFERENCES corporate_actions
);

CREATE TABLE bar_observations (
    interval_start timestamptz NOT NULL,
    id uuid NOT NULL,
    instrument_id uuid NOT NULL REFERENCES instruments,
    provider_id text NOT NULL REFERENCES providers,
    batch_id uuid NOT NULL REFERENCES observation_batches,
    timeframe text NOT NULL CHECK (timeframe IN ('1m','5m','15m','4h','1d')),
    interval_end timestamptz NOT NULL,
    session_date date NOT NULL,
    session_kind text NOT NULL CHECK (session_kind IN ('RTH','ETH')),
    price_basis text NOT NULL CHECK (price_basis IN ('provider_native','as_traded','split_adjusted','total_return_adjusted')),
    policy_id text NOT NULL REFERENCES policy_versions,
    open numeric(20,8) NOT NULL,
    high numeric(20,8) NOT NULL,
    low numeric(20,8) NOT NULL,
    close numeric(20,8) NOT NULL,
    volume bigint CHECK (volume >= 0),
    expected_minutes integer CHECK (expected_minutes > 0),
    observed_minutes integer CHECK (observed_minutes >= 0),
    complete boolean NOT NULL,
    is_short_session_bucket boolean NOT NULL DEFAULT false,
    content_sha256 text NOT NULL CHECK (length(content_sha256) = 64),
    input_refs jsonb NOT NULL DEFAULT '[]',
    supersedes_id uuid,
    quality_flags text[] NOT NULL DEFAULT '{}',
    PRIMARY KEY(interval_start,id),
    UNIQUE(interval_start,batch_id,instrument_id,timeframe,session_kind,price_basis,policy_id),
    CHECK (interval_end > interval_start),
    CHECK (low <= high AND open BETWEEN low AND high AND close BETWEEN low AND high),
    CHECK (observed_minutes IS NULL OR expected_minutes IS NULL OR observed_minutes <= expected_minutes),
    CHECK (timeframe <> '1m' OR interval_end = interval_start + interval '1 minute')
) PARTITION BY RANGE(interval_start);

CREATE INDEX bar_lookup ON bar_observations(instrument_id,timeframe,interval_start DESC,provider_id);

CREATE INDEX bar_batch ON bar_observations(batch_id);

CREATE VIEW ohlcv_1m AS SELECT * FROM bar_observations WHERE timeframe = '1m';

CREATE VIEW ohlcv_daily AS SELECT * FROM bar_observations WHERE timeframe = '1d';

CREATE VIEW ohlcv_derived AS SELECT * FROM bar_observations WHERE timeframe IN ('5m','15m','4h');

CREATE TABLE option_snapshots (
    observed_at timestamptz NOT NULL,
    id uuid NOT NULL,
    batch_id uuid NOT NULL REFERENCES observation_batches,
    contract_id uuid NOT NULL REFERENCES option_contracts,
    contract_terms_id uuid REFERENCES contract_terms,
    provider_id text NOT NULL REFERENCES providers,
    observation_kind text NOT NULL CHECK (observation_kind IN ('poll','event','checkpoint')),
    provider_event_at timestamptz,
    provider_event_ns bigint,
    provider_sequence text,
    last_trade_at timestamptz,
    bid numeric(20,8),
    ask numeric(20,8),
    last numeric(20,8),
    bid_size bigint,
    ask_size bigint,
    reported_iv double precision,
    reported_volume bigint,
    reported_open_interest bigint,
    volume_session_date date,
    open_interest_asof_date date,
    in_the_money boolean,
    field_semantics jsonb NOT NULL,
    quality_flags text[] NOT NULL DEFAULT '{}',
    record_revision integer NOT NULL DEFAULT 1 CHECK (record_revision > 0),
    supersedes_id uuid,
    supersedes_observed_at timestamptz,
    content_sha256 text NOT NULL CHECK (length(content_sha256) = 64),
    PRIMARY KEY(observed_at,id),
    UNIQUE(observed_at,batch_id,contract_id,record_revision),
    CHECK ((supersedes_id IS NULL) = (supersedes_observed_at IS NULL))
) PARTITION BY RANGE(observed_at);

CREATE INDEX snapshot_contract_time ON option_snapshots(contract_id,observed_at DESC,provider_id);

CREATE INDEX snapshot_batch ON option_snapshots(batch_id);

CREATE INDEX snapshot_time_brin ON option_snapshots USING brin(observed_at);

CREATE TABLE underlying_marks (
    id uuid PRIMARY KEY,
    instrument_id uuid NOT NULL REFERENCES instruments,
    provider_id text NOT NULL REFERENCES providers,
    batch_id uuid NOT NULL REFERENCES observation_batches,
    observed_at timestamptz NOT NULL,
    provider_event_at timestamptz,
    kind text NOT NULL CHECK (kind IN ('bid_ask_mid','last_trade','bar_close','provider_reference')),
    price numeric(20,8) NOT NULL,
    price_basis text NOT NULL,
    quality_flags text[] NOT NULL DEFAULT '{}',
    field_semantics jsonb NOT NULL
);

CREATE INDEX mark_asof ON underlying_marks(instrument_id,observed_at DESC);

CREATE TABLE data_gaps (
    id uuid PRIMARY KEY,
    provider_id text NOT NULL REFERENCES providers,
    instrument_id uuid REFERENCES instruments,
    kind text NOT NULL,
    start_at timestamptz NOT NULL,
    end_at timestamptz NOT NULL,
    scope jsonb NOT NULL,
    status text NOT NULL CHECK (status IN ('open','retrying','repaired','unrecoverable','expected_closed','halt_confirmed')),
    first_detected_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    recoverable_until timestamptz,
    attempt_count integer NOT NULL DEFAULT 0,
    repair_job_id uuid REFERENCES collection_jobs,
    evidence jsonb NOT NULL,
    CHECK (end_at > start_at)
);

CREATE TABLE quality_issues (
    id uuid PRIMARY KEY,
    batch_id uuid REFERENCES observation_batches,
    entity_ref jsonb NOT NULL,
    code text NOT NULL,
    severity text NOT NULL CHECK (severity IN ('info','warning','error')),
    details jsonb NOT NULL,
    detected_at timestamptz NOT NULL,
    resolved_at timestamptz,
    resolution jsonb
);
"""


def upgrade() -> None:
    op.execute(DDL)


def downgrade() -> None:
    require_disposable_for_downgrade(op.get_bind())
    op.execute("DROP TABLE IF EXISTS quality_issues")
    op.execute("DROP TABLE IF EXISTS data_gaps")
    op.execute("DROP TABLE IF EXISTS underlying_marks")
    op.execute("DROP TABLE IF EXISTS option_snapshots")
    op.execute("DROP VIEW IF EXISTS ohlcv_derived")
    op.execute("DROP VIEW IF EXISTS ohlcv_daily")
    op.execute("DROP VIEW IF EXISTS ohlcv_1m")
    op.execute("DROP TABLE IF EXISTS bar_observations")
    op.execute("DROP TABLE IF EXISTS corporate_actions")
    op.execute("DROP TABLE IF EXISTS observation_batches")
    op.execute("DROP TABLE IF EXISTS payload_objects")
