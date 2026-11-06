-- Planning reference, PostgreSQL 18. Translate to ordered Alembic migrations.
-- Empty partitioned parents are intentional: migration/runtime provisions months.
-- Timestamps are UTC at API boundaries; PostgreSQL stores timestamptz instants.
BEGIN;

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
    -- Logical snapshot reference: validated in repository; survives hot partition detach.
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

COMMIT;
