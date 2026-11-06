"""Database-enforced invariants and operational state (additive to the reference DDL).

- Immutable fact tables refuse UPDATE/DELETE/TRUNCATE. Pruning archived history
  detaches/drops verified partitions; it never deletes rows.
- Batches may only move staged -> published (with known_at >= recorded_at) or
  staged -> quarantined. Facts can be inserted only into a *staged* batch of the
  same provider, so a published batch is sealed.
- Poll snapshots carry the batch's fetch completion time as ``observed_at``.
- Operational tables for pacing, circuits, dirty derived windows, cursors,
  scheduler heartbeat, export jobs and incidents.

Revision ID: 0005_integrity_and_operations
Revises: 0004_roles_and_partitions
"""

from alembic import op

from marketagg.db.migrate import require_disposable_for_downgrade

revision = "0005_integrity_and_operations"
down_revision = "0004_roles_and_partitions"
branch_labels = None
depends_on = None

FACT_TABLES = (
    "policy_versions",
    "instruments",
    "instrument_identifiers",
    "option_contracts",
    "contract_identifiers",
    "contract_terms",
    "universe_memberships",
    "corporate_actions",
    "bar_observations",
    "option_snapshots",
    "underlying_marks",
    "derived_option_metrics",
    "surfaces",
    "dataset_manifests",
)

FUNCTIONS = """
CREATE FUNCTION marketagg_forbid_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'immutable table %: % refused', TG_TABLE_NAME, TG_OP
        USING ERRCODE = 'integrity_constraint_violation';
END;
$$;

CREATE FUNCTION marketagg_batch_transition() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'observation_batches rows cannot be deleted'
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF (to_jsonb(NEW) - 'status' - 'known_at') IS DISTINCT FROM
       (to_jsonb(OLD) - 'status' - 'known_at') THEN
        RAISE EXCEPTION 'only status/known_at of a batch may change'
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF OLD.status = NEW.status AND OLD.known_at IS NOT DISTINCT FROM NEW.known_at THEN
        RETURN NEW;
    END IF;
    IF OLD.status <> 'staged' THEN
        RAISE EXCEPTION 'batch % is already %', OLD.id, OLD.status
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.status = 'published' AND NEW.known_at < NEW.recorded_at THEN
        RAISE EXCEPTION 'known_at % precedes recorded_at %', NEW.known_at, NEW.recorded_at
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION marketagg_fact_batch_check() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    b record;
BEGIN
    SELECT provider_id, status, fetch_completed_at INTO b
    FROM observation_batches WHERE id = NEW.batch_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'batch % not found', NEW.batch_id
            USING ERRCODE = 'foreign_key_violation';
    END IF;
    IF b.status <> 'staged' THEN
        RAISE EXCEPTION 'batch % is % and sealed; facts go into a new staged batch',
            NEW.batch_id, b.status USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF b.provider_id <> NEW.provider_id THEN
        RAISE EXCEPTION 'row provider % differs from batch provider %',
            NEW.provider_id, b.provider_id USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF TG_TABLE_NAME = 'option_snapshots' THEN
        IF NEW.observation_kind = 'poll' AND NEW.observed_at <> b.fetch_completed_at THEN
            RAISE EXCEPTION 'poll observed_at % must equal batch fetch_completed_at %',
                NEW.observed_at, b.fetch_completed_at
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
"""

OPERATIONAL = """
ALTER TABLE payload_objects
    ADD COLUMN body_deleted_at timestamptz,
    ADD COLUMN missing_body_reason text,
    ADD CONSTRAINT payload_body_state CHECK (
        (body_deleted_at IS NULL) = (missing_body_reason IS NULL));

CREATE TABLE provider_pacing (
    provider_id text PRIMARY KEY REFERENCES providers,
    capacity numeric NOT NULL CHECK (capacity > 0),
    tokens numeric NOT NULL CHECK (tokens >= 0),
    refill_per_second numeric NOT NULL CHECK (refill_per_second > 0),
    updated_at timestamptz NOT NULL
);

CREATE TABLE provider_circuits (
    provider_id text NOT NULL REFERENCES providers,
    endpoint text NOT NULL,
    state text NOT NULL CHECK (state IN ('closed', 'open', 'disabled')),
    opened_at timestamptz,
    open_until timestamptz,
    consecutive_failures integer NOT NULL DEFAULT 0,
    reason text,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (provider_id, endpoint)
);

CREATE TABLE derived_dirty_windows (
    instrument_id uuid NOT NULL REFERENCES instruments,
    provider_id text NOT NULL REFERENCES providers,
    timeframe text NOT NULL CHECK (timeframe IN ('5m', '15m', '4h')),
    bucket_start timestamptz NOT NULL,
    bucket_end timestamptz NOT NULL,
    price_basis text NOT NULL,
    session_kind text NOT NULL,
    source_policy_id text NOT NULL REFERENCES policy_versions,
    dirtied_at timestamptz NOT NULL,
    processed_at timestamptz,
    PRIMARY KEY (instrument_id, provider_id, timeframe, bucket_start, price_basis,
                 session_kind, source_policy_id),
    CHECK (bucket_end > bucket_start)
);
CREATE INDEX dirty_pending ON derived_dirty_windows(bucket_end) WHERE processed_at IS NULL;

CREATE TABLE backfill_cursors (
    scope_key text PRIMARY KEY,
    cursor jsonb NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE scheduler_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    holder text NOT NULL,
    heartbeat_at timestamptz NOT NULL,
    last_planned_at timestamptz
);

CREATE TABLE dataset_jobs (
    id uuid PRIMARY KEY,
    request jsonb NOT NULL,
    request_sha256 text NOT NULL CHECK (length(request_sha256) = 64),
    state text NOT NULL CHECK (state IN ('queued', 'running', 'succeeded', 'failed',
                                         'cancelled')),
    created_at timestamptz NOT NULL,
    started_at timestamptz,
    finished_at timestamptz,
    manifest_id uuid REFERENCES dataset_manifests,
    error jsonb,
    expires_at timestamptz,
    pinned boolean NOT NULL DEFAULT false
);

CREATE TABLE operational_incidents (
    id uuid PRIMARY KEY,
    kind text NOT NULL,
    severity text NOT NULL CHECK (severity IN ('info', 'warning', 'critical')),
    details jsonb NOT NULL,
    opened_at timestamptz NOT NULL,
    resolved_at timestamptz
);
CREATE INDEX incidents_open ON operational_incidents(kind) WHERE resolved_at IS NULL;

CREATE INDEX batch_status_known ON observation_batches(status, known_at);
CREATE INDEX gaps_open ON data_gaps(provider_id, instrument_id, start_at)
    WHERE status IN ('open', 'retrying');

COMMENT ON COLUMN derived_option_metrics.snapshot_id IS
    'Logical snapshot reference: validated in repository; survives hot partition detach.';

GRANT SELECT ON provider_pacing, provider_circuits, derived_dirty_windows, backfill_cursors,
    scheduler_state, dataset_jobs, operational_incidents
    TO marketagg_ingest, marketagg_reader, marketagg_export, marketagg_backup,
       marketagg_maintenance;
GRANT INSERT, UPDATE ON provider_pacing, provider_circuits, derived_dirty_windows,
    backfill_cursors, scheduler_state, operational_incidents TO marketagg_ingest;
GRANT INSERT, UPDATE ON dataset_jobs TO marketagg_export;
GRANT UPDATE (body_deleted_at, missing_body_reason) ON payload_objects
    TO marketagg_maintenance;
"""


def _fact_triggers() -> str:
    statements = []
    for table in FACT_TABLES:
        statements.append(
            f"CREATE TRIGGER {table}_immutable BEFORE UPDATE OR DELETE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION marketagg_forbid_mutation();"
        )
        statements.append(
            f"CREATE TRIGGER {table}_no_truncate BEFORE TRUNCATE ON {table} "
            "FOR EACH STATEMENT EXECUTE FUNCTION marketagg_forbid_mutation();"
        )
    for table in ("bar_observations", "option_snapshots", "underlying_marks",
                  "corporate_actions"):
        statements.append(
            f"CREATE TRIGGER {table}_batch_check BEFORE INSERT ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION marketagg_fact_batch_check();"
        )
    statements.append(
        "CREATE TRIGGER observation_batches_transition BEFORE UPDATE OR DELETE "
        "ON observation_batches FOR EACH ROW EXECUTE FUNCTION marketagg_batch_transition();"
    )
    return "\n".join(statements)


def upgrade() -> None:
    op.execute(FUNCTIONS)
    op.execute(_fact_triggers())
    op.execute(OPERATIONAL)


def downgrade() -> None:
    require_disposable_for_downgrade(op.get_bind())
    for table in ("operational_incidents", "dataset_jobs", "scheduler_state",
                  "backfill_cursors", "derived_dirty_windows", "provider_circuits",
                  "provider_pacing"):
        op.execute(f"DROP TABLE IF EXISTS {table}")
    op.execute("DROP TRIGGER IF EXISTS observation_batches_transition ON observation_batches")
    for table in FACT_TABLES:
        op.execute(f"DROP TRIGGER IF EXISTS {table}_immutable ON {table}")
        op.execute(f"DROP TRIGGER IF EXISTS {table}_no_truncate ON {table}")
    for table in ("bar_observations", "option_snapshots", "underlying_marks",
                  "corporate_actions"):
        op.execute(f"DROP TRIGGER IF EXISTS {table}_batch_check ON {table}")
    op.execute("ALTER TABLE payload_objects DROP CONSTRAINT IF EXISTS payload_body_state, "
               "DROP COLUMN IF EXISTS body_deleted_at, DROP COLUMN IF EXISTS missing_body_reason")
    for fn in ("marketagg_fact_batch_check", "marketagg_batch_transition",
               "marketagg_forbid_mutation"):
        op.execute(f"DROP FUNCTION IF EXISTS {fn}()")
