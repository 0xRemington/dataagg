-- Login roles for services (run once as the owner after `marketagg migrate`).
-- Passwords are set interactively with \password, never stored in this file.
-- Group roles (marketagg_ingest, marketagg_reader, ...) are created by migration 0004.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'marketagg_app') THEN
        CREATE ROLE marketagg_app LOGIN IN ROLE marketagg_ingest;      -- scheduler, worker
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'marketagg_api') THEN
        CREATE ROLE marketagg_api LOGIN IN ROLE marketagg_reader, marketagg_export;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'marketagg_backup_login') THEN
        CREATE ROLE marketagg_backup_login LOGIN IN ROLE marketagg_backup;
    END IF;
END;
$$;
ALTER ROLE marketagg_api SET statement_timeout = '15s';
\password marketagg_app
\password marketagg_api
\password marketagg_backup_login
