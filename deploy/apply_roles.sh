#!/usr/bin/env bash
# Create the service login roles (deploy/roles.sql) and set their passwords from secrets/.
# Run after the first `migrate`; safe to re-run. Passwords travel over psql's stdin as
# psql variables, never on a command line.
set -euo pipefail
cd "$(dirname "$0")/.."
compose=(docker compose -f deploy/compose.yaml)
{
    printf '\\set app_pw %s\n\\set api_pw %s\n\\set backup_pw %s\n' \
        "$(cat secrets/db_app_password)" "$(cat secrets/db_api_password)" \
        "$(cat secrets/db_backup_password)"
    grep -v '^\\password' deploy/roles.sql
    cat <<'SQL'
ALTER ROLE marketagg_app PASSWORD :'app_pw';
ALTER ROLE marketagg_api PASSWORD :'api_pw';
ALTER ROLE marketagg_backup_login PASSWORD :'backup_pw';
SQL
} | "${compose[@]}" exec -T db psql -v ON_ERROR_STOP=1 -q -U marketagg_owner -d marketagg
echo "login roles ready"
