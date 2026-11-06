#!/usr/bin/env bash
# Create every secret file the Compose stack mounts (idempotent: existing files are kept).
# Run from the repository root: deploy/setup_secrets.sh
# Secrets are generated locally, written mode 0600 under secrets/, and never printed,
# except the dashboard password, which is written to secrets/ui_password for the operator.
set -euo pipefail
umask 077
cd "$(dirname "$0")/.."
mkdir -p secrets
chmod 700 secrets

gen() {  # gen FILE BYTES -> hex secret, created once
    [[ -s "secrets/$1" ]] || python3 -c "import secrets,sys; sys.stdout.write(secrets.token_hex($2))" \
        > "secrets/$1"
}

gen db_owner_password 24
gen db_app_password 24
gen db_api_password 24
gen db_backup_password 24
gen api_read_token 32
gen api_export_token 32
gen backup_key 32          # 64 hex chars; keep a copy OFF this VPS or backups are unrecoverable
gen ui_password 12

url() {  # url FILE ROLE PASSWORD_FILE
    [[ -s "secrets/$1" ]] || printf 'postgresql://%s:%s@db:5432/marketagg' "$2" \
        "$(cat "secrets/$3")" > "secrets/$1"
}
url database_url_owner marketagg_owner db_owner_password
url database_url marketagg_app db_app_password
url database_url_api marketagg_api db_api_password
url database_url_backup marketagg_backup_login db_backup_password

if [[ ! -s secrets/Caddyfile || secrets/ui_password -nt secrets/Caddyfile ]]; then
    hash=$(printf '%s\n' "$(cat secrets/ui_password)" \
        | docker run --rm -i caddy:2.10.2-alpine caddy hash-password | tail -1)
    [[ "$hash" == \$2* ]] || { echo "error: could not hash the dashboard password" >&2; exit 1; }
    sed -e "s|__UI_USER__|${UI_USER:-operator}|" -e "s|__UI_PASSWORD_HASH__|${hash}|" \
        deploy/Caddyfile.template > secrets/Caddyfile
fi
# Compose bind-mounts file secrets with host ownership: app containers (uid 10001) read
# them through supplementary group 10050 (group_add in compose.yaml); PostgreSQL (uid 999
# in the official image) owns only its own password file.
SECRETS_GID=10050
sudo chown "$(id -u):${SECRETS_GID}" secrets/*
sudo chmod 640 secrets/*
sudo chown 999:999 secrets/db_owner_password
sudo chmod 600 secrets/db_owner_password
sudo chmod 644 secrets/Caddyfile  # read by the proxy container; holds only a bcrypt hash
echo "secrets ready in secrets/ (dashboard user: ${UI_USER:-operator}, password in secrets/ui_password)"
