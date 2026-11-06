# Deploy and upgrade (private single VPS)

No VPS, cloud account or vendor service is created by this repository. Record the actual region, specs and quote when you provision one.

## First deployment

This is the procedure used on the live VPS on 2026-10-26 (Ubuntu 24.04, Docker 29.1, Compose 2.40).

1. Install Docker: `sudo apt-get install docker.io docker-compose-v2`, then `sudo usermod -aG docker ubuntu`. On hosts with less than 4 GiB RAM, add swap; this host has a 2 GiB `/swapfile`.
2. Create every secret: `deploy/setup_secrets.sh`. It is idempotent and never prints secrets. It writes `secrets/` with:
   - database passwords and the four role URLs;
   - the API read and export tokens;
   - the backup key. **Copy `secrets/backup_key` off the VPS, or backups cannot be restored after host loss.**
   - the dashboard password (`secrets/ui_password`, user `operator`) and `secrets/Caddyfile`, which holds only a bcrypt hash.

   App secrets are group 10050, mode 0640, and app containers get `group_add: 10050`. The DB owner password is owned by uid 999 (postgres).
3. Write `.env` (mode 0600) with the live settings: `PROVIDER=yahoo`, `LIVE_DATA_ENABLED=true`, `SOURCE_REVIEW_ID=operator-2026-10-26`, `UNIVERSE_CONFIG=config/universe.yahoo.yaml`, `BACKUP_DESTINATION=/backups`. `MARKETAGG_PUBLIC_PORT` (default 8000) picks the public port.
4. Validate and dry-run:
   ```text
   docker compose -f deploy/compose.yaml config --quiet
   docker compose -f deploy/compose.yaml build
   docker compose -f deploy/compose.yaml run --rm --no-deps scheduler config check --profile production
   ```
5. Start the database, migrate, then create the login roles. Passwords go over psql stdin:
   ```text
   docker compose -f deploy/compose.yaml up -d db
   docker compose -f deploy/compose.yaml run --rm migrate
   deploy/apply_roles.sh
   ```
6. Start everything: `docker compose -f deploy/compose.yaml up -d`. On every start, `migrate --bootstrap` runs as the owner before any service. It registers the provider, policies, instruments and partitions, and enqueues the one-time history bootstrap: 5-year daily backfill, recent 1m sweep, and derivation.
7. Install the host timers (units in `/etc/systemd/system/`):
   - `marketagg-backup.timer` takes a verified backup daily at 17:00 UTC, during the session.
   - `marketagg-pilot-report.timer` runs `deploy/pilot_report.sh` on 2026-10-31.
8. Restrict the proxy port to Tailscale: `sudo systemctl enable --now marketagg-firewall.service` (runs `deploy/firewall.sh`). Docker-published ports bypass ufw, so the unit drops port-8000 traffic not arriving on `tailscale0` in the `DOCKER-USER` chain, and re-applies after reboots and Docker restarts. Port 8000 does not need to be open in the cloud firewall; SSH (22) is left as it is.

## Upgrades

1. Record the current image digest and schema revision, then take a verified backup.
2. Pause new claims by stopping the scheduler. Let the worker drain, or let its leases expire.
3. Apply the additive migration (`migrate` service). Deploy the compatible image.
4. Run read/write smoke tests and resume the scheduler.
5. Roll back the application only if the schema stays backward compatible. Never downgrade observation tables. Restore into a separate instance if needed.

## Network and access

- **Entry point (tailnet only):** the `proxy` service (Caddy, digest-pinned) publishes `0.0.0.0:8000`, but `marketagg-firewall.service` drops every connection to it that does not arrive over Tailscale (since 2026-10-27; public scanners were probing it). Use `http://100.66.225.1:8000`. The dashboard is behind HTTP basic auth (user `operator`, password in `secrets/ui_password`). `/v1/*` goes to the API, which needs `Authorization: Bearer <secrets/api_read_token>`. `/health/*` is unauthenticated and contains no secrets.
- **Plain HTTP:** Caddy itself serves no TLS; Tailscale's WireGuard tunnel encrypts traffic between your devices and the VPS.
- **Private services:** the database, API and UI publish no host ports. Containers attached only to an `internal` network cannot publish ports anyway. Only the worker (Yahoo fetches) and the proxy join the `egress` network.
- **Container hardening:** containers run as uid 10001 with a read-only root filesystem, all capabilities dropped (the proxy keeps only `NET_BIND_SERVICE`, which its binary requires) and no Docker socket.
