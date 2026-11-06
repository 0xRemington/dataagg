# 06 — Deployment, operations, capacity, and security

## Deployment contract

Target one supported Linux VPS with Docker Engine/Compose, 4 vCPU, 8 GiB RAM and 160–200 GiB SSD as a planning envelope. A 2 vCPU/4 GiB variant is acceptable only after constrained load tests, smaller pools and monitoring disabled. Pin actual image digests and Python dependencies during implementation. No cloud account or deployment is authorized/created by this planning handoff.

| Service | Responsibility | Initial memory budget / connection policy |
|---|---|---|
| db | PostgreSQL 18 durable facts/jobs/catalog | 3 GiB limit; tune shared buffers around 1 GiB, total max connections 40 |
| migrate | One-shot schema upgrade before app start | 256 MiB; migration owner only |
| scheduler | One planner and advisory-lock holder | 256 MiB; dedicated lock connection + pool <=2 |
| worker | Collection, aggregation, bounded export/archive tasks | 1.5 GiB; pool <=5; one heavy job at a time |
| api | Private research HTTP API, two process workers initially | 512 MiB total; pool <=3 per process |
| ui | Private Streamlit/Plotly UI | 512 MiB; no DB connections |
| monitoring (optional profile) | Metrics + host/database exporters | <=512 MiB combined; otherwise queryable tables + health command |

Reserve host RAM/headroom; don't allocate the entire 8 GiB to container limits. Advisory-lock connections cannot be transaction-pooled. Add statement timeouts (15s UI, bounded larger exports), connection timeout, worker RSS monitoring, and log rotation. Query/compute jobs yield to ingestion. No unrestricted arbitrary SQL endpoint.

`deploy/compose.yaml` must have a private DB network, no host DB port, persistent volumes for DB/payload/archive/spool, loopback API/UI bindings, health checks, restart policies, and stop grace long enough to finish or abort fenced transactions. Migration waits for DB health; app services wait for migration success. Compose startup alone does not imply readiness; use dependency conditions as documented by [Docker](https://docs.docker.com/compose/how-tos/startup-order/).

API/UI are accessed through operator VPN or SSH tunnel. Example loopback ports: API 8000, UI 8501. An exposed public hostname needs authenticated TLS proxy and a separate deployment decision. UI server holds a read token; export mutation uses a separate scope. App containers run non-root, no Docker socket, dropped capabilities, read-only root filesystem where compatible and explicit writable volumes. Host updates and disk encryption depend on selected VPS capabilities.

## Configuration and secrets contract

T01/T17 create `.env.example` with no live credentials and typed validation:

```dotenv
MARKETAGG_ENV=development
PROVIDER=fake
LIVE_DATA_ENABLED=false
SOURCE_REVIEW_ID=
DATABASE_URL_FILE=/run/secrets/database_url
API_READ_TOKEN_FILE=/run/secrets/api_read_token
API_EXPORT_TOKEN_FILE=/run/secrets/api_export_token
UNIVERSE_CONFIG=config/universe.yaml
POLICY_CONFIG=config/policies.yaml
REQUESTS_PER_MINUTE=10
PLANNED_UTILIZATION_LIMIT=0.70
PROVIDER_CONCURRENCY=1
HOT_RETENTION_DAYS=90
RAW_RETENTION_DAYS=30
MAX_LOCAL_DATA_GIB=100
MAX_EXPORT_ROWS=5000000
BACKUP_DESTINATION=
BACKUP_KEY_FILE=/run/secrets/backup_key
```

Values are proposal defaults. Live enablement requires a reviewed provider policy and credentials/rights where applicable; a boolean alone is insufficient. Use mounted secret files with restricted host permissions; never commit `.env` or secret material, emit them in logs, or put secrets in command-line arguments. Separate owner/migration, ingest, reader, export-job, and backup DB roles. Runtime writers cannot alter schemas/drop partitions or mutate immutable facts directly. A maintenance role alone may prune verified archives.

Versioned YAML contains symbol allowlist, per-symbol calendar/source IDs, expiry limits, sampling rules, basis policies, age limits, retry/deadline limits, retention, and archive paths. Startup prints a redacted config hash. Config changes create a new policy version; running/exported datasets retain the previous hash. Reject contradictory settings such as live enabled with fake-only capabilities or a public bind without configured auth.

## Deployment and upgrade runbook (future commands)

1. Provision private host access, storage, secret files and backup destination; record exact region/spec/quote. Verify free disk and time synchronization. Install supported Docker/Compose; no application secrets in shell history.
2. Pull/build pinned images, run `docker compose -f deploy/compose.yaml config --quiet`, and `marketagg config check --profile production`. Use a dry-run schedule/budget report before enabling live fetches.
3. Start DB, run one-shot migration, verify expected partitions/current schema. Then start API/UI/scheduler/worker with provider fake and run smoke acceptance.
4. Establish off-host backup and restore drill. Enable reviewed live source and the small universe, then monitor one complete session before unattended operation.
5. For upgrades, record app digest/schema, back up, pause new claims and drain/expire leases, apply additive migration, deploy compatible app, run read/write smoke tests, then resume. Rollback application only if schema backward compatible. Never solve rollback by destructive downgrade of observations; restore to a separate instance if necessary.

Partition maintenance provisions current month plus two future months, and all months needed by scheduled historical jobs. Partition names are derived from validated date values, never raw user SQL. No default catch-all partition hides provisioning failure. A missing partition is an actionable readiness failure for the affected write scope. Archive workers and backup workers coordinate locks so pruning cannot race a backup/catalog snapshot.

## Capacity calculation

Use measured `pg_total_relation_size`, payload byte counters, archive bytes and WAL generation. Planning assumptions below are **estimates, not benchmark results or vendor quotes**.

```text
rows/session = sum(expiry_contract_count * successful_polls) + broad_extra_rows
hot_bytes = rows/session * sessions_in_hot_window * measured_bytes_per_row_with_indexes
archive_bytes = archived_rows * measured_parquet_bytes_per_row
disk_required = hot facts + metadata/indexes + raw/spool + archive staging
                + WAL reserve + local backup + logs/OS + free headroom
```

Example from 03: 8 priority expiries x 200 total call+put contracts x 78 polls = 124,800 rows/session. Another 12 expiries x 200 x 7 polls = 16,800. Base total = **141,600**; reserve <=4,000 rows for a separate broad sweep (conservative double-count allowance) -> **145,600/session**. Actual contract counts can differ greatly. At 900 bytes/row **including indexes** as a provisional measurement placeholder, 63 trading sessions (~90 calendar days) consume 8.26 GB decimal (~7.69 GiB); a 252-session year is 36.69 million rows and 33.0 GB decimal if all kept hot. A 2,000-byte measured row more than doubles this estimate. Bar revisions, metadata, payloads and WAL are additional.

Canonical 1m base rows for two symbols are 780/session, ~196,560/year before revisions; derived 5m+15m+4h add 212/session. Daily adds two/session. Options dominate this sample, but payload blobs/index/WAL can dominate actual disk. Repeated unchanged minute fetches should not create bar rows; they still produce run/payload accounting. Don't multiply an all-in row estimate by an index factor a second time.

Illustrative 160 GiB disk allocation: hot facts/indexes 25 GiB, metadata/other series 5, raw/spool 15, archive staging/cache 10, WAL reserve 15, local backup staging 20, OS/images/logs 15, free headroom 55. These are caps/budgets, not expected utilization. Hard disk budgets cannot be guaranteed simply by capping row counts; monitor filesystem usage and projected growth. Archive compressed size assumption 100–300 bytes/row must be measured; keep long-term cold history off-host and cache it locally within the budget.

Planning monthly infrastructure allowance: VPS USD 20–40, offsite storage/backup USD 5–15, monitoring/domain/egress USD 0–10; total USD 25–65 before taxes, paid data, and unusual transfer. These are budgeting assumptions, **not sourced current offers**. Aim near USD 50 subject to an actual quote. Data subscription and exchange/professional/non-display fees require vendor-specific review and may dominate the total; do not assume zero because initial software is free.

## Service objectives and alert thresholds

These are operator objectives for the five-session pilot, not upstream SLAs.

| Metric | Initial objective / action |
|---|---|
| Scheduled priority chain coverage | >=95% of intended expiry slots completed within 120s, measured per scope; all failures counted |
| Underlying minute coverage | >=99% of scheduled RTH minutes by next-session sweep, excluding only verified calendar closures; halts shown separately |
| Source-to-publication processing | p95 <=5s after fetch completion for ordinary chain/minute batches |
| API latency | p95 <2s for one-symbol one-month 15m + one-contract history on 90-day pilot-equivalent synthetic load |
| Export | <=60s for 100,000 rows under pilot envelope; larger jobs async/bounded |
| Scheduler lag | Warning >60s for 3 cycles; critical >5m during open session |
| Priority poll age | Warning >2 expected cadences, critical >3; per symbol/expiry |
| Null/schema rates | Any required field/type drift critical; bid/ask-null proportion +20 percentage points vs trailing five-session baseline warning |
| Disk | Warn at 70%, stop bulk backfill/exports at 80%, suspend collectors at 90% after durable incident log; no unverified deletion |
| Clock skew | >2s stops publication and alerts |
| Backup | Warning if last verified offsite copy >26h; critical >48h |
| PostgreSQL | Alert failed checkpoints, checksum errors, stalled archiving if enabled, WAL growth and connection exhaustion |

Distinguish quote-event age from fetch age in metrics. Unknown event time reports an unknown ratio, not zero seconds. Use bounded metric labels (provider/job kind/scope tier), keeping contract IDs and arbitrary exception strings out of Prometheus labels; detailed scope diagnostics belong in DB tables/logs. Logs include run/job/batch correlation, counts, timing, error class and config hash, never credentials or full restricted payloads. Operator notifications remain disabled until an authorized destination is configured; in-app health and logs work immediately.

## Archives, retention, and backup

Normalized observations: 90 days hot initially, older immutable monthly partitions archived after a seven-day correction grace and successful independent verification. Payload/result retention: 30 days, longer for incidents (90 days) if permitted. Keep payload hashes/metadata after body expiry, with explicit missing-body reason. Source data rights may require shorter retention. Expire exports after seven days unless pinned; keep manifests and required input generations for pinned reproducibility. Logs: 14 days local; daily operational summaries 180 days.

Parquet layout: `dataset=<name>/schema=<n>/source=<id>/year=YYYY/month=MM/[instrument=<id>/]generation=<n>/part-*.parquet`. Aim for 64–256 MiB files after measuring; don't create one file per contract. Embed version/type metadata; preserve immutable row IDs, original observation/knowledge times, and all references necessary for queries. Write temporary object -> verify content/count/type/hash -> offsite copy/check -> atomic active catalog switch. Pin a manifest generation for each query/export. Only then, after a seven-day deletion grace and restore proof, may maintenance detach/drop verified hot partitions. No filesystem glob deletion of unknown objects.

Use nightly custom-format logical DB backup plus role/config metadata and a catalog-consistent object manifest. Copy/encrypt to an independent account or bucket; backup credentials scoped to required prefix. Pin referenced object generations during backup; copy all referenced payload/archive objects before declaring the backup complete. A DB dump alone does not contain external files. [PostgreSQL logical backups](https://www.postgresql.org/docs/18/backup-dump.html) support consistent DB snapshots; application-level object consistency remains our responsibility.

Default RPO objective **24h**, RTO **4h**, measured at pilot scale. Nightly backup cannot promise five-minute RPO. Keep 7 daily, 4 weekly and 3 monthly backup generations off-host, subject to rights/budget; ensure key recovery independent of the VPS. If losing a trading day of unique option snapshots is unacceptable, add physical base backups plus continuous WAL archiving and verify a <=15m RPO before claiming it. Logical dumps cannot be replayed with WAL as a substitute for a physical base backup; see [PostgreSQL PITR documentation](https://www.postgresql.org/docs/18/continuous-archiving.html). External object recovery must meet the same RPO.

Monthly restore drill and every schema/backup change: provision separate DB, recover keys/config/roles, restore schema/facts/catalog, fetch objects, validate hashes and logical refs, execute old asof/panel/export checks, measure readiness and recovered cutoff, then record evidence. Real outage: disable new live claims, preserve damaged state, restore into a replacement location, verify, switch service endpoint, and mark lost intervals. Expired options history cannot be reconstructed from today's chain. Resume future collection plus supported underlying repair; never backdate new observations.

## Security and data-use checklist

- [ ] Source policy records jurisdiction/account, terms version/date, automated collection permission, local retention, backup rights, private exports, display/non-display use and redistribution restrictions. Yahoo terms retrieval remains unresolved in planning; obtain applicable terms before enabling live use.
- [ ] Package license is reviewed separately from market-data rights; synthetic fixtures are default public/test artifacts.
- [ ] OPRA/vendor entitlements and professional/nonprofessional status are checked for a licensed feed; agreements may govern storage, derived metrics and historical replay as well as live display. No presumed redistribution right.
- [ ] Private VPN/loopback access, no DB public port, API scopes, TLS when traffic leaves local/private transport, secret rotation and dependency/image scanning are verified.
- [ ] Least-privilege DB roles, owner-only migrations, parameterized SQL, path allowlists, authorized downloads, bounded requests and CSV formula protection are tested.
- [ ] Backup encryption keys recoverable separately, offsite restores tested, access logs redacted, raw retention rights enforced; no credentials inside archived payloads.
- [ ] Feed auth failures stop retries; no bypass of provider access restrictions. An opt-in checkbox is not legal permission.

These checks implement the brief's private research and source restrictions; they are not a declaration that a particular use is permitted.

## Growth triggers

Expand universe only after one measured week with >=30 days projected free headroom and <=70% planned request-budget use. Increase disk/retention tier when growth forecasts cross that headroom; reduce scope while capacity is unresolved. Investigate TimescaleDB only if partition/index/query tuning still misses API/retention objectives with measured evidence. Add Redis only if a licensed stream requires shared latest-state reads that PostgreSQL/in-process bounded cache cannot meet; it is never the history of record. Move heavy analytics to dedicated compute when ingest deadlines or memory suffer despite job isolation. Adopt an entitled feed when event freshness, broader coverage or precise intraday research is a requirement; faster Yahoo polling cannot establish those capabilities.
