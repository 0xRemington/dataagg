# Daily operations

| Check | Command / signal | Action |
|---|---|---|
| Overall status | `marketagg ops status --profile production --json` (exit 1 when critical) | Investigate every `alerts[]` entry |
| Stalled scope | `ingestion_late` (>2 cadences) / `ingestion_stalled` (>3) per symbol and scope | Check circuits, provider status and the worker log |
| Unknown freshness | `quote_freshness.unknown_ratio` | Expected to be 1.0 for Yahoo. It is never treated as fresh |
| Provider disabled | `provider_disabled` alert after 401/403 | Resolve the access/permission issue, then `CircuitBreaker.enable`. Never rotate identities or proxies |
| Rate limited | `provider_open` with `open_until` | Wait. Consider reducing scope. Do not retry aggressively |
| Disk | warn 70%, bulk work stops 80%, collectors suspend 90% | Archive verified months, expand disk, check WAL/log growth. No unverified deletion |
| Backups | warning >26 h, critical >48 h since last verified backup | Run and verify a backup |
| Gaps | `/v1/quality`, `data_gaps` | Recoverable minute gaps are repaired earliest-expiring first. Missed option slots are permanent |

## Archiving a month

```text
marketagg ops archive --profile production --dataset option_snapshots --month 2025-01
```

The command writes the archive, verifies it, copies it to `ARCHIVE_OFFSITE`, checks the copy, and activates it.

Hot partitions may be pruned only:

- after a seven-day grace period;
- after an off-host verified copy exists;
- after a successful restore drill.

Pruning uses `ArchiveManager.prune_hot_partition`, which detaches and drops only the validated monthly partition.

## Notifications

Operator notifications are disabled until an authorised destination is configured. In-app status and logs work immediately. Logs are JSON with redacted secrets.
