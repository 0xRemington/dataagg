# Backup, restore drill, and disaster recovery

## Nightly backup

```text
docker compose -f deploy/compose.yaml --profile ops run --rm backup
```

`marketagg ops backup --verify` takes the following steps:

1. Take a custom-format `pg_dump`.
2. Copy every object the catalog references (active/verified archive generations and retained payload bodies).
3. Encrypt each file with AES-256-GCM in 1 MiB authenticated chunks.
4. Write an HMAC-authenticated manifest with plaintext and ciphertext checksums.
5. Verify the whole set at the destination: decrypt, check checksums, and check that `pg_restore --list` can read the dump.

A backup that exists but has not been verified is not a backup.

- Copy `/backups` to an independent account or bucket. Scope the backup credentials to the backup prefix. Keep 7 daily, 4 weekly and 3 monthly generations.
- Keep the backup key **off the VPS** as well. It cannot be recovered from the backups themselves.
- RPO objective is 24 h, RTO objective is 4 h. Nightly logical backups cannot deliver a 5-minute RPO. A ≤15 min RPO needs physical base backups plus WAL archiving, and that must be verified before it is claimed.

## Monthly restore drill (and after every schema/backup change)

```text
marketagg ops restore --profile production --backup /backups/<folder> \
  --admin-url-file secrets/database_url_owner \
  --target-database marketagg_restore_YYYYMMDD --target-data-root /restore/YYYYMMDD
```

The restore command:

- refuses to restore over the source database or into non-empty storage roots;
- creates a new database;
- restores schema and facts;
- decrypts objects into the new root;
- validates every catalog reference and checksum;
- reports the RPO (latest recoverable knowledge time) and RTO.

After the restore, point a read-only API at the restored database. Re-run a pinned export and compare its semantic hash with the original manifest. `tests/acceptance/test_restore.py` automates this against disposable databases.

## Real outage

1. Disable new live claims and preserve the damaged state.
2. Restore into a replacement location and verify it.
3. Switch the service endpoint.
4. Mark lost intervals as gaps. Expired option chains cannot be reconstructed: resume future collection and supported underlying repair, and never backdate new observations.
