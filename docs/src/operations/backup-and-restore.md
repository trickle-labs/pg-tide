# Backup and Restore

PostgreSQL's native backup tooling works seamlessly with pg_tide because the
entire outbox, inbox, and relay catalog is stored as ordinary tables in the
`tide` schema. There is nothing special to configure — your existing backup
strategy already covers pg_tide.

## What needs to be backed up

| Object | Table / Schema | Backed up by |
|--------|----------------|--------------|
| Outbox configurations | `tide.tide_outbox_config` | `pg_dump` / CNPG backup |
| Outbox messages | `tide.tide_outbox_messages` | `pg_dump` / CNPG backup |
| Consumer groups | `tide.tide_consumer_groups` | `pg_dump` / CNPG backup |
| Consumer offsets | `tide.tide_consumer_offsets` | `pg_dump` / CNPG backup |
| Inbox configurations | `tide.tide_inbox_config` | `pg_dump` / CNPG backup |
| Inbox message tables | `tide.<name>_inbox` | `pg_dump` / CNPG backup |
| Relay pipeline configs | `tide.relay_outbox_config`, `tide.relay_inbox_config` | `pg_dump` / CNPG backup |

## Logical backup with pg_dump

```bash
# Back up only the tide schema (fast, portable):
pg_dump \
  --schema=tide \
  --no-owner \
  --no-privileges \
  --format=custom \
  --file=pg_tide_backup.dump \
  "$DATABASE_URL"

# Restore:
pg_restore \
  --schema=tide \
  --no-owner \
  --clean \
  --if-exists \
  --dbname="$DATABASE_URL" \
  pg_tide_backup.dump
```

## Physical backup (recommended for production)

Physical backups via `pg_basebackup` or a CNPG `Backup` resource capture the
entire cluster, including the `tide` schema, WAL segments, and all extension
files. This is the preferred approach because:

- Point-in-time recovery (PITR) is available — restore to any moment between two backups.
- Outbox messages and consumer offsets are consistent with the application tables
  they reference.
- No extra configuration is needed.

### CloudNativePG

```yaml
apiVersion: postgresql.cnpg.io/v1
kind: ScheduledBackup
metadata:
  name: pg-tide-daily-backup
spec:
  schedule: "0 2 * * *"    # daily at 02:00 UTC
  backupOwnerReference: self
  cluster:
    name: pg-tide-cluster
```

## Point-in-time recovery

Because the relay tracks progress via committed offsets in
`tide.tide_consumer_offsets`, restoring to a point in time is safe:

1. Stop the relay before beginning the restore.
2. Restore the database to the target point in time.
3. After the restore, check `SELECT * FROM tide.consumer_lag` — any messages
   whose `committed_offset` is now in the future (relative to the restored
   outbox) will simply be re-processed.
4. The idempotent inbox prevents duplicates from these re-deliveries.
5. Restart the relay.

## What you do NOT need to back up

The relay binary itself holds no persistent state. All configuration, offsets,
and messages live in PostgreSQL. A new relay instance pointing at a restored
database will pick up exactly where the previous one left off.

## Retention and storage sizing

Outbox messages are retained for `retention_hours` (default: 24 hours). Run
`tide.outbox_truncate_delivered()` regularly to keep storage usage flat:

```sql
-- Clean all outboxes in one query:
SELECT tide.outbox_truncate_delivered();

-- Or via pg_cron (requires pg_cron extension):
SELECT cron.schedule(
  'cleanup-outbox',
  '0 * * * *',                              -- every hour
  'SELECT tide.outbox_truncate_delivered()'
);
```

Inbox message tables accumulate rows until `processed_retention_hours` expires.
Use `tide.inbox_truncate_processed('my-inbox')` or set `processed_retention_hours`
to a lower value for high-throughput inboxes.
