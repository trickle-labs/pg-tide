# Maintenance

This page covers the ongoing maintenance tasks for a pg_tide deployment: backups, upgrades, retention management, and capacity planning. Because pg_tide stores all state in PostgreSQL, maintenance is straightforward — your existing PostgreSQL operational practices already cover most of what you need.

---

## Backup and Restore

### What needs to be backed up

The good news: **everything pg_tide needs is in PostgreSQL**. There's no external state, no local files, no configuration that lives outside the database. Your existing backup strategy already covers pg_tide.

| Object | Table/Schema | Purpose |
|--------|-------------|---------|
| Outbox configurations | `tide.tide_outbox_config` | Outbox definitions (names, retention, thresholds) |
| Outbox messages | `tide.tide_outbox_messages` | Pending and recently-consumed messages |
| Consumer groups | `tide.tide_consumer_groups` | Group definitions |
| Consumer offsets | `tide.tide_consumer_offsets` | Processing progress (critical for resume) |
| Consumer leases | `tide.tide_consumer_leases` | In-flight batch reservations |
| Inbox configurations | `tide.tide_inbox_config` | Inbox definitions |
| Inbox message tables | `tide."{name}_inbox"` | Per-inbox message tables |
| Relay pipeline configs | `tide.relay_outbox_config`, `tide.relay_inbox_config` | Pipeline definitions |
| Relay offsets | `tide.relay_consumer_offsets` | Relay progress tracking |

### Logical backup with pg_dump

For targeted backups of just the pg_tide state (useful for migration or cloning):

```bash
# Back up only the tide schema
pg_dump \
  --schema=tide \
  --no-owner \
  --no-privileges \
  --format=custom \
  --file=pg_tide_backup.dump \
  "$DATABASE_URL"

# Restore
pg_restore \
  --schema=tide \
  --no-owner \
  --clean \
  --if-exists \
  --dbname="$DATABASE_URL" \
  pg_tide_backup.dump
```

### Physical backup (recommended for production)

Physical backups via `pg_basebackup` or a CloudNativePG `Backup` resource capture the entire cluster. This is the preferred approach because:

- **Point-in-time recovery (PITR)** is available — restore to any moment
- **Consistency** — outbox messages and consumer offsets are consistent with the application tables they reference
- **No extra configuration** — pg_tide is just tables, indexes, and functions

### Point-in-time recovery

Restoring to a previous point in time is safe with pg_tide:

1. **Stop the relay** before beginning the restore
2. **Restore the database** to the target point in time
3. **Check consumer lag:** `SELECT * FROM tide.consumer_lag` — any messages whose offset is now ahead of the restored outbox will be re-delivered on relay startup
4. **The inbox dedup prevents duplicates** — re-delivered messages are caught by the UNIQUE constraint
5. **Restart the relay** — it resumes from the restored committed offset

### What you do NOT need to back up

The relay binary holds no persistent state. All configuration, offsets, and messages live in PostgreSQL. A new relay instance pointing at a restored database picks up exactly where the previous one left off.

---

## Upgrades

### Extension upgrades

pg_tide uses PostgreSQL's built-in extension versioning. Each version transition has a migration script:

```sql
-- Check current version
SELECT extversion FROM pg_extension WHERE extname = 'pg_tide';

-- Upgrade (PostgreSQL runs the migration script automatically)
ALTER EXTENSION pg_tide UPDATE TO '0.2.0';
```

The migration script (`sql/pg_tide--0.1.0--0.2.0.sql`) handles all schema changes. Your data is preserved.

**Rollback:** PostgreSQL does not support extension downgrades via `ALTER EXTENSION`. To roll back, restore from a backup taken before the upgrade.

**Best practice:** Always take a backup immediately before upgrading the extension.

### Relay upgrades

The relay binary is stateless, making upgrades trivial:

**Standalone binary:**

```bash
# Stop the current relay
systemctl stop pg-tide-relay

# Replace the binary
curl -LO https://github.com/trickle-labs/pg-tide/releases/latest/download/pg-tide-x86_64-unknown-linux-gnu.tar.gz
tar xzf pg-tide-*.tar.gz
sudo mv pg-tide /usr/local/bin/

# Restart
systemctl start pg-tide-relay
```

**Docker / Kubernetes:**

```bash
kubectl set image deployment/pg-tide-relay relay=ghcr.io/trickle-labs/pg-tide:0.2.0
```

Rolling updates work seamlessly: new instances wait for advisory locks, old instances release them during graceful shutdown.

### Zero-downtime upgrade procedure

For deployments that cannot tolerate any message delivery gap:

1. Deploy new relay instances alongside old ones (same `relay_group_id`)
2. New instances start and attempt to acquire advisory locks (blocked by old instances)
3. Gracefully stop old instances (`SIGTERM` or pod termination)
4. Old instances drain in-flight messages, commit final offsets, release locks
5. New instances acquire the freed locks within seconds
6. Processing resumes from the last committed offset — no gap, no duplicates

### Compatibility matrix

| pg_tide Extension | Relay Binary | PostgreSQL |
|-------------------|-------------|-----------|
| 0.1.x | 0.1.x | 18+ |

**Rule:** Always upgrade the extension first, then the relay binary. The relay is forward-compatible with same-minor extension versions.

---

## Retention and Cleanup

### Outbox retention

Each outbox has a configurable `retention_hours`. After messages are consumed and the retention period elapses, they're eligible for cleanup:

```sql
-- Create with custom retention
SELECT tide.outbox_create('high-volume', p_retention_hours := 12);

-- Change retention for an existing outbox
UPDATE tide.tide_outbox_config
SET retention_hours = 24
WHERE outbox_name = 'high-volume';
```

Trigger cleanup manually:

```sql
SELECT tide.outbox_truncate_delivered();
```

Or automate with pg_cron:

```sql
-- Clean all outboxes every hour
SELECT cron.schedule(
  'cleanup-outbox',
  '0 * * * *',
  'SELECT tide.outbox_truncate_delivered()'
);
```

### Inbox retention

Inbox tables accumulate processed messages for auditing. The `processed_retention_hours` parameter controls when they're cleaned:

```sql
-- Create inbox with aggressive cleanup (24h retention)
SELECT tide.inbox_create('high-volume-inbox',
  p_processed_retention_hours := 24,
  p_dlq_retention_hours := 168
);

-- Manual cleanup
SELECT tide.inbox_truncate_processed('high-volume-inbox');
```

### Storage sizing

For capacity planning, estimate storage needs:

| Factor | Formula |
|--------|---------|
| **Outbox storage** | `message_rate × avg_message_size × retention_hours × 3600` |
| **Index overhead** | ~30% of table size |
| **Inbox storage** | `inbound_rate × avg_message_size × processed_retention_hours × 3600` |

**Example:** 1,000 messages/second × 1 KB average × 24 hours retention = ~82 GB of outbox data before cleanup. In practice, with regular cleanup, steady-state usage is much lower because consumed messages are cleaned before retention expires.

---

## Monitoring and Health

### Essential monitoring queries

Run these periodically (or expose via a PostgreSQL exporter):

```sql
-- Pending messages (should be low if relay is healthy)
SELECT * FROM tide.outbox_pending;

-- Consumer lag (alert if growing)
SELECT * FROM tide.consumer_lag WHERE lag > 1000;

-- Active relay pipelines
SELECT name, enabled, config->>'outbox' as outbox
FROM tide.relay_outbox_config WHERE enabled;

-- Relay offset freshness (stale = relay might be down)
SELECT pipeline_id, updated_at, now() - updated_at AS age
FROM tide.relay_consumer_offsets
WHERE now() - updated_at > interval '5 minutes';
```

### Prometheus alerting rules

```yaml
groups:
  - name: pg-tide
    rules:
      - alert: PgTideRelayDown
        expr: pg_tide_relay_pipeline_healthy == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "pg-tide relay pipeline {{ $labels.pipeline }} is unhealthy"

      - alert: PgTideHighConsumerLag
        expr: pg_tide_consumer_lag > 10000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Consumer {{ $labels.group_name }} has lag of {{ $value }}"

      - alert: PgTideDeliveryErrors
        expr: rate(pg_tide_relay_publish_errors_total[5m]) > 0
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "pg-tide relay delivery errors on {{ $labels.pipeline }}"
```

---

## Routine Maintenance Tasks

| Task | Frequency | Method |
|------|-----------|--------|
| Check consumer lag | Continuous (Prometheus) | `SELECT * FROM tide.consumer_lag` |
| Outbox cleanup | Hourly (pg_cron) | `SELECT tide.outbox_truncate_delivered()` |
| Inbox cleanup | Daily (pg_cron) | `SELECT tide.inbox_truncate_processed('name')` |
| Relay log review | Daily | Check for recurring errors or warnings |
| Extension upgrades | As released | `ALTER EXTENSION pg_tide UPDATE TO 'x.y.z'` |
| Relay upgrades | As released | Rolling binary replacement |
| Backup verification | Weekly | Restore to a test environment and verify |
| Index bloat check | Monthly | `REINDEX INDEX CONCURRENTLY` if needed |
