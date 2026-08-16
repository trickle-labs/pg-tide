# Maintenance

pg_tide state is in PostgreSQL. Back up the `tide` schema together with the
application tables whose transactions publish outbox rows.

## Backup and upgrade

```bash
pg_dump --schema=tide --no-owner --no-privileges --format=custom \
  --file=pg_tide_backup.dump "$DATABASE_URL"
pg_restore --schema=tide --no-owner --clean --if-exists \
  --dbname="$DATABASE_URL" pg_tide_backup.dump
```

Before an extension upgrade, take a backup and stop old relay binaries. Apply
the versioned migration, deploy the matching relay, then verify offsets and
delivery. PostgreSQL extension downgrades are not supported; use PITR or the
pre-upgrade backup.

## Bounded outbox cleanup

Use the bounded API rather than an unbounded delete:

```sql
SELECT tide.outbox_sweep(
  p_outbox_name := NULL,
  p_batch_size := 1000,
  p_dry_run := TRUE
);
```

`p_batch_size` accepts 1–10,000. One call handles at most one batch per
selected outbox, locks candidates with `FOR UPDATE SKIP LOCKED`, and returns
participants, blockers, retention cutoff, safe offset, affected rows,
`has_more`, highest deleted ID, duration, and partition action. Dry-run
examines at most `batch_size + 1` candidates; it does not count the entire
table.

The safe offset is the minimum checkpoint across every configured native
pipeline, relay group, consumer group, enabled fan-in member, and overlapping
lease. A disabled pipeline still blocks cleanup. Both retention age and safe
offset are required. `consumed_at` and delivery receipts are not cleanup
authority.

Automation should call `outbox_sweep()`. The deprecated
`outbox_truncate_delivered()` wrapper performs one 1,000-row sweep:

```sql
SELECT tide.outbox_truncate_delivered();
```

For pg_cron, keep each transaction bounded:

```sql
SELECT cron.schedule(
  'pg-tide-outbox-sweep',
  '*/5 * * * *',
  $$SELECT tide.outbox_sweep(NULL, 1000, false)$$
);
```

For Kubernetes, invoke `pg-tide sweep --batch-size 1000 --max-batches 10` from
a CronJob. Use `--dry-run` first. The command reports blockers and progress
per outbox and exits non-zero if any outbox fails; a database error is never
reported as zero rows.

## Rewind and retirement

Delete a pipeline or consumer group only when its replay history is no longer
needed. Disabling pauses delivery and intentionally preserves retention
protection. Rewind requires the normal authorization, confirmation, ownership
drain, and audit checks, and additionally refuses a target below the highest
deleted message ID. The exact retained boundary is allowed.

```sql
SELECT outbox_name, safe_offset, highest_deleted_id, blockers
FROM tide.outbox_retention_status
ORDER BY outbox_name;
```

## Vacuum, WAL, and storage

Bounded deletes limit lock time and WAL bursts and give autovacuum regular
work. Inspect dead tuples, vacuum timestamps, relation sizes, and WAL deltas:

```sql
SELECT relname, n_live_tup, n_dead_tup, last_autovacuum,
       autovacuum_count
FROM pg_stat_user_tables
WHERE schemaname = 'tide';

SELECT wal_bytes, stats_reset FROM pg_stat_wal;
```

Set per-table autovacuum parameters from the measured retention profile:

```sql
ALTER TABLE tide.tide_outbox_messages SET (
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_analyze_scale_factor = 0.01
);
```

Reserve disk for retained rows, WAL, vacuum headroom, and the configured sink
outage. A sink outage accumulates committed rows. Relay backoff/rate limiting
protects the sink but does not reject application transactions, and
`inline_threshold` is not a native pending-row cap.

## Partition maintenance

Inspect `tide.outbox_storage_config` and
`tide.outbox_retention_status` before maintenance. The canonical parent remains
`tide.tide_outbox_messages`; optional children are ID-range partitions. Run
`tide.outbox_maintain_partitions(2, false)` after checking its dry-run output.
Whole-child removal is allowed only when every logical outbox row in the child
passes its own age, participant offset, and active-lease predicates. Otherwise
bounded row cleanup continues.

Heap-to-ID-range conversion is a blocking maintenance-window operation and
requires explicit confirmation. It is not live conversion and must not be run
while relays or publishers are active.

## Health queries

Use the v0.43 status surfaces:

```sql
SELECT * FROM tide.outbox_retention_status;
SELECT * FROM tide.relay_pipeline_lag;
SELECT * FROM tide.outbox_cleanup_state;
```

Alert on stale cleanup, blockers, default-partition rows, storage-layout
mismatch, growing exact lag, dead-tuple ratio, WAL rate, and disk remaining
after the outage reservation.
