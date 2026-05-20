# Partition Management

This runbook covers operational procedures for managing `pg_tide` outbox table
partitioning.  The partitioning feature was introduced in v0.20.0 (ADR-006) and
the shared-table semantics in v0.22.0 (ADR-007).

---

## Background

By default every outbox writes into the single shared table
`tide.tide_outbox_messages`, discriminated by the `stream_table` column.
For high-volume streams you can convert an outbox to a **dedicated partitioned
table** (`tide.tide_outbox_messages_<name>`) with time-based partitions (daily,
weekly, or monthly).

Partitioning benefits:
- Partition pruning for retention sweeps (`tide.outbox_sweep()`)
- Smaller index footprints per partition
- Parallel vacuum across partitions
- Easy per-partition `DETACH PARTITION … DROP` for disk-pressure relief

Relevant design docs:
- [ADR-006 — Outbox Table Partitioning](../adr/adr-006-outbox-table-partitioning.md)
- [ADR-007 — Shared Partition Table Semantics](../adr/adr-007-shared-partition-table-semantics.md)

---

## 1. Selecting a Partition Strategy

| Strategy  | Partition size  | Recommended when…                          |
|-----------|-----------------|---------------------------------------------|
| `daily`   | 1 day           | >5 M messages/day, retention ≤ 7 days       |
| `weekly`  | 7 days          | 500 K–5 M messages/day, retention 7–30 days |
| `monthly` | ~30 days        | < 500 K messages/day, retention > 30 days  |

Use the query below to estimate current message volume before deciding:

```sql
SELECT
    stream_table,
    COUNT(*)                                          AS total_messages,
    COUNT(*) FILTER (WHERE created_at > now() - INTERVAL '1 day') AS last_24h
FROM tide.tide_outbox_messages
GROUP BY stream_table
ORDER BY total_messages DESC;
```

---

## 2. Converting an Outbox to Partitioned

```sql
SELECT tide.outbox_convert_to_partitioned(
    p_outbox_name         := 'orders',
    p_partition_strategy  := 'daily',       -- 'daily' | 'weekly' | 'monthly'
    p_confirm_shared_table_migration := TRUE  -- required when other outboxes
                                              -- still use the shared table
);
```

### Prerequisites

The function enforces these guards (will raise if violated):

1. **Outbox name length** — the derived backup table name must fit within
   PostgreSQL's 63-byte `NAMEDATALEN` limit.
2. **Shared table migration confirmation** — if other active outboxes still
   write to the unpartitioned shared table, you must pass
   `p_confirm_shared_table_migration := TRUE` to acknowledge the table rename
   window.
3. **Advisory lock** — the function acquires an advisory lock for the duration
   of the migration.  Concurrent `outbox_publish()` calls will block briefly
   until the lock is released.

### What the function does

1. Renames `tide.tide_outbox_messages` to `tide.tide_outbox_messages_backup`.
2. Creates a new partitioned table `tide.tide_outbox_messages_<outbox_name>`
   using `PARTITION BY RANGE (created_at)`.
3. Attaches the backup table as a catchup partition.
4. Creates initial future partitions based on the selected strategy.
5. Updates `tide.tide_outbox_config` with the strategy and new table name.
6. Releases the advisory lock.

### Rollback

If the migration fails part-way, restore from the backup:

```sql
-- Rename backup table back to the shared table name.
ALTER TABLE tide.tide_outbox_messages_backup
    RENAME TO tide_outbox_messages;

-- Reset the outbox config.
UPDATE tide.tide_outbox_config
SET   partition_strategy = NULL,
      stream_table       = 'tide.tide_outbox_messages'
WHERE outbox_name = 'orders';
```

---

## 3. Monitoring Partition Health

### Using `pg-tide doctor --partition-check`

```bash
pg-tide doctor \
    --postgres-url "postgres://user:pass@localhost:5432/mydb" \
    --partition-check
```

This checks:
- That at least one future partition exists for each partitioned outbox
- That no partition is more than 2 partition-periods old without a matching
  `DROP` in the audit log
- That `pg_partman` extension (if present) is correctly configured

### Via SQL

```sql
-- List all partitions for the 'orders' outbox.
SELECT
    c.relname          AS partition_name,
    pg_get_expr(c.relpartbound, c.oid) AS partition_bounds,
    pg_size_pretty(pg_total_relation_size(c.oid)) AS size
FROM   pg_inherits i
JOIN   pg_class    p ON p.oid = i.inhparent
JOIN   pg_class    c ON c.oid = i.inhrelid
WHERE  p.relname = 'tide_outbox_messages_orders'
ORDER  BY c.relname;
```

```sql
-- Consumer lag per outbox with partition awareness.
SELECT * FROM tide.consumer_lag ORDER BY lag DESC;
```

---

## 4. Manual Partition Creation

`pg_tide` creates partitions automatically during the conversion and via the
scheduled sweep job.  If a partition is missing (e.g. the sweep job was paused
over a partition boundary), create it manually:

```sql
-- Example: create next week's daily partition for 'orders'.
CREATE TABLE tide.tide_outbox_messages_orders_2025_w22
    PARTITION OF tide.tide_outbox_messages_orders
    FOR VALUES FROM ('2025-05-26') TO ('2025-06-02');
```

```sql
-- Or use the tide helper (v0.21.0+).
SELECT tide.outbox_ensure_partitions(
    p_outbox_name := 'orders',
    p_ahead_periods := 4    -- create 4 future periods
);
```

---

## 5. Emergency Partition Drop for Disk Pressure

> **Warning:** Dropping a partition permanently deletes all messages in its
> time range.  Only do this if the data has already been relayed and you have
> confirmed the consumer offset is past the end of the partition.

```sql
-- Step 1: Verify the consumer has passed the partition boundary.
SELECT * FROM tide.consumer_lag WHERE outbox_name = 'orders';

-- Step 2: Detach the partition (non-destructive — data still exists).
ALTER TABLE tide.tide_outbox_messages_orders
    DETACH PARTITION tide.tide_outbox_messages_orders_2025_01_01;

-- Step 3: Drop the detached table.
DROP TABLE tide.tide_outbox_messages_orders_2025_01_01;
```

---

## 6. Verifying Partition Pruning

Use `EXPLAIN (PARTITIONS)` to confirm the query planner prunes old partitions:

```sql
EXPLAIN (ANALYZE, BUFFERS, PARTITIONS)
SELECT COUNT(*)
FROM   tide.tide_outbox_messages_orders
WHERE  created_at > now() - INTERVAL '1 day';
```

Check the `Partitions selected:` line in the output — it should be significantly
smaller than `Partitions total:`.

You can also confirm partition inheritance via `pg_inherits`:

```sql
SELECT
    p.relname AS parent_table,
    COUNT(*)  AS partition_count
FROM   pg_inherits i
JOIN   pg_class    p ON p.oid = i.inhparent
JOIN   pg_namespace n ON n.oid = p.relnamespace
WHERE  n.nspname = 'tide'
GROUP  BY p.relname
ORDER  BY p.relname;
```

---

## 7. Automated Partition Maintenance

For long-running deployments, delegate partition creation and pruning to
[`pg_partman`](https://github.com/pgpartman/pg_partman):

```sql
-- Configure pg_partman for the 'orders' outbox (daily partitions, 7-day retention).
SELECT partman.create_parent(
    p_parent_table    := 'tide.tide_outbox_messages_orders',
    p_control         := 'created_at',
    p_type            := 'range',
    p_interval        := 'daily',
    p_premake         := 4,
    p_retention       := '7 days',
    p_retention_keep_table := FALSE
);
```

Then run the `pg_partman` maintenance job on a schedule:

```sql
CALL partman.run_maintenance_proc();
```

---

## See Also

- [ADR-006 — Outbox Table Partitioning](../adr/adr-006-outbox-table-partitioning.md)
- [ADR-007 — Shared Partition Table Semantics](../adr/adr-007-shared-partition-table-semantics.md)
- [Maintenance Operations](maintenance.md)
- [Monitoring Cookbook](monitoring-cookbook.md)
