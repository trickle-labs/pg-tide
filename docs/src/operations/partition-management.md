# Partition management

v0.43 keeps one public parent:
`tide.tide_outbox_messages`. Optional children use numeric
`PARTITION BY RANGE (id)` bounds shared by every logical outbox. The old
per-outbox `created_at` layout, `stream_table` procedures, and `pg_partman`
examples are historical and are not supported.

## Inspect storage

```sql
SELECT layout, partition_span, premake, last_maintenance_at
FROM tide.outbox_storage_config;

SELECT c.relname AS child, pg_get_expr(c.relpartbound, c.oid) AS bounds,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS total_bytes
FROM pg_inherits i
JOIN pg_class p ON p.oid = i.inhparent
JOIN pg_class c ON c.oid = i.inhrelid
JOIN pg_namespace n ON n.oid = p.relnamespace
WHERE n.nspname = 'tide' AND p.relname = 'tide_outbox_messages'
ORDER BY c.relname;
```

`heap` is the default. `id_range` records the actual ID-range parent.
`legacy_noncanonical` is a fail-closed diagnostic state for old
`created_at`/dedicated-table layouts; do not reinterpret it automatically.

## Preflight and conversion

Conversion applies to the complete canonical parent, not one outbox:

```sql
SELECT tide.admin_convert_outbox_storage(
  p_partition_span := 10000000,
  p_premake := 2,
  confirm_blocking_copy := TRUE
);
```

Both maintenance functions require a superuser or membership in
`pg_tide_admin`; they are not executable by `PUBLIC`.

Run during a maintenance window with publishing and relay workers drained.
Preflight must report temporary disk requirements and refuse without explicit
confirmation. The operation verifies row count, ID range, and checksum samples,
then swaps atomically in one transaction. An error or outer `ROLLBACK` restores
the original heap parent. Zero-downtime conversion is not promised.

The deprecated `outbox_convert_to_partitioned(name, strategy, ...)` refuses
new conversions and points to the global operation.

## Provision and recover

Scheduled maintenance uses:

```sql
SELECT tide.outbox_maintain_partitions(
  p_ahead := 2,
  p_dry_run := TRUE
);
```

The helper reads span and premake from `outbox_storage_config`, creates missing
future ranges and indexes idempotently, and drains the default partition in
bounded batches. It records created/drained/dropped actions in
`tide.tide_partition_events`. A non-empty default partition is an alert, not a
reason to create overlapping bounds. Empty historical children are dropped
only after the maintenance fence proves they contain no rows. Rerun the helper
after a stopped maintenance job.

## Safe removal

A whole child may be detached and dropped only when every row in it is older
than that row's logical outbox retention cutoff, at or below that outbox's safe
participant offset, and outside active leases. Hold all affected outbox fences
in sorted order while proving the predicate. If one outbox is slower, do
bounded row cleanup for eligible sibling rows and leave the child attached.

```sql
SELECT *
FROM tide.outbox_retention_status
WHERE storage_layout = 'id_range'
ORDER BY outbox_name;
```

Never manually drop a child based only on `consumed_at` or a global ID gap.

## Query-plan verification

The public polling query is unchanged:

```sql
EXPLAIN (FORMAT JSON)
SELECT id, outbox_name, payload, headers, created_at
FROM tide.tide_outbox_messages
WHERE outbox_name = 'orders' AND id > 1000
ORDER BY id
LIMIT 100;
```

The plan should prune children wholly below the checkpoint and use the
`(outbox_name, id)` index on scanned children. Use JSON-plan assertions rather
than exact cost numbers.
