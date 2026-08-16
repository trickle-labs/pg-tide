# ADR-006: Outbox Table Partitioning

**Status:** Superseded by [ADR-013](adr-013-retention-partitioning-and-postgresql-cost.md) for v0.43.0
**Date:** 2026-05-19  
**Author:** pg_tide Contributors  

## Context

The single-table outbox design (ADR-001) stores all outbox messages in
`tide.tide_outbox_messages` with an `outbox_name` discriminator column.  For
low-to-medium write rates this is simple and effective.  For high-throughput
outboxes (millions of events per day) two operational problems emerge:

1. **Unbounded table growth** — `outbox_truncate_delivered()` deletes rows but
   PostgreSQL's MVCC means dead tuples accumulate until `VACUUM` reclaims them.
   At high write rates table bloat can grow faster than autovacuum reclaims it,
   causing query performance degradation and storage cost growth.

2. **Retention window scans** — Deleting by `consumed_at < now() - interval`
   on a large table requires a sequential scan or expensive index-only scan even
   with proper indexing.  Partition pruning eliminates this cost by making the
   entire partition detachable and droppable as a DDL operation.

**Options evaluated:**

1. **Status quo (TTL truncation only)** — Keep `outbox_truncate_delivered()`;
   document autovacuum tuning.  Zero schema change risk.
2. **PostgreSQL declarative range partitioning on `created_at`** — Partition
   `tide.tide_outbox_messages` by `created_at` time range (daily/weekly/monthly).
   Partition pruning applies to retention queries and consumer polls.
3. **Hash partitioning on `outbox_name`** — Partition by hashed `outbox_name`
   for write-scalability.  Does not help retention queries.
4. **Per-outbox child tables** — Reverts to the multi-table design rejected in
   ADR-001; breaks the uniform consumer API.

## Decision (historical)

The `created_at` partition-key and per-outbox conversion design below are
retained as historical context only. They are not a supported v0.43.0 layout.
ADR-013 replaces them with optional ID-range partitions under the canonical
shared parent and a single blocking, global conversion operation.

We adopt **Option 2: PostgreSQL declarative range partitioning on `created_at`**
as an opt-in extension to `tide.outbox_create()`.

### Design contract

```sql
-- Opt in at creation time (default: 'none' = current behaviour).
SELECT tide.outbox_create(
    'orders',
    retention_hours => 168,    -- 7 days
    partition_strategy => 'daily'   -- 'none' | 'daily' | 'weekly' | 'monthly'
);
```

### Partition lifecycle

- When `partition_strategy != 'none'`, `outbox_create()` creates the backing
  table as a `PARTITION BY RANGE (created_at)` parent and auto-provisions the
  initial partition covering the current interval plus the next.
- The relay `pg-tide sweep` command (or an optional background task managed by
  the coordinator) creates the next partition before the current one fills,
  preventing insert failures during window transitions.
- `outbox_truncate_delivered()` is extended to **detach and drop** partitions
  whose entire retention window has expired, keeping active partition count
  within a configurable rolling window (`retention_partitions`, default 7 for
  daily strategy).  Dropping a partition is a DDL operation — no heap scan,
  no VACUUM overhead.

### Consumer group and advisory lock compatibility

- `tide.poll_outbox()`, `tide.commit_offset()`, and
  `tide.consumer_lease_acquire()` are unchanged at the SQL API level.
  PostgreSQL's partition-aware planning routes `WHERE id > $last_offset` to the
  appropriate partition.
- Advisory lock key derivation (`pg_try_advisory_lock(group_hash, pipeline_hash)`)
  is computed from the logical outbox name, not the partition name, so lock
  behaviour is unchanged.
- Regression tests must assert correct `WHERE id > $last_offset` query plans
  against a partitioned table to confirm partition pruning is applied.

### Live migration

`tide.outbox_convert_to_partitioned(name, strategy)` migrates an existing
unpartitioned outbox to the new schema using an advisory-lock swap:

1. Create new partitioned parent table and initial partitions.
2. Copy existing rows in batches.
3. Acquire the outbox advisory lock.
4. `RENAME` old table to backup name; `RENAME` new table to canonical name.
5. Release lock.  Relay resumes on the partitioned table with no message loss.

Total relay downtime is bounded by the time to acquire the advisory lock
(at most one relay poll interval, typically ≤ 1 s).

## Rationale for opt-in vs. default

Making partitioning the default would introduce a breaking schema change for
all existing deployments.  Opt-in allows operators to migrate outboxes on their
own schedule, validate behaviour in staging, and skip partitioning for
low-volume outboxes where it adds complexity with no benefit.

The `partition_strategy` parameter is additive — existing `outbox_create()`
callers without the parameter continue to use the unpartitioned table.

## Consequences

- **Positive**: Retention operations (drop old partitions) are O(1) DDL
  operations rather than O(n) heap scans.  Eliminates VACUUM pressure for
  high-throughput outboxes.
- **Positive**: `WHERE created_at BETWEEN $start AND $end` analytical queries
  benefit from partition pruning (e.g. DuckLake batch reads, replay workbench).
- **Positive**: Partition detach is near-instant and does not hold access
  exclusive locks beyond the detach DDL itself.
- **Negative**: `outbox_create()` signature gains a new parameter; Helm chart
  and documentation must be updated.
- **Negative**: Live migration requires relay coordinator downtime of one poll
  interval; must be documented in the schema migration runbook.
- **Neutral**: Hash partitioning on `id` was considered for write-scale but
  rejected because it does not improve retention and complicates the
  `WHERE id > $last_offset` consumer poll pattern.

## Implementation timeline

This ADR is published in v0.24.0 as the design contract.
Implementation is planned for v0.25.0 (see ROADMAP.md).
