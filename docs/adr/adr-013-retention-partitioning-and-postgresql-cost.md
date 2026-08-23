# ADR-013: Retention, ID-Range Partitioning, and PostgreSQL Cost Contract

**Status:** Accepted
**Date:** 2026-08-16
**Release:** v0.43.0
**Related:** ADR-006, ADR-007, ADR-011, ADR-012

## Context

The native outbox is a shared PostgreSQL relation. A global identity sequence is
not commit ordered, `consumed_at` describes no individual pipeline, and a
single unbounded delete can create avoidable lock, WAL, and vacuum spikes.
Partitioning and capacity claims are useful only when they preserve the public
parent and are backed by reproducible measurements.

## Decisions

### Retention participants and safe cleanup

For an outbox, the participant set is:

1. every native relay pipeline configured for that outbox, including disabled
   pipelines (disabled means paused, not retired);
2. every relay-group offset for those pipelines;
3. every consumer group and its consumers;
4. enabled fan-in members and overlapping active consumer leases.

A configured participant without an offset has safe offset `0`. An orphan
offset whose pipeline was deleted is not a participant. The safe cleanup
offset is the minimum participant checkpoint. With no participants, the
outbox retention window is the remaining contract.

A row is eligible only when both conditions hold:

```text
created_at <= now() - retention interval
AND id <= safe cleanup offset
```

`consumed_at` and delivery receipts are observability/compatibility fields, not
cleanup authority. A disabled pipeline remains a blocker until it is deleted.
Administrative rewind may target the retained boundary, but refuses a target
below the highest deleted message ID.

### Bounded sweep and progress

`tide.outbox_sweep(text default NULL, integer default 1000, boolean default
false) -> jsonb` is the authoritative maintenance API. Batch size is 1–10,000,
one batch is processed per selected outbox, and candidate rows are selected in
deterministic ID order with `FOR UPDATE SKIP LOCKED`. Dry-run examines at most
`batch_size + 1` candidates and reports `has_more`; it never performs an
unbounded count.

The result reports the outbox, retention cutoff, safe offset, participants and
blockers, eligible rows, affected rows, `has_more`, highest deleted ID,
duration, and partition action. A successful delete or partition action updates
`tide.outbox_cleanup_state` in the same transaction. Failures remain errors and
the CLI exits non-zero; they are never converted to zero-row success.

`outbox_truncate_delivered()` remains a deprecated compatibility wrapper over
one 1,000-row sweep. Automation should call `outbox_sweep()`.

### Long-transaction fence

Publishers take a transaction-scoped shared advisory lock in an outbox-specific
namespace before inserting. Native polling, cleanup, partition removal, and
conversion take the matching exclusive lock for short database work only.
Polling copies rows and releases the lock before decoding or sink I/O. Shared
locks preserve concurrent publishers for one outbox; unrelated outboxes use
different keys. Partition operations acquire affected outbox locks in sorted
name order. This guarantees that a lower-ID transaction cannot commit after a
poller has advanced past its invisible row.

### Shared ID-range storage

`tide.tide_outbox_messages` remains the only public parent relation. Optional
partitioning is `PARTITION BY RANGE (id)` with numeric child bounds, a default
partition for recovery, a default span of 10,000,000 IDs, and two future
partitions pre-created. Every child has the canonical `(outbox_name, id)`
polling index and cleanup indexes selected by query plans. The global identity
sequence is shared by all logical outboxes.

`tide.outbox_storage_config` records `heap`, `id_range`, or
`legacy_noncanonical`, the span, premake count, and last maintenance time.
Configuration is read from this catalog, never inferred from child names or
the deprecated `partition_strategy`/`retention_partitions` columns. A default
partition is visible in status and doctor output and is drained in bounded
batches. A whole child is dropped only after every row satisfies its own
outbox's age, safe offset, and lease predicates; otherwise row cleanup
continues without dropping the child.

Conversion is deliberately a blocking maintenance-window operation:
`tide.admin_convert_outbox_storage(bigint default 10000000, integer default 2,
boolean confirm_blocking_copy default false)`. It applies to the complete
canonical parent, requires explicit confirmation and drained relays, checks
disk evidence when available, verifies row count/min/max/checksum samples, and
commits atomically. The old per-outbox conversion function refuses new
conversions and points to this operation. Zero-downtime conversion is not a
v0.43 promise.

### Cost and benchmark contract

Criterion and direct inserts are microbenchmarks only. Operational benchmarks
cross the public SQL API, packaged PostgreSQL 18 extension, real `pg-tide`
process, and NATS JetStream. Soaks hold those profiles long enough to observe
growth and recovery. Every result proves published/acknowledged identities and
the final checkpoint.

The source of each budget is fixed:

| Cost | Measurement source |
|---|---|
| Publish overhead | matched business-only vs. business-plus-`outbox_publish()` transaction histograms |
| Delivery latency | benchmark publish timestamp to acknowledged NATS observation |
| Relay CPU/RSS | Linux child-process `/proc` samples |
| WAL | `pg_stat_wal.wal_bytes` deltas |
| Table/index growth | `pg_relation_size`, `pg_indexes_size`, `pg_total_relation_size` |
| Vacuum/bloat | `pg_stat_user_tables` and `pgstattuple` |
| Catalog/offset queries | normalized `pg_stat_statements` counts |
| Recovery | backlog at sink restoration and time/slope to steady state |
| HA interruption | timestamped owner loss to resumed acknowledged delivery |

`benchmarks/budgets-v1.toml` is the reviewed machine-readable gate.
`benchmarks/operational/baseline-v1.json` records the named reference
environment. Values remain environment-specific and are not universal capacity
claims.

## Consequences

Retention is conservative and a paused pipeline can intentionally hold disk.
Sweep cost and WAL are bounded by the batch limit. ID-range partitions preserve
the canonical polling query and shared-parent semantics, while conversion
requires maintenance capacity and temporary disk. Operators must reserve disk
for the configured sink-outage window and alert on lag, blocked cleanup,
default-partition rows, dead tuples, and growth slopes.
