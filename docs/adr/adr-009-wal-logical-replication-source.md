# ADR-009: WAL Logical-Replication Source

**Status:** Accepted  
**Date:** 2026-05-20  
**Deciders:** pg-tide maintainers  
**Supersedes:** —  
**Superseded by:** —

---

## Context

pg-tide's primary CDC mechanism is a **polling source**: the relay worker
issues `SELECT id, payload FROM tide."<outbox>" WHERE id > $last_offset`
on a configurable interval (default 1 s). This is simple, reliable, and
correct, but it introduces latency proportional to the poll interval and adds
read load to the PostgreSQL primary proportional to the number of pipelines.

For high-throughput workloads (millions of events per day) or latency-sensitive
pipelines (sub-100 ms end-to-end), a **WAL-based logical-replication source**
eliminates the polling overhead entirely: the relay subscribes to the PostgreSQL
replication stream and receives INSERT events the moment they are committed,
with no polling loop and no repeated read queries.

PostgreSQL's `pgoutput` logical decoding plugin (built-in since PostgreSQL 10)
provides a standard wire protocol for consuming WAL changes. The v0.32.0 spike
validates that `tokio-postgres` can establish a replication connection, create
a temporary slot, and decode `pgoutput` messages within the existing relay
architecture.

---

## Decision

Implement a `PgLogicalSource` behind a `wal-source` Cargo feature flag as a
**v0.32.0 feasibility spike**. The spike must:

1. Establish a replication connection using `replication=database` in the
   connection URL.
2. Create a **temporary** logical replication slot using the `pgoutput` output
   plugin (`CREATE_REPLICATION_SLOT … TEMPORARY LOGICAL pgoutput`).
3. Receive and decode `pgoutput` messages for INSERT events on a configured
   table and emit `RelayMessage` values equivalent to those produced by
   `OutboxPollerSource`.
4. Define the LSN-to-consumer-offset mapping strategy.
5. Document findings and open design questions in this ADR.

The polling source (`OutboxPollerSource`) **remains the default** for v1.0.0.
The WAL source is additive and opt-in; it will become the recommended path for
high-throughput deployments in v1.1.0.

---

## Design

### Replication Connection

PostgreSQL requires a separate replication connection (distinct from regular
OLTP connections). The connection URL must include `replication=database`.
The relay opens one replication connection per `wal-source` pipeline.

```
postgresql://user:pass@host:5432/dbname?replication=database
```

This connection is **not** managed by the `deadpool-postgres` pool (pool
connections cannot be used for replication). Each `PgLogicalSource` holds its
own dedicated `tokio_postgres::Client` for the replication stream.

### Replication Slot Lifecycle

| Mode | v0.32.0 spike | v1.1.0 target |
|------|--------------|---------------|
| **Ephemeral** (TEMPORARY) | ✅ Default | Opt-in (`slot_mode = "ephemeral"`) |
| **Permanent** | Not implemented | Default (`slot_mode = "permanent"`) |

**Ephemeral slots** (`CREATE_REPLICATION_SLOT … TEMPORARY`) are automatically
dropped when the connection closes. They are safe for development and testing
but do **not** guarantee at-least-once delivery across relay restarts (the slot
disappears on disconnect, and WAL changes after the last confirmed LSN may be
missed if the relay was down when they were committed).

**Permanent slots** (v1.1.0) persist across relay restarts. The relay must
explicitly manage slot lifecycle (`DROP_REPLICATION_SLOT`) to prevent WAL
accumulation on the PostgreSQL primary.

### LSN-to-Consumer-Offset Mapping

The polling source uses the outbox row `id` column as a monotonic offset.
The WAL source uses the PostgreSQL LSN (Log Sequence Number) as the offset.
LSNs are 64-bit integers formatted as `HI/LO` hexadecimal pairs.

**v0.32.0 spike**: Uses the outbox row `id` as an LSN surrogate. The relay
queries `tide."<table>" WHERE id > $last_seen_id` — semantically identical to
the polling source — to validate the concept without implementing the full
`pgoutput` protocol stack.

**v1.1.0 target**: Map LSN directly to committed offset using a new
`tide.wal_source_offsets(pipeline_name TEXT, slot_name TEXT, lsn TEXT)` table.
The relay sends standby status updates (feedback messages) to the PostgreSQL
primary after each confirmed batch, advancing the slot's `confirmed_flush_lsn`
and allowing WAL to be reclaimed.

### Delivery Guarantee

The WAL source inherits pg-tide's **at-least-once** delivery guarantee:

- On successful sink delivery and `acknowledge()`, the relay advances the
  confirmed LSN (standby status update) so the slot can release WAL.
- On relay crash before acknowledgement, the slot's `confirmed_flush_lsn`
  is not advanced. On reconnect, WAL is replayed from the last confirmed
  position. Messages that were delivered but not acknowledged will be
  re-delivered; the inbox's `ON CONFLICT (event_id) DO NOTHING` deduplication
  provides the safety net.
- The `dedup_key` from the outbox row maps directly to `event_id` in the inbox,
  giving the same deduplication semantics as the polling source.

### Interaction with Outbox Partitioning

When outbox table partitioning (ADR-006) is enabled, the replication slot must
be created on the **parent** partitioned table (i.e., `tide_outbox_messages`),
not on individual partitions. PostgreSQL logical replication correctly handles
row changes on partitioned tables as of PostgreSQL 14 with
`publish_via_partition_root = true` in the publication definition.

The `pg-tide` relay must create the publication with this flag set:
```sql
CREATE PUBLICATION pg_tide_pub
  FOR TABLE tide_outbox_messages
  WITH (publish_via_partition_root = true);
```

This is documented in the operations guide for `wal-source` deployments.

### Advisory Lock Coordination

The relay's existing advisory lock coordination (ADR-002) applies unchanged.
Each `wal-source` pipeline competes for advisory lock ownership under the same
`(relay_group_id, pipeline_name)` namespace as polling pipelines. Only one
relay instance can own each pipeline at a time, preventing two instances from
consuming the same replication slot simultaneously (which would cause errors
from PostgreSQL since a slot can only be consumed by one connection).

### Supported Use Cases vs. Polling Source

| Aspect | Polling Source | WAL Source |
|--------|---------------|------------|
| **Latency** | ~poll interval (1 s default) | Sub-millisecond (commit-time) |
| **PostgreSQL load** | N × read queries/s | 1 replication stream per slot |
| **Slot management** | None required | Slot must be monitored for WAL accumulation |
| **Partition support** | Works on partitioned tables | Requires `publish_via_partition_root` |
| **Default for v1.0** | ✅ Yes | No |
| **Recommended for high-throughput** | No | ✅ v1.1.0+ |
| **pg_logical required** | No | Yes (`wal_level = logical`) |

### `wal_level = logical` Requirement

The PostgreSQL server must be configured with `wal_level = logical` (instead of
the default `replica`). This requires a PostgreSQL restart. Cloud-managed
PostgreSQL services (AWS RDS, Cloud SQL, Azure Database for PostgreSQL) support
`wal_level = logical` but may require a parameter group change and instance
reboot. The pre-flight `pg-tide doctor` check will verify `wal_level` when a
`wal-source` pipeline is configured.

---

## Alternatives Considered

### 1. `pg_recvlogical` shell-out
Use `pg_recvlogical` as a subprocess. **Rejected**: adds a PostgreSQL client
binary dependency to the relay Docker image, and inter-process communication
adds latency and complexity.

### 2. `pg_logical_emit_message()` + polling
Emit WAL messages via `pg_logical_emit_message()` from outbox triggers, poll
with `pg_logical_slot_get_changes()`. **Rejected**: still polling-based; does
not improve latency. Also requires a permanent slot even for monitoring.

### 3. `LISTEN/NOTIFY` only
Extend the existing `LISTEN tide_outbox_new` approach to carry full payload.
**Rejected**: PostgreSQL `NOTIFY` payloads are limited to 8 KB; large payloads
cannot be transported. Also, `NOTIFY` channels do not guarantee delivery across
disconnects (missed during connection downtime).

---

## Consequences

### Positive
- Eliminates polling overhead for high-throughput outboxes.
- Enables sub-millisecond end-to-end latency for latency-sensitive pipelines.
- Reduces read load on the PostgreSQL primary by replacing N polling queries
  with one replication stream per slot.
- Reuses the existing `RelayMessage` and `AckToken` types unchanged.

### Negative
- Requires `wal_level = logical` — a configuration change and restart for
  existing deployments.
- Adds a new connection type (replication connections) that bypasses the
  `deadpool-postgres` pool, requiring separate monitoring and lifecycle
  management.
- Permanent slot leaks are a serious operational hazard (WAL accumulation can
  fill disk on the primary). The relay must implement robust slot health checks
  in `pg-tide doctor` before the full v1.1.0 implementation.
- The `pgoutput` protocol is significantly more complex than the polling SELECT:
  it requires tracking `Relation` messages, handling `BEGIN`/`COMMIT` boundaries,
  and sending standby status updates. The v0.32.0 spike defers this complexity
  to v1.1.0.

### Open Questions for v1.1.0
1. Should permanent slot creation be automated by the relay, or require operator
   pre-creation (with `pg-tide doctor` validating the slot exists)?
2. What is the appropriate WAL accumulation warning threshold for `pg-tide doctor`?
   (Proposed: warn when slot `pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) > 1 GB`.)
3. How should the relay handle `ALTER TABLE` changes (schema evolution) detected
   via `Relation` messages with new column counts? (Proposed: route to the
   schema evolution handler already used by the polling source.)
4. Should fan-in pipelines (ADR introduced in v0.29.0) be supported with WAL
   sources? (Initially: one WAL source per pipeline only.)
