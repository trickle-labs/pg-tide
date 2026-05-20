# ADR-007: Shared Partition Table Semantics

**Status:** Accepted  
**Date:** 2026-05-20  
**Relates to:** [ADR-001 Single-Table Outbox](adr-001-single-table-outbox.md), [ADR-006 Outbox Table Partitioning](adr-006-outbox-table-partitioning.md)

---

## Context

`tide_outbox_messages` is a **single shared table** used by all outboxes in the pg_tide schema
(per ADR-001).  When ADR-006 introduced declarative range partitioning via
`tide.outbox_convert_to_partitioned()`, the implementation performed a **global** DDL operation:
renaming `tide_outbox_messages` to a backup table and renaming a new partitioned shadow to
`tide_outbox_messages`.  This affects **all** outboxes simultaneously, not just the one being
converted.

Prior assessments identified two gaps in the v0.25.0 implementation:

1. **Silent NAMEDATALEN truncation** — the backup table name `tide_outbox_messages_backup_<name>`
   (29-byte prefix) plus a long outbox name could exceed PostgreSQL's 63-byte identifier limit,
   causing silent truncation and potential name collisions between outboxes.
2. **Missing prerequisite guard** — converting one outbox while others still use the shared
   unpartitioned table breaks concurrent writers on the other outboxes.

---

## Decision

### Option A: Per-outbox separate tables
Each outbox gets its own message table (`tide.tide_outbox_messages_<name>`).  Partitioning is
trivially per-outbox.

**Tradeoffs:**
- Eliminates the global rename issue.
- Requires a major breaking change to the ADR-001 shared-table design.
- Breaks all existing relay consumers, SQL queries, and documentation.
- Multiplies table count proportionally with outbox count (poor for systems with many small outboxes).

### Option B: Per-outbox partition key column
Add an `outbox_name` partition key column and use LIST partitioning per outbox.

**Tradeoffs:**
- Per-outbox partitions without a global rename.
- Requires a large-scale data migration.
- Changes the primary access pattern (index on `outbox_name` already exists; LIST partition adds
  pruning).
- Complex to implement incrementally without downtime.

### Option C: Global rename with explicit prerequisite guard (chosen)
Retain the ADR-001 shared-table design.  Partitioning is a **global operation** — all outboxes are
migrated simultaneously.  Guard the conversion with:

1. A **NAMEDATALEN check** that rejects outbox names long enough to produce identifiers exceeding
   63 bytes.
2. A **prerequisite check** that rejects conversion unless all outboxes already use partitioned
   strategy OR the operator explicitly passes `confirm_shared_table_migration = TRUE`.
3. The `confirm_shared_table_migration` parameter follows the `admin_rewind_offset()` opt-in
   pattern introduced in v0.23.0 — explicit acknowledgement of a destructive operation.

**Tradeoffs:**
- No breaking change to ADR-001.
- Clear operational procedure: convert all outboxes in a single maintenance window.
- Operators who understand the global scope can bypass the check deliberately.
- Cosmetically asymmetric (one function converts all, not just one), but documented here.

---

## Consequences

### Migration procedure (recommended)

To convert a pg_tide installation to partitioned outboxes:

1. **Identify all outboxes:**
   ```sql
   SELECT outbox_name, partition_strategy
   FROM tide.tide_outbox_config
   ORDER BY outbox_name;
   ```

2. **Schedule a maintenance window** (brief — the advisory-lock swap takes milliseconds, but
   `tide_outbox_messages` will be temporarily unavailable to writers during the rename).

3. **Convert all outboxes simultaneously** — call `outbox_convert_to_partitioned()` once for each
   outbox **in the same transaction block**:
   ```sql
   BEGIN;
   SELECT tide.outbox_convert_to_partitioned('orders', 'daily', TRUE);
   SELECT tide.outbox_convert_to_partitioned('payments', 'daily', TRUE);
   SELECT tide.outbox_convert_to_partitioned('notifications', 'daily', TRUE);
   COMMIT;
   ```
   Pass `confirm_shared_table_migration = TRUE` to acknowledge the global scope.

4. **Verify relay delivery** — monitor relay consumer lag after the migration.

5. **Drop backup tables** (after verifying all messages are delivered):
   ```sql
   DROP TABLE IF EXISTS tide.tide_outbox_messages_backup_orders;
   DROP TABLE IF EXISTS tide.tide_outbox_messages_backup_payments;
   DROP TABLE IF EXISTS tide.tide_outbox_messages_backup_notifications;
   ```

### Single-outbox migration (advanced)

For environments where only one outbox needs partitioning and a full maintenance window is not
possible, use the opt-in parameter:

```sql
-- Warning: this will briefly lock writes to ALL outboxes.
-- Ensure all other outbox consumers are paused or tolerant of write latency.
SELECT tide.outbox_convert_to_partitioned('orders', 'daily', TRUE);
```

The prerequisite guard is bypassed, but the operator has explicitly acknowledged the global impact.

### NAMEDATALEN constraint

The backup table prefix `tide_outbox_messages_backup_` consumes 29 bytes, leaving at most
**34 characters** for the outbox name fragment (after replacing hyphens with underscores) before
the 63-byte limit is exceeded.

`outbox_create()` enforces this constraint at creation time when `partition_strategy <> 'none'`.
`outbox_convert_to_partitioned()` enforces it at conversion time.

If an existing outbox name exceeds this limit, the operator must:
1. Create a new outbox with a shorter name.
2. Migrate relay consumers to the new outbox.
3. Drop the old outbox after draining.

---

## Cross-references

- **ADR-001**: Documents the single-table design decision.
- **ADR-006**: Documents the partitioning strategy and partition sweep automation.
- **[operations/partition-management.md](../src/operations/partition-management.md)** (v0.27.0):
  Full operations runbook for partition management.
