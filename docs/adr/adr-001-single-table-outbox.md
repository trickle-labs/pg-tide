# ADR-001: Single-Table Outbox Design

**Status:** Accepted  
**Date:** 2026-05-06  
**Author:** pg_tide Contributors  

## Context

The transactional outbox pattern requires storing outgoing events alongside
application data in the same database transaction to ensure atomicity.
The primary design question is whether to use one shared outbox table for all
streams (single-table) or separate tables per stream (multi-table).

**Option A: Single shared table** — all outbox events go into
`tide.tide_outbox_messages` with an `outbox_name` discriminator column.

**Option B: Per-outbox tables** — each named outbox gets its own table
(e.g. `tide.orders_outbox`, `tide.payments_outbox`).

## Decision

We chose **Option A: single shared table with a discriminator column**.

Rationale:

1. **Simpler schema management** — No DDL required to create a new outbox.
   `tide.outbox_create()` only inserts a config row.
2. **Uniform indexing** — A single composite index on
   `(outbox_name, id, consumed_at)` covers all consumer queries without
   per-outbox index management.
3. **Consistent retention** — `outbox_truncate_delivered()` operates on
   a single table with a `WHERE outbox_name = $1` predicate.
4. **No partition explosion** — Applications often have dozens of outboxes;
   individual tables would fragment the catalog.

## Consequences

- **Positive**: Zero DDL for outbox creation; straightforward consumer queries.
- **Positive**: A single Prometheus metric (`consumer_lag`) with a
  `pipeline` label covers all outboxes.
- **Negative**: At very high write rates (>100k/s), table-level lock contention
  could become a bottleneck. This is deferred to v1.x where partitioning by
  `outbox_name` or time range can be introduced without API changes.
- **Negative**: Row-level `consumed_at` scanning requires careful index
  maintenance; `VACUUM` frequency must account for the update pattern.
