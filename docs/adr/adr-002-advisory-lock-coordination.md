# ADR-002: Advisory Lock Coordination

**Status:** Accepted  
**Date:** 2026-05-06  
**Author:** pg_tide Contributors  

## Context

The `pg-tide` relay binary must distribute pipeline ownership across multiple
instances (for HA/active-active deployments) without double-processing.
The coordination mechanism must be lightweight, use no external dependencies
(no ZooKeeper, etcd, or Consul), and survive relay restarts cleanly.

**Candidates evaluated:**

- **PostgreSQL advisory locks** — `pg_try_advisory_lock(key1, key2)` within
  PostgreSQL itself; locks are automatically released when the session ends.
- **Database row leases** — a `relay_leases` table with a TTL column;
  coordinator polls and refreshes.
- **External coordination service** — etcd/Consul distributed locks.

## Decision

We chose **PostgreSQL session-level advisory locks** via
`pg_try_advisory_lock(hashtext($relay_group_id), hashtext($pipeline_name))`.

Rationale:

1. **Zero external dependencies** — PostgreSQL is already the required
   infrastructure component; no additional service to operate.
2. **Automatic cleanup** — Advisory locks are automatically released when the
   PostgreSQL session closes, ensuring no zombie locks even after relay crashes.
3. **Deterministic ownership** — `pg_try_advisory_lock` returns immediately
   (non-blocking); a relay instance either owns a pipeline or skips it.
4. **Group namespacing** — The `relay_group_id` parameter in the first key
   allows multiple independent relay deployments to coexist against the same
   database without interfering.

## Consequences

- **Positive**: No TTL refresh loop required; lock lifecycle matches connection
  lifecycle exactly.
- **Positive**: Pipeline ownership is visible via `pg_locks` for operational
  debugging.
- **Positive**: The coordination model works correctly with a single relay
  instance (degenerate case) without any configuration changes.
- **Negative**: Advisory lock capacity is bounded by `max_locks_per_transaction`.
  With the default 50-pipeline limit, this is not a concern; it becomes
  relevant above ~500 pipelines per relay group.
- **Negative**: If a relay instance loses its database connection without a
  clean shutdown, the advisory lock is released immediately and another instance
  will claim the pipeline before the original's in-flight batch is confirmed.
  This creates a brief dedup window where the DLQ or idempotent inbox
  `ON CONFLICT DO NOTHING` absorbs duplicates.

## v0.42.0 clarification

The ownership lock is held by the exact dedicated PostgreSQL session used by
the pipeline worker. Pooled metadata connections must not acquire or release
session-level ownership locks. Losing the ownership session cancels the worker
before another instance takes over; graceful shutdown drains or terminates the
worker before the session is released. The canonical lock identity is
`(relay_group_id, tenant_name, direction, pipeline_name)`, derived by one
shared helper. Mixed v0.41.0/v0.42.0 relay ownership is unsupported during the
lock-identity transition.
