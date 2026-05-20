-- pg_tide upgrade: 0.31.0 → 0.32.0
--
-- v0.32.0 — Performance Engineering, Code Internals Quality & Benchmark Hardening
--
-- Changes:
--   • Extension version comment updated.
--   • No DDL schema changes in this release — all changes are in the relay binary
--     and extension Rust hot-path implementations:
--       - Publisher-ACL SPI consolidation (3 → 1 round-trip per outbox_publish)
--       - inbox_status() fleet summary N+1 elimination (N+1 → 2 SPI calls)
--       - Webhook HMAC expect() replaced with unreachable!()
--       - Coordinator secrets-logging unwrap_or_default() fixed to "{}" fallback
--       - Fleet inbox_status() SPI error propagation hardened
--   • WAL logical-replication source groundwork (feature-gated, wal-source)
--   • Extended Criterion benchmarks (consumer-group poll path, 500-row inbox batch)
--   • ADR-009: WAL logical-replication source design document published

-- Update extension version marker.
COMMENT ON EXTENSION pg_tide IS 'pg_tide: transactional outbox, idempotent inbox, relay catalog — v0.32.0';
