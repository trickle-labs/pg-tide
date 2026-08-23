-- pg_tide 0.53.0 -> 0.54.0
-- v0.54 adds lifecycle and release integration only; no catalog DDL is required.
-- Keep the adjacent migration for extension upgrades.
COMMENT ON EXTENSION pg_tide IS
    'pg_tide: transactional outbox, idempotent inbox, relay catalog — v0.54.0';
