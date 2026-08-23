-- pg_tide 0.52.0 -> 0.53.0
-- v0.53 adds benchmark and relay observability contracts only; no catalog
-- migration is required. Keep the adjacent migration for extension upgrades.
COMMENT ON EXTENSION pg_tide IS
    'pg_tide: transactional outbox, idempotent inbox, relay catalog — v0.53.0';
