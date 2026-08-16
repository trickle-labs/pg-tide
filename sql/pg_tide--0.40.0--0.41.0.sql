-- pg_tide v0.40.0 → v0.41.0 migration
--
-- v0.41.0 changes release metadata only. No catalog objects or user data
-- are changed.
COMMENT ON EXTENSION pg_tide IS
    'Transactional outbox, idempotent inbox, and relay catalog for PostgreSQL — v0.41.0';
