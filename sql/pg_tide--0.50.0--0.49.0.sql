-- Exact reverse of the no-op 0.49.0 -> 0.50.0 transition.
COMMENT ON EXTENSION pg_tide IS
    'pg_tide: transactional outbox, idempotent inbox, relay catalog';
