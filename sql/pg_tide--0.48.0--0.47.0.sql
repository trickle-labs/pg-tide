-- Exact reverse of the data-preserving 0.47.0 -> 0.48.0 transition.
COMMENT ON EXTENSION pg_tide IS
    'pg_tide: transactional outbox, idempotent inbox, relay catalog';
