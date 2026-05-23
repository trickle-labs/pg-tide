-- pg_tide upgrade: 0.33.0 → 0.34.0
--
-- v0.34.0 — Universal Reverse Pipeline Sinks & DuckLake Ecosystem Completeness
--
-- Changes:
--   • Relay binary: registers 8 previously unregistered sinks in build_sink()
--     (ducklake, clickhouse, mongodb, bigquery, snowflake, delta, iceberg,
--     pg_outbox) enabling any external source to route to any sink without
--     an intermediate pg-tide inbox.
--   • No SQL catalog changes required for the relay-side registration fix.
--   • Extension version comment updated.

-- Update extension version marker.
COMMENT ON EXTENSION pg_tide IS 'pg_tide: transactional outbox, idempotent inbox, relay catalog — v0.34.0';
