-- pg_tide v0.45.0 -> v0.44.0 reverse migration.
--
-- Only v0.45.0 observational objects are removed.  Delivery tables, offsets,
-- DLQ rows, messages, and pipeline configuration are preserved.

DO $rollback$
DECLARE
    active_owners BIGINT;
BEGIN
    IF to_regclass('tide.relay_runtime_status') IS NULL THEN
        RETURN;
    END IF;

    SELECT COUNT(*) INTO active_owners
    FROM tide.relay_runtime_status
    WHERE owner_token IS NOT NULL
      AND last_owner_heartbeat IS NOT NULL
      AND last_owner_heartbeat > now() - interval '2 minutes';

    IF active_owners > 0 THEN
        RAISE EXCEPTION
            'v0.45.0 downgrade refused: % active relay owner(s) are still reporting; '
            'stop or roll back v0.45.0 relays before downgrading',
            active_owners;
    END IF;

    DROP VIEW IF EXISTS tide.relay_pipeline_status;
    DROP INDEX IF EXISTS tide.relay_runtime_status_heartbeat_idx;
    DROP INDEX IF EXISTS tide.relay_runtime_status_error_idx;
    DROP INDEX IF EXISTS tide.relay_dlq_unresolved_pipeline_idx;
    DROP TABLE tide.relay_runtime_status;
END $rollback$;

COMMENT ON EXTENSION pg_tide IS
    'pg_tide: transactional outbox, idempotent inbox, relay catalog — v0.44.0';
