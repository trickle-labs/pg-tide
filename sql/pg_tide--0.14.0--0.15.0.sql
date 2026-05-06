-- pg_tide 0.14.0 → 0.15.0
--
-- v0.15.0: TLS Enforcement, Secret Redaction, Outbox Retention Sweep,
--          Worker Resilience, Connection Pooling, Schema Registry Passthrough.
--
-- Changes:
--   1. Outbox Retention Sweep: tide.outbox_truncate_delivered() function that
--      deletes consumed messages older than the configured retention_hours.

-- ── 1. Outbox Retention Sweep ───────────────────────────────────────────────

-- Delete consumed outbox messages older than the outbox's configured
-- retention_hours.  Returns the number of rows deleted.
--
-- Intended to be called by the `pg-tide sweep` CLI command on a schedule
-- (e.g. via pg_cron or an external cron job).
--
-- Example usage:
--   SELECT tide.outbox_truncate_delivered('my_outbox');
CREATE OR REPLACE FUNCTION tide.outbox_truncate_delivered(
    p_outbox_name TEXT
)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
DECLARE
    v_retention INT;
    v_deleted   BIGINT;
BEGIN
    SELECT retention_hours
    INTO   v_retention
    FROM   tide.tide_outbox_config
    WHERE  outbox_name = p_outbox_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'outbox_truncate_delivered: outbox ''%'' does not exist',
            p_outbox_name;
    END IF;

    DELETE FROM tide.tide_outbox_messages
    WHERE  outbox_name = p_outbox_name
      AND  consumed_at IS NOT NULL
      AND  consumed_at < now() - (v_retention || ' hours')::interval;

    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    RETURN v_deleted;
END;
$$;

COMMENT ON FUNCTION tide.outbox_truncate_delivered(TEXT) IS
    'Delete consumed outbox messages older than the outbox retention window. '
    'Returns the number of rows deleted. Added in v0.15.0.';
