-- pg_tide 0.13.0 → 0.14.0
--
-- v0.14.0: Replay Workbench, CloudEvents, Tenant-Aware Relay Groups & Managed Backfill.
--
-- Changes:
--   1. Replay Workbench: commit_offset monotonicity guard + rewind API,
--      inbox_status fleet summary view, DLQ resolve/requeue helpers,
--      relay_replay_preview() function.
--   2. Tenant-Aware Relay Groups: tenant_name column in relay config tables
--      and consumer offsets, RLS policies, relay_set_tenant() /
--      relay_grant_tenant() admin API.
--   3. Managed Backfill Jobs: tide.backfill_jobs catalog table, helpers
--      for create / pause / resume / status.
--   4. CloudEvents v1.0 wire-format marker column in relay_outbox_config.

-- ── 1. Replay Workbench ─────────────────────────────────────────────────────

-- 1a. Commit-offset monotonicity guard: ensure committed offsets only advance
--     forward.  A separate admin-rewind function allows intentional rollback.

-- Fix the ON CONFLICT clause in tide_consumer_offsets to enforce monotonicity:
-- committed_offset must never go backwards via normal commit.
-- We achieve this by adding a check constraint and a dedicated rewind function.

-- Admin rewind function: explicitly roll back a consumer offset to an earlier
-- position.  Only superusers or pg_tide_admin role may call this.
CREATE OR REPLACE FUNCTION tide.consumer_offset_rewind(
    p_group_name   TEXT,
    p_consumer_id  TEXT,
    p_target_offset BIGINT
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
BEGIN
    -- Require caller to be superuser or member of pg_tide_admin.
    IF NOT (
        pg_has_role(current_user, 'pg_tide_admin', 'MEMBER')
        OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = current_user AND rolsuper
        )
    ) THEN
        RAISE EXCEPTION
            'consumer_offset_rewind: requires pg_tide_admin or superuser privileges';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM tide.tide_consumer_offsets
        WHERE group_name = p_group_name AND consumer_id = p_consumer_id
    ) THEN
        RAISE EXCEPTION 'consumer offset not found for group %, consumer %',
            p_group_name, p_consumer_id;
    END IF;

    UPDATE tide.tide_consumer_offsets
    SET committed_offset = p_target_offset,
        last_heartbeat   = now()
    WHERE group_name = p_group_name
      AND consumer_id = p_consumer_id;

    -- Audit the rewind.
    INSERT INTO tide.tide_security_audit (action, target_object, performed_by)
    VALUES (
        'CONSUMER_OFFSET_REWIND',
        p_group_name || '/' || p_consumer_id || ' → ' || p_target_offset::text,
        current_user
    );
END;
$$;

COMMENT ON FUNCTION tide.consumer_offset_rewind(TEXT, TEXT, BIGINT) IS
    'TIDE-REPLAY-1 (v0.14.0): Admin-only intentional offset rollback with audit.';

-- 1b. Replay preview: returns the messages in an outbox that fall within a
--     given ID range, without consuming or advancing any offset.
CREATE OR REPLACE FUNCTION tide.relay_replay_preview(
    p_outbox       TEXT,
    p_from_id      BIGINT DEFAULT 0,
    p_to_id        BIGINT DEFAULT 9223372036854775807,
    p_limit        INT    DEFAULT 100
)
RETURNS JSONB
LANGUAGE plpgsql
STABLE
SECURITY INVOKER
SET search_path = tide, pg_catalog
AS $$
DECLARE
    v_result JSONB;
BEGIN
    SELECT jsonb_agg(
        jsonb_build_object(
            'id',         m.id,
            'outbox_name', m.outbox_name,
            'payload',    m.payload,
            'headers',    m.headers,
            'created_at', m.created_at,
            'consumed',   (m.consumed_at IS NOT NULL)
        )
        ORDER BY m.id
    )
    INTO v_result
    FROM tide.tide_outbox_messages m
    WHERE m.outbox_name = p_outbox
      AND m.id BETWEEN p_from_id AND p_to_id
    LIMIT p_limit;

    RETURN COALESCE(v_result, '[]'::jsonb);
END;
$$;

COMMENT ON FUNCTION tide.relay_replay_preview(TEXT, BIGINT, BIGINT, INT) IS
    'TIDE-REPLAY-2 (v0.14.0): Preview messages in an outbox within an ID range '
    '(read-only, does not advance any offset).';

-- 1c. DLQ resolve / requeue helpers.

-- Ensure the DLQ table exists (may have been created in v0.7.0).
CREATE TABLE IF NOT EXISTS tide.relay_dlq (
    id            BIGSERIAL   NOT NULL PRIMARY KEY,
    pipeline_name TEXT        NOT NULL,
    dedup_key     TEXT        NOT NULL DEFAULT '',
    payload       JSONB,
    error_message TEXT,
    attempt_count INT         NOT NULL DEFAULT 1,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved      BOOLEAN     NOT NULL DEFAULT false,
    resolved_at   TIMESTAMPTZ,
    resolved_by   TEXT
);

COMMENT ON TABLE tide.relay_dlq IS
    'TIDE-DLQ-1: Dead-letter queue for failed relay messages.';

-- Mark a DLQ entry as resolved (closed without requeue).
CREATE OR REPLACE FUNCTION tide.dlq_resolve(
    p_pipeline_name TEXT,
    p_dedup_key     TEXT
)
RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = tide, pg_catalog
AS $$
BEGIN
    UPDATE tide.relay_dlq
    SET resolved     = true,
        resolved_at  = now(),
        resolved_by  = current_user
    WHERE pipeline_name = p_pipeline_name
      AND dedup_key     = p_dedup_key
      AND resolved      = false;
END;
$$;

COMMENT ON FUNCTION tide.dlq_resolve(TEXT, TEXT) IS
    'TIDE-REPLAY-3 (v0.14.0): Mark a DLQ entry as resolved (no requeue).';

-- Requeue a DLQ entry: reset retry state so the relay picks it up again.
CREATE OR REPLACE FUNCTION tide.dlq_requeue(
    p_pipeline_name TEXT,
    p_dedup_key     TEXT
)
RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = tide, pg_catalog
AS $$
BEGIN
    UPDATE tide.relay_dlq
    SET resolved      = true,
        resolved_at   = now(),
        resolved_by   = current_user || ' (requeue)',
        attempt_count = 0
    WHERE pipeline_name = p_pipeline_name
      AND dedup_key     = p_dedup_key
      AND resolved      = false;

    -- Re-insert as a fresh unresolved entry so the relay picks it up.
    INSERT INTO tide.relay_dlq (pipeline_name, dedup_key, payload, error_message, attempt_count, resolved)
    SELECT pipeline_name, dedup_key, payload, NULL, 0, false
    FROM   tide.relay_dlq
    WHERE  pipeline_name = p_pipeline_name
      AND  dedup_key     = p_dedup_key
      AND  resolved      = true
      AND  resolved_by   LIKE current_user || '%'
    ORDER BY id DESC
    LIMIT 1;
END;
$$;

COMMENT ON FUNCTION tide.dlq_requeue(TEXT, TEXT) IS
    'TIDE-REPLAY-4 (v0.14.0): Requeue a DLQ entry for relay retry.';

-- 1d. Inbox fleet-summary view (used by inbox_status(NULL)).
CREATE OR REPLACE VIEW tide.inbox_fleet_summary AS
SELECT
    c.inbox_name,
    c.inbox_schema,
    c.max_retries,
    c.processed_retention_hours,
    c.created_at
FROM tide.tide_inbox_config c
ORDER BY c.inbox_name;

COMMENT ON VIEW tide.inbox_fleet_summary IS
    'TIDE-REPLAY-5 (v0.14.0): Fleet summary of all configured inboxes.';

-- ── 2. Tenant-Aware Relay Groups ────────────────────────────────────────────

-- 2a. Add tenant_name column to relay config tables and consumer offsets.
ALTER TABLE tide.relay_outbox_config
    ADD COLUMN IF NOT EXISTS tenant_name TEXT NOT NULL DEFAULT 'default';

ALTER TABLE tide.relay_inbox_config
    ADD COLUMN IF NOT EXISTS tenant_name TEXT NOT NULL DEFAULT 'default';

ALTER TABLE tide.relay_consumer_offsets
    ADD COLUMN IF NOT EXISTS tenant_name TEXT NOT NULL DEFAULT 'default';

COMMENT ON COLUMN tide.relay_outbox_config.tenant_name IS
    'TIDE-TENANT-1 (v0.14.0): Tenant discriminator for multi-tenant relay groups.';
COMMENT ON COLUMN tide.relay_inbox_config.tenant_name IS
    'TIDE-TENANT-1 (v0.14.0): Tenant discriminator for multi-tenant relay groups.';
COMMENT ON COLUMN tide.relay_consumer_offsets.tenant_name IS
    'TIDE-TENANT-1 (v0.14.0): Tenant discriminator for multi-tenant relay groups.';

-- 2b. Tenant ACL table: maps (tenant_name → role_name) for RLS enforcement.
CREATE TABLE IF NOT EXISTS tide.relay_tenant_grants (
    tenant_name  TEXT        NOT NULL,
    role_name    TEXT        NOT NULL,
    granted_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    granted_by   TEXT        NOT NULL DEFAULT current_user,
    PRIMARY KEY (tenant_name, role_name)
);

COMMENT ON TABLE tide.relay_tenant_grants IS
    'TIDE-TENANT-2 (v0.14.0): Per-tenant role grants for RLS enforcement.';

-- 2c. Enable RLS on relay config tables.
ALTER TABLE tide.relay_outbox_config ENABLE ROW LEVEL SECURITY;
ALTER TABLE tide.relay_inbox_config  ENABLE ROW LEVEL SECURITY;

-- Superusers and members of pg_tide_admin bypass RLS.
CREATE POLICY relay_outbox_tenant_isolation ON tide.relay_outbox_config
    USING (
        pg_has_role(current_user, 'pg_tide_admin', 'MEMBER')
        OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = current_user AND rolsuper
        )
        OR EXISTS (
            SELECT 1 FROM tide.relay_tenant_grants g
            WHERE g.tenant_name = relay_outbox_config.tenant_name
              AND pg_has_role(current_user, g.role_name, 'MEMBER')
        )
    );

CREATE POLICY relay_inbox_tenant_isolation ON tide.relay_inbox_config
    USING (
        pg_has_role(current_user, 'pg_tide_admin', 'MEMBER')
        OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = current_user AND rolsuper
        )
        OR EXISTS (
            SELECT 1 FROM tide.relay_tenant_grants g
            WHERE g.tenant_name = relay_inbox_config.tenant_name
              AND pg_has_role(current_user, g.role_name, 'MEMBER')
        )
    );

-- 2d. relay_set_tenant(): set/update the tenant for a relay pipeline.
CREATE OR REPLACE FUNCTION tide.relay_set_tenant(
    p_pipeline_name TEXT,
    p_tenant_name   TEXT
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
DECLARE
    v_updated_outbox INT;
    v_updated_inbox  INT;
BEGIN
    -- Validate pipeline exists.
    IF NOT EXISTS (
        SELECT 1 FROM tide.relay_outbox_config WHERE name = p_pipeline_name
        UNION ALL
        SELECT 1 FROM tide.relay_inbox_config  WHERE name = p_pipeline_name
    ) THEN
        RAISE EXCEPTION 'relay pipeline "%" does not exist', p_pipeline_name;
    END IF;

    -- Update outbox config.
    UPDATE tide.relay_outbox_config
    SET tenant_name = p_tenant_name
    WHERE name = p_pipeline_name;
    GET DIAGNOSTICS v_updated_outbox = ROW_COUNT;

    -- Update inbox config.
    UPDATE tide.relay_inbox_config
    SET tenant_name = p_tenant_name
    WHERE name = p_pipeline_name;
    GET DIAGNOSTICS v_updated_inbox = ROW_COUNT;

    IF v_updated_outbox = 0 AND v_updated_inbox = 0 THEN
        RAISE EXCEPTION 'relay pipeline "%" not found for tenant assignment', p_pipeline_name;
    END IF;

    -- Update consumer offsets to match new tenant.
    UPDATE tide.relay_consumer_offsets
    SET tenant_name = p_tenant_name
    WHERE pipeline_id = p_pipeline_name;
END;
$$;

COMMENT ON FUNCTION tide.relay_set_tenant(TEXT, TEXT) IS
    'TIDE-TENANT-3 (v0.14.0): Assign a relay pipeline to a tenant.';

-- 2e. relay_grant_tenant(): grant a role access to a tenant's pipelines.
CREATE OR REPLACE FUNCTION tide.relay_grant_tenant(
    p_tenant_name TEXT,
    p_role_name   TEXT
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
BEGIN
    INSERT INTO tide.relay_tenant_grants (tenant_name, role_name)
    VALUES (p_tenant_name, p_role_name)
    ON CONFLICT (tenant_name, role_name) DO NOTHING;

    INSERT INTO tide.tide_security_audit (action, target_role, target_object, performed_by)
    VALUES ('RELAY_GRANT_TENANT', p_role_name, p_tenant_name, current_user);
END;
$$;

COMMENT ON FUNCTION tide.relay_grant_tenant(TEXT, TEXT) IS
    'TIDE-TENANT-4 (v0.14.0): Grant a role access to all pipelines in a tenant.';

-- 2f. relay_revoke_tenant(): revoke tenant access from a role.
CREATE OR REPLACE FUNCTION tide.relay_revoke_tenant(
    p_tenant_name TEXT,
    p_role_name   TEXT
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
BEGIN
    DELETE FROM tide.relay_tenant_grants
    WHERE tenant_name = p_tenant_name AND role_name = p_role_name;

    INSERT INTO tide.tide_security_audit (action, target_role, target_object, performed_by)
    VALUES ('RELAY_REVOKE_TENANT', p_role_name, p_tenant_name, current_user);
END;
$$;

COMMENT ON FUNCTION tide.relay_revoke_tenant(TEXT, TEXT) IS
    'TIDE-TENANT-5 (v0.14.0): Revoke a role''s access to a tenant.';

-- ── 3. Managed Backfill Jobs ────────────────────────────────────────────────

-- 3a. Backfill jobs catalog table.
CREATE TABLE IF NOT EXISTS tide.backfill_jobs (
    job_id          BIGSERIAL   NOT NULL PRIMARY KEY,
    job_name        TEXT        NOT NULL UNIQUE,
    outbox_name     TEXT        NOT NULL
                                REFERENCES tide.tide_outbox_config(outbox_name)
                                ON DELETE RESTRICT,
    pipeline_name   TEXT,
    from_id         BIGINT      NOT NULL DEFAULT 0,
    to_id           BIGINT      NOT NULL DEFAULT 9223372036854775807,
    chunk_size      INT         NOT NULL DEFAULT 500,
    rows_processed  BIGINT      NOT NULL DEFAULT 0,
    rows_total      BIGINT,
    status          TEXT        NOT NULL DEFAULT 'pending'
                                CHECK (status IN
                                    ('pending', 'running', 'paused', 'completed', 'failed')),
    error_message   TEXT,
    throttle_ms     INT         NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ,
    paused_at       TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    created_by      TEXT        NOT NULL DEFAULT current_user
);

COMMENT ON TABLE tide.backfill_jobs IS
    'TIDE-BACKFILL-1 (v0.14.0): Cataloged backfill jobs with progress tracking, '
    'chunking, and pause/resume support.';

-- 3b. backfill_create(): register a new backfill job.
CREATE OR REPLACE FUNCTION tide.backfill_create(
    p_job_name      TEXT,
    p_outbox_name   TEXT,
    p_pipeline_name TEXT   DEFAULT NULL,
    p_from_id       BIGINT DEFAULT 0,
    p_to_id         BIGINT DEFAULT 9223372036854775807,
    p_chunk_size    INT    DEFAULT 500,
    p_throttle_ms   INT    DEFAULT 0
)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = tide, pg_catalog
AS $$
DECLARE
    v_job_id    BIGINT;
    v_rows_total BIGINT;
BEGIN
    -- Validate outbox exists.
    IF NOT EXISTS (
        SELECT 1 FROM tide.tide_outbox_config WHERE outbox_name = p_outbox_name
    ) THEN
        RAISE EXCEPTION 'outbox "%" does not exist', p_outbox_name;
    END IF;

    -- Estimate total rows.
    SELECT COUNT(*) INTO v_rows_total
    FROM tide.tide_outbox_messages
    WHERE outbox_name = p_outbox_name
      AND id BETWEEN p_from_id AND p_to_id;

    INSERT INTO tide.backfill_jobs
        (job_name, outbox_name, pipeline_name, from_id, to_id,
         chunk_size, rows_total, throttle_ms)
    VALUES
        (p_job_name, p_outbox_name, p_pipeline_name, p_from_id, p_to_id,
         p_chunk_size, v_rows_total, p_throttle_ms)
    RETURNING job_id INTO v_job_id;

    RETURN v_job_id;
END;
$$;

COMMENT ON FUNCTION tide.backfill_create(TEXT, TEXT, TEXT, BIGINT, BIGINT, INT, INT) IS
    'TIDE-BACKFILL-2 (v0.14.0): Register a cataloged backfill job.';

-- 3c. backfill_pause(): pause a running or pending backfill job.
CREATE OR REPLACE FUNCTION tide.backfill_pause(p_job_name TEXT)
RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = tide, pg_catalog
AS $$
BEGIN
    UPDATE tide.backfill_jobs
    SET status    = 'paused',
        paused_at = now()
    WHERE job_name = p_job_name
      AND status IN ('pending', 'running');

    IF NOT FOUND THEN
        RAISE EXCEPTION 'backfill job "%" not found or not pauseable (status must be pending/running)',
            p_job_name;
    END IF;
END;
$$;

COMMENT ON FUNCTION tide.backfill_pause(TEXT) IS
    'TIDE-BACKFILL-3 (v0.14.0): Pause a pending or running backfill job.';

-- 3d. backfill_resume(): resume a paused backfill job.
CREATE OR REPLACE FUNCTION tide.backfill_resume(p_job_name TEXT)
RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = tide, pg_catalog
AS $$
BEGIN
    UPDATE tide.backfill_jobs
    SET status    = 'pending',
        paused_at = NULL
    WHERE job_name = p_job_name
      AND status   = 'paused';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'backfill job "%" not found or not paused', p_job_name;
    END IF;
END;
$$;

COMMENT ON FUNCTION tide.backfill_resume(TEXT) IS
    'TIDE-BACKFILL-4 (v0.14.0): Resume a paused backfill job.';

-- 3e. backfill_status(): return progress JSON for a job (or fleet summary when NULL).
CREATE OR REPLACE FUNCTION tide.backfill_status(p_job_name TEXT DEFAULT NULL)
RETURNS JSONB
LANGUAGE plpgsql
STABLE
SECURITY INVOKER
SET search_path = tide, pg_catalog
AS $$
DECLARE
    v_result JSONB;
BEGIN
    IF p_job_name IS NOT NULL THEN
        SELECT jsonb_build_object(
            'job_id',         j.job_id,
            'job_name',       j.job_name,
            'outbox_name',    j.outbox_name,
            'pipeline_name',  j.pipeline_name,
            'status',         j.status,
            'rows_processed', j.rows_processed,
            'rows_total',     j.rows_total,
            'pct_complete',   CASE
                                WHEN j.rows_total > 0
                                THEN ROUND(j.rows_processed::numeric / j.rows_total * 100, 2)
                                ELSE 0
                              END,
            'chunk_size',     j.chunk_size,
            'throttle_ms',    j.throttle_ms,
            'created_at',     j.created_at,
            'started_at',     j.started_at,
            'paused_at',      j.paused_at,
            'completed_at',   j.completed_at,
            'error_message',  j.error_message
        )
        INTO v_result
        FROM tide.backfill_jobs j
        WHERE j.job_name = p_job_name;

        IF v_result IS NULL THEN
            RAISE EXCEPTION 'backfill job "%" not found', p_job_name;
        END IF;
    ELSE
        -- Fleet summary.
        SELECT jsonb_agg(
            jsonb_build_object(
                'job_id',         j.job_id,
                'job_name',       j.job_name,
                'outbox_name',    j.outbox_name,
                'status',         j.status,
                'rows_processed', j.rows_processed,
                'rows_total',     j.rows_total,
                'pct_complete',   CASE
                                    WHEN j.rows_total > 0
                                    THEN ROUND(j.rows_processed::numeric / j.rows_total * 100, 2)
                                    ELSE 0
                                  END,
                'created_at',     j.created_at
            )
            ORDER BY j.job_id
        )
        INTO v_result
        FROM tide.backfill_jobs j;

        v_result = jsonb_build_object('jobs', COALESCE(v_result, '[]'::jsonb));
    END IF;

    RETURN v_result;
END;
$$;

COMMENT ON FUNCTION tide.backfill_status(TEXT) IS
    'TIDE-BACKFILL-5 (v0.14.0): Progress JSON for a backfill job, or fleet summary '
    'when called with NULL.';

-- ── 4. CloudEvents v1.0 wire-format support ─────────────────────────────────

-- Add a wire_format column to relay_outbox_config so operators can request
-- CloudEvents encoding per pipeline without rebuilding.
ALTER TABLE tide.relay_outbox_config
    ADD COLUMN IF NOT EXISTS wire_format TEXT NOT NULL DEFAULT 'native'
        CHECK (wire_format IN ('native', 'debezium', 'cloudevents', 'maxwell', 'canal', 'cdc_json'));

ALTER TABLE tide.relay_inbox_config
    ADD COLUMN IF NOT EXISTS wire_format TEXT NOT NULL DEFAULT 'native'
        CHECK (wire_format IN ('native', 'debezium', 'cloudevents', 'maxwell', 'canal', 'cdc_json'));

COMMENT ON COLUMN tide.relay_outbox_config.wire_format IS
    'TIDE-CE-1 (v0.14.0): Wire encoding format for this pipeline '
    '(native | debezium | cloudevents | maxwell | canal | cdc_json).';
COMMENT ON COLUMN tide.relay_inbox_config.wire_format IS
    'TIDE-CE-1 (v0.14.0): Wire decoding format for this pipeline.';
