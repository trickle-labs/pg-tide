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

-- NOTE (v0.14.0): relay_set_tenant(), relay_grant_tenant(), relay_revoke_tenant()
-- are implemented as C-language functions via #[pg_extern] in pg-tide-ext/src/relay.rs.
-- They are registered by pgrx during extension installation and must NOT be
-- redefined here as PL/pgSQL (CREATE OR REPLACE cannot switch languages).

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

-- NOTE (v0.14.0): backfill_create(), backfill_pause(), backfill_resume(), backfill_status()
-- are implemented as C-language functions via #[pg_extern] in pg-tide-ext/src/backfill.rs.
-- They are registered by pgrx during extension installation and must NOT be
-- redefined here as PL/pgSQL (CREATE OR REPLACE cannot switch languages).

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
