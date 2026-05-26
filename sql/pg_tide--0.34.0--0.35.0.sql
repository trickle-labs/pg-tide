-- pg_tide upgrade: 0.34.0 → 0.35.0
--
-- v0.35.0 — Assessment-7 P1/P2 Bug Fixes, KMS Encryption & Fan-In Performance Hardening
--
-- Changes:
--   1. relay_provision_tenant() / relay_deprovision_tenant() — role-name validation
--      guard before EXECUTE format() to prevent SQL injection and reserved-role collision.
--   2. backfill_progress() — division-by-zero fix for zero elapsed time / zero throughput.
--   3. relay_pipeline_dep_add() — SIMILAR TO trigger_policy validation (replaces LIKE).
--   4. relay_pipeline_deps.trigger_policy — ADD CHECK constraint for defence-in-depth.
--   5. relay_truncate_delivery_receipts() — background sweep SQL function.
--   6. relay_fanin_config — fan-in source type supported by relay coordinator worker with
--      UNNEST batch offset upserts for multi-source performance (Rust-side change; no
--      schema change required beyond the existing relay_consumer_offsets.fanin_member column).
--   7. Extension version comment updated.

-- ── 1. relay_provision_tenant() — role-name validation ───────────────────────

CREATE OR REPLACE FUNCTION tide.relay_provision_tenant(
    p_tenant_id TEXT,
    p_db_role   NAME
)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
BEGIN
    -- Validate tenant_id (no special characters beyond alphanum, _, -)
    IF p_tenant_id ~ '[^a-zA-Z0-9_\-]' OR length(p_tenant_id) = 0 OR length(p_tenant_id) > 63 THEN
        RAISE EXCEPTION 'invalid tenant_id: must be 1–63 chars, alphanumeric, underscore, or hyphen';
    END IF;

    -- v0.35.0 P1: Validate role name before EXECUTE format() to prevent reserved-role
    -- collision and unexpected identifier quoting.
    IF NOT (p_db_role::TEXT ~ '^[A-Za-z_][A-Za-z0-9_]{0,62}$') THEN
        RAISE EXCEPTION 'role name must match [A-Za-z_][A-Za-z0-9_]{0,62}: %', p_db_role;
    END IF;
    IF p_db_role::TEXT = ANY(ARRAY[
        'pg_monitor', 'pg_read_all_data', 'pg_read_all_settings',
        'pg_signal_backend', 'pg_write_all_data', 'pg_read_all_stats',
        'pg_stat_scan_tables', 'pg_database_owner', 'pg_execute_server_program',
        'pg_read_server_files', 'pg_write_server_files',
        'tide_admin', 'postgres'
    ]) THEN
        RAISE EXCEPTION 'reserved role name may not be used for tenant provisioning: %', p_db_role;
    END IF;

    -- Create the role if it does not exist.
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = p_db_role::TEXT) THEN
        EXECUTE format('CREATE ROLE %I NOLOGIN', p_db_role);
    END IF;

    -- Grant access to tide schema objects (SELECT, INSERT, UPDATE on core tables).
    EXECUTE format('GRANT USAGE ON SCHEMA tide TO %I', p_db_role);
    EXECUTE format(
        'GRANT SELECT, INSERT ON tide.tide_outbox_messages, tide.tide_inbox_messages TO %I',
        p_db_role
    );
    EXECUTE format(
        'GRANT SELECT ON tide.relay_outbox_config, tide.relay_inbox_config, tide.relay_consumer_offsets TO %I',
        p_db_role
    );
    EXECUTE format(
        'GRANT INSERT ON tide.relay_delivery_receipts TO %I',
        p_db_role
    );

    -- Record in catalog.
    INSERT INTO tide.relay_tenant_roles (tenant_id, db_role)
    VALUES (p_tenant_id, p_db_role)
    ON CONFLICT (tenant_id) DO UPDATE SET db_role = EXCLUDED.db_role, provisioned_at = now();
END;
$$;

-- ── 2. relay_deprovision_tenant() — role-name validation ─────────────────────

CREATE OR REPLACE FUNCTION tide.relay_deprovision_tenant(
    p_tenant_id TEXT
)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
DECLARE
    v_role NAME;
    v_member_count INT;
BEGIN
    SELECT db_role INTO v_role
    FROM tide.relay_tenant_roles
    WHERE tenant_id = p_tenant_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'tenant ''%'' not found in relay_tenant_roles', p_tenant_id;
    END IF;

    -- v0.35.0 P1: Validate role name (defensive — should already be valid from provisioning).
    IF NOT (v_role::TEXT ~ '^[A-Za-z_][A-Za-z0-9_]{0,62}$') THEN
        RAISE EXCEPTION 'stored role name is invalid (was it provisioned before v0.35.0?): %', v_role;
    END IF;

    -- Revoke grants.
    EXECUTE format('REVOKE ALL ON ALL TABLES IN SCHEMA tide FROM %I', v_role);
    EXECUTE format('REVOKE USAGE ON SCHEMA tide FROM %I', v_role);

    -- Drop role if it has no other memberships.
    SELECT COUNT(*) INTO v_member_count
    FROM pg_auth_members
    WHERE member = (SELECT oid FROM pg_roles WHERE rolname = v_role::TEXT);

    IF v_member_count = 0 THEN
        EXECUTE format('DROP ROLE IF EXISTS %I', v_role);
    END IF;

    DELETE FROM tide.relay_tenant_roles WHERE tenant_id = p_tenant_id;
END;
$$;

-- ── 3. backfill_progress() — division-by-zero fix ────────────────────────────

-- v0.35.0 P2: Guard against zero elapsed time and zero throughput.
-- When a backfill job was just created or no rows have been processed yet,
-- estimated_completion returns NULL rather than dividing by zero.

CREATE OR REPLACE FUNCTION tide.backfill_progress(
    p_job_name TEXT
)
RETURNS TABLE (
    rows_processed          BIGINT,
    total_rows              BIGINT,
    pct_complete            NUMERIC,
    estimated_completion    TIMESTAMPTZ,
    status                  TEXT
)
LANGUAGE SQL
STABLE
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
    SELECT
        rows_processed,
        COALESCE(rows_total, 0) AS total_rows,
        CASE
            WHEN COALESCE(rows_total, 0) > 0
            THEN ROUND(rows_processed::NUMERIC / rows_total * 100, 2)
            ELSE 0::NUMERIC
        END AS pct_complete,
        CASE
            -- No rows processed yet: cannot estimate completion.
            WHEN rows_processed = 0 THEN NULL
            -- Elapsed time < 1 second: throughput not yet measurable.
            WHEN EXTRACT(epoch FROM (now() - started_at)) < 1 THEN NULL
            -- All rows already processed: already done.
            WHEN COALESCE(rows_total, 0) <= rows_processed THEN NULL
            -- Normal case: estimate based on observed throughput.
            ELSE now() + (
                (COALESCE(rows_total, 0) - rows_processed)::float /
                GREATEST(
                    rows_processed::float /
                    NULLIF(EXTRACT(epoch FROM (now() - started_at)), 0),
                    0.001
                )
            ) * interval '1 second'
        END AS estimated_completion,
        status
    FROM tide.backfill_jobs
    WHERE job_name = p_job_name;
$$;

-- ── 4. relay_pipeline_dep_add() — SIMILAR TO trigger_policy validation ────────

-- v0.35.0 P2: Replace NOT LIKE check with SIMILAR TO for precise validation.
-- Also adds the SIMILAR TO pattern that matches on_offset_gte(N) where N is
-- a sequence of digits, preventing values like on_offset_gte(notanumber).

CREATE OR REPLACE FUNCTION tide.relay_pipeline_dep_add(
    p_upstream_pipeline    TEXT,
    p_downstream_pipeline  TEXT,
    p_trigger_policy       TEXT DEFAULT 'always'
)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
DECLARE
    v_cycle RECORD;
BEGIN
    IF p_upstream_pipeline IS NULL OR trim(p_upstream_pipeline) = '' THEN
        RAISE EXCEPTION 'upstream_pipeline must not be empty';
    END IF;
    IF p_downstream_pipeline IS NULL OR trim(p_downstream_pipeline) = '' THEN
        RAISE EXCEPTION 'downstream_pipeline must not be empty';
    END IF;
    IF p_upstream_pipeline = p_downstream_pipeline THEN
        RAISE EXCEPTION 'pipeline cannot depend on itself: ''%''', p_upstream_pipeline;
    END IF;

    -- v0.35.0 P2: SIMILAR TO validation — precise regex that only allows
    -- 'always', 'on_idle', or 'on_offset_gte(<digits>)'.
    IF p_trigger_policy NOT SIMILAR TO 'always|on_idle|on_offset_gte\([0-9]+\)' THEN
        RAISE EXCEPTION
            'invalid trigger_policy ''%''; valid: always | on_idle | on_offset_gte(N)',
            p_trigger_policy;
    END IF;

    -- Tentatively insert the edge.
    INSERT INTO tide.relay_pipeline_deps (upstream_pipeline, downstream_pipeline, trigger_policy)
    VALUES (p_upstream_pipeline, p_downstream_pipeline, p_trigger_policy)
    ON CONFLICT (upstream_pipeline, downstream_pipeline)
    DO UPDATE SET trigger_policy = EXCLUDED.trigger_policy;

    -- Cycle detection: if relay_dag_check() returns any row, roll back.
    FOR v_cycle IN SELECT * FROM tide.relay_dag_check() LOOP
        -- Remove the edge we just inserted, then raise.
        DELETE FROM tide.relay_pipeline_deps
        WHERE upstream_pipeline = p_upstream_pipeline
          AND downstream_pipeline = p_downstream_pipeline;
        RAISE EXCEPTION 'cycle detected in pipeline DAG: %', v_cycle.cycle_path;
    END LOOP;
END;
$$;

-- ── 5. relay_pipeline_deps trigger_policy CHECK constraint ────────────────────

-- v0.35.0 P2: Defence-in-depth — also add a CHECK constraint at the table level
-- so direct SQL inserts cannot bypass the function validation.
-- Use ADD CONSTRAINT IF NOT EXISTS style (catches fresh installs).
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_schema = 'tide'
          AND table_name = 'relay_pipeline_deps'
          AND constraint_name = 'chk_relay_pipeline_deps_trigger_policy'
    ) THEN
        ALTER TABLE tide.relay_pipeline_deps
            ADD CONSTRAINT chk_relay_pipeline_deps_trigger_policy
            CHECK (trigger_policy SIMILAR TO 'always|on_idle|on_offset_gte\([0-9]+\)');
    END IF;
END;
$$;

-- ── 6. relay_truncate_delivery_receipts() — background sweep function ─────────

-- v0.35.0 P2: SQL function that the coordinator background sweep task calls.
-- Deletes delivery receipt rows older than the supplied retention interval.
-- Default usage: SELECT tide.relay_truncate_delivery_receipts('24 hours'::interval);

CREATE OR REPLACE FUNCTION tide.relay_truncate_delivery_receipts(
    p_older_than INTERVAL DEFAULT '24 hours'
)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
DECLARE
    v_deleted BIGINT;
BEGIN
    DELETE FROM tide.relay_delivery_receipts
    WHERE delivered_at < now() - p_older_than;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    RETURN v_deleted;
END;
$$;

COMMENT ON FUNCTION tide.relay_truncate_delivery_receipts(INTERVAL) IS
    'TIDE-RECEIPT-SWEEP (v0.35.0): Delete delivery receipt rows older than the '
    'given retention interval. Called automatically by the relay coordinator '
    'background sweep task every sweep_interval_hours (default 24 h). '
    'Also safe to call manually for one-off pruning.';

-- ── 7. relay_consumer_offsets — unique index for fan-in member tracking ────────

-- v0.35.0 P2: The fan-in coordinator worker tracks per-member offsets using
-- (relay_group_id, pipeline_id, fanin_member) as the identity key.  Add a
-- unique index on the triple (fanin_member IS NOT NULL) so the UNNEST batch
-- upsert can use ON CONFLICT with a precise target.
CREATE UNIQUE INDEX IF NOT EXISTS uq_relay_consumer_offsets_fanin
    ON tide.relay_consumer_offsets (relay_group_id, pipeline_id, fanin_member)
    WHERE fanin_member IS NOT NULL;

-- ── 8. Extension version comment ─────────────────────────────────────────────

COMMENT ON EXTENSION pg_tide IS 'pg_tide: transactional outbox, idempotent inbox, relay catalog — v0.35.0';
