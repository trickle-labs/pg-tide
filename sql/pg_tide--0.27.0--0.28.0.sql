-- pg_tide 0.27.0 → 0.28.0
--
-- v0.28.0: Delivery Receipts, Canonical Config Enforcement,
--          Claim-Check Native Pathway & Per-Tenant DB Role Provisioning
--
-- Changes:
--   1. tide.relay_delivery_receipts — auditable log of every confirmed
--      message delivery; written by the relay after Sink::publish() ack.
--   2. tide.outbox_delivery_confirm() — convenience query for receipt ranges.
--   3. tide.relay_truncate_delivery_receipts() — retention pruning helper.
--   4. tide.outbox_publish_large() — threshold-based claim-check publish;
--      large payloads stored in pg_largeobject, small ones delegate to
--      outbox_publish().
--   5. tide.relay_tenant_roles — per-tenant DB role provisioning catalog.
--   6. tide.relay_provision_tenant() — create role and grant scoped access.
--   7. tide.relay_deprovision_tenant() — revoke and remove tenant role mapping.
--   8. db_role TEXT column on tide.tide_outbox_config and tide.tide_inbox_config —
--      when set the relay opens a SET ROLE session for that pipeline's worker.

-- ── 1. Delivery receipts catalog table ───────────────────────────────────────

CREATE TABLE IF NOT EXISTS tide.relay_delivery_receipts (
    id              BIGSERIAL        NOT NULL,
    pipeline_name   TEXT             NOT NULL,
    outbox_name     TEXT             NOT NULL,
    message_id      BIGINT           NOT NULL,
    dedup_key       TEXT             NOT NULL DEFAULT '',
    delivered_at    TIMESTAMPTZ      NOT NULL DEFAULT now(),
    sink_type       TEXT             NOT NULL DEFAULT '',
    tenant_name     TEXT             NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_relay_delivery_receipts_pipeline_at
    ON tide.relay_delivery_receipts (pipeline_name, delivered_at);

CREATE INDEX IF NOT EXISTS idx_relay_delivery_receipts_outbox_msg
    ON tide.relay_delivery_receipts (outbox_name, message_id);

-- ── 2. outbox_delivery_confirm() ─────────────────────────────────────────────

CREATE OR REPLACE FUNCTION tide.outbox_delivery_confirm(
    p_outbox_name TEXT,
    p_from_id     BIGINT DEFAULT 0,
    p_to_id       BIGINT DEFAULT 9223372036854775807
)
RETURNS TABLE (
    confirmed_count BIGINT,
    latest_delivered_at TIMESTAMPTZ
)
LANGUAGE SQL
STABLE
SET search_path = tide, pg_catalog
AS $$
    SELECT
        COUNT(*)::BIGINT AS confirmed_count,
        MAX(delivered_at)         AS latest_delivered_at
    FROM tide.relay_delivery_receipts
    WHERE outbox_name = p_outbox_name
      AND message_id BETWEEN p_from_id AND p_to_id;
$$;

-- ── 3. relay_truncate_delivery_receipts() ────────────────────────────────────

CREATE OR REPLACE FUNCTION tide.relay_truncate_delivery_receipts(
    p_older_than INTERVAL DEFAULT INTERVAL '30 days'
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

-- ── 4. outbox_publish_large() ────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION tide.outbox_publish_large(
    p_name           TEXT,
    p_payload        JSONB,
    p_dedup_key      TEXT    DEFAULT '',
    p_threshold_bytes INT    DEFAULT 65536
)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
DECLARE
    v_payload_size INT;
    v_loid         OID;
    v_fd           INT;
    v_envelope     JSONB;
BEGIN
    -- Validate outbox name to prevent SQL injection via dynamic SQL.
    IF p_name ~ '[^a-zA-Z0-9_\-]' THEN
        RAISE EXCEPTION 'invalid outbox name: %', p_name
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    v_payload_size := pg_column_size(p_payload);

    IF v_payload_size <= p_threshold_bytes THEN
        -- Below threshold: delegate to standard outbox_publish().
        PERFORM tide.outbox_publish(p_name, p_payload, '{}'::JSONB);
        RETURN;
    END IF;

    -- Above threshold: store payload in pg_largeobject and write claim-check.
    -- SAFETY: lo_creat, lo_open, lowrite, lo_close are standard PostgreSQL
    -- large-object functions; the OID is owned by the calling role.
    v_loid := lo_creat(-1);
    v_fd   := lo_open(v_loid, 131072); -- INV_WRITE = 0x20000
    PERFORM lowrite(v_fd, convert_to(p_payload::TEXT, 'UTF8'));
    PERFORM lo_close(v_fd);

    -- Grant relay role SELECT access (lo_get uses SELECT privilege).
    -- The relay role must exist; this is a no-op if not granted separately.
    BEGIN
        EXECUTE format('GRANT SELECT ON LARGE OBJECT %s TO pg_read_all_data', v_loid);
    EXCEPTION WHEN OTHERS THEN
        NULL; -- ignore if role does not exist; operator must grant manually
    END;

    v_envelope := jsonb_build_object(
        '_cc',   true,
        'oid',   v_loid::TEXT,
        'size',  v_payload_size,
        'dedup_key', p_dedup_key
    );

    PERFORM tide.outbox_publish(p_name, v_envelope, '{}'::JSONB);
END;
$$;

-- ── 5. relay_tenant_roles catalog table ──────────────────────────────────────

CREATE TABLE IF NOT EXISTS tide.relay_tenant_roles (
    tenant_id      TEXT        NOT NULL,
    db_role        NAME        NOT NULL,
    provisioned_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id)
);

-- ── 6. relay_provision_tenant() ──────────────────────────────────────────────

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

-- ── 7. relay_deprovision_tenant() ─────────────────────────────────────────────

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

-- ── 8. db_role column on outbox/inbox config ──────────────────────────────────

ALTER TABLE tide.tide_outbox_config
    ADD COLUMN IF NOT EXISTS db_role TEXT;

ALTER TABLE tide.tide_inbox_config
    ADD COLUMN IF NOT EXISTS db_role TEXT;
