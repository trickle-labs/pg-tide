-- pg_tide 0.12.0 → 0.13.0
--
-- v0.13.0: Security hardening, reliability & performance.
--
-- Changes:
--   1. Outbox-level publisher ACLs: tide.outbox_publishers table + helpers.
--   2. SECURITY DEFINER hardening: create tide_security_audit BEFORE the
--      functions that write to it; add SET search_path to all definer fns.
--   3. Schema evolution guardrails: tide.relay_schema_fingerprints table.
--   4. DLQ unique idempotency: add unique index on (pipeline_name, dedup_key).
--   5. Connection limit: max_owned_pipelines column in relay config.

-- ── 1. Outbox-level Publisher ACLs ─────────────────────────────────────────

-- Publisher ACL table: maps (outbox_name → role_name) for fine-grained publish
-- authorization.  When this table has at least one row for a given outbox, only
-- roles listed there (or superusers) may publish to that outbox via
-- tide.outbox_publish().
CREATE TABLE IF NOT EXISTS tide.outbox_publishers (
    outbox_name  TEXT        NOT NULL
                             REFERENCES tide.tide_outbox_config(outbox_name)
                             ON DELETE CASCADE,
    role_name    TEXT        NOT NULL,
    granted_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    granted_by   TEXT        NOT NULL DEFAULT current_user,
    PRIMARY KEY (outbox_name, role_name)
);

COMMENT ON TABLE tide.outbox_publishers IS
    'TIDE-SEC-3 (v0.13.0): Per-outbox publisher ACL.  When at least one row '
    'exists for an outbox_name, only listed roles may call outbox_publish().';

-- Convenience helper: grant a role publish access to a specific outbox.
CREATE OR REPLACE FUNCTION tide.outbox_grant_publish(
    p_outbox TEXT,
    p_role   TEXT
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
BEGIN
    -- Validate outbox exists.
    IF NOT EXISTS (
        SELECT 1 FROM tide.tide_outbox_config WHERE outbox_name = p_outbox
    ) THEN
        RAISE EXCEPTION 'outbox "%" does not exist', p_outbox;
    END IF;

    INSERT INTO tide.outbox_publishers (outbox_name, role_name)
    VALUES (p_outbox, p_role)
    ON CONFLICT (outbox_name, role_name) DO NOTHING;

    -- Record in audit log.
    INSERT INTO tide.tide_security_audit (action, target_role, target_object, performed_by)
    VALUES ('GRANT_OUTBOX_PUBLISH', p_role, p_outbox, current_user);
END;
$$;

COMMENT ON FUNCTION tide.outbox_grant_publish(TEXT, TEXT) IS
    'TIDE-SEC-3 (v0.13.0): Grant a role fine-grained publish access to a specific outbox.';

-- Convenience helper: revoke publish access.
CREATE OR REPLACE FUNCTION tide.outbox_revoke_publish(
    p_outbox TEXT,
    p_role   TEXT
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
BEGIN
    DELETE FROM tide.outbox_publishers
    WHERE outbox_name = p_outbox AND role_name = p_role;

    INSERT INTO tide.tide_security_audit (action, target_role, target_object, performed_by)
    VALUES ('REVOKE_OUTBOX_PUBLISH', p_role, p_outbox, current_user);
END;
$$;

COMMENT ON FUNCTION tide.outbox_revoke_publish(TEXT, TEXT) IS
    'TIDE-SEC-3 (v0.13.0): Revoke fine-grained publish access from a role for an outbox.';

-- ── 2. SECURITY DEFINER hardening ──────────────────────────────────────────

-- Ensure the audit table exists BEFORE any SECURITY DEFINER function references it.
-- (The table was already created in v0.1.0 as tide_security_audit; this is a
--  guard to create it if for any reason it is missing.)
CREATE TABLE IF NOT EXISTS tide.tide_security_audit (
    id            BIGSERIAL   NOT NULL PRIMARY KEY,
    action        TEXT        NOT NULL,
    target_role   TEXT,
    target_object TEXT,
    performed_by  TEXT        NOT NULL DEFAULT current_user,
    performed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE tide.tide_security_audit IS
    'TIDE-SEC-1 (v0.13.0): Audit log for SECURITY DEFINER functions.';

-- Harden existing SECURITY DEFINER functions with SET search_path.
-- Re-create grant_publish / revoke_publish with hardened search_path.
CREATE OR REPLACE FUNCTION tide.grant_publish(p_role TEXT, p_outbox TEXT)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM tide.tide_outbox_config WHERE outbox_name = p_outbox
    ) THEN
        RAISE EXCEPTION 'outbox "%" does not exist', p_outbox;
    END IF;

    EXECUTE format(
        'GRANT INSERT ON tide.tide_outbox_messages TO %I',
        p_role
    );
    INSERT INTO tide.tide_security_audit (action, target_role, target_object, performed_by)
    VALUES ('GRANT_PUBLISH', p_role, p_outbox, current_user);
END;
$$;

CREATE OR REPLACE FUNCTION tide.revoke_publish(p_role TEXT, p_outbox TEXT)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
BEGIN
    EXECUTE format(
        'REVOKE INSERT ON tide.tide_outbox_messages FROM %I',
        p_role
    );
    INSERT INTO tide.tide_security_audit (action, target_role, target_object, performed_by)
    VALUES ('REVOKE_PUBLISH', p_role, p_outbox, current_user);
END;
$$;

-- ── 3. Schema evolution guardrails ─────────────────────────────────────────

-- Stores a per-(pipeline, topic) schema fingerprint so the relay can detect
-- additive vs. breaking schema changes and apply the configured policy.
CREATE TABLE IF NOT EXISTS tide.relay_schema_fingerprints (
    pipeline_name   TEXT        NOT NULL,
    topic           TEXT        NOT NULL,
    fingerprint     TEXT        NOT NULL,   -- SHA-256 hex of sorted column list
    column_count    INT         NOT NULL DEFAULT 0,
    column_names    TEXT[]      NOT NULL DEFAULT '{}',
    on_schema_change TEXT       NOT NULL DEFAULT 'warn'
                                CHECK (on_schema_change IN
                                       ('warn', 'pause', 'dlq', 'continue')),
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_name, topic)
);

COMMENT ON TABLE tide.relay_schema_fingerprints IS
    'TIDE-SCHEMA-1 (v0.13.0): Per-pipeline schema evolution tracking.  '
    'Fingerprints detect additive vs. breaking schema changes.';

-- ── 4. DLQ: unique idempotency index ───────────────────────────────────────

-- Ensure the DLQ table has a unique index on (pipeline_name, dedup_key) so
-- concurrent DLQ inserts are safely idempotent.
CREATE UNIQUE INDEX IF NOT EXISTS uq_relay_dlq_pipeline_dedup
    ON tide.relay_dlq (pipeline_name, dedup_key)
    WHERE resolved = false;

COMMENT ON INDEX tide.uq_relay_dlq_pipeline_dedup IS
    'TIDE-DLQ-1 (v0.13.0): Unique idempotency key for active DLQ entries.';

-- ── 5. Connection limit config ─────────────────────────────────────────────

-- Store max_owned_pipelines per relay group as a simple catalog entry so the
-- coordinator can enforce connection limits at runtime.
CREATE TABLE IF NOT EXISTS tide.relay_limits (
    relay_group_id      TEXT    NOT NULL PRIMARY KEY,
    max_owned_pipelines INT     NOT NULL DEFAULT 50,
    max_connections     INT     NOT NULL DEFAULT 60,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE tide.relay_limits IS
    'TIDE-PERF-1 (v0.13.0): Per-relay-group connection and pipeline limits.';
