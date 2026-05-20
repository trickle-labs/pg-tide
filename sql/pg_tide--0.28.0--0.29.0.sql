-- pg_tide 0.28.0 → 0.29.0
--
-- v0.29.0: Pipeline Templates, Multi-Outbox Fan-In, Lifecycle Management
--          & Backfill Completion
--
-- Changes:
--   1. tide.relay_pipeline_templates — built-in template library catalog table
--   2. tide.relay_template_create/drop/validate — template CRUD functions
--   3. tide.relay_set_outbox_from_template / relay_set_inbox_from_template
--   4. Built-in templates: kafka-topic-mirror, ducklake-daily-sink,
--      nats-jetstream-fanout, pg-inbox-relay, webhook-notification
--   5. tide.relay_fanin_config — multi-outbox fan-in pipeline catalog table
--   6. tide.relay_set_fanin() — fan-in pipeline registration
--   7. fanin_member column on tide.relay_consumer_offsets
--   8. tide.relay_config_audit — config change history table + trigger
--   9. tide.relay_pipeline_state — pipeline pause/resume state table
--  10. auto_resume_after INTERVAL on tide_outbox_config and tide_inbox_config
--  11. tide.relay_config_history() — view for pipeline config diffs
--  12. tide.relay_pipeline_pause_reason() — pause reason query function
--  13. tide.backfill_progress() — progress API for backfill jobs
--  14. tide.backfill_cancel() — cancel a backfill job

-- ── 1. Pipeline template catalog table ───────────────────────────────────────

CREATE TABLE IF NOT EXISTS tide.relay_pipeline_templates (
    name            TEXT        NOT NULL PRIMARY KEY,
    config          JSONB       NOT NULL,
    description     TEXT        NOT NULL DEFAULT '',
    required_keys   JSONB       NOT NULL DEFAULT '[]',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE tide.relay_pipeline_templates IS
    'TIDE-TEMPLATE-1 (v0.29.0): Built-in and user-defined pipeline templates with '
    'placeholder substitution. Templates have {{key}} placeholders in their config '
    'JSON that must be supplied by the caller when instantiating a pipeline.';

CREATE INDEX IF NOT EXISTS idx_relay_pipeline_templates_name
    ON tide.relay_pipeline_templates (name);

-- ── 2. Template CRUD functions ────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION tide.relay_template_create(
    p_name        TEXT,
    p_config      JSONB,
    p_description TEXT        DEFAULT '',
    p_required_keys JSONB     DEFAULT '[]'
)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
BEGIN
    IF p_name IS NULL OR trim(p_name) = '' THEN
        RAISE EXCEPTION 'template name must not be empty';
    END IF;
    IF p_config IS NULL THEN
        RAISE EXCEPTION 'template config must not be NULL';
    END IF;

    INSERT INTO tide.relay_pipeline_templates (name, config, description, required_keys)
    VALUES (p_name, p_config, COALESCE(p_description, ''), COALESCE(p_required_keys, '[]'))
    ON CONFLICT (name) DO UPDATE
        SET config       = EXCLUDED.config,
            description  = EXCLUDED.description,
            required_keys = EXCLUDED.required_keys,
            updated_at   = now();
END;
$$;

CREATE OR REPLACE FUNCTION tide.relay_template_drop(
    p_name TEXT
)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
BEGIN
    DELETE FROM tide.relay_pipeline_templates WHERE name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'template ''%'' not found', p_name;
    END IF;
END;
$$;

-- ── 3. Template validation ────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION tide.relay_template_validate(
    p_name      TEXT,
    p_overrides JSONB DEFAULT '{}'
)
RETURNS TABLE (
    is_valid        BOOLEAN,
    missing_keys    TEXT[],
    invalid_keys    TEXT[]
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
DECLARE
    v_required_keys JSONB;
    v_missing       TEXT[] := ARRAY[]::TEXT[];
    v_key           TEXT;
BEGIN
    SELECT required_keys INTO v_required_keys
    FROM tide.relay_pipeline_templates
    WHERE name = p_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'template ''%'' not found', p_name;
    END IF;

    -- Check each required key is present in overrides.
    FOR v_key IN SELECT jsonb_array_elements_text(v_required_keys) LOOP
        IF NOT (p_overrides ? v_key) OR (p_overrides ->> v_key) IS NULL OR trim(p_overrides ->> v_key) = '' THEN
            v_missing := array_append(v_missing, v_key);
        END IF;
    END LOOP;

    RETURN QUERY SELECT
        (array_length(v_missing, 1) IS NULL OR array_length(v_missing, 1) = 0),
        v_missing,
        ARRAY[]::TEXT[];
END;
$$;

-- ── 3b. Template instantiation helpers ───────────────────────────────────────

CREATE OR REPLACE FUNCTION tide.relay_set_outbox_from_template(
    p_outbox_name   TEXT,
    p_template_name TEXT,
    p_overrides     JSONB DEFAULT '{}'
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
DECLARE
    v_template_config   JSONB;
    v_resolved_config   JSONB;
    v_config_text       TEXT;
    v_key               TEXT;
    v_val               TEXT;
    v_required_keys     JSONB;
    v_missing           TEXT[];
BEGIN
    SELECT config, required_keys
    INTO v_template_config, v_required_keys
    FROM tide.relay_pipeline_templates
    WHERE name = p_template_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'template ''%'' not found', p_template_name;
    END IF;

    -- Check required keys.
    v_missing := ARRAY[]::TEXT[];
    FOR v_key IN SELECT jsonb_array_elements_text(v_required_keys) LOOP
        IF NOT (p_overrides ? v_key) OR trim(p_overrides ->> v_key) = '' THEN
            v_missing := array_append(v_missing, v_key);
        END IF;
    END LOOP;
    IF array_length(v_missing, 1) > 0 THEN
        RAISE EXCEPTION 'template ''%'' requires keys: %', p_template_name, array_to_string(v_missing, ', ');
    END IF;

    -- Merge template config with overrides (overrides win on top-level keys).
    v_resolved_config := v_template_config || p_overrides;

    -- Substitute {{key}} placeholders from overrides.
    v_config_text := v_resolved_config::TEXT;
    FOR v_key, v_val IN SELECT key, value::TEXT FROM jsonb_each_text(p_overrides) LOOP
        v_config_text := replace(v_config_text, '{{' || v_key || '}}', v_val);
    END LOOP;
    -- Also substitute outbox_name.
    v_config_text := replace(v_config_text, '{{outbox_name}}', p_outbox_name);
    v_resolved_config := v_config_text::JSONB;

    -- Set the pipeline name if not already set.
    IF NOT (v_resolved_config ? 'name') THEN
        v_resolved_config := jsonb_set(v_resolved_config, '{name}', to_jsonb(p_outbox_name || '-pipeline'));
    END IF;

    -- Delegate to relay_set_outbox_v2.
    PERFORM tide.relay_set_outbox_v2(v_resolved_config);
    RETURN v_resolved_config;
END;
$$;

CREATE OR REPLACE FUNCTION tide.relay_set_inbox_from_template(
    p_inbox_name    TEXT,
    p_template_name TEXT,
    p_overrides     JSONB DEFAULT '{}'
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
DECLARE
    v_template_config   JSONB;
    v_resolved_config   JSONB;
    v_config_text       TEXT;
    v_key               TEXT;
    v_val               TEXT;
    v_required_keys     JSONB;
    v_missing           TEXT[];
BEGIN
    SELECT config, required_keys
    INTO v_template_config, v_required_keys
    FROM tide.relay_pipeline_templates
    WHERE name = p_template_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'template ''%'' not found', p_template_name;
    END IF;

    -- Check required keys.
    v_missing := ARRAY[]::TEXT[];
    FOR v_key IN SELECT jsonb_array_elements_text(v_required_keys) LOOP
        IF NOT (p_overrides ? v_key) OR trim(p_overrides ->> v_key) = '' THEN
            v_missing := array_append(v_missing, v_key);
        END IF;
    END LOOP;
    IF array_length(v_missing, 1) > 0 THEN
        RAISE EXCEPTION 'template ''%'' requires keys: %', p_template_name, array_to_string(v_missing, ', ');
    END IF;

    -- Merge and substitute.
    v_resolved_config := v_template_config || p_overrides;
    v_config_text := v_resolved_config::TEXT;
    FOR v_key, v_val IN SELECT key, value::TEXT FROM jsonb_each_text(p_overrides) LOOP
        v_config_text := replace(v_config_text, '{{' || v_key || '}}', v_val);
    END LOOP;
    v_config_text := replace(v_config_text, '{{inbox_name}}', p_inbox_name);
    v_resolved_config := v_config_text::JSONB;

    IF NOT (v_resolved_config ? 'name') THEN
        v_resolved_config := jsonb_set(v_resolved_config, '{name}', to_jsonb(p_inbox_name || '-pipeline'));
    END IF;

    PERFORM tide.relay_set_inbox_v2(v_resolved_config);
    RETURN v_resolved_config;
END;
$$;

-- ── 4. Built-in templates ─────────────────────────────────────────────────────

INSERT INTO tide.relay_pipeline_templates (name, description, required_keys, config)
VALUES (
    'kafka-topic-mirror',
    'Forward messages from a pg_tide outbox to an Apache Kafka topic.',
    '["outbox_name", "kafka_bootstrap_servers", "kafka_topic"]',
    '{
        "name": "{{outbox_name}}-kafka",
        "source_type": "outbox",
        "source": {"outbox": "{{outbox_name}}"},
        "sink_type": "kafka",
        "sink": {
            "bootstrap_servers": "{{kafka_bootstrap_servers}}",
            "topic": "{{kafka_topic}}"
        }
    }'
)
ON CONFLICT (name) DO NOTHING;

INSERT INTO tide.relay_pipeline_templates (name, description, required_keys, config)
VALUES (
    'ducklake-daily-sink',
    'Sink outbox messages into a DuckLake table with daily partitioning.',
    '["outbox_name", "ducklake_catalog", "ducklake_table"]',
    '{
        "name": "{{outbox_name}}-ducklake",
        "source_type": "outbox",
        "source": {"outbox": "{{outbox_name}}"},
        "sink_type": "ducklake",
        "sink": {
            "catalog": "{{ducklake_catalog}}",
            "table": "{{ducklake_table}}",
            "partition_by": "day"
        }
    }'
)
ON CONFLICT (name) DO NOTHING;

INSERT INTO tide.relay_pipeline_templates (name, description, required_keys, config)
VALUES (
    'nats-jetstream-fanout',
    'Fan out outbox messages to a NATS JetStream subject.',
    '["outbox_name", "nats_url", "nats_subject"]',
    '{
        "name": "{{outbox_name}}-nats",
        "source_type": "outbox",
        "source": {"outbox": "{{outbox_name}}"},
        "sink_type": "nats",
        "sink": {
            "url": "{{nats_url}}",
            "subject": "{{nats_subject}}"
        }
    }'
)
ON CONFLICT (name) DO NOTHING;

INSERT INTO tide.relay_pipeline_templates (name, description, required_keys, config)
VALUES (
    'pg-inbox-relay',
    'Relay messages from one pg_tide outbox into another PostgreSQL inbox.',
    '["outbox_name", "target_postgres_url", "target_inbox_name"]',
    '{
        "name": "{{outbox_name}}-inbox-relay",
        "source_type": "outbox",
        "source": {"outbox": "{{outbox_name}}"},
        "sink_type": "pg_inbox",
        "sink": {
            "postgres_url": "{{target_postgres_url}}",
            "inbox": "{{target_inbox_name}}"
        }
    }'
)
ON CONFLICT (name) DO NOTHING;

INSERT INTO tide.relay_pipeline_templates (name, description, required_keys, config)
VALUES (
    'webhook-notification',
    'Send outbox messages to an HTTP webhook endpoint with HMAC signing.',
    '["outbox_name", "webhook_url", "hmac_secret"]',
    '{
        "name": "{{outbox_name}}-webhook",
        "source_type": "outbox",
        "source": {"outbox": "{{outbox_name}}"},
        "sink_type": "webhook",
        "sink": {
            "url": "{{webhook_url}}",
            "signing_secret": "{{hmac_secret}}",
            "signing_algorithm": "hmac-sha256"
        }
    }'
)
ON CONFLICT (name) DO NOTHING;

-- ── 5. Multi-outbox fan-in config catalog table ───────────────────────────────

CREATE TABLE IF NOT EXISTS tide.relay_fanin_config (
    name            TEXT        NOT NULL PRIMARY KEY,
    outbox_names    TEXT[]      NOT NULL,
    sink_type       TEXT        NOT NULL,
    config          JSONB       NOT NULL DEFAULT '{}',
    merge_strategy  TEXT        NOT NULL DEFAULT 'round_robin'
                                CHECK (merge_strategy IN ('round_robin', 'priority', 'subject_hash')),
    enabled         BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    tenant_name     TEXT        NOT NULL DEFAULT ''
);

COMMENT ON TABLE tide.relay_fanin_config IS
    'TIDE-FANIN-1 (v0.29.0): Multi-outbox fan-in pipeline configurations. '
    'Each fan-in pipeline combines messages from multiple outboxes into a single sink.';

-- ── 6. relay_set_fanin() ─────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION tide.relay_set_fanin(
    p_name          TEXT,
    p_outbox_names  TEXT[],
    p_sink_type     TEXT,
    p_config        JSONB DEFAULT '{}'
)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
DECLARE
    v_outbox TEXT;
BEGIN
    -- Validate name.
    IF p_name IS NULL OR trim(p_name) = '' THEN
        RAISE EXCEPTION 'fan-in pipeline name must not be empty';
    END IF;

    -- Validate outbox_names not empty.
    IF p_outbox_names IS NULL OR array_length(p_outbox_names, 1) < 1 THEN
        RAISE EXCEPTION 'fan-in pipeline ''%'' requires at least one outbox', p_name;
    END IF;

    -- Validate all named outboxes exist.
    FOREACH v_outbox IN ARRAY p_outbox_names LOOP
        IF NOT EXISTS (SELECT 1 FROM tide.tide_outbox_config WHERE outbox_name = v_outbox) THEN
            RAISE EXCEPTION 'outbox ''%'' does not exist (required by fan-in ''%'')', v_outbox, p_name;
        END IF;
    END LOOP;

    INSERT INTO tide.relay_fanin_config (name, outbox_names, sink_type, config)
    VALUES (p_name, p_outbox_names, p_sink_type, COALESCE(p_config, '{}'))
    ON CONFLICT (name) DO UPDATE
        SET outbox_names = EXCLUDED.outbox_names,
            sink_type    = EXCLUDED.sink_type,
            config       = EXCLUDED.config,
            updated_at   = now();
END;
$$;

-- ── 7. fanin_member column on relay_consumer_offsets ─────────────────────────

ALTER TABLE tide.relay_consumer_offsets
    ADD COLUMN IF NOT EXISTS fanin_member TEXT;

COMMENT ON COLUMN tide.relay_consumer_offsets.fanin_member IS
    'TIDE-FANIN-2 (v0.29.0): For fan-in pipelines, identifies which source outbox '
    'this offset row tracks. NULL for regular (non-fan-in) pipelines.';

-- ── 8. Config audit table + trigger ──────────────────────────────────────────

CREATE TABLE IF NOT EXISTS tide.relay_config_audit (
    id              BIGSERIAL   NOT NULL PRIMARY KEY,
    pipeline_name   TEXT        NOT NULL,
    pipeline_type   TEXT        NOT NULL DEFAULT 'outbox'
                                CHECK (pipeline_type IN ('outbox', 'inbox', 'fanin')),
    changed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    changed_by      TEXT        NOT NULL DEFAULT current_user,
    old_config      JSONB,
    new_config      JSONB
);

CREATE INDEX IF NOT EXISTS idx_relay_config_audit_pipeline
    ON tide.relay_config_audit (pipeline_name, changed_at DESC);

COMMENT ON TABLE tide.relay_config_audit IS
    'TIDE-LIFECYCLE-1 (v0.29.0): Immutable audit log of pipeline configuration changes. '
    'Populated by triggers on relay_outbox_config and relay_inbox_config.';

-- Trigger function for outbox config changes.
CREATE OR REPLACE FUNCTION tide.relay_config_audit_outbox()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        INSERT INTO tide.relay_config_audit (pipeline_name, pipeline_type, changed_by, old_config, new_config)
        VALUES (NEW.name, 'outbox', current_user, OLD.config, NEW.config);
    ELSIF TG_OP = 'INSERT' THEN
        INSERT INTO tide.relay_config_audit (pipeline_name, pipeline_type, changed_by, old_config, new_config)
        VALUES (NEW.name, 'outbox', current_user, NULL, NEW.config);
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER trg_relay_outbox_config_audit
    AFTER INSERT OR UPDATE ON tide.relay_outbox_config
    FOR EACH ROW EXECUTE FUNCTION tide.relay_config_audit_outbox();

-- Trigger function for inbox config changes.
CREATE OR REPLACE FUNCTION tide.relay_config_audit_inbox()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        INSERT INTO tide.relay_config_audit (pipeline_name, pipeline_type, changed_by, old_config, new_config)
        VALUES (NEW.name, 'inbox', current_user, OLD.config, NEW.config);
    ELSIF TG_OP = 'INSERT' THEN
        INSERT INTO tide.relay_config_audit (pipeline_name, pipeline_type, changed_by, old_config, new_config)
        VALUES (NEW.name, 'inbox', current_user, NULL, NEW.config);
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER trg_relay_inbox_config_audit
    AFTER INSERT OR UPDATE ON tide.relay_inbox_config
    FOR EACH ROW EXECUTE FUNCTION tide.relay_config_audit_inbox();

-- ── 9. Pipeline state table ───────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS tide.relay_pipeline_state (
    name                TEXT        NOT NULL PRIMARY KEY,
    last_error          TEXT,
    error_class         TEXT        CHECK (error_class IN ('transient', 'permanent', NULL)),
    pause_started_at    TIMESTAMPTZ,
    failure_count       INT         NOT NULL DEFAULT 0,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE tide.relay_pipeline_state IS
    'TIDE-LIFECYCLE-2 (v0.29.0): Runtime state for pipeline pause/resume tracking. '
    'Written by the relay coordinator on every worker state transition.';

-- ── 10. auto_resume_after on config tables ────────────────────────────────────

ALTER TABLE tide.tide_outbox_config
    ADD COLUMN IF NOT EXISTS auto_resume_after INTERVAL;

ALTER TABLE tide.tide_inbox_config
    ADD COLUMN IF NOT EXISTS auto_resume_after INTERVAL;

COMMENT ON COLUMN tide.tide_outbox_config.auto_resume_after IS
    'TIDE-LIFECYCLE-3 (v0.29.0): When set, a paused outbox pipeline is automatically '
    're-enabled by the coordinator after this interval elapses since pause_started_at.';

COMMENT ON COLUMN tide.tide_inbox_config.auto_resume_after IS
    'TIDE-LIFECYCLE-3 (v0.29.0): When set, a paused inbox pipeline is automatically '
    're-enabled by the coordinator after this interval elapses since pause_started_at.';

-- ── 11. relay_config_history() view ──────────────────────────────────────────

CREATE OR REPLACE FUNCTION tide.relay_config_history(
    p_pipeline_name TEXT
)
RETURNS TABLE (
    change_id       BIGINT,
    pipeline_name   TEXT,
    pipeline_type   TEXT,
    changed_at      TIMESTAMPTZ,
    changed_by      TEXT,
    old_config      JSONB,
    new_config      JSONB
)
LANGUAGE SQL
STABLE
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
    SELECT id, pipeline_name, pipeline_type, changed_at, changed_by, old_config, new_config
    FROM tide.relay_config_audit
    WHERE pipeline_name = p_pipeline_name
    ORDER BY changed_at DESC;
$$;

-- ── 12. relay_pipeline_pause_reason() ────────────────────────────────────────

CREATE OR REPLACE FUNCTION tide.relay_pipeline_pause_reason(
    p_pipeline_name TEXT
)
RETURNS TABLE (
    pipeline_name       TEXT,
    last_error          TEXT,
    error_class         TEXT,
    pause_started_at    TIMESTAMPTZ,
    failure_count       INT
)
LANGUAGE SQL
STABLE
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
    SELECT name, last_error, error_class, pause_started_at, failure_count
    FROM tide.relay_pipeline_state
    WHERE name = p_pipeline_name;
$$;

-- ── 13. backfill_progress() ───────────────────────────────────────────────────

-- NOTE: backfill_progress() is implemented as a SQL function here because
-- the pgrx Rust functions use TEXT-based job_name keys (not UUIDs).
-- This function wraps the existing backfill_jobs table.

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
            WHEN rows_processed > 0
              AND started_at IS NOT NULL
              AND COALESCE(rows_total, 0) > rows_processed
            THEN now() + (
                (COALESCE(rows_total, 0) - rows_processed)::NUMERIC
                / NULLIF(rows_processed, 0)
                * EXTRACT(EPOCH FROM (now() - started_at))
                * INTERVAL '1 second'
            )
            ELSE NULL
        END AS estimated_completion,
        status
    FROM tide.backfill_jobs
    WHERE job_name = p_job_name;
$$;

-- ── 14. backfill_cancel() ─────────────────────────────────────────────────────

-- NOTE: backfill_cancel() is a SQL function; pgrx-based functions for pause,
-- resume, and status already exist.  Cancel sets status to 'failed' with an
-- explicit cancellation message and cannot be resumed.

CREATE OR REPLACE FUNCTION tide.backfill_cancel(
    p_job_name TEXT
)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
DECLARE
    v_updated INT;
BEGIN
    UPDATE tide.backfill_jobs
    SET status        = 'failed',
        error_message = 'cancelled by operator',
        completed_at  = now()
    WHERE job_name = p_job_name
      AND status IN ('pending', 'running', 'paused');

    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated = 0 THEN
        RAISE EXCEPTION 'backfill job ''%'' not found or already completed/failed', p_job_name;
    END IF;
END;
$$;
