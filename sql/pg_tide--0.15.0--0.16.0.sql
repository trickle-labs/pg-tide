-- pg_tide 0.15.0 → 0.16.0
--
-- v0.16.0: Developer Experience & Observability
--
-- Changes:
--   1. outbox_create_if_not_exists() — idempotent outbox creation helper.
--   2. relay_set_inbox_v2() — single-JSONB parameter version with documented keys.
--      The original 8-parameter relay_set_inbox() signature is retained as a
--      compatibility shim.
--   3. Documentation comments on relay_enable() / relay_disable() clarifying the
--      intentional silent no-op when the pipeline does not exist.

-- ── 1. outbox_create_if_not_exists() ────────────────────────────────────────

-- Idempotent outbox creation: creates the outbox if it does not already exist.
-- If the outbox already exists, this function is a no-op (returns false).
-- Returns true when the outbox was created, false when it already existed.
--
-- Example usage:
--   SELECT tide.outbox_create_if_not_exists('my_outbox');
--   SELECT tide.outbox_create_if_not_exists('my_outbox', 48, 5000);
CREATE OR REPLACE FUNCTION tide.outbox_create_if_not_exists(
    p_outbox_name     TEXT,
    p_retention_hours INT  DEFAULT 24,
    p_inline_threshold INT DEFAULT 10000
)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM tide.tide_outbox_config WHERE outbox_name = p_outbox_name
    ) THEN
        RETURN false;
    END IF;

    INSERT INTO tide.tide_outbox_config (outbox_name, retention_hours, inline_threshold)
    VALUES (p_outbox_name, p_retention_hours, p_inline_threshold);

    RETURN true;
END;
$$;

COMMENT ON FUNCTION tide.outbox_create_if_not_exists(TEXT, INT, INT) IS
    'Idempotent outbox creation. Creates the outbox if it does not exist; '
    'returns true if created, false if it already existed. Added in v0.16.0.';

-- ── 2. relay_set_inbox_v2() — single-JSONB config parameter ─────────────────

-- New single-parameter form of relay_set_inbox that accepts a JSONB config
-- object with documented keys:
--
--   name        TEXT    (required) Pipeline name.
--   inbox       TEXT    (required) Target inbox name.
--   source      TEXT    (default: 'stdout') Source backend type.
--   config      JSONB   (default: {}) Source-specific configuration.
--   batch_size  INT     (default: 100)
--   enabled     BOOL    (default: true)
--   max_retries INT     (default: 3)
--   idempotent  BOOL    (default: true)
--
-- Example:
--   SELECT tide.relay_set_inbox_v2('{
--     "name": "my-pipeline",
--     "inbox": "notifications",
--     "source": "kafka",
--     "config": {"brokers": "localhost:9092", "topic": "events"},
--     "batch_size": 50
--   }');
CREATE OR REPLACE FUNCTION tide.relay_set_inbox_v2(
    p_config JSONB
)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
DECLARE
    v_name        TEXT    := p_config->>'name';
    v_inbox       TEXT    := p_config->>'inbox';
    v_source      TEXT    := COALESCE(p_config->>'source', 'stdout');
    v_src_config  JSONB   := COALESCE(p_config->'config', '{}'::jsonb);
    v_batch_size  INT     := COALESCE((p_config->>'batch_size')::INT, 100);
    v_enabled     BOOL    := COALESCE((p_config->>'enabled')::BOOL, true);
    v_max_retries INT     := COALESCE((p_config->>'max_retries')::INT, 3);
    v_idempotent  BOOL    := COALESCE((p_config->>'idempotent')::BOOL, true);
    v_full_config JSONB;
BEGIN
    IF v_name IS NULL OR v_name = '' THEN
        RAISE EXCEPTION 'relay_set_inbox_v2: config must include a non-empty "name" key';
    END IF;
    IF v_inbox IS NULL OR v_inbox = '' THEN
        RAISE EXCEPTION 'relay_set_inbox_v2: config must include a non-empty "inbox" key';
    END IF;

    v_full_config := jsonb_build_object(
        'source_type', v_source,
        'source',      v_src_config,
        'sink_type',   'inbox',
        'sink',        jsonb_build_object(
                           'inbox',       v_inbox,
                           'max_retries', v_max_retries,
                           'idempotent',  v_idempotent
                       ),
        'batch_size',  v_batch_size
    );

    INSERT INTO tide.relay_inbox_config (name, enabled, config)
    VALUES (v_name, v_enabled, v_full_config)
    ON CONFLICT (name) DO UPDATE
        SET enabled = EXCLUDED.enabled,
            config  = EXCLUDED.config;

    PERFORM pg_notify('tide_relay_config', v_name);
END;
$$;

COMMENT ON FUNCTION tide.relay_set_inbox_v2(JSONB) IS
    'Single-JSONB-parameter reverse pipeline configuration. '
    'Accepts keys: name, inbox, source, config, batch_size, enabled, '
    'max_retries, idempotent. Added in v0.16.0.';

-- ── 3. Refresh documentation on relay_enable / relay_disable ────────────────

COMMENT ON FUNCTION tide.relay_enable(TEXT) IS
    'Enable a relay pipeline. If the pipeline does not exist, this function '
    'is a silent no-op — the caller is not required to verify existence first. '
    'Sends a pg_notify(''tide_relay_config'') to wake up any listening relay '
    'instances.';

COMMENT ON FUNCTION tide.relay_disable(TEXT) IS
    'Disable a relay pipeline. If the pipeline does not exist, this function '
    'is a silent no-op — the caller is not required to verify existence first. '
    'Sends a pg_notify(''tide_relay_config'') to wake up any listening relay '
    'instances.';
