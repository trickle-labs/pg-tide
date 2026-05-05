-- pg_tide 0.8.0 → 0.9.0
-- v0.9.0: Connector Ecosystem Foundation
--   - Singer protocol adapter (STATE persistence for resumable incremental syncs,
--     SCHEMA drift detection with configurable on_schema_change policy)
--   - Airbyte protocol adapter
--   - Fivetran HVR endpoint (webhook flavor)

-- ── Singer Protocol State ──────────────────────────────────────────────────

-- Persist Singer tap STATE messages for crash-recovery and resumable
-- incremental syncs.  One row per (pipeline_name, tap_name).
CREATE TABLE tide.singer_state (
    pipeline_name   TEXT        NOT NULL,
    tap_name        TEXT        NOT NULL,
    state_value     JSONB       NOT NULL,
    written_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_name, tap_name)
);

COMMENT ON TABLE tide.singer_state IS
    'TIDE-9 (v0.9.0): Singer tap STATE checkpoints for resumable incremental syncs. '
    'DELETE a row to force a full re-sync on the next tap startup.';

-- ── Singer Schema Log ──────────────────────────────────────────────────────

-- Audit log of every Singer SCHEMA message received.
-- Used to detect schema drift (new/removed/changed properties).
CREATE TABLE tide.singer_schema_log (
    id              BIGSERIAL   PRIMARY KEY,
    pipeline_name   TEXT        NOT NULL,
    tap_name        TEXT        NOT NULL,
    stream_name     TEXT        NOT NULL,
    schema_value    JSONB       NOT NULL,
    key_properties  TEXT[]      NOT NULL DEFAULT '{}',
    logged_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE tide.singer_schema_log IS
    'TIDE-9 (v0.9.0): Audit log of Singer SCHEMA messages. Compare consecutive entries '
    'to detect schema drift (new columns, type changes).';

CREATE INDEX singer_schema_log_lookup_idx
    ON tide.singer_schema_log (pipeline_name, tap_name, stream_name, logged_at DESC);

-- ── Airbyte Protocol State ─────────────────────────────────────────────────

-- Persist Airbyte source STATE messages for crash-recovery.
-- One row per (pipeline_name, source_name).
CREATE TABLE tide.relay_airbyte_state (
    pipeline_name   TEXT        NOT NULL,
    source_name     TEXT        NOT NULL,
    state_value     JSONB       NOT NULL,
    written_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_name, source_name)
);

COMMENT ON TABLE tide.relay_airbyte_state IS
    'TIDE-9 (v0.9.0): Airbyte source STATE checkpoints for resumable syncs.';

-- ── SQL API ────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION tide.singer_state_list()
RETURNS SETOF tide.singer_state
LANGUAGE sql STABLE SECURITY INVOKER
AS $$
    SELECT * FROM tide.singer_state ORDER BY written_at DESC;
$$;

COMMENT ON FUNCTION tide.singer_state_list() IS
    'List all Singer tap STATE checkpoints ordered by most recently written.';

CREATE OR REPLACE FUNCTION tide.singer_schema_drift(
    p_pipeline_name TEXT,
    p_tap_name      TEXT,
    p_stream_name   TEXT
)
RETURNS TABLE (
    property    TEXT,
    change_type TEXT,
    old_type    TEXT,
    new_type    TEXT,
    detected_at TIMESTAMPTZ
)
LANGUAGE sql STABLE SECURITY INVOKER
AS $$
    WITH ranked AS (
        SELECT schema_value,
               logged_at,
               ROW_NUMBER() OVER (ORDER BY logged_at DESC) AS rn
          FROM tide.singer_schema_log
         WHERE pipeline_name = p_pipeline_name
           AND tap_name      = p_tap_name
           AND stream_name   = p_stream_name
    ),
    latest  AS (SELECT schema_value FROM ranked WHERE rn = 1),
    previous AS (SELECT schema_value FROM ranked WHERE rn = 2)
    SELECT
        key                                             AS property,
        CASE
            WHEN prev_props->key IS NULL THEN 'added'
            WHEN curr_props->key IS NULL THEN 'removed'
            ELSE 'changed'
        END                                             AS change_type,
        CASE WHEN prev_props->key IS NOT NULL
             THEN (prev_props->key->>'type') END        AS old_type,
        CASE WHEN curr_props->key IS NOT NULL
             THEN (curr_props->key->>'type') END        AS new_type,
        (SELECT logged_at FROM ranked WHERE rn = 1)     AS detected_at
    FROM
        (SELECT (latest.schema_value->'properties') AS curr_props,
                (previous.schema_value->'properties') AS prev_props
           FROM latest, previous) AS schemas,
        jsonb_object_keys(
            COALESCE(schemas.curr_props, '{}') ||
            COALESCE(schemas.prev_props, '{}')
        ) AS key
    WHERE
        (schemas.curr_props->key) IS DISTINCT FROM (schemas.prev_props->key)
    ;
$$;

COMMENT ON FUNCTION tide.singer_schema_drift(TEXT, TEXT, TEXT) IS
    'Return properties that changed between the two most recent Singer SCHEMA messages '
    'for the given (pipeline, tap, stream).';
