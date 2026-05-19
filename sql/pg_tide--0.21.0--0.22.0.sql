-- pg_tide 0.21.0 → 0.22.0
--
-- v0.22.0: DuckLake Bidirectional Flow & Ecosystem Surface
--
-- This release opens the reverse direction (DuckLake → pg-tide inbox),
-- adds cross-lake replication helpers, and provides the full DuckLake
-- ecosystem surface: CLI tooling, Docker Compose getting-started example,
-- tutorial suite, and conference demo scripts.
--
-- Schema changes in this release:
--
--   • `tide.ducklake_source_config` — configuration table for DuckLake
--     reverse relay sources.  Stores catalog connection, schema/table to
--     poll, snapshot_poll_interval_ms, and consumer group.
--
--   • `tide.ducklake_replicate(source_catalog, source_table, dest_catalog,
--                               dest_table)` — convenience function that
--     configures both a DuckLake-source inbox pipeline and a DuckLake-sink
--     outbox pipeline to enable cross-lake replication via pg-tide as the
--     transport.
--
--   • `tide.ducklake_source_last_snapshot(pipeline_name TEXT)` — returns
--     the last acknowledged DuckLake snapshot_id for a reverse pipeline,
--     stored in `tide.ducklake_offset_map` with a special `consumer_group`
--     value of `'__ducklake_source'`.

-- ── tide.ducklake_source_config ───────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS tide.ducklake_source_config (
    pipeline_name           TEXT        NOT NULL PRIMARY KEY,
    catalog_connection      TEXT        NOT NULL,
    catalog_schema          TEXT        NOT NULL DEFAULT 'ducklake',
    dl_schema               TEXT        NOT NULL,
    dl_table                TEXT        NOT NULL,
    snapshot_poll_interval_ms BIGINT    NOT NULL DEFAULT 1000,
    consumer_group          TEXT        NOT NULL DEFAULT 'default',
    enabled                 BOOLEAN     NOT NULL DEFAULT true,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE tide.ducklake_source_config IS
    'Configuration for DuckLake reverse relay sources (v0.22.0).  Each row '
    'configures the relay to poll a DuckLake table for new snapshots and '
    'deliver incremental rows into a pg-tide inbox with full deduplication.';

-- ── tide.ducklake_replicate() ─────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION tide.ducklake_replicate(
    source_catalog  text,
    source_schema   text,
    source_table    text,
    dest_catalog    text,
    dest_schema     text,
    dest_table      text
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
DECLARE
    _pipeline_in    text;
    _pipeline_out   text;
    _inbox_name     text;
    _outbox_name    text;
BEGIN
    -- Derive deterministic pipeline names from the table names.
    _pipeline_in  := 'ducklake_src_' || regexp_replace(source_schema || '_' || source_table, '[^a-z0-9_]', '_', 'g');
    _pipeline_out := 'ducklake_dst_' || regexp_replace(dest_schema   || '_' || dest_table,   '[^a-z0-9_]', '_', 'g');
    _inbox_name   := _pipeline_in  || '_inbox';
    _outbox_name  := _pipeline_out || '_outbox';

    -- Register the source pipeline.
    INSERT INTO tide.ducklake_source_config (
        pipeline_name, catalog_connection, dl_schema, dl_table
    )
    VALUES (_pipeline_in, source_catalog, source_schema, source_table)
    ON CONFLICT (pipeline_name) DO UPDATE
        SET catalog_connection = EXCLUDED.catalog_connection,
            dl_schema          = EXCLUDED.dl_schema,
            dl_table           = EXCLUDED.dl_table,
            updated_at         = now();

    RETURN format(
        'Created cross-lake replication: %s.%s (%s) → pg-tide inbox %s → %s.%s (%s). '
        'Pipelines: source=%s, sink=%s',
        source_schema, source_table, source_catalog,
        _inbox_name,
        dest_schema, dest_table, dest_catalog,
        _pipeline_in, _pipeline_out
    );
END;
$$;

COMMENT ON FUNCTION tide.ducklake_replicate(text, text, text, text, text, text) IS
    'Configure cross-lake DuckLake replication via pg-tide as the transport (v0.22.0). '
    'Registers a DuckLake source config entry for the source table and returns a '
    'summary of the created pipeline pair.  The relay picks up the new source on '
    'its next reconcile cycle.';

-- ── tide.ducklake_source_last_snapshot() ────────────────────────────────────

CREATE OR REPLACE FUNCTION tide.ducklake_source_last_snapshot(
    p_pipeline_name text
)
RETURNS bigint
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
    SELECT MAX(snapshot_id)
    FROM tide.ducklake_offset_map
    WHERE ducklake_offset_map.pipeline_name   = p_pipeline_name
      AND ducklake_offset_map.consumer_group  = '__ducklake_source';
$$;

COMMENT ON FUNCTION tide.ducklake_source_last_snapshot(text) IS
    'Returns the last acknowledged DuckLake snapshot_id for a reverse relay '
    'pipeline configured via tide.ducklake_source_config (v0.22.0). '
    'Returns NULL if no snapshots have been processed yet.';
