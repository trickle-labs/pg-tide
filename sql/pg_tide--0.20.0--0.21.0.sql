-- pg_tide 0.20.0 → 0.21.0
--
-- v0.21.0: DuckLake Streaming, Inlining & Schema Evolution
--
-- This release adds data inlining for sub-threshold batches, automatic schema
-- evolution bridging for DuckLake tables, snapshot-to-consumer-offset mapping
-- (enabling DuckLake time-travel replay), auto-partition configuration, and a
-- DLQ archive sink that moves aged DLQ entries to a DuckLake table.
--
-- Schema changes in this release:
--
--   • `tide.ducklake_offset_map` — records the mapping from pg-tide consumer
--     group offset to DuckLake snapshot ID, written atomically with each
--     snapshot commit.  Enables SQL time-travel replay via DuckDB
--     `AT (VERSION => snapshot_id)`.
--
--   • `tide.ducklake_partition_config` — stores partition strategy per
--     pipeline / table (daily, monthly, bucket:N, none).  Written by the relay
--     sink when auto-partition is configured.
--
--   • `tide.ducklake_replay_range(pipeline_name text, from_offset bigint,
--                                  to_offset bigint)` → TEXT
--     Returns the DuckDB `AT (VERSION => …)` range expression for the given
--     consumer-group offset range, ready to paste into a DuckDB session.
--
--   • `tide.ducklake_column_history(pipeline_name text)` — SQL view listing
--     every `ducklake_column` entry associated with a given pipeline's DuckLake
--     table, together with the snapshot at which each column was added.  Use
--     this to track schema evolution over time.

-- ── tide.ducklake_offset_map ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS tide.ducklake_offset_map (
    pipeline_name   TEXT        NOT NULL,
    consumer_group  TEXT        NOT NULL,
    outbox_offset   BIGINT      NOT NULL,
    snapshot_id     BIGINT      NOT NULL,
    committed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_name, consumer_group, outbox_offset)
);

COMMENT ON TABLE tide.ducklake_offset_map IS
    'Maps pg-tide consumer-group offsets to DuckLake snapshot IDs so that '
    'subscribers can use DuckDB time-travel to replay events by offset range.';

-- ── tide.ducklake_partition_config ──────────────────────────────────────────

CREATE TABLE IF NOT EXISTS tide.ducklake_partition_config (
    pipeline_name   TEXT NOT NULL,
    catalog_schema  TEXT NOT NULL DEFAULT 'ducklake',
    namespace       TEXT NOT NULL,
    table_name      TEXT NOT NULL,
    partition_type  TEXT NOT NULL DEFAULT 'none',   -- 'daily' | 'monthly' | 'bucket:N' | 'none'
    partition_col   TEXT NOT NULL DEFAULT '_committed_at',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_name, namespace, table_name)
);

COMMENT ON TABLE tide.ducklake_partition_config IS
    'Stores the partition strategy chosen for each DuckLake table written by '
    'the pg-tide relay.  Written by the relay sink on first batch when '
    'ducklake_partition is set in the pipeline config.';

-- ── tide.ducklake_replay_range() ─────────────────────────────────────────────

CREATE OR REPLACE FUNCTION tide.ducklake_replay_range(
    pipeline_name  text,
    from_offset    bigint,
    to_offset      bigint
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
DECLARE
    _pipeline  text    := pipeline_name;
    _from      bigint  := from_offset;
    _to        bigint  := to_offset;
    _from_snap BIGINT;
    _to_snap   BIGINT;
BEGIN
    -- Look up the snapshot that corresponds to (or immediately follows)
    -- from_offset and to_offset for this pipeline.
    SELECT m.snapshot_id INTO _from_snap
    FROM tide.ducklake_offset_map m
    WHERE m.pipeline_name   = _pipeline
      AND m.outbox_offset  >= _from
    ORDER BY m.outbox_offset ASC
    LIMIT 1;

    SELECT m.snapshot_id INTO _to_snap
    FROM tide.ducklake_offset_map m
    WHERE m.pipeline_name   = _pipeline
      AND m.outbox_offset  <= _to
    ORDER BY m.outbox_offset DESC
    LIMIT 1;

    IF _from_snap IS NULL OR _to_snap IS NULL THEN
        RETURN NULL;
    END IF;

    RETURN format('AT (VERSION => %s) .. AT (VERSION => %s)', _from_snap, _to_snap);
END;
$$;

COMMENT ON FUNCTION tide.ducklake_replay_range(text, bigint, bigint) IS
    'Returns a DuckDB AT (VERSION => …) range expression for the given '
    'consumer-group offset range.  Paste the result into a DuckDB session '
    'to replay events between two pg-tide consumer offsets.  Returns NULL '
    'if the offset range has not been indexed in ducklake_offset_map.';

-- ── tide.ducklake_column_history() ───────────────────────────────────────────

CREATE OR REPLACE FUNCTION tide.ducklake_column_history(pipeline_name text)
RETURNS TABLE (
    namespace    text,
    table_nm     text,
    column_nm    text,
    column_type  text,
    column_order integer,
    nullable     boolean,
    snapshot_id  bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
DECLARE
    _pipeline text := pipeline_name;
    _rec RECORD;
BEGIN
    -- Iterate over every partition_config entry for this pipeline to discover
    -- which (catalog_schema, namespace, table_name) tuples are relevant, then
    -- join against the DuckLake catalog tables living in that schema.
    FOR _rec IN
        SELECT p.catalog_schema, p.namespace, p.table_name
        FROM   tide.ducklake_partition_config p
        WHERE  p.pipeline_name = _pipeline
    LOOP
        BEGIN
            RETURN QUERY EXECUTE format(
                $q$
                SELECT
                    s.schema_name   AS namespace,
                    t.table_name    AS table_nm,
                    c.column_name   AS column_nm,
                    c.column_type   AS column_type,
                    c.column_order  AS column_order,
                    c.nullable      AS nullable,
                    COALESCE(
                        (SELECT MIN(snap.snapshot_id)
                         FROM %1$I.ducklake_snapshot snap
                         WHERE snap.table_id = c.table_id),
                        0
                    ) AS snapshot_id
                FROM %1$I.ducklake_column c
                JOIN %1$I.ducklake_table  t ON t.table_id   = c.table_id
                JOIN %1$I.ducklake_schema s ON s.schema_id  = t.schema_id
                WHERE s.schema_name = %2$L
                  AND t.table_name  = %3$L
                ORDER BY c.column_order, c.column_id
                $q$,
                _rec.catalog_schema,
                _rec.namespace,
                _rec.table_name
            );
        EXCEPTION WHEN undefined_table THEN
            -- The DuckLake catalog tables don't exist yet — skip this entry.
            NULL;
        END;
    END LOOP;
END;
$$;

COMMENT ON FUNCTION tide.ducklake_column_history(text) IS
    'Returns every ducklake_column entry for the DuckLake tables written by '
    'the given pipeline, together with the earliest snapshot ID at which each '
    'column appears.  Use this to track schema evolution over time.';
