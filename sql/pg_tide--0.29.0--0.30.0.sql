-- pg_tide 0.29.0 → 0.30.0
--
-- v0.30.0: Pipeline Dependency DAG, AsyncAPI Completeness & Pre-GA Final Hardening
--
-- Changes:
--   1. tide.relay_pipeline_deps — DAG edge catalog table
--   2. tide.relay_pipeline_dep_add() — add/update a DAG edge with cycle detection
--   3. tide.relay_pipeline_dep_drop() — remove a DAG edge
--   4. tide.relay_dag_check() — recursive CTE cycle detection function

-- ── 1. Pipeline dependency DAG table ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS tide.relay_pipeline_deps (
    upstream_pipeline   TEXT        NOT NULL,
    downstream_pipeline TEXT        NOT NULL,
    trigger_policy      TEXT        NOT NULL DEFAULT 'always',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (upstream_pipeline, downstream_pipeline)
);

COMMENT ON TABLE tide.relay_pipeline_deps IS
    'TIDE-DAG-1 (v0.30.0): Pipeline dependency edges for DAG-aware coordinator '
    'acquisition. trigger_policy values: always, on_idle, on_offset_gte(N).';

CREATE INDEX IF NOT EXISTS idx_relay_pipeline_deps_downstream
    ON tide.relay_pipeline_deps (downstream_pipeline);

-- ── 2. relay_pipeline_dep_add() ──────────────────────────────────────────────

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
    IF p_trigger_policy NOT IN ('always', 'on_idle')
       AND p_trigger_policy NOT LIKE 'on_offset_gte(%)'
    THEN
        RAISE EXCEPTION 'unknown trigger_policy ''%''; expected always, on_idle, or on_offset_gte(N)', p_trigger_policy;
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

-- ── 3. relay_pipeline_dep_drop() ─────────────────────────────────────────────

CREATE OR REPLACE FUNCTION tide.relay_pipeline_dep_drop(
    p_upstream_pipeline   TEXT,
    p_downstream_pipeline TEXT
)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
BEGIN
    DELETE FROM tide.relay_pipeline_deps
    WHERE upstream_pipeline  = p_upstream_pipeline
      AND downstream_pipeline = p_downstream_pipeline;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'dependency edge ''%'' → ''%'' not found',
            p_upstream_pipeline, p_downstream_pipeline;
    END IF;
END;
$$;

-- ── 4. relay_dag_check() — recursive CTE cycle detection ────────────────────

CREATE OR REPLACE FUNCTION tide.relay_dag_check()
RETURNS TABLE (cycle_path TEXT[])
LANGUAGE sql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
    -- Walks the relay_pipeline_deps graph using a recursive CTE.
    -- Returns one row per cycle found, with the path of pipeline names.
    -- Returns no rows when the graph is acyclic.
    WITH RECURSIVE dag_walk (start_node, current_node, path, cycle) AS (
        -- Seed: every direct edge is a potential start.
        SELECT
            upstream_pipeline,
            downstream_pipeline,
            ARRAY[upstream_pipeline, downstream_pipeline],
            (upstream_pipeline = downstream_pipeline)
        FROM tide.relay_pipeline_deps

        UNION ALL

        -- Extend: follow edges from current_node.
        SELECT
            dw.start_node,
            e.downstream_pipeline,
            dw.path || e.downstream_pipeline,
            (e.downstream_pipeline = ANY(dw.path))
        FROM dag_walk dw
        JOIN tide.relay_pipeline_deps e ON e.upstream_pipeline = dw.current_node
        WHERE NOT dw.cycle
    )
    SELECT path AS cycle_path
    FROM dag_walk
    WHERE cycle
    LIMIT 1;
$$;

COMMENT ON FUNCTION tide.relay_dag_check() IS
    'TIDE-DAG-4 (v0.30.0): Detect cycles in the pipeline dependency graph using '
    'a recursive CTE. Returns one row with the cycle path when a cycle is found, '
    'or no rows for a valid DAG.';
