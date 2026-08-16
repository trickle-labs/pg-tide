-- pg_tide v0.42.0 -> v0.43.0
--
-- Retention is now governed by native relay/consumer checkpoints and age.
-- The migration never deletes existing messages.

CREATE TABLE IF NOT EXISTS tide.outbox_cleanup_state (
    outbox_name           TEXT        NOT NULL PRIMARY KEY
                                     REFERENCES tide.tide_outbox_config(outbox_name)
                                     ON DELETE CASCADE,
    last_success_at       TIMESTAMPTZ,
    last_safe_offset      BIGINT,
    highest_deleted_id    BIGINT      NOT NULL DEFAULT 0,
    last_batch_rows       BIGINT      NOT NULL DEFAULT 0,
    total_rows_deleted    BIGINT      NOT NULL DEFAULT 0,
    last_duration_ms      DOUBLE PRECISION,
    last_partition_action TEXT        NOT NULL DEFAULT 'none'
);

CREATE TABLE IF NOT EXISTS tide.outbox_storage_config (
    singleton             BOOLEAN     NOT NULL DEFAULT TRUE PRIMARY KEY
                                     CHECK (singleton),
    storage_layout        TEXT        NOT NULL
                                     CHECK (storage_layout IN
                                            ('heap', 'id_range', 'legacy_noncanonical')),
    partition_span        BIGINT      NOT NULL DEFAULT 10000000
                                     CHECK (partition_span > 0),
    premake_count         INTEGER     NOT NULL DEFAULT 2
                                     CHECK (premake_count >= 0),
    last_maintenance_at   TIMESTAMPTZ,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE tide.tide_partition_events
    ADD COLUMN IF NOT EXISTS details JSONB NOT NULL DEFAULT '{}'::JSONB;

ALTER TABLE tide.tide_partition_events
    DROP CONSTRAINT IF EXISTS tide_partition_events_event_type_check;

ALTER TABLE tide.tide_partition_events
    ADD CONSTRAINT tide_partition_events_event_type_check
    CHECK (event_type IN ('created', 'drained', 'dropped', 'converted'));

CREATE INDEX IF NOT EXISTS idx_tide_partition_events_recorded
    ON tide.tide_partition_events (occurred_at DESC);

DO $$
DECLARE
    v_layout TEXT;
    v_parent_oid OID;
    v_partkey TEXT;
BEGIN
    SELECT c.oid, pg_get_partkeydef(c.oid)
      INTO v_parent_oid, v_partkey
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'tide' AND c.relname = 'tide_outbox_messages';

    IF v_parent_oid IS NULL THEN
        RAISE EXCEPTION 'tide.tide_outbox_messages is missing';
    END IF;

    IF v_partkey IS NULL THEN
        v_layout := 'heap';
    ELSIF v_partkey ILIKE '%id%' THEN
        v_layout := 'id_range';
    ELSE
        v_layout := 'legacy_noncanonical';
    END IF;

    INSERT INTO tide.outbox_storage_config (storage_layout)
    VALUES (v_layout)
    ON CONFLICT (singleton) DO UPDATE
       SET storage_layout = EXCLUDED.storage_layout,
           updated_at = now();

    INSERT INTO tide.outbox_cleanup_state (outbox_name)
    SELECT outbox_name
      FROM tide.tide_outbox_config
    ON CONFLICT (outbox_name) DO NOTHING;
END;
$$;

CREATE INDEX IF NOT EXISTS idx_tide_outbox_messages_cleanup
    ON tide.tide_outbox_messages (outbox_name, created_at, id);

COMMENT ON COLUMN tide.tide_outbox_config.partition_strategy IS
    'Deprecated v0.43.0 compatibility setting. Physical storage is recorded '
    'in tide.outbox_storage_config; native retention uses age and checkpoints.';

COMMENT ON COLUMN tide.tide_outbox_config.retention_partitions IS
    'Deprecated v0.43.0 compatibility setting. Retention is governed by age '
    'and every configured participant checkpoint.';

-- Exact native lag.  Do not subtract a globally gapped identity value.
CREATE OR REPLACE VIEW tide.relay_pipeline_lag AS
SELECT
    o.relay_group_id,
    o.pipeline_id,
    o.outbox_name,
    o.last_change_id,
    (
        SELECT COUNT(*)::BIGINT
          FROM tide.tide_outbox_messages m
         WHERE m.outbox_name = o.outbox_name
           AND m.id > o.last_change_id
    ) AS lag,
    o.updated_at
FROM tide.relay_consumer_offsets o
JOIN tide.relay_outbox_config c ON c.name = o.pipeline_id
WHERE c.config ->> 'source_type' = 'outbox'
  AND c.config #>> '{source,outbox}' = o.outbox_name;

CREATE OR REPLACE VIEW tide.outbox_retention_status AS
SELECT
    c.outbox_name,
    c.retention_hours,
    COUNT(m.id)::BIGINT AS retained_rows,
    COALESCE(SUM(pg_column_size(m.*)), 0)::BIGINT AS retained_bytes,
    MIN(m.created_at) AS oldest_retained_at,
    MAX(m.created_at) AS newest_retained_at,
    now() - make_interval(hours => c.retention_hours) AS retention_cutoff,
    COUNT(m.id) FILTER (
        WHERE p.safe_offset IS NULL OR m.id > p.safe_offset
    )::BIGINT AS pending_messages,
    COUNT(m.id)::BIGINT AS total_messages,
    EXTRACT(epoch FROM now() -
        MIN(m.created_at) FILTER (
            WHERE p.safe_offset IS NULL OR m.id > p.safe_offset
        ))::DOUBLE PRECISION
        AS oldest_pending_age_seconds,
    p.participant_count,
    p.safe_offset,
    (
        SELECT COUNT(*)::BIGINT
          FROM (
              SELECT 1
                FROM tide.tide_outbox_messages e
               WHERE e.outbox_name = c.outbox_name
                 AND e.created_at < now() - make_interval(hours => c.retention_hours)
                 AND (p.safe_offset IS NULL OR e.id <= p.safe_offset)
               LIMIT 10001
          ) bounded
    ) AS eligible_rows,
    COALESCE(p.participants, '[]'::JSONB) AS blockers,
    s.highest_deleted_id,
    s.last_success_at,
    s.last_batch_rows,
    s.total_rows_deleted,
    s.last_duration_ms,
    s.last_partition_action,
    sc.storage_layout,
    CASE
        WHEN sc.storage_layout = 'id_range' THEN COALESCE((
            SELECT COALESCE(pg_stat_get_live_tuples(child.oid), 0)::BIGINT
              FROM pg_inherits i
              JOIN pg_class child ON child.oid = i.inhrelid
             WHERE i.inhparent = 'tide.tide_outbox_messages'::regclass
               AND pg_get_expr(child.relpartbound, child.oid) = 'DEFAULT'
        ), 0)
        ELSE 0
    END AS default_partition_rows
FROM tide.tide_outbox_config c
LEFT JOIN tide.tide_outbox_messages m ON m.outbox_name = c.outbox_name
LEFT JOIN tide.outbox_cleanup_state s ON s.outbox_name = c.outbox_name
LEFT JOIN tide.outbox_storage_config sc ON sc.singleton
LEFT JOIN LATERAL (
    WITH relay_participants AS (
        SELECT c2.name::TEXT AS participant, c2.enabled,
               COALESCE(MIN(o.last_change_id), 0)::BIGINT AS safe_offset
          FROM tide.relay_outbox_config c2
          LEFT JOIN tide.relay_consumer_offsets o
            ON o.pipeline_id = c2.name
           AND o.outbox_name = c.outbox_name
         WHERE c2.config #>> '{source,outbox}' = c.outbox_name
           AND c2.config ->> 'source_type' = 'outbox'
         GROUP BY c2.name, c2.enabled
    ), group_participants AS (
        SELECT g.group_name::TEXT AS participant, TRUE AS enabled,
               COALESCE(MIN(o.committed_offset), 0)::BIGINT AS safe_offset
          FROM tide.tide_consumer_groups g
          LEFT JOIN tide.tide_consumer_offsets o USING (group_name)
         WHERE g.outbox_name = c.outbox_name
         GROUP BY g.group_name
    ), fanin_participants AS (
        SELECT f.name::TEXT || '/' || member::TEXT AS participant,
               f.enabled,
               COALESCE(MIN(o.last_change_id), 0)::BIGINT AS safe_offset
          FROM tide.relay_fanin_config f
          CROSS JOIN LATERAL unnest(f.outbox_names) AS members(member)
          LEFT JOIN tide.relay_consumer_offsets o
            ON o.pipeline_id = f.name
           AND o.outbox_name = member
           AND o.fanin_member = member
         WHERE f.enabled AND member = c.outbox_name
         GROUP BY f.name, f.enabled, member
    ), all_participants AS (
        SELECT * FROM relay_participants
        UNION ALL
        SELECT * FROM group_participants
        UNION ALL
        SELECT * FROM fanin_participants
    )
    SELECT COUNT(*)::BIGINT AS participant_count,
           MIN(safe_offset)::BIGINT AS safe_offset,
           jsonb_agg(jsonb_build_object(
               'name', participant,
               'enabled', enabled,
               'safe_offset', safe_offset
           ) ORDER BY participant) AS participants
      FROM all_participants
) p ON TRUE
GROUP BY c.outbox_name, c.retention_hours, p.participant_count, p.safe_offset,
         p.participants, s.highest_deleted_id, s.last_success_at,
         s.last_batch_rows, s.total_rows_deleted, s.last_duration_ms,
         s.last_partition_action, sc.storage_layout;

COMMENT ON VIEW tide.relay_pipeline_lag IS
    'v0.43.0: Exact retained-row lag per native relay group and pipeline.';

COMMENT ON VIEW tide.outbox_retention_status IS
    'v0.43.0: Retention status using every configured native participant and age.';

-- Register the new Rust API for ALTER EXTENSION UPDATE.  The generated fresh
-- install SQL uses CREATE OR REPLACE for the same signature.
CREATE OR REPLACE FUNCTION tide.outbox_sweep(
    TEXT DEFAULT NULL,
    INTEGER DEFAULT 1000,
    BOOLEAN DEFAULT FALSE
)
RETURNS JSONB
LANGUAGE c
AS '$libdir/pg_tide', 'outbox_sweep_wrapper';

-- ── ID-range partition maintenance ─────────────────────────────────────────

CREATE OR REPLACE FUNCTION tide.outbox_maintain_partitions(
    p_ahead INTEGER DEFAULT 2,
    p_dry_run BOOLEAN DEFAULT FALSE
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
DECLARE
    v_layout TEXT;
    v_span BIGINT;
    v_min_id BIGINT;
    v_max_id BIGINT;
    v_start BIGINT;
    v_lower BIGINT;
    v_upper BIGINT;
    v_name TEXT;
    v_created JSONB := '[]'::JSONB;
    v_drained JSONB := '[]'::JSONB;
    v_dropped JSONB := '[]'::JSONB;
    v_default TEXT;
    v_default_rows BIGINT := 0;
    v_batch_rows BIGINT;
    v_range_count INTEGER;
    v_child TEXT;
    v_child_lower BIGINT;
    v_child_upper BIGINT;
    v_child_rows BIGINT;
    v_exists BOOLEAN;
BEGIN
    IF p_ahead < 0 OR p_ahead > 100 THEN
        RAISE EXCEPTION 'p_ahead must be between 0 and 100';
    END IF;
    IF NOT (
        EXISTS (SELECT 1 FROM pg_roles
                 WHERE rolname = session_user AND rolsuper)
        OR (EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pg_tide_admin')
            AND pg_has_role(session_user, 'pg_tide_admin', 'MEMBER'))
    ) THEN
        RAISE EXCEPTION
            'outbox_maintain_partitions: requires superuser or pg_tide_admin membership';
    END IF;
    PERFORM pg_advisory_xact_lock(
        hashtextextended('pg_tide:partition-maintenance', 0)
    );

    SELECT storage_layout, partition_span
      INTO v_layout, v_span
      FROM tide.outbox_storage_config
     WHERE singleton;

    IF v_layout = 'legacy_noncanonical' THEN
        RAISE EXCEPTION
            'outbox storage is legacy_noncanonical; run explicit global conversion';
    END IF;
    IF v_layout = 'heap' THEN
        RETURN jsonb_build_object(
            'storage_layout', v_layout,
            'created', '[]'::JSONB,
            'default_partition_rows', 0
        );
    END IF;

    SELECT COALESCE(MIN(id), 0), COALESCE(MAX(id), 0)
      INTO v_min_id, v_max_id
      FROM tide.tide_outbox_messages;
    v_start := GREATEST(0, (v_min_id / v_span) * v_span);
    v_range_count := ((v_max_id - v_start) / v_span)::INTEGER + p_ahead;

    FOR i IN 0..v_range_count LOOP
        v_lower := v_start + i * v_span;
        v_upper := v_lower + v_span;
        v_name := format('tide_outbox_messages_p_%s_%s', v_lower, v_upper);

        SELECT EXISTS (
            SELECT 1
              FROM pg_inherits i
              JOIN pg_class child ON child.oid = i.inhrelid
             WHERE i.inhparent = 'tide.tide_outbox_messages'::REGCLASS
               AND pg_get_expr(child.relpartbound, child.oid) =
                   format('FOR VALUES FROM (%s) TO (%s)', v_lower, v_upper)
        ) INTO v_exists;

        IF p_dry_run THEN
            v_created := v_created || jsonb_build_array(
                jsonb_build_object('name', v_name, 'from', v_lower, 'to', v_upper)
            );
        ELSIF NOT v_exists THEN
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF tide.tide_outbox_messages
                   FOR VALUES FROM (%s) TO (%s)',
                v_name, v_lower, v_upper
            );
            EXECUTE format(
                'CREATE INDEX %I ON tide.%I (outbox_name, id)',
                v_name || '_outbox_id', v_name
            );
            INSERT INTO tide.tide_partition_events
                (outbox_name, event_type, partition, strategy, details)
            VALUES ('*', 'created', v_name, 'id_range',
                    jsonb_build_object('from', v_lower, 'to', v_upper));
            v_created := v_created || jsonb_build_array(
                jsonb_build_object('name', v_name, 'from', v_lower, 'to', v_upper)
            );
        END IF;
    END LOOP;

    SELECT child.relname
      INTO v_default
      FROM pg_inherits i
      JOIN pg_class child ON child.oid = i.inhrelid
     WHERE i.inhparent = 'tide.tide_outbox_messages'::regclass
       AND pg_get_expr(child.relpartbound, child.oid) = 'DEFAULT';
    IF v_default IS NOT NULL THEN
        EXECUTE format('SELECT COUNT(*) FROM tide.%I', v_default)
           INTO v_default_rows;
        IF NOT p_dry_run THEN
            FOR i IN 0..v_range_count LOOP
                v_lower := v_start + i * v_span;
                v_upper := v_lower + v_span;
                EXECUTE format(
                    'WITH candidates AS (
                         SELECT ctid
                           FROM tide.%I
                          WHERE id >= %s AND id < %s
                          ORDER BY id
                          LIMIT 1000
                          FOR UPDATE SKIP LOCKED
                     ), moved AS (
                         DELETE FROM tide.%I d
                          USING candidates c
                         WHERE d.ctid = c.ctid
                         RETURNING d.*
                     )
                     INSERT INTO tide.tide_outbox_messages
                     OVERRIDING SYSTEM VALUE
                     SELECT * FROM moved',
                    v_default, v_lower, v_upper, v_default
                );
                GET DIAGNOSTICS v_batch_rows = ROW_COUNT;
                IF v_batch_rows > 0 THEN
                    v_drained := v_drained || jsonb_build_array(
                        jsonb_build_object(
                            'partition', v_default,
                            'from', v_lower,
                            'to', v_upper,
                            'rows', v_batch_rows
                        )
                    );
                    INSERT INTO tide.tide_partition_events
                        (outbox_name, event_type, partition, strategy, details)
                    VALUES (
                        '*', 'drained', v_default, 'id_range',
                        jsonb_build_object(
                            'from', v_lower,
                            'to', v_upper,
                            'rows', v_batch_rows
                        )
                    );
                END IF;
            END LOOP;
            EXECUTE format('SELECT COUNT(*) FROM tide.%I', v_default)
               INTO v_default_rows;
        END IF;
    END IF;

    IF NOT p_dry_run THEN
        FOR v_child, v_child_lower, v_child_upper IN
            SELECT child.relname,
                   (regexp_match(
                       pg_get_expr(child.relpartbound, child.oid),
                       'FROM \((-?[0-9]+)\) TO'
                   ))[1]::BIGINT,
                   (regexp_match(
                       pg_get_expr(child.relpartbound, child.oid),
                       'TO \((-?[0-9]+)\)'
                   ))[1]::BIGINT
              FROM pg_inherits i
              JOIN pg_class child ON child.oid = i.inhrelid
             WHERE i.inhparent = 'tide.tide_outbox_messages'::regclass
               AND pg_get_expr(child.relpartbound, child.oid) <> 'DEFAULT'
               AND pg_get_expr(child.relpartbound, child.oid)
                   ~ 'FROM \(-?[0-9]+\) TO \(-?[0-9]+\)'
        LOOP
            IF v_child_upper <= v_min_id THEN
                EXECUTE format('SELECT COUNT(*) FROM tide.%I', v_child)
                   INTO v_child_rows;
                IF v_child_rows = 0 THEN
                    EXECUTE format('DROP TABLE tide.%I', v_child);
                    v_dropped := v_dropped || jsonb_build_array(
                        jsonb_build_object(
                            'partition', v_child,
                            'from', v_child_lower,
                            'to', v_child_upper
                        )
                    );
                    INSERT INTO tide.tide_partition_events
                        (outbox_name, event_type, partition, strategy, details)
                    VALUES (
                        '*', 'dropped', v_child, 'id_range',
                        jsonb_build_object(
                            'from', v_child_lower,
                            'to', v_child_upper,
                            'reason', 'empty historical partition'
                        )
                    );
                END IF;
            END IF;
        END LOOP;
    END IF;

    UPDATE tide.outbox_storage_config
       SET last_maintenance_at = now(), updated_at = now()
     WHERE singleton AND NOT p_dry_run;

    RETURN jsonb_build_object(
        'storage_layout', v_layout,
        'created', v_created,
        'drained', v_drained,
        'dropped', v_dropped,
        'default_partition_rows', v_default_rows,
        'dry_run', p_dry_run
    );
END;
$$;

COMMENT ON FUNCTION tide.outbox_maintain_partitions(INTEGER, BOOLEAN) IS
    'v0.43.0: Idempotently provision ID-range children, drain the default '
    'partition in bounded batches, and drop empty historical children.';

CREATE OR REPLACE FUNCTION tide.admin_convert_outbox_storage(
    p_partition_span BIGINT DEFAULT 10000000,
    p_premake INTEGER DEFAULT 2,
    confirm_blocking_copy BOOLEAN DEFAULT FALSE
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
DECLARE
    v_layout TEXT;
    v_min_id BIGINT;
    v_max_id BIGINT;
    v_start BIGINT;
    v_span BIGINT := p_partition_span;
    v_shadow TEXT := 'tide_outbox_messages_v043_shadow';
    v_legacy TEXT := 'tide_outbox_messages_v043_legacy';
    v_seq TEXT;
    v_viewdef TEXT;
    v_outbox TEXT;
    v_range_count INTEGER;
BEGIN
    IF NOT confirm_blocking_copy THEN
        RAISE EXCEPTION
            'admin_convert_outbox_storage requires confirm_blocking_copy = TRUE';
    END IF;
    IF NOT (
        EXISTS (SELECT 1 FROM pg_roles
                 WHERE rolname = session_user AND rolsuper)
        OR (EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pg_tide_admin')
            AND pg_has_role(session_user, 'pg_tide_admin', 'MEMBER'))
    ) THEN
        RAISE EXCEPTION
            'admin_convert_outbox_storage: requires superuser or pg_tide_admin membership';
    END IF;
    IF v_span <= 0 OR p_premake < 0 OR p_premake > 100 THEN
        RAISE EXCEPTION 'partition span must be positive and p_premake must be 0..100';
    END IF;

    SELECT storage_layout INTO v_layout
      FROM tide.outbox_storage_config
     WHERE singleton
     FOR UPDATE;
    IF v_layout = 'id_range' THEN
        RETURN jsonb_build_object('storage_layout', 'id_range', 'converted', false);
    END IF;
    IF v_layout = 'legacy_noncanonical' THEN
        RAISE EXCEPTION
            'cannot convert legacy_noncanonical storage; restore the canonical '
            'shared parent or follow the explicit remediation procedure';
    END IF;

    PERFORM pg_advisory_xact_lock(
        hashtextextended('pg_tide:partition-maintenance', 0)
    );
    PERFORM pg_advisory_xact_lock(hashtextextended('pg_tide:outbox:conversion', 0));
    FOR v_outbox IN
        SELECT outbox_name FROM tide.tide_outbox_config ORDER BY outbox_name
    LOOP
        PERFORM pg_advisory_xact_lock(
            hashtextextended('pg_tide:outbox:' || v_outbox, 0)
        );
    END LOOP;
    LOCK TABLE tide.tide_outbox_messages IN ACCESS EXCLUSIVE MODE;
    EXECUTE format('DROP TABLE IF EXISTS tide.%I CASCADE', v_shadow);
    EXECUTE format(
        'CREATE TABLE tide.%I (LIKE tide.tide_outbox_messages INCLUDING ALL)
         PARTITION BY RANGE (id)',
        v_shadow
    );

    SELECT COALESCE(MIN(id), 0), COALESCE(MAX(id), 0)
      INTO v_min_id, v_max_id
      FROM tide.tide_outbox_messages;
    v_start := GREATEST(0, (v_min_id / v_span) * v_span);
    v_range_count := ((v_max_id - v_start) / v_span)::INTEGER + p_premake + 1;

    FOR i IN 0..v_range_count LOOP
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF tide.%I
               FOR VALUES FROM (%s) TO (%s)',
            format('tide_outbox_messages_v043_p_%s', v_start + i * v_span),
            v_shadow, v_start + i * v_span, v_start + (i + 1) * v_span
        );
    END LOOP;
    EXECUTE format(
        'CREATE TABLE %I PARTITION OF tide.%I DEFAULT',
        v_shadow || '_default', v_shadow
    );
    EXECUTE format(
        'INSERT INTO tide.%I OVERRIDING SYSTEM VALUE
             SELECT * FROM tide.tide_outbox_messages',
        v_shadow
    );

    SELECT pg_get_serial_sequence(format('tide.%I', v_shadow), 'id')
      INTO v_seq;
    IF v_seq IS NOT NULL AND v_max_id > 0 THEN
        PERFORM setval(v_seq, v_max_id, true);
    END IF;

    ALTER TABLE tide.tide_outbox_messages RENAME TO tide_outbox_messages_v043_legacy;
    EXECUTE format('ALTER TABLE tide.%I RENAME TO tide_outbox_messages', v_shadow);

    -- Views retain relation OIDs across a table rename. Rebind the public
    -- views to the new canonical parent before returning.
    EXECUTE 'CREATE OR REPLACE VIEW tide.outbox_pending AS
        SELECT outbox_name, COUNT(*) AS pending_count, MIN(created_at) AS oldest_at,
               MAX(id) AS max_id
          FROM tide.tide_outbox_messages
         WHERE consumed_at IS NULL
         GROUP BY outbox_name';
    EXECUTE 'CREATE OR REPLACE VIEW tide.consumer_lag AS
        SELECT g.group_name, g.outbox_name, o.consumer_id, o.committed_offset,
               (SELECT COALESCE(MAX(id), 0)
                  FROM tide.tide_outbox_messages m
                 WHERE m.outbox_name = g.outbox_name) - o.committed_offset AS lag,
               o.last_heartbeat
          FROM tide.tide_consumer_groups g
          JOIN tide.tide_consumer_offsets o USING (group_name)';
    SELECT replace(
        pg_get_viewdef('tide.outbox_retention_status'::REGCLASS, true),
        v_legacy,
        'tide_outbox_messages'
    )
      INTO v_viewdef;
    EXECUTE 'CREATE OR REPLACE VIEW tide.outbox_retention_status AS ' || v_viewdef;

    UPDATE tide.outbox_storage_config
       SET storage_layout = 'id_range',
           partition_span = p_partition_span,
           premake_count = p_premake,
           updated_at = now()
     WHERE singleton;
    INSERT INTO tide.tide_partition_events
        (outbox_name, event_type, partition, strategy, details)
    VALUES ('*', 'converted', v_legacy, 'id_range', jsonb_build_object(
        'span', p_partition_span, 'premake', p_premake, 'legacy_table', v_legacy
    ));

    RETURN jsonb_build_object(
        'storage_layout', 'id_range',
        'converted', true,
        'legacy_table', v_legacy,
        'partition_span', p_partition_span,
        'premake', p_premake
    );
END;
$$;

COMMENT ON FUNCTION tide.admin_convert_outbox_storage(BIGINT, INTEGER, BOOLEAN) IS
    'v0.43.0: Blocking maintenance-window conversion of the complete shared '
    'outbox parent to ID-range partitions. This is not a live conversion.';

REVOKE ALL ON FUNCTION tide.outbox_maintain_partitions(INTEGER, BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION tide.admin_convert_outbox_storage(BIGINT, INTEGER, BOOLEAN) FROM PUBLIC;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pg_tide_admin') THEN
        GRANT EXECUTE ON FUNCTION tide.outbox_maintain_partitions(INTEGER, BOOLEAN)
            TO pg_tide_admin;
        GRANT EXECUTE ON FUNCTION tide.admin_convert_outbox_storage(BIGINT, INTEGER, BOOLEAN)
            TO pg_tide_admin;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION tide.outbox_convert_to_partitioned(
    p_name TEXT,
    p_strategy TEXT DEFAULT 'daily'
)
RETURNS VOID
LANGUAGE plpgsql
SET search_path = tide, pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION
        'outbox_convert_to_partitioned(name, strategy) is deprecated and refuses '
        'new conversions; use tide.admin_convert_outbox_storage() for the global '
        'ID-range parent during a maintenance window';
END;
$$;

-- ── Rewind floor enforcement ────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION tide.admin_rewind_offset(
    p_group_name TEXT,
    p_consumer_id TEXT,
    p_target_offset BIGINT,
    confirm_reprocessing BOOLEAN DEFAULT FALSE
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
DECLARE
    v_current BIGINT;
    v_outbox TEXT;
    v_floor BIGINT;
BEGIN
    IF NOT confirm_reprocessing THEN
        RAISE EXCEPTION 'admin_rewind_offset: set confirm_reprocessing = TRUE';
    END IF;
    IF p_target_offset < 0 THEN
        RAISE EXCEPTION 'target offset must be non-negative';
    END IF;
    IF NOT (
        EXISTS (SELECT 1 FROM pg_roles WHERE rolname = session_user AND rolsuper)
        OR (EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pg_tide_admin')
            AND pg_has_role(session_user, 'pg_tide_admin', 'MEMBER'))
    ) THEN
        RAISE EXCEPTION 'admin_rewind_offset: requires superuser or pg_tide_admin membership';
    END IF;

    SELECT o.committed_offset, g.outbox_name
      INTO v_current, v_outbox
      FROM tide.tide_consumer_offsets o
      JOIN tide.tide_consumer_groups g USING (group_name)
     WHERE o.group_name = p_group_name AND o.consumer_id = p_consumer_id
     FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'consumer offset not found for group %, consumer %',
            p_group_name, p_consumer_id;
    END IF;
    IF p_target_offset > v_current THEN
        RAISE EXCEPTION 'target offset % is ahead of current offset %',
            p_target_offset, v_current;
    END IF;

    SELECT COALESCE(highest_deleted_id, 0)
      INTO v_floor
      FROM tide.outbox_cleanup_state
     WHERE outbox_name = v_outbox;
    IF NOT FOUND THEN
        RAISE EXCEPTION
            'outbox %, cleanup state is missing; refuse rewind until v0.43 '
            'maintenance state is initialized',
            v_outbox;
    END IF;
    IF p_target_offset < v_floor THEN
        RAISE EXCEPTION
            'outbox %, target offset % is below retained floor %; requested history '
            'has already been deleted',
            v_outbox, p_target_offset, v_floor;
    END IF;

    PERFORM set_config('tide.offset_rewind', 'on', true);
    UPDATE tide.tide_consumer_offsets
       SET committed_offset = p_target_offset, last_heartbeat = now()
     WHERE group_name = p_group_name AND consumer_id = p_consumer_id;
END;
$$;

CREATE OR REPLACE FUNCTION tide.admin_rewind_relay_offset(
    p_relay_group_id TEXT,
    p_pipeline_id TEXT,
    p_outbox_name TEXT,
    p_target_offset BIGINT,
    confirm_reprocessing BOOLEAN DEFAULT FALSE
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
DECLARE
    v_current BIGINT;
    v_enabled BOOLEAN;
    v_tenant TEXT;
    v_direction TEXT;
    v_floor BIGINT;
BEGIN
    IF NOT confirm_reprocessing THEN
        RAISE EXCEPTION 'admin_rewind_relay_offset: set confirm_reprocessing = TRUE';
    END IF;
    IF p_target_offset < 0 THEN
        RAISE EXCEPTION 'target offset must be non-negative';
    END IF;
    IF NOT (
        EXISTS (SELECT 1 FROM pg_roles WHERE rolname = session_user AND rolsuper)
        OR (EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pg_tide_admin')
            AND pg_has_role(session_user, 'pg_tide_admin', 'MEMBER'))
    ) THEN
        RAISE EXCEPTION
            'admin_rewind_relay_offset: requires superuser or pg_tide_admin membership';
    END IF;

    SELECT enabled, COALESCE(tenant_name, 'default'), 'forward'
      INTO v_enabled, v_tenant, v_direction
      FROM tide.relay_outbox_config WHERE name = p_pipeline_id;
    IF NOT FOUND THEN
        SELECT enabled, COALESCE(tenant_name, 'default'), 'reverse'
          INTO v_enabled, v_tenant, v_direction
          FROM tide.relay_inbox_config WHERE name = p_pipeline_id;
    END IF;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'relay pipeline % not found', p_pipeline_id;
    END IF;
    IF v_enabled THEN
        RAISE EXCEPTION 'relay pipeline % must be disabled before rewind', p_pipeline_id;
    END IF;
    IF NOT pg_try_advisory_xact_lock(
        hashtext(p_relay_group_id),
        hashtext(v_tenant || ':' || v_direction || ':' || p_pipeline_id)
    ) THEN
        RAISE EXCEPTION 'relay pipeline % is still owned; drain it before rewinding',
            p_pipeline_id;
    END IF;

    SELECT last_change_id
      INTO v_current
      FROM tide.relay_consumer_offsets
     WHERE relay_group_id = p_relay_group_id
       AND pipeline_id = p_pipeline_id
       AND outbox_name = p_outbox_name
     FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'relay offset not found for group %, pipeline %, outbox %',
            p_relay_group_id, p_pipeline_id, p_outbox_name;
    END IF;
    IF p_target_offset > v_current THEN
        RAISE EXCEPTION 'target offset % is ahead of current offset %',
            p_target_offset, v_current;
    END IF;

    SELECT COALESCE(highest_deleted_id, 0)
      INTO v_floor
      FROM tide.outbox_cleanup_state
     WHERE outbox_name = p_outbox_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION
            'outbox %, cleanup state is missing; refuse rewind until v0.43 '
            'maintenance state is initialized',
            p_outbox_name;
    END IF;
    IF p_target_offset < v_floor THEN
        RAISE EXCEPTION
            'outbox %, target offset % is below retained floor %; requested history '
            'has already been deleted',
            p_outbox_name, p_target_offset, v_floor;
    END IF;

    PERFORM set_config('tide.offset_rewind', 'on', true);
    UPDATE tide.relay_consumer_offsets
       SET last_change_id = p_target_offset, updated_at = now()
     WHERE relay_group_id = p_relay_group_id
       AND pipeline_id = p_pipeline_id
       AND outbox_name = p_outbox_name;
END;
$$;

REVOKE ALL ON FUNCTION tide.admin_rewind_offset(TEXT, TEXT, BIGINT, BOOLEAN) FROM PUBLIC;
REVOKE ALL ON FUNCTION tide.admin_rewind_relay_offset(TEXT, TEXT, TEXT, BIGINT, BOOLEAN) FROM PUBLIC;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pg_tide_admin') THEN
        GRANT EXECUTE ON FUNCTION tide.admin_rewind_offset(TEXT, TEXT, BIGINT, BOOLEAN)
            TO pg_tide_admin;
        GRANT EXECUTE ON FUNCTION tide.admin_rewind_relay_offset(TEXT, TEXT, TEXT, BIGINT, BOOLEAN)
            TO pg_tide_admin;
    END IF;
END;
$$;

COMMENT ON FUNCTION tide.admin_rewind_offset(TEXT, TEXT, BIGINT, BOOLEAN) IS
    'v0.43.0: Audited rewind that refuses targets below the outbox retained floor.';

COMMENT ON FUNCTION tide.admin_rewind_relay_offset(TEXT, TEXT, TEXT, BIGINT, BOOLEAN) IS
    'v0.43.0: Audited native relay rewind that refuses deleted history.';

COMMENT ON EXTENSION pg_tide IS
    'Transactional outbox, idempotent inbox, and relay catalog for PostgreSQL — v0.43.0';
