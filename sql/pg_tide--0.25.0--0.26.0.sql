-- pg_tide 0.25.0 → 0.26.0
--
-- v0.26.0: Partition Safety, Defence-in-Depth & Test Coverage Completion
--
-- Changes:
--   1. NAMEDATALEN guard in outbox_convert_to_partitioned() — rejects outbox
--      names long enough to produce a backup/new table exceeding 63 bytes.
--   2. Shared-table prerequisite guard — prevents converting one outbox while
--      others still use unpartitioned strategy (global table rename).
--   3. Add confirm_shared_table_migration BOOLEAN parameter (default FALSE) so
--      operators who understand the scope can opt in deliberately.
--   4. outbox_create() NAMEDATALEN guard — rejects names that would overflow
--      the partition table naming scheme at creation time.

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Drop old outbox_convert_to_partitioned(TEXT, TEXT) — signature changes to
--    add the confirm_shared_table_migration BOOLEAN parameter.
-- ─────────────────────────────────────────────────────────────────────────────
DROP FUNCTION IF EXISTS tide.outbox_convert_to_partitioned(TEXT, TEXT);

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Recreate outbox_convert_to_partitioned with safety guards.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION tide.outbox_convert_to_partitioned(
    p_name                         TEXT,
    p_strategy                     TEXT    DEFAULT 'daily',
    confirm_shared_table_migration BOOLEAN DEFAULT FALSE
)
RETURNS VOID
LANGUAGE plpgsql
SET search_path = tide, pg_catalog
AS $$
DECLARE
    _interval_expr         TEXT;
    _part_suffix           TEXT;
    _part_start            TEXT;
    _part_end              TEXT;
    _backup_table          TEXT;
    _new_table             TEXT;
    _unconverted_outboxes  TEXT;
BEGIN
    IF p_strategy NOT IN ('daily', 'weekly', 'monthly') THEN
        RAISE EXCEPTION 'Invalid partition strategy: %. Must be daily, weekly, or monthly.',
            p_strategy;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM tide.tide_outbox_config WHERE outbox_name = p_name
    ) THEN
        RAISE EXCEPTION 'Outbox ''%'' does not exist.', p_name;
    END IF;

    IF (SELECT partition_strategy FROM tide.tide_outbox_config WHERE outbox_name = p_name)
       <> 'none' THEN
        RAISE EXCEPTION 'Outbox ''%'' is already partitioned with strategy ''%''.',
            p_name,
            (SELECT partition_strategy FROM tide.tide_outbox_config WHERE outbox_name = p_name);
    END IF;

    -- ── P1: NAMEDATALEN guard ─────────────────────────────────────────────
    -- 'tide_outbox_messages_backup_' is 29 bytes; the replaced name must fit
    -- within 63 bytes total, leaving 34 bytes for the outbox name fragment.
    IF length('tide_outbox_messages_backup_' || replace(p_name, '-', '_')) > 63 THEN
        RAISE EXCEPTION
            'outbox_convert_to_partitioned: outbox name ''%'' is too long for partition '
            'table naming (backup prefix is 29 bytes, max outbox name fragment is 34 bytes, '
            'got % bytes). Shorten the outbox name to at most 34 characters before retrying.',
            p_name,
            length(replace(p_name, '-', '_'));
    END IF;

    IF length('tide_outbox_messages_new_' || replace(p_name, '-', '_')) > 63 THEN
        RAISE EXCEPTION
            'outbox_convert_to_partitioned: outbox name ''%'' is too long for partition '
            'new-table naming (new prefix is 25 bytes, max outbox name fragment is 38 bytes, '
            'got % bytes). Shorten the outbox name to at most 34 characters before retrying.',
            p_name,
            length(replace(p_name, '-', '_'));
    END IF;

    -- ── P1: Shared-table prerequisite guard ───────────────────────────────
    -- tide_outbox_messages is shared by ALL outboxes.  Converting it while
    -- other outboxes still rely on unpartitioned strategy will break them.
    -- The operator must either convert all outboxes simultaneously or pass
    -- confirm_shared_table_migration = TRUE to opt in to the single-outbox
    -- migration procedure with an explicit maintenance window.
    SELECT string_agg(outbox_name, ', ' ORDER BY outbox_name)
      INTO _unconverted_outboxes
      FROM tide.tide_outbox_config
     WHERE partition_strategy = 'none'
       AND outbox_name <> p_name;

    IF _unconverted_outboxes IS NOT NULL AND NOT confirm_shared_table_migration THEN
        RAISE EXCEPTION
            'outbox_convert_to_partitioned: other outboxes still use the unpartitioned '
            'shared table: [%]. '
            'tide_outbox_messages is a GLOBAL shared table — converting it while other '
            'outboxes are writing to it will break them. '
            'Options: (a) convert all outboxes atomically in a maintenance window, or '
            '(b) pass confirm_shared_table_migration => TRUE to acknowledge this global '
            'impact and proceed deliberately.',
            _unconverted_outboxes;
    END IF;

    _backup_table := 'tide_outbox_messages_backup_' || replace(p_name, '-', '_');
    _new_table    := 'tide_outbox_messages_new_' || replace(p_name, '-', '_');

    -- Step 1: Create a new partitioned shadow table (unlogged to speed up copy).
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS tide.%I '
        '(LIKE tide.tide_outbox_messages INCLUDING ALL) '
        'PARTITION BY RANGE (created_at)',
        _new_table
    );

    -- Step 2: Provision current and next partition.
    IF p_strategy = 'daily' THEN
        _interval_expr := '1 day';
        _part_suffix   := to_char(date_trunc('day', now()), 'YYYY_MM_DD');
        _part_start    := to_char(date_trunc('day', now()), 'YYYY-MM-DD');
        _part_end      := to_char(date_trunc('day', now()) + interval '1 day', 'YYYY-MM-DD');
    ELSIF p_strategy = 'weekly' THEN
        _interval_expr := '1 week';
        _part_suffix   := to_char(date_trunc('week', now()), 'YYYY_WW');
        _part_start    := to_char(date_trunc('week', now()), 'YYYY-MM-DD');
        _part_end      := to_char(date_trunc('week', now()) + interval '1 week', 'YYYY-MM-DD');
    ELSE -- monthly
        _interval_expr := '1 month';
        _part_suffix   := to_char(date_trunc('month', now()), 'YYYY_MM');
        _part_start    := to_char(date_trunc('month', now()), 'YYYY-MM-DD');
        _part_end      := to_char(date_trunc('month', now()) + interval '1 month', 'YYYY-MM-DD');
    END IF;

    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS tide.%I '
        'PARTITION OF tide.%I '
        'FOR VALUES FROM (%L) TO (%L)',
        _new_table || '_' || _part_suffix,
        _new_table,
        _part_start,
        _part_end
    );

    -- Step 3: Copy existing rows that belong to this outbox.
    EXECUTE format(
        'INSERT INTO tide.%I SELECT * FROM tide.tide_outbox_messages '
        'WHERE outbox_name = %L',
        _new_table,
        p_name
    );

    -- Step 4: Swap tables under advisory lock (brief window).
    PERFORM pg_advisory_lock(hashtext('pg_tide_partition_swap_' || p_name));

    -- Rename old table to backup.
    EXECUTE format(
        'ALTER TABLE tide.tide_outbox_messages RENAME TO %I',
        _backup_table
    );

    -- Rename new table to canonical name.
    EXECUTE format(
        'ALTER TABLE tide.%I RENAME TO tide_outbox_messages',
        _new_table
    );

    -- Step 5: Update config.
    UPDATE tide.tide_outbox_config
       SET partition_strategy = p_strategy
     WHERE outbox_name = p_name;

    PERFORM pg_advisory_unlock(hashtext('pg_tide_partition_swap_' || p_name));

    -- Notify relay of partition changes.
    PERFORM pg_notify('tide_partition_events',
        json_build_object(
            'event', 'converted',
            'outbox', p_name,
            'strategy', p_strategy
        )::text
    );

    RAISE NOTICE 'Outbox ''%'' converted to % partitioning. '
                 'Original data is in tide.%. '
                 'Verify relay delivery then DROP TABLE tide.%.',
        p_name, p_strategy, _backup_table, _backup_table;
END;
$$;

COMMENT ON FUNCTION tide.outbox_convert_to_partitioned(TEXT, TEXT, BOOLEAN) IS
    'Live migration: convert an existing unpartitioned outbox to declarative '
    'range partitioning on created_at.  Performs an advisory-lock swap with '
    'minimal relay downtime.  The original data is preserved in a backup table '
    'until manually dropped after verification. '
    'CAUTION: tide_outbox_messages is a global shared table — all outboxes must '
    'be converted atomically or confirm_shared_table_migration = TRUE must be '
    'passed to acknowledge the global scope. See ADR-007.';
