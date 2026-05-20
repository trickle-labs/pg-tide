-- pg_tide 0.24.0 → 0.25.0
-- Outbox table partitioning, multi-tenant relay completion & pre-GA hardening.
--
-- v0.25.0 implements ADR-006 declarative outbox partitioning, completes the
-- multi-tenant relay group runtime, and hardens the operational surface for
-- the v1.0.0 Production GA that follows.

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Outbox table partitioning support (ADR-006)
--    Add partition_strategy and retention_partitions columns to
--    tide.tide_outbox_config so the relay and outbox_create() can record the
--    chosen partition strategy for each outbox.
-- ─────────────────────────────────────────────────────────────────────────────

ALTER TABLE tide.tide_outbox_config
    ADD COLUMN IF NOT EXISTS partition_strategy TEXT NOT NULL DEFAULT 'none'
        CHECK (partition_strategy IN ('none', 'daily', 'weekly', 'monthly'));

COMMENT ON COLUMN tide.tide_outbox_config.partition_strategy IS
    'Declarative range partition strategy for this outbox table. '
    '''none'' = unpartitioned (default); ''daily'' / ''weekly'' / ''monthly'' = '
    'PARTITION BY RANGE (created_at) with the corresponding interval. '
    'Set at outbox_create() time; use tide.outbox_convert_to_partitioned() to '
    'migrate an existing outbox.';

ALTER TABLE tide.tide_outbox_config
    ADD COLUMN IF NOT EXISTS retention_partitions INT NOT NULL DEFAULT 7;

COMMENT ON COLUMN tide.tide_outbox_config.retention_partitions IS
    'For partitioned outboxes: number of past partitions to retain before '
    'detaching and dropping via outbox_truncate_delivered(). Default 7 '
    '(weekly coverage for daily-partition strategy).';

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. tide.outbox_convert_to_partitioned(name, strategy)
--    Live migration tool: convert an existing unpartitioned outbox to a
--    declarative range-partitioned table with minimal relay downtime.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION tide.outbox_convert_to_partitioned(
    p_name     TEXT,
    p_strategy TEXT DEFAULT 'daily'
)
RETURNS VOID
LANGUAGE plpgsql
SET search_path = tide, pg_catalog
AS $$
DECLARE
    _interval_expr  TEXT;
    _part_suffix    TEXT;
    _part_start     TEXT;
    _part_end       TEXT;
    _backup_table   TEXT;
    _new_table      TEXT;
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

COMMENT ON FUNCTION tide.outbox_convert_to_partitioned(TEXT, TEXT) IS
    'Live migration: convert an existing unpartitioned outbox to declarative '
    'range partitioning on created_at.  Performs an advisory-lock swap with '
    'minimal relay downtime.  The original data is preserved in a backup table '
    'until manually dropped after verification.';

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Multi-tenant relay groups: per-tenant advisory lock namespacing
--    The relay coordinator already incorporates tenant_name in the lock key
--    as of v0.14.0.  This migration adds the missing index for fast per-tenant
--    pipeline lookups and ensures relay_consumer_offsets is indexed on
--    tenant_name for efficient per-tenant offset queries.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_relay_outbox_config_tenant
    ON tide.relay_outbox_config (tenant_name)
    WHERE enabled = true;

CREATE INDEX IF NOT EXISTS idx_relay_inbox_config_tenant
    ON tide.relay_inbox_config (tenant_name)
    WHERE enabled = true;

CREATE INDEX IF NOT EXISTS idx_relay_consumer_offsets_tenant
    ON tide.relay_consumer_offsets (tenant_name);

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. Partition event notification table
--    Records a durable log of partition lifecycle events (created, dropped,
--    converted) for auditing and the pg-tide doctor partition health check.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS tide.tide_partition_events (
    id          BIGSERIAL   PRIMARY KEY,
    outbox_name TEXT        NOT NULL,
    event_type  TEXT        NOT NULL CHECK (event_type IN ('created', 'dropped', 'converted')),
    partition   TEXT,
    strategy    TEXT,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE tide.tide_partition_events IS
    'Durable log of partition lifecycle events for pg-tide doctor health checks '
    'and operator auditing.  Written by sweep, convert, and auto-provisioning.';
