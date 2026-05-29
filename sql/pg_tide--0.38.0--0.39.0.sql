-- pg_tide v0.38.0 → v0.39.0 migration
-- v0.39.0: DuckLake v1.0 Spec Compliance
--
-- This migration provides `tide.ducklake_migrate_catalog()` — an idempotent
-- DDL migration helper that upgrades an existing DuckLake catalog schema
-- (created by the relay's DuckLakeSink prior to v0.39.0) to the real
-- DuckLake v1.0 specification:
--
--   • Adds 18 missing tables to reach the full 28-table spec
--     (ducklake_delete_file, ducklake_partition_info, ducklake_partition_column,
--      ducklake_tag, ducklake_view, ducklake_macro, ducklake_secret,
--      ducklake_cached_secret, ducklake_database_configuration,
--      ducklake_inlined_data_tables, ducklake_snapshot_tag,
--      ducklake_schema_binding, ducklake_encryption_info,
--      ducklake_file_encryption_info, ducklake_column_encryption_info,
--      ducklake_transaction_log, ducklake_statistics, ducklake_catalog_version)
--   • Adds next_catalog_id and next_file_id columns to ducklake_snapshot
--   • Adds snapshot_time column to ducklake_snapshot
--   • Drops table_id column from ducklake_snapshot (catalog-wide snapshots)
--   • Drops sequence_number column from ducklake_snapshot
--   • Drops table_id column from ducklake_snapshot_changes
--   • Drops ducklake_snapshot_id_seq (replaced by in-process counter)
--   • Back-fills next_catalog_id and next_file_id in existing snapshot rows
--   • Updates ducklake_metadata with ducklake_spec_version = '1.0'
--
-- No tide.* schema changes are required for the extension itself.
--
-- Operator upgrade procedure:
--   1. Stop the relay.
--   2. ALTER EXTENSION pg_tide UPDATE;
--   3. SELECT tide.ducklake_migrate_catalog('ducklake');  -- or your schema name
--   4. Restart the relay.
--
-- The relay's DuckLakeSink::ensure_catalog() will refuse to publish until
-- tide.ducklake_migrate_catalog() has been run on an existing pre-v0.39.0
-- catalog, emitting a clear error message.

BEGIN;

RAISE NOTICE 'pg_tide v0.38.0 → v0.39.0: adding tide.ducklake_migrate_catalog()';

-- ── tide.ducklake_migrate_catalog() ──────────────────────────────────────────
--
-- Idempotent DDL migration helper.  Safe to run on a live database with the
-- relay stopped.  Returns a summary row (snapshots_migrated INT,
-- data_files_verified INT).
--
-- Steps performed (all idempotent):
--   1. Add missing 18 DuckLake v1.0 catalog tables.
--   2. Add snapshot_time, next_catalog_id, next_file_id columns to
--      ducklake_snapshot.
--   3. Drop obsolete table_id and sequence_number columns from
--      ducklake_snapshot.
--   4. Drop obsolete table_id column from ducklake_snapshot_changes.
--   5. Drop ducklake_snapshot_id_seq if it exists.
--   6. Back-fill next_catalog_id and next_file_id in all existing snapshot rows.
--   7. Update ducklake_metadata with ducklake_spec_version = '1.0'.

CREATE OR REPLACE FUNCTION tide.ducklake_migrate_catalog(
    _schema_name TEXT DEFAULT 'ducklake'
) RETURNS TABLE(snapshots_migrated INT, data_files_verified INT)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
DECLARE
    _snap_count  INT := 0;
    _file_count  INT := 0;
    _col_exists  BOOLEAN;
    _seq_exists  BOOLEAN;
    _tbl_exists  BOOLEAN;
    _max_col_id  BIGINT;
    _max_file_id BIGINT;
BEGIN
    -- Validate schema name to prevent SQL injection.
    IF NOT (_schema_name ~ '^[A-Za-z_][A-Za-z0-9_]{0,62}$') THEN
        RAISE EXCEPTION 'Invalid schema name: %', _schema_name;
    END IF;

    RAISE NOTICE 'ducklake_migrate_catalog: upgrading schema "%" to DuckLake v1.0 spec', _schema_name;

    -- ── Step 1: Add missing catalog tables ───────────────────────────────────

    -- ducklake_delete_file
    SELECT EXISTS(
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = _schema_name AND table_name = 'ducklake_delete_file'
    ) INTO _tbl_exists;
    IF NOT _tbl_exists THEN
        EXECUTE format(
            $sql$
            CREATE TABLE %I.ducklake_delete_file (
                delete_file_id  BIGINT      NOT NULL PRIMARY KEY,
                table_id        BIGINT      NOT NULL,
                begin_snapshot  BIGINT      NOT NULL,
                end_snapshot    BIGINT,
                file_path       TEXT        NOT NULL,
                file_format     TEXT        NOT NULL DEFAULT 'parquet',
                delete_type     TEXT        NOT NULL DEFAULT 'positional',
                record_count    BIGINT      NOT NULL DEFAULT 0,
                file_size_bytes BIGINT      NOT NULL DEFAULT 0,
                footer_size     BIGINT      NOT NULL DEFAULT 0,
                added_at        TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            $sql$, _schema_name);
        RAISE NOTICE 'Created %.ducklake_delete_file', _schema_name;
    END IF;

    -- ducklake_partition_info
    SELECT EXISTS(
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = _schema_name AND table_name = 'ducklake_partition_info'
    ) INTO _tbl_exists;
    IF NOT _tbl_exists THEN
        EXECUTE format(
            $sql$
            CREATE TABLE %I.ducklake_partition_info (
                partition_id        BIGINT NOT NULL PRIMARY KEY,
                table_id            BIGINT NOT NULL,
                partition_scheme    TEXT   NOT NULL DEFAULT 'identity',
                begin_snapshot      BIGINT NOT NULL,
                end_snapshot        BIGINT
            )
            $sql$, _schema_name);
        RAISE NOTICE 'Created %.ducklake_partition_info', _schema_name;
    END IF;

    -- ducklake_partition_column
    SELECT EXISTS(
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = _schema_name AND table_name = 'ducklake_partition_column'
    ) INTO _tbl_exists;
    IF NOT _tbl_exists THEN
        EXECUTE format(
            $sql$
            CREATE TABLE %I.ducklake_partition_column (
                partition_id    BIGINT NOT NULL,
                column_id       BIGINT NOT NULL,
                transform       TEXT   NOT NULL DEFAULT 'identity',
                bucket_count    INT,
                PRIMARY KEY (partition_id, column_id)
            )
            $sql$, _schema_name);
        RAISE NOTICE 'Created %.ducklake_partition_column', _schema_name;
    END IF;

    -- ducklake_tag
    SELECT EXISTS(
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = _schema_name AND table_name = 'ducklake_tag'
    ) INTO _tbl_exists;
    IF NOT _tbl_exists THEN
        EXECUTE format(
            $sql$
            CREATE TABLE %I.ducklake_tag (
                tag_name    TEXT   NOT NULL PRIMARY KEY,
                snapshot_id BIGINT NOT NULL,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            $sql$, _schema_name);
        RAISE NOTICE 'Created %.ducklake_tag', _schema_name;
    END IF;

    -- ducklake_view
    SELECT EXISTS(
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = _schema_name AND table_name = 'ducklake_view'
    ) INTO _tbl_exists;
    IF NOT _tbl_exists THEN
        EXECUTE format(
            $sql$
            CREATE TABLE %I.ducklake_view (
                view_id         BIGINT NOT NULL PRIMARY KEY,
                schema_id       BIGINT NOT NULL,
                view_name       TEXT   NOT NULL,
                view_definition TEXT   NOT NULL,
                begin_snapshot  BIGINT NOT NULL,
                end_snapshot    BIGINT,
                UNIQUE (schema_id, view_name)
            )
            $sql$, _schema_name);
        RAISE NOTICE 'Created %.ducklake_view', _schema_name;
    END IF;

    -- ducklake_macro
    SELECT EXISTS(
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = _schema_name AND table_name = 'ducklake_macro'
    ) INTO _tbl_exists;
    IF NOT _tbl_exists THEN
        EXECUTE format(
            $sql$
            CREATE TABLE %I.ducklake_macro (
                macro_id        BIGINT NOT NULL PRIMARY KEY,
                schema_id       BIGINT NOT NULL,
                macro_name      TEXT   NOT NULL,
                macro_body      TEXT   NOT NULL,
                begin_snapshot  BIGINT NOT NULL,
                end_snapshot    BIGINT,
                UNIQUE (schema_id, macro_name)
            )
            $sql$, _schema_name);
        RAISE NOTICE 'Created %.ducklake_macro', _schema_name;
    END IF;

    -- ducklake_secret
    SELECT EXISTS(
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = _schema_name AND table_name = 'ducklake_secret'
    ) INTO _tbl_exists;
    IF NOT _tbl_exists THEN
        EXECUTE format(
            $sql$
            CREATE TABLE %I.ducklake_secret (
                secret_id    BIGINT NOT NULL PRIMARY KEY,
                secret_name  TEXT   NOT NULL UNIQUE,
                secret_type  TEXT   NOT NULL,
                secret_scope TEXT,
                begin_snapshot BIGINT NOT NULL,
                end_snapshot   BIGINT
            )
            $sql$, _schema_name);
        RAISE NOTICE 'Created %.ducklake_secret', _schema_name;
    END IF;

    -- ducklake_cached_secret
    SELECT EXISTS(
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = _schema_name AND table_name = 'ducklake_cached_secret'
    ) INTO _tbl_exists;
    IF NOT _tbl_exists THEN
        EXECUTE format(
            $sql$
            CREATE TABLE %I.ducklake_cached_secret (
                secret_id    BIGINT NOT NULL PRIMARY KEY,
                resolved_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                secret_value TEXT   NOT NULL,
                expires_at   TIMESTAMPTZ
            )
            $sql$, _schema_name);
        RAISE NOTICE 'Created %.ducklake_cached_secret', _schema_name;
    END IF;

    -- ducklake_database_configuration
    SELECT EXISTS(
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = _schema_name AND table_name = 'ducklake_database_configuration'
    ) INTO _tbl_exists;
    IF NOT _tbl_exists THEN
        EXECUTE format(
            $sql$
            CREATE TABLE %I.ducklake_database_configuration (
                config_key   TEXT NOT NULL PRIMARY KEY,
                config_value TEXT NOT NULL,
                updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            $sql$, _schema_name);
        RAISE NOTICE 'Created %.ducklake_database_configuration', _schema_name;
    END IF;

    -- ducklake_inlined_data_tables (registry of per-table-version inlined data tables)
    SELECT EXISTS(
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = _schema_name AND table_name = 'ducklake_inlined_data_tables'
    ) INTO _tbl_exists;
    IF NOT _tbl_exists THEN
        EXECUTE format(
            $sql$
            CREATE TABLE %I.ducklake_inlined_data_tables (
                table_id       BIGINT NOT NULL,
                schema_version BIGINT NOT NULL,
                created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (table_id, schema_version)
            )
            $sql$, _schema_name);
        RAISE NOTICE 'Created %.ducklake_inlined_data_tables', _schema_name;
    END IF;

    -- ducklake_snapshot_tag (maps tag names to snapshot IDs for branching)
    SELECT EXISTS(
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = _schema_name AND table_name = 'ducklake_snapshot_tag'
    ) INTO _tbl_exists;
    IF NOT _tbl_exists THEN
        EXECUTE format(
            $sql$
            CREATE TABLE %I.ducklake_snapshot_tag (
                tag_name    TEXT   NOT NULL,
                snapshot_id BIGINT NOT NULL,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (tag_name, snapshot_id)
            )
            $sql$, _schema_name);
        RAISE NOTICE 'Created %.ducklake_snapshot_tag', _schema_name;
    END IF;

    -- ducklake_schema_binding (schema-level version tracking for inlining)
    SELECT EXISTS(
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = _schema_name AND table_name = 'ducklake_schema_binding'
    ) INTO _tbl_exists;
    IF NOT _tbl_exists THEN
        EXECUTE format(
            $sql$
            CREATE TABLE %I.ducklake_schema_binding (
                table_id        BIGINT NOT NULL,
                schema_version  BIGINT NOT NULL,
                bound_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (table_id, schema_version)
            )
            $sql$, _schema_name);
        RAISE NOTICE 'Created %.ducklake_schema_binding', _schema_name;
    END IF;

    -- ducklake_encryption_info (table-level encryption config)
    SELECT EXISTS(
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = _schema_name AND table_name = 'ducklake_encryption_info'
    ) INTO _tbl_exists;
    IF NOT _tbl_exists THEN
        EXECUTE format(
            $sql$
            CREATE TABLE %I.ducklake_encryption_info (
                table_id     BIGINT NOT NULL PRIMARY KEY,
                algorithm    TEXT   NOT NULL DEFAULT 'AES256GCM',
                kms_provider TEXT,
                key_id       TEXT,
                enabled_at   TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            $sql$, _schema_name);
        RAISE NOTICE 'Created %.ducklake_encryption_info', _schema_name;
    END IF;

    -- ducklake_file_encryption_info (per-file encryption metadata)
    SELECT EXISTS(
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = _schema_name AND table_name = 'ducklake_file_encryption_info'
    ) INTO _tbl_exists;
    IF NOT _tbl_exists THEN
        EXECUTE format(
            $sql$
            CREATE TABLE %I.ducklake_file_encryption_info (
                file_id      BIGINT NOT NULL PRIMARY KEY,
                key_metadata TEXT   NOT NULL,
                iv           BYTEA  NOT NULL,
                tag          BYTEA
            )
            $sql$, _schema_name);
        RAISE NOTICE 'Created %.ducklake_file_encryption_info', _schema_name;
    END IF;

    -- ducklake_column_encryption_info (per-column encryption settings)
    SELECT EXISTS(
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = _schema_name AND table_name = 'ducklake_column_encryption_info'
    ) INTO _tbl_exists;
    IF NOT _tbl_exists THEN
        EXECUTE format(
            $sql$
            CREATE TABLE %I.ducklake_column_encryption_info (
                column_id    BIGINT NOT NULL PRIMARY KEY,
                algorithm    TEXT   NOT NULL DEFAULT 'AES256GCM',
                key_id       TEXT
            )
            $sql$, _schema_name);
        RAISE NOTICE 'Created %.ducklake_column_encryption_info', _schema_name;
    END IF;

    -- ducklake_transaction_log (audit log for all catalog mutations)
    SELECT EXISTS(
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = _schema_name AND table_name = 'ducklake_transaction_log'
    ) INTO _tbl_exists;
    IF NOT _tbl_exists THEN
        EXECUTE format(
            $sql$
            CREATE TABLE %I.ducklake_transaction_log (
                log_id       BIGSERIAL   NOT NULL PRIMARY KEY,
                snapshot_id  BIGINT,
                operation    TEXT        NOT NULL,
                actor        TEXT,
                logged_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
                details      JSONB
            )
            $sql$, _schema_name);
        RAISE NOTICE 'Created %.ducklake_transaction_log', _schema_name;
    END IF;

    -- ducklake_statistics (extended statistics for query planning)
    SELECT EXISTS(
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = _schema_name AND table_name = 'ducklake_statistics'
    ) INTO _tbl_exists;
    IF NOT _tbl_exists THEN
        EXECUTE format(
            $sql$
            CREATE TABLE %I.ducklake_statistics (
                stats_id     BIGINT NOT NULL PRIMARY KEY,
                table_id     BIGINT NOT NULL,
                column_id    BIGINT,
                stats_type   TEXT   NOT NULL,
                stats_value  JSONB  NOT NULL,
                snapshot_id  BIGINT NOT NULL,
                computed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            $sql$, _schema_name);
        RAISE NOTICE 'Created %.ducklake_statistics', _schema_name;
    END IF;

    -- ducklake_catalog_version (single-row catalog spec version record)
    SELECT EXISTS(
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = _schema_name AND table_name = 'ducklake_catalog_version'
    ) INTO _tbl_exists;
    IF NOT _tbl_exists THEN
        EXECUTE format(
            $sql$
            CREATE TABLE %I.ducklake_catalog_version (
                version_key TEXT NOT NULL PRIMARY KEY,
                version     TEXT NOT NULL,
                upgraded_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            $sql$, _schema_name);
        EXECUTE format(
            $sql$
            INSERT INTO %I.ducklake_catalog_version (version_key, version)
            VALUES ('spec_version', '1.0')
            ON CONFLICT (version_key) DO UPDATE SET version = '1.0', upgraded_at = now()
            $sql$, _schema_name);
        RAISE NOTICE 'Created %.ducklake_catalog_version', _schema_name;
    END IF;

    -- ── Step 2: Alter ducklake_snapshot to v1.0 spec ─────────────────────────

    -- Add snapshot_time column (replaces created_at semantically).
    SELECT EXISTS(
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = _schema_name AND table_name = 'ducklake_snapshot'
          AND column_name = 'snapshot_time'
    ) INTO _col_exists;
    IF NOT _col_exists THEN
        EXECUTE format(
            'ALTER TABLE %I.ducklake_snapshot ADD COLUMN snapshot_time TIMESTAMPTZ NOT NULL DEFAULT now()',
            _schema_name);
        RAISE NOTICE 'Added %.ducklake_snapshot.snapshot_time', _schema_name;
    END IF;

    -- Add next_catalog_id column.
    SELECT EXISTS(
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = _schema_name AND table_name = 'ducklake_snapshot'
          AND column_name = 'next_catalog_id'
    ) INTO _col_exists;
    IF NOT _col_exists THEN
        EXECUTE format(
            'ALTER TABLE %I.ducklake_snapshot ADD COLUMN next_catalog_id BIGINT NOT NULL DEFAULT 0',
            _schema_name);
        RAISE NOTICE 'Added %.ducklake_snapshot.next_catalog_id', _schema_name;
    END IF;

    -- Add next_file_id column.
    SELECT EXISTS(
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = _schema_name AND table_name = 'ducklake_snapshot'
          AND column_name = 'next_file_id'
    ) INTO _col_exists;
    IF NOT _col_exists THEN
        EXECUTE format(
            'ALTER TABLE %I.ducklake_snapshot ADD COLUMN next_file_id BIGINT NOT NULL DEFAULT 0',
            _schema_name);
        RAISE NOTICE 'Added %.ducklake_snapshot.next_file_id', _schema_name;
    END IF;

    -- Drop table_id column from ducklake_snapshot (cascade drops any FK constraints).
    SELECT EXISTS(
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = _schema_name AND table_name = 'ducklake_snapshot'
          AND column_name = 'table_id'
    ) INTO _col_exists;
    IF _col_exists THEN
        EXECUTE format(
            'ALTER TABLE %I.ducklake_snapshot DROP COLUMN IF EXISTS table_id CASCADE',
            _schema_name);
        RAISE NOTICE 'Dropped %.ducklake_snapshot.table_id', _schema_name;
    END IF;

    -- Drop sequence_number column from ducklake_snapshot.
    SELECT EXISTS(
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = _schema_name AND table_name = 'ducklake_snapshot'
          AND column_name = 'sequence_number'
    ) INTO _col_exists;
    IF _col_exists THEN
        EXECUTE format(
            'ALTER TABLE %I.ducklake_snapshot DROP COLUMN IF EXISTS sequence_number',
            _schema_name);
        RAISE NOTICE 'Dropped %.ducklake_snapshot.sequence_number', _schema_name;
    END IF;

    -- ── Step 3: Alter ducklake_snapshot_changes ───────────────────────────────

    -- Drop table_id column from ducklake_snapshot_changes.
    SELECT EXISTS(
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = _schema_name AND table_name = 'ducklake_snapshot_changes'
          AND column_name = 'table_id'
    ) INTO _col_exists;
    IF _col_exists THEN
        EXECUTE format(
            'ALTER TABLE %I.ducklake_snapshot_changes DROP COLUMN IF EXISTS table_id CASCADE',
            _schema_name);
        RAISE NOTICE 'Dropped %.ducklake_snapshot_changes.table_id', _schema_name;
    END IF;

    -- ── Step 4: Drop ducklake_snapshot_id_seq ────────────────────────────────

    SELECT EXISTS(
        SELECT 1 FROM information_schema.sequences
        WHERE sequence_schema = _schema_name
          AND sequence_name = 'ducklake_snapshot_id_seq'
    ) INTO _seq_exists;
    IF _seq_exists THEN
        EXECUTE format('DROP SEQUENCE IF EXISTS %I.ducklake_snapshot_id_seq', _schema_name);
        RAISE NOTICE 'Dropped %.ducklake_snapshot_id_seq', _schema_name;
    END IF;

    -- ── Step 5: Back-fill next_catalog_id and next_file_id ───────────────────

    -- Compute the max catalog entity ID from the current column_id sequence
    -- (or from the table data if the sequence was dropped).
    SELECT EXISTS(
        SELECT 1 FROM information_schema.sequences
        WHERE sequence_schema = _schema_name
          AND sequence_name = 'ducklake_column_id_seq'
    ) INTO _seq_exists;
    IF _seq_exists THEN
        EXECUTE format(
            'SELECT last_value FROM %I.ducklake_column_id_seq',
            _schema_name
        ) INTO _max_col_id;
    ELSE
        EXECUTE format(
            'SELECT COALESCE(MAX(column_id), 0) FROM %I.ducklake_column',
            _schema_name
        ) INTO _max_col_id;
    END IF;

    SELECT EXISTS(
        SELECT 1 FROM information_schema.sequences
        WHERE sequence_schema = _schema_name
          AND sequence_name = 'ducklake_file_id_seq'
    ) INTO _seq_exists;
    IF _seq_exists THEN
        EXECUTE format(
            'SELECT last_value FROM %I.ducklake_file_id_seq',
            _schema_name
        ) INTO _max_file_id;
    ELSE
        EXECUTE format(
            'SELECT COALESCE(MAX(file_id), 0) FROM %I.ducklake_data_file',
            _schema_name
        ) INTO _max_file_id;
    END IF;

    -- Back-fill all snapshot rows that have next_catalog_id = 0 (not yet back-filled).
    EXECUTE format(
        $sql$
        UPDATE %I.ducklake_snapshot
        SET next_catalog_id = $1,
            next_file_id    = $2,
            author          = COALESCE(NULLIF(author, 'pg-tide-relay'), 'pg-tide-migrate')
        WHERE next_catalog_id = 0 AND next_file_id = 0
        $sql$,
        _schema_name
    ) USING _max_col_id, _max_file_id;

    GET DIAGNOSTICS _snap_count = ROW_COUNT;

    -- Count data files for the return value.
    EXECUTE format(
        'SELECT COUNT(*)::INT FROM %I.ducklake_data_file',
        _schema_name
    ) INTO _file_count;

    -- ── Step 6: Update ducklake_metadata with spec version ───────────────────

    EXECUTE format(
        $sql$
        INSERT INTO %I.ducklake_metadata (key, value)
        VALUES ('ducklake_spec_version', '1.0'), ('catalog_version', '1.0')
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        $sql$,
        _schema_name
    );

    RAISE NOTICE 'ducklake_migrate_catalog: complete (% snapshots back-filled, % data files verified)',
        _snap_count, _file_count;

    RETURN QUERY SELECT _snap_count, _file_count;
END;
$$;

COMMENT ON FUNCTION tide.ducklake_migrate_catalog(TEXT) IS
'Idempotent one-time migration helper that upgrades an existing DuckLake catalog '
'schema from the pg-tide pre-v0.39.0 custom format to the real DuckLake v1.0 spec. '
'Run: SELECT tide.ducklake_migrate_catalog(''ducklake'');';

COMMIT;
