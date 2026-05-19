-- pg_tide 0.19.0 → 0.20.0
--
-- v0.20.0: DuckLake Native Catalog Integration
--
-- This release upgrades the DuckLake relay sink to speak the real DuckLake v1.0
-- catalog protocol. The relay now writes to official DuckLake catalog tables
-- (`ducklake_snapshot`, `ducklake_data_file`, `ducklake_file_column_stats`, etc.)
-- inside a single PostgreSQL transaction per batch — making pg-tide the only
-- pipeline tool that can guarantee exactly-once delivery from a PostgreSQL
-- transaction to a DuckLake data lake.
--
-- Schema changes in this release:
--
--   • `tide.ducklake_attach(catalog_schema text DEFAULT 'ducklake',
--                            data_path      text DEFAULT '')` → TEXT
--     Returns the DuckDB ATTACH statement pre-populated with the connection
--     string and catalog schema, removing friction for first-time users.
--
--   • `tide.ducklake_migrate_catalog(catalog_schema text DEFAULT 'ducklake')`
--     One-time migration helper: converts any existing `tide.ducklake_snapshots`
--     rows (v0.10.0 format) into the new DuckLake v1.0 catalog format and
--     drops the old table. Safe to call multiple times (idempotent).
--
-- The relay-side DuckLake v1.0 catalog tables are created automatically by the
-- relay sink on first use — no manual DDL is required.

-- ── tide.ducklake_attach ─────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION tide.ducklake_attach(
    catalog_schema text DEFAULT 'ducklake',
    data_path      text DEFAULT ''
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
DECLARE
    _dbname     text;
    _host       text;
    _port       text;
    _attach_str text;
    _data_clause text := '';
BEGIN
    -- Resolve connection parameters from the current session.
    SELECT current_database() INTO _dbname;
    SELECT setting INTO _host FROM pg_settings WHERE name = 'listen_addresses';
    SELECT setting INTO _port FROM pg_settings WHERE name = 'port';

    -- Use 'localhost' when listen_addresses is '*' or empty.
    IF _host IS NULL OR _host = '' OR _host = '*' THEN
        _host := 'localhost';
    END IF;

    IF data_path <> '' THEN
        _data_clause := format(', DATA_PATH %L', data_path);
    END IF;

    -- v0.23.0: Use %L (dollar-quoted literal) for user-supplied values to
    -- prevent malformed ATTACH statements when the database name, host, or port
    -- contain quotes or other special characters.
    _attach_str := format(
        'ATTACH ''ducklake:postgres:dbname=%s host=%s port=%s'' AS %I%s;',
        replace(_dbname, '''', ''''''),
        replace(_host,   '''', ''''''),
        replace(_port,   '''', ''''''),
        catalog_schema, _data_clause
    );

    RETURN _attach_str;
END;
$$;

COMMENT ON FUNCTION tide.ducklake_attach(text, text) IS
    'Returns a DuckDB ATTACH statement for the DuckLake catalog stored in this '
    'PostgreSQL database. Pass DATA_PATH to specify the object storage root.';

GRANT EXECUTE ON FUNCTION tide.ducklake_attach(text, text) TO PUBLIC;

-- ── tide.ducklake_migrate_catalog ────────────────────────────────────────────

CREATE OR REPLACE FUNCTION tide.ducklake_migrate_catalog(
    catalog_schema text DEFAULT 'ducklake'
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = tide, pg_catalog
AS $$
DECLARE
    _old_count bigint := 0;
    _schema_id bigint;
    _table_id  bigint;
    _snap_id   bigint;
    _file_id   bigint;
    r          record;
BEGIN
    -- Nothing to do if the legacy table does not exist.
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_tables
        WHERE schemaname = 'tide' AND tablename = 'ducklake_snapshots'
    ) THEN
        RAISE NOTICE 'tide.ducklake_snapshots does not exist — nothing to migrate.';
        RETURN;
    END IF;

    SELECT COUNT(*) INTO _old_count FROM tide.ducklake_snapshots;
    IF _old_count = 0 THEN
        RAISE NOTICE 'tide.ducklake_snapshots is empty — dropping old table.';
        DROP TABLE tide.ducklake_snapshots;
        RETURN;
    END IF;

    RAISE NOTICE 'Migrating % rows from tide.ducklake_snapshots → DuckLake v1.0 catalog (schema: %)',
        _old_count, catalog_schema;

    -- Iterate over legacy snapshot rows and insert into the v1.0 catalog.
    FOR r IN SELECT * FROM tide.ducklake_snapshots ORDER BY id LOOP
        -- Upsert ducklake_schema (namespace).
        EXECUTE format(
            'INSERT INTO %I.ducklake_schema (schema_id, schema_name)
             VALUES (nextval(%L), $1)
             ON CONFLICT (schema_name) DO UPDATE SET schema_name = EXCLUDED.schema_name
             RETURNING schema_id',
            catalog_schema,
            catalog_schema || '.ducklake_schema_id_seq'
        ) INTO _schema_id USING r.namespace;

        -- Upsert ducklake_table.
        EXECUTE format(
            'INSERT INTO %I.ducklake_table (table_id, schema_id, table_name)
             VALUES (nextval(%L), $1, $2)
             ON CONFLICT (schema_id, table_name)
                 DO UPDATE SET table_name = EXCLUDED.table_name
             RETURNING table_id',
            catalog_schema,
            catalog_schema || '.ducklake_table_id_seq'
        ) INTO _table_id USING _schema_id, r.table_name;

        -- Ensure ducklake_table_stats row.
        EXECUTE format(
            'INSERT INTO %I.ducklake_table_stats (table_id, next_row_id, row_count)
             VALUES ($1, 0, 0) ON CONFLICT DO NOTHING',
            catalog_schema
        ) USING _table_id;

        -- Insert ducklake_snapshot.
        EXECUTE format(
            'INSERT INTO %I.ducklake_snapshot
                 (snapshot_id, table_id, schema_version, sequence_number, created_at, author)
             VALUES (nextval(%L), $1, 0,
                 COALESCE((SELECT MAX(sequence_number) + 1
                           FROM %I.ducklake_snapshot WHERE table_id = $1), 0),
                 $2, ''pg-tide-relay (migrated)'')
             RETURNING snapshot_id',
            catalog_schema,
            catalog_schema || '.ducklake_snapshot_id_seq',
            catalog_schema
        ) INTO _snap_id USING _table_id, r.committed_at;

        -- Insert ducklake_data_file.
        EXECUTE format(
            'INSERT INTO %I.ducklake_data_file
                 (file_id, table_id, begin_snapshot, file_path, file_format,
                  record_count, file_size_bytes, footer_size, added_at)
             VALUES (nextval(%L), $1, $2, $3, ''parquet'', $4, $5, 0, $6)
             RETURNING file_id',
            catalog_schema,
            catalog_schema || '.ducklake_file_id_seq'
        ) INTO _file_id
        USING _table_id, _snap_id, r.parquet_path,
              r.num_records, r.file_size_bytes, r.committed_at;

        -- Update table stats.
        EXECUTE format(
            'UPDATE %I.ducklake_table_stats
             SET next_row_id = next_row_id + $1, row_count = row_count + $1
             WHERE table_id = $2',
            catalog_schema
        ) USING r.num_records, _table_id;

    END LOOP;

    -- Drop the now-migrated legacy table.
    DROP TABLE tide.ducklake_snapshots;
    RAISE NOTICE 'Migration complete. tide.ducklake_snapshots dropped.';
END;
$$;

COMMENT ON FUNCTION tide.ducklake_migrate_catalog(text) IS
    'One-time migration: converts tide.ducklake_snapshots (v0.10.0 format) to the '
    'real DuckLake v1.0 catalog tables in catalog_schema. Idempotent and safe to '
    'call even if no legacy rows exist.';

-- Grant to pg_tide_admin if the role exists (it is created externally by the
-- DBA, so it may not exist in fresh installs or test environments).
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'pg_tide_admin') THEN
        GRANT EXECUTE ON FUNCTION tide.ducklake_migrate_catalog(text) TO pg_tide_admin;
    END IF;
END
$$;

-- ── Extension version comment ─────────────────────────────────────────────────

COMMENT ON EXTENSION pg_tide IS
    'Transactional outbox, idempotent inbox, and relay catalog for PostgreSQL — v0.20.0';
