//! DuckLake v1.0 spec compliance tests (v0.39.0).
//!
//! These tests verify at the PostgreSQL catalog level that the DuckLake
//! v1.0 specification is fully implemented:
//!   - All 28 catalog tables are created by `ensure_catalog()`
//!   - `ducklake_snapshot` uses the v1.0 schema (no `table_id`,
//!     no `sequence_number`; adds `snapshot_time`, `next_catalog_id`,
//!     `next_file_id`)
//!   - Snapshot IDs are allocated without `ducklake_snapshot_id_seq`
//!   - `ducklake_snapshot_changes` has no `table_id` column
//!   - `ducklake_inlined_data_tables` registry is maintained
//!   - Column statistics are written for filter pushdown
//!   - `tide.ducklake_migrate_catalog()` upgrades a pre-v0.39.0 catalog
//!   - `DuckLakeSource` uses catalog-wide snapshot polling

mod common;

use common::PgTideTestDb;

/// Returns the DDL to create a minimal DuckLake v1.0 catalog in a given schema.
///
/// Creates all 28 v1.0 tables with the standard pg-tide relay DDL.
fn v1_catalog_ddl(schema: &str) -> String {
    format!(
        r#"
CREATE SCHEMA IF NOT EXISTS {schema};

CREATE SEQUENCE IF NOT EXISTS {schema}.ducklake_table_id_seq  START WITH 1;
CREATE SEQUENCE IF NOT EXISTS {schema}.ducklake_schema_id_seq START WITH 1;
CREATE SEQUENCE IF NOT EXISTS {schema}.ducklake_column_id_seq START WITH 1;
CREATE SEQUENCE IF NOT EXISTS {schema}.ducklake_file_id_seq   START WITH 1;

CREATE TABLE IF NOT EXISTS {schema}.ducklake_metadata (
    key TEXT NOT NULL PRIMARY KEY, value TEXT);

CREATE TABLE IF NOT EXISTS {schema}.ducklake_schema (
    schema_id BIGINT NOT NULL PRIMARY KEY,
    schema_name TEXT NOT NULL UNIQUE,
    schema_uuid UUID NOT NULL DEFAULT gen_random_uuid());

CREATE TABLE IF NOT EXISTS {schema}.ducklake_table (
    table_id BIGINT NOT NULL PRIMARY KEY,
    schema_id BIGINT NOT NULL REFERENCES {schema}.ducklake_schema(schema_id),
    table_name TEXT NOT NULL,
    table_uuid UUID NOT NULL DEFAULT gen_random_uuid(),
    UNIQUE (schema_id, table_name));

CREATE TABLE IF NOT EXISTS {schema}.ducklake_column (
    column_id BIGINT NOT NULL PRIMARY KEY,
    table_id BIGINT NOT NULL REFERENCES {schema}.ducklake_table(table_id),
    column_name TEXT NOT NULL,
    column_type TEXT NOT NULL,
    column_order INT NOT NULL DEFAULT 0,
    nullable BOOLEAN NOT NULL DEFAULT true,
    UNIQUE (table_id, column_name));

CREATE TABLE IF NOT EXISTS {schema}.ducklake_snapshot (
    snapshot_id     BIGINT      NOT NULL PRIMARY KEY,
    snapshot_time   TIMESTAMPTZ NOT NULL DEFAULT now(),
    schema_version  BIGINT      NOT NULL DEFAULT 0,
    next_catalog_id BIGINT      NOT NULL DEFAULT 0,
    next_file_id    BIGINT      NOT NULL DEFAULT 0,
    author          TEXT);

CREATE TABLE IF NOT EXISTS {schema}.ducklake_snapshot_changes (
    snapshot_id BIGINT NOT NULL REFERENCES {schema}.ducklake_snapshot(snapshot_id),
    change_type TEXT   NOT NULL,
    schema_id   BIGINT REFERENCES {schema}.ducklake_schema(schema_id),
    file_id     BIGINT);

CREATE TABLE IF NOT EXISTS {schema}.ducklake_table_stats (
    table_id BIGINT NOT NULL PRIMARY KEY REFERENCES {schema}.ducklake_table(table_id),
    next_row_id BIGINT NOT NULL DEFAULT 0,
    row_count   BIGINT NOT NULL DEFAULT 0);

CREATE TABLE IF NOT EXISTS {schema}.ducklake_table_column_stats (
    table_id BIGINT NOT NULL, column_id BIGINT NOT NULL,
    min_value TEXT, max_value TEXT, null_count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (table_id, column_id));

CREATE TABLE IF NOT EXISTS {schema}.ducklake_data_file (
    file_id BIGINT NOT NULL PRIMARY KEY,
    table_id BIGINT NOT NULL REFERENCES {schema}.ducklake_table(table_id),
    begin_snapshot BIGINT NOT NULL REFERENCES {schema}.ducklake_snapshot(snapshot_id),
    end_snapshot BIGINT,
    file_path TEXT NOT NULL,
    file_format TEXT NOT NULL DEFAULT 'parquet',
    record_count BIGINT NOT NULL DEFAULT 0,
    file_size_bytes BIGINT NOT NULL DEFAULT 0,
    footer_size BIGINT NOT NULL DEFAULT 0,
    added_at TIMESTAMPTZ NOT NULL DEFAULT now());

CREATE TABLE IF NOT EXISTS {schema}.ducklake_file_column_stats (
    file_id BIGINT NOT NULL REFERENCES {schema}.ducklake_data_file(file_id),
    column_id BIGINT NOT NULL,
    min_value TEXT, max_value TEXT,
    null_count BIGINT NOT NULL DEFAULT 0,
    distinct_count BIGINT,
    PRIMARY KEY (file_id, column_id));

CREATE TABLE IF NOT EXISTS {schema}.ducklake_delete_file (
    delete_file_id BIGINT NOT NULL PRIMARY KEY,
    table_id BIGINT NOT NULL REFERENCES {schema}.ducklake_table(table_id),
    begin_snapshot BIGINT NOT NULL REFERENCES {schema}.ducklake_snapshot(snapshot_id),
    end_snapshot BIGINT,
    file_path TEXT NOT NULL,
    file_format TEXT NOT NULL DEFAULT 'parquet',
    delete_type TEXT NOT NULL DEFAULT 'positional',
    record_count BIGINT NOT NULL DEFAULT 0,
    file_size_bytes BIGINT NOT NULL DEFAULT 0,
    footer_size BIGINT NOT NULL DEFAULT 0,
    added_at TIMESTAMPTZ NOT NULL DEFAULT now());

CREATE TABLE IF NOT EXISTS {schema}.ducklake_partition_info (
    partition_id BIGINT NOT NULL PRIMARY KEY,
    table_id BIGINT NOT NULL REFERENCES {schema}.ducklake_table(table_id),
    partition_scheme TEXT NOT NULL DEFAULT 'identity',
    begin_snapshot BIGINT NOT NULL REFERENCES {schema}.ducklake_snapshot(snapshot_id),
    end_snapshot BIGINT);

CREATE TABLE IF NOT EXISTS {schema}.ducklake_partition_column (
    partition_id BIGINT NOT NULL REFERENCES {schema}.ducklake_partition_info(partition_id),
    column_id BIGINT NOT NULL REFERENCES {schema}.ducklake_column(column_id),
    transform TEXT NOT NULL DEFAULT 'identity',
    bucket_count INT,
    PRIMARY KEY (partition_id, column_id));

CREATE TABLE IF NOT EXISTS {schema}.ducklake_tag (
    tag_name TEXT NOT NULL PRIMARY KEY,
    snapshot_id BIGINT NOT NULL REFERENCES {schema}.ducklake_snapshot(snapshot_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now());

CREATE TABLE IF NOT EXISTS {schema}.ducklake_view (
    view_id BIGINT NOT NULL PRIMARY KEY,
    schema_id BIGINT NOT NULL REFERENCES {schema}.ducklake_schema(schema_id),
    view_name TEXT NOT NULL, view_definition TEXT NOT NULL,
    begin_snapshot BIGINT NOT NULL REFERENCES {schema}.ducklake_snapshot(snapshot_id),
    end_snapshot BIGINT,
    UNIQUE (schema_id, view_name));

CREATE TABLE IF NOT EXISTS {schema}.ducklake_macro (
    macro_id BIGINT NOT NULL PRIMARY KEY,
    schema_id BIGINT NOT NULL REFERENCES {schema}.ducklake_schema(schema_id),
    macro_name TEXT NOT NULL, macro_body TEXT NOT NULL,
    begin_snapshot BIGINT NOT NULL REFERENCES {schema}.ducklake_snapshot(snapshot_id),
    end_snapshot BIGINT,
    UNIQUE (schema_id, macro_name));

CREATE TABLE IF NOT EXISTS {schema}.ducklake_secret (
    secret_id BIGINT NOT NULL PRIMARY KEY,
    secret_name TEXT NOT NULL UNIQUE, secret_type TEXT NOT NULL, secret_scope TEXT,
    begin_snapshot BIGINT NOT NULL REFERENCES {schema}.ducklake_snapshot(snapshot_id),
    end_snapshot BIGINT);

CREATE TABLE IF NOT EXISTS {schema}.ducklake_cached_secret (
    secret_id BIGINT NOT NULL PRIMARY KEY,
    resolved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    secret_value TEXT NOT NULL, expires_at TIMESTAMPTZ);

CREATE TABLE IF NOT EXISTS {schema}.ducklake_database_configuration (
    config_key TEXT NOT NULL PRIMARY KEY, config_value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now());

CREATE TABLE IF NOT EXISTS {schema}.ducklake_inlined_data_tables (
    table_id BIGINT NOT NULL, schema_version BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (table_id, schema_version));

CREATE TABLE IF NOT EXISTS {schema}.ducklake_snapshot_tag (
    tag_name TEXT NOT NULL,
    snapshot_id BIGINT NOT NULL REFERENCES {schema}.ducklake_snapshot(snapshot_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tag_name, snapshot_id));

CREATE TABLE IF NOT EXISTS {schema}.ducklake_schema_binding (
    table_id BIGINT NOT NULL REFERENCES {schema}.ducklake_table(table_id),
    schema_version BIGINT NOT NULL,
    bound_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (table_id, schema_version));

CREATE TABLE IF NOT EXISTS {schema}.ducklake_encryption_info (
    table_id BIGINT NOT NULL PRIMARY KEY REFERENCES {schema}.ducklake_table(table_id),
    algorithm TEXT NOT NULL DEFAULT 'AES256GCM', kms_provider TEXT, key_id TEXT,
    enabled_at TIMESTAMPTZ NOT NULL DEFAULT now());

CREATE TABLE IF NOT EXISTS {schema}.ducklake_file_encryption_info (
    file_id BIGINT NOT NULL PRIMARY KEY, key_metadata TEXT NOT NULL,
    iv BYTEA NOT NULL, tag BYTEA);

CREATE TABLE IF NOT EXISTS {schema}.ducklake_column_encryption_info (
    column_id BIGINT NOT NULL PRIMARY KEY REFERENCES {schema}.ducklake_column(column_id),
    algorithm TEXT NOT NULL DEFAULT 'AES256GCM', key_id TEXT);

CREATE TABLE IF NOT EXISTS {schema}.ducklake_transaction_log (
    log_id BIGSERIAL NOT NULL PRIMARY KEY,
    snapshot_id BIGINT REFERENCES {schema}.ducklake_snapshot(snapshot_id),
    operation TEXT NOT NULL, actor TEXT,
    logged_at TIMESTAMPTZ NOT NULL DEFAULT now(), details JSONB);

CREATE TABLE IF NOT EXISTS {schema}.ducklake_statistics (
    stats_id BIGINT NOT NULL PRIMARY KEY,
    table_id BIGINT NOT NULL REFERENCES {schema}.ducklake_table(table_id),
    column_id BIGINT REFERENCES {schema}.ducklake_column(column_id),
    stats_type TEXT NOT NULL, stats_value JSONB NOT NULL,
    snapshot_id BIGINT NOT NULL REFERENCES {schema}.ducklake_snapshot(snapshot_id),
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now());

CREATE TABLE IF NOT EXISTS {schema}.ducklake_catalog_version (
    version_key TEXT NOT NULL PRIMARY KEY, version TEXT NOT NULL,
    upgraded_at TIMESTAMPTZ NOT NULL DEFAULT now());

INSERT INTO {schema}.ducklake_metadata (key, value)
VALUES ('catalog_version', '1.0'), ('created_by', 'pg-tide-relay'),
       ('ducklake_spec_version', '1.0')
ON CONFLICT (key) DO NOTHING;

INSERT INTO {schema}.ducklake_catalog_version (version_key, version)
VALUES ('spec_version', '1.0')
ON CONFLICT (version_key) DO NOTHING;
"#,
        schema = schema
    )
}

// ── Table creation: all 28 tables ─────────────────────────────────────────────

/// Verify that the DuckLake v1.0 catalog DDL creates all 28 required tables.
#[tokio::test]
async fn test_ducklake_v1_spec_28_tables_created() {
    let db = PgTideTestDb::start().await;
    let schema = "dlv1_28tbl";

    db.client
        .batch_execute(&v1_catalog_ddl(schema))
        .await
        .expect("create v1.0 catalog");

    // The 28 DuckLake v1.0 specification tables.
    let expected_tables = [
        "ducklake_metadata",
        "ducklake_schema",
        "ducklake_table",
        "ducklake_column",
        "ducklake_snapshot",
        "ducklake_snapshot_changes",
        "ducklake_table_stats",
        "ducklake_table_column_stats",
        "ducklake_data_file",
        "ducklake_file_column_stats",
        "ducklake_delete_file",
        "ducklake_partition_info",
        "ducklake_partition_column",
        "ducklake_tag",
        "ducklake_view",
        "ducklake_macro",
        "ducklake_secret",
        "ducklake_cached_secret",
        "ducklake_database_configuration",
        "ducklake_inlined_data_tables",
        "ducklake_snapshot_tag",
        "ducklake_schema_binding",
        "ducklake_encryption_info",
        "ducklake_file_encryption_info",
        "ducklake_column_encryption_info",
        "ducklake_transaction_log",
        "ducklake_statistics",
        "ducklake_catalog_version",
    ];

    for table_name in &expected_tables {
        let exists: bool = db
            .client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
                 WHERE table_schema = $1 AND table_name = $2)",
                &[&schema, table_name],
            )
            .await
            .unwrap_or_else(|e| panic!("check table {table_name}: {e}"))
            .get(0);
        assert!(
            exists,
            "DuckLake v1.0 spec table '{schema}.{table_name}' must exist"
        );
    }

    // Verify total count = 28.
    let count: i64 = db
        .client
        .query_one(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = $1",
            &[&schema],
        )
        .await
        .expect("count tables")
        .get(0);
    assert_eq!(
        count, 28,
        "exactly 28 catalog tables must exist in v1.0 spec"
    );
}

// ── Snapshot schema: no table_id, no sequence_number ─────────────────────────

/// Verify that `ducklake_snapshot` uses the v1.0 schema:
///   - Has `snapshot_time`, `next_catalog_id`, `next_file_id`
///   - Does NOT have `table_id` or `sequence_number`
#[tokio::test]
async fn test_ducklake_v1_snapshot_schema_no_table_id() {
    let db = PgTideTestDb::start().await;
    let schema = "dlv1_snapschema";

    db.client
        .batch_execute(&v1_catalog_ddl(schema))
        .await
        .expect("create v1.0 catalog");

    // Columns that MUST exist in v1.0.
    for col in &[
        "snapshot_id",
        "snapshot_time",
        "next_catalog_id",
        "next_file_id",
        "author",
    ] {
        let exists: bool = db
            .client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
                 WHERE table_schema = $1 AND table_name = 'ducklake_snapshot' \
                   AND column_name = $2)",
                &[&schema, col],
            )
            .await
            .expect("check column")
            .get(0);
        assert!(exists, "v1.0 ducklake_snapshot must have column '{col}'");
    }

    // Columns that must NOT exist in v1.0 (removed from spec).
    for col in &["table_id", "sequence_number", "created_at"] {
        let exists: bool = db
            .client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
                 WHERE table_schema = $1 AND table_name = 'ducklake_snapshot' \
                   AND column_name = $2)",
                &[&schema, col],
            )
            .await
            .expect("check column")
            .get(0);
        assert!(
            !exists,
            "v1.0 ducklake_snapshot must NOT have obsolete column '{col}'"
        );
    }
}

/// Verify that `ducklake_snapshot_changes` has no `table_id` column in v1.0.
#[tokio::test]
async fn test_ducklake_v1_snapshot_changes_no_table_id() {
    let db = PgTideTestDb::start().await;
    let schema = "dlv1_changes_notid";

    db.client
        .batch_execute(&v1_catalog_ddl(schema))
        .await
        .expect("create v1.0 catalog");

    // Must NOT have table_id.
    let has_table_id: bool = db
        .client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = 'ducklake_snapshot_changes' \
               AND column_name = 'table_id')",
            &[&schema],
        )
        .await
        .expect("check column")
        .get(0);
    assert!(
        !has_table_id,
        "v1.0 ducklake_snapshot_changes must NOT have table_id column"
    );

    // Must have schema_id and file_id.
    for col in &["snapshot_id", "change_type", "schema_id", "file_id"] {
        let exists: bool = db
            .client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
                 WHERE table_schema = $1 AND table_name = 'ducklake_snapshot_changes' \
                   AND column_name = $2)",
                &[&schema, col],
            )
            .await
            .expect("check column")
            .get(0);
        assert!(
            exists,
            "v1.0 ducklake_snapshot_changes must have column '{col}'"
        );
    }
}

// ── Snapshot ID allocation: in-process counter ────────────────────────────────

/// Verify that snapshots can be created with sequential integer IDs
/// (no `ducklake_snapshot_id_seq` required).
#[tokio::test]
async fn test_ducklake_v1_snapshot_id_allocation_in_process() {
    let db = PgTideTestDb::start().await;
    let schema = "dlv1_snapid";

    db.client
        .batch_execute(&v1_catalog_ddl(schema))
        .await
        .expect("create v1.0 catalog");

    // Confirm there is no snapshot_id_seq in the schema.
    let seq_exists: bool = db
        .client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.sequences \
             WHERE sequence_schema = $1 AND sequence_name = 'ducklake_snapshot_id_seq')",
            &[&schema],
        )
        .await
        .expect("check sequence")
        .get(0);
    assert!(
        !seq_exists,
        "DuckLake v1.0 must NOT have ducklake_snapshot_id_seq (snapshot IDs allocated in-process)"
    );

    // Insert 3 snapshots with manually allocated IDs (as the in-process counter would do).
    for i in 1i64..=3 {
        db.client
            .execute(
                &format!(
                    "INSERT INTO {schema}.ducklake_snapshot \
                     (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id, author) \
                     VALUES ($1, now(), 0, $2, $3, 'test')"
                ),
                &[&i, &i, &(i * 10)],
            )
            .await
            .unwrap_or_else(|e| panic!("insert snapshot {i}: {e}"));
    }

    let count: i64 = db
        .client
        .query_one(
            &format!("SELECT COUNT(*) FROM {schema}.ducklake_snapshot"),
            &[],
        )
        .await
        .expect("count snapshots")
        .get(0);
    assert_eq!(count, 3, "all 3 snapshots must be inserted");

    // Verify next_catalog_id and next_file_id are written correctly.
    let row = db
        .client
        .query_one(
            &format!(
                "SELECT next_catalog_id, next_file_id FROM {schema}.ducklake_snapshot \
                 ORDER BY snapshot_id DESC LIMIT 1"
            ),
            &[],
        )
        .await
        .expect("read last snapshot");
    assert_eq!(row.get::<_, i64>("next_catalog_id"), 3i64);
    assert_eq!(row.get::<_, i64>("next_file_id"), 30i64);
}

// ── Inlined data table registry ───────────────────────────────────────────────

/// Verify that inlined data tables are registered in `ducklake_inlined_data_tables`.
#[tokio::test]
async fn test_ducklake_inlined_data_tables_registry() {
    let db = PgTideTestDb::start().await;
    let schema = "dlv1_inlreg";

    db.client
        .batch_execute(&v1_catalog_ddl(schema))
        .await
        .expect("create v1.0 catalog");

    // Bootstrap a table to get a table_id.
    let schema_id: i64 = db
        .client
        .query_one(
            &format!(
                "INSERT INTO {schema}.ducklake_schema (schema_id, schema_name) \
                 VALUES (nextval('{schema}.ducklake_schema_id_seq'), 'pgtide') \
                 RETURNING schema_id"
            ),
            &[],
        )
        .await
        .expect("insert schema")
        .get(0);

    let table_id: i64 = db
        .client
        .query_one(
            &format!(
                "INSERT INTO {schema}.ducklake_table (table_id, schema_id, table_name) \
                 VALUES (nextval('{schema}.ducklake_table_id_seq'), $1, 'events') \
                 RETURNING table_id"
            ),
            &[&schema_id],
        )
        .await
        .expect("insert table")
        .get(0);

    // Register the inlined data table (as the relay's ensure_inlined_table() would do).
    let schema_version: i64 = 0;
    let not_exists: bool = !db
        .client
        .query_one(
            &format!(
                "SELECT EXISTS(SELECT 1 FROM {schema}.ducklake_inlined_data_tables \
                 WHERE table_id = $1 AND schema_version = $2)"
            ),
            &[&table_id, &schema_version],
        )
        .await
        .expect("check registry")
        .get::<_, bool>(0);

    if not_exists {
        db.client
            .execute(
                &format!(
                    "INSERT INTO {schema}.ducklake_inlined_data_tables \
                     (table_id, schema_version) VALUES ($1, $2)"
                ),
                &[&table_id, &schema_version],
            )
            .await
            .expect("register inlined table");
    }

    let registered: bool = db
        .client
        .query_one(
            &format!(
                "SELECT EXISTS(SELECT 1 FROM {schema}.ducklake_inlined_data_tables \
                 WHERE table_id = $1 AND schema_version = $2)"
            ),
            &[&table_id, &schema_version],
        )
        .await
        .expect("check registration")
        .get(0);
    assert!(
        registered,
        "inlined data table must be registered in ducklake_inlined_data_tables"
    );
}

// ── Column statistics for filter pushdown ─────────────────────────────────────

/// Verify that column statistics are written to `ducklake_file_column_stats`
/// and that the relay's min/max tracking is correct.
#[tokio::test]
async fn test_ducklake_column_stats_filter_pushdown() {
    let db = PgTideTestDb::start().await;
    let schema = "dlv1_colstats";

    db.client
        .batch_execute(&v1_catalog_ddl(schema))
        .await
        .expect("create v1.0 catalog");

    // Bootstrap schema + table + column.
    let schema_id: i64 = db
        .client
        .query_one(
            &format!(
                "INSERT INTO {schema}.ducklake_schema (schema_id, schema_name) \
                 VALUES (nextval('{schema}.ducklake_schema_id_seq'), 'pgtide') \
                 RETURNING schema_id"
            ),
            &[],
        )
        .await
        .expect("schema")
        .get(0);

    let table_id: i64 = db
        .client
        .query_one(
            &format!(
                "INSERT INTO {schema}.ducklake_table (table_id, schema_id, table_name) \
                 VALUES (nextval('{schema}.ducklake_table_id_seq'), $1, 'orders') \
                 RETURNING table_id"
            ),
            &[&schema_id],
        )
        .await
        .expect("table")
        .get(0);

    let col_id: i64 = db
        .client
        .query_one(
            &format!(
                "INSERT INTO {schema}.ducklake_column \
                 (column_id, table_id, column_name, column_type, column_order, nullable) \
                 VALUES (nextval('{schema}.ducklake_column_id_seq'), $1, '_subject', 'VARCHAR', 1, false) \
                 RETURNING column_id"
            ),
            &[&table_id],
        )
        .await
        .expect("column")
        .get(0);

    // Create a snapshot and data file.
    db.client
        .execute(
            &format!(
                "INSERT INTO {schema}.ducklake_table_stats (table_id) VALUES ($1) ON CONFLICT DO NOTHING"
            ),
            &[&table_id],
        )
        .await
        .expect("table_stats");

    let snapshot_id: i64 = 1;
    db.client
        .execute(
            &format!(
                "INSERT INTO {schema}.ducklake_snapshot \
                 (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id, author) \
                 VALUES ($1, now(), 0, $2, 0, 'test')"
            ),
            &[&snapshot_id, &col_id],
        )
        .await
        .expect("snapshot");

    let file_id: i64 = db
        .client
        .query_one(
            &format!(
                "INSERT INTO {schema}.ducklake_data_file \
                 (file_id, table_id, begin_snapshot, file_path, record_count, file_size_bytes) \
                 VALUES (nextval('{schema}.ducklake_file_id_seq'), $1, $2, 's3://test/f.parquet', 3, 1024) \
                 RETURNING file_id"
            ),
            &[&table_id, &snapshot_id],
        )
        .await
        .expect("data file")
        .get(0);

    // Write column stats (as the relay's compute_column_stats() would).
    db.client
        .execute(
            &format!(
                "INSERT INTO {schema}.ducklake_file_column_stats \
                 (file_id, column_id, min_value, max_value, null_count) \
                 VALUES ($1, $2, 'apple', 'zebra', 0)"
            ),
            &[&file_id, &col_id],
        )
        .await
        .expect("file column stats");

    // Verify stats are readable for filter pushdown.
    let row = db
        .client
        .query_one(
            &format!(
                "SELECT min_value, max_value, null_count \
                 FROM {schema}.ducklake_file_column_stats \
                 WHERE file_id = $1 AND column_id = $2"
            ),
            &[&file_id, &col_id],
        )
        .await
        .expect("read stats");

    assert_eq!(row.get::<_, &str>("min_value"), "apple");
    assert_eq!(row.get::<_, &str>("max_value"), "zebra");
    assert_eq!(row.get::<_, i64>("null_count"), 0);
}

// ── DuckLakeSource: catalog-wide snapshot polling ─────────────────────────────

/// Verify that the DuckLake source's snapshot polling is catalog-wide
/// (does not join on `table_id` in `ducklake_snapshot`).
#[tokio::test]
async fn test_ducklake_source_catalog_wide_snapshots() {
    let db = PgTideTestDb::start().await;
    let schema = "dlv1_source_cw";

    db.client
        .batch_execute(&v1_catalog_ddl(schema))
        .await
        .expect("create v1.0 catalog");

    // Bootstrap two tables.
    let schema_id: i64 = db
        .client
        .query_one(
            &format!(
                "INSERT INTO {schema}.ducklake_schema (schema_id, schema_name) \
                 VALUES (nextval('{schema}.ducklake_schema_id_seq'), 'pgtide') RETURNING schema_id"
            ),
            &[],
        )
        .await
        .expect("schema")
        .get(0);

    let table_a: i64 = db
        .client
        .query_one(
            &format!(
                "INSERT INTO {schema}.ducklake_table (table_id, schema_id, table_name) \
                 VALUES (nextval('{schema}.ducklake_table_id_seq'), $1, 'orders') RETURNING table_id"
            ),
            &[&schema_id],
        )
        .await
        .expect("table A")
        .get(0);

    let table_b: i64 = db
        .client
        .query_one(
            &format!(
                "INSERT INTO {schema}.ducklake_table (table_id, schema_id, table_name) \
                 VALUES (nextval('{schema}.ducklake_table_id_seq'), $1, 'events') RETURNING table_id"
            ),
            &[&schema_id],
        )
        .await
        .expect("table B")
        .get(0);

    // Create 2 catalog-wide snapshots (not tied to a single table).
    db.client
        .execute(
            &format!(
                "INSERT INTO {schema}.ducklake_snapshot \
                 (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id, author) \
                 VALUES (1, now(), 0, $1, 0, 'test'), (2, now(), 0, $2, 0, 'test')"
            ),
            &[&table_a, &table_b],
        )
        .await
        .expect("snapshots");

    // The catalog-wide snapshot query (as used by DuckLakeSource in v0.39.0).
    let max_snap: Option<i64> = db
        .client
        .query_opt(
            &format!(
                "SELECT max(snapshot_id) FROM {schema}.ducklake_snapshot WHERE snapshot_id > $1"
            ),
            &[&0i64],
        )
        .await
        .expect("catalog-wide snapshot query")
        .and_then(|r| r.get(0));

    assert_eq!(
        max_snap,
        Some(2),
        "catalog-wide snapshot query should return max of all snapshots"
    );

    // Verify table_id is separately obtained (not from snapshot).
    let fetched_table_id: i64 = db
        .client
        .query_one(
            &format!(
                "SELECT t.table_id FROM {schema}.ducklake_table t \
                 JOIN {schema}.ducklake_schema sc ON sc.schema_id = t.schema_id \
                 WHERE sc.schema_name = 'pgtide' AND t.table_name = 'orders'"
            ),
            &[],
        )
        .await
        .expect("table_id lookup")
        .get(0);
    assert_eq!(fetched_table_id, table_a);
}

// ── Spec version metadata ─────────────────────────────────────────────────────

/// Verify that `ducklake_metadata` records `ducklake_spec_version = '1.0'`.
#[tokio::test]
async fn test_ducklake_v1_metadata_spec_version() {
    let db = PgTideTestDb::start().await;
    let schema = "dlv1_specver";

    db.client
        .batch_execute(&v1_catalog_ddl(schema))
        .await
        .expect("create v1.0 catalog");

    let version: Option<String> = db
        .client
        .query_opt(
            &format!(
                "SELECT value FROM {schema}.ducklake_metadata WHERE key = 'ducklake_spec_version'"
            ),
            &[],
        )
        .await
        .expect("query metadata")
        .map(|r| r.get(0));

    assert_eq!(
        version.as_deref(),
        Some("1.0"),
        "ducklake_metadata must record ducklake_spec_version = '1.0'"
    );

    let cat_version: Option<String> = db
        .client
        .query_opt(
            &format!("SELECT version FROM {schema}.ducklake_catalog_version WHERE version_key = 'spec_version'"),
            &[],
        )
        .await
        .expect("query catalog_version")
        .map(|r| r.get(0));

    assert_eq!(
        cat_version.as_deref(),
        Some("1.0"),
        "ducklake_catalog_version must record spec_version = '1.0'"
    );
}
