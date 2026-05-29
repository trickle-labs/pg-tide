//! v0.39.0 schema migration validation tests.
//!
//! Verifies that:
//!   1. The `pg_tide--0.38.0--0.39.0.sql` migration script applies cleanly.
//!   2. `tide.ducklake_migrate_catalog()` correctly upgrades a pre-v0.39.0
//!      catalog schema (old snapshot + snapshot_changes DDL) to v1.0.
//!   3. The migration function is idempotent (safe to run twice).
//!   4. The migration back-fills `next_catalog_id` and `next_file_id` in
//!      existing snapshot rows.

mod common;

use common::{strip_extension_comments, PgTideTestDb};

/// Returns the SQL for the v0.38.0→v0.39.0 upgrade script.
const UPGRADE_SQL: &str = include_str!("../../sql/pg_tide--0.38.0--0.39.0.sql");

/// v0.38.0-era (pre-spec) DuckLake catalog DDL — the format written by the
/// relay before v0.39.0.
fn pre_v39_catalog_ddl(schema: &str) -> String {
    format!(
        r#"
CREATE SCHEMA IF NOT EXISTS {schema};

CREATE SEQUENCE IF NOT EXISTS {schema}.ducklake_snapshot_id_seq START WITH 1;
CREATE SEQUENCE IF NOT EXISTS {schema}.ducklake_table_id_seq    START WITH 1;
CREATE SEQUENCE IF NOT EXISTS {schema}.ducklake_schema_id_seq   START WITH 1;
CREATE SEQUENCE IF NOT EXISTS {schema}.ducklake_column_id_seq   START WITH 1;
CREATE SEQUENCE IF NOT EXISTS {schema}.ducklake_file_id_seq     START WITH 1;

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

-- Pre-v0.39.0 snapshot: per-table with table_id and sequence_number.
CREATE TABLE IF NOT EXISTS {schema}.ducklake_snapshot (
    snapshot_id     BIGINT      NOT NULL PRIMARY KEY,
    table_id        BIGINT      NOT NULL REFERENCES {schema}.ducklake_table(table_id),
    schema_version  BIGINT      NOT NULL DEFAULT 0,
    sequence_number BIGINT      NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    author          TEXT);

-- Pre-v0.39.0 snapshot_changes: has table_id.
CREATE TABLE IF NOT EXISTS {schema}.ducklake_snapshot_changes (
    snapshot_id BIGINT NOT NULL REFERENCES {schema}.ducklake_snapshot(snapshot_id),
    change_type TEXT   NOT NULL,
    table_id    BIGINT REFERENCES {schema}.ducklake_table(table_id),
    schema_id   BIGINT REFERENCES {schema}.ducklake_schema(schema_id),
    file_id     BIGINT);

CREATE TABLE IF NOT EXISTS {schema}.ducklake_table_stats (
    table_id BIGINT NOT NULL PRIMARY KEY REFERENCES {schema}.ducklake_table(table_id),
    next_row_id BIGINT NOT NULL DEFAULT 0, row_count BIGINT NOT NULL DEFAULT 0);

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

INSERT INTO {schema}.ducklake_metadata (key, value)
VALUES ('catalog_version', '1.0'), ('created_by', 'pg-tide-relay')
ON CONFLICT (key) DO NOTHING;
"#
    )
}

// ── Test 1: migration script applies cleanly ──────────────────────────────────

/// Verify that the `pg_tide--0.38.0--0.39.0.sql` migration script applies
/// cleanly after the full base schema is installed.
#[tokio::test]
async fn test_v039_migration_script_applies_cleanly() {
    let db = PgTideTestDb::start().await;

    // The full schema (v0.1.0 through v0.38.0) is already installed by PgTideTestDb::start().
    // Apply the v0.38.0→v0.39.0 upgrade.
    let processed = strip_extension_comments(UPGRADE_SQL);
    db.client
        .batch_execute(&processed)
        .await
        .expect("v0.38.0→v0.39.0 migration must apply without error");

    // Verify that tide.ducklake_migrate_catalog() function exists.
    let exists: bool = db
        .client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.routines \
             WHERE routine_schema = 'tide' \
               AND routine_name = 'ducklake_migrate_catalog')",
            &[],
        )
        .await
        .expect("check function exists")
        .get(0);
    assert!(
        exists,
        "tide.ducklake_migrate_catalog() must exist after v0.38.0→v0.39.0 migration"
    );
}

// ── Test 2: migrate_catalog upgrades pre-v0.39.0 catalog ─────────────────────

/// Verify that `tide.ducklake_migrate_catalog()` upgrades a pre-v0.39.0
/// DuckLake catalog to the full v1.0 spec.
#[tokio::test]
async fn test_v039_migrate_catalog_upgrades_pre_v39_schema() {
    let db = PgTideTestDb::start().await;
    let schema = "old_catalog_test";

    // Apply the upgrade script so tide.ducklake_migrate_catalog() is available.
    let processed = strip_extension_comments(UPGRADE_SQL);
    db.client
        .batch_execute(&processed)
        .await
        .expect("apply upgrade SQL");

    // Create the pre-v0.39.0 catalog schema.
    db.client
        .batch_execute(&pre_v39_catalog_ddl(schema))
        .await
        .expect("create pre-v39 catalog");

    // Seed some snapshot data in the old format.
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
                 VALUES (nextval('{schema}.ducklake_table_id_seq'), $1, 'orders') \
                 RETURNING table_id"
            ),
            &[&schema_id],
        )
        .await
        .expect("insert table")
        .get(0);

    // Insert two snapshots using old DDL.
    for i in 1i64..=2 {
        db.client
            .execute(
                &format!(
                    "INSERT INTO {schema}.ducklake_snapshot \
                     (snapshot_id, table_id, sequence_number, author) \
                     VALUES (nextval('{schema}.ducklake_snapshot_id_seq'), $1, $2, 'pg-tide-relay')"
                ),
                &[&table_id, &(i - 1)],
            )
            .await
            .expect("insert old snapshot");
    }

    // Run the migration function.
    let result_rows = db
        .client
        .query(
            &format!("SELECT snapshots_migrated, data_files_verified FROM tide.ducklake_migrate_catalog('{schema}')"),
            &[],
        )
        .await
        .expect("run ducklake_migrate_catalog");

    assert_eq!(result_rows.len(), 1, "should return one summary row");
    let snaps_migrated: i32 = result_rows[0].get("snapshots_migrated");
    assert_eq!(snaps_migrated, 2, "should have back-filled 2 snapshots");

    // Verify new columns exist in ducklake_snapshot.
    for col in &["snapshot_time", "next_catalog_id", "next_file_id"] {
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
            exists,
            "after migration, ducklake_snapshot must have column '{col}'"
        );
    }

    // Verify obsolete columns are gone.
    for col in &["table_id", "sequence_number"] {
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
            "after migration, ducklake_snapshot must NOT have column '{col}'"
        );
    }

    // Verify ducklake_snapshot_id_seq has been dropped.
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
        "after migration, ducklake_snapshot_id_seq must be dropped"
    );

    // Verify 18 new tables were added (count all tables that start with ducklake_).
    let table_count: i64 = db
        .client
        .query_one(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = $1 AND table_name LIKE 'ducklake_%'",
            &[&schema],
        )
        .await
        .expect("count tables")
        .get(0);
    assert_eq!(
        table_count, 28,
        "after migration, catalog must have 28 DuckLake tables"
    );

    // Verify spec version was set.
    let spec_ver: Option<String> = db
        .client
        .query_opt(
            &format!(
                "SELECT value FROM {schema}.ducklake_metadata WHERE key = 'ducklake_spec_version'"
            ),
            &[],
        )
        .await
        .expect("check spec version")
        .map(|r| r.get(0));
    assert_eq!(
        spec_ver.as_deref(),
        Some("1.0"),
        "migration must set ducklake_spec_version = '1.0'"
    );
}

// ── Test 3: migration is idempotent ──────────────────────────────────────────

/// Verify that `tide.ducklake_migrate_catalog()` is safe to run twice
/// (idempotent — second run should succeed without errors).
#[tokio::test]
async fn test_v039_migrate_catalog_idempotent() {
    let db = PgTideTestDb::start().await;
    let schema = "idem_catalog_test";

    let processed = strip_extension_comments(UPGRADE_SQL);
    db.client
        .batch_execute(&processed)
        .await
        .expect("apply upgrade SQL");

    db.client
        .batch_execute(&pre_v39_catalog_ddl(schema))
        .await
        .expect("create pre-v39 catalog");

    // Run migrate_catalog twice.
    db.client
        .query(
            &format!("SELECT * FROM tide.ducklake_migrate_catalog('{schema}')"),
            &[],
        )
        .await
        .expect("first migrate_catalog call");

    // Second call must also succeed (all operations are idempotent).
    db.client
        .query(
            &format!("SELECT * FROM tide.ducklake_migrate_catalog('{schema}')"),
            &[],
        )
        .await
        .expect("second migrate_catalog call must be idempotent");
}

// ── Test 4: back-fill verification ───────────────────────────────────────────

/// Verify that `tide.ducklake_migrate_catalog()` correctly back-fills
/// `next_catalog_id` and `next_file_id` in existing snapshot rows.
#[tokio::test]
async fn test_v039_migrate_catalog_backfills_ids() {
    let db = PgTideTestDb::start().await;
    let schema = "backfill_test";

    let processed = strip_extension_comments(UPGRADE_SQL);
    db.client
        .batch_execute(&processed)
        .await
        .expect("apply upgrade SQL");

    db.client
        .batch_execute(&pre_v39_catalog_ddl(schema))
        .await
        .expect("create pre-v39 catalog");

    // Seed some catalog rows to advance the sequences.
    let schema_id: i64 = db
        .client
        .query_one(
            &format!(
                "INSERT INTO {schema}.ducklake_schema (schema_id, schema_name) \
                 VALUES (nextval('{schema}.ducklake_schema_id_seq'), 'ns') RETURNING schema_id"
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
                 VALUES (nextval('{schema}.ducklake_table_id_seq'), $1, 'tbl') RETURNING table_id"
            ),
            &[&schema_id],
        )
        .await
        .expect("table")
        .get(0);

    // Advance column_id sequence to simulate columns being added.
    for _ in 0..5 {
        db.client
            .execute(
                &format!("SELECT nextval('{schema}.ducklake_column_id_seq')"),
                &[],
            )
            .await
            .expect("advance column seq");
    }

    // Insert one snapshot in old format.
    db.client
        .execute(
            &format!(
                "INSERT INTO {schema}.ducklake_snapshot \
                 (snapshot_id, table_id, sequence_number, author) \
                 VALUES (nextval('{schema}.ducklake_snapshot_id_seq'), $1, 0, 'test')"
            ),
            &[&table_id],
        )
        .await
        .expect("old snapshot");

    // Run migration.
    db.client
        .query(
            &format!("SELECT * FROM tide.ducklake_migrate_catalog('{schema}')"),
            &[],
        )
        .await
        .expect("migrate_catalog");

    // After migration, the snapshot should have next_catalog_id ≥ 5 (column seq advanced to 5).
    let row = db
        .client
        .query_one(
            &format!("SELECT next_catalog_id FROM {schema}.ducklake_snapshot LIMIT 1"),
            &[],
        )
        .await
        .expect("read back-filled snapshot");

    let next_cat_id: i64 = row.get(0);
    assert!(
        next_cat_id >= 5,
        "back-filled next_catalog_id should be ≥ 5 (sequence was advanced to 5), got {next_cat_id}"
    );
}

// ── Test 5: invalid schema name rejected ─────────────────────────────────────

/// Verify that `tide.ducklake_migrate_catalog()` rejects SQL-injection
/// attempts via an invalid schema name.
#[tokio::test]
async fn test_v039_migrate_catalog_rejects_invalid_schema_name() {
    let db = PgTideTestDb::start().await;

    let processed = strip_extension_comments(UPGRADE_SQL);
    db.client
        .batch_execute(&processed)
        .await
        .expect("apply upgrade SQL");

    // An invalid schema name containing SQL metacharacters should fail.
    let result = db
        .client
        .query(
            "SELECT * FROM tide.ducklake_migrate_catalog($1)",
            &[&"'; DROP TABLE tide.relay_outbox_config; --"],
        )
        .await;

    assert!(
        result.is_err(),
        "ducklake_migrate_catalog must reject invalid schema names"
    );
}
