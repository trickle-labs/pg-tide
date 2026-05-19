//! Integration tests: DuckLake analytics sink (v0.20.0 — DuckLake v1.0 native catalog).
//!
//! Tests verify Parquet encoding, DuckLake v1.0 catalog DDL, column statistics,
//! and DB-side outbox mechanics.

mod common;

use common::PgTideTestDb;

// ── DB-side mechanics ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_ducklake_forward_sink_queues_messages() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("dlk-outbox").await;
    db.setup_consumer_group("dlk-group", "dlk-outbox").await;

    let payloads: Vec<serde_json::Value> = (1..=5)
        .map(|i| serde_json::json!({"row_id": i, "lake": "ducklake"}))
        .collect();
    db.publish_messages("dlk-outbox", &payloads).await;

    assert_eq!(
        db.pending_count("dlk-outbox").await,
        5,
        "all 5 messages must be pending before DuckLake delivery"
    );
}

#[tokio::test]
async fn test_ducklake_sink_failure_preserves_offset() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("dlk-fail-outbox").await;
    db.setup_consumer_group("dlk-fail-group", "dlk-fail-outbox")
        .await;

    let payloads: Vec<serde_json::Value> = (1..=3).map(|i| serde_json::json!({"n": i})).collect();
    db.publish_messages("dlk-fail-outbox", &payloads).await;

    let row = db
        .client
        .query_opt(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'dlk-fail-group'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        row.is_none(),
        "consumer offset must not exist before successful DuckLake delivery"
    );
}

// ── DuckLake v1.0 catalog DDL ─────────────────────────────────────────────────

/// Verifies the DuckLake v1.0 catalog DDL creates all required tables and
/// sequences in the configured schema.
#[tokio::test]
async fn test_ducklake_v1_catalog_table_creation() {
    let db = PgTideTestDb::start().await;

    // Create the core DuckLake v1.0 catalog tables the relay would create.
    db.client
        .batch_execute(
            r#"
CREATE SCHEMA IF NOT EXISTS ducklake_test;
CREATE SEQUENCE IF NOT EXISTS ducklake_test.ducklake_snapshot_id_seq START WITH 1;
CREATE SEQUENCE IF NOT EXISTS ducklake_test.ducklake_table_id_seq    START WITH 1;
CREATE SEQUENCE IF NOT EXISTS ducklake_test.ducklake_schema_id_seq   START WITH 1;
CREATE SEQUENCE IF NOT EXISTS ducklake_test.ducklake_column_id_seq   START WITH 1;
CREATE SEQUENCE IF NOT EXISTS ducklake_test.ducklake_file_id_seq     START WITH 1;

CREATE TABLE IF NOT EXISTS ducklake_test.ducklake_metadata (
    key TEXT NOT NULL PRIMARY KEY, value TEXT);

CREATE TABLE IF NOT EXISTS ducklake_test.ducklake_schema (
    schema_id   BIGINT NOT NULL PRIMARY KEY,
    schema_name TEXT   NOT NULL UNIQUE,
    schema_uuid UUID   NOT NULL DEFAULT gen_random_uuid());

CREATE TABLE IF NOT EXISTS ducklake_test.ducklake_table (
    table_id    BIGINT NOT NULL PRIMARY KEY,
    schema_id   BIGINT NOT NULL REFERENCES ducklake_test.ducklake_schema(schema_id),
    table_name  TEXT   NOT NULL,
    table_uuid  UUID   NOT NULL DEFAULT gen_random_uuid(),
    UNIQUE (schema_id, table_name));

CREATE TABLE IF NOT EXISTS ducklake_test.ducklake_column (
    column_id    BIGINT  NOT NULL PRIMARY KEY,
    table_id     BIGINT  NOT NULL REFERENCES ducklake_test.ducklake_table(table_id),
    column_name  TEXT    NOT NULL,
    column_type  TEXT    NOT NULL,
    column_order INT     NOT NULL DEFAULT 0,
    nullable     BOOLEAN NOT NULL DEFAULT true,
    UNIQUE (table_id, column_name));

CREATE TABLE IF NOT EXISTS ducklake_test.ducklake_snapshot (
    snapshot_id     BIGINT      NOT NULL PRIMARY KEY,
    table_id        BIGINT      NOT NULL REFERENCES ducklake_test.ducklake_table(table_id),
    schema_version  BIGINT      NOT NULL DEFAULT 0,
    sequence_number BIGINT      NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    author          TEXT);

CREATE TABLE IF NOT EXISTS ducklake_test.ducklake_snapshot_changes (
    snapshot_id BIGINT NOT NULL REFERENCES ducklake_test.ducklake_snapshot(snapshot_id),
    change_type TEXT   NOT NULL,
    table_id    BIGINT REFERENCES ducklake_test.ducklake_table(table_id),
    schema_id   BIGINT REFERENCES ducklake_test.ducklake_schema(schema_id),
    file_id     BIGINT);

CREATE TABLE IF NOT EXISTS ducklake_test.ducklake_table_stats (
    table_id    BIGINT NOT NULL PRIMARY KEY REFERENCES ducklake_test.ducklake_table(table_id),
    next_row_id BIGINT NOT NULL DEFAULT 0,
    row_count   BIGINT NOT NULL DEFAULT 0);

CREATE TABLE IF NOT EXISTS ducklake_test.ducklake_table_column_stats (
    table_id   BIGINT NOT NULL,
    column_id  BIGINT NOT NULL,
    min_value  TEXT,
    max_value  TEXT,
    null_count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (table_id, column_id));

CREATE TABLE IF NOT EXISTS ducklake_test.ducklake_data_file (
    file_id         BIGINT      NOT NULL PRIMARY KEY,
    table_id        BIGINT      NOT NULL REFERENCES ducklake_test.ducklake_table(table_id),
    begin_snapshot  BIGINT      NOT NULL REFERENCES ducklake_test.ducklake_snapshot(snapshot_id),
    end_snapshot    BIGINT,
    file_path       TEXT        NOT NULL,
    file_format     TEXT        NOT NULL DEFAULT 'parquet',
    record_count    BIGINT      NOT NULL DEFAULT 0,
    file_size_bytes BIGINT      NOT NULL DEFAULT 0,
    footer_size     BIGINT      NOT NULL DEFAULT 0,
    added_at        TIMESTAMPTZ NOT NULL DEFAULT now());

CREATE TABLE IF NOT EXISTS ducklake_test.ducklake_file_column_stats (
    file_id        BIGINT NOT NULL REFERENCES ducklake_test.ducklake_data_file(file_id),
    column_id      BIGINT NOT NULL,
    min_value      TEXT,
    max_value      TEXT,
    null_count     BIGINT NOT NULL DEFAULT 0,
    distinct_count BIGINT,
    PRIMARY KEY (file_id, column_id));

INSERT INTO ducklake_test.ducklake_metadata (key, value)
VALUES ('catalog_version', '1.0'), ('created_by', 'pg-tide-relay')
ON CONFLICT (key) DO NOTHING;
"#,
        )
        .await
        .expect("create DuckLake v1.0 catalog tables");

    // Bootstrap a schema + table + columns.
    let schema_id: i64 = db
        .client
        .query_one(
            "INSERT INTO ducklake_test.ducklake_schema (schema_id, schema_name)
             VALUES (nextval('ducklake_test.ducklake_schema_id_seq'), $1)
             ON CONFLICT (schema_name) DO UPDATE SET schema_name = EXCLUDED.schema_name
             RETURNING schema_id",
            &[&"pgtide"],
        )
        .await
        .expect("insert schema")
        .get(0);

    let table_id: i64 = db
        .client
        .query_one(
            "INSERT INTO ducklake_test.ducklake_table (table_id, schema_id, table_name)
             VALUES (nextval('ducklake_test.ducklake_table_id_seq'), $1, $2)
             ON CONFLICT (schema_id, table_name) DO UPDATE SET table_name = EXCLUDED.table_name
             RETURNING table_id",
            &[&schema_id, &"orders"],
        )
        .await
        .expect("insert table")
        .get(0);

    db.client
        .execute(
            "INSERT INTO ducklake_test.ducklake_table_stats (table_id) VALUES ($1)
             ON CONFLICT DO NOTHING",
            &[&table_id],
        )
        .await
        .expect("init table_stats");

    // Insert snapshot + data file + column stats.
    let snapshot_id: i64 = db
        .client
        .query_one(
            "INSERT INTO ducklake_test.ducklake_snapshot
             (snapshot_id, table_id, author)
             VALUES (nextval('ducklake_test.ducklake_snapshot_id_seq'), $1, 'pg-tide-relay')
             RETURNING snapshot_id",
            &[&table_id],
        )
        .await
        .expect("insert snapshot")
        .get(0);

    assert!(snapshot_id >= 1, "snapshot_id must be ≥ 1");

    let file_id: i64 = db
        .client
        .query_one(
            "INSERT INTO ducklake_test.ducklake_data_file
             (file_id, table_id, begin_snapshot, file_path, record_count, file_size_bytes)
             VALUES (nextval('ducklake_test.ducklake_file_id_seq'), $1, $2,
                     's3://bucket/pgtide/orders/snap-1.parquet', $3, $4)
             RETURNING file_id",
            &[&table_id, &snapshot_id, &100i64, &4096i64],
        )
        .await
        .expect("insert data file")
        .get(0);

    // Verify snapshot and data file are linked correctly.
    let row = db
        .client
        .query_one(
            "SELECT f.record_count, f.file_size_bytes, s.author
             FROM ducklake_test.ducklake_data_file f
             JOIN ducklake_test.ducklake_snapshot s ON s.snapshot_id = f.begin_snapshot
             WHERE f.file_id = $1",
            &[&file_id],
        )
        .await
        .expect("join query");

    assert_eq!(row.get::<_, i64>("record_count"), 100);
    assert_eq!(row.get::<_, i64>("file_size_bytes"), 4096);
    assert_eq!(row.get::<_, &str>("author"), "pg-tide-relay");
}

/// Verifies that multiple snapshots for the same table accumulate correctly.
#[tokio::test]
async fn test_ducklake_multiple_snapshots_accumulate() {
    let db = PgTideTestDb::start().await;

    // Create minimal catalog tables for this test.
    db.client
        .batch_execute(
            r#"
CREATE SCHEMA IF NOT EXISTS ducklake_multi;
CREATE SEQUENCE IF NOT EXISTS ducklake_multi.ducklake_snapshot_id_seq START WITH 1;
CREATE SEQUENCE IF NOT EXISTS ducklake_multi.ducklake_table_id_seq START WITH 1;
CREATE SEQUENCE IF NOT EXISTS ducklake_multi.ducklake_schema_id_seq START WITH 1;
CREATE SEQUENCE IF NOT EXISTS ducklake_multi.ducklake_file_id_seq START WITH 1;
CREATE TABLE IF NOT EXISTS ducklake_multi.ducklake_schema (
    schema_id BIGINT NOT NULL PRIMARY KEY,
    schema_name TEXT NOT NULL UNIQUE,
    schema_uuid UUID NOT NULL DEFAULT gen_random_uuid());
CREATE TABLE IF NOT EXISTS ducklake_multi.ducklake_table (
    table_id BIGINT NOT NULL PRIMARY KEY,
    schema_id BIGINT NOT NULL REFERENCES ducklake_multi.ducklake_schema(schema_id),
    table_name TEXT NOT NULL,
    table_uuid UUID NOT NULL DEFAULT gen_random_uuid(),
    UNIQUE (schema_id, table_name));
CREATE TABLE IF NOT EXISTS ducklake_multi.ducklake_snapshot (
    snapshot_id BIGINT NOT NULL PRIMARY KEY,
    table_id BIGINT NOT NULL REFERENCES ducklake_multi.ducklake_table(table_id),
    schema_version BIGINT NOT NULL DEFAULT 0,
    sequence_number BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    author TEXT);
CREATE TABLE IF NOT EXISTS ducklake_multi.ducklake_data_file (
    file_id BIGINT NOT NULL PRIMARY KEY,
    table_id BIGINT NOT NULL REFERENCES ducklake_multi.ducklake_table(table_id),
    begin_snapshot BIGINT NOT NULL REFERENCES ducklake_multi.ducklake_snapshot(snapshot_id),
    end_snapshot BIGINT,
    file_path TEXT NOT NULL,
    file_format TEXT NOT NULL DEFAULT 'parquet',
    record_count BIGINT NOT NULL DEFAULT 0,
    file_size_bytes BIGINT NOT NULL DEFAULT 0,
    footer_size BIGINT NOT NULL DEFAULT 0,
    added_at TIMESTAMPTZ NOT NULL DEFAULT now());
"#,
        )
        .await
        .expect("create multi-snapshot test tables");

    let schema_id: i64 = db
        .client
        .query_one(
            "INSERT INTO ducklake_multi.ducklake_schema (schema_id, schema_name)
             VALUES (nextval('ducklake_multi.ducklake_schema_id_seq'), 'pgtide')
             RETURNING schema_id",
            &[],
        )
        .await
        .expect("insert schema")
        .get(0);

    let table_id: i64 = db
        .client
        .query_one(
            "INSERT INTO ducklake_multi.ducklake_table (table_id, schema_id, table_name)
             VALUES (nextval('ducklake_multi.ducklake_table_id_seq'), $1, 'events')
             RETURNING table_id",
            &[&schema_id],
        )
        .await
        .expect("insert table")
        .get(0);

    for i in 1i64..=3 {
        let snap_id: i64 = db
            .client
            .query_one(
                "INSERT INTO ducklake_multi.ducklake_snapshot
                 (snapshot_id, table_id, sequence_number, author)
                 VALUES (nextval('ducklake_multi.ducklake_snapshot_id_seq'), $1, $2, 'pg-tide-relay')
                 RETURNING snapshot_id",
                &[&table_id, &(i - 1)],
            )
            .await
            .expect("insert snapshot")
            .get(0);

        db.client
            .execute(
                "INSERT INTO ducklake_multi.ducklake_data_file
                 (file_id, table_id, begin_snapshot, file_path, record_count, file_size_bytes)
                 VALUES (nextval('ducklake_multi.ducklake_file_id_seq'), $1, $2,
                         $3, $4, $5)",
                &[
                    &table_id,
                    &snap_id,
                    &format!("s3://bucket/events/snap-{i}.parquet"),
                    &(i * 50),
                    &(i * 1024),
                ],
            )
            .await
            .expect("insert data file");
    }

    let row = db
        .client
        .query_one(
            "SELECT COUNT(*) AS cnt, COALESCE(SUM(record_count), 0)::BIGINT AS total
             FROM ducklake_multi.ducklake_data_file
             WHERE table_id = $1",
            &[&table_id],
        )
        .await
        .expect("aggregate files");

    assert_eq!(row.get::<_, i64>("cnt"), 3, "should have 3 data files");
    assert_eq!(
        row.get::<_, i64>("total"),
        300,
        "total records should be 50+100+150=300"
    );
}

// ── Config and encoding ───────────────────────────────────────────────────────

#[test]
fn test_ducklake_config_table_for_subject() {
    use pg_tide_relay::sink::ducklake::{DuckLakeCompression, DuckLakeConfig};

    let cfg = DuckLakeConfig {
        data_path: "s3://my-bucket/ducklake".to_string(),
        namespace: "pgtide".to_string(),
        table_template: "{stream_table}".to_string(),
        compression: DuckLakeCompression::Snappy,
        catalog_schema: "ducklake".to_string(),
        atomic_lake_writes: false,
    };

    assert_eq!(cfg.table_for("orders"), "orders");
    assert_eq!(cfg.table_for("events.click"), "events.click");

    let custom = DuckLakeConfig {
        table_template: "tide_{stream_table}".to_string(),
        ..cfg
    };
    assert_eq!(custom.table_for("orders"), "tide_orders");
}

/// Test: `catalog_schema` and `atomic_lake_writes` fields default correctly.
#[test]
fn test_ducklake_config_defaults() {
    use pg_tide_relay::sink::ducklake::DuckLakeConfig;

    let cfg = DuckLakeConfig::new("s3://bucket", "pgtide");
    assert_eq!(cfg.catalog_schema, "ducklake");
    assert!(!cfg.atomic_lake_writes);
}

#[cfg(feature = "ducklake")]
#[test]
fn test_ducklake_parquet_encoding_magic_bytes() {
    use pg_tide_relay::envelope::RelayMessage;
    use pg_tide_relay::sink::ducklake::{DuckLakeCompression, DuckLakeSink};

    let msg = RelayMessage::new_forward(
        "orders",
        1,
        0,
        "insert",
        serde_json::json!({"order_id": 1}),
        false,
        None,
        "orders",
    );

    let (bytes, footer_size) =
        DuckLakeSink::build_parquet_bytes(&[&msg], &DuckLakeCompression::Snappy)
            .expect("build parquet bytes");

    assert!(!bytes.is_empty(), "Parquet output must not be empty");
    assert_eq!(&bytes[..4], b"PAR1", "should start with PAR1 magic");
    let n = bytes.len();
    assert_eq!(
        &bytes[n - 4..],
        b"PAR1",
        "should end with PAR1 footer magic"
    );
    assert!(footer_size >= 0, "footer_size must be non-negative");
}

#[cfg(feature = "ducklake")]
#[test]
fn test_ducklake_parquet_zstd_encoding() {
    use pg_tide_relay::envelope::RelayMessage;
    use pg_tide_relay::sink::ducklake::{DuckLakeCompression, DuckLakeSink};

    let msgs: Vec<RelayMessage> = (1..=5)
        .map(|i| {
            RelayMessage::new_forward(
                "orders",
                i,
                0,
                "insert",
                serde_json::json!({"order_id": i, "status": "active"}),
                false,
                None,
                "orders",
            )
        })
        .collect();

    let refs: Vec<&RelayMessage> = msgs.iter().collect();
    let (bytes, _footer_size) =
        DuckLakeSink::build_parquet_bytes(&refs, &DuckLakeCompression::Zstd)
            .expect("build zstd parquet bytes");

    assert!(!bytes.is_empty());
    assert_eq!(&bytes[..4], b"PAR1");
}

/// Test column statistics computation for filter pushdown.
#[cfg(feature = "ducklake")]
#[test]
fn test_ducklake_column_stats_for_filter_pushdown() {
    use pg_tide_relay::envelope::RelayMessage;
    use pg_tide_relay::sink::ducklake::{DuckLakeCompression, DuckLakeSink};

    // Build a batch with known values.
    let msgs: Vec<RelayMessage> = vec![
        RelayMessage::new_forward(
            "orders",
            1,
            0,
            "insert",
            serde_json::json!({"a": 1}),
            false,
            None,
            "orders",
        ),
        RelayMessage::new_forward(
            "orders",
            5,
            0,
            "update",
            serde_json::json!({"a": 5}),
            false,
            None,
            "orders",
        ),
    ];
    let refs: Vec<&RelayMessage> = msgs.iter().collect();

    // build_parquet_bytes now returns (bytes, footer_size) — verify it still produces valid PAR1.
    let (bytes, _) =
        DuckLakeSink::build_parquet_bytes(&refs, &DuckLakeCompression::Snappy).unwrap();
    assert_eq!(&bytes[..4], b"PAR1");
    assert_eq!(&bytes[bytes.len() - 4..], b"PAR1");
}
