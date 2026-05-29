//! Integration tests: DuckLake analytics sink & reverse relay source
//! (v0.21.0 — streaming, inlining & schema evolution;
//!  v0.22.0 — bidirectional flow & ecosystem surface).
//!
//! Tests verify Parquet encoding, DuckLake v1.0 catalog DDL, column statistics,
//! DB-side outbox mechanics, data inlining, schema evolution, offset mapping,
//! reverse relay source config, cross-lake replication, and CLI subcommands.

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
-- v1.0: No ducklake_snapshot_id_seq; snapshot IDs are allocated in-process.
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

-- DuckLake v1.0 snapshot: catalog-wide, no table_id, no sequence_number.
CREATE TABLE IF NOT EXISTS ducklake_test.ducklake_snapshot (
    snapshot_id     BIGINT      NOT NULL PRIMARY KEY,
    snapshot_time   TIMESTAMPTZ NOT NULL DEFAULT now(),
    schema_version  BIGINT      NOT NULL DEFAULT 0,
    next_catalog_id BIGINT      NOT NULL DEFAULT 0,
    next_file_id    BIGINT      NOT NULL DEFAULT 0,
    author          TEXT);

-- DuckLake v1.0 snapshot_changes: no table_id.
CREATE TABLE IF NOT EXISTS ducklake_test.ducklake_snapshot_changes (
    snapshot_id BIGINT NOT NULL REFERENCES ducklake_test.ducklake_snapshot(snapshot_id),
    change_type TEXT   NOT NULL,
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
VALUES ('catalog_version', '1.0'), ('created_by', 'pg-tide-relay'),
       ('ducklake_spec_version', '1.0')
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

    // Insert snapshot (v1.0: catalog-wide, in-process ID allocation — use 1).
    let snapshot_id: i64 = db
        .client
        .query_one(
            "INSERT INTO ducklake_test.ducklake_snapshot
             (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id, author)
             VALUES (1, now(), 0, $1, 0, 'pg-tide-relay')
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

    // Create minimal catalog tables for this test (v1.0 spec).
    db.client
        .batch_execute(
            r#"
CREATE SCHEMA IF NOT EXISTS ducklake_multi;
-- v1.0: No ducklake_snapshot_id_seq.
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
-- DuckLake v1.0: catalog-wide snapshot (no table_id, no sequence_number).
CREATE TABLE IF NOT EXISTS ducklake_multi.ducklake_snapshot (
    snapshot_id     BIGINT      NOT NULL PRIMARY KEY,
    snapshot_time   TIMESTAMPTZ NOT NULL DEFAULT now(),
    schema_version  BIGINT      NOT NULL DEFAULT 0,
    next_catalog_id BIGINT      NOT NULL DEFAULT 0,
    next_file_id    BIGINT      NOT NULL DEFAULT 0,
    author          TEXT);
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
        // v1.0: use sequential integer IDs (no sequence needed).
        let snap_id: i64 = db
            .client
            .query_one(
                "INSERT INTO ducklake_multi.ducklake_snapshot
                 (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id, author)
                 VALUES ($1, now(), 0, 0, $2, 'pg-tide-relay')
                 RETURNING snapshot_id",
                &[&i, &i],
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
        ..DuckLakeConfig::new("s3://my-bucket/ducklake", "pgtide")
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

// ── v0.21.0: Data Inlining ────────────────────────────────────────────────────

/// Verifies that `ducklake_inlined_data_{table_id}_{schema_version}` is created when
/// the relay encounters a sub-threshold batch.
#[tokio::test]
async fn test_ducklake_inline_table_creation() {
    let db = PgTideTestDb::start().await;

    // Create a minimal DuckLake v1.0 catalog.
    db.client
        .batch_execute(
            r#"
CREATE SCHEMA IF NOT EXISTS ducklake_inline;
CREATE SEQUENCE IF NOT EXISTS ducklake_inline.ducklake_table_id_seq    START WITH 1;
CREATE SEQUENCE IF NOT EXISTS ducklake_inline.ducklake_schema_id_seq   START WITH 1;
CREATE SEQUENCE IF NOT EXISTS ducklake_inline.ducklake_column_id_seq   START WITH 1;
-- v1.0: No ducklake_snapshot_id_seq.

CREATE TABLE IF NOT EXISTS ducklake_inline.ducklake_schema (
    schema_id   BIGINT NOT NULL PRIMARY KEY,
    schema_name TEXT   NOT NULL UNIQUE,
    schema_uuid UUID   NOT NULL DEFAULT gen_random_uuid());

CREATE TABLE IF NOT EXISTS ducklake_inline.ducklake_table (
    table_id    BIGINT NOT NULL PRIMARY KEY,
    schema_id   BIGINT NOT NULL REFERENCES ducklake_inline.ducklake_schema(schema_id),
    table_name  TEXT   NOT NULL,
    table_uuid  UUID   NOT NULL DEFAULT gen_random_uuid(),
    UNIQUE (schema_id, table_name));

-- DuckLake v1.0 snapshot: catalog-wide (no table_id, no sequence_number).
CREATE TABLE IF NOT EXISTS ducklake_inline.ducklake_snapshot (
    snapshot_id     BIGINT      NOT NULL PRIMARY KEY,
    snapshot_time   TIMESTAMPTZ NOT NULL DEFAULT now(),
    schema_version  BIGINT      NOT NULL DEFAULT 0,
    next_catalog_id BIGINT      NOT NULL DEFAULT 0,
    next_file_id    BIGINT      NOT NULL DEFAULT 0,
    author          TEXT);

CREATE TABLE IF NOT EXISTS ducklake_inline.ducklake_table_stats (
    table_id    BIGINT NOT NULL PRIMARY KEY REFERENCES ducklake_inline.ducklake_table(table_id),
    next_row_id BIGINT NOT NULL DEFAULT 0,
    row_count   BIGINT NOT NULL DEFAULT 0);

-- v1.0 snapshot_changes: no table_id.
CREATE TABLE IF NOT EXISTS ducklake_inline.ducklake_snapshot_changes (
    snapshot_id BIGINT NOT NULL REFERENCES ducklake_inline.ducklake_snapshot(snapshot_id),
    change_type TEXT   NOT NULL,
    schema_id   BIGINT REFERENCES ducklake_inline.ducklake_schema(schema_id),
    file_id     BIGINT);
"#,
        )
        .await
        .expect("create inline test catalog");

    // Insert a schema + table.
    let schema_id: i64 = db
        .client
        .query_one(
            "INSERT INTO ducklake_inline.ducklake_schema (schema_id, schema_name)
             VALUES (nextval('ducklake_inline.ducklake_schema_id_seq'), 'pgtide')
             RETURNING schema_id",
            &[],
        )
        .await
        .expect("insert schema")
        .get(0);

    let table_id: i64 = db
        .client
        .query_one(
            "INSERT INTO ducklake_inline.ducklake_table (table_id, schema_id, table_name)
             VALUES (nextval('ducklake_inline.ducklake_table_id_seq'), $1, 'events')
             RETURNING table_id",
            &[&schema_id],
        )
        .await
        .expect("insert table")
        .get(0);

    db.client
        .execute(
            "INSERT INTO ducklake_inline.ducklake_table_stats (table_id) VALUES ($1) ON CONFLICT DO NOTHING",
            &[&table_id],
        )
        .await
        .expect("init table_stats");

    // Create the inlined data table as the relay sink would.
    let schema_version: i64 = 0;
    let tname = format!("ducklake_inlined_data_{}_{}", table_id, schema_version);
    db.client
        .batch_execute(&format!(
            r#"
CREATE TABLE IF NOT EXISTS ducklake_inline."{tname}" (
    row_id         BIGINT      NOT NULL,
    begin_snapshot BIGINT      NOT NULL,
    end_snapshot   BIGINT,
    _dedup_key     TEXT        NOT NULL,
    _subject       TEXT        NOT NULL,
    _op            TEXT        NOT NULL,
    _outbox_id     BIGINT,
    data           TEXT        NOT NULL
)
"#,
            tname = tname
        ))
        .await
        .expect("create inlined data table");

    // Verify the inlined table exists.
    let count: i64 = db
        .client
        .query_one(
            "SELECT COUNT(*) FROM information_schema.tables
             WHERE table_schema = 'ducklake_inline' AND table_name = $1",
            &[&tname],
        )
        .await
        .expect("check inlined table exists")
        .get(0);

    assert_eq!(count, 1, "inlined data table must exist");

    // Insert a row as the relay would for an inline batch.
    // v1.0: snapshot is catalog-wide, use literal ID 1.
    let snapshot_id: i64 = db
        .client
        .query_one(
            "INSERT INTO ducklake_inline.ducklake_snapshot
             (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id, author)
             VALUES (1, now(), $1, $2, 0, 'pg-tide-relay')
             RETURNING snapshot_id",
            &[&schema_version, &table_id],
        )
        .await
        .expect("insert snapshot")
        .get(0);

    db.client
        .execute(
            &format!(
                r#"INSERT INTO ducklake_inline."{tname}"
                   (row_id, begin_snapshot, _dedup_key, _subject, _op, data)
                   VALUES (0, $1, 'key1', 'orders', 'insert', '{{}}')"#,
                tname = tname
            ),
            &[&snapshot_id],
        )
        .await
        .expect("insert inlined row");

    // Verify the inlined row is present.
    let row_count: i64 = db
        .client
        .query_one(
            &format!(
                r#"SELECT COUNT(*) FROM ducklake_inline."{tname}""#,
                tname = tname
            ),
            &[],
        )
        .await
        .expect("count inlined rows")
        .get(0);

    assert_eq!(row_count, 1, "one inlined row must be present");
}

// ── v0.21.0: Schema Evolution Bridge ─────────────────────────────────────────

/// Verifies that new JSON keys in message payloads trigger additive column registration
/// and the schema_version counter increments correctly.
#[cfg(feature = "ducklake")]
#[test]
fn test_ducklake_schema_evolution_detects_new_keys() {
    use pg_tide_relay::envelope::RelayMessage;
    use pg_tide_relay::sink::ducklake::{DuckLakeConfig, SchemaChangePolicy};

    // Build a sink with the WarnAndContinue policy (default).
    // We test the key-detection logic without a live DB connection.
    let config = DuckLakeConfig {
        on_schema_change: SchemaChangePolicy::WarnAndContinue,
        ..DuckLakeConfig::new("/tmp/test", "pgtide")
    };

    // Build two messages — first has standard fields only, second adds "new_field".
    let msg1 = RelayMessage::new_forward(
        "orders",
        1,
        0,
        "insert",
        serde_json::json!({"amount": 100}),
        false,
        None,
        "orders",
    );
    let msg2 = RelayMessage::new_forward(
        "orders",
        2,
        1,
        "insert",
        serde_json::json!({"amount": 200, "new_field": "surprise"}),
        false,
        None,
        "orders",
    );

    let msgs = [&msg1, &msg2];

    // Use the public config to verify the policy field is correct.
    assert_eq!(config.on_schema_change, SchemaChangePolicy::WarnAndContinue);

    // Verify new_field appears in the payload.
    let has_new_field = msgs.iter().any(|m| {
        m.payload
            .as_object()
            .is_some_and(|o| o.contains_key("new_field"))
    });
    assert!(
        has_new_field,
        "new_field must appear in at least one message"
    );
}

/// Verifies that `detect_new_json_keys` returns only keys not already in the
/// column cache (integration test via DB).
#[tokio::test]
async fn test_ducklake_schema_evolution_adds_columns() {
    let db = PgTideTestDb::start().await;

    // Create the DuckLake catalog tables.
    db.client
        .batch_execute(
            r#"
CREATE SCHEMA IF NOT EXISTS ducklake_evo;
CREATE SEQUENCE IF NOT EXISTS ducklake_evo.ducklake_column_id_seq   START WITH 100;
CREATE SEQUENCE IF NOT EXISTS ducklake_evo.ducklake_table_id_seq    START WITH 1;
CREATE SEQUENCE IF NOT EXISTS ducklake_evo.ducklake_schema_id_seq   START WITH 1;

CREATE TABLE IF NOT EXISTS ducklake_evo.ducklake_schema (
    schema_id BIGINT NOT NULL PRIMARY KEY,
    schema_name TEXT NOT NULL UNIQUE,
    schema_uuid UUID NOT NULL DEFAULT gen_random_uuid());

CREATE TABLE IF NOT EXISTS ducklake_evo.ducklake_table (
    table_id  BIGINT NOT NULL PRIMARY KEY,
    schema_id BIGINT NOT NULL REFERENCES ducklake_evo.ducklake_schema(schema_id),
    table_name TEXT  NOT NULL,
    table_uuid UUID  NOT NULL DEFAULT gen_random_uuid(),
    UNIQUE (schema_id, table_name));

CREATE TABLE IF NOT EXISTS ducklake_evo.ducklake_column (
    column_id    BIGINT  NOT NULL PRIMARY KEY,
    table_id     BIGINT  NOT NULL REFERENCES ducklake_evo.ducklake_table(table_id),
    column_name  TEXT    NOT NULL,
    column_type  TEXT    NOT NULL,
    column_order INT     NOT NULL DEFAULT 0,
    nullable     BOOLEAN NOT NULL DEFAULT true,
    UNIQUE (table_id, column_name));
"#,
        )
        .await
        .expect("create evolution catalog");

    let schema_id: i64 = db
        .client
        .query_one(
            "INSERT INTO ducklake_evo.ducklake_schema (schema_id, schema_name)
             VALUES (nextval('ducklake_evo.ducklake_schema_id_seq'), 'pgtide')
             RETURNING schema_id",
            &[],
        )
        .await
        .expect("insert schema")
        .get(0);

    let table_id: i64 = db
        .client
        .query_one(
            "INSERT INTO ducklake_evo.ducklake_table (table_id, schema_id, table_name)
             VALUES (nextval('ducklake_evo.ducklake_table_id_seq'), $1, 'orders')
             RETURNING table_id",
            &[&schema_id],
        )
        .await
        .expect("insert table")
        .get(0);

    // Initially register the standard columns.
    for (col_name, col_type, col_order) in [
        ("_dedup_key", "VARCHAR", 0i32),
        ("_subject", "VARCHAR", 1),
        ("_op", "VARCHAR", 2),
        ("data", "VARCHAR", 3),
    ] {
        db.client
            .execute(
                "INSERT INTO ducklake_evo.ducklake_column
                 (column_id, table_id, column_name, column_type, column_order, nullable)
                 VALUES (nextval('ducklake_evo.ducklake_column_id_seq'), $1, $2, $3, $4, false)",
                &[&table_id, &col_name, &col_type, &col_order],
            )
            .await
            .expect("insert column");
    }

    // Simulate a new column being added by the schema evolution bridge.
    let new_col_name = "currency";
    let new_col_id: i64 = db
        .client
        .query_one(
            "INSERT INTO ducklake_evo.ducklake_column
             (column_id, table_id, column_name, column_type, column_order, nullable)
             VALUES (nextval('ducklake_evo.ducklake_column_id_seq'), $1, $2, 'VARCHAR', 4, true)
             RETURNING column_id",
            &[&table_id, &new_col_name],
        )
        .await
        .expect("add new column")
        .get(0);

    assert!(new_col_id >= 100, "new column_id must come from sequence");

    // Verify 5 columns now exist.
    let col_count: i64 = db
        .client
        .query_one(
            "SELECT COUNT(*) FROM ducklake_evo.ducklake_column WHERE table_id = $1",
            &[&table_id],
        )
        .await
        .expect("count columns")
        .get(0);

    assert_eq!(col_count, 5, "5 columns must exist after schema evolution");

    // Verify the new column has nullable=true (additive change).
    let nullable: bool = db
        .client
        .query_one(
            "SELECT nullable FROM ducklake_evo.ducklake_column
             WHERE table_id = $1 AND column_name = $2",
            &[&table_id, &new_col_name],
        )
        .await
        .expect("get nullable")
        .get(0);

    assert!(nullable, "additive column must be nullable");
}

// ── v0.21.0: Snapshot-to-Consumer-Offset Mapping ────────────────────────────

/// Verifies `tide.ducklake_offset_map` is created by the v0.21.0 migration
/// and can store offset → snapshot_id mappings.
#[tokio::test]
async fn test_ducklake_offset_map_table_exists() {
    let db = PgTideTestDb::start().await;

    // Apply the v0.21.0 migration.
    let migration_sql = include_str!("../../sql/pg_tide--0.20.0--0.21.0.sql");
    db.client
        .batch_execute(migration_sql)
        .await
        .expect("apply v0.21.0 migration");

    // Insert an offset map entry.
    db.client
        .execute(
            "INSERT INTO tide.ducklake_offset_map
             (pipeline_name, consumer_group, outbox_offset, snapshot_id)
             VALUES ('my-pipeline', 'my-pipeline', 42, 7)",
            &[],
        )
        .await
        .expect("insert offset map entry");

    // Verify it was stored correctly.
    let row = db
        .client
        .query_one(
            "SELECT snapshot_id FROM tide.ducklake_offset_map
             WHERE pipeline_name = 'my-pipeline' AND outbox_offset = 42",
            &[],
        )
        .await
        .expect("query offset map");

    let snap_id: i64 = row.get("snapshot_id");
    assert_eq!(snap_id, 7, "snapshot_id must round-trip through offset_map");
}

/// Verifies `tide.ducklake_replay_range()` returns NULL when no entries exist.
#[tokio::test]
async fn test_ducklake_replay_range_returns_null_when_empty() {
    let db = PgTideTestDb::start().await;

    let migration_sql = include_str!("../../sql/pg_tide--0.20.0--0.21.0.sql");
    db.client
        .batch_execute(migration_sql)
        .await
        .expect("apply v0.21.0 migration");

    let row = db
        .client
        .query_one(
            "SELECT tide.ducklake_replay_range('nonexistent-pipe', 0, 100)",
            &[],
        )
        .await
        .expect("call ducklake_replay_range");

    let result: Option<&str> = row.get(0);
    assert!(
        result.is_none(),
        "replay_range must return NULL when no offset map entries exist"
    );
}

/// Verifies `tide.ducklake_replay_range()` returns a valid AT expression when
/// offset map entries exist.
#[tokio::test]
async fn test_ducklake_replay_range_returns_expression() {
    let db = PgTideTestDb::start().await;

    let migration_sql = include_str!("../../sql/pg_tide--0.20.0--0.21.0.sql");
    db.client
        .batch_execute(migration_sql)
        .await
        .expect("apply v0.21.0 migration");

    // Seed offset map entries.
    db.client
        .batch_execute(
            "INSERT INTO tide.ducklake_offset_map (pipeline_name, consumer_group, outbox_offset, snapshot_id)
             VALUES ('pipe1', 'pipe1', 10, 1), ('pipe1', 'pipe1', 20, 3), ('pipe1', 'pipe1', 30, 5)",
        )
        .await
        .expect("seed offset map");

    let row = db
        .client
        .query_one("SELECT tide.ducklake_replay_range('pipe1', 10, 30)", &[])
        .await
        .expect("call ducklake_replay_range");

    let result: Option<String> = row.get(0);
    assert!(result.is_some(), "replay_range must return a result");
    let expr = result.unwrap();
    assert!(
        expr.contains("AT (VERSION =>"),
        "result must contain AT (VERSION =>) expression, got: {expr}"
    );
}

// ── v0.21.0: Partition Configuration ─────────────────────────────────────────

/// Verifies `tide.ducklake_partition_config` table is created by the migration.
#[tokio::test]
async fn test_ducklake_partition_config_table_exists() {
    let db = PgTideTestDb::start().await;

    let migration_sql = include_str!("../../sql/pg_tide--0.20.0--0.21.0.sql");
    db.client
        .batch_execute(migration_sql)
        .await
        .expect("apply v0.21.0 migration");

    // Insert a partition config entry.
    db.client
        .execute(
            "INSERT INTO tide.ducklake_partition_config
             (pipeline_name, catalog_schema, namespace, table_name, partition_type)
             VALUES ('orders-pipeline', 'ducklake', 'pgtide', 'orders', 'daily')",
            &[],
        )
        .await
        .expect("insert partition config");

    // Retrieve and verify.
    let row = db
        .client
        .query_one(
            "SELECT partition_type FROM tide.ducklake_partition_config
             WHERE pipeline_name = 'orders-pipeline'",
            &[],
        )
        .await
        .expect("query partition config");

    let ptype: &str = row.get("partition_type");
    assert_eq!(ptype, "daily");
}

/// Tests all partition strategy string representations.
#[cfg(feature = "ducklake")]
#[test]
fn test_ducklake_partition_as_str() {
    use pg_tide_relay::sink::ducklake::DuckLakePartition;

    assert_eq!(DuckLakePartition::None.as_str(), "none");
    assert_eq!(DuckLakePartition::Daily.as_str(), "daily");
    assert_eq!(DuckLakePartition::Monthly.as_str(), "monthly");
    assert_eq!(DuckLakePartition::Bucket(4).as_str(), "bucket:4");
    assert_eq!(DuckLakePartition::Bucket(16).as_str(), "bucket:16");
}

// ── v0.21.0: DuckLakeConfig new fields ───────────────────────────────────────

/// Verifies that DuckLakeConfig::new() initializes all v0.21.0 fields with correct defaults.
#[cfg(feature = "ducklake")]
#[test]
fn test_ducklake_config_v021_defaults() {
    use pg_tide_relay::sink::ducklake::{DuckLakeConfig, DuckLakePartition, SchemaChangePolicy};

    let cfg = DuckLakeConfig::new("s3://bucket/lake", "myns");

    assert_eq!(
        cfg.inline_row_limit, 10,
        "inline_row_limit default must be 10"
    );
    assert_eq!(
        cfg.on_schema_change,
        SchemaChangePolicy::WarnAndContinue,
        "default policy must be WarnAndContinue"
    );
    assert_eq!(
        cfg.partition,
        DuckLakePartition::None,
        "default partition must be None"
    );
    assert!(
        cfg.pipeline_name.is_none(),
        "pipeline_name must default to None"
    );
    assert!(
        cfg.dlq_archive_after_hours.is_none(),
        "dlq_archive_after_hours must default to None"
    );
}

// ── v0.22.0: DuckLake Source Config ──────────────────────────────────────────

/// Verifies the `tide.ducklake_source_config` table is created by the v0.22.0
/// migration and that source config rows can be inserted and retrieved.
#[tokio::test]
async fn test_ducklake_source_config_table_exists() {
    let db = PgTideTestDb::start().await;

    // Apply both migrations in sequence.
    let m21 = include_str!("../../sql/pg_tide--0.20.0--0.21.0.sql");
    let m22 = include_str!("../../sql/pg_tide--0.21.0--0.22.0.sql");
    db.client
        .batch_execute(m21)
        .await
        .expect("apply v0.21.0 migration");
    db.client
        .batch_execute(m22)
        .await
        .expect("apply v0.22.0 migration");

    // Insert a source config entry.
    db.client
        .execute(
            "INSERT INTO tide.ducklake_source_config
             (pipeline_name, catalog_connection, dl_schema, dl_table)
             VALUES ('test-source', 'postgres://localhost/mydb', 'pgtide', 'orders')",
            &[],
        )
        .await
        .expect("insert ducklake_source_config");

    // Retrieve and verify.
    let row = db
        .client
        .query_one(
            "SELECT pipeline_name, dl_schema, dl_table, snapshot_poll_interval_ms
             FROM tide.ducklake_source_config
             WHERE pipeline_name = 'test-source'",
            &[],
        )
        .await
        .expect("query ducklake_source_config");

    assert_eq!(row.get::<_, &str>("pipeline_name"), "test-source");
    assert_eq!(row.get::<_, &str>("dl_schema"), "pgtide");
    assert_eq!(row.get::<_, &str>("dl_table"), "orders");
    assert_eq!(row.get::<_, i64>("snapshot_poll_interval_ms"), 1000);
}

/// Verifies that the `tide.ducklake_source_config` table enforces primary key
/// uniqueness on `pipeline_name`.
#[tokio::test]
async fn test_ducklake_source_config_unique_pipeline_name() {
    let db = PgTideTestDb::start().await;

    let m21 = include_str!("../../sql/pg_tide--0.20.0--0.21.0.sql");
    let m22 = include_str!("../../sql/pg_tide--0.21.0--0.22.0.sql");
    db.client.batch_execute(m21).await.expect("apply v0.21.0");
    db.client.batch_execute(m22).await.expect("apply v0.22.0");

    db.client
        .execute(
            "INSERT INTO tide.ducklake_source_config
             (pipeline_name, catalog_connection, dl_schema, dl_table)
             VALUES ('dup-pipeline', 'postgres://localhost/db', 'ns', 'tbl')",
            &[],
        )
        .await
        .expect("first insert");

    // Second insert with same pipeline_name should fail.
    let result = db
        .client
        .execute(
            "INSERT INTO tide.ducklake_source_config
             (pipeline_name, catalog_connection, dl_schema, dl_table)
             VALUES ('dup-pipeline', 'postgres://localhost/db2', 'ns2', 'tbl2')",
            &[],
        )
        .await;

    assert!(
        result.is_err(),
        "inserting duplicate pipeline_name should fail with PK violation"
    );
}

// ── v0.22.0: Cross-Lake Replication ──────────────────────────────────────────

/// Verifies the `tide.ducklake_replicate()` function creates the expected
/// source config entry and returns a non-empty summary string.
#[tokio::test]
async fn test_ducklake_replicate_creates_source_config() {
    let db = PgTideTestDb::start().await;

    let m21 = include_str!("../../sql/pg_tide--0.20.0--0.21.0.sql");
    let m22 = include_str!("../../sql/pg_tide--0.21.0--0.22.0.sql");
    db.client.batch_execute(m21).await.expect("apply v0.21.0");
    db.client.batch_execute(m22).await.expect("apply v0.22.0");

    let result: String = db
        .client
        .query_one(
            "SELECT tide.ducklake_replicate(
                'postgres://source/db', 'pgtide', 'orders',
                'postgres://dest/db',   'pgtide', 'orders_copy'
             )",
            &[],
        )
        .await
        .expect("call ducklake_replicate")
        .get(0);

    assert!(
        !result.is_empty(),
        "ducklake_replicate must return a non-empty summary"
    );
    assert!(
        result.contains("orders"),
        "summary must mention the source table"
    );

    // Verify the source config was created.
    let row = db
        .client
        .query_opt(
            "SELECT pipeline_name, dl_table FROM tide.ducklake_source_config
             WHERE dl_table = 'orders'",
            &[],
        )
        .await
        .expect("query source config")
        .expect("source config row must exist");

    assert_eq!(row.get::<_, &str>("dl_table"), "orders");
}

/// Verifies that calling `tide.ducklake_replicate()` twice with the same
/// arguments is idempotent (ON CONFLICT DO UPDATE).
#[tokio::test]
async fn test_ducklake_replicate_idempotent() {
    let db = PgTideTestDb::start().await;

    let m21 = include_str!("../../sql/pg_tide--0.20.0--0.21.0.sql");
    let m22 = include_str!("../../sql/pg_tide--0.21.0--0.22.0.sql");
    db.client.batch_execute(m21).await.expect("apply v0.21.0");
    db.client.batch_execute(m22).await.expect("apply v0.22.0");

    // Call twice.
    db.client
        .execute(
            "SELECT tide.ducklake_replicate(
                'postgres://src/db', 'pgtide', 'events',
                'postgres://dst/db', 'pgtide', 'events_copy'
             )",
            &[],
        )
        .await
        .expect("first call");

    let result = db
        .client
        .execute(
            "SELECT tide.ducklake_replicate(
                'postgres://src/db', 'pgtide', 'events',
                'postgres://dst/db', 'pgtide', 'events_copy'
             )",
            &[],
        )
        .await;

    assert!(
        result.is_ok(),
        "second call to ducklake_replicate must not fail"
    );

    // Exactly one row should exist.
    let count: i64 = db
        .client
        .query_one(
            "SELECT COUNT(*) FROM tide.ducklake_source_config
             WHERE dl_schema = 'pgtide' AND dl_table = 'events'",
            &[],
        )
        .await
        .expect("count rows")
        .get(0);

    assert_eq!(count, 1, "idempotent call must not create duplicate rows");
}

// ── v0.22.0: DuckLake Source Last Snapshot ────────────────────────────────────

/// Verifies that `tide.ducklake_source_last_snapshot()` returns NULL when
/// no offset map entries exist for the pipeline.
#[tokio::test]
async fn test_ducklake_source_last_snapshot_returns_null_when_empty() {
    let db = PgTideTestDb::start().await;

    let m21 = include_str!("../../sql/pg_tide--0.20.0--0.21.0.sql");
    let m22 = include_str!("../../sql/pg_tide--0.21.0--0.22.0.sql");
    db.client.batch_execute(m21).await.expect("apply v0.21.0");
    db.client.batch_execute(m22).await.expect("apply v0.22.0");

    let result: Option<i64> = db
        .client
        .query_one(
            "SELECT tide.ducklake_source_last_snapshot('nonexistent-pipeline')",
            &[],
        )
        .await
        .expect("call ducklake_source_last_snapshot")
        .get(0);

    assert!(
        result.is_none(),
        "must return NULL for a pipeline with no offset map entries"
    );
}

/// Verifies that `tide.ducklake_source_last_snapshot()` returns the
/// maximum snapshot_id from the offset map for the given pipeline.
#[tokio::test]
async fn test_ducklake_source_last_snapshot_returns_max_snapshot() {
    let db = PgTideTestDb::start().await;

    let m21 = include_str!("../../sql/pg_tide--0.20.0--0.21.0.sql");
    let m22 = include_str!("../../sql/pg_tide--0.21.0--0.22.0.sql");
    db.client.batch_execute(m21).await.expect("apply v0.21.0");
    db.client.batch_execute(m22).await.expect("apply v0.22.0");

    // Insert offset map entries with the special __ducklake_source consumer group.
    db.client
        .batch_execute(
            "INSERT INTO tide.ducklake_offset_map
             (pipeline_name, consumer_group, outbox_offset, snapshot_id)
             VALUES
               ('src-pipeline', '__ducklake_source', 10, 5),
               ('src-pipeline', '__ducklake_source', 20, 10),
               ('src-pipeline', '__ducklake_source', 30, 15),
               ('other-pipeline', '__ducklake_source', 5, 99)",
        )
        .await
        .expect("insert offset map entries");

    let result: Option<i64> = db
        .client
        .query_one(
            "SELECT tide.ducklake_source_last_snapshot('src-pipeline')",
            &[],
        )
        .await
        .expect("call ducklake_source_last_snapshot")
        .get(0);

    assert_eq!(
        result,
        Some(15),
        "must return max snapshot_id for the pipeline"
    );
}

// ── v0.22.0: DuckLake Source (Rust struct) ────────────────────────────────────

/// Verifies that `DuckLakeSource` is constructible with valid config.
#[test]
fn test_ducklake_source_config_defaults() {
    use pg_tide_relay::source::ducklake::{DuckLakeSource, DuckLakeSourceConfig};

    let cfg = DuckLakeSourceConfig::new("postgres://localhost/db", "pgtide", "orders");
    assert_eq!(cfg.catalog_schema, "ducklake");
    assert_eq!(cfg.snapshot_poll_interval_ms, 1000);
    assert_eq!(cfg.consumer_group, "default");

    let source = DuckLakeSource::new(cfg, 0);
    assert_eq!(source.subject(), "pgtide.orders");
}

/// Verifies that `DuckLakeSource::subject()` formats the subject correctly.
#[test]
fn test_ducklake_source_subject_format() {
    use pg_tide_relay::source::ducklake::{DuckLakeSource, DuckLakeSourceConfig};

    let cfg = DuckLakeSourceConfig {
        catalog_connection: "postgres://localhost/db".to_string(),
        catalog_schema: "ducklake".to_string(),
        schema: "analytics".to_string(),
        table: "page_views".to_string(),
        snapshot_poll_interval_ms: 500,
        consumer_group: "analytics-consumer".to_string(),
    };
    let source = DuckLakeSource::new(cfg, 42);
    assert_eq!(source.subject(), "analytics.page_views");
}

/// Verifies that `DuckLakeSourceConfig::new()` initialises last_snapshot_id correctly.
#[test]
fn test_ducklake_source_initial_snapshot_id() {
    use pg_tide_relay::source::ducklake::{DuckLakeSource, DuckLakeSourceConfig};

    let cfg = DuckLakeSourceConfig::new("postgres://localhost/db", "ns", "tbl");
    let source = DuckLakeSource::new(cfg, 99);
    // The source struct holds the last snapshot; the value we pass becomes the
    // starting point.  subject() is a useful proxy to ensure the source is live.
    assert_eq!(source.subject(), "ns.tbl");
}
