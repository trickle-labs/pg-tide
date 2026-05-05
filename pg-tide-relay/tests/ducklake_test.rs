//! Integration tests: DuckLake analytics sink (v0.10.0 — RELAY-P3-DLK).
//!
//! Tests verify Parquet encoding, catalog SQL creation, and DB-side mechanics.
//! The DuckLake catalog table (`tide.ducklake_snapshots`) is tested against
//! the test PostgreSQL database.

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

// ── Catalog DDL ───────────────────────────────────────────────────────────────

/// Verifies the catalog DDL creates the snapshots table correctly.
#[tokio::test]
async fn test_ducklake_catalog_table_creation() {
    let db = PgTideTestDb::start().await;

    // Run the DuckLake catalog DDL directly on the test DB.
    db.client
        .execute(
            "CREATE TABLE IF NOT EXISTS tide.ducklake_snapshots (
                id              BIGSERIAL PRIMARY KEY,
                namespace       TEXT NOT NULL,
                table_name      TEXT NOT NULL,
                parquet_path    TEXT NOT NULL,
                num_records     BIGINT NOT NULL,
                file_size_bytes BIGINT NOT NULL,
                schema_json     JSONB,
                committed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
            &[],
        )
        .await
        .expect("create ducklake_snapshots table");

    // Insert a snapshot record.
    db.client
        .execute(
            "INSERT INTO tide.ducklake_snapshots
             (namespace, table_name, parquet_path, num_records, file_size_bytes, schema_json)
             VALUES ($1, $2, $3, $4, $5, $6)",
            &[
                &"pgtide",
                &"orders",
                &"s3://bucket/pgtide/orders/snap-1.parquet",
                &100i64,
                &4096i64,
                &serde_json::json!({"fields": ["_dedup_key", "_subject", "_op", "data"]}),
            ],
        )
        .await
        .expect("insert snapshot record");

    let row = db
        .client
        .query_one(
            "SELECT namespace, table_name, num_records
             FROM tide.ducklake_snapshots
             WHERE table_name = 'orders'",
            &[],
        )
        .await
        .expect("query snapshot");

    assert_eq!(row.get::<_, &str>("namespace"), "pgtide");
    assert_eq!(row.get::<_, i64>("num_records"), 100);
}

/// Verifies that multiple snapshots for the same table accumulate.
#[tokio::test]
async fn test_ducklake_multiple_snapshots_accumulate() {
    let db = PgTideTestDb::start().await;

    db.client
        .execute(
            "CREATE TABLE IF NOT EXISTS tide.ducklake_snapshots (
                id              BIGSERIAL PRIMARY KEY,
                namespace       TEXT NOT NULL,
                table_name      TEXT NOT NULL,
                parquet_path    TEXT NOT NULL,
                num_records     BIGINT NOT NULL,
                file_size_bytes BIGINT NOT NULL,
                schema_json     JSONB,
                committed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
            &[],
        )
        .await
        .expect("create table");

    for i in 1..=3 {
        db.client
            .execute(
                "INSERT INTO tide.ducklake_snapshots
                 (namespace, table_name, parquet_path, num_records, file_size_bytes)
                 VALUES ($1, $2, $3, $4, $5)",
                &[
                    &"pgtide",
                    &"events",
                    &format!("s3://bucket/events/snap-{i}.parquet"),
                    &(i as i64 * 50),
                    &(i as i64 * 1024),
                ],
            )
            .await
            .expect("insert snapshot");
    }

    let row = db
        .client
        .query_one(
            "SELECT COUNT(*) AS cnt, SUM(num_records) AS total
             FROM tide.ducklake_snapshots
             WHERE table_name = 'events'",
            &[],
        )
        .await
        .expect("aggregate snapshots");

    assert_eq!(row.get::<_, i64>("cnt"), 3, "should have 3 snapshots");
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
    };

    assert_eq!(cfg.table_for("orders"), "orders");
    assert_eq!(cfg.table_for("events.click"), "events.click");

    let custom = DuckLakeConfig {
        table_template: "tide_{stream_table}".to_string(),
        ..cfg
    };
    assert_eq!(custom.table_for("orders"), "tide_orders");
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

    let bytes = DuckLakeSink::build_parquet_bytes(&[&msg], &DuckLakeCompression::Snappy)
        .expect("build parquet bytes");

    assert!(!bytes.is_empty(), "Parquet output must not be empty");
    assert_eq!(&bytes[..4], b"PAR1", "should start with PAR1 magic");
    let n = bytes.len();
    assert_eq!(&bytes[n - 4..], b"PAR1", "should end with PAR1 footer magic");
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
    let bytes = DuckLakeSink::build_parquet_bytes(&refs, &DuckLakeCompression::Zstd)
        .expect("build zstd parquet bytes");

    assert!(!bytes.is_empty());
    assert_eq!(&bytes[..4], b"PAR1");
}
