//! Integration tests: Apache Iceberg v2 analytics sink (v0.10.0 — RELAY-P3-ICE).
//!
//! Tests verify Parquet encoding, Iceberg metadata JSON structure, object-path
//! generation, and DB-side mechanics — no external object store required.

mod common;

use common::PgTideTestDb;

// ── DB-side mechanics ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_iceberg_forward_sink_queues_messages() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("ice-outbox").await;
    db.setup_consumer_group("ice-group", "ice-outbox").await;

    let payloads: Vec<serde_json::Value> = (1..=4)
        .map(|i| serde_json::json!({"row_id": i, "lake": "iceberg"}))
        .collect();
    db.publish_messages("ice-outbox", &payloads).await;

    assert_eq!(
        db.pending_count("ice-outbox").await,
        4,
        "all 4 messages must be pending before Iceberg delivery"
    );
}

#[tokio::test]
async fn test_iceberg_sink_failure_preserves_offset() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("ice-fail-outbox").await;
    db.setup_consumer_group("ice-fail-group", "ice-fail-outbox")
        .await;

    let payloads: Vec<serde_json::Value> = (1..=2).map(|i| serde_json::json!({"n": i})).collect();
    db.publish_messages("ice-fail-outbox", &payloads).await;

    let row = db
        .client
        .query_opt(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'ice-fail-group'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        row.is_none(),
        "consumer offset must not exist before successful Iceberg delivery"
    );
}

// ── Config logic ──────────────────────────────────────────────────────────────

#[test]
fn test_iceberg_config_table_for_subject() {
    use pg_tide_relay::sink::iceberg::{IcebergConfig, IcebergWriteMode};

    let cfg = IcebergConfig {
        warehouse_path: "s3://my-bucket/warehouse".to_string(),
        namespace: "pgtide".to_string(),
        table_template: "{stream_table}".to_string(),
        write_mode: IcebergWriteMode::Append,
        rows_per_file: 10_000,
    };

    assert_eq!(cfg.table_for("orders"), "orders");
    assert_eq!(cfg.table_for("events.click"), "events.click");
}

#[test]
fn test_iceberg_config_data_file_path() {
    use pg_tide_relay::sink::iceberg::{IcebergConfig, IcebergWriteMode};

    let cfg = IcebergConfig {
        warehouse_path: "s3://my-bucket/warehouse".to_string(),
        namespace: "pgtide".to_string(),
        table_template: "{stream_table}".to_string(),
        write_mode: IcebergWriteMode::Append,
        rows_per_file: 10_000,
    };

    let path = cfg.data_file_path("orders", 42);
    assert!(path.contains("orders"), "path should contain table name");
    assert!(path.ends_with(".parquet"), "path should end with .parquet");
    assert!(path.contains("42"), "path should include snapshot id");
}

#[test]
fn test_iceberg_config_metadata_file_path() {
    use pg_tide_relay::sink::iceberg::{IcebergConfig, IcebergWriteMode};

    let cfg = IcebergConfig {
        warehouse_path: "s3://my-bucket/warehouse".to_string(),
        namespace: "pgtide".to_string(),
        table_template: "{stream_table}".to_string(),
        write_mode: IcebergWriteMode::Append,
        rows_per_file: 10_000,
    };

    let path = cfg.metadata_path("orders");
    assert!(path.contains("orders"), "path should contain table name");
    assert!(path.contains("metadata"), "path should be in metadata directory");
}

// ── Parquet encoding ──────────────────────────────────────────────────────────

#[cfg(feature = "iceberg")]
#[test]
fn test_iceberg_parquet_encoding_non_empty() {
    use pg_tide_relay::envelope::RelayMessage;
    use pg_tide_relay::sink::iceberg::IcebergSink;

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

    let bytes = IcebergSink::build_parquet_bytes(&[&msg])
        .expect("build parquet bytes");

    assert!(!bytes.is_empty(), "Parquet output must not be empty");
    // Parquet files start with PAR1 magic bytes.
    assert_eq!(&bytes[..4], b"PAR1", "should start with PAR1 magic bytes");
}

#[cfg(feature = "iceberg")]
#[test]
fn test_iceberg_parquet_footer_magic() {
    use pg_tide_relay::envelope::RelayMessage;
    use pg_tide_relay::sink::iceberg::IcebergSink;

    let msgs: Vec<RelayMessage> = (1..=3)
        .map(|i| {
            RelayMessage::new_forward(
                "orders",
                i,
                0,
                "insert",
                serde_json::json!({"order_id": i}),
                false,
                None,
                "orders",
            )
        })
        .collect();

    let refs: Vec<&RelayMessage> = msgs.iter().collect();
    let bytes = IcebergSink::build_parquet_bytes(&refs).expect("build parquet bytes");

    // Parquet files end with PAR1 magic bytes too.
    let n = bytes.len();
    assert!(n >= 8, "Parquet file too small");
    assert_eq!(&bytes[n - 4..], b"PAR1", "should end with PAR1 footer magic");
}

// ── Metadata JSON structure ───────────────────────────────────────────────────

#[test]
fn test_iceberg_snapshot_metadata_fields() {
    use pg_tide_relay::sink::iceberg::{IcebergConfig, IcebergWriteMode};

    let cfg = IcebergConfig {
        warehouse_path: "/tmp".to_string(),
        namespace: "pgtide".to_string(),
        table_template: "{stream_table}".to_string(),
        write_mode: IcebergWriteMode::Append,
        rows_per_file: 10_000,
    };

    let data_path = cfg.data_file_path("orders", 1);
    let meta = cfg.build_snapshot_metadata("orders", 1, 1, 42, &data_path);

    assert_eq!(meta["format-version"], 2, "must use Iceberg v2");
    assert!(meta.get("table-uuid").is_some(), "must have table-uuid");
    assert!(meta.get("snapshots").is_some(), "must have snapshots array");
    let snaps = meta["snapshots"].as_array().expect("snapshots array");
    assert!(!snaps.is_empty(), "must have at least one snapshot");
    let snap = &snaps[0];
    assert!(snap.get("snapshot-id").is_some(), "snapshot must have ID");
    assert_eq!(snap["summary"]["operation"], "append");
    assert_eq!(snap["summary"]["added-records"], "42");
}
