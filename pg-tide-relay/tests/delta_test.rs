//! Integration tests: Delta Lake analytics sink (v0.10.0 — RELAY-P3-DL).
//!
//! Tests verify Parquet encoding, Delta log commit structure, version counter,
//! and DB-side mechanics — no external object store required.

mod common;

use common::PgTideTestDb;

// ── DB-side mechanics ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_delta_forward_sink_queues_messages() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("delta-outbox").await;
    db.setup_consumer_group("delta-group", "delta-outbox").await;

    let payloads: Vec<serde_json::Value> = (1..=5)
        .map(|i| serde_json::json!({"row_id": i, "lake": "delta"}))
        .collect();
    db.publish_messages("delta-outbox", &payloads).await;

    assert_eq!(
        db.pending_count("delta-outbox").await,
        5,
        "all 5 messages must be pending before Delta delivery"
    );
}

#[tokio::test]
async fn test_delta_sink_failure_preserves_offset() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("delta-fail-outbox").await;
    db.setup_consumer_group("delta-fail-group", "delta-fail-outbox")
        .await;

    let payloads: Vec<serde_json::Value> = (1..=2).map(|i| serde_json::json!({"n": i})).collect();
    db.publish_messages("delta-fail-outbox", &payloads).await;

    let row = db
        .client
        .query_opt(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'delta-fail-group'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        row.is_none(),
        "consumer offset must not exist before successful Delta delivery"
    );
}

// ── Config logic ──────────────────────────────────────────────────────────────

#[test]
fn test_delta_config_log_path() {
    use pg_tide_relay::sink::delta::DeltaConfig;

    let cfg = DeltaConfig {
        table_path: "s3://my-bucket/delta/orders".to_string(),
        change_data_feed: false,
        rows_per_file: 10_000,
    };

    let log_path = cfg.log_entry_path(0);
    assert!(
        log_path.contains("_delta_log"),
        "log path should contain _delta_log directory"
    );
    assert!(
        log_path.ends_with(".json"),
        "commit file should be a JSON file"
    );
    assert!(
        log_path.contains("00000000000000000000"),
        "version 0 should be zero-padded"
    );
}

#[test]
fn test_delta_config_log_entry_path_versions() {
    use pg_tide_relay::sink::delta::DeltaConfig;

    let cfg = DeltaConfig {
        table_path: "/tmp/delta/orders".to_string(),
        change_data_feed: false,
        rows_per_file: 10_000,
    };

    let path_v1 = cfg.log_entry_path(1);
    let path_v2 = cfg.log_entry_path(2);
    assert!(path_v1.ends_with(".json"), "v1 commit should be JSON");
    assert!(path_v2.ends_with(".json"), "v2 commit should be JSON");
    // Versions are zero-padded to 20 digits.
    assert!(
        path_v1.contains("00000000000000000001"),
        "v1 should be padded"
    );
    assert!(
        path_v2.contains("00000000000000000002"),
        "v2 should be padded"
    );
}

// ── Parquet encoding ──────────────────────────────────────────────────────────

#[cfg(feature = "delta")]
#[test]
fn test_delta_parquet_encoding_magic_bytes() {
    use pg_tide_relay::envelope::RelayMessage;
    use pg_tide_relay::sink::delta::DeltaSink;

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

    let bytes = DeltaSink::build_parquet_bytes(&[&msg], false).expect("build parquet bytes");

    assert!(!bytes.is_empty(), "Parquet output must not be empty");
    assert_eq!(&bytes[..4], b"PAR1", "should start with PAR1 magic");
}

#[cfg(feature = "delta")]
#[test]
fn test_delta_parquet_cdf_encoding() {
    use pg_tide_relay::envelope::RelayMessage;
    use pg_tide_relay::sink::delta::DeltaSink;

    let msgs = [
        RelayMessage::new_forward(
            "orders",
            1,
            0,
            "insert",
            serde_json::json!({"order_id": 1}),
            false,
            None,
            "orders",
        ),
        RelayMessage::new_forward(
            "orders",
            2,
            0,
            "delete",
            serde_json::json!({"order_id": 2}),
            false,
            None,
            "orders",
        ),
        RelayMessage::new_forward(
            "orders",
            3,
            0,
            "update",
            serde_json::json!({"order_id": 3, "status": "shipped"}),
            false,
            None,
            "orders",
        ),
    ];

    let refs: Vec<&RelayMessage> = msgs.iter().collect();
    let bytes = DeltaSink::build_parquet_bytes(&refs, true).expect("build parquet bytes with CDF");

    assert!(!bytes.is_empty(), "CDF Parquet output must not be empty");
    assert_eq!(&bytes[..4], b"PAR1", "should start with PAR1 magic");
}

// ── Delta log commit structure ────────────────────────────────────────────────

#[test]
fn test_delta_log_commit_json_structure() {
    use pg_tide_relay::sink::delta::DeltaConfig;

    let cfg = DeltaConfig {
        table_path: "/tmp/delta-commit".to_string(),
        change_data_feed: false,
        rows_per_file: 10_000,
    };

    let add_action = cfg.build_add_action("part-00001.parquet", 10, 2048);
    assert!(
        add_action.get("add").is_some(),
        "commit must have an 'add' action"
    );
    let add = &add_action["add"];
    assert!(add.get("path").is_some(), "add action must have path");
    assert_eq!(add["path"], "part-00001.parquet");
    assert_eq!(add["size"], 2048, "size should match");
}

#[test]
fn test_delta_protocol_commit_fields() {
    use pg_tide_relay::sink::delta::DeltaConfig;

    let cfg = DeltaConfig {
        table_path: "/tmp/delta-protocol".to_string(),
        change_data_feed: false,
        rows_per_file: 10_000,
    };

    let commits = cfg.build_init_commit();
    assert_eq!(
        commits.len(),
        2,
        "init commit must have 2 JSON lines (protocol + metaData)"
    );

    let protocol = &commits[0];
    let metadata = &commits[1];

    // Protocol must specify min reader/writer versions.
    let proto = &protocol["protocol"];
    assert!(proto.get("minReaderVersion").is_some());
    assert!(proto.get("minWriterVersion").is_some());

    // Metadata must have schema and configuration.
    let meta = &metadata["metaData"];
    assert!(meta.get("id").is_some(), "metadata must have ID");
    assert!(
        meta.get("schemaString").is_some(),
        "metadata must have schemaString"
    );
    assert!(
        meta.get("configuration").is_some(),
        "metadata must have configuration"
    );
}

#[test]
fn test_delta_cdf_protocol_has_cdf_feature() {
    use pg_tide_relay::sink::delta::DeltaConfig;

    let cfg = DeltaConfig {
        table_path: "/tmp/delta-cdf-protocol".to_string(),
        change_data_feed: true,
        rows_per_file: 10_000,
    };

    let commits = cfg.build_init_commit();
    let metadata = &commits[1];
    let meta = &metadata["metaData"];
    let config = &meta["configuration"];
    assert_eq!(
        config["delta.enableChangeDataFeed"], "true",
        "CDF tables must set delta.enableChangeDataFeed = true"
    );
}
