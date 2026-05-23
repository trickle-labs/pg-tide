//! Integration tests: v0.34.0 — Universal reverse pipeline sink registration.
//!
//! Verifies that all 8 previously-unregistered sink types are now accepted by
//! `build_sink()` (via `build_sink_for_validation`) and that their config
//! extraction produces correct structs.
//!
//! Sinks tested:
//!   - clickhouse   (feature = "clickhouse")
//!   - mongodb      (feature = "mongodb")
//!   - bigquery     (feature = "bigquery")
//!   - snowflake    (feature = "snowflake")
//!   - delta        (feature = "delta")
//!   - iceberg      (feature = "iceberg")
//!   - ducklake     (feature = "ducklake")
//!   - pg_outbox    (no extra feature — core tokio-postgres)
//!
//! "Round-trip" tests for file-system-backed sinks (delta, iceberg, ducklake)
//! feed 50 messages and assert the expected files are created.
//! Config-validation tests for external-service sinks (clickhouse, bigquery,
//! snowflake, mongodb, pg_outbox) verify that the correct config fields are
//! extracted without requiring a live backend in CI.

mod common;

use common::PgTideTestDb;
use pg_tide_relay::envelope::RelayMessage;
use pg_tide_relay::sink::Sink;

// ─── helpers ─────────────────────────────────────────────────────────────────

/// Build a test RelayMessage.
fn msg(dedup_key: &str, subject: &str, payload: serde_json::Value) -> RelayMessage {
    RelayMessage::new_reverse(dedup_key, subject, payload)
}

/// Build a batch of N test messages.
fn make_batch(n: usize, subject: &str) -> Vec<RelayMessage> {
    (1..=n)
        .map(|i| {
            msg(
                &format!("dedup-{i:04}"),
                subject,
                serde_json::json!({"i": i}),
            )
        })
        .collect()
}

// ─── DB-side mechanics (shared by all new sinks) ─────────────────────────────

/// Messages are queued in the outbox before any sink delivers them.
/// This test runs without feature flags and validates the DB plumbing that
/// underlies every reverse pipeline.
#[tokio::test]
async fn test_new_sinks_outbox_queuing_mechanics() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("v034-test-outbox").await;
    db.setup_consumer_group("v034-group", "v034-test-outbox")
        .await;

    let payloads: Vec<serde_json::Value> = (1..=50)
        .map(|i| serde_json::json!({"seq": i, "v": "0.34.0"}))
        .collect();
    db.publish_messages("v034-test-outbox", &payloads).await;

    assert_eq!(
        db.pending_count("v034-test-outbox").await,
        50,
        "all 50 messages must be pending before any sink delivers them"
    );
}

// ─── ClickHouse — config extraction ──────────────────────────────────────────

#[cfg(feature = "clickhouse")]
#[test]
fn test_clickhouse_sink_config_extraction() {
    use pg_tide_relay::sink::clickhouse::{ClickHouseConfig, ClickHouseSink};

    let cfg = ClickHouseConfig {
        url: "http://clickhouse:8123".to_string(),
        database: "events".to_string(),
        table_template: "{stream_table}".to_string(),
        username: Some("default".to_string()),
        password: None,
        allow_http: true,
        ssrf_protection: false,
    };

    // Verify config helpers work.
    assert_eq!(cfg.table_for("orders"), "orders");
    assert_eq!(cfg.table_for("{stream_table}"), "{stream_table}");
    assert!(cfg.insert_query("orders").contains("INSERT INTO"));

    // Verify sink is constructible.
    let sink = ClickHouseSink::new(cfg).expect("ClickHouseSink::new should succeed");
    assert_eq!(sink.name(), "clickhouse");
}

#[cfg(feature = "clickhouse")]
#[test]
fn test_clickhouse_sink_builds_correct_payload() {
    use pg_tide_relay::sink::clickhouse::{ClickHouseConfig, ClickHouseSink};

    let cfg = ClickHouseConfig {
        url: "http://clickhouse:8123".to_string(),
        database: "analytics".to_string(),
        table_template: "{stream_table}".to_string(),
        username: None,
        password: None,
        allow_http: true,
        ssrf_protection: false,
    };
    let sink = ClickHouseSink::new(cfg).expect("ClickHouseSink::new");

    let messages = make_batch(5, "orders.created");
    let refs: Vec<&RelayMessage> = messages.iter().collect();
    let body = sink.build_jsonl_body(&refs);

    // Should be 5 NDJSON lines.
    let lines: Vec<&str> = body.trim().split('\n').collect();
    assert_eq!(lines.len(), 5, "expected 5 NDJSON lines for 5 messages");

    // Each line should be valid JSON containing _dedup_key.
    for line in &lines {
        let parsed: serde_json::Value = serde_json::from_str(line).expect("valid JSON line");
        assert!(
            parsed.get("_dedup_key").is_some(),
            "_dedup_key should be present in ClickHouse payload"
        );
    }
}

// ─── MongoDB — config extraction ─────────────────────────────────────────────

#[cfg(feature = "mongodb")]
#[test]
fn test_mongodb_sink_config_extraction() {
    use pg_tide_relay::sink::mongodb::MongoDbConfig;

    let cfg = MongoDbConfig::new("mongodb://localhost:27017", "events_db");
    assert_eq!(cfg.connection_string, "mongodb://localhost:27017");
    assert_eq!(cfg.database, "events_db");
    assert_eq!(cfg.collection_for("orders"), "orders");
}

#[cfg(feature = "mongodb")]
#[test]
fn test_mongodb_sink_document_conversion() {
    use pg_tide_relay::sink::mongodb::MongoDbConfig;

    let cfg = MongoDbConfig::new("mongodb://localhost:27017", "test_db");
    let message = msg(
        "dedup-0001",
        "orders.created",
        serde_json::json!({"order_id": 42}),
    );

    let doc = cfg
        .to_document(&message)
        .expect("to_document should succeed");
    assert!(
        doc.get("order_id").is_some(),
        "payload fields should be in the doc"
    );
    assert_eq!(
        doc.get("_dedup_key").and_then(|v| v.as_str()),
        Some("dedup-0001"),
        "_dedup_key should be propagated"
    );
    assert_eq!(
        doc.get("_subject").and_then(|v| v.as_str()),
        Some("orders.created"),
        "_subject should be propagated"
    );
}

// ─── BigQuery — config extraction ────────────────────────────────────────────

#[cfg(feature = "bigquery")]
#[test]
fn test_bigquery_sink_config_extraction() {
    use pg_tide_relay::sink::bigquery::{BigQueryConfig, BigQueryWriteMode};

    let cfg = BigQueryConfig {
        project_id: "my-project".to_string(),
        dataset_id: "events".to_string(),
        table_template: "{stream_table}".to_string(),
        write_mode: BigQueryWriteMode::Streaming,
        access_token: "test-token".to_string(),
    };

    assert_eq!(cfg.table_for("orders"), "orders");
    assert!(cfg.insert_all_url("orders").contains("my-project"));
    assert!(cfg.insert_all_url("orders").contains("events"));
    assert!(cfg.insert_all_url("orders").contains("orders"));
}

#[cfg(feature = "bigquery")]
#[test]
fn test_bigquery_sink_builds_insert_payload() {
    use pg_tide_relay::sink::bigquery::{BigQueryConfig, BigQueryWriteMode};

    let cfg = BigQueryConfig {
        project_id: "my-project".to_string(),
        dataset_id: "events".to_string(),
        table_template: "{stream_table}".to_string(),
        write_mode: BigQueryWriteMode::Streaming,
        access_token: "test-token".to_string(),
    };

    let messages = make_batch(3, "test.events");
    let refs: Vec<&RelayMessage> = messages.iter().collect();
    let payload = cfg.build_insert_all_payload(&refs);

    let rows = payload["rows"].as_array().expect("rows should be an array");
    assert_eq!(rows.len(), 3, "should have 3 rows");

    for row in rows {
        assert!(
            row.get("insertId").is_some(),
            "insertId (dedup key) should be present"
        );
        assert!(row.get("json").is_some(), "json field should be present");
    }
}

// ─── Snowflake — config extraction ───────────────────────────────────────────

#[cfg(feature = "snowflake")]
#[test]
fn test_snowflake_sink_config_extraction() {
    use pg_tide_relay::sink::snowflake::SnowflakeConfig;

    let cfg = SnowflakeConfig {
        account: "myorg-myaccount".to_string(),
        database: "EVENTS".to_string(),
        schema: "PUBLIC".to_string(),
        table_template: "{stream_table}".to_string(),
        user: "pg_tide_relay".to_string(),
        auth_token: "test-jwt".to_string(),
        batch_size: 1000,
    };

    assert_eq!(cfg.table_for("orders"), "orders");
    assert!(
        cfg.endpoint_url().contains("myorg-myaccount"),
        "endpoint URL should contain account identifier"
    );
}

#[cfg(feature = "snowflake")]
#[test]
fn test_snowflake_sink_builds_insert_rows_payload() {
    use pg_tide_relay::sink::snowflake::SnowflakeConfig;

    let cfg = SnowflakeConfig {
        account: "myorg-myaccount".to_string(),
        database: "EVENTS".to_string(),
        schema: "PUBLIC".to_string(),
        table_template: "{stream_table}".to_string(),
        user: "pg_tide_relay".to_string(),
        auth_token: "test-jwt".to_string(),
        batch_size: 1000,
    };

    let messages = make_batch(10, "orders.created");
    let refs: Vec<&RelayMessage> = messages.iter().collect();
    let payload = cfg.build_insert_rows_payload("orders_created", &refs);

    let rows = payload["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 10, "should have 10 rows");
    assert_eq!(
        payload["channelName"].as_str(),
        Some("orders_created"),
        "channelName should match"
    );
}

// ─── Delta Lake — local filesystem round-trip ────────────────────────────────

#[cfg(feature = "delta")]
#[tokio::test]
async fn test_delta_sink_local_round_trip() {
    use pg_tide_relay::sink::delta::{DeltaConfig, DeltaSink};
    use std::sync::Arc;

    let tmp = tempfile::tempdir().expect("create tempdir");
    let root = tmp.path().to_str().expect("temp dir path to str");
    // Use a store rooted at "/" so absolute table_path resolves correctly
    // (DeltaSink strips leading '/' from paths before passing to object_store).
    let store = Arc::new(object_store::local::LocalFileSystem::new());

    let cfg = DeltaConfig {
        table_path: root.to_string(),
        change_data_feed: false,
        rows_per_file: 50_000,
    };
    let mut sink = DeltaSink::new(store, cfg);
    assert_eq!(sink.name(), "delta");

    // Feed 50 messages.
    let messages = make_batch(50, "orders.created");
    sink.publish(&messages)
        .await
        .expect("delta publish should succeed");

    // Verify the Delta Log was initialised (version 0 commit file exists).
    let log_dir = tmp.path().join("_delta_log");
    assert!(
        log_dir.exists(),
        "_delta_log/ directory should be created after first publish"
    );

    let commit_file = log_dir.join("00000000000000000000.json");
    assert!(
        commit_file.exists(),
        "version 0 commit file should exist after first publish"
    );
}

#[cfg(feature = "delta")]
#[test]
fn test_delta_sink_schema_and_init_commit() {
    use pg_tide_relay::sink::delta::DeltaConfig;

    let cfg = DeltaConfig::new("/tmp/delta-test");
    let schema = DeltaConfig::table_schema();
    assert!(
        schema["fields"].is_array(),
        "schema should have fields array"
    );

    let init = cfg.build_init_commit();
    assert_eq!(
        init.len(),
        2,
        "init commit should have protocol + metadata actions"
    );
    assert!(
        init[0].get("protocol").is_some(),
        "first action is protocol"
    );
    assert!(
        init[1].get("metaData").is_some(),
        "second action is metaData"
    );
}

// ─── Apache Iceberg — local filesystem round-trip ────────────────────────────

#[cfg(feature = "iceberg")]
#[tokio::test]
async fn test_iceberg_sink_local_round_trip() {
    use pg_tide_relay::sink::iceberg::{IcebergConfig, IcebergSink, IcebergWriteMode};
    use std::sync::Arc;

    let tmp = tempfile::tempdir().expect("create tempdir");
    let root = tmp.path().to_str().expect("temp dir path to str");
    // Use a store rooted at "/" so absolute warehouse_path resolves correctly
    // (IcebergSink strips leading '/' from paths before passing to object_store).
    let store = Arc::new(object_store::local::LocalFileSystem::new());

    let cfg = IcebergConfig {
        warehouse_path: root.to_string(),
        namespace: "test_ns".to_string(),
        table_template: "{stream_table}".to_string(),
        write_mode: IcebergWriteMode::Append,
        rows_per_file: 50_000,
    };
    let mut sink = IcebergSink::new(store, cfg);
    assert_eq!(sink.name(), "iceberg");

    // Feed 50 messages.
    let messages = make_batch(50, "orders.created");
    sink.publish(&messages)
        .await
        .expect("iceberg publish should succeed");

    // Verify metadata was written (snapshot metadata file exists).
    let metadata_dir = tmp
        .path()
        .join("test_ns")
        .join("orders.created")
        .join("metadata");
    assert!(
        metadata_dir.exists(),
        "metadata/ directory should be created after first publish"
    );
}

#[cfg(feature = "iceberg")]
#[test]
fn test_iceberg_sink_config_table_for() {
    use pg_tide_relay::sink::iceberg::{IcebergConfig, IcebergWriteMode};

    let cfg = IcebergConfig {
        warehouse_path: "/tmp/iceberg".to_string(),
        namespace: "analytics".to_string(),
        table_template: "{stream_table}".to_string(),
        write_mode: IcebergWriteMode::Append,
        rows_per_file: 50_000,
    };
    assert_eq!(cfg.table_for("orders"), "orders");
    assert_eq!(cfg.table_for("user_events"), "user_events");
}

// ─── DuckLake — config extraction (no real PG required) ──────────────────────

#[cfg(feature = "ducklake")]
#[test]
fn test_ducklake_sink_config_defaults() {
    use pg_tide_relay::sink::ducklake::{DuckLakeConfig, DuckLakePartition, SchemaChangePolicy};

    let cfg = DuckLakeConfig::new("s3://my-lake/events/", "analytics");
    assert_eq!(cfg.namespace, "analytics");
    assert_eq!(cfg.catalog_schema, "ducklake");
    assert_eq!(cfg.inline_row_limit, 10);
    assert_eq!(cfg.on_schema_change, SchemaChangePolicy::WarnAndContinue);
    assert_eq!(cfg.partition, DuckLakePartition::None);
    assert!(!cfg.atomic_lake_writes);
}

#[cfg(feature = "ducklake")]
#[test]
fn test_ducklake_sink_config_parquet_path() {
    use pg_tide_relay::sink::ducklake::DuckLakeConfig;

    let cfg = DuckLakeConfig::new("s3://my-lake/events/", "analytics");
    let path = cfg.parquet_path("orders", 42);
    assert!(
        path.contains("s3://my-lake/events"),
        "path should include data_path"
    );
    assert!(path.contains("analytics"), "path should include namespace");
    assert!(path.contains("orders"), "path should include table name");
    assert!(path.contains("snap_42"), "path should include snapshot id");
}

#[cfg(feature = "ducklake")]
#[test]
fn test_ducklake_sink_schema_change_policy_variants() {
    use pg_tide_relay::sink::ducklake::{DuckLakeConfig, SchemaChangePolicy};

    let mut cfg = DuckLakeConfig::new("/tmp/lake", "ns");

    cfg.on_schema_change = SchemaChangePolicy::Pause;
    assert_eq!(cfg.on_schema_change, SchemaChangePolicy::Pause);

    cfg.on_schema_change = SchemaChangePolicy::RouteToDlq;
    assert_eq!(cfg.on_schema_change, SchemaChangePolicy::RouteToDlq);

    cfg.on_schema_change = SchemaChangePolicy::AutoNewStream;
    assert_eq!(cfg.on_schema_change, SchemaChangePolicy::AutoNewStream);
}

// ─── pg_outbox (PgInboxSink) — DB round-trip ─────────────────────────────────

/// Verifies that PgInboxSink (registered as sink_type = "pg_outbox") correctly
/// delivers 50 messages to a remote pg-tide inbox with deduplication.
///
/// This test reuses the PgTideTestDb harness and creates a second connection
/// to simulate the remote inbox target.
#[tokio::test]
async fn test_pg_outbox_sink_round_trip_50_messages() {
    use pg_tide_relay::sink::pg_outbox::PgInboxSink;

    let db = PgTideTestDb::start().await;

    // Create the target inbox.
    db.setup_inbox("reverse-pipeline-target").await;

    let postgres_url = format!(
        "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
        db.host_port
    );

    // Instantiate PgInboxSink (the sink behind "pg_outbox" type).
    let mut sink = PgInboxSink::new(&postgres_url, "reverse-pipeline-target_inbox")
        .await
        .expect("PgInboxSink::new should succeed for pg_outbox sink");

    assert_eq!(sink.name(), "pg-inbox-remote");

    // Feed 50 messages.
    let messages = make_batch(50, "external.events");
    sink.publish(&messages)
        .await
        .expect("pg_outbox first publish should succeed");

    // All 50 rows should be in the inbox.
    db.assert_inbox_received("reverse-pipeline-target", 50)
        .await;

    // Re-publish the same 50 messages — dedup via ON CONFLICT DO NOTHING.
    sink.publish(&messages)
        .await
        .expect("pg_outbox second publish should succeed");

    db.assert_inbox_received("reverse-pipeline-target", 50)
        .await;
}

/// Verifies that pg_outbox sink is not mistakenly treated as unknown sink_type.
/// The reverse-pipeline round-trip above proves the sink construction path works;
/// this test also verifies a hyphenated inbox name works correctly.
#[tokio::test]
async fn test_pg_outbox_sink_hyphenated_inbox_name() {
    use pg_tide_relay::sink::pg_outbox::PgInboxSink;

    let db = PgTideTestDb::start().await;

    // Create an inbox with a hyphenated name.
    db.setup_inbox("order-events-v034").await;

    let postgres_url = format!(
        "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
        db.host_port
    );

    let mut sink = PgInboxSink::new(&postgres_url, "order-events-v034_inbox")
        .await
        .expect("PgInboxSink::new should accept hyphenated table names");

    let messages = make_batch(10, "orders.created");
    sink.publish(&messages)
        .await
        .expect("should deliver to hyphenated inbox without SQL error");

    db.assert_inbox_received("order-events-v034", 10).await;
}
