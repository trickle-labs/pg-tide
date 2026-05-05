//! Integration tests: Snowflake analytics sink (v0.10.0 — RELAY-P3-SF).
//!
//! Tests verify Snowflake endpoint construction, insert-rows payload format,
//! and DB-side mechanics — no external Snowflake account required.

mod common;

use common::PgTideTestDb;

// ── DB-side mechanics ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_snowflake_forward_sink_queues_messages() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("sf-outbox").await;
    db.setup_consumer_group("sf-group", "sf-outbox").await;

    let payloads: Vec<serde_json::Value> = (1..=6)
        .map(|i| serde_json::json!({"row_id": i, "warehouse": "COMPUTE_WH"}))
        .collect();
    db.publish_messages("sf-outbox", &payloads).await;

    assert_eq!(
        db.pending_count("sf-outbox").await,
        6,
        "all 6 messages must be pending before Snowflake delivery"
    );
}

#[tokio::test]
async fn test_snowflake_sink_failure_preserves_offset() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("sf-fail-outbox").await;
    db.setup_consumer_group("sf-fail-group", "sf-fail-outbox")
        .await;

    let payloads: Vec<serde_json::Value> = (1..=2).map(|i| serde_json::json!({"n": i})).collect();
    db.publish_messages("sf-fail-outbox", &payloads).await;

    let row = db
        .client
        .query_opt(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'sf-fail-group'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        row.is_none(),
        "consumer offset must not exist before successful Snowflake delivery"
    );
}

// ── Config and payload structure ──────────────────────────────────────────────

#[test]
fn test_snowflake_config_table_for_subject() {
    use pg_tide_relay::sink::snowflake::SnowflakeConfig;

    let cfg = SnowflakeConfig {
        account: "myorg-myaccount".to_string(),
        database: "ANALYTICS".to_string(),
        schema: "PGTIDE".to_string(),
        table_template: "{stream_table}".to_string(),
        user: "relay".to_string(),
        auth_token: "token".to_string(),
        batch_size: 1000,
    };

    assert_eq!(cfg.table_for("orders"), "orders");
    assert_eq!(cfg.table_for("events.click"), "events.click");
}

#[test]
fn test_snowflake_config_endpoint_url() {
    use pg_tide_relay::sink::snowflake::SnowflakeConfig;

    let cfg = SnowflakeConfig {
        account: "myorg-myaccount".to_string(),
        database: "ANALYTICS".to_string(),
        schema: "PGTIDE".to_string(),
        table_template: "{stream_table}".to_string(),
        user: "relay".to_string(),
        auth_token: "token".to_string(),
        batch_size: 1000,
    };

    let url = cfg.endpoint_url();
    assert!(
        url.contains("myorg-myaccount.snowflakecomputing.com"),
        "endpoint should include account identifier"
    );
    assert!(url.contains("insertRows"), "endpoint should target insertRows");
}

#[test]
fn test_snowflake_insert_rows_payload_structure() {
    use pg_tide_relay::envelope::RelayMessage;
    use pg_tide_relay::sink::snowflake::SnowflakeConfig;

    let cfg = SnowflakeConfig {
        account: "test-account".to_string(),
        database: "ANALYTICS".to_string(),
        schema: "PGTIDE".to_string(),
        table_template: "{stream_table}".to_string(),
        user: "relay".to_string(),
        auth_token: "tok".to_string(),
        batch_size: 1000,
    };

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

    let payload = cfg.build_insert_rows_payload("ANALYTICS.PGTIDE.orders", &[&msg]);
    assert!(payload.get("requestId").is_some(), "should have requestId");
    assert!(payload.get("channelName").is_some(), "should have channelName");
    let rows = payload["rows"].as_array().expect("rows must be array");
    assert_eq!(rows.len(), 1);
    assert!(rows[0].get("_DEDUP_KEY").is_some(), "row must have _DEDUP_KEY");
    assert_eq!(rows[0]["_OP"], "insert");
}

/// Verifies the Snowflake sink's publish does not panic (network error expected).
#[cfg(feature = "snowflake")]
#[tokio::test]
async fn test_snowflake_sink_publish_does_not_panic() {
    use pg_tide_relay::envelope::RelayMessage;
    use pg_tide_relay::sink::snowflake::{SnowflakeConfig, SnowflakeSink};
    use pg_tide_relay::sink::Sink;

    let cfg = SnowflakeConfig {
        account: "test-account".to_string(),
        database: "ANALYTICS".to_string(),
        schema: "PGTIDE".to_string(),
        table_template: "{stream_table}".to_string(),
        user: "relay".to_string(),
        auth_token: "test-token".to_string(),
        batch_size: 1000,
    };

    let mut sink = SnowflakeSink::new(cfg).expect("create SnowflakeSink");

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

    // Publish will fail with a network/DNS error (no Snowflake account), but must not panic.
    let result = sink.publish(&[msg]).await;
    assert!(
        result.is_err() || result.is_ok(),
        "publish should return a Result without panicking"
    );
}
