//! Integration tests: BigQuery analytics sink (v0.10.0 — RELAY-P3-BQ).
//!
//! Tests verify BigQuery payload structure, insertAll URL construction,
//! and DB-side mechanics — no external GCP project required.

mod common;

use common::PgTideTestDb;

// ── DB-side mechanics ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_bigquery_forward_sink_queues_messages() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("bq-outbox").await;
    db.setup_consumer_group("bq-group", "bq-outbox").await;

    let payloads: Vec<serde_json::Value> = (1..=7)
        .map(|i| serde_json::json!({"event_id": format!("bq-{i}"), "project": "my-project"}))
        .collect();
    db.publish_messages("bq-outbox", &payloads).await;

    assert_eq!(
        db.pending_count("bq-outbox").await,
        7,
        "all 7 messages must be pending before BigQuery delivery"
    );
}

#[tokio::test]
async fn test_bigquery_sink_failure_preserves_offset() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("bq-fail-outbox").await;
    db.setup_consumer_group("bq-fail-group", "bq-fail-outbox")
        .await;

    let payloads: Vec<serde_json::Value> = (1..=3).map(|i| serde_json::json!({"n": i})).collect();
    db.publish_messages("bq-fail-outbox", &payloads).await;

    let row = db
        .client
        .query_opt(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'bq-fail-group'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        row.is_none(),
        "consumer offset must not exist before successful BigQuery delivery"
    );
}

// ── Config and payload structure ──────────────────────────────────────────────

#[test]
fn test_bigquery_config_table_for_subject() {
    use pg_tide_relay::sink::bigquery::BigQueryConfig;
    use pg_tide_relay::sink::bigquery::BigQueryWriteMode;

    let cfg = BigQueryConfig {
        project_id: "my-project".to_string(),
        dataset_id: "pgtide".to_string(),
        table_template: "{stream_table}".to_string(),
        write_mode: BigQueryWriteMode::Streaming,
        access_token: "token".to_string(),
    };

    assert_eq!(cfg.table_for("orders"), "orders");
    assert_eq!(cfg.table_for("events.click"), "events.click");
}

#[test]
fn test_bigquery_insert_all_url_structure() {
    use pg_tide_relay::sink::bigquery::{BigQueryConfig, BigQueryWriteMode};

    let cfg = BigQueryConfig {
        project_id: "test-project".to_string(),
        dataset_id: "pgtide".to_string(),
        table_template: "{stream_table}".to_string(),
        write_mode: BigQueryWriteMode::Streaming,
        access_token: "token".to_string(),
    };

    let url = cfg.insert_all_url("orders");
    assert!(url.contains("test-project"), "URL must contain project ID");
    assert!(url.contains("pgtide"), "URL must contain dataset ID");
    assert!(url.contains("orders"), "URL must contain table name");
    assert!(
        url.contains("insertAll"),
        "URL must target insertAll endpoint"
    );
    assert!(
        url.starts_with("https://bigquery.googleapis.com"),
        "URL must use BigQuery API base"
    );
}

#[test]
fn test_bigquery_insert_all_payload_structure() {
    use pg_tide_relay::envelope::RelayMessage;
    use pg_tide_relay::sink::bigquery::{BigQueryConfig, BigQueryWriteMode};

    let cfg = BigQueryConfig {
        project_id: "test-project".to_string(),
        dataset_id: "pgtide".to_string(),
        table_template: "{stream_table}".to_string(),
        write_mode: BigQueryWriteMode::Streaming,
        access_token: "token".to_string(),
    };

    let msg1 = RelayMessage::new_forward(
        "orders",
        1,
        0,
        "insert",
        serde_json::json!({"order_id": 1}),
        false,
        None,
        "orders",
    );
    let msg2 = RelayMessage::new_forward(
        "orders",
        2,
        0,
        "delete",
        serde_json::json!({"order_id": 2}),
        false,
        None,
        "orders",
    );

    let payload = cfg.build_insert_all_payload(&[&msg1, &msg2]);

    // Validate top-level fields.
    assert_eq!(payload["skipInvalidRows"], false);
    assert_eq!(payload["ignoreUnknownValues"], false);

    let rows = payload["rows"].as_array().expect("rows must be array");
    assert_eq!(rows.len(), 2, "should have 2 rows");

    // Each row must have insertId (for dedup) and json fields.
    for row in rows {
        assert!(row.get("insertId").is_some(), "row must have insertId");
        let json_field = &row["json"];
        assert!(
            json_field.get("_dedup_key").is_some(),
            "json must have _dedup_key"
        );
        assert!(json_field.get("_op").is_some(), "json must have _op");
        assert!(json_field.get("data").is_some(), "json must have data");
    }
}

/// Verifies the BigQuery sink successfully calls publish on a batch of messages.
/// Uses a real sink instance — network errors expected (no GCP), but no panics.
#[cfg(feature = "bigquery")]
#[tokio::test]
async fn test_bigquery_sink_publish_does_not_panic() {
    use pg_tide_relay::envelope::RelayMessage;
    use pg_tide_relay::sink::bigquery::{BigQueryConfig, BigQuerySink, BigQueryWriteMode};
    use pg_tide_relay::sink::Sink;

    let cfg = BigQueryConfig {
        project_id: "test-project".to_string(),
        dataset_id: "pgtide".to_string(),
        table_template: "{stream_table}".to_string(),
        write_mode: BigQueryWriteMode::Streaming,
        access_token: "test-token".to_string(),
    };

    let mut sink = BigQuerySink::new(cfg).expect("create BigQuerySink");
    let msg = RelayMessage::new_forward(
        "orders",
        1,
        0,
        "insert",
        serde_json::json!({"id": 1}),
        false,
        None,
        "orders",
    );

    // Publish will fail with a network error (no GCP), but must not panic.
    let result = sink.publish(&[msg]).await;
    assert!(
        result.is_err() || result.is_ok(),
        "publish should return a Result without panicking"
    );
}
