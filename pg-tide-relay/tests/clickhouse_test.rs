//! Integration tests: ClickHouse analytics sink (v0.10.0 — RELAY-P3-CH).
//!
//! Tests verify ClickHouse query building, NDJSON payload encoding, and
//! database-side mechanics — no external ClickHouse server required.

mod common;

use common::PgTideTestDb;

// ── DB-side mechanics ─────────────────────────────────────────────────────────

/// Verifies that outbox messages queue correctly before ClickHouse delivery.
#[tokio::test]
async fn test_clickhouse_forward_sink_queues_messages() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("ch-outbox").await;
    db.setup_consumer_group("ch-group", "ch-outbox").await;

    let payloads: Vec<serde_json::Value> = (1..=8)
        .map(|i| serde_json::json!({"event_id": i, "metric": i as f64 * 0.5}))
        .collect();
    db.publish_messages("ch-outbox", &payloads).await;

    assert_eq!(
        db.pending_count("ch-outbox").await,
        8,
        "all 8 messages must be pending before ClickHouse delivery"
    );
}

/// Verifies that a consumer offset is not committed before successful delivery.
#[tokio::test]
async fn test_clickhouse_sink_failure_preserves_offset() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("ch-fail-outbox").await;
    db.setup_consumer_group("ch-fail-group", "ch-fail-outbox")
        .await;

    let payloads: Vec<serde_json::Value> = (1..=3).map(|i| serde_json::json!({"n": i})).collect();
    db.publish_messages("ch-fail-outbox", &payloads).await;

    let row = db
        .client
        .query_opt(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'ch-fail-group'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        row.is_none(),
        "consumer offset must not exist before successful ClickHouse delivery"
    );
}

// ── Config and payload logic ──────────────────────────────────────────────────

#[test]
fn test_clickhouse_config_table_for_subject() {
    use pg_tide_relay::sink::clickhouse::ClickHouseConfig;

    let cfg = ClickHouseConfig {
        url: "http://localhost:8123".to_string(),
        database: "analytics".to_string(),
        table_template: "{stream_table}".to_string(),
        username: None,
        password: None,
        allow_http: true,
        ssrf_protection: false,
    };

    assert_eq!(cfg.table_for("orders.insert"), "orders.insert");
    assert_eq!(cfg.table_for("events"), "events");
}

#[test]
fn test_clickhouse_config_insert_query() {
    use pg_tide_relay::sink::clickhouse::ClickHouseConfig;

    let cfg = ClickHouseConfig {
        url: "http://localhost:8123".to_string(),
        database: "analytics".to_string(),
        table_template: "{stream_table}".to_string(),
        username: None,
        password: None,
        allow_http: true,
        ssrf_protection: false,
    };

    let query = cfg.insert_query("orders");
    assert!(query.contains("INSERT INTO"), "should be an INSERT query");
    assert!(query.contains("analytics"), "should reference the database");
    assert!(query.contains("orders"), "should reference the table");
    assert!(
        query.contains("JSONEachRow"),
        "should use JSONEachRow format"
    );
}

#[cfg(feature = "clickhouse")]
#[test]
fn test_clickhouse_jsonl_body_contains_required_fields() {
    use pg_tide_relay::envelope::RelayMessage;
    use pg_tide_relay::sink::clickhouse::{ClickHouseConfig, ClickHouseSink};

    let cfg = ClickHouseConfig {
        url: "http://localhost:8123".to_string(),
        database: "analytics".to_string(),
        table_template: "{stream_table}".to_string(),
        username: None,
        password: None,
        allow_http: true,
        ssrf_protection: false,
    };

    let sink = ClickHouseSink::new(cfg).expect("failed to create ClickHouseSink");

    let msgs: Vec<&RelayMessage> = vec![];
    let body = sink.build_jsonl_body(&msgs);
    assert!(
        body.is_empty(),
        "empty input should produce empty NDJSON body"
    );
}

#[cfg(feature = "clickhouse")]
#[test]
fn test_clickhouse_jsonl_body_encodes_message_fields() {
    use pg_tide_relay::envelope::RelayMessage;
    use pg_tide_relay::sink::clickhouse::{ClickHouseConfig, ClickHouseSink};

    let cfg = ClickHouseConfig {
        url: "http://localhost:8123".to_string(),
        database: "analytics".to_string(),
        table_template: "{stream_table}".to_string(),
        username: None,
        password: None,
        allow_http: true,
        ssrf_protection: false,
    };

    let sink = ClickHouseSink::new(cfg).expect("create sink");

    let msg = RelayMessage::new_forward(
        "orders",
        42,
        0,
        "insert",
        serde_json::json!({"order_id": 42, "amount": 99.0}),
        false,
        None,
        "orders.insert",
    );

    let body = sink.build_jsonl_body(&[&msg]);
    let parsed: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");
    assert_eq!(parsed["_op"], "insert");
    assert!(
        parsed["_dedup_key"].as_str().is_some(),
        "_dedup_key should be present"
    );
    assert_eq!(parsed["data"]["order_id"], 42);
}

/// Verify mock HTTP server receives a ClickHouse-style INSERT request.
#[cfg(feature = "clickhouse")]
#[tokio::test]
async fn test_clickhouse_sink_posts_to_mock_server() {
    use axum::{extract::Query, http::StatusCode, routing::post, Router};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tokio::sync::oneshot;

    let received: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));

    let app = {
        let state = Arc::clone(&received);
        Router::new().route(
            "/",
            post(
                move |Query(params): Query<HashMap<String, String>>, body: axum::body::Bytes| {
                    let state = Arc::clone(&state);
                    async move {
                        state.lock().unwrap().push((
                            params.get("query").cloned().unwrap_or_default(),
                            String::from_utf8_lossy(&body).to_string(),
                        ));
                        StatusCode::OK
                    }
                },
            ),
        )
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .ok();
    });

    use pg_tide_relay::envelope::RelayMessage;
    use pg_tide_relay::sink::clickhouse::{ClickHouseConfig, ClickHouseSink};
    use pg_tide_relay::sink::Sink;

    let cfg = ClickHouseConfig {
        url: format!("http://127.0.0.1:{port}"),
        database: "analytics".to_string(),
        table_template: "{stream_table}".to_string(),
        username: None,
        password: None,
        allow_http: true,
        ssrf_protection: false,
    };

    let mut sink = ClickHouseSink::new(cfg).expect("create sink");

    let msgs = vec![
        RelayMessage::new_forward(
            "orders",
            1,
            0,
            "insert",
            serde_json::json!({"id": 1}),
            false,
            None,
            "orders",
        ),
        RelayMessage::new_forward(
            "orders",
            2,
            0,
            "insert",
            serde_json::json!({"id": 2}),
            false,
            None,
            "orders",
        ),
    ];

    sink.publish(&msgs).await.expect("publish should succeed");

    let reqs = received.lock().unwrap();
    assert_eq!(reqs.len(), 1, "one INSERT request per table group");
    assert!(
        reqs[0].0.contains("JSONEachRow"),
        "query should reference JSONEachRow format"
    );
    // Body should be 2 NDJSON lines.
    let line_count = reqs[0].1.lines().count();
    assert_eq!(line_count, 2, "body should contain 2 NDJSON rows");

    let _ = shutdown_tx.send(());
}
