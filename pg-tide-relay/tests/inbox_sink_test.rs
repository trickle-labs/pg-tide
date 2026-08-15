//! Integration test: inbox sink delivery, deduplication, and mark processed/failed.

mod common;

use common::PgTideTestDb;

#[tokio::test]
async fn test_inbox_delivery() {
    let db = PgTideTestDb::start().await;
    db.setup_inbox("notifications").await;

    // Deliver a message.
    let payload = serde_json::json!({"type": "welcome", "user_id": 42});
    db.deliver_to_inbox("notifications", "evt-001", &payload)
        .await;

    db.assert_inbox_received("notifications", 1).await;
}

#[tokio::test]
async fn test_inbox_deduplication() {
    let db = PgTideTestDb::start().await;
    db.setup_inbox("payments").await;

    let payload = serde_json::json!({"amount": 99.99});

    // Deliver the same event_id twice — should be deduplicated.
    db.deliver_to_inbox("payments", "pay-001", &payload).await;
    db.deliver_to_inbox("payments", "pay-001", &payload).await;

    db.assert_inbox_received("payments", 1).await;
}

#[tokio::test]
async fn test_inbox_mark_processed() {
    let db = PgTideTestDb::start().await;
    db.setup_inbox("orders").await;

    let payload = serde_json::json!({"order_id": 7});
    db.deliver_to_inbox("orders", "ord-007", &payload).await;

    // Mark as processed.
    db.client
        .execute(
            r#"UPDATE tide."orders_inbox" SET processed_at = now() WHERE event_id = $1"#,
            &[&"ord-007"],
        )
        .await
        .unwrap();

    // Verify processed.
    let row = db
        .client
        .query_one(
            r#"SELECT processed_at IS NOT NULL FROM tide."orders_inbox" WHERE event_id = $1"#,
            &[&"ord-007"],
        )
        .await
        .unwrap();
    let is_processed: bool = row.get(0);
    assert!(is_processed);
}

#[tokio::test]
async fn test_inbox_mark_failed_increments_retry() {
    let db = PgTideTestDb::start().await;
    db.setup_inbox("tasks").await;

    let payload = serde_json::json!({"task": "process_video"});
    db.deliver_to_inbox("tasks", "task-001", &payload).await;

    // Mark as failed twice.
    for _ in 0..2 {
        db.client
            .execute(
                r#"UPDATE tide."tasks_inbox"
                   SET retry_count = retry_count + 1, last_error = 'timeout'
                   WHERE event_id = $1"#,
                &[&"task-001"],
            )
            .await
            .unwrap();
    }

    // Verify retry count.
    let row = db
        .client
        .query_one(
            r#"SELECT retry_count FROM tide."tasks_inbox" WHERE event_id = $1"#,
            &[&"task-001"],
        )
        .await
        .unwrap();
    let retries: i32 = row.get(0);
    assert_eq!(retries, 2);
}

#[tokio::test]
async fn test_inbox_batch_delivery() {
    let db = PgTideTestDb::start().await;
    db.setup_inbox("events").await;

    // Deliver a batch of 50 messages.
    for i in 1..=50 {
        let payload = serde_json::json!({"seq": i});
        let event_id = format!("batch-{i:04}");
        db.deliver_to_inbox("events", &event_id, &payload).await;
    }

    db.assert_inbox_received("events", 50).await;
}

// ── v0.23.0: PgInboxSink round-trip test ─────────────────────────────────────

/// v0.23.0: Verify that `PgInboxSink` delivers messages with the correct column
/// mapping: `event_id`, `source`, `payload`, and `headers`.
///
/// Also verifies that re-publishing the same 50 messages produces zero
/// duplicates (idempotent deduplication via ON CONFLICT (event_id) DO NOTHING).
#[tokio::test]
async fn test_pg_inbox_sink_round_trip() {
    use pg_tide_relay::sink::{pg_outbox::PgInboxSink, Sink};

    let db = PgTideTestDb::start().await;
    db.setup_inbox("remote_test").await;

    let db_url = format!(
        "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres sslmode=disable",
        db.host_port
    );

    let mut sink = PgInboxSink::new(&db_url, "remote_test_inbox")
        .await
        .expect("PgInboxSink::new should succeed");

    // Build 50 unique test messages.
    let messages: Vec<pg_tide_relay::envelope::RelayMessage> = (1..=50u32)
        .map(|i| pg_tide_relay::envelope::RelayMessage {
            dedup_key: format!("round-trip-{i:04}"),
            subject: format!("orders.created.{i}"),
            payload: serde_json::json!({"order_id": i, "test": "pg_inbox_sink_round_trip"}),
            op: "insert".to_string(),
            is_full_refresh: false,
            outbox_id: None,
            refresh_id: None,
            outbox_name: None,
            headers: None,
            created_at: None,
            ack_token: pg_tide_relay::envelope::AckToken::None,
        })
        .collect();

    // First publish: all 50 should be inserted.
    sink.publish(&messages).await.expect("first publish");
    db.assert_inbox_received("remote_test", 50).await;

    // Verify a sample row's columns.
    let row = db
        .client
        .query_one(
            r#"SELECT event_id, source, payload, headers
               FROM tide."remote_test_inbox"
               WHERE event_id = 'round-trip-0001'"#,
            &[],
        )
        .await
        .expect("row lookup");

    assert_eq!(row.get::<_, &str>("event_id"), "round-trip-0001");
    assert_eq!(row.get::<_, &str>("source"), "orders.created.1");

    let payload: serde_json::Value = row.get("payload");
    assert_eq!(payload["order_id"], 1);

    let headers: serde_json::Value = row.get("headers");
    assert_eq!(headers["event_type"], "orders.created.1");

    // Second publish (duplicates): all 50 should be silently ignored.
    sink.publish(&messages)
        .await
        .expect("second publish (dedup)");
    // Count must still be 50 — no new rows inserted.
    db.assert_inbox_received("remote_test", 50).await;
}
