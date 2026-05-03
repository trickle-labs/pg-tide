//! Integration test: full outbox → relay → inbox round-trip.
//!
//! Simulates the relay's core loop: poll outbox messages, transform them,
//! deliver to an inbox, and commit the consumer offset.

mod common;

use common::PgTideTestDb;

#[tokio::test]
async fn test_outbox_to_inbox_round_trip() {
    let db = PgTideTestDb::start().await;

    // Set up source (outbox) and destination (inbox).
    db.setup_outbox("orders").await;
    db.setup_inbox("order-events").await;
    db.setup_consumer_group("relay-default", "orders").await;

    // Publish messages to outbox.
    let payloads: Vec<serde_json::Value> = (1..=5)
        .map(|i| serde_json::json!({"order_id": i, "status": "created"}))
        .collect();
    db.publish_messages("orders", &payloads).await;

    // Simulate relay: read from outbox, write to inbox, commit offset.
    let rows = db
        .client
        .query(
            "SELECT id, payload FROM tide.tide_outbox_messages
             WHERE outbox_name = 'orders' AND consumed_at IS NULL
             ORDER BY id LIMIT 100",
            &[],
        )
        .await
        .expect("failed to poll outbox");

    assert_eq!(rows.len(), 5);

    let mut last_id: i64 = 0;
    for row in &rows {
        let id: i64 = row.get(0);
        let payload: serde_json::Value = row.get(1);
        let event_id = format!("orders:{id}");

        // Deliver to inbox.
        db.deliver_to_inbox("order-events", &event_id, &payload)
            .await;
        last_id = id;
    }

    // Mark outbox messages as consumed.
    db.client
        .execute(
            "UPDATE tide.tide_outbox_messages SET consumed_at = now()
             WHERE outbox_name = 'orders' AND id <= $1",
            &[&last_id],
        )
        .await
        .unwrap();

    // Commit consumer offset.
    db.commit_offset("relay-default", "relay-0", last_id).await;

    // Verify: no pending outbox messages.
    assert_eq!(db.pending_count("orders").await, 0);

    // Verify: inbox received all 5 messages.
    db.assert_inbox_received("order-events", 5).await;

    // Verify: consumer offset is at last_id.
    let row = db
        .client
        .query_one(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'relay-default' AND consumer_id = 'relay-0'",
            &[],
        )
        .await
        .unwrap();
    let offset: i64 = row.get(0);
    assert_eq!(offset, last_id);
}

#[tokio::test]
async fn test_round_trip_idempotency() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("events").await;
    db.setup_inbox("processed-events").await;
    db.setup_consumer_group("relay-idem", "events").await;

    // Publish a single message.
    db.publish_messages("events", &[serde_json::json!({"key": "value"})])
        .await;

    let rows = db
        .client
        .query(
            "SELECT id, payload FROM tide.tide_outbox_messages
             WHERE outbox_name = 'events' AND consumed_at IS NULL ORDER BY id",
            &[],
        )
        .await
        .unwrap();

    let id: i64 = rows[0].get(0);
    let payload: serde_json::Value = rows[0].get(1);
    let event_id = format!("events:{id}");

    // Deliver twice (simulating retry after crash).
    db.deliver_to_inbox("processed-events", &event_id, &payload)
        .await;
    db.deliver_to_inbox("processed-events", &event_id, &payload)
        .await;

    // Only one message in inbox thanks to UNIQUE constraint on event_id.
    db.assert_inbox_received("processed-events", 1).await;
}

#[tokio::test]
async fn test_round_trip_preserves_payload_integrity() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("complex").await;
    db.setup_inbox("complex-sink").await;

    // Publish a complex payload.
    let complex_payload = serde_json::json!({
        "nested": {"array": [1, 2, 3], "null_field": null},
        "unicode": "héllo wörld 🌊",
        "large_number": 9007199254740993_i64,
    });
    db.publish_messages("complex", std::slice::from_ref(&complex_payload))
        .await;

    let rows = db
        .client
        .query(
            "SELECT id, payload FROM tide.tide_outbox_messages
             WHERE outbox_name = 'complex' ORDER BY id",
            &[],
        )
        .await
        .unwrap();

    let id: i64 = rows[0].get(0);
    let read_payload: serde_json::Value = rows[0].get(1);

    // Payload should survive the round-trip through PostgreSQL JSONB.
    assert_eq!(read_payload, complex_payload);

    // Deliver to inbox and verify.
    db.deliver_to_inbox("complex-sink", &format!("complex:{id}"), &read_payload)
        .await;

    let inbox_row = db
        .client
        .query_one(
            r#"SELECT payload FROM tide."complex-sink_inbox" WHERE event_id = $1"#,
            &[&format!("complex:{id}")],
        )
        .await
        .unwrap();
    let inbox_payload: serde_json::Value = inbox_row.get(0);
    assert_eq!(inbox_payload, complex_payload);
}
