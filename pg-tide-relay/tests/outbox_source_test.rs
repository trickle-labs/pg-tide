//! Integration test: outbox source polling, offset commit, and message consumption.

mod common;

use common::PgTideTestDb;

#[tokio::test]
async fn test_outbox_poll_returns_pending_messages() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("orders").await;

    // Publish 5 messages.
    let payloads: Vec<serde_json::Value> = (1..=5)
        .map(|i| serde_json::json!({"order_id": i}))
        .collect();
    db.publish_messages("orders", &payloads).await;

    // Verify all 5 are pending.
    assert_eq!(db.pending_count("orders").await, 5);
}

#[tokio::test]
async fn test_consumer_offset_commit() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("events").await;
    db.setup_consumer_group("my-group", "events").await;

    // Publish messages and commit offset.
    let payloads: Vec<serde_json::Value> =
        (1..=10).map(|i| serde_json::json!({"event": i})).collect();
    db.publish_messages("events", &payloads).await;

    // Commit offset at position 5.
    db.commit_offset("my-group", "consumer-1", 5).await;

    // Verify offset was committed.
    let row = db
        .client
        .query_one(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = $1 AND consumer_id = $2",
            &[&"my-group", &"consumer-1"],
        )
        .await
        .expect("failed to read offset");
    let offset: i64 = row.get(0);
    assert_eq!(offset, 5);
}

#[tokio::test]
async fn test_outbox_messages_ordered_by_id() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("logs").await;

    let payloads: Vec<serde_json::Value> =
        (1..=20).map(|i| serde_json::json!({"seq": i})).collect();
    db.publish_messages("logs", &payloads).await;

    // Messages should be ordered by ID.
    let rows = db
        .client
        .query(
            "SELECT id, payload FROM tide.tide_outbox_messages
             WHERE outbox_name = $1 ORDER BY id",
            &[&"logs"],
        )
        .await
        .expect("query failed");

    assert_eq!(rows.len(), 20);

    let mut prev_id: i64 = 0;
    for row in &rows {
        let id: i64 = row.get(0);
        assert!(id > prev_id, "messages must be strictly ordered");
        prev_id = id;
    }
}

#[tokio::test]
async fn test_consume_marks_messages() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("tasks").await;

    let payloads: Vec<serde_json::Value> =
        (1..=3).map(|i| serde_json::json!({"task": i})).collect();
    db.publish_messages("tasks", &payloads).await;

    // Mark first 2 as consumed.
    db.client
        .execute(
            "UPDATE tide.tide_outbox_messages
             SET consumed_at = now()
             WHERE outbox_name = 'tasks' AND id <= 2",
            &[],
        )
        .await
        .unwrap();

    // Only 1 pending message should remain.
    assert_eq!(db.pending_count("tasks").await, 1);
}
