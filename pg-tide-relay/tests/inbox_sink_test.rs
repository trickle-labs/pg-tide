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
