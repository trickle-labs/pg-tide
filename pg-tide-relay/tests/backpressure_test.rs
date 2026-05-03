//! Integration test: high-volume publish — verify no message loss under load.

mod common;

use common::PgTideTestDb;

#[tokio::test]
async fn test_high_volume_publish_no_loss() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("high-volume").await;

    const MESSAGE_COUNT: usize = 1000;

    // Publish 1000 messages in batches of 100.
    for batch_start in (0..MESSAGE_COUNT).step_by(100) {
        let payloads: Vec<serde_json::Value> = (batch_start..batch_start + 100)
            .map(|i| serde_json::json!({"seq": i, "batch": batch_start / 100}))
            .collect();
        db.publish_messages("high-volume", &payloads).await;
    }

    // Verify all messages are present.
    let row = db
        .client
        .query_one(
            "SELECT COUNT(*)::bigint FROM tide.tide_outbox_messages
             WHERE outbox_name = 'high-volume'",
            &[],
        )
        .await
        .unwrap();
    let total: i64 = row.get(0);
    assert_eq!(total, MESSAGE_COUNT as i64);

    // Verify all are pending (none consumed).
    assert_eq!(db.pending_count("high-volume").await, MESSAGE_COUNT as i64);
}

#[tokio::test]
async fn test_high_volume_consume_all() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("consume-all").await;
    db.setup_consumer_group("bulk-consumer", "consume-all")
        .await;

    const MESSAGE_COUNT: usize = 500;

    let payloads: Vec<serde_json::Value> = (0..MESSAGE_COUNT)
        .map(|i| serde_json::json!({"n": i}))
        .collect();
    db.publish_messages("consume-all", &payloads).await;

    // Simulate relay consuming all messages in batches.
    let mut last_offset: i64 = 0;
    loop {
        let rows = db
            .client
            .query(
                "SELECT id FROM tide.tide_outbox_messages
                 WHERE outbox_name = 'consume-all' AND consumed_at IS NULL AND id > $1
                 ORDER BY id LIMIT 100",
                &[&last_offset],
            )
            .await
            .unwrap();

        if rows.is_empty() {
            break;
        }

        let batch_max: i64 = rows.last().unwrap().get(0);

        // Mark as consumed.
        db.client
            .execute(
                "UPDATE tide.tide_outbox_messages SET consumed_at = now()
                 WHERE outbox_name = 'consume-all' AND id > $1 AND id <= $2",
                &[&last_offset, &batch_max],
            )
            .await
            .unwrap();

        last_offset = batch_max;
    }

    // Commit final offset.
    db.commit_offset("bulk-consumer", "relay-0", last_offset)
        .await;

    // Verify: no pending messages.
    assert_eq!(db.pending_count("consume-all").await, 0);

    // Verify: offset matches last message ID.
    let row = db
        .client
        .query_one(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'bulk-consumer'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), last_offset);
}

#[tokio::test]
async fn test_concurrent_outbox_isolation() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("outbox-a").await;
    db.setup_outbox("outbox-b").await;

    // Publish to both outboxes.
    let payloads_a: Vec<serde_json::Value> = (1..=100)
        .map(|i| serde_json::json!({"from": "a", "n": i}))
        .collect();
    let payloads_b: Vec<serde_json::Value> = (1..=200)
        .map(|i| serde_json::json!({"from": "b", "n": i}))
        .collect();

    db.publish_messages("outbox-a", &payloads_a).await;
    db.publish_messages("outbox-b", &payloads_b).await;

    // Verify isolation: each outbox has the correct count.
    assert_eq!(db.pending_count("outbox-a").await, 100);
    assert_eq!(db.pending_count("outbox-b").await, 200);
}
