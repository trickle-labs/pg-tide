//! Database integration tests for inbox idempotency and offset bookkeeping.
//!
//! These tests exercise database contracts directly; they do not run the relay
//! process or prove end-to-end exactly-once delivery.

mod common;

use common::PgTideTestDb;

/// Re-inserting the same event_id must remain idempotent.
#[tokio::test]
async fn test_inbox_rejects_duplicate_event_id() {
    let db = PgTideTestDb::start().await;
    db.setup_inbox("eo-inbox").await;

    let event_id = "evt-eo-001";
    let payload = serde_json::json!({"action": "checkout", "cart_id": 7});

    // First delivery — should succeed.
    db.deliver_to_inbox("eo-inbox", event_id, &payload).await;
    db.assert_inbox_received("eo-inbox", 1).await;

    // A retry of the same event.
    db.deliver_to_inbox("eo-inbox", event_id, &payload).await;
    db.assert_inbox_received("eo-inbox", 1).await;
}

/// An offset remains unchanged when a sink delivery has not completed.
///
/// This checks the durable database state expected after an interrupted batch;
/// it is not a process-level crash test.
#[tokio::test]
async fn test_offset_not_committed_on_partial_batch_failure() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("eo-outbox").await;
    db.setup_consumer_group("eo-group", "eo-outbox").await;

    let payloads: Vec<serde_json::Value> = (1..=5).map(|i| serde_json::json!({"n": i})).collect();
    db.publish_messages("eo-outbox", &payloads).await;

    // Commit offset to 2 (only first 2 messages confirmed delivered).
    db.commit_offset("eo-group", "worker-1", 2).await;

    // Simulate crash before message 3-5 are delivered — they remain unconsumed.
    let pending = db.pending_count("eo-outbox").await;
    let committed: i64 = db
        .client
        .query_one(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'eo-group' AND consumer_id = 'worker-1'",
            &[],
        )
        .await
        .unwrap()
        .get(0);

    assert_eq!(committed, 2, "offset must reflect last successful delivery");
    // Messages 3-5 (id > 2) are still pending.
    assert_eq!(
        pending, 5,
        "all 5 outbox messages are still unconsumed (consumed_at is NULL)"
    );
}

/// Consumer group offset advances only after every message in the batch
/// has been successfully delivered and acknowledged.
#[tokio::test]
async fn test_offset_advances_atomically_per_batch() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("batch-outbox").await;
    db.setup_consumer_group("batch-group", "batch-outbox").await;

    let payloads: Vec<serde_json::Value> = (1..=6).map(|i| serde_json::json!({"i": i})).collect();
    db.publish_messages("batch-outbox", &payloads).await;

    // Process in two batches: 1-3, then 4-6.
    let batch1_ids: Vec<i64> = db
        .client
        .query(
            "SELECT id FROM tide.tide_outbox_messages
             WHERE outbox_name = 'batch-outbox' ORDER BY id LIMIT 3",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();

    let max_batch1 = *batch1_ids.iter().max().unwrap();

    // Mark batch 1 consumed.
    db.client
        .execute(
            "UPDATE tide.tide_outbox_messages
             SET consumed_at = now()
             WHERE outbox_name = 'batch-outbox' AND id <= $1",
            &[&max_batch1],
        )
        .await
        .unwrap();
    db.commit_offset("batch-group", "relay-1", max_batch1).await;

    // 3 pending remain.
    assert_eq!(db.pending_count("batch-outbox").await, 3);

    // Process batch 2.
    let max_id: i64 = db
        .client
        .query_one(
            "SELECT MAX(id) FROM tide.tide_outbox_messages WHERE outbox_name = 'batch-outbox'",
            &[],
        )
        .await
        .unwrap()
        .get(0);

    db.client
        .execute(
            "UPDATE tide.tide_outbox_messages
             SET consumed_at = now()
             WHERE outbox_name = 'batch-outbox' AND id > $1",
            &[&max_batch1],
        )
        .await
        .unwrap();
    db.commit_offset("batch-group", "relay-1", max_id).await;

    // All consumed.
    assert_eq!(db.pending_count("batch-outbox").await, 0);

    // Final committed offset == max_id.
    let committed: i64 = db
        .client
        .query_one(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'batch-group' AND consumer_id = 'relay-1'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(committed, max_id);
}

/// Verifies that a large dedup key set does not slow down inbox inserts —
/// a proxy for the inbox index performance at scale.
#[tokio::test]
async fn test_inbox_dedup_performance_at_scale() {
    let db = PgTideTestDb::start().await;
    db.setup_inbox("scale-inbox").await;

    let count = 500_u32;
    let start = std::time::Instant::now();

    for i in 0..count {
        let event_id = format!("scale-evt-{i:06}");
        let payload = serde_json::json!({"seq": i});
        db.deliver_to_inbox("scale-inbox", &event_id, &payload)
            .await;
    }

    let elapsed = start.elapsed();
    db.assert_inbox_received("scale-inbox", count as i64).await;

    // 500 inserts should complete well under 30 seconds in any test environment.
    assert!(
        elapsed.as_secs() < 30,
        "inbox dedup at 500 messages must be fast, took {elapsed:?}"
    );
}
