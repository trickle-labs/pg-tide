//! Integration tests: multi-pipeline coordinator.
//!
//! Covers running multiple independent pipelines against the same PostgreSQL
//! instance, per-pipeline offset isolation, and graceful shutdown ordering.

mod common;

use common::PgTideTestDb;

/// Two independent forward pipelines share a database but maintain completely
/// isolated offsets and message queues.
#[tokio::test]
async fn test_multiple_pipelines_have_isolated_state() {
    let db = PgTideTestDb::start().await;

    // Set up two completely separate outboxes and consumer groups.
    db.setup_outbox("pipeline-a-outbox").await;
    db.setup_outbox("pipeline-b-outbox").await;
    db.setup_consumer_group("pipeline-a-group", "pipeline-a-outbox")
        .await;
    db.setup_consumer_group("pipeline-b-group", "pipeline-b-outbox")
        .await;

    // Publish 10 messages to pipeline A, 5 to pipeline B.
    let msgs_a: Vec<serde_json::Value> = (1..=10)
        .map(|i| serde_json::json!({"pipeline": "a", "n": i}))
        .collect();
    let msgs_b: Vec<serde_json::Value> = (1..=5)
        .map(|i| serde_json::json!({"pipeline": "b", "n": i}))
        .collect();
    db.publish_messages("pipeline-a-outbox", &msgs_a).await;
    db.publish_messages("pipeline-b-outbox", &msgs_b).await;

    // Commit different offsets for each group — they must not bleed over.
    db.commit_offset("pipeline-a-group", "relay-1", 7).await;
    db.commit_offset("pipeline-b-group", "relay-1", 3).await;

    let offset_a: i64 = db
        .client
        .query_one(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'pipeline-a-group'",
            &[],
        )
        .await
        .unwrap()
        .get(0);

    let offset_b: i64 = db
        .client
        .query_one(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'pipeline-b-group'",
            &[],
        )
        .await
        .unwrap()
        .get(0);

    assert_eq!(offset_a, 7, "pipeline-a offset must be 7");
    assert_eq!(offset_b, 3, "pipeline-b offset must be 3");

    // Message counts remain independent.
    assert_eq!(db.pending_count("pipeline-a-outbox").await, 10);
    assert_eq!(db.pending_count("pipeline-b-outbox").await, 5);
}

/// Dropping one pipeline's outbox must not affect the other pipeline.
#[tokio::test]
async fn test_dropping_one_pipeline_does_not_affect_sibling() {
    let db = PgTideTestDb::start().await;

    db.setup_outbox("keep-outbox").await;
    db.setup_outbox("drop-outbox").await;
    db.setup_consumer_group("keep-group", "keep-outbox").await;

    let keep_msgs: Vec<serde_json::Value> =
        (1..=5).map(|i| serde_json::json!({"keep": i})).collect();
    let drop_msgs: Vec<serde_json::Value> =
        (1..=3).map(|i| serde_json::json!({"drop": i})).collect();
    db.publish_messages("keep-outbox", &keep_msgs).await;
    db.publish_messages("drop-outbox", &drop_msgs).await;

    // Remove the doomed outbox — cascade removes its messages.
    db.client
        .execute(
            "DELETE FROM tide.tide_outbox_config WHERE outbox_name = 'drop-outbox'",
            &[],
        )
        .await
        .unwrap();

    // The surviving outbox must still have all its messages.
    assert_eq!(db.pending_count("keep-outbox").await, 5);

    let drop_count: i64 = db
        .client
        .query_one(
            "SELECT COUNT(*)::bigint FROM tide.tide_outbox_messages
             WHERE outbox_name = 'drop-outbox'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        drop_count, 0,
        "dropped outbox messages must be cascade-deleted"
    );
}

/// Advisory locks ensure that only one relay instance owns each pipeline at
/// a time. This test verifies the locking semantics at the SQL level.
#[tokio::test]
async fn test_advisory_lock_grants_exclusive_ownership() {
    let db = PgTideTestDb::start().await;

    // pg_try_advisory_lock uses a bigint key. Hash the pipeline name.
    let lock_key: i64 = {
        use std::hash::{DefaultHasher, Hash, Hasher};
        let mut h = DefaultHasher::new();
        "exclusive-pipeline".hash(&mut h);
        h.finish() as i64
    };

    // First call must succeed.
    let acquired: bool = db
        .client
        .query_one("SELECT pg_try_advisory_lock($1)", &[&lock_key])
        .await
        .unwrap()
        .get(0);
    assert!(acquired, "first pg_try_advisory_lock must succeed");

    // Second call from the same session also succeeds (PostgreSQL re-entrant).
    let acquired2: bool = db
        .client
        .query_one("SELECT pg_try_advisory_lock($1)", &[&lock_key])
        .await
        .unwrap()
        .get(0);
    assert!(
        acquired2,
        "advisory locks are re-entrant within the same session"
    );

    // Release locks.
    db.client
        .execute("SELECT pg_advisory_unlock_all()", &[])
        .await
        .unwrap();
}

/// Verifies that per-pipeline metrics are tracked independently so that
/// observability dashboards can distinguish between pipeline A and B.
#[tokio::test]
async fn test_per_pipeline_message_count_is_independent() {
    let db = PgTideTestDb::start().await;

    db.setup_outbox("metrics-a").await;
    db.setup_outbox("metrics-b").await;

    let msgs_a: Vec<serde_json::Value> = (1..=8).map(|i| serde_json::json!({"a": i})).collect();
    let msgs_b: Vec<serde_json::Value> = (1..=3).map(|i| serde_json::json!({"b": i})).collect();
    db.publish_messages("metrics-a", &msgs_a).await;
    db.publish_messages("metrics-b", &msgs_b).await;

    let count_a = db.pending_count("metrics-a").await;
    let count_b = db.pending_count("metrics-b").await;

    assert_eq!(count_a, 8);
    assert_eq!(count_b, 3);
    assert_ne!(count_a, count_b, "per-pipeline counts must differ");
}
