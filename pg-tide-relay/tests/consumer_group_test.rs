//! Integration test: consumer groups — offset tracking, multiple consumers, heartbeat.

mod common;

use common::PgTideTestDb;

#[tokio::test]
async fn test_multiple_consumer_groups_independent_offsets() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("shared-outbox").await;
    db.setup_consumer_group("group-a", "shared-outbox").await;
    db.setup_consumer_group("group-b", "shared-outbox").await;

    // Publish messages.
    let payloads: Vec<serde_json::Value> = (1..=10).map(|i| serde_json::json!({"n": i})).collect();
    db.publish_messages("shared-outbox", &payloads).await;

    // Group A commits at offset 7, Group B at offset 3.
    db.commit_offset("group-a", "consumer-1", 7).await;
    db.commit_offset("group-b", "consumer-1", 3).await;

    // Verify independent offsets.
    let row_a = db
        .client
        .query_one(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'group-a'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row_a.get::<_, i64>(0), 7);

    let row_b = db
        .client
        .query_one(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'group-b'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row_b.get::<_, i64>(0), 3);
}

#[tokio::test]
async fn test_consumer_heartbeat_updates_timestamp() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("heartbeat-test").await;
    db.setup_consumer_group("hb-group", "heartbeat-test").await;
    db.commit_offset("hb-group", "worker-1", 0).await;

    // Record initial heartbeat time.
    let before = db
        .client
        .query_one(
            "SELECT last_heartbeat FROM tide.tide_consumer_offsets
             WHERE group_name = 'hb-group' AND consumer_id = 'worker-1'",
            &[],
        )
        .await
        .unwrap();
    let ts_before: chrono::DateTime<chrono::Utc> = before.get(0);

    // Wait a tiny bit and update heartbeat.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    db.client
        .execute(
            "UPDATE tide.tide_consumer_offsets SET last_heartbeat = now()
             WHERE group_name = 'hb-group' AND consumer_id = 'worker-1'",
            &[],
        )
        .await
        .unwrap();

    let after = db
        .client
        .query_one(
            "SELECT last_heartbeat FROM tide.tide_consumer_offsets
             WHERE group_name = 'hb-group' AND consumer_id = 'worker-1'",
            &[],
        )
        .await
        .unwrap();
    let ts_after: chrono::DateTime<chrono::Utc> = after.get(0);

    assert!(ts_after > ts_before, "heartbeat timestamp should advance");
}

#[tokio::test]
async fn test_consumer_lag_view() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("lag-test").await;
    db.setup_consumer_group("lag-group", "lag-test").await;

    // Publish 20 messages.
    let payloads: Vec<serde_json::Value> = (1..=20).map(|i| serde_json::json!({"i": i})).collect();
    db.publish_messages("lag-test", &payloads).await;

    // Commit offset at 5 — lag should be max_id - 5.
    db.commit_offset("lag-group", "c1", 5).await;

    let row = db
        .client
        .query_one(
            "SELECT lag FROM tide.consumer_lag
             WHERE group_name = 'lag-group' AND consumer_id = 'c1'",
            &[],
        )
        .await
        .unwrap();
    let lag: i64 = row.get(0);

    // Lag should be positive (max_id - 5).
    assert!(lag > 0, "consumer lag should be positive, got {lag}");
}

#[tokio::test]
async fn test_drop_consumer_group_cascades() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("cascade-test").await;
    db.setup_consumer_group("doomed-group", "cascade-test")
        .await;
    db.commit_offset("doomed-group", "worker-a", 10).await;

    // Drop the consumer group.
    db.client
        .execute(
            "DELETE FROM tide.tide_consumer_groups WHERE group_name = 'doomed-group'",
            &[],
        )
        .await
        .unwrap();

    // Offsets should be cascaded away.
    let count = db
        .client
        .query_one(
            "SELECT COUNT(*)::bigint FROM tide.tide_consumer_offsets
             WHERE group_name = 'doomed-group'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(count.get::<_, i64>(0), 0);
}
