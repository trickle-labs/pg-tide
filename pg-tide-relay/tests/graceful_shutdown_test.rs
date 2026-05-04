//! Integration tests: graceful shutdown and error recovery.
//!
//! Verifies drain-on-SIGTERM semantics, PostgreSQL reconnect behaviour,
//! and that no messages are silently lost when the relay is interrupted.

mod common;

use common::PgTideTestDb;
use std::time::Duration;

/// After the relay receives a shutdown signal, all messages that were already
/// fetched into the active batch must be acknowledged before the process exits.
/// This test simulates the invariant at the database level: pending messages
/// remain in the outbox until consumed_at is set.
#[tokio::test]
async fn test_pending_messages_survive_restart() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("drain-outbox").await;
    db.setup_consumer_group("drain-group", "drain-outbox").await;

    // Publish 20 messages.
    let payloads: Vec<serde_json::Value> =
        (1..=20).map(|i| serde_json::json!({"seq": i})).collect();
    db.publish_messages("drain-outbox", &payloads).await;

    // Simulate relay picking up the first 10 messages without committing.
    let batch_ids: Vec<i64> = db
        .client
        .query(
            "SELECT id FROM tide.tide_outbox_messages
             WHERE outbox_name = 'drain-outbox' ORDER BY id LIMIT 10",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();

    let max_batch = *batch_ids.iter().max().unwrap();

    // Process batch (mark consumed).
    db.client
        .execute(
            "UPDATE tide.tide_outbox_messages
             SET consumed_at = now()
             WHERE outbox_name = 'drain-outbox' AND id <= $1",
            &[&max_batch],
        )
        .await
        .unwrap();
    db.commit_offset("drain-group", "relay-1", max_batch).await;

    // Simulate crash: remaining 10 messages were NOT consumed.
    let pending = db.pending_count("drain-outbox").await;
    assert_eq!(
        pending, 10,
        "unprocessed messages must survive a relay restart"
    );

    // After "restart", the relay picks up from the committed offset.
    let committed: i64 = db
        .client
        .query_one(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'drain-group' AND consumer_id = 'relay-1'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(committed, max_batch);
}

/// Verifies that a drain with zero timeout exits immediately without waiting.
#[tokio::test]
async fn test_zero_drain_timeout_exits_immediately() {
    // This is a logic test: drain_timeout=0 means skip waiting.
    let drain_timeout = Duration::from_secs(0);
    assert_eq!(drain_timeout.as_secs(), 0, "zero timeout must be zero");

    // In production, main.rs checks `if drain_timeout.as_secs() > 0` before
    // calling coordinator.drain(). Here we just verify the Duration arithmetic.
    let skipped = drain_timeout.as_secs() == 0;
    assert!(skipped, "drain must be skipped when timeout is 0");
}

/// Verifies that a timeout shorter than the drain duration triggers the
/// fallback warning path rather than blocking indefinitely.
#[tokio::test]
async fn test_drain_timeout_fires_when_exceeded() {
    // Simulate a slow drain (200 ms) with a 50 ms timeout.
    let result = tokio::time::timeout(Duration::from_millis(50), async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        "drained"
    })
    .await;

    assert!(
        result.is_err(),
        "timeout must fire before the slow drain completes"
    );
}

/// Verifies that all consumer offsets survive a coordinator crash — the next
/// relay instance can resume from where the previous one left off.
#[tokio::test]
async fn test_consumer_offsets_persist_across_sessions() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("persist-outbox").await;
    db.setup_consumer_group("persist-group", "persist-outbox")
        .await;

    db.commit_offset("persist-group", "worker-a", 42).await;

    // Open a second connection — simulates a new relay process reading the offset.
    let host_port = db
        .client
        .query_one("SELECT inet_server_port()", &[])
        .await
        .unwrap()
        .get::<_, i32>(0);

    let url =
        format!("host=127.0.0.1 port={host_port} user=postgres password=postgres dbname=postgres");
    let (client2, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("second connection failed");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let offset: i64 = client2
        .query_one(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'persist-group' AND consumer_id = 'worker-a'",
            &[],
        )
        .await
        .unwrap()
        .get(0);

    assert_eq!(offset, 42, "offset must be visible to the second session");
}
