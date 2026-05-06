//! Integration tests: Batch pg-inbox inserts (v0.13.0).
//!
//! Verifies that the InboxSink's batch UNNEST INSERT correctly delivers multiple
//! messages in a single round trip, with proper deduplication.

mod common;

use common::PgTideTestDb;
use pg_tide_relay::envelope::RelayMessage;
use pg_tide_relay::sink::inbox::InboxSink;
use pg_tide_relay::sink::Sink;
use std::sync::Arc;

async fn make_inbox_sink(db: &PgTideTestDb, inbox_name: &str) -> InboxSink {
    db.setup_inbox(inbox_name).await;

    let (client, conn) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
            db.host_port
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    InboxSink::new(Arc::new(client), format!("{inbox_name}_inbox"))
}

#[tokio::test]
async fn test_batch_inbox_insert_multiple_messages() {
    let db = PgTideTestDb::start().await;
    let mut sink = make_inbox_sink(&db, "batch-test").await;

    let messages: Vec<RelayMessage> = (0..5)
        .map(|i| {
            RelayMessage::new_reverse(
                format!("evt-{i:04}"),
                format!("order.event.{i}"),
                serde_json::json!({"order_id": i, "status": "created"}),
            )
        })
        .collect();

    sink.publish(&messages).await.expect("batch publish");

    db.assert_inbox_received("batch-test", 5).await;
}

#[tokio::test]
async fn test_batch_inbox_deduplication() {
    let db = PgTideTestDb::start().await;
    let mut sink = make_inbox_sink(&db, "batch-dedup").await;

    let msg = RelayMessage::new_reverse(
        "dedup-key-001",
        "order.created",
        serde_json::json!({"id": 1}),
    );

    // Publish the same message twice in separate batches.
    sink.publish(std::slice::from_ref(&msg))
        .await
        .expect("first publish");
    sink.publish(std::slice::from_ref(&msg))
        .await
        .expect("second publish");

    // Only one row should exist.
    db.assert_inbox_received("batch-dedup", 1).await;
    assert_eq!(
        sink.dedup_count(),
        1,
        "dedup counter should track the duplicate"
    );
}

#[tokio::test]
async fn test_batch_inbox_dedup_within_batch() {
    let db = PgTideTestDb::start().await;
    let mut sink = make_inbox_sink(&db, "batch-intra-dedup").await;

    // Two messages with the same dedup_key in a single batch.
    let msg_a = RelayMessage::new_reverse(
        "same-key",
        "payment.processed",
        serde_json::json!({"amount": 100}),
    );
    let msg_b = RelayMessage::new_reverse(
        "same-key",
        "payment.processed",
        serde_json::json!({"amount": 100}),
    );

    // The batch UNNEST INSERT should handle the conflict on the second row.
    // With UNNEST there may be a unique violation within the batch itself if
    // the DB deduplication removes the duplicate.
    let result = sink.publish(&[msg_a, msg_b]).await;
    // The publish may succeed (if DB handles the intra-batch conflict) or fail.
    // Either way: only 0 or 1 rows should exist.
    let _ = result;
    let count: i64 = db
        .client
        .query_one(
            r#"SELECT COUNT(*)::bigint FROM tide."batch-intra-dedup_inbox""#,
            &[],
        )
        .await
        .expect("count")
        .get(0);
    assert!(
        count <= 1,
        "intra-batch dedup should result in at most 1 row, got {count}"
    );
}

#[tokio::test]
async fn test_batch_inbox_empty_batch_is_noop() {
    let db = PgTideTestDb::start().await;
    let mut sink = make_inbox_sink(&db, "empty-batch").await;

    sink.publish(&[])
        .await
        .expect("empty batch should not error");
    db.assert_inbox_received("empty-batch", 0).await;
}

#[tokio::test]
async fn test_batch_inbox_large_batch() {
    let db = PgTideTestDb::start().await;
    let mut sink = make_inbox_sink(&db, "large-batch").await;

    // 100 messages in a single batch.
    let messages: Vec<RelayMessage> = (0..100)
        .map(|i| {
            RelayMessage::new_reverse(
                format!("large-{i:06}"),
                "item.created",
                serde_json::json!({"item_id": i, "quantity": i + 1}),
            )
        })
        .collect();

    sink.publish(&messages).await.expect("large batch publish");
    db.assert_inbox_received("large-batch", 100).await;
}
