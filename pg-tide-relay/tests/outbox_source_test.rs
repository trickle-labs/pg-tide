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

// ── v0.40.0 (ADR-011) native shared-table source contract tests ───────────

use pg_tide_relay::envelope::AckToken;
use pg_tide_relay::source::outbox::OutboxPollerSource;
use pg_tide_relay::source::Source;
use std::sync::Arc;
use tokio_postgres::NoTls;

/// Open a standalone client to the same container (the source owns an Arc).
async fn source_client(db: &PgTideTestDb) -> Arc<tokio_postgres::Client> {
    let url = format!(
        "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
        db.host_port
    );
    let (client, conn) = tokio_postgres::connect(&url, NoTls)
        .await
        .expect("connect source client");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Arc::new(client)
}

async fn native_source(
    db: &PgTideTestDb,
    outbox: &str,
    group: &str,
    pipeline: &str,
) -> OutboxPollerSource {
    OutboxPollerSource::new_simple_native(
        source_client(db).await,
        outbox,
        "{outbox}.{op}",
        group,
        pipeline,
    )
    .await
    .expect("native source")
}

#[tokio::test]
async fn native_poll_isolates_outboxes_and_orders_by_id() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("orders").await;
    db.setup_outbox("payments").await;

    // Interleave global IDs across the two logical outboxes.
    for i in 0..6 {
        let outbox = if i % 2 == 0 { "orders" } else { "payments" };
        db.publish_messages(outbox, &[serde_json::json!({"seq": i})])
            .await;
    }

    let mut orders = native_source(&db, "orders", "g", "orders-p").await;
    let batch = orders.poll(100).await.expect("poll orders");
    // Only orders rows, strictly increasing outbox_id.
    assert_eq!(
        batch.len(),
        3,
        "orders outbox must yield exactly its 3 rows"
    );
    let mut prev = 0i64;
    for msg in &batch {
        assert_eq!(msg.outbox_name.as_deref(), Some("orders"));
        let id = msg.outbox_id.expect("outbox_id");
        assert!(id > prev, "ids must be strictly increasing");
        prev = id;
        assert!(
            msg.dedup_key.starts_with("outbox_orders:"),
            "stable dedup identity keeps the outbox_ prefix: {}",
            msg.dedup_key
        );
    }
}

#[tokio::test]
async fn native_poll_respects_mvcc_commit_boundary() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("orders").await;

    // Open a transaction that publishes but does not commit.
    let mut writer = source_client(&db).await;
    let writer = Arc::get_mut(&mut writer).unwrap();
    let tx = writer.transaction().await.expect("begin");
    tx.execute(
        "INSERT INTO tide.tide_outbox_messages (outbox_name, payload, headers) \
         VALUES ('orders', '{\"x\":1}'::jsonb, '{}'::jsonb)",
        &[],
    )
    .await
    .expect("insert in tx");

    // The relay's polling connection must not see the uncommitted row.
    let mut source = native_source(&db, "orders", "g", "orders-mvcc").await;
    let before = source.poll(100).await.expect("poll before commit");
    assert!(before.is_empty(), "uncommitted row must be invisible");

    tx.commit().await.expect("commit");
    let after = source.poll(100).await.expect("poll after commit");
    assert_eq!(after.len(), 1, "committed row becomes visible");
}

#[tokio::test]
async fn native_restart_begins_after_stored_offset() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("orders").await;
    for i in 0..4 {
        db.publish_messages("orders", &[serde_json::json!({"seq": i})])
            .await;
    }

    let mut src = native_source(&db, "orders", "g", "orders-restart").await;
    let first = src.poll(2).await.expect("poll 2");
    assert_eq!(first.len(), 2);
    let last = first.last().unwrap();
    src.acknowledge(last).await.expect("ack");

    // A fresh source (restart) must resume strictly after the stored offset.
    let mut restarted = native_source(&db, "orders", "g", "orders-restart").await;
    let resumed = restarted.poll(100).await.expect("poll after restart");
    assert_eq!(
        resumed.len(),
        2,
        "restart resumes after the committed offset"
    );
    let acked_id = match last.ack_token {
        AckToken::OutboxOffset(v) => v,
        _ => panic!("expected outbox offset ack token"),
    };
    for msg in &resumed {
        assert!(
            msg.outbox_id.unwrap() > acked_id,
            "resumed rows must be strictly after the stored offset"
        );
    }
}

#[tokio::test]
async fn native_same_pipeline_name_different_outbox_has_separate_offset() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("orders").await;
    db.setup_outbox("payments").await;
    for i in 0..3 {
        db.publish_messages("orders", &[serde_json::json!({"o": i})])
            .await;
        db.publish_messages("payments", &[serde_json::json!({"p": i})])
            .await;
    }

    // Same pipeline_id, different outboxes — offsets must be independent.
    let mut a = native_source(&db, "orders", "g", "shared").await;
    let ba = a.poll(100).await.expect("poll orders");
    a.acknowledge(ba.last().unwrap()).await.expect("ack orders");

    let mut b = native_source(&db, "payments", "g", "shared").await;
    let bb = b.poll(100).await.expect("poll payments");
    assert_eq!(
        bb.len(),
        3,
        "the payments pipeline must not inherit the orders offset"
    );
}

#[tokio::test]
async fn native_stable_dedup_ids_survive_second_poll() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("orders").await;
    for i in 0..3 {
        db.publish_messages("orders", &[serde_json::json!({"seq": i})])
            .await;
    }

    let mut s1 = native_source(&db, "orders", "g", "orders-dedup").await;
    let first: Vec<String> = s1
        .poll(100)
        .await
        .expect("poll 1")
        .into_iter()
        .map(|m| m.dedup_key)
        .collect();

    // A second, independent poll from offset 0 yields identical dedup keys.
    let mut s2 = native_source(&db, "orders", "g", "orders-dedup-2").await;
    let second: Vec<String> = s2
        .poll(100)
        .await
        .expect("poll 2")
        .into_iter()
        .map(|m| m.dedup_key)
        .collect();

    assert_eq!(
        first, second,
        "stable dedup identities must survive re-poll"
    );
}
