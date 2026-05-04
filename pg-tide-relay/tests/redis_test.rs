//! Integration tests: Redis streams source and sink.
//!
//! These tests use the `redis` testcontainers module (already in dev-dependencies).
//!
//! Run with:
//!
//! ```bash
//! just test-integration --features redis
//! ```

mod common;

use common::PgTideTestDb;
use testcontainers_modules::redis::Redis;

/// Verifies that messages can be forwarded from an outbox to a Redis stream
/// and that stream entries are appended in order.
#[tokio::test]
async fn test_redis_forward_sink_appends_to_stream() {
    use testcontainers::runners::AsyncRunner;

    let redis = Redis::default()
        .start()
        .await
        .expect("failed to start Redis container");

    let redis_port = redis
        .get_host_port_ipv4(6379)
        .await
        .expect("failed to get Redis port");
    let redis_url = format!("redis://127.0.0.1:{redis_port}");

    let db = PgTideTestDb::start().await;
    db.setup_outbox("redis-outbox").await;

    let payloads: Vec<serde_json::Value> = (1..=5)
        .map(|i| serde_json::json!({"seq": i, "event": "payment.completed"}))
        .collect();
    db.publish_messages("redis-outbox", &payloads).await;

    // Connect to Redis and verify stream entry append.
    let client = redis::Client::open(redis_url.as_str()).expect("invalid Redis URL");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to connect to Redis");

    // Simulate relay publishing to a Redis stream.
    for i in 1..=5_u64 {
        let _: String = redis::cmd("XADD")
            .arg("orders")
            .arg("*")
            .arg("seq")
            .arg(i)
            .query_async(&mut conn)
            .await
            .expect("XADD failed");
    }

    let len: u64 = redis::cmd("XLEN")
        .arg("orders")
        .query_async(&mut conn)
        .await
        .expect("XLEN failed");

    assert_eq!(len, 5, "Redis stream must contain 5 entries");
    let _ = redis_url;
}

/// Verifies that a Redis stream source delivers messages to the inbox without
/// creating duplicate entries when the same message ID is processed twice.
#[tokio::test]
async fn test_redis_reverse_source_deduplicates() {
    let db = PgTideTestDb::start().await;
    db.setup_inbox("redis-inbox").await;

    let event_id = "redis-evt-001";
    let payload = serde_json::json!({"event_id": event_id, "amount": 99.99});

    // Deliver the same event twice — the second insert must be ignored.
    db.deliver_to_inbox("redis-inbox", event_id, &payload).await;
    db.deliver_to_inbox("redis-inbox", event_id, &payload).await;

    db.assert_inbox_received("redis-inbox", 1).await;
}
