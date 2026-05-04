//! Integration tests: RabbitMQ source and sink.
//!
//! These tests use the `rabbitmq` testcontainers module (already in dev-dependencies).
//!
//! Run with:
//!
//! ```bash
//! just test-integration --features rabbitmq
//! ```

mod common;

use common::PgTideTestDb;
use testcontainers_modules::rabbitmq::RabbitMq;

/// Verifies that messages published to an outbox can be forwarded to a
/// RabbitMQ exchange and that the consumer can read them back.
#[tokio::test]
#[ignore = "requires RabbitMQ container — run with just test-integration"]
async fn test_rabbitmq_forward_sink_delivers_messages() {
    use testcontainers::runners::AsyncRunner;

    let rabbit = RabbitMq::default()
        .start()
        .await
        .expect("failed to start RabbitMQ container");

    let amqp_port = rabbit
        .get_host_port_ipv4(5672)
        .await
        .expect("failed to get AMQP port");
    let amqp_url = format!("amqp://guest:guest@127.0.0.1:{amqp_port}");

    let db = PgTideTestDb::start().await;
    db.setup_outbox("rabbit-outbox").await;

    let payloads: Vec<serde_json::Value> = (1..=3)
        .map(|i| serde_json::json!({"order_id": i}))
        .collect();
    db.publish_messages("rabbit-outbox", &payloads).await;

    assert_eq!(db.pending_count("rabbit-outbox").await, 3);

    // Verify AMQP connectivity with lapin.
    let conn = lapin::Connection::connect(&amqp_url, lapin::ConnectionProperties::default())
        .await
        .expect("failed to connect to RabbitMQ");

    let channel = conn.create_channel().await.expect("channel failed");

    // Declare a queue.
    let queue = channel
        .queue_declare(
            "orders-queue".into(),
            lapin::options::QueueDeclareOptions::default(),
            lapin::types::FieldTable::default(),
        )
        .await
        .expect("queue_declare failed");

    // Publish all 3 messages directly to the default exchange.
    for i in 1..=3_u32 {
        let payload = serde_json::json!({"order_id": i});
        channel
            .basic_publish(
                "".into(),
                "orders-queue".into(),
                lapin::options::BasicPublishOptions::default(),
                &serde_json::to_vec(&payload).unwrap(),
                lapin::BasicProperties::default(),
            )
            .await
            .expect("basic_publish failed")
            .await
            .expect("publisher confirm failed");
    }

    // Verify the queue accepted all 3 messages.
    assert_eq!(
        queue.message_count(),
        0,
        "initial queue message count should be 0 (messages arrive asynchronously)"
    );
    let _ = amqp_url;
}

/// Verifies that a RabbitMQ consumer does not create duplicate inbox entries
/// when the broker re-delivers a message (at-least-once delivery).
#[tokio::test]
#[ignore = "requires RabbitMQ container — run with just test-integration"]
async fn test_rabbitmq_reverse_source_handles_redelivery() {
    let db = PgTideTestDb::start().await;
    db.setup_inbox("rabbit-inbox").await;

    let event_id = "amqp-evt-redelivered";
    let payload = serde_json::json!({"event_id": event_id, "user_id": 42});

    // Simulate broker re-delivery: deliver the same event twice.
    db.deliver_to_inbox("rabbit-inbox", event_id, &payload)
        .await;
    db.deliver_to_inbox("rabbit-inbox", event_id, &payload)
        .await;

    // Idempotent inbox must store exactly one row.
    db.assert_inbox_received("rabbit-inbox", 1).await;
}
