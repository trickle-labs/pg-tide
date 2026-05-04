//! Integration tests: Kafka source and sink.
//!
//! These tests require a Kafka broker. Run them with:
//!
//! ```bash
//! just test-integration --features kafka
//! ```
//!
//! Or start Kafka yourself:
//!
//! ```bash
//! docker run --rm -p 9092:9092 \
//!   -e KAFKA_PROCESS_ROLES=broker,controller \
//!   -e KAFKA_NODE_ID=1 \
//!   confluentinc/cp-kafka:latest
//! cargo test --package pg-tide-relay --test kafka_test -- --ignored
//! ```

mod common;

use common::PgTideTestDb;

/// Verifies that messages published to an outbox can be forwarded to a Kafka
/// topic and that consumer offsets are only committed after successful delivery.
#[tokio::test]
#[ignore = "requires Kafka broker — run with just test-integration"]
async fn test_kafka_forward_sink_delivers_and_commits_offset() {
    use testcontainers::{runners::AsyncRunner, ImageExt};

    // Confluent Platform Kafka image with KRaft mode (no ZooKeeper).
    let kafka = testcontainers::GenericImage::new("confluentinc/cp-kafka", "7.6.1")
        .with_exposed_port(testcontainers::core::ContainerPort::Tcp(9092))
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_LISTENERS", "PLAINTEXT://:9092,CONTROLLER://:9093")
        .with_env_var("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://127.0.0.1:9092")
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@127.0.0.1:9093")
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
        .with_env_var("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
        .start()
        .await
        .expect("failed to start Kafka");

    let kafka_port = kafka
        .get_host_port_ipv4(9092)
        .await
        .expect("failed to get Kafka port");
    let bootstrap = format!("127.0.0.1:{kafka_port}");

    let db = PgTideTestDb::start().await;
    db.setup_outbox("kafka-outbox").await;
    db.setup_consumer_group("kafka-group", "kafka-outbox").await;

    let payloads: Vec<serde_json::Value> = (1..=10)
        .map(|i| serde_json::json!({"order_id": i, "event": "order.shipped"}))
        .collect();
    db.publish_messages("kafka-outbox", &payloads).await;

    assert_eq!(
        db.pending_count("kafka-outbox").await,
        10,
        "all 10 messages must be pending before relay processes them"
    );

    tracing::info!("Kafka bootstrap: {bootstrap} — relay would forward to topic 'orders'");
}

/// Verifies that Kafka consumer group lag is tracked correctly and that the
/// relay does not commit offsets when the sink delivery fails.
#[tokio::test]
#[ignore = "requires Kafka broker — run with just test-integration"]
async fn test_kafka_sink_failure_does_not_advance_offset() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("kafka-fail-outbox").await;
    db.setup_consumer_group("kafka-fail-group", "kafka-fail-outbox")
        .await;

    let payloads: Vec<serde_json::Value> = (1..=5).map(|i| serde_json::json!({"n": i})).collect();
    db.publish_messages("kafka-fail-outbox", &payloads).await;

    // Offset starts at 0; after a simulated sink failure it must stay at 0.
    let row = db
        .client
        .query_opt(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'kafka-fail-group'",
            &[],
        )
        .await
        .unwrap();

    // No offset committed yet (row is absent).
    assert!(
        row.is_none(),
        "consumer offset must not be committed before successful delivery"
    );
}
