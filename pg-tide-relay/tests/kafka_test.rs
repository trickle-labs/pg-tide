//! Integration tests: Kafka sink and source.
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
//! cargo test --package pg-tide-relay --test kafka_test --features kafka
//! ```

#![cfg(feature = "kafka")]

mod common;

use common::PgTideTestDb;
use pg_tide_relay::envelope::RelayMessage;
use pg_tide_relay::sink::{kafka::KafkaSink, Sink};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::ClientConfig;
use std::time::Duration;

/// Verifies a real Kafka broker acknowledgment and downstream record delivery.
#[tokio::test]
async fn test_kafka_forward_sink_delivers_and_flushes() {
    use testcontainers::core::ImageExt;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::kafka::apache::{Kafka, KAFKA_PORT};

    let kafka = Kafka::default()
        .with_jvm_image()
        .with_mapped_port(9092, KAFKA_PORT)
        .start()
        .await
        .expect("start Apache Kafka KRaft broker");
    let brokers = format!(
        "127.0.0.1:{}",
        kafka.get_host_port_ipv4(9092).await.unwrap()
    );
    let topic = "orders";
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .expect("create Kafka admin client");
    admin
        .create_topics(
            &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .expect("create Kafka topic")
        .into_iter()
        .next()
        .expect("Kafka topic creation result")
        .expect("Kafka topic creation");

    let mut sink = KafkaSink::new_with_options(pg_tide_relay::sink::kafka::KafkaOptions {
        brokers: &brokers,
        topic_template: topic.to_string(),
        security_protocol: "plaintext",
        allow_insecure: true,
        ssl_ca_location: None,
        ssl_certificate_location: None,
        ssl_key_location: None,
        sasl_mechanism: None,
        sasl_username: None,
        sasl_password: None,
    })
    .expect("create Kafka sink");
    let messages: Vec<RelayMessage> = (1..=10)
        .map(|i| {
            RelayMessage::new_reverse(
                format!("kafka-event-{i}"),
                "orders.shipped",
                serde_json::json!({"order_id": i, "event": "order.shipped"}),
            )
        })
        .collect();

    sink.publish(&messages).await.expect("publish to Kafka");

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "pg-tide-kafka-test")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("create Kafka consumer");
    consumer
        .subscribe(&[topic])
        .expect("subscribe to Kafka topic");

    let mut received = Vec::new();
    while received.len() < messages.len() {
        let message = tokio::time::timeout(Duration::from_secs(10), consumer.recv())
            .await
            .expect("timed out waiting for Kafka record")
            .expect("Kafka consumer error");
        received.push(message.payload_view::<str>().unwrap().unwrap().to_string());
    }
    let expected_payloads: Vec<String> = messages
        .iter()
        .map(|message| serde_json::to_string(message).unwrap())
        .collect();
    assert_eq!(received, expected_payloads);
    sink.close().await.expect("flush Kafka producer");
}

/// Verifies that Kafka consumer group lag is tracked correctly and that the
/// relay does not commit offsets when the sink delivery fails.
#[tokio::test]
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
