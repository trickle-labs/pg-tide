//! Authoritative public-API end-to-end test: native outbox -> real relay
//! compiled `pg-tide` process -> Apache Kafka KRaft broker and consumer.
//!
//! Run this ignored evidence test with a PostgreSQL 18 server that has the
//! pg_tide extension installed:
//!
//! ```text
//! PG_TIDE_E2E_DATABASE_URL=postgres://... cargo test --test public_api_outbox_to_kafka_e2e \
//!   --no-default-features --features core-kafka -- --ignored --nocapture
//! ```

#![cfg(feature = "kafka")]

use std::time::Duration;

mod common;

use common::process::RelayProcess;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::ClientConfig;
use tokio_postgres::NoTls;

const E2E_ENV: &str = "PG_TIDE_E2E_DATABASE_URL";
const TOPIC: &str = "orders-kafka";

async fn connect(url: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .expect("connect PostgreSQL");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn wait_for_offset(client: &tokio_postgres::Client) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let row = client
            .query_opt(
                "SELECT last_change_id FROM tide.relay_consumer_offsets \
                 WHERE relay_group_id = 'e2e-kafka' AND pipeline_id = 'orders-kafka' \
                   AND outbox_name = 'orders'",
                &[],
            )
            .await
            .expect("query Kafka source offset");
        if row.is_some() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Kafka coordinator did not commit a source offset"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires an installed pg_tide extension and PG_TIDE_E2E_DATABASE_URL"]
async fn public_api_outbox_to_kafka_e2e() {
    let database_url = std::env::var(E2E_ENV).expect("PG_TIDE_E2E_DATABASE_URL must be set");

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
        kafka.get_host_port_ipv4(9092).await.expect("Kafka port")
    );

    let admin: rdkafka::admin::AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .expect("create Kafka admin client");
    admin
        .create_topics(
            &[rdkafka::admin::NewTopic::new(
                TOPIC,
                1,
                rdkafka::admin::TopicReplication::Fixed(1),
            )],
            &rdkafka::admin::AdminOptions::new(),
        )
        .await
        .expect("create Kafka topic")
        .into_iter()
        .next()
        .expect("Kafka topic creation result")
        .expect("Kafka topic creation");

    let client = connect(&database_url).await;
    client
        .batch_execute("DROP EXTENSION IF EXISTS pg_tide CASCADE; CREATE EXTENSION pg_tide;")
        .await
        .expect("install pg_tide extension");
    client
        .execute(
            "SELECT tide.outbox_create_if_not_exists('orders', 24, 10000, 'none')",
            &[],
        )
        .await
        .expect("create outbox");
    let pipeline = serde_json::json!({
        "name": "orders-kafka",
        "outbox": "orders",
        "sink_type": "kafka",
        "config": {"brokers": brokers, "topic": TOPIC},
        "batch_size": 50
    });
    client
        .execute("SELECT tide.relay_set_outbox_v2($1::jsonb)", &[&pipeline])
        .await
        .expect("configure Kafka pipeline");

    let relay = RelayProcess::start(&database_url, "e2e-kafka");

    let writer = connect(&database_url).await;
    writer
        .execute(
            "SELECT tide.outbox_publish('orders', $1::jsonb, $2::jsonb)",
            &[
                &serde_json::json!({"order_id": "K-1"}),
                &serde_json::json!({"event_type": "order.created"}),
            ],
        )
        .await
        .expect("publish outbox event");

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "pg-tide-kafka-e2e")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("create Kafka consumer");
    consumer
        .subscribe(&[TOPIC])
        .expect("subscribe Kafka consumer");
    let message = tokio::time::timeout(Duration::from_secs(20), consumer.recv())
        .await
        .expect("timed out waiting for coordinator delivery")
        .expect("Kafka consumer error");
    let payload = message
        .payload_view::<str>()
        .expect("Kafka payload")
        .expect("UTF-8 Kafka payload");
    assert!(
        payload.contains("K-1"),
        "Kafka payload must contain the event"
    );
    wait_for_offset(&client).await;

    relay.stop().await;
}
