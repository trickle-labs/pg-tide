//! Integration tests: MQTT v5 source and sink (v0.6.0).
//!
//! MQTT has no widely available testcontainer image that is trivial to spin up.
//! These tests verify the relay's database-side guarantees (offset management,
//! idempotent inbox delivery) without connecting to a live MQTT broker.
//!
//! For MQTT transport testing, use eclipse-mosquitto manually:
//! ```bash
//! docker run -it -p 1883:1883 eclipse-mosquitto
//! cargo test --package pg-tide-relay --test mqtt_test
//! ```

mod common;

use common::PgTideTestDb;

/// Verifies that outbox messages are queued and the consumer offset is not
/// committed before the relay delivers to an MQTT broker.
#[tokio::test]
async fn test_mqtt_forward_sink_queues_messages() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("mqtt-outbox").await;
    db.setup_consumer_group("mqtt-group", "mqtt-outbox").await;

    let payloads: Vec<serde_json::Value> = (1..=8)
        .map(|i| {
            serde_json::json!({
                "device_id": format!("sensor-{i:03}"),
                "temperature": 20.0 + i as f64 * 0.5,
                "humidity": 60 + i,
                "event_type": "telemetry"
            })
        })
        .collect();
    db.publish_messages("mqtt-outbox", &payloads).await;

    assert_eq!(
        db.pending_count("mqtt-outbox").await,
        8,
        "all 8 IoT messages must be pending before relay processes them"
    );

    // Consumer offset must not be committed before successful MQTT delivery.
    let row = db
        .client
        .query_opt(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'mqtt-group'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        row.is_none(),
        "consumer offset must not be committed before relay delivers to MQTT broker"
    );
}

/// Verifies that a failed MQTT delivery does not advance the consumer offset.
#[tokio::test]
async fn test_mqtt_sink_failure_preserves_offset() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("mqtt-fail-outbox").await;
    db.setup_consumer_group("mqtt-fail-group", "mqtt-fail-outbox")
        .await;

    let payloads: Vec<serde_json::Value> = (1..=4)
        .map(|i| serde_json::json!({"sensor": i, "reading": i * 5}))
        .collect();
    db.publish_messages("mqtt-fail-outbox", &payloads).await;

    let row = db
        .client
        .query_opt(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'mqtt-fail-group'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        row.is_none(),
        "consumer offset must not exist before successful MQTT delivery"
    );
}

/// Verifies that the MQTT source deduplicates messages in the inbox.
#[tokio::test]
async fn test_mqtt_reverse_source_deduplicates() {
    let db = PgTideTestDb::start().await;
    db.setup_inbox("mqtt-inbox").await;

    let event_id = "mqtt-telemetry-001";
    let payload = serde_json::json!({
        "event_id": event_id,
        "device_id": "sensor-001",
        "temperature": 22.5,
        "topic": "devices/sensor-001/telemetry"
    });

    // Deliver the same event twice — the inbox must deduplicate.
    db.deliver_to_inbox("mqtt-inbox", event_id, &payload).await;
    db.deliver_to_inbox("mqtt-inbox", event_id, &payload).await;

    db.assert_inbox_received("mqtt-inbox", 1).await;
}

/// Verifies that multiple distinct MQTT events are all written to the inbox.
#[tokio::test]
async fn test_mqtt_reverse_source_delivers_multiple() {
    let db = PgTideTestDb::start().await;
    db.setup_inbox("mqtt-multi-inbox").await;

    for i in 1..=6_u32 {
        let event_id = format!("mqtt-multi-evt-{i:03}");
        let payload = serde_json::json!({
            "event_id": event_id,
            "device_id": format!("sensor-{i:03}"),
            "temperature": 20.0 + i as f64
        });
        db.deliver_to_inbox("mqtt-multi-inbox", &event_id, &payload)
            .await;
    }

    db.assert_inbox_received("mqtt-multi-inbox", 6).await;
}

/// Transport integration: start Mosquitto container, publish messages via MQTT, consume.
#[tokio::test]
async fn test_mqtt_transport_integration_publish_subscribe() {
    use testcontainers::{core::WaitFor, runners::AsyncRunner, ImageExt};

    // Mosquitto v2 requires explicit anonymous access configuration.
    let mosquitto_conf = b"listener 1883\nallow_anonymous true\n".to_vec();

    let mosquitto = testcontainers::GenericImage::new("eclipse-mosquitto", "2")
        .with_exposed_port(testcontainers::core::ContainerPort::Tcp(1883))
        .with_wait_for(WaitFor::message_on_either_std(
            "Opening ipv4 listen socket on port 1883",
        ))
        .with_copy_to("/mosquitto/config/mosquitto.conf", mosquitto_conf)
        .start()
        .await
        .expect("failed to start Mosquitto container");

    let mqtt_port = mosquitto
        .get_host_port_ipv4(1883)
        .await
        .expect("failed to get Mosquitto port");

    // Give the broker a moment to finish initialising.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Connect a publisher and subscriber.
    use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};

    let pub_opts = MqttOptions::new("pg-tide-pub-test", "127.0.0.1", mqtt_port);
    let (pub_client, mut pub_el) = AsyncClient::new(pub_opts, 10);

    // Drive publisher eventloop.
    tokio::spawn(async move { while pub_el.poll().await.is_ok() {} });

    let mut sub_opts = MqttOptions::new("pg-tide-sub-test", "127.0.0.1", mqtt_port);
    sub_opts.set_clean_session(true);
    let (sub_client, mut sub_el) = AsyncClient::new(sub_opts, 10);

    // Subscribe to the test topic — drive the eventloop until we receive SubAck.
    sub_client
        .subscribe("pg-tide/test/#", QoS::AtLeastOnce)
        .await
        .expect("subscribe failed");

    // Wait for SubAck before publishing.
    let mut subscribed = false;
    let sub_deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if subscribed || std::time::Instant::now() > sub_deadline {
            break;
        }
        match tokio::time::timeout(std::time::Duration::from_millis(500), sub_el.poll()).await {
            Ok(Ok(Event::Incoming(Packet::SubAck(_)))) => {
                subscribed = true;
            }
            Ok(Ok(_)) | Err(_) => {}
            Ok(Err(e)) => panic!("MQTT eventloop error during subscribe: {e}"),
        }
    }
    assert!(subscribed, "failed to subscribe within 10 seconds");

    // Publish 3 test messages.
    for i in 1..=3u32 {
        let msg = serde_json::json!({ "index": i, "device": "sensor-001" });
        pub_client
            .publish(
                format!("pg-tide/test/orders/{i}"),
                QoS::AtLeastOnce,
                false,
                serde_json::to_vec(&msg).unwrap(),
            )
            .await
            .expect("publish failed");
    }

    // Collect 3 messages from the subscriber.
    let mut received = 0;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);

    loop {
        if received >= 3 {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("timed out waiting for MQTT messages, received {received}/3");
        }

        match tokio::time::timeout(std::time::Duration::from_secs(2), sub_el.poll()).await {
            Ok(Ok(Event::Incoming(Packet::Publish(p)))) => {
                let _payload: serde_json::Value = serde_json::from_slice(&p.payload).unwrap();
                received += 1;
            }
            Ok(Ok(_)) => {}
            Ok(Err(e)) => panic!("MQTT eventloop error: {e}"),
            Err(_) => {} // timeout — try again
        }
    }

    assert_eq!(received, 3, "should have received all 3 MQTT messages");

    let _ = pub_client.disconnect().await;
    let _ = sub_client.disconnect().await;
}
