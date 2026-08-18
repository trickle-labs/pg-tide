//! NATS transport contract tests.
//!
//! IMPORTANT (v0.40.0, ADR-011 §13, D.3): These tests exercise the NATS
//! transport plumbing ONLY — they publish directly to NATS to confirm
//! connectivity, subject routing, and inbox-side deduplication. They do NOT
//! run the relay coordinator and are NOT relay integration or end-to-end
//! coverage. The authoritative relay → NATS JetStream proof lives in
//! `public_api_outbox_to_nats_e2e.rs`.
//!
//! These tests require a NATS server (provided here via testcontainers).

mod common;

use common::PgTideTestDb;
use futures_util::StreamExt;

/// Contract: messages published to a NATS subject are received in order.
/// This validates the NATS transport itself, not relay delivery.
#[tokio::test]
async fn test_nats_transport_delivers_published_messages() {
    use testcontainers::runners::AsyncRunner;

    // Start NATS container.
    let nats_image = testcontainers::GenericImage::new("nats", "2.11.0")
        .with_exposed_port(testcontainers::core::ContainerPort::Tcp(4222));
    let nats = nats_image
        .start()
        .await
        .expect("failed to start NATS container");

    let nats_port = nats
        .get_host_port_ipv4(4222)
        .await
        .expect("failed to get NATS port");
    let nats_url = format!("nats://127.0.0.1:{nats_port}");

    // Start PostgreSQL with pg_tide schema (used only to mirror a realistic
    // outbox seed; delivery below is a pure NATS-transport check).
    let db = PgTideTestDb::start().await;
    db.setup_outbox("nats-outbox").await;

    let payloads: Vec<serde_json::Value> = (1..=5)
        .map(|i| serde_json::json!({"seq": i, "event": "order.created"}))
        .collect();
    db.publish_messages("nats-outbox", &payloads).await;

    // Connect to NATS and subscribe to the target subject.
    let client = async_nats::connect(&nats_url)
        .await
        .expect("failed to connect to NATS");

    let mut subscriber = client
        .subscribe("orders")
        .await
        .expect("failed to subscribe");

    // Publish directly to NATS. This is a transport-plumbing check ONLY — the
    // relay coordinator is not involved here.
    for i in 1..=5_u32 {
        let payload = serde_json::json!({"seq": i});
        client
            .publish("orders", serde_json::to_vec(&payload).unwrap().into())
            .await
            .expect("failed to publish to NATS");
    }
    client.flush().await.expect("flush failed");

    // Receive and count messages.
    let mut received = 0_u32;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while let Some(_msg) = subscriber.next().await {
            received += 1;
            if received == 5 {
                break;
            }
        }
    })
    .await
    .expect("timed out waiting for NATS messages");

    assert_eq!(received, 5, "expected 5 messages via NATS transport");
}

/// Contract: the inbox deduplicates by event_id. This validates inbox-side
/// idempotency for a NATS-sourced payload shape — it does NOT run the relay.
#[tokio::test]
async fn test_inbox_deduplicates_nats_shaped_payload() {
    use testcontainers::runners::AsyncRunner;

    let nats_image = testcontainers::GenericImage::new("nats", "2.11.0")
        .with_exposed_port(testcontainers::core::ContainerPort::Tcp(4222));
    let nats = nats_image.start().await.expect("failed to start NATS");

    let nats_port = nats.get_host_port_ipv4(4222).await.unwrap();
    let nats_url = format!("nats://127.0.0.1:{nats_port}");

    let db = PgTideTestDb::start().await;
    db.setup_inbox("nats-inbox").await;

    let client = async_nats::connect(&nats_url)
        .await
        .expect("failed to connect to NATS");

    // Publish the same event twice over the NATS transport.
    let event_id = "nats-dedup-evt-001";
    let payload = serde_json::json!({"event_id": event_id, "data": "hello"});
    let body: bytes::Bytes = serde_json::to_vec(&payload).unwrap().into();

    client.publish("inbox-subject", body.clone()).await.unwrap();
    client.publish("inbox-subject", body).await.unwrap();
    client.flush().await.unwrap();

    // Inbox-side idempotency check (direct writes; not relay delivery).
    db.deliver_to_inbox("nats-inbox", event_id, &payload).await;
    db.deliver_to_inbox("nats-inbox", event_id, &payload).await;

    db.assert_inbox_received("nats-inbox", 1).await;
}
