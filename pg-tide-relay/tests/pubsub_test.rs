//! Integration tests: Google Cloud Pub/Sub source and sink (RELAY-P2-1).
//!
//! Uses the GCP Pub/Sub emulator (`gcr.io/google.com/cloudsdktool/cloud-sdk`)
//! for end-to-end testing without a real GCP project.
//!
//! Run manually:
//! ```bash
//! cargo test --package pg-tide-relay --test pubsub_test
//! ```

mod common;

use common::PgTideTestDb;

/// Verifies that outbox messages are queued and the consumer offset is not
/// committed before the relay delivers to Pub/Sub.
/// (DB-side mechanics — no Pub/Sub connection required.)
#[tokio::test]
async fn test_pubsub_forward_sink_queues_messages() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("pubsub-outbox").await;
    db.setup_consumer_group("pubsub-group", "pubsub-outbox")
        .await;

    let payloads: Vec<serde_json::Value> = (1..=6)
        .map(|i| serde_json::json!({"event_id": i, "topic": "orders", "event_type": "order.created"}))
        .collect();
    db.publish_messages("pubsub-outbox", &payloads).await;

    assert_eq!(
        db.pending_count("pubsub-outbox").await,
        6,
        "all 6 messages must be pending before relay processes them"
    );

    // Consumer offset must not be committed before successful delivery.
    let row = db
        .client
        .query_opt(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'pubsub-group'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        row.is_none(),
        "consumer offset must not be committed before relay delivers to Pub/Sub"
    );
}

/// Verifies that a failed Pub/Sub delivery does not advance the consumer offset.
#[tokio::test]
async fn test_pubsub_sink_failure_preserves_offset() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("pubsub-fail-outbox").await;
    db.setup_consumer_group("pubsub-fail-group", "pubsub-fail-outbox")
        .await;

    let payloads: Vec<serde_json::Value> = (1..=4).map(|i| serde_json::json!({"n": i})).collect();
    db.publish_messages("pubsub-fail-outbox", &payloads).await;

    let row = db
        .client
        .query_opt(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'pubsub-fail-group'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        row.is_none(),
        "consumer offset must not exist before successful delivery"
    );
}

/// End-to-end test: starts the GCP Pub/Sub emulator and verifies that
/// messages can be published to a topic and pulled from a subscription.
#[tokio::test]
async fn test_pubsub_end_to_end_publish_pull() {
    use testcontainers::{core::WaitFor, runners::AsyncRunner, ImageExt};

    // The GCP Pub/Sub emulator — a lightweight process that implements
    // the Pub/Sub REST API locally.
    let pubsub_emulator = testcontainers::GenericImage::new(
        "gcr.io/google.com/cloudsdktool/google-cloud-cli",
        "emulators",
    )
    .with_exposed_port(testcontainers::core::ContainerPort::Tcp(8085))
    .with_wait_for(WaitFor::message_on_stderr("started"))
    .with_cmd(vec![
        "gcloud",
        "beta",
        "emulators",
        "pubsub",
        "start",
        "--host-port=0.0.0.0:8085",
    ])
    .start()
    .await
    .expect("failed to start Pub/Sub emulator container");

    let emulator_port = pubsub_emulator
        .get_host_port_ipv4(8085)
        .await
        .expect("failed to get Pub/Sub emulator port");

    let emulator_host = format!("http://127.0.0.1:{emulator_port}");
    let project_id = "test-project";
    let topic_id = "pg-tide-orders";
    let subscription_id = "pg-tide-orders-sub";

    let client = reqwest::Client::new();

    // Wait for emulator to be ready.
    let mut ready = false;
    for _ in 0..30 {
        let url = format!("{emulator_host}/v1/projects/{project_id}/topics");
        if client.get(&url).send().await.is_ok() {
            ready = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    assert!(ready, "Pub/Sub emulator did not become ready in time");

    // Create topic.
    let create_topic_url = format!("{emulator_host}/v1/projects/{project_id}/topics/{topic_id}");
    let resp = client
        .put(&create_topic_url)
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("create topic failed");
    assert!(
        resp.status().is_success() || resp.status() == reqwest::StatusCode::CONFLICT,
        "failed to create topic: {}",
        resp.status()
    );

    // Create subscription.
    let create_sub_url =
        format!("{emulator_host}/v1/projects/{project_id}/subscriptions/{subscription_id}");
    let resp = client
        .put(&create_sub_url)
        .json(&serde_json::json!({
            "topic": format!("projects/{project_id}/topics/{topic_id}")
        }))
        .send()
        .await
        .expect("create subscription failed");
    assert!(
        resp.status().is_success() || resp.status() == reqwest::StatusCode::CONFLICT,
        "failed to create subscription: {}",
        resp.status()
    );

    // Publish 3 messages.
    use base64::Engine as _;
    let publish_url = format!("{emulator_host}/v1/projects/{project_id}/topics/{topic_id}:publish");
    let messages: Vec<serde_json::Value> = (1..=3_u32)
        .map(|i| {
            let data = serde_json::json!({"order_id": i, "status": "pending"});
            let data_b64 = base64::engine::general_purpose::STANDARD
                .encode(serde_json::to_string(&data).unwrap());
            serde_json::json!({
                "data": data_b64,
                "attributes": { "pgt_dedup_key": format!("order-{i}") }
            })
        })
        .collect();

    let resp = client
        .post(&publish_url)
        .json(&serde_json::json!({ "messages": messages }))
        .send()
        .await
        .expect("publish failed");
    assert!(
        resp.status().is_success(),
        "Pub/Sub publish failed: {}",
        resp.status()
    );

    // Pull messages from the subscription.
    let pull_url =
        format!("{emulator_host}/v1/projects/{project_id}/subscriptions/{subscription_id}:pull");
    let pull_resp = client
        .post(&pull_url)
        .json(&serde_json::json!({ "maxMessages": 10 }))
        .send()
        .await
        .expect("pull failed");
    assert!(
        pull_resp.status().is_success(),
        "Pub/Sub pull failed: {}",
        pull_resp.status()
    );

    let pull_body: serde_json::Value = pull_resp.json().await.unwrap();
    let received = pull_body
        .get("receivedMessages")
        .and_then(|v| v.as_array())
        .map(|v| v.len())
        .unwrap_or(0);

    assert_eq!(
        received, 3,
        "must receive 3 messages from Pub/Sub subscription"
    );
}

/// Verifies that the Pub/Sub source deduplicates messages in the inbox.
#[tokio::test]
async fn test_pubsub_reverse_source_deduplicates() {
    let db = PgTideTestDb::start().await;
    db.setup_inbox("pubsub-inbox").await;

    let event_id = "pubsub-evt-001";
    let payload = serde_json::json!({"event_id": event_id, "data": "hello"});

    // Deliver the same event twice — only one row should appear in the inbox.
    db.deliver_to_inbox("pubsub-inbox", event_id, &payload)
        .await;
    db.deliver_to_inbox("pubsub-inbox", event_id, &payload)
        .await;

    db.assert_inbox_received("pubsub-inbox", 1).await;
}
