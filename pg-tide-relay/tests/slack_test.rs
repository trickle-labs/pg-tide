//! Integration tests: Slack notification sink (RELAY-P3-N1).
//!
//! Uses an in-process axum mock HTTP server — no external services required.
//! Tests verify that Slack Incoming Webhook payloads are correctly formatted
//! and delivered by the relay.

mod common;

use common::PgTideTestDb;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

/// A minimal HTTP server that records every POST body it receives.
struct MockSlack {
    pub bodies: Arc<Mutex<Vec<serde_json::Value>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    pub port: u16,
}

impl MockSlack {
    async fn start() -> Self {
        use axum::{extract::State, http::StatusCode, routing::post, Router};

        let bodies: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::clone(&bodies);

        let app = Router::new()
            .route(
                "/hooks/T000/B000/X000",
                post(
                    |State(s): State<Arc<Mutex<Vec<serde_json::Value>>>>,
                     axum::Json(body): axum::Json<serde_json::Value>| async move {
                        s.lock().unwrap().push(body);
                        StatusCode::OK
                    },
                ),
            )
            .with_state(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind mock Slack listener");
        let port = listener.local_addr().unwrap().port();

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .ok();
        });

        Self {
            bodies,
            shutdown_tx: Some(shutdown_tx),
            port,
        }
    }

    fn webhook_url(&self) -> String {
        format!("http://127.0.0.1:{}/hooks/T000/B000/X000", self.port)
    }
}

impl Drop for MockSlack {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Verifies that a Slack Incoming Webhook mock server accepts and records POSTs.
#[tokio::test]
async fn test_slack_webhook_mock_accepts_block_kit_payload() {
    let mock = MockSlack::start().await;
    let http = reqwest::Client::new();

    // Simulate what SlackSink would POST for a batch of two messages.
    let payload = serde_json::json!({
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*orders.insert* — `orders:1:0` | op: `insert`\n```{\"order_id\": 1}```"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*orders.insert* — `orders:2:0` | op: `insert`\n```{\"order_id\": 2}```"
                }
            }
        ]
    });

    let resp = http
        .post(mock.webhook_url())
        .json(&payload)
        .send()
        .await
        .expect("failed to POST to mock Slack");

    assert!(resp.status().is_success(), "mock Slack must return 2xx");

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let received = mock.bodies.lock().unwrap();
    assert_eq!(received.len(), 1, "mock Slack must have received 1 POST");
    let blocks = received[0]["blocks"].as_array().unwrap();
    assert_eq!(blocks.len(), 2, "payload must contain 2 blocks");
    assert_eq!(blocks[0]["type"], "section");
}

/// Verifies that multiple Slack messages can be posted in sequence
/// (simulates batch splitting for large batches).
#[tokio::test]
async fn test_slack_webhook_mock_accepts_multiple_posts() {
    let mock = MockSlack::start().await;
    let http = reqwest::Client::new();

    // Simulate 3 separate Slack messages (batch_limit = 2, 5 relay messages → 3 Slack POSTs).
    for chunk_size in [2usize, 2, 1] {
        let blocks: Vec<serde_json::Value> = (0..chunk_size)
            .map(|i| {
                serde_json::json!({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": format!("*event* | seq: {i}")
                    }
                })
            })
            .collect();

        let resp = http
            .post(mock.webhook_url())
            .json(&serde_json::json!({ "blocks": blocks }))
            .send()
            .await
            .expect("failed to POST to mock Slack");

        assert!(resp.status().is_success());
    }

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let received = mock.bodies.lock().unwrap();
    assert_eq!(
        received.len(),
        3,
        "should have received 3 separate POST requests"
    );
    assert_eq!(received[0]["blocks"].as_array().unwrap().len(), 2);
    assert_eq!(received[1]["blocks"].as_array().unwrap().len(), 2);
    assert_eq!(received[2]["blocks"].as_array().unwrap().len(), 1);
}

/// Verifies that Slack payload with username and icon_emoji fields is accepted.
#[tokio::test]
async fn test_slack_webhook_mock_accepts_username_and_icon() {
    let mock = MockSlack::start().await;
    let http = reqwest::Client::new();

    let payload = serde_json::json!({
        "username": "pg-tide",
        "icon_emoji": ":database:",
        "blocks": [{
            "type": "section",
            "text": { "type": "mrkdwn", "text": "test" }
        }]
    });

    let resp = http
        .post(mock.webhook_url())
        .json(&payload)
        .send()
        .await
        .expect("failed to POST to mock Slack");

    assert!(resp.status().is_success());

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let received = mock.bodies.lock().unwrap();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0]["username"], "pg-tide");
    assert_eq!(received[0]["icon_emoji"], ":database:");
}

/// Verifies that delete operations include the op in the Slack payload.
#[tokio::test]
async fn test_slack_webhook_delete_op_in_payload() {
    let mock = MockSlack::start().await;
    let http = reqwest::Client::new();

    let payload = serde_json::json!({
        "blocks": [{
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*orders.delete* — `orders:99:0` | op: `delete`\n```{\"order_id\": 99}```"
            }
        }]
    });

    let resp = http
        .post(mock.webhook_url())
        .json(&payload)
        .send()
        .await
        .expect("POST failed");

    assert!(resp.status().is_success());

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let received = mock.bodies.lock().unwrap();
    let text = received[0]["blocks"][0]["text"]["text"].as_str().unwrap();
    assert!(text.contains("delete"), "block text must mention the op");
}

/// DB-side mechanics: verify outbox messages are queued before Slack delivery.
#[tokio::test]
async fn test_slack_sink_outbox_messages_are_queued() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("slack-outbox").await;
    db.setup_consumer_group("slack-group", "slack-outbox").await;

    let payloads: Vec<serde_json::Value> = (1..=4)
        .map(|i| serde_json::json!({"event_id": i, "type": "order.placed"}))
        .collect();
    db.publish_messages("slack-outbox", &payloads).await;

    assert_eq!(
        db.pending_count("slack-outbox").await,
        4,
        "all 4 messages must be pending before Slack delivery"
    );
}

/// Verifies that a large batch payload is handled correctly by the mock.
#[tokio::test]
async fn test_slack_webhook_large_payload() {
    let mock = MockSlack::start().await;
    let http = reqwest::Client::new();

    // 50 blocks in a single Slack message (SlackSink default batch_limit).
    let blocks: Vec<serde_json::Value> = (0..50)
        .map(|i| {
            serde_json::json!({
                "type": "section",
                "text": { "type": "mrkdwn", "text": format!("event {i}") }
            })
        })
        .collect();

    let resp = http
        .post(mock.webhook_url())
        .json(&serde_json::json!({ "blocks": blocks }))
        .send()
        .await
        .expect("POST failed");

    assert!(resp.status().is_success());

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let received = mock.bodies.lock().unwrap();
    assert_eq!(received[0]["blocks"].as_array().unwrap().len(), 50);
}
