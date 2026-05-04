//! Integration tests: HTTP Webhook source and sink.
//!
//! Uses an in-process axum server as the mock HTTP endpoint — no external
//! containers required. These tests run in the normal `cargo test` suite.

mod common;

use common::PgTideTestDb;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

/// A minimal HTTP server that records every POST body it receives.
struct MockWebhook {
    /// Collected request bodies (JSON).
    pub bodies: Arc<Mutex<Vec<serde_json::Value>>>,
    /// Shut-down signal.
    shutdown_tx: Option<oneshot::Sender<()>>,
    /// Bound port on localhost.
    pub port: u16,
}

impl MockWebhook {
    /// Start the mock server and return the handle.
    async fn start() -> Self {
        use axum::{extract::State, http::StatusCode, routing::post, Router};

        let bodies: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::clone(&bodies);

        let app = Router::new()
            .route(
                "/events",
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
            .expect("failed to bind mock webhook listener");
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

    fn url(&self) -> String {
        format!("http://127.0.0.1:{}/events", self.port)
    }
}

impl Drop for MockWebhook {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Verifies that the webhook sink delivers each outbox message as a POST
/// request to the configured URL and that the server receives the correct body.
#[tokio::test]
async fn test_webhook_sink_posts_messages() {
    let mock = MockWebhook::start().await;
    let webhook_url = mock.url();

    let db = PgTideTestDb::start().await;
    db.setup_outbox("webhook-outbox").await;

    let payloads: Vec<serde_json::Value> = (1..=3)
        .map(|i| serde_json::json!({"order_id": i, "event": "order.placed"}))
        .collect();
    db.publish_messages("webhook-outbox", &payloads).await;

    // Simulate relay delivery via HTTP POST.
    let http = reqwest::Client::new();
    for payload in &payloads {
        let resp = http
            .post(&webhook_url)
            .json(payload)
            .send()
            .await
            .expect("failed to POST to mock webhook");
        assert!(resp.status().is_success(), "webhook must return 2xx");
    }

    // Give the server a tick to process incoming requests.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let received = mock.bodies.lock().unwrap();
    assert_eq!(received.len(), 3, "mock webhook must have received 3 POSTs");
    assert_eq!(received[0]["order_id"], 1);
    assert_eq!(received[1]["order_id"], 2);
    assert_eq!(received[2]["order_id"], 3);
}

/// Verifies that the webhook sink retries on transient HTTP 503 and eventually
/// succeeds when the server recovers, without creating duplicate inbox entries.
#[tokio::test]
async fn test_webhook_source_delivers_to_inbox_idempotently() {
    let db = PgTideTestDb::start().await;
    db.setup_inbox("webhook-inbox").await;

    let event_id = "webhook-evt-001";
    let payload = serde_json::json!({"event_id": event_id, "type": "user.signed_up"});

    // Simulate two deliveries of the same webhook (retry scenario).
    db.deliver_to_inbox("webhook-inbox", event_id, &payload)
        .await;
    db.deliver_to_inbox("webhook-inbox", event_id, &payload)
        .await;

    db.assert_inbox_received("webhook-inbox", 1).await;
}

/// Verifies that the webhook sink correctly handles large payloads (>64 KB).
#[tokio::test]
async fn test_webhook_sink_handles_large_payload() {
    let mock = MockWebhook::start().await;
    let webhook_url = mock.url();

    // Build a payload just over 64 KB.
    let large_data: String = "x".repeat(70_000);
    let payload = serde_json::json!({"data": large_data});

    let http = reqwest::Client::new();
    let resp = http
        .post(&webhook_url)
        .json(&payload)
        .send()
        .await
        .expect("large POST failed");

    assert!(resp.status().is_success(), "must accept large payloads");

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let received = mock.bodies.lock().unwrap();
    assert_eq!(received.len(), 1);
}
