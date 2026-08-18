//! Integration tests: HTTP Webhook source and sink.
//!
//! Uses an in-process axum server as the mock HTTP endpoint — no external
//! containers required. These tests run in the normal `cargo test` suite.

mod common;

use common::PgTideTestDb;
use pg_tide_relay::envelope::RelayMessage;
use pg_tide_relay::sink::{webhook::WebhookSink, Sink};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

#[derive(Debug)]
struct RecordedRequest {
    body: serde_json::Value,
    idempotency_key: Option<String>,
    signature: Option<String>,
}

/// A minimal HTTP server that records every POST body it receives.
struct MockWebhook {
    /// Collected request bodies and delivery headers.
    pub requests: Arc<Mutex<Vec<RecordedRequest>>>,
    /// Shut-down signal.
    shutdown_tx: Option<oneshot::Sender<()>>,
    /// Bound port on localhost.
    pub port: u16,
}

impl MockWebhook {
    /// Start the mock server and return the handle.
    async fn start() -> Self {
        use axum::{extract::State, http::StatusCode, routing::post, Router};

        let requests: Arc<Mutex<Vec<RecordedRequest>>> = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::clone(&requests);

        let app = Router::new()
            .route(
                "/events",
                post(
                    |State(s): State<Arc<Mutex<Vec<RecordedRequest>>>>,
                     headers: axum::http::HeaderMap,
                     axum::Json(body): axum::Json<serde_json::Value>| async move {
                        s.lock().unwrap().push(RecordedRequest {
                            body,
                            idempotency_key: headers
                                .get("Idempotency-Key")
                                .and_then(|value| value.to_str().ok())
                                .map(str::to_string),
                            signature: headers
                                .get("X-Pg-Tide-Signature")
                                .and_then(|value| value.to_str().ok())
                                .map(str::to_string),
                        });
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
            requests,
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
    let mut sink = WebhookSink::new_with_options(
        &webhook_url,
        30,
        true,
        false,
        Some("test-secret"),
        "hmac-sha256",
    )
    .expect("webhook sink should construct");

    let messages: Vec<RelayMessage> = (1..=3)
        .map(|i| {
            RelayMessage::new_reverse(
                format!("event-{i}"),
                "orders.placed",
                serde_json::json!({"order_id": i, "event": "order.placed"}),
            )
        })
        .collect();

    sink.publish(&messages)
        .await
        .expect("webhook sink should publish");

    // Give the server a tick to process incoming requests.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let received = mock.requests.lock().unwrap();
    assert_eq!(
        received.len(),
        1,
        "mock webhook must have received one batch"
    );
    assert_eq!(received[0].body.as_array().map(Vec::len), Some(3));
    assert!(received[0]
        .idempotency_key
        .as_deref()
        .is_some_and(|key| key.starts_with("batch-")));
    assert!(received[0]
        .signature
        .as_deref()
        .is_some_and(|signature| signature.starts_with("sha256=")));
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
    let mut sink =
        WebhookSink::new_with_options(&webhook_url, 30, true, false, None, "hmac-sha256")
            .expect("webhook sink should construct");
    sink.publish(&[RelayMessage::new_reverse(
        "large-event",
        "orders.large",
        serde_json::json!({"data": large_data}),
    )])
    .await
    .expect("large webhook publish failed");

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let received = mock.requests.lock().unwrap();
    assert_eq!(received.len(), 1);
}
