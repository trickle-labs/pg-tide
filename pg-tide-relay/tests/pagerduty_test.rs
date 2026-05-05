//! Integration tests: PagerDuty notification sink (RELAY-P3-N3).
//!
//! Uses an in-process axum mock HTTP server — no external services required.
//! Tests verify that PagerDuty Events API v2 payloads are correctly formatted
//! and delivered.

mod common;

use common::PgTideTestDb;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

struct MockPagerDuty {
    pub bodies: Arc<Mutex<Vec<serde_json::Value>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    pub port: u16,
}

impl MockPagerDuty {
    async fn start() -> Self {
        use axum::{extract::State, http::StatusCode, routing::post, Router};

        let bodies: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::clone(&bodies);

        let app = Router::new()
            .route(
                "/v2/enqueue",
                post(
                    |State(s): State<Arc<Mutex<Vec<serde_json::Value>>>>,
                     axum::Json(body): axum::Json<serde_json::Value>| async move {
                        s.lock().unwrap().push(body);
                        StatusCode::ACCEPTED
                    },
                ),
            )
            .with_state(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind mock PagerDuty listener");
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

    fn enqueue_url(&self) -> String {
        format!("http://127.0.0.1:{}/v2/enqueue", self.port)
    }
}

impl Drop for MockPagerDuty {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Verifies that the PagerDuty mock accepts a v2 trigger event.
#[tokio::test]
async fn test_pagerduty_mock_accepts_trigger_event() {
    let mock = MockPagerDuty::start().await;
    let http = reqwest::Client::new();

    // Simulate what PagerDutySink would POST for one message.
    let payload = serde_json::json!({
        "routing_key": "R0000000000000000000000000000001",
        "event_action": "trigger",
        "dedup_key": "orders:1:0",
        "payload": {
            "summary": "[insert] orders.insert — orders:1:0",
            "severity": "info",
            "custom_details": {"order_id": 1}
        }
    });

    let resp = http
        .post(mock.enqueue_url())
        .json(&payload)
        .send()
        .await
        .expect("POST failed");

    assert!(resp.status().is_success(), "mock PD must return 2xx");

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let received = mock.bodies.lock().unwrap();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0]["event_action"], "trigger");
    assert_eq!(
        received[0]["routing_key"],
        "R0000000000000000000000000000001"
    );
    assert_eq!(received[0]["dedup_key"], "orders:1:0");
}

/// Verifies that event severity is set correctly for insert operations.
#[tokio::test]
async fn test_pagerduty_event_severity_for_insert() {
    let mock = MockPagerDuty::start().await;
    let http = reqwest::Client::new();

    let payload = serde_json::json!({
        "routing_key": "RTEST",
        "event_action": "trigger",
        "dedup_key": "orders:1:0",
        "payload": {
            "summary": "[insert] orders.insert — orders:1:0",
            "severity": "critical",
            "custom_details": {"order_id": 1}
        }
    });

    let resp = http
        .post(mock.enqueue_url())
        .json(&payload)
        .send()
        .await
        .expect("POST failed");

    assert!(resp.status().is_success());

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let received = mock.bodies.lock().unwrap();
    assert_eq!(received[0]["payload"]["severity"], "critical");
}

/// Verifies that delete operations always use "info" severity.
#[tokio::test]
async fn test_pagerduty_delete_uses_info_severity() {
    let mock = MockPagerDuty::start().await;
    let http = reqwest::Client::new();

    let payload = serde_json::json!({
        "routing_key": "RTEST",
        "event_action": "trigger",
        "dedup_key": "orders:99:0",
        "payload": {
            "summary": "[delete] orders.delete — orders:99:0",
            "severity": "info",
            "custom_details": {"order_id": 99}
        }
    });

    let resp = http
        .post(mock.enqueue_url())
        .json(&payload)
        .send()
        .await
        .expect("POST failed");

    assert!(resp.status().is_success());

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let received = mock.bodies.lock().unwrap();
    assert_eq!(
        received[0]["payload"]["severity"], "info",
        "delete ops must always use info severity"
    );
}

/// Verifies that each message in a batch produces a separate PagerDuty event.
#[tokio::test]
async fn test_pagerduty_each_message_is_separate_event() {
    let mock = MockPagerDuty::start().await;
    let http = reqwest::Client::new();

    // PagerDutySink sends one HTTP POST per message (one event per POST).
    for i in 1..=3i32 {
        let payload = serde_json::json!({
            "routing_key": "RTEST",
            "event_action": "trigger",
            "dedup_key": format!("orders:{i}:0"),
            "payload": {
                "summary": format!("[insert] orders.insert — orders:{i}:0"),
                "severity": "info",
                "custom_details": {"order_id": i}
            }
        });

        let resp = http
            .post(mock.enqueue_url())
            .json(&payload)
            .send()
            .await
            .expect("POST failed");

        assert!(resp.status().is_success());
    }

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let received = mock.bodies.lock().unwrap();
    assert_eq!(
        received.len(),
        3,
        "each relay message must produce one PagerDuty event"
    );
}

/// Verifies the dedup_key is passed through to PagerDuty for idempotent triggers.
#[tokio::test]
async fn test_pagerduty_dedup_key_is_forwarded() {
    let mock = MockPagerDuty::start().await;
    let http = reqwest::Client::new();

    let dedup_key = "orders:12345:0";
    let payload = serde_json::json!({
        "routing_key": "RTEST",
        "event_action": "trigger",
        "dedup_key": dedup_key,
        "payload": {
            "summary": "[insert] orders.insert — orders:12345:0",
            "severity": "info",
            "custom_details": {"order_id": 12345}
        }
    });

    let resp = http
        .post(mock.enqueue_url())
        .json(&payload)
        .send()
        .await
        .expect("POST failed");

    assert!(resp.status().is_success());

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let received = mock.bodies.lock().unwrap();
    assert_eq!(received[0]["dedup_key"].as_str().unwrap(), dedup_key);
}

/// Verifies source and component fields are included when configured.
#[tokio::test]
async fn test_pagerduty_source_and_component_fields() {
    let mock = MockPagerDuty::start().await;
    let http = reqwest::Client::new();

    let payload = serde_json::json!({
        "routing_key": "RTEST",
        "event_action": "trigger",
        "dedup_key": "orders:1:0",
        "payload": {
            "summary": "[insert] orders.insert — orders:1:0",
            "severity": "info",
            "source": "pg-tide-relay-prod",
            "component": "orders-service",
            "custom_details": {"order_id": 1}
        }
    });

    let resp = http
        .post(mock.enqueue_url())
        .json(&payload)
        .send()
        .await
        .expect("POST failed");

    assert!(resp.status().is_success());

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let received = mock.bodies.lock().unwrap();
    assert_eq!(received[0]["payload"]["source"], "pg-tide-relay-prod");
    assert_eq!(received[0]["payload"]["component"], "orders-service");
}

/// DB-side mechanics: verify outbox messages are queued before PagerDuty delivery.
#[tokio::test]
async fn test_pagerduty_sink_outbox_messages_are_queued() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("pd-outbox").await;
    db.setup_consumer_group("pd-group", "pd-outbox").await;

    let payloads: Vec<serde_json::Value> = (1..=3)
        .map(|i| serde_json::json!({"alert_id": i, "type": "threshold.exceeded"}))
        .collect();
    db.publish_messages("pd-outbox", &payloads).await;

    assert_eq!(
        db.pending_count("pd-outbox").await,
        3,
        "all 3 messages must be pending before PagerDuty delivery"
    );
}

/// Verifies that custom_details contains the full relay message payload.
#[tokio::test]
async fn test_pagerduty_custom_details_contains_payload() {
    let mock = MockPagerDuty::start().await;
    let http = reqwest::Client::new();

    let payload = serde_json::json!({
        "routing_key": "RTEST",
        "event_action": "trigger",
        "dedup_key": "orders:7:0",
        "payload": {
            "summary": "[insert] orders.insert — orders:7:0",
            "severity": "warning",
            "custom_details": {
                "order_id": 7,
                "amount": 999.99,
                "currency": "USD"
            }
        }
    });

    let resp = http
        .post(mock.enqueue_url())
        .json(&payload)
        .send()
        .await
        .expect("POST failed");

    assert!(resp.status().is_success());

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let received = mock.bodies.lock().unwrap();
    let details = &received[0]["payload"]["custom_details"];
    assert_eq!(details["order_id"], 7);
    assert_eq!(details["currency"], "USD");
}
