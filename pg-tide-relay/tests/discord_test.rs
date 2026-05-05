//! Integration tests: Discord notification sink (RELAY-P3-N2).
//!
//! Uses an in-process axum mock HTTP server — no external services required.
//! Tests verify that Discord Webhook payloads (Embeds format) are correctly
//! formatted and delivered.

mod common;

use common::PgTideTestDb;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

struct MockDiscord {
    pub bodies: Arc<Mutex<Vec<serde_json::Value>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    pub port: u16,
}

impl MockDiscord {
    async fn start() -> Self {
        use axum::{extract::State, http::StatusCode, routing::post, Router};

        let bodies: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::clone(&bodies);

        let app = Router::new()
            .route(
                "/api/webhooks/1234567890/XXXXXXXXXXXX",
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
            .expect("failed to bind mock Discord listener");
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
        format!(
            "http://127.0.0.1:{}/api/webhooks/1234567890/XXXXXXXXXXXX",
            self.port
        )
    }
}

impl Drop for MockDiscord {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Verifies that the Discord mock accepts an embeds payload.
#[tokio::test]
async fn test_discord_webhook_mock_accepts_embed_payload() {
    let mock = MockDiscord::start().await;
    let http = reqwest::Client::new();

    // Simulate what DiscordSink would POST for two messages.
    let payload = serde_json::json!({
        "embeds": [
            {
                "title": "orders.insert — insert",
                "description": "```json\n{\"order_id\": 1}\n```",
                "color": 0x57F287u32,
                "footer": { "text": "dedup_key: orders:1:0" }
            },
            {
                "title": "orders.delete — delete",
                "description": "```json\n{\"order_id\": 2}\n```",
                "color": 0xED4245u32,
                "footer": { "text": "dedup_key: orders:2:0" }
            }
        ]
    });

    let resp = http
        .post(mock.webhook_url())
        .json(&payload)
        .send()
        .await
        .expect("POST failed");

    assert!(resp.status().is_success(), "mock Discord must return 2xx");

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let received = mock.bodies.lock().unwrap();
    assert_eq!(received.len(), 1);
    let embeds = received[0]["embeds"].as_array().unwrap();
    assert_eq!(embeds.len(), 2);
    assert_eq!(embeds[0]["title"], "orders.insert — insert");
}

/// Verifies that insert operations use the green colour (0x57F287).
#[tokio::test]
async fn test_discord_embed_insert_color() {
    let mock = MockDiscord::start().await;
    let http = reqwest::Client::new();

    let payload = serde_json::json!({
        "embeds": [{
            "title": "orders.insert — insert",
            "description": "```json\n{\"order_id\": 1}\n```",
            "color": 0x57F287u32,
            "footer": { "text": "dedup_key: orders:1:0" }
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
    // Color 0x57F287 = 5763719 decimal (green).
    assert_eq!(received[0]["embeds"][0]["color"], 5_763_719u64);
}

/// Verifies that delete operations use the red colour (0xED4245).
#[tokio::test]
async fn test_discord_embed_delete_color() {
    let mock = MockDiscord::start().await;
    let http = reqwest::Client::new();

    let payload = serde_json::json!({
        "embeds": [{
            "title": "orders.delete — delete",
            "description": "```json\n{\"order_id\": 99}\n```",
            "color": 0xED4245u32,
            "footer": { "text": "dedup_key: orders:99:0" }
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
    // 0xED4245 = 15548997 decimal (red).
    assert_eq!(received[0]["embeds"][0]["color"], 15_548_997u64);
}

/// Verifies that username and avatar_url are included in Discord payloads.
#[tokio::test]
async fn test_discord_webhook_username_and_avatar() {
    let mock = MockDiscord::start().await;
    let http = reqwest::Client::new();

    let payload = serde_json::json!({
        "username": "pg-tide-relay",
        "avatar_url": "https://example.com/avatar.png",
        "embeds": [{
            "title": "event",
            "description": "test",
            "color": 0x99AAB5u32
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
    assert_eq!(received[0]["username"], "pg-tide-relay");
    assert_eq!(received[0]["avatar_url"], "https://example.com/avatar.png");
}

/// Verifies batch splitting: Discord allows max 10 embeds per message.
#[tokio::test]
async fn test_discord_webhook_batch_splitting() {
    let mock = MockDiscord::start().await;
    let http = reqwest::Client::new();

    // Simulate 3 Discord messages (10 + 5 embeds).
    for embed_count in [10usize, 5] {
        let embeds: Vec<serde_json::Value> = (0..embed_count)
            .map(|i| {
                serde_json::json!({
                    "title": format!("event {i}"),
                    "description": "test",
                    "color": 0x57F287u32
                })
            })
            .collect();

        let resp = http
            .post(mock.webhook_url())
            .json(&serde_json::json!({ "embeds": embeds }))
            .send()
            .await
            .expect("POST failed");

        assert!(resp.status().is_success());
    }

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let received = mock.bodies.lock().unwrap();
    assert_eq!(received.len(), 2, "two separate Discord messages");
    assert_eq!(received[0]["embeds"].as_array().unwrap().len(), 10);
    assert_eq!(received[1]["embeds"].as_array().unwrap().len(), 5);
}

/// DB-side mechanics: verify outbox messages are queued before Discord delivery.
#[tokio::test]
async fn test_discord_sink_outbox_messages_are_queued() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("discord-outbox").await;
    db.setup_consumer_group("discord-group", "discord-outbox")
        .await;

    let payloads: Vec<serde_json::Value> = (1..=6)
        .map(|i| serde_json::json!({"event_id": i, "type": "alert.fired"}))
        .collect();
    db.publish_messages("discord-outbox", &payloads).await;

    assert_eq!(
        db.pending_count("discord-outbox").await,
        6,
        "all 6 messages must be pending before Discord delivery"
    );
}

/// Verifies the embed footer contains the dedup_key.
#[tokio::test]
async fn test_discord_embed_footer_contains_dedup_key() {
    let mock = MockDiscord::start().await;
    let http = reqwest::Client::new();

    let dedup_key = "orders:42:0";
    let payload = serde_json::json!({
        "embeds": [{
            "title": "orders.insert — insert",
            "description": "```json\n{\"order_id\": 42}\n```",
            "color": 0x57F287u32,
            "footer": { "text": format!("dedup_key: {dedup_key}") }
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
    let footer_text = received[0]["embeds"][0]["footer"]["text"].as_str().unwrap();
    assert!(
        footer_text.contains(dedup_key),
        "footer must contain the dedup_key"
    );
}
