//! Public API E2E: native outbox -> real `pg-tide` process -> HTTP webhook.

mod common;

use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use common::process::{wait_until, RelayProcess};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_postgres::NoTls;

const DATABASE_ENV: &str = "PG_TIDE_E2E_DATABASE_URL";
const OUTBOX: &str = "e2e_webhook_outbox";

async fn connect(url: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .expect("connect PostgreSQL");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires pg_tide extension, built pg-tide binary, and PG_TIDE_E2E_DATABASE_URL"]
async fn public_api_outbox_to_webhook_e2e() {
    let received = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
    let app = Router::new()
        .route(
            "/events",
            post(
                |State(received): State<Arc<Mutex<Vec<serde_json::Value>>>>,
                 Json(body): Json<serde_json::Value>| async move {
                    received.lock().expect("record webhook body").push(body);
                    StatusCode::OK
                },
            ),
        )
        .with_state(Arc::clone(&received));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind webhook listener");
    let port = listener.local_addr().expect("webhook address").port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve webhook");
    });

    let database_url = std::env::var(DATABASE_ENV).expect("PG_TIDE_E2E_DATABASE_URL must be set");
    let client = connect(&database_url).await;
    client
        .batch_execute("DROP EXTENSION IF EXISTS pg_tide CASCADE; CREATE EXTENSION pg_tide;")
        .await
        .expect("install pg_tide extension");
    client
        .execute(
            "SELECT tide.outbox_create_if_not_exists($1, 24, 10000, 'none')",
            &[&OUTBOX],
        )
        .await
        .expect("create outbox");
    let pipeline = serde_json::json!({
        "name": "e2e-webhook",
        "outbox": OUTBOX,
        "sink_type": "webhook",
        "config": {
            "url": format!("http://127.0.0.1:{port}/events"),
            "allow_http": true,
            "ssrf_protection": false
        },
        "batch_size": 10
    });
    client
        .execute("SELECT tide.relay_set_outbox_v2($1::jsonb)", &[&pipeline])
        .await
        .expect("configure webhook pipeline");
    client
        .execute(
            "SELECT tide.outbox_publish($1, $2::jsonb, $3::jsonb)",
            &[
                &OUTBOX,
                &serde_json::json!({"order_id": "WH-1"}),
                &serde_json::json!({"event_type": "order.created"}),
            ],
        )
        .await
        .expect("publish outbox event");

    let relay = RelayProcess::start(&database_url, "e2e-webhook");
    wait_until(Duration::from_secs(20), || {
        let received = Arc::clone(&received);
        async move { received.lock().expect("read webhook bodies").len() == 1 }
    })
    .await;
    let bodies = { received.lock().expect("read webhook body").clone() };
    assert_eq!(bodies.len(), 1);
    assert!(bodies[0].to_string().contains("WH-1"));
    relay.stop().await;
}
