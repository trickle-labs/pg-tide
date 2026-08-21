//! Public API E2E: native outbox -> real `pg-tide` process -> PostgreSQL inbox.

mod common;

use common::process::{wait_until, RelayProcess};
use std::time::Duration;
use tokio_postgres::NoTls;

const DATABASE_ENV: &str = "PG_TIDE_E2E_DATABASE_URL";
const OUTBOX: &str = "e2e_pg_inbox_outbox";
const INBOX: &str = "e2e_pg_inbox";

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
async fn public_api_outbox_to_pg_inbox_e2e() {
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
    client
        .execute("SELECT tide.inbox_create($1)", &[&INBOX])
        .await
        .expect("create inbox");
    let pipeline = serde_json::json!({
        "name": "e2e-pg-inbox",
        "outbox": OUTBOX,
        "sink_type": "inbox",
        "config": {"inbox": INBOX},
        "batch_size": 10
    });
    client
        .execute("SELECT tide.relay_set_outbox_v2($1::jsonb)", &[&pipeline])
        .await
        .expect("configure inbox pipeline");
    client
        .execute(
            "SELECT tide.outbox_publish($1, $2::jsonb, $3::jsonb)",
            &[
                &OUTBOX,
                &serde_json::json!({"order_id": "PG-1"}),
                &serde_json::json!({"event_type": "order.created"}),
            ],
        )
        .await
        .expect("publish outbox event");

    let relay = RelayProcess::start(&database_url, "e2e-pg-inbox");
    wait_until(Duration::from_secs(20), || async {
        client
            .query_opt(
                "SELECT payload FROM tide.\"e2e_pg_inbox_inbox\" WHERE payload->>'order_id' = 'PG-1'",
                &[],
            )
            .await
            .map(|row| row.is_some())
            .unwrap_or(false)
    })
    .await;
    let row = client
        .query_one(
            "SELECT payload FROM tide.\"e2e_pg_inbox_inbox\" WHERE payload->>'order_id' = 'PG-1'",
            &[],
        )
        .await
        .expect("query delivered inbox event");
    let payload: serde_json::Value = row.get(0);
    assert_eq!(payload["order_id"], "PG-1");
    relay.stop().await;
}
