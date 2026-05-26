// v0.36.0 SQL validation integration tests.
//
// Tests:
//   1. Positional `relay_set_outbox()` 6-arg form must not exist after v0.36.0 migration.
//   2. Positional `relay_set_inbox()` 8-arg form must not exist after v0.36.0 migration.
//   3. `relay_set_outbox_v2(jsonb)` still works after v0.36.0 migration.
//   Note: `relay_set_inbox_v2(jsonb)` is a pgrx #[pg_extern] and only exists when the
//   extension is loaded; it is covered by pg-tide-ext pgrx unit tests instead.
mod common;

use std::time::Duration;
use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::NoTls;

async fn connect_retry(url: &str) -> tokio_postgres::Client {
    for _ in 0..20 {
        if let Ok((client, conn)) = tokio_postgres::connect(url, NoTls).await {
            tokio::spawn(async move {
                let _ = conn.await;
            });
            return client;
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    panic!("failed to connect to postgres after 20 retries");
}

/// Sets up a fresh database with the full migration chain through v0.36.0.
async fn setup_db() -> (
    tokio_postgres::Client,
    testcontainers::ContainerAsync<Postgres>,
) {
    let container = Postgres::default()
        .with_tag("18")
        .start()
        .await
        .expect("postgres container");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    let url = format!("host=127.0.0.1 port={port} user=postgres password=postgres dbname=postgres");
    let client = connect_retry(&url).await;
    common::install_full_schema(&client).await;
    (client, container)
}

// ── Positional API removal (v0.36.0) ─────────────────────────────────────────

/// After the v0.36.0 migration, `tide.relay_set_outbox` (6-param positional
/// form) must no longer exist.
#[tokio::test]
async fn test_relay_set_outbox_positional_form_absent() {
    let (client, _container) = setup_db().await;
    let exists: bool = client
        .query_one(
            "SELECT EXISTS(
               SELECT 1 FROM information_schema.routines
               WHERE routine_schema = 'tide'
                 AND routine_name = 'relay_set_outbox'
                 AND routine_type = 'FUNCTION'
             )",
            &[],
        )
        .await
        .expect("routine check")
        .get(0);
    assert!(
        !exists,
        "tide.relay_set_outbox() positional form must not exist after v0.36.0 migration"
    );
}

/// After the v0.36.0 migration, `tide.relay_set_inbox` (8-param positional
/// form) must no longer exist.
#[tokio::test]
async fn test_relay_set_inbox_positional_form_absent() {
    let (client, _container) = setup_db().await;
    let exists: bool = client
        .query_one(
            "SELECT EXISTS(
               SELECT 1 FROM information_schema.routines
               WHERE routine_schema = 'tide'
                 AND routine_name = 'relay_set_inbox'
                 AND routine_type = 'FUNCTION'
             )",
            &[],
        )
        .await
        .expect("routine check")
        .get(0);
    assert!(
        !exists,
        "tide.relay_set_inbox() positional form must not exist after v0.36.0 migration"
    );
}

/// `tide.relay_set_outbox_v2(jsonb)` must still be present and functional
/// after the v0.36.0 migration.
#[tokio::test]
async fn test_relay_set_outbox_v2_still_works() {
    let (client, _container) = setup_db().await;
    let result = client
        .execute(
            r#"SELECT tide.relay_set_outbox_v2($1)"#,
            &[&serde_json::json!({
                "outbox_name": "v036_test_outbox",
                "sink_type": "http",
                "sink_config": {"url": "http://localhost:9999"},
                "batch_size": 50,
                "enabled": true
            })],
        )
        .await;
    assert!(
        result.is_ok(),
        "relay_set_outbox_v2() must still work after v0.36.0 migration, got: {:?}",
        result
    );
}

// Note: tide.relay_set_inbox_v2(jsonb) is a pgrx #[pg_extern] and only exists
// when the pg_tide extension is loaded.  It cannot be exercised in the plain-SQL
// testcontainers environment.  Functional coverage is provided by the pgrx unit
// tests in pg-tide-ext.
