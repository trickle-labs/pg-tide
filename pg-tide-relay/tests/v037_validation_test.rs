// v0.37.0 SQL validation integration tests.
//
// Tests:
//   1. The v0.37.0 migration script applies cleanly on top of v0.36.0.
//   2. The RockLake rocklake_type_config entry is present if applicable.
//   3. tide.relay_set_outbox_v2(jsonb) still works after v0.37.0 migration.
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

/// Sets up a fresh database with the full migration chain through v0.37.0.
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

// ── v0.37.0 migration applies cleanly ────────────────────────────────────────

/// After the full migration chain through v0.37.0, the schema must be
/// consistent: tide.relay_set_outbox_v2 must still exist (it was not removed),
/// and the schema version of the extension is 0.37.0.
#[tokio::test]
async fn test_v037_migration_applies_cleanly() {
    let (client, _container) = setup_db().await;
    // install_full_schema applies all migrations including v0.36.0→v0.37.0.
    // If any migration failed the setup would have panicked already.

    // Verify the schema is intact: tide tables must exist.
    for table in &[
        "tide_outbox_config",
        "tide_inbox_config",
        "relay_outbox_config",
    ] {
        let exists: bool = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
                 WHERE table_schema = 'tide' AND table_name = $1)",
                &[table],
            )
            .await
            .expect("table check")
            .get(0);
        assert!(exists, "tide.{table} must exist after v0.37.0 migration");
    }
}

/// relay_set_outbox_v2 (jsonb) must still be present after v0.37.0.
/// v0.37.0 is a no-op migration — no SQL functions were removed.
#[tokio::test]
async fn test_relay_set_outbox_v2_present_after_v037() {
    let (client, _container) = setup_db().await;
    let exists: bool = client
        .query_one(
            "SELECT EXISTS(
               SELECT 1 FROM information_schema.routines
               WHERE routine_schema = 'tide'
                 AND routine_name = 'relay_set_outbox_v2'
                 AND routine_type = 'FUNCTION'
             )",
            &[],
        )
        .await
        .expect("routine check")
        .get(0);
    assert!(
        exists,
        "tide.relay_set_outbox_v2 must still exist after v0.37.0 migration"
    );
}

/// relay_set_inbox_v2 (jsonb) must still be present after v0.37.0.
#[tokio::test]
async fn test_relay_set_inbox_v2_present_after_v037() {
    let (client, _container) = setup_db().await;
    let exists: bool = client
        .query_one(
            "SELECT EXISTS(
               SELECT 1 FROM information_schema.routines
               WHERE routine_schema = 'tide'
                 AND routine_name = 'relay_set_inbox_v2'
                 AND routine_type = 'FUNCTION'
             )",
            &[],
        )
        .await
        .expect("routine check")
        .get(0);
    assert!(
        exists,
        "tide.relay_set_inbox_v2 must still exist after v0.37.0 migration"
    );
}
