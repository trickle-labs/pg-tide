// v0.38.0 SQL validation integration tests.
//
// Tests:
//   1. The v0.38.0 migration script applies cleanly on top of v0.37.0.
//   2. All tide.* functions remain intact after the no-op migration.
//   3. max_write_retries and read_replica_url config fields compile and work.
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

/// Sets up a fresh database with the full migration chain through v0.38.0.
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

// ── v0.38.0 migration applies cleanly ────────────────────────────────────────

/// After the full migration chain through v0.38.0, the schema must be
/// consistent: all tide tables must exist and no breaking changes occurred.
#[tokio::test]
async fn test_v038_migration_applies_cleanly() {
    let (client, _container) = setup_db().await;

    // Verify the schema is intact: core tide tables must exist.
    for table in &[
        "tide_outbox_config",
        "tide_inbox_config",
        "relay_outbox_config",
        "relay_inbox_config",
        "relay_consumer_offsets",
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
        assert!(exists, "tide.{table} must exist after v0.38.0 migration");
    }
}

/// All v2 relay API functions must still be present after v0.38.0.
/// v0.38.0 is a no-op SQL migration — only the relay binary changed.
/// Note: relay_set_outbox_v2 has a plpgsql stub in the migration chain;
/// relay_set_inbox_v2 is pgrx-only and only exists when the extension binary
/// is loaded (not in plain testcontainer environments).
#[tokio::test]
async fn test_relay_api_functions_present_after_v038() {
    let (client, _container) = setup_db().await;
    // Only check relay_set_outbox_v2 — it has a plpgsql stub from v0.17→0.18.
    // relay_set_inbox_v2 is a Rust #[pg_extern] with no SQL stub; it cannot
    // exist in a plain testcontainer environment without the pgrx extension.
    for fn_name in &["relay_set_outbox_v2"] {
        let exists: bool = client
            .query_one(
                "SELECT EXISTS(
                   SELECT 1 FROM information_schema.routines
                   WHERE routine_schema = 'tide'
                     AND routine_name = $1
                     AND routine_type = 'FUNCTION'
                 )",
                &[fn_name],
            )
            .await
            .expect("routine check")
            .get(0);
        assert!(
            exists,
            "tide.{fn_name} must still exist after v0.38.0 migration"
        );
    }
}

/// Positional relay API forms must remain absent after v0.38.0.
/// These were removed in v0.36.0 and must not be re-introduced.
#[tokio::test]
async fn test_positional_relay_forms_absent_after_v038() {
    let (client, _container) = setup_db().await;
    for fn_name in &["relay_set_outbox", "relay_set_inbox"] {
        let exists: bool = client
            .query_one(
                "SELECT EXISTS(
                   SELECT 1 FROM information_schema.routines
                   WHERE routine_schema = 'tide'
                     AND routine_name = $1
                     AND routine_type = 'FUNCTION'
                 )",
                &[fn_name],
            )
            .await
            .expect("routine check")
            .get(0);
        assert!(
            !exists,
            "tide.{fn_name}() positional form must not exist after v0.38.0 migration"
        );
    }
}

// ── RockLake config fields (compile-time) ─────────────────────────────────────

/// Verifies the Phase 7 config fields are wired into `RockLakeConfig`.
/// This is a compile-time / unit test — no database required.
/// Requires the `rocklake` feature.
#[test]
#[cfg(feature = "rocklake")]
fn test_rocklake_config_phase7_fields() {
    use pg_tide_relay::sink::rocklake::RockLakeConfig;

    let config = RockLakeConfig::new("s3://bucket/events", "analytics");
    assert_eq!(
        config.max_write_retries, 5,
        "default max_write_retries must be 5"
    );
    assert!(
        config.read_replica_url.is_none(),
        "default read_replica_url must be None"
    );

    let mut config2 = RockLakeConfig::new("s3://bucket/events", "analytics");
    config2.max_write_retries = 10;
    config2.read_replica_url = Some("postgres://replica:5432/catalog".to_string());
    assert_eq!(config2.max_write_retries, 10);
    assert_eq!(
        config2.read_replica_url.as_deref(),
        Some("postgres://replica:5432/catalog")
    );
}
