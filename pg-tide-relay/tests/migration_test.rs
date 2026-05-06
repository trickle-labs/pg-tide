/// Migration upgrade test: install v0.1.0 schema, apply every upgrade script
/// sequentially, and assert catalog assertions at the final version.
///
/// This test verifies that all DDL migrations are self-consistent and that
/// the schema produced by sequential upgrades matches the expected state.
mod common;

use std::time::Duration;
use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::NoTls;

// Include all migration scripts as compile-time string constants.
const V0_1_0: &str = include_str!("../../sql/pg_tide--0.1.0.sql");
const V0_1_0_TO_0_2_0: &str = include_str!("../../sql/pg_tide--0.1.0--0.2.0.sql");
const V0_2_0_TO_0_3_0: &str = include_str!("../../sql/pg_tide--0.2.0--0.3.0.sql");
const V0_3_0_TO_0_4_0: &str = include_str!("../../sql/pg_tide--0.3.0--0.4.0.sql");
const V0_4_0_TO_0_5_0: &str = include_str!("../../sql/pg_tide--0.4.0--0.5.0.sql");
const V0_5_0_TO_0_6_0: &str = include_str!("../../sql/pg_tide--0.5.0--0.6.0.sql");
const V0_6_0_TO_0_7_0: &str = include_str!("../../sql/pg_tide--0.6.0--0.7.0.sql");
const V0_7_0_TO_0_8_0: &str = include_str!("../../sql/pg_tide--0.7.0--0.8.0.sql");
const V0_8_0_TO_0_9_0: &str = include_str!("../../sql/pg_tide--0.8.0--0.9.0.sql");
const V0_9_0_TO_0_10_0: &str = include_str!("../../sql/pg_tide--0.9.0--0.10.0.sql");
const V0_10_0_TO_0_11_0: &str = include_str!("../../sql/pg_tide--0.10.0--0.11.0.sql");
const V0_11_0_TO_0_12_0: &str = include_str!("../../sql/pg_tide--0.11.0--0.12.0.sql");

/// All upgrade scripts in order.
const UPGRADES: &[(&str, &str)] = &[
    ("0.1.0 → 0.2.0", V0_1_0_TO_0_2_0),
    ("0.2.0 → 0.3.0", V0_2_0_TO_0_3_0),
    ("0.3.0 → 0.4.0", V0_3_0_TO_0_4_0),
    ("0.4.0 → 0.5.0", V0_4_0_TO_0_5_0),
    ("0.5.0 → 0.6.0", V0_5_0_TO_0_6_0),
    ("0.6.0 → 0.7.0", V0_6_0_TO_0_7_0),
    ("0.7.0 → 0.8.0", V0_7_0_TO_0_8_0),
    ("0.8.0 → 0.9.0", V0_8_0_TO_0_9_0),
    ("0.9.0 → 0.10.0", V0_9_0_TO_0_10_0),
    ("0.10.0 → 0.11.0", V0_10_0_TO_0_11_0),
    ("0.11.0 → 0.12.0", V0_11_0_TO_0_12_0),
];

async fn connect_with_retry(url: &str) -> tokio_postgres::Client {
    let mut attempt = 0u32;
    loop {
        match tokio_postgres::connect(url, NoTls).await {
            Ok((client, conn)) => {
                tokio::spawn(async move {
                    let _ = conn.await;
                });
                return client;
            }
            Err(e) => {
                attempt += 1;
                if attempt >= 20 {
                    panic!("failed to connect after {attempt} attempts: {e}");
                }
                tokio::time::sleep(Duration::from_millis(400)).await;
            }
        }
    }
}

#[tokio::test]
async fn test_sequential_migration_upgrade() {
    let container = Postgres::default()
        .with_tag("18")
        .start()
        .await
        .expect("failed to start postgres");

    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get port");

    let url = format!("host=127.0.0.1 port={port} user=postgres password=postgres dbname=postgres");
    let client = connect_with_retry(&url).await;

    // Install base schema (v0.1.0).
    client
        .batch_execute("CREATE SCHEMA IF NOT EXISTS tide;")
        .await
        .expect("create schema");
    client
        .batch_execute(V0_1_0)
        .await
        .expect("install v0.1.0 schema");

    // Assert v0.1.0 baseline: required tables exist.
    for table in &[
        "tide_outbox_config",
        "tide_outbox_messages",
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
        assert!(exists, "v0.1.0 should have table tide.{table}");
    }

    // v0.1.0: relay_consumer_offsets has last_offset TEXT.
    let has_last_offset: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
             WHERE table_schema = 'tide' AND table_name = 'relay_consumer_offsets' \
             AND column_name = 'last_offset')",
            &[],
        )
        .await
        .expect("column check")
        .get(0);
    assert!(
        has_last_offset,
        "v0.1.0 should have relay_consumer_offsets.last_offset"
    );

    // Apply all upgrades in sequence.
    for (label, sql) in UPGRADES {
        client
            .batch_execute(sql)
            .await
            .unwrap_or_else(|e| panic!("upgrade {label} failed: {e}"));
    }

    // After v0.12.0 upgrade: relay_consumer_offsets should have last_change_id.
    let has_change_id: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
             WHERE table_schema = 'tide' AND table_name = 'relay_consumer_offsets' \
             AND column_name = 'last_change_id')",
            &[],
        )
        .await
        .expect("column check")
        .get(0);
    assert!(
        has_change_id,
        "after v0.12.0 upgrade, relay_consumer_offsets must have last_change_id column"
    );

    // The old last_offset TEXT column should be gone.
    let still_has_last_offset: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
             WHERE table_schema = 'tide' AND table_name = 'relay_consumer_offsets' \
             AND column_name = 'last_offset')",
            &[],
        )
        .await
        .expect("column check")
        .get(0);
    assert!(
        !still_has_last_offset,
        "after v0.12.0 upgrade, relay_consumer_offsets.last_offset should be dropped"
    );

    // worker_id column should now exist.
    let has_worker_id: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
             WHERE table_schema = 'tide' AND table_name = 'relay_consumer_offsets' \
             AND column_name = 'worker_id')",
            &[],
        )
        .await
        .expect("column check")
        .get(0);
    assert!(
        has_worker_id,
        "after v0.12.0 upgrade, relay_consumer_offsets must have worker_id column"
    );

    // Verify basic DML still works after all upgrades.
    client
        .execute(
            "INSERT INTO tide.tide_outbox_config (outbox_name) VALUES ('post_upgrade_test')",
            &[],
        )
        .await
        .expect("DML after upgrade");

    client
        .execute(
            "INSERT INTO tide.relay_consumer_offsets \
             (relay_group_id, pipeline_id, last_change_id, worker_id) \
             VALUES ('default', 'post-upgrade-pipeline', 0, 'test-worker')",
            &[],
        )
        .await
        .expect("relay_consumer_offsets insert after upgrade");
}
