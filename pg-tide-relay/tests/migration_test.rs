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
const V0_12_0_TO_0_13_0: &str = include_str!("../../sql/pg_tide--0.12.0--0.13.0.sql");
const V0_13_0_TO_0_14_0: &str = include_str!("../../sql/pg_tide--0.13.0--0.14.0.sql");
const V0_14_0_TO_0_15_0: &str = include_str!("../../sql/pg_tide--0.14.0--0.15.0.sql");
const V0_15_0_TO_0_16_0: &str = include_str!("../../sql/pg_tide--0.15.0--0.16.0.sql");

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
    ("0.12.0 → 0.13.0", V0_12_0_TO_0_13_0),
    ("0.13.0 → 0.14.0", V0_13_0_TO_0_14_0),
    ("0.14.0 → 0.15.0", V0_14_0_TO_0_15_0),
    ("0.15.0 → 0.16.0", V0_15_0_TO_0_16_0),
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

    // After v0.13.0 upgrade: outbox_publishers table should exist.
    let has_outbox_publishers: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
             WHERE table_schema = 'tide' AND table_name = 'outbox_publishers')",
            &[],
        )
        .await
        .expect("table check")
        .get(0);
    assert!(
        has_outbox_publishers,
        "after v0.13.0 upgrade, tide.outbox_publishers must exist"
    );

    // relay_schema_fingerprints table should exist.
    let has_fingerprints: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
             WHERE table_schema = 'tide' AND table_name = 'relay_schema_fingerprints')",
            &[],
        )
        .await
        .expect("table check")
        .get(0);
    assert!(
        has_fingerprints,
        "after v0.13.0 upgrade, tide.relay_schema_fingerprints must exist"
    );

    // After v0.15.0 upgrade: outbox_truncate_delivered() should exist.
    let has_sweep_fn: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.routines \
             WHERE routine_schema = 'tide' AND routine_name = 'outbox_truncate_delivered')",
            &[],
        )
        .await
        .expect("routine check")
        .get(0);
    assert!(
        has_sweep_fn,
        "after v0.15.0 upgrade, tide.outbox_truncate_delivered() must exist"
    );

    // After v0.16.0 upgrade: outbox_create_if_not_exists() should exist.
    let has_idem_fn: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.routines \
             WHERE routine_schema = 'tide' AND routine_name = 'outbox_create_if_not_exists')",
            &[],
        )
        .await
        .expect("routine check")
        .get(0);
    assert!(
        has_idem_fn,
        "after v0.16.0 upgrade, tide.outbox_create_if_not_exists() must exist"
    );

    // After v0.16.0 upgrade: relay_set_inbox_v2() should exist.
    let has_inbox_v2: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.routines \
             WHERE routine_schema = 'tide' AND routine_name = 'relay_set_inbox_v2')",
            &[],
        )
        .await
        .expect("routine check")
        .get(0);
    assert!(
        has_inbox_v2,
        "after v0.16.0 upgrade, tide.relay_set_inbox_v2() must exist"
    );

    // Verify outbox_create_if_not_exists() works correctly.
    let created: bool = client
        .query_one(
            "SELECT tide.outbox_create_if_not_exists('upgrade_test_outbox')",
            &[],
        )
        .await
        .expect("outbox_create_if_not_exists first call")
        .get(0);
    assert!(created, "first call should return true (created)");

    let created_again: bool = client
        .query_one(
            "SELECT tide.outbox_create_if_not_exists('upgrade_test_outbox')",
            &[],
        )
        .await
        .expect("outbox_create_if_not_exists second call")
        .get(0);
    assert!(
        !created_again,
        "second call should return false (already exists)"
    );
}
