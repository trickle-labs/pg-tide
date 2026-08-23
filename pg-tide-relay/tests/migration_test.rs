/// Engineering migration test: install v0.1.0 schema, apply every raw SQL
/// upgrade script sequentially, and assert catalog assertions at the final version.
///
/// This intentionally exercises the historical raw SQL chain. Packaged
/// lifecycle parity is covered by the v0.51.0 artifact tests.
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
const V0_16_0_TO_0_17_0: &str = include_str!("../../sql/pg_tide--0.16.0--0.17.0.sql");
const V0_17_0_TO_0_18_0: &str = include_str!("../../sql/pg_tide--0.17.0--0.18.0.sql");
const V0_18_0_TO_0_19_0: &str = include_str!("../../sql/pg_tide--0.18.0--0.19.0.sql");
const V0_19_0_TO_0_20_0: &str = include_str!("../../sql/pg_tide--0.19.0--0.20.0.sql");
const V0_20_0_TO_0_21_0: &str = include_str!("../../sql/pg_tide--0.20.0--0.21.0.sql");
const V0_21_0_TO_0_22_0: &str = include_str!("../../sql/pg_tide--0.21.0--0.22.0.sql");
const V0_22_0_TO_0_23_0: &str = include_str!("../../sql/pg_tide--0.22.0--0.23.0.sql");
const V0_23_0_TO_0_24_0: &str = include_str!("../../sql/pg_tide--0.23.0--0.24.0.sql");
const V0_24_0_TO_0_25_0: &str = include_str!("../../sql/pg_tide--0.24.0--0.25.0.sql");
const V0_25_0_TO_0_26_0: &str = include_str!("../../sql/pg_tide--0.25.0--0.26.0.sql");
const V0_26_0_TO_0_27_0: &str = include_str!("../../sql/pg_tide--0.26.0--0.27.0.sql");
const V0_27_0_TO_0_28_0: &str = include_str!("../../sql/pg_tide--0.27.0--0.28.0.sql");
const V0_28_0_TO_0_29_0: &str = include_str!("../../sql/pg_tide--0.28.0--0.29.0.sql");
const V0_29_0_TO_0_30_0: &str = include_str!("../../sql/pg_tide--0.29.0--0.30.0.sql");
const V0_30_0_TO_0_31_0: &str = include_str!("../../sql/pg_tide--0.30.0--0.31.0.sql");
const V0_31_0_TO_0_32_0: &str = include_str!("../../sql/pg_tide--0.31.0--0.32.0.sql");
const V0_32_0_TO_0_33_0: &str = include_str!("../../sql/pg_tide--0.32.0--0.33.0.sql");
const V0_33_0_TO_0_34_0: &str = include_str!("../../sql/pg_tide--0.33.0--0.34.0.sql");
const V0_34_0_TO_0_35_0: &str = include_str!("../../sql/pg_tide--0.34.0--0.35.0.sql");
const V0_35_0_TO_0_36_0: &str = include_str!("../../sql/pg_tide--0.35.0--0.36.0.sql");
const V0_36_0_TO_0_37_0: &str = include_str!("../../sql/pg_tide--0.36.0--0.37.0.sql");
const V0_37_0_TO_0_38_0: &str = include_str!("../../sql/pg_tide--0.37.0--0.38.0.sql");
const V0_38_0_TO_0_39_0: &str = include_str!("../../sql/pg_tide--0.38.0--0.39.0.sql");
const V0_39_0_TO_0_40_0: &str = include_str!("../../sql/pg_tide--0.39.0--0.40.0.sql");
const V0_40_0_TO_0_41_0: &str = include_str!("../../sql/pg_tide--0.40.0--0.41.0.sql");
const V0_41_0_TO_0_42_0: &str = include_str!("../../sql/pg_tide--0.41.0--0.42.0.sql");
const V0_42_0_TO_0_43_0: &str = include_str!("../../sql/pg_tide--0.42.0--0.43.0.sql");
const V0_43_0_TO_0_44_0: &str = include_str!("../../sql/pg_tide--0.43.0--0.44.0.sql");
const V0_44_0_TO_0_45_0: &str = include_str!("../../sql/pg_tide--0.44.0--0.45.0.sql");
const V0_45_0_TO_0_46_0: &str = include_str!("../../sql/pg_tide--0.45.0--0.46.0.sql");
const V0_46_0_TO_0_47_0: &str = include_str!("../../sql/pg_tide--0.46.0--0.47.0.sql");
const V0_47_0_TO_0_48_0: &str = include_str!("../../sql/pg_tide--0.47.0--0.48.0.sql");
const V0_48_0_TO_0_49_0: &str = include_str!("../../sql/pg_tide--0.48.0--0.49.0.sql");
const V0_49_0_TO_0_50_0: &str = include_str!("../../sql/pg_tide--0.49.0--0.50.0.sql");
const V0_50_0_TO_0_51_0: &str = include_str!("../../sql/pg_tide--0.50.0--0.51.0.sql");
const V0_51_0_TO_0_52_0: &str = include_str!("../../sql/pg_tide--0.51.0--0.52.0.sql");
const V0_52_0_TO_0_53_0: &str = include_str!("../../sql/pg_tide--0.52.0--0.53.0.sql");
const V0_53_0_TO_0_54_0: &str = include_str!("../../sql/pg_tide--0.53.0--0.54.0.sql");

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
    ("0.16.0 → 0.17.0", V0_16_0_TO_0_17_0),
    ("0.17.0 → 0.18.0", V0_17_0_TO_0_18_0),
    ("0.18.0 → 0.19.0", V0_18_0_TO_0_19_0),
    ("0.19.0 → 0.20.0", V0_19_0_TO_0_20_0),
    ("0.20.0 → 0.21.0", V0_20_0_TO_0_21_0),
    ("0.21.0 → 0.22.0", V0_21_0_TO_0_22_0),
    ("0.22.0 → 0.23.0", V0_22_0_TO_0_23_0),
    ("0.23.0 → 0.24.0", V0_23_0_TO_0_24_0),
    ("0.24.0 → 0.25.0", V0_24_0_TO_0_25_0),
    ("0.25.0 → 0.26.0", V0_25_0_TO_0_26_0),
    ("0.26.0 → 0.27.0", V0_26_0_TO_0_27_0),
    ("0.27.0 → 0.28.0", V0_27_0_TO_0_28_0),
    ("0.28.0 → 0.29.0", V0_28_0_TO_0_29_0),
    ("0.29.0 → 0.30.0", V0_29_0_TO_0_30_0),
    ("0.30.0 → 0.31.0", V0_30_0_TO_0_31_0),
    ("0.31.0 → 0.32.0", V0_31_0_TO_0_32_0),
    ("0.32.0 → 0.33.0", V0_32_0_TO_0_33_0),
    ("0.33.0 → 0.34.0", V0_33_0_TO_0_34_0),
    ("0.34.0 → 0.35.0", V0_34_0_TO_0_35_0),
    ("0.35.0 → 0.36.0", V0_35_0_TO_0_36_0),
    ("0.36.0 → 0.37.0", V0_36_0_TO_0_37_0),
    ("0.37.0 → 0.38.0", V0_37_0_TO_0_38_0),
    ("0.38.0 → 0.39.0", V0_38_0_TO_0_39_0),
    ("0.39.0 → 0.40.0", V0_39_0_TO_0_40_0),
    ("0.40.0 → 0.41.0", V0_40_0_TO_0_41_0),
    ("0.41.0 → 0.42.0", V0_41_0_TO_0_42_0),
    ("0.42.0 → 0.43.0", V0_42_0_TO_0_43_0),
    ("0.43.0 → 0.44.0", V0_43_0_TO_0_44_0),
    ("0.44.0 → 0.45.0", V0_44_0_TO_0_45_0),
    ("0.45.0 → 0.46.0", V0_45_0_TO_0_46_0),
    ("0.46.0 → 0.47.0", V0_46_0_TO_0_47_0),
    ("0.47.0 → 0.48.0", V0_47_0_TO_0_48_0),
    ("0.48.0 → 0.49.0", V0_48_0_TO_0_49_0),
    ("0.49.0 → 0.50.0", V0_49_0_TO_0_50_0),
    ("0.50.0 → 0.51.0", V0_50_0_TO_0_51_0),
    ("0.51.0 → 0.52.0", V0_51_0_TO_0_52_0),
    ("0.52.0 → 0.53.0", V0_52_0_TO_0_53_0),
    ("0.53.0 → 0.54.0", V0_53_0_TO_0_54_0),
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
        .batch_execute(&common::strip_extension_comments(V0_1_0))
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
        let processed = common::strip_extension_comments(sql);
        let processed = if *label == "0.42.0 → 0.43.0" {
            common::strip_unavailable_v043_bindings(&processed)
        } else {
            processed
        };
        client.batch_execute(&processed).await.unwrap_or_else(|e| {
            let detail = e
                .as_db_error()
                .map(|db| format!("{}: {}", db.severity(), db.message()))
                .unwrap_or_else(|| e.to_string());
            panic!("upgrade {label} failed: {detail}");
        });
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
             (relay_group_id, pipeline_id, outbox_name, last_change_id, worker_id) \
             VALUES ('default', 'post-upgrade-pipeline', 'post_upgrade_test', 0, 'test-worker')",
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

    // v0.17.0 note: outbox_truncate_delivered(), outbox_create_if_not_exists(),
    // and relay_set_inbox_v2() are now implemented exclusively as Rust
    // #[pg_extern] functions (the plpgsql duplicates were removed in v0.17.0).
    // This SQL-only migration test does not load the Rust extension, so those
    // functions are not available here. Their presence is verified by the pgrx
    // test suite (test-ext-pgrx CI job) and by the E2E test (public_api_outbox_to_nats_e2e).

    // After v0.22.0 upgrade: DuckLake source tables should exist.
    let has_ducklake_source_config: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
             WHERE table_schema = 'tide' AND table_name = 'ducklake_source_config')",
            &[],
        )
        .await
        .expect("table check")
        .get(0);
    assert!(
        has_ducklake_source_config,
        "after v0.22.0 upgrade, tide.ducklake_source_config must exist"
    );

    // After v0.23.0 upgrade: admin_rewind_offset() function should exist.
    let has_admin_rewind: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.routines \
             WHERE routine_schema = 'tide' AND routine_name = 'admin_rewind_offset')",
            &[],
        )
        .await
        .expect("function check")
        .get(0);
    assert!(
        has_admin_rewind,
        "after v0.23.0 upgrade, tide.admin_rewind_offset() must exist"
    );

    // After v0.25.0 upgrade: partition_strategy column should exist.
    let has_partition_strategy: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
             WHERE table_schema = 'tide' AND table_name = 'tide_outbox_config' \
             AND column_name = 'partition_strategy')",
            &[],
        )
        .await
        .expect("column check")
        .get(0);
    assert!(
        has_partition_strategy,
        "after v0.25.0 upgrade, tide_outbox_config must have partition_strategy column"
    );

    // retention_partitions column should exist.
    let has_retention_partitions: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
             WHERE table_schema = 'tide' AND table_name = 'tide_outbox_config' \
             AND column_name = 'retention_partitions')",
            &[],
        )
        .await
        .expect("column check")
        .get(0);
    assert!(
        has_retention_partitions,
        "after v0.25.0 upgrade, tide_outbox_config must have retention_partitions column"
    );

    // tide_partition_events table should exist.
    let has_partition_events: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
             WHERE table_schema = 'tide' AND table_name = 'tide_partition_events')",
            &[],
        )
        .await
        .expect("table check")
        .get(0);
    assert!(
        has_partition_events,
        "after v0.25.0 upgrade, tide.tide_partition_events must exist"
    );

    // outbox_convert_to_partitioned() function should exist.
    let has_convert_fn: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.routines \
             WHERE routine_schema = 'tide' AND routine_name = 'outbox_convert_to_partitioned')",
            &[],
        )
        .await
        .expect("function check")
        .get(0);
    assert!(
        has_convert_fn,
        "after v0.25.0 upgrade, tide.outbox_convert_to_partitioned() must exist"
    );

    // partition_strategy column accepts only valid values.
    client
        .execute(
            "INSERT INTO tide.tide_outbox_config \
             (outbox_name, partition_strategy, retention_partitions) \
             VALUES ('test_partition_outbox', 'daily', 7)",
            &[],
        )
        .await
        .expect("insert with partition_strategy=daily");

    let strategy: String = client
        .query_one(
            "SELECT partition_strategy FROM tide.tide_outbox_config \
             WHERE outbox_name = 'test_partition_outbox'",
            &[],
        )
        .await
        .expect("select partition_strategy")
        .get(0);
    assert_eq!(strategy, "daily", "partition_strategy should be 'daily'");

    // After v0.30.0 upgrade: relay_pipeline_deps table should exist.
    let has_pipeline_deps: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
             WHERE table_schema = 'tide' AND table_name = 'relay_pipeline_deps')",
            &[],
        )
        .await
        .expect("table check")
        .get(0);
    assert!(
        has_pipeline_deps,
        "after v0.30.0 upgrade, tide.relay_pipeline_deps must exist"
    );

    // relay_dag_check() function should exist.
    let has_dag_check: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.routines \
             WHERE routine_schema = 'tide' AND routine_name = 'relay_dag_check')",
            &[],
        )
        .await
        .expect("function check")
        .get(0);
    assert!(
        has_dag_check,
        "after v0.30.0 upgrade, tide.relay_dag_check() must exist"
    );

    // relay_dag_check() must return no rows (empty graph = acyclic).
    let dag_rows = client
        .query("SELECT cycle_path FROM tide.relay_dag_check()", &[])
        .await
        .expect("relay_dag_check()");
    assert!(
        dag_rows.is_empty(),
        "relay_dag_check() must return no rows for an empty DAG"
    );

    // relay_pipeline_dep_add() must succeed for a valid edge.
    client
        .execute(
            "SELECT tide.relay_pipeline_dep_add('pipeline-a', 'pipeline-b', 'always')",
            &[],
        )
        .await
        .expect("relay_pipeline_dep_add()");

    // relay_pipeline_dep_drop() must remove the edge.
    client
        .execute(
            "SELECT tide.relay_pipeline_dep_drop('pipeline-a', 'pipeline-b')",
            &[],
        )
        .await
        .expect("relay_pipeline_dep_drop()");

    // Verify edge is gone.
    let dep_count: i64 = client
        .query_one("SELECT COUNT(*) FROM tide.relay_pipeline_deps", &[])
        .await
        .expect("count deps")
        .get(0);
    assert_eq!(dep_count, 0, "relay_pipeline_deps must be empty after drop");

    // After v0.36.0 upgrade: positional API forms must no longer exist.
    // relay_set_outbox(text, text, text, jsonb, integer, boolean) must be gone.
    let has_positional_outbox: bool = client
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
        !has_positional_outbox,
        "after v0.36.0 upgrade, tide.relay_set_outbox() positional form must not exist"
    );

    // relay_set_inbox(text, text, jsonb, integer, text, boolean, integer, boolean) must be gone.
    let has_positional_inbox: bool = client
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
        !has_positional_inbox,
        "after v0.36.0 upgrade, tide.relay_set_inbox() positional form must not exist"
    );

    // relay_set_outbox_v2() must still exist (created as SQL in v0.17.0→v0.18.0).
    // Note: relay_set_inbox_v2() is a pgrx #[pg_extern] and is only present when
    // the extension is loaded — it cannot be checked in a plain-SQL test env.
    let has_outbox_v2: bool = client
        .query_one(
            "SELECT EXISTS(
               SELECT 1 FROM information_schema.routines
               WHERE routine_schema = 'tide'
                 AND routine_name = 'relay_set_outbox_v2'
             )",
            &[],
        )
        .await
        .expect("routine check")
        .get(0);
    assert!(
        has_outbox_v2,
        "after v0.36.0 upgrade, tide.relay_set_outbox_v2() must still exist"
    );

    // ── v0.40.0 (ADR-011) assertions ─────────────────────────────────────

    // The unconditional (outbox_name, id) polling index must exist and must
    // NOT be partial (no WHERE clause).
    let poll_index_def: Option<String> = client
        .query_opt(
            "SELECT indexdef FROM pg_indexes \
             WHERE schemaname = 'tide' AND indexname = 'idx_tide_outbox_messages_poll'",
            &[],
        )
        .await
        .expect("poll index lookup")
        .map(|r| r.get(0));
    let poll_index_def = poll_index_def.expect("idx_tide_outbox_messages_poll must exist");
    assert!(
        !poll_index_def.to_lowercase().contains(" where "),
        "poll index must be unconditional (not partial): {poll_index_def}"
    );

    // relay_consumer_offsets.outbox_name must exist and be NOT NULL.
    let outbox_name_nullable: String = client
        .query_one(
            "SELECT is_nullable FROM information_schema.columns \
             WHERE table_schema = 'tide' AND table_name = 'relay_consumer_offsets' \
               AND column_name = 'outbox_name'",
            &[],
        )
        .await
        .expect("outbox_name column check")
        .get(0);
    assert_eq!(
        outbox_name_nullable, "NO",
        "relay_consumer_offsets.outbox_name must be NOT NULL after v0.40.0"
    );

    // The offset primary key must be (relay_group_id, pipeline_id, outbox_name):
    // inserting the same pipeline id for two different outboxes must succeed.
    client
        .execute(
            "INSERT INTO tide.tide_outbox_config (outbox_name) VALUES ('orders_v40'), ('audit_v40')",
            &[],
        )
        .await
        .expect("create outboxes for offset key test");
    client
        .execute(
            "INSERT INTO tide.relay_consumer_offsets \
             (relay_group_id, pipeline_id, outbox_name, last_change_id, worker_id) \
             VALUES ('g', 'same-pipeline', 'orders_v40', 10, 'w'), \
                    ('g', 'same-pipeline', 'audit_v40', 20, 'w')",
            &[],
        )
        .await
        .expect("same pipeline id for different outboxes must be allowed");

    // v0.49.0 removes the empty fan-in catalog after its preflight.
    let has_fanin_catalog: bool = client
        .query_one(
            "SELECT to_regclass('tide.relay_fanin_config') IS NOT NULL",
            &[],
        )
        .await
        .expect("fanin catalog lookup")
        .get(0);
    assert!(
        !has_fanin_catalog,
        "v0.49.0 must remove the empty fan-in catalog"
    );

    for function in [
        "relay_set_inbox_v2",
        "relay_set_fanin",
        "relay_fanin_enable",
        "relay_fanin_disable",
        "relay_fanin_delete",
        "relay_fanin_list",
        "backfill_create",
        "backfill_pause",
        "backfill_resume",
        "backfill_status",
        "backfill_progress",
        "backfill_cancel",
    ] {
        let exists: bool = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
                 WHERE n.nspname = 'tide' AND p.proname = $1)",
                &[&function],
            )
            .await
            .expect("retired function lookup")
            .get(0);
        assert!(!exists, "v0.49.0 must remove tide.{function}()");
    }

    // ── v0.45.0 observational runtime status ─────────────────────────────
    let has_runtime_status: bool = client
        .query_one(
            "SELECT to_regclass('tide.relay_runtime_status') IS NOT NULL",
            &[],
        )
        .await
        .expect("runtime status table lookup")
        .get(0);
    assert!(
        has_runtime_status,
        "v0.45.0 runtime status table must exist"
    );

    let has_status_view: bool = client
        .query_one(
            "SELECT to_regclass('tide.relay_pipeline_status') IS NOT NULL",
            &[],
        )
        .await
        .expect("status view lookup")
        .get(0);
    assert!(has_status_view, "v0.45.0 sanitized status view must exist");

    client
        .execute(
            "INSERT INTO tide.relay_outbox_config
             (name, config) VALUES
             ('status-pipeline', '{\"source\":{\"outbox\":\"orders_v40\"}}')",
            &[],
        )
        .await
        .expect("status pipeline insert");
    client
        .execute(
            "INSERT INTO tide.relay_runtime_status
             (relay_group_id, pipeline_id, direction, owner_token)
             VALUES ('default', 'status-pipeline', 'forward', 'opaque-owner')",
            &[],
        )
        .await
        .expect("runtime status insert");
    let ownership: String = client
        .query_one(
            "SELECT ownership FROM tide.relay_pipeline_status
             WHERE pipeline_id = 'status-pipeline'",
            &[],
        )
        .await
        .expect("status view query")
        .get(0);
    assert_eq!(ownership, "stale");

    let has_owner_token_column: bool = client
        .query_one(
            "SELECT EXISTS(
               SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'tide'
                 AND table_name = 'relay_pipeline_status'
                 AND column_name = 'owner_token'
             )",
            &[],
        )
        .await
        .expect("status view columns")
        .get(0);
    assert!(
        !has_owner_token_column,
        "sanitized status view must not expose owner tokens"
    );
}

#[tokio::test]
async fn test_v049_migration_fails_closed_for_enabled_removed_config() {
    let db = common::PgTideTestDb::start_base_v0_1().await;
    for (label, sql) in common::MIGRATIONS {
        if *label == "0.48.0 -> 0.49.0" {
            break;
        }
        let processed = common::strip_extension_comments(sql);
        let processed = if *label == "0.42.0 -> 0.43.0" {
            common::strip_unavailable_v043_bindings(&processed)
        } else {
            processed
        };
        db.client
            .batch_execute(&processed)
            .await
            .unwrap_or_else(|e| panic!("upgrade {label} failed: {e}"));
    }

    db.client
        .execute(
            "INSERT INTO tide.tide_outbox_config (outbox_name) VALUES ('surf5-outbox')",
            &[],
        )
        .await
        .expect("create outbox");
    db.client
        .execute(
            "INSERT INTO tide.relay_outbox_config (name, enabled, config) \
             VALUES ('surf5-redis', true, '{\"source_type\":\"outbox\",\"sink_type\":\"redis\"}')",
            &[],
        )
        .await
        .expect("create removed pipeline");

    let error = db
        .client
        .batch_execute(V0_48_0_TO_0_49_0)
        .await
        .expect_err("enabled removed config must block migration");
    let detail = error
        .as_db_error()
        .map(|db| db.message().to_string())
        .unwrap_or_else(|| error.to_string());
    assert!(detail.contains("PGTIDE_CONFIG_UNSUPPORTED_SURFACE"));
    assert!(detail.contains("surf5-redis"));
    assert!(detail.contains("last_version=0.48.0"));

    db.client
        .execute(
            "UPDATE tide.relay_outbox_config SET enabled = false WHERE name = 'surf5-redis'",
            &[],
        )
        .await
        .expect("disable removed pipeline");
    db.client
        .batch_execute(&common::strip_extension_comments(V0_48_0_TO_0_49_0))
        .await
        .expect("disabled removed config may be preserved during migration");

    let preserved: bool = db
        .client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM tide.relay_outbox_config WHERE name = 'surf5-redis' AND NOT enabled)",
            &[],
        )
        .await
        .expect("preserved disabled config lookup")
        .get(0);
    assert!(preserved, "disabled unsupported config must be preserved");
}

#[tokio::test]
async fn test_fresh_and_sequential_upgrade_surface_parity() {
    let fresh = common::PgTideTestDb::start().await;
    let upgraded = common::PgTideTestDb::start_base_v0_1().await;
    for (label, sql) in common::MIGRATIONS {
        let processed = common::strip_extension_comments(sql);
        let processed = if *label == "0.42.0 -> 0.43.0" {
            common::strip_unavailable_v043_bindings(&processed)
        } else {
            processed
        };
        upgraded
            .client
            .batch_execute(&processed)
            .await
            .unwrap_or_else(|e| panic!("upgrade {label} failed: {e}"));
    }

    assert_eq!(
        relation_objects(&fresh.client)
            .await
            .expect("fresh objects"),
        relation_objects(&upgraded.client)
            .await
            .expect("upgraded objects"),
        "fresh and sequential upgrade catalogs must have the same relation surface"
    );
}

async fn relation_objects(
    client: &tokio_postgres::Client,
) -> Result<Vec<(String, String)>, tokio_postgres::Error> {
    client
        .query(
            "SELECT c.relname, c.relkind::text FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = 'tide' AND c.relkind IN ('r', 'v', 'm', 'i') \
             ORDER BY c.relkind, c.relname",
            &[],
        )
        .await
        .map(|rows| {
            rows.into_iter()
                .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
                .collect()
        })
}
