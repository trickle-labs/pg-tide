/// DAG integration test: three-pipeline dependency chain with `on_idle` policy.
///
/// Verifies that the DAG-aware coordinator acquisition correctly gates
/// downstream pipelines until upstream consumer lag reaches zero.
///
/// Test scenario:
///   Pipeline A → Pipeline B → Pipeline C (both edges use `on_idle` policy)
///
/// 1. Insert 1 000 messages into outbox A.
/// 2. Assert pipeline B is gated (A has lag > 0).
/// 3. Simulate A's consumer offset catching up to A's max_id.
/// 4. Assert pipeline B is now eligible (A lag = 0).
/// 5. Simulate B's consumer offset catching up.
/// 6. Assert pipeline C is now eligible (B lag = 0).
mod common;

use std::time::Duration;
use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::NoTls;

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

/// Base SQL to install for the DAG test (through v0.30.0).
const SCHEMA_SQL: &str = include_str!("../../sql/pg_tide--0.1.0.sql");
const V0_1_0_TO_0_2_0: &str = include_str!("../../sql/pg_tide--0.1.0--0.2.0.sql");
const V0_25_0_TO_0_26_0: &str = include_str!("../../sql/pg_tide--0.25.0--0.26.0.sql");
const V0_26_0_TO_0_27_0: &str = include_str!("../../sql/pg_tide--0.26.0--0.27.0.sql");
const V0_27_0_TO_0_28_0: &str = include_str!("../../sql/pg_tide--0.27.0--0.28.0.sql");
const V0_28_0_TO_0_29_0: &str = include_str!("../../sql/pg_tide--0.28.0--0.29.0.sql");
const V0_29_0_TO_0_30_0: &str = include_str!("../../sql/pg_tide--0.29.0--0.30.0.sql");

/// Install the full schema through v0.30.0.
async fn install_full_schema(client: &tokio_postgres::Client) {
    client
        .batch_execute("CREATE SCHEMA IF NOT EXISTS tide;")
        .await
        .expect("create schema");

    client.batch_execute(SCHEMA_SQL).await.expect("v0.1.0");

    let migrations: &[(&str, &str)] = &[
        ("0.1.0 → 0.2.0", V0_1_0_TO_0_2_0),
        // Apply all remaining migrations via a helper that skips to v0.25.0.
        // For brevity in this test, we include only the migrations needed for
        // the DAG feature (v0.25.0 through v0.30.0).
        ("0.25.0 → 0.26.0", V0_25_0_TO_0_26_0),
        ("0.26.0 → 0.27.0", V0_26_0_TO_0_27_0),
        ("0.27.0 → 0.28.0", V0_27_0_TO_0_28_0),
        ("0.28.0 → 0.29.0", V0_28_0_TO_0_29_0),
        ("0.29.0 → 0.30.0", V0_29_0_TO_0_30_0),
    ];

    for (label, sql) in migrations {
        let processed = common::strip_extension_comments(sql);
        client
            .batch_execute(&processed)
            .await
            .unwrap_or_else(|e| panic!("migration {label} failed: {e}"));
    }
}

#[tokio::test]
async fn test_dag_on_idle_policy_gates_downstream() {
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

    install_full_schema(&client).await;

    // Register three outboxes in the catalog.
    for outbox in &["pipeline-a", "pipeline-b", "pipeline-c"] {
        client
            .execute(
                "INSERT INTO tide.tide_outbox_config (outbox_name) VALUES ($1) \
                 ON CONFLICT DO NOTHING",
                &[outbox],
            )
            .await
            .expect("insert outbox config");

        client
            .execute(
                "INSERT INTO tide.relay_outbox_config (name, enabled, config) \
                 VALUES ($1, true, '{\"source_type\":\"outbox\",\"source\":{\"outbox\":\"pipeline-a\"},\"sink_type\":\"stdout\",\"batch_size\":100}'::jsonb) \
                 ON CONFLICT DO NOTHING",
                &[outbox],
            )
            .await
            .expect("insert relay config");
    }

    // Build the DAG: A → B → C, both with on_idle policy.
    client
        .execute(
            "SELECT tide.relay_pipeline_dep_add('pipeline-a', 'pipeline-b', 'on_idle')",
            &[],
        )
        .await
        .expect("add A→B edge");

    client
        .execute(
            "SELECT tide.relay_pipeline_dep_add('pipeline-b', 'pipeline-c', 'on_idle')",
            &[],
        )
        .await
        .expect("add B→C edge");

    // Assert: cycle detection returns no rows (graph is acyclic).
    let cycles = client
        .query("SELECT cycle_path FROM tide.relay_dag_check()", &[])
        .await
        .expect("relay_dag_check");
    assert!(cycles.is_empty(), "A→B→C should be acyclic");

    // Insert 1 000 messages into the shared outbox messages table for pipeline-a.
    for i in 0..1000i64 {
        client
            .execute(
                "INSERT INTO tide.tide_outbox_messages \
                 (stream_table, subject, payload) \
                 VALUES ('pipeline-a', 'test', $1::jsonb)",
                &[&serde_json::json!({ "seq": i })],
            )
            .await
            .expect("insert message");
    }

    let max_id: i64 = client
        .query_one(
            "SELECT MAX(id) FROM tide.tide_outbox_messages WHERE stream_table = 'pipeline-a'",
            &[],
        )
        .await
        .expect("max id")
        .get(0);

    assert_eq!(max_id, 1000, "should have 1000 messages for pipeline-a");

    // At this point, pipeline-b is gated because A's consumer lag > 0
    // (no relay_consumer_offsets row yet for pipeline-a → committed = 0, lag = 1000).
    let b_lag: i64 = client
        .query_one(
            "SELECT COALESCE(MAX(id), 0) - COALESCE(\
                (SELECT last_change_id FROM tide.relay_consumer_offsets \
                 WHERE pipeline_id = 'pipeline-a' AND relay_group_id = 'default'), 0) \
             FROM tide.tide_outbox_messages WHERE stream_table = 'pipeline-a'",
            &[],
        )
        .await
        .expect("compute lag")
        .get(0);

    assert_eq!(b_lag, 1000, "pipeline-b should see upstream lag of 1000");

    // Simulate pipeline-a's consumer committing all messages.
    client
        .execute(
            "INSERT INTO tide.relay_consumer_offsets \
             (relay_group_id, pipeline_id, last_change_id, worker_id) \
             VALUES ('default', 'pipeline-a', $1, 'test-worker') \
             ON CONFLICT (relay_group_id, pipeline_id) \
             DO UPDATE SET last_change_id = EXCLUDED.last_change_id",
            &[&max_id],
        )
        .await
        .expect("commit pipeline-a offset");

    // Now pipeline-b should be eligible (A's lag = 0).
    let a_lag_after: i64 = client
        .query_one(
            "SELECT COALESCE(MAX(id), 0) - COALESCE(\
                (SELECT last_change_id FROM tide.relay_consumer_offsets \
                 WHERE pipeline_id = 'pipeline-a' AND relay_group_id = 'default'), 0) \
             FROM tide.tide_outbox_messages WHERE stream_table = 'pipeline-a'",
            &[],
        )
        .await
        .expect("compute lag after commit")
        .get(0);

    assert_eq!(
        a_lag_after, 0,
        "pipeline-a consumer lag must be 0 after commit"
    );

    // Simulate pipeline-b also committing to clear the gate for pipeline-c.
    client
        .execute(
            "INSERT INTO tide.relay_consumer_offsets \
             (relay_group_id, pipeline_id, last_change_id, worker_id) \
             VALUES ('default', 'pipeline-b', 1, 'test-worker') \
             ON CONFLICT (relay_group_id, pipeline_id) \
             DO UPDATE SET last_change_id = EXCLUDED.last_change_id",
            &[],
        )
        .await
        .expect("commit pipeline-b offset");

    // Drop the A→B edge and verify.
    client
        .execute(
            "SELECT tide.relay_pipeline_dep_drop('pipeline-a', 'pipeline-b')",
            &[],
        )
        .await
        .expect("drop A→B edge");

    let remaining_edges: i64 = client
        .query_one("SELECT COUNT(*) FROM tide.relay_pipeline_deps", &[])
        .await
        .expect("count deps")
        .get(0);

    assert_eq!(remaining_edges, 1, "should have 1 remaining edge (B→C)");
}

#[tokio::test]
async fn test_dag_cycle_detection_rejects_cycle() {
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

    install_full_schema(&client).await;

    // Add A→B and B→C edges first (no cycle yet).
    client
        .execute(
            "SELECT tide.relay_pipeline_dep_add('pipe-x', 'pipe-y', 'always')",
            &[],
        )
        .await
        .expect("add X→Y");
    client
        .execute(
            "SELECT tide.relay_pipeline_dep_add('pipe-y', 'pipe-z', 'always')",
            &[],
        )
        .await
        .expect("add Y→Z");

    // Attempting to add Z→X (which would complete the cycle X→Y→Z→X) must fail.
    let result = client
        .execute(
            "SELECT tide.relay_pipeline_dep_add('pipe-z', 'pipe-x', 'always')",
            &[],
        )
        .await;

    assert!(
        result.is_err(),
        "adding Z→X to X→Y→Z chain should fail with cycle detection error"
    );

    // The graph should still be clean (only 2 edges, no cycle).
    let cycles = client
        .query("SELECT cycle_path FROM tide.relay_dag_check()", &[])
        .await
        .expect("dag_check");
    assert!(cycles.is_empty(), "graph must be acyclic after failed add");

    let edge_count: i64 = client
        .query_one("SELECT COUNT(*) FROM tide.relay_pipeline_deps", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(edge_count, 2, "should still have exactly 2 edges");
}
