/// DAG topology tests: diamond, fan-out, multi-level mixed policies, and
/// additional cycle detection scenarios.
///
/// v0.36.0 additions:
///   - Diamond topology: A→B, A→C, B→D, C→D
///   - Fan-out topology: A→B, A→C, A→D (parallel branches)
///   - Multi-level mixed policies: A→(on_idle)B→(on_offset_gte_500)C
///   - Self-loop cycle detection: A→A must be rejected
///   - Two-node cycle detection: A→B, B→A must be rejected
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
    let client = connect_with_retry(&url).await;
    common::install_full_schema(&client).await;
    (client, container)
}

// ── Diamond topology ─────────────────────────────────────────────────────────
//
// A → B, A → C, B → D, C → D
//
// D has two upstream dependencies (B and C). Both edges into D use 'on_idle'.
// Verifies that the DAG correctly tracks multi-parent dependencies without
// a cycle error (diamond is acyclic).

#[tokio::test]
async fn test_dag_diamond_topology_is_acyclic() {
    let (client, _container) = setup_db().await;

    // Insert diamond edges: A→B, A→C, B→D, C→D.
    client
        .execute(
            "SELECT tide.relay_pipeline_dep_add('diamond-a', 'diamond-b', 'on_idle')",
            &[],
        )
        .await
        .expect("A→B");
    client
        .execute(
            "SELECT tide.relay_pipeline_dep_add('diamond-a', 'diamond-c', 'on_idle')",
            &[],
        )
        .await
        .expect("A→C");
    client
        .execute(
            "SELECT tide.relay_pipeline_dep_add('diamond-b', 'diamond-d', 'on_idle')",
            &[],
        )
        .await
        .expect("B→D");
    client
        .execute(
            "SELECT tide.relay_pipeline_dep_add('diamond-c', 'diamond-d', 'on_idle')",
            &[],
        )
        .await
        .expect("C→D");

    // The graph must be acyclic.
    let cycles = client
        .query("SELECT cycle_path FROM tide.relay_dag_check()", &[])
        .await
        .expect("relay_dag_check");
    assert!(
        cycles.is_empty(),
        "diamond topology A→B, A→C, B→D, C→D must be acyclic; got: {:?}",
        cycles
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect::<Vec<_>>()
    );

    // Verify exactly 4 edges were inserted.
    let edge_count: i64 = client
        .query_one("SELECT COUNT(*) FROM tide.relay_pipeline_deps", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(edge_count, 4, "diamond should have exactly 4 edges");
}

// ── Fan-out topology ──────────────────────────────────────────────────────────
//
// A → B, A → C, A → D  (three independent downstream branches)
//
// Verifies that a single upstream pipeline can have multiple downstream
// dependents without triggering cycle detection.

#[tokio::test]
async fn test_dag_fan_out_topology_is_acyclic() {
    let (client, _container) = setup_db().await;

    client
        .execute(
            "SELECT tide.relay_pipeline_dep_add('fanout-a', 'fanout-b', 'on_idle')",
            &[],
        )
        .await
        .expect("A→B");
    client
        .execute(
            "SELECT tide.relay_pipeline_dep_add('fanout-a', 'fanout-c', 'on_idle')",
            &[],
        )
        .await
        .expect("A→C");
    client
        .execute(
            "SELECT tide.relay_pipeline_dep_add('fanout-a', 'fanout-d', 'always')",
            &[],
        )
        .await
        .expect("A→D (always)");

    let cycles = client
        .query("SELECT cycle_path FROM tide.relay_dag_check()", &[])
        .await
        .expect("relay_dag_check");
    assert!(cycles.is_empty(), "fan-out A→B, A→C, A→D must be acyclic");

    let edge_count: i64 = client
        .query_one("SELECT COUNT(*) FROM tide.relay_pipeline_deps", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(edge_count, 3, "fan-out should have exactly 3 edges");
}

// ── Multi-level mixed trigger policies ────────────────────────────────────────
//
// A →(on_idle) B →(on_offset_gte(500)) C
//
// Tests that the DAG accepts a chain where different edges use different
// trigger policies.

#[tokio::test]
async fn test_dag_mixed_trigger_policies_are_accepted() {
    let (client, _container) = setup_db().await;

    // A→B with on_idle policy.
    client
        .execute(
            "SELECT tide.relay_pipeline_dep_add('mixed-a', 'mixed-b', 'on_idle')",
            &[],
        )
        .await
        .expect("A→B on_idle");

    // B→C with on_offset_gte(N) policy.
    client
        .execute(
            "SELECT tide.relay_pipeline_dep_add('mixed-b', 'mixed-c', 'on_offset_gte(500)')",
            &[],
        )
        .await
        .expect("B→C on_offset_gte(500)");

    let cycles = client
        .query("SELECT cycle_path FROM tide.relay_dag_check()", &[])
        .await
        .expect("relay_dag_check");
    assert!(
        cycles.is_empty(),
        "mixed-policy chain A→B→C must be acyclic"
    );

    // Verify the trigger_policy values were stored correctly.
    let policies: Vec<String> = client
        .query(
            "SELECT trigger_policy FROM tide.relay_pipeline_deps ORDER BY downstream_pipeline",
            &[],
        )
        .await
        .expect("select policies")
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    assert_eq!(
        policies,
        vec!["on_idle", "on_offset_gte(500)"],
        "trigger policies should be stored as supplied"
    );
}

// ── Cycle detection: self-loop ────────────────────────────────────────────────
//
// A→A must be rejected immediately.

#[tokio::test]
async fn test_dag_self_loop_is_rejected() {
    let (client, _container) = setup_db().await;

    let result = client
        .execute(
            "SELECT tide.relay_pipeline_dep_add('self-loop', 'self-loop', 'always')",
            &[],
        )
        .await;

    assert!(
        result.is_err(),
        "self-loop A→A must be rejected by relay_pipeline_dep_add"
    );

    // Graph must still be clean.
    let edge_count: i64 = client
        .query_one("SELECT COUNT(*) FROM tide.relay_pipeline_deps", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(
        edge_count, 0,
        "no edges should exist after rejected self-loop"
    );
}

// ── Cycle detection: two-node cycle ──────────────────────────────────────────
//
// A→B, then B→A must be rejected.

#[tokio::test]
async fn test_dag_two_node_cycle_is_rejected() {
    let (client, _container) = setup_db().await;

    // A→B succeeds.
    client
        .execute(
            "SELECT tide.relay_pipeline_dep_add('two-cycle-a', 'two-cycle-b', 'always')",
            &[],
        )
        .await
        .expect("A→B");

    // B→A must fail (closes the cycle).
    let result = client
        .execute(
            "SELECT tide.relay_pipeline_dep_add('two-cycle-b', 'two-cycle-a', 'always')",
            &[],
        )
        .await;

    assert!(
        result.is_err(),
        "B→A in A→B chain should be rejected (two-node cycle)"
    );

    // Graph must still be clean (only 1 edge: A→B).
    let edge_count: i64 = client
        .query_one("SELECT COUNT(*) FROM tide.relay_pipeline_deps", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(edge_count, 1, "only the A→B edge should remain");

    let cycles = client
        .query("SELECT cycle_path FROM tide.relay_dag_check()", &[])
        .await
        .expect("dag_check");
    assert!(
        cycles.is_empty(),
        "graph must remain acyclic after rejected B→A"
    );
}

// ── Cycle detection: diamond with closing back-edge ──────────────────────────
//
// A→B, A→C, B→D, C→D, D→A must be rejected (diamond + back-edge creates cycle).

#[tokio::test]
async fn test_dag_diamond_back_edge_is_rejected() {
    let (client, _container) = setup_db().await;

    // Build valid diamond: A→B, A→C, B→D, C→D.
    for (up, down) in [
        ("dback-a", "dback-b"),
        ("dback-a", "dback-c"),
        ("dback-b", "dback-d"),
        ("dback-c", "dback-d"),
    ] {
        client
            .execute(
                &format!("SELECT tide.relay_pipeline_dep_add('{up}', '{down}', 'always')"),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("add {up}→{down}: {e}"));
    }

    // Closing back-edge D→A must be rejected.
    let result = client
        .execute(
            "SELECT tide.relay_pipeline_dep_add('dback-d', 'dback-a', 'always')",
            &[],
        )
        .await;

    assert!(
        result.is_err(),
        "D→A closing back-edge in diamond should be rejected"
    );

    // Graph must have exactly 4 edges (the valid diamond).
    let edge_count: i64 = client
        .query_one("SELECT COUNT(*) FROM tide.relay_pipeline_deps", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(edge_count, 4, "should still have exactly 4 diamond edges");

    let cycles = client
        .query("SELECT cycle_path FROM tide.relay_dag_check()", &[])
        .await
        .expect("dag_check");
    assert!(
        cycles.is_empty(),
        "diamond graph must remain acyclic after rejected back-edge"
    );
}
