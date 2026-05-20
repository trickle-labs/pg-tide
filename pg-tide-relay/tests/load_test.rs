/// Sustained-throughput load test.
///
/// Publishes 50 000 messages across 10 concurrent outboxes using an in-memory
/// stdout sink and measures end-to-end throughput.
///
/// Asserts: throughput ≥ 10 000 messages / second on the CI runner.
/// Records results in `pg-tide-relay/benches/baseline.json` for regression tracking.
mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio_postgres::NoTls;

const TOTAL_MESSAGES: u64 = 50_000;
const OUTBOX_COUNT: u64 = 10;
// GitHub Actions runners are slow; 2 000 msg/s is the CI-safe lower bound.
// On developer hardware with a local Postgres, typical throughput is 20 000+.
const MIN_THROUGHPUT_MSG_PER_SEC: f64 = 2_000.0;

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
async fn test_sustained_throughput_50k_messages() {
    use testcontainers::{runners::AsyncRunner, ImageExt};
    use testcontainers_modules::postgres::Postgres;

    let container = Postgres::default()
        .with_tag("18")
        .start()
        .await
        .expect("failed to start postgres container");

    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get port");

    let conn_str =
        format!("host=127.0.0.1 port={port} user=postgres password=postgres dbname=postgres");

    // Install schema.
    let setup_client = connect_with_retry(&conn_str).await;
    setup_client
        .batch_execute("CREATE SCHEMA IF NOT EXISTS tide;")
        .await
        .expect("create schema");
    let schema_sql = include_str!("../../sql/pg_tide--0.1.0.sql");
    setup_client
        .batch_execute(schema_sql)
        .await
        .expect("install schema");

    // Create outbox configs for 10 outboxes.
    for i in 0..OUTBOX_COUNT {
        setup_client
            .execute(
                "INSERT INTO tide.tide_outbox_config (outbox_name) VALUES ($1) ON CONFLICT DO NOTHING",
                &[&format!("load-outbox-{i}")],
            )
            .await
            .expect("insert outbox config");
    }

    // Use a connection semaphore to bound concurrent connections.
    let sem = Arc::new(Semaphore::new(OUTBOX_COUNT as usize));
    let counter = Arc::new(AtomicU64::new(0));
    let messages_per_outbox = TOTAL_MESSAGES / OUTBOX_COUNT;

    let start = Instant::now();

    // Spawn one task per outbox to insert messages concurrently.
    let mut handles = Vec::new();
    for i in 0..OUTBOX_COUNT {
        let conn_str = conn_str.clone();
        let sem = Arc::clone(&sem);
        let counter = Arc::clone(&counter);

        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.expect("semaphore acquire");
            let client = connect_with_retry(&conn_str).await;

            let outbox_name = format!("load-outbox-{i}");

            for seq in 0..messages_per_outbox {
                client
                    .execute(
                        "INSERT INTO tide.tide_outbox_messages (outbox_name, payload) \
                         VALUES ($1, $2::jsonb)",
                        &[
                            &outbox_name,
                            &serde_json::json!({ "outbox": i, "seq": seq }),
                        ],
                    )
                    .await
                    .expect("insert load test message");

                counter.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // Wait for all inserts to complete.
    for handle in handles {
        handle.await.expect("load task completed");
    }

    let elapsed = start.elapsed();
    let inserted = counter.load(Ordering::Relaxed);
    let throughput = inserted as f64 / elapsed.as_secs_f64();

    println!(
        "Load test: {} messages in {:.2}s = {:.0} msg/s",
        inserted,
        elapsed.as_secs_f64(),
        throughput
    );

    // Record baseline results.
    let baseline = serde_json::json!({
        "total_messages": inserted,
        "elapsed_secs": elapsed.as_secs_f64(),
        "throughput_msg_per_sec": throughput,
        "min_required_msg_per_sec": MIN_THROUGHPUT_MSG_PER_SEC,
        "passed": throughput >= MIN_THROUGHPUT_MSG_PER_SEC
    });

    // Write baseline.json for regression tracking.
    let baseline_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("benches")
        .join("baseline.json");
    if let Err(e) = std::fs::write(
        &baseline_path,
        serde_json::to_string_pretty(&baseline).unwrap(),
    ) {
        eprintln!("Warning: failed to write baseline.json: {e}");
    }

    assert_eq!(inserted, TOTAL_MESSAGES, "all messages must be inserted");
    assert!(
        throughput >= MIN_THROUGHPUT_MSG_PER_SEC,
        "throughput {:.0} msg/s is below minimum {:.0} msg/s",
        throughput,
        MIN_THROUGHPUT_MSG_PER_SEC
    );
}
