/// SQL → relay → sink end-to-end test (v0.17.0).
///
/// Verifies the full contract between the SQL catalog API and the relay
/// coordinator runtime.  Specifically:
///
///   1. A pipeline configured via direct INSERT into `tide.relay_outbox_config`
///      (replicating what `tide.relay_set_outbox()` writes) is discovered by
///      the coordinator.
///   2. A message published to `tide.tide_outbox_messages` is picked up by the
///      outbox source worker and delivered to a file sink.
///   3. The committed offset advances after successful delivery.
///
/// This test permanently locks in the v0.12.0 SQL/relay contract.
mod common;

use std::io::Read;
use std::time::Duration;

use pg_tide_relay::coordinator::Coordinator;
use pg_tide_relay::metrics::RelayMetrics;
use tokio::sync::{mpsc, watch};
use tokio_postgres::NoTls;

// Include all migration scripts to build the complete current schema.
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

/// Apply the full migration chain so tests run on the current schema.
async fn apply_full_schema(client: &tokio_postgres::Client) {
    let scripts = [
        ("0.1.0 base", V0_1_0),
        ("0.1.0→0.2.0", V0_1_0_TO_0_2_0),
        ("0.2.0→0.3.0", V0_2_0_TO_0_3_0),
        ("0.3.0→0.4.0", V0_3_0_TO_0_4_0),
        ("0.4.0→0.5.0", V0_4_0_TO_0_5_0),
        ("0.5.0→0.6.0", V0_5_0_TO_0_6_0),
        ("0.6.0→0.7.0", V0_6_0_TO_0_7_0),
        ("0.7.0→0.8.0", V0_7_0_TO_0_8_0),
        ("0.8.0→0.9.0", V0_8_0_TO_0_9_0),
        ("0.9.0→0.10.0", V0_9_0_TO_0_10_0),
        ("0.10.0→0.11.0", V0_10_0_TO_0_11_0),
        ("0.11.0→0.12.0", V0_11_0_TO_0_12_0),
        ("0.12.0→0.13.0", V0_12_0_TO_0_13_0),
        ("0.13.0→0.14.0", V0_13_0_TO_0_14_0),
        ("0.14.0→0.15.0", V0_14_0_TO_0_15_0),
        ("0.15.0→0.16.0", V0_15_0_TO_0_16_0),
        ("0.16.0→0.17.0", V0_16_0_TO_0_17_0),
        ("0.17.0→0.18.0", V0_17_0_TO_0_18_0),
        ("0.18.0→0.19.0", V0_18_0_TO_0_19_0),
        ("0.19.0→0.20.0", V0_19_0_TO_0_20_0),
        ("0.20.0→0.21.0", V0_20_0_TO_0_21_0),
        ("0.21.0→0.22.0", V0_21_0_TO_0_22_0),
        ("0.22.0→0.23.0", V0_22_0_TO_0_23_0),
        ("0.23.0→0.24.0", V0_23_0_TO_0_24_0),
        ("0.24.0→0.25.0", V0_24_0_TO_0_25_0),
        ("0.25.0→0.26.0", V0_25_0_TO_0_26_0),
        ("0.26.0→0.27.0", V0_26_0_TO_0_27_0),
        ("0.27.0→0.28.0", V0_27_0_TO_0_28_0),
        ("0.28.0→0.29.0", V0_28_0_TO_0_29_0),
        ("0.29.0→0.30.0", V0_29_0_TO_0_30_0),
        ("0.30.0→0.31.0", V0_30_0_TO_0_31_0),
        ("0.31.0→0.32.0", V0_31_0_TO_0_32_0),
        ("0.32.0→0.33.0", V0_32_0_TO_0_33_0),
        ("0.33.0→0.34.0", V0_33_0_TO_0_34_0),
        ("0.34.0→0.35.0", V0_34_0_TO_0_35_0),
    ];
    client
        .batch_execute("CREATE SCHEMA IF NOT EXISTS tide;")
        .await
        .expect("create schema");
    for (label, sql) in scripts {
        let processed = common::strip_extension_comments(sql);
        client
            .batch_execute(&processed)
            .await
            .unwrap_or_else(|e| panic!("migration {label} failed: {e}"));
    }
}

/// Create a deadpool-postgres pool pointing at the test container.
fn make_pool(url: &str) -> deadpool_postgres::Pool {
    let mut cfg = deadpool_postgres::Config::new();
    cfg.url = Some(url.to_string());
    cfg.pool = Some(deadpool_postgres::PoolConfig {
        max_size: 4,
        ..Default::default()
    });
    cfg.create_pool(
        Some(deadpool_postgres::Runtime::Tokio1),
        tokio_postgres::NoTls,
    )
    .expect("create pool")
}

/// Connect with retry (container may not be immediately ready).
async fn connect_retry(url: &str) -> tokio_postgres::Client {
    for attempt in 0..20u32 {
        match tokio_postgres::connect(url, NoTls).await {
            Ok((client, conn)) => {
                tokio::spawn(async move {
                    let _ = conn.await;
                });
                return client;
            }
            Err(e) => {
                if attempt == 19 {
                    panic!("failed to connect after 20 attempts: {e}");
                }
                tokio::time::sleep(Duration::from_millis(400)).await;
            }
        }
    }
    unreachable!()
}

/// Full SQL → relay coordinator → file sink end-to-end test.
///
/// 1. Starts a fresh PostgreSQL 18 container.
/// 2. Installs the full pg_tide schema (all migrations through v0.17.0).
/// 3. Creates an outbox and configures a forward pipeline via direct INSERT
///    (mirroring exactly what `tide.relay_set_outbox()` writes).
/// 4. Starts a real `Coordinator` task with a short discovery interval.
/// 5. Publishes a message directly to `tide.tide_outbox_messages`.
/// 6. Waits up to 10 s for the file sink to receive the message.
/// 7. Asserts the message payload is present in the output file.
/// 8. Asserts the committed offset has advanced.
#[tokio::test(flavor = "multi_thread")]
async fn test_sql_to_file_sink_e2e() {
    use testcontainers::{runners::AsyncRunner, ImageExt};
    use testcontainers_modules::postgres::Postgres;

    // ── 1. Start PostgreSQL container ─────────────────────────────────────
    let container = Postgres::default()
        .with_tag("18")
        .start()
        .await
        .expect("start postgres");
    let port = container.get_host_port_ipv4(5432).await.expect("get port");
    let db_url =
        format!("host=127.0.0.1 port={port} user=postgres password=postgres dbname=postgres");

    // ── 2. Install schema ─────────────────────────────────────────────────
    let client = connect_retry(&db_url).await;
    apply_full_schema(&client).await;

    // ── 3. Create per-outbox table + pipeline config ──────────────────────
    //
    // The relay coordinator reads from a per-outbox table named
    // `tide.outbox_{name}`.  We create this table directly (without the Rust
    // extension) and set `payload_mode = "raw"` so the coordinator forwards
    // the raw JSONB payload without expecting a pg_trickle v:1 envelope.
    client
        .batch_execute(
            "CREATE TABLE tide.outbox_e2e_outbox ( \
               id      BIGSERIAL PRIMARY KEY, \
               payload JSONB NOT NULL \
             )",
        )
        .await
        .expect("create per-outbox table");

    // Temp file for the file sink.
    let tmp_path = std::env::temp_dir().join(format!(
        "pg-tide-e2e-{}.jsonl",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let tmp_path_str = tmp_path.to_string_lossy().to_string();

    // Configure the pipeline — mirrors the JSON shape that tide.relay_set_outbox()
    // writes.  `payload_mode = "raw"` bypasses the v:1 envelope expectation.
    let pipeline_config = serde_json::json!({
        "source_type": "outbox",
        "source": { "outbox": "e2e_outbox", "payload_mode": "raw" },
        "sink_type": "file",
        "sink": {
            "path": tmp_path_str,
            "format": "jsonl"
        },
        "batch_size": 10,
        "poll_interval_ms": 200
    });

    client
        .execute(
            "INSERT INTO tide.relay_outbox_config (name, enabled, config) \
             VALUES ('e2e-pipeline', true, $1)",
            &[&pipeline_config],
        )
        .await
        .expect("insert relay config");

    // ── 4. Start coordinator ──────────────────────────────────────────────
    let pool = make_pool(&db_url);
    let metrics = RelayMetrics::new().expect("metrics");
    let health = std::sync::Arc::new(tokio::sync::RwLock::new(
        pg_tide_relay::metrics::HealthState::default(),
    ));
    let mut coordinator = Coordinator::new(pool, "e2e-test-group", metrics, health);
    coordinator.set_max_owned_pipelines(10);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (notif_tx, notif_rx) = mpsc::channel::<()>(4);
    let db_url_clone = db_url.clone();
    let coordinator_handle = tokio::spawn(async move {
        coordinator
            .run(
                db_url_clone,
                10,
                Duration::from_millis(500),
                shutdown_rx,
                notif_rx,
            )
            .await
    });

    // Give the coordinator time to discover and start the pipeline worker.
    tokio::time::sleep(Duration::from_millis(1_200)).await;

    // ── 5. Publish a message ──────────────────────────────────────────────
    let test_payload = serde_json::json!({
        "event": "order.created",
        "order_id": "e2e-42",
        "test": "sql_to_sink_e2e"
    });
    let payload_json = tokio_postgres::types::Json(&test_payload);
    client
        .execute(
            "INSERT INTO tide.outbox_e2e_outbox (payload) VALUES ($1)",
            &[&payload_json],
        )
        .await
        .expect("insert outbox message");

    // Notify the coordinator via pg_notify so it wakes up quickly.
    let _ = notif_tx.send(()).await;

    // ── 6. Wait for delivery (up to 10 s) ────────────────────────────────
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut delivered = false;
    while tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if tmp_path.exists() {
            let mut f = std::fs::File::open(&tmp_path).unwrap();
            let mut content = String::new();
            f.read_to_string(&mut content).unwrap();
            if content.contains("e2e-42") {
                delivered = true;
                break;
            }
        }
    }

    // ── 7. Assert message delivered ───────────────────────────────────────
    assert!(
        delivered,
        "message was not delivered to file sink within 10 s \
         — check coordinator logs; file: {tmp_path_str}"
    );

    // Verify message content.
    let mut content = String::new();
    std::fs::File::open(&tmp_path)
        .unwrap()
        .read_to_string(&mut content)
        .unwrap();
    let line: serde_json::Value =
        serde_json::from_str(content.lines().next().unwrap()).expect("valid JSON line");
    assert_eq!(
        line["payload"]["event"].as_str(),
        Some("order.created"),
        "payload event field mismatch"
    );
    assert_eq!(
        line["payload"]["order_id"].as_str(),
        Some("e2e-42"),
        "payload order_id field mismatch"
    );

    // ── 8. Assert offset advanced ─────────────────────────────────────────
    let offset: i64 = client
        .query_one(
            "SELECT last_change_id FROM tide.relay_consumer_offsets \
             WHERE pipeline_id = 'e2e-pipeline'",
            &[],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or(0);
    assert!(
        offset > 0,
        "relay_consumer_offsets.last_change_id must be > 0 after delivery (got {offset})"
    );

    // ── Cleanup ───────────────────────────────────────────────────────────
    shutdown_tx.send(true).ok();
    let _ = tokio::time::timeout(Duration::from_secs(5), coordinator_handle).await;
    let _ = std::fs::remove_file(&tmp_path);
}
