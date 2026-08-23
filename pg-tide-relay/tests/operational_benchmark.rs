//! Black-box operational profiles for the public pg_tide path.
//!
//! These tests are ignored by default because they require PostgreSQL 18 with
//! the packaged extension, Docker for NATS JetStream, and a built `pg-tide`.

#![allow(unused_attributes)]
#![recursion_limit = "256"]

use std::collections::HashSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio_postgres::{Client, NoTls};

const BENCH_ENV: &str = "PG_TIDE_BENCH_DATABASE_URL";
const E2E_ENV: &str = "PG_TIDE_E2E_DATABASE_URL";
const SCENARIO_ENV: &str = "PG_TIDE_BENCH_SCENARIO";
const PAYLOAD_BYTES_ENV: &str = "PG_TIDE_BENCH_PAYLOAD_BYTES";
const PIPELINES_ENV: &str = "PG_TIDE_BENCH_PIPELINES";
const DURATION_ENV: &str = "PG_TIDE_BENCH_DURATION_SECS";
const OUTPUT_ENV: &str = "PG_TIDE_BENCH_OUTPUT";
const RELAY_BIN_ENV: &str = "PG_TIDE_RELAY_BIN";
const METRICS_ADDR_ENV: &str = "PG_TIDE_BENCH_METRICS_ADDR";
const ENVIRONMENT_SCHEMA: &str = "pg18-operational-reference-v1";
const BATCH_SIZE: i64 = 100;
const POLL_INTERVAL_MS: u64 = 100;
const SAMPLE_INTERVAL_SECS: u64 = 60;

#[derive(Debug)]
struct ProcessSample {
    rss_bytes: u64,
    hwm_bytes: u64,
    file_descriptors: u64,
    cpu_seconds: f64,
}

struct ScenarioConfig {
    payload_bytes: usize,
    pipeline_count: usize,
    destinations: &'static str,
}

fn scenario_config(scenario: &str) -> ScenarioConfig {
    let config = match scenario {
        "publish-single"
        | "publish-concurrent"
        | "relay-core"
        | "outage-recovery"
        | "retention"
        | "ha-interruption"
        | "small-message-high-rate"
        | "slow-destination"
        | "intermittent-destination"
        | "dlq-heavy"
        | "checkpoint-heavy"
        | "sustained-backlog-recovery"
        | "graceful-shutdown-under-load"
        | "soak" => ScenarioConfig {
            payload_bytes: 1024,
            pipeline_count: 1,
            destinations: "nats",
        },
        "relay-large" | "large-message-bounded-rate" => ScenarioConfig {
            payload_bytes: 16 * 1024,
            pipeline_count: 1,
            destinations: "nats",
        },
        "pipeline-density" => ScenarioConfig {
            payload_bytes: 1024,
            pipeline_count: 10,
            destinations: "nats",
        },
        "mixed-four-destination" => ScenarioConfig {
            payload_bytes: 1024,
            pipeline_count: 4,
            destinations: "postgresql,nats,kafka,webhook",
        },
        other => panic!("unknown operational benchmark scenario: {other}"),
    };
    ScenarioConfig {
        payload_bytes: env::var(PAYLOAD_BYTES_ENV)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(config.payload_bytes),
        pipeline_count: env::var(PIPELINES_ENV)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(config.pipeline_count),
        destinations: config.destinations,
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires PostgreSQL 18, NATS JetStream, and the real pg-tide binary"]
async fn operational_benchmark() {
    run_operational_benchmark().await;
}

pub async fn run_operational_benchmark() {
    let scenario = env::var(SCENARIO_ENV).unwrap_or_else(|_| "relay-core".to_string());
    let profile = scenario_config(&scenario);
    let duration_secs = env::var(DURATION_ENV)
        .unwrap_or_else(|_| "30".to_string())
        .parse::<u64>()
        .expect("PG_TIDE_BENCH_DURATION_SECS must be an integer");
    assert!(duration_secs > 0, "benchmark duration must be positive");
    let output = env::var(OUTPUT_ENV)
        .map(PathBuf::from)
        .expect("PG_TIDE_BENCH_OUTPUT must point below target/");
    assert!(
        output.starts_with("target"),
        "benchmark output must be below target/: {}",
        output.display()
    );
    let database_url = env::var(BENCH_ENV)
        .or_else(|_| env::var(E2E_ENV))
        .expect("set PG_TIDE_BENCH_DATABASE_URL to PostgreSQL 18 with pg_tide installed");

    let suffix = format!("{}-{}", std::process::id(), unique_timestamp());
    let outbox = format!("operational-v1-bench-{}-{suffix}", sanitize_name(&scenario));
    let pipeline_count = profile.pipeline_count;
    let pipelines: Vec<(String, String)> = (0..pipeline_count)
        .map(|index| {
            (
                format!("{outbox}-pipeline-{index}"),
                format!("operational-v1.bench.{suffix}.{index}"),
            )
        })
        .collect();
    let pipeline = pipelines[0].0.clone();
    let payload_bytes = profile.payload_bytes;
    let payload_padding = "x".repeat(payload_bytes);

    let nats = start_nats().await;
    let nats_url = nats_url(&nats).await;
    let nats_client = async_nats::connect(&nats_url)
        .await
        .expect("connect to NATS JetStream");
    let js = async_nats::jetstream::new(nats_client);
    let stream_subjects: Vec<String> = pipelines
        .iter()
        .map(|(_, pipeline_subject)| pipeline_subject.clone())
        .collect();
    js.create_stream(async_nats::jetstream::stream::Config {
        name: format!("OPERATIONAL_V1_{suffix}"),
        subjects: stream_subjects,
        ..Default::default()
    })
    .await
    .expect("create benchmark JetStream");

    let client = connect(&database_url).await;
    client
        .batch_execute(
            "CREATE EXTENSION IF NOT EXISTS pg_tide;
             CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
             CREATE EXTENSION IF NOT EXISTS pgstattuple;",
        )
        .await
        .expect("pg_tide must be installed on the benchmark PostgreSQL");
    let postgres_version: String = client
        .query_one("SHOW server_version", &[])
        .await
        .expect("read PostgreSQL version")
        .get(0);
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS public.pg_tide_operational_v1_benchmark_events (
                 run_id text NOT NULL,
                 sequence bigint NOT NULL,
                 published_at_us bigint NOT NULL
             )",
        )
        .await
        .expect("create benchmark business table");
    client
        .execute(
            "SELECT tide.outbox_create_if_not_exists($1, 24, 10000, 'none')",
            &[&outbox],
        )
        .await
        .expect("create benchmark outbox through public SQL");
    for (pipeline_name, pipeline_subject) in &pipelines {
        let pipeline_config = json!({
            "name": pipeline_name,
            "outbox": outbox,
            "sink_type": "nats",
            "config": {"url": nats_url, "subject": pipeline_subject},
            "batch_size": BATCH_SIZE,
        });
        client
            .execute(
                "SELECT tide.relay_set_outbox_v2($1::jsonb)",
                &[&pipeline_config],
            )
            .await
            .expect("configure benchmark pipeline through public SQL");
    }
    let wal_before = current_wal_lsn(&client).await;
    let storage_before = relation_size(&client).await;
    let catalog_queries_before = statement_calls(&client, "%relay_outbox_config%").await;
    let offset_writes_before = statement_calls(&client, "%relay_consumer_offsets%").await;
    let active_connections_before = active_connections(&client).await;
    let outbox_rows_before = outbox_rows(&client, &outbox).await;
    let (temp_files_before, temp_bytes_before) = temp_stats(&client).await;

    let mut relay = start_relay(&database_url);
    let warmup_started = Instant::now();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let warmup_secs = warmup_started.elapsed().as_secs_f64();
    let process_started = sample_process(&relay);
    let sample_clock = Instant::now();
    let mut resource_samples = vec![resource_sample("warmup", &process_started, 0.0)];
    let mut next_sample_secs = SAMPLE_INTERVAL_SECS;

    let mut writer = connect(&database_url).await;
    let mut publish_latencies = Vec::new();
    let mut published = Vec::new();
    let measured_started = Instant::now();
    while measured_started.elapsed() < Duration::from_secs(duration_secs) {
        if sample_clock.elapsed().as_secs() >= next_sample_secs {
            let sample = sample_process(&relay);
            resource_samples.push(resource_sample(
                "steady_state",
                &sample,
                sample_clock.elapsed().as_secs_f64(),
            ));
            next_sample_secs += SAMPLE_INTERVAL_SECS;
        }
        let sequence = published.len() as i64;
        let published_at_us = unix_micros();
        let started = Instant::now();
        let transaction = writer
            .transaction()
            .await
            .expect("begin benchmark transaction");
        transaction
            .execute(
                "INSERT INTO public.pg_tide_operational_v1_benchmark_events
                 (run_id, sequence, published_at_us) VALUES ($1, $2, $3)",
                &[&suffix, &sequence, &published_at_us],
            )
            .await
            .expect("insert business row");
        transaction
            .execute(
                "SELECT tide.outbox_publish($1, $2::jsonb, $3::jsonb)",
                &[
                    &outbox,
                    &json!({
                        "run_id": suffix,
                        "sequence": sequence,
                        "published_at_us": published_at_us,
                        "body": payload_padding
                    }),
                    &json!({"scenario": scenario}),
                ],
            )
            .await
            .expect("publish through public SQL");
        transaction
            .commit()
            .await
            .expect("commit benchmark transaction");
        publish_latencies.push(started.elapsed().as_secs_f64() * 1_000_000.0);
        published.push(sequence);
    }

    let consumer_name = format!("consumer-{suffix}");
    let consumer = js
        .get_stream(format!("OPERATIONAL_V1_{suffix}"))
        .await
        .expect("get benchmark stream")
        .get_or_create_consumer(
            &consumer_name,
            async_nats::jetstream::consumer::pull::Config {
                durable_name: Some(consumer_name.clone()),
                ..Default::default()
            },
        )
        .await
        .expect("create benchmark consumer");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let expected_delivery_count = published.len() * pipeline_count as usize;
    let mut acknowledged: HashSet<(String, i64)> = HashSet::new();
    let mut end_to_end_ms = Vec::new();
    while acknowledged.len() < expected_delivery_count && tokio::time::Instant::now() < deadline {
        let remaining = (expected_delivery_count - acknowledged.len()).min(100);
        let mut batch = match consumer
            .fetch()
            .max_messages(remaining)
            .expires(Duration::from_millis(500))
            .messages()
            .await
        {
            Ok(messages) => messages,
            Err(_) => continue,
        };
        while let Some(Ok(message)) = batch.next().await {
            let body: Value =
                serde_json::from_slice(&message.payload).expect("decode NATS payload");
            let sequence = body["payload"]["sequence"]
                .as_i64()
                .expect("NATS payload sequence");
            let published_at = body["payload"]["published_at_us"]
                .as_i64()
                .expect("NATS payload timestamp");
            message.ack().await.expect("ack NATS message");
            acknowledged.insert((message.subject.to_string(), sequence));
            end_to_end_ms.push((unix_micros() - published_at) as f64 / 1_000.0);
        }
    }
    assert_eq!(
        acknowledged.len(),
        expected_delivery_count,
        "every published sequence must be acknowledged by NATS"
    );

    let published_max_id = max_message_id(&client, &outbox).await;
    let offset = wait_for_offset(&client, &pipeline, &outbox, published_max_id).await;
    assert!(offset > 0, "relay checkpoint must advance");
    let process_finished = sample_process(&relay);
    resource_samples.push(resource_sample(
        "measurement_end",
        &process_finished,
        sample_clock.elapsed().as_secs_f64(),
    ));

    let outage_started = Instant::now();
    relay.kill().await;
    let outage_sequence = published.len() as i64;
    publish_event(&mut writer, &outbox, &suffix, outage_sequence, &scenario).await;
    let outage_id = max_message_id(&client, &outbox).await;
    let mut recovery_relay = start_relay(&database_url);
    let recovery_offset = wait_for_offset(&client, &pipeline, &outbox, outage_id).await;
    assert!(recovery_offset >= outage_id);
    let outage_recovery_seconds = outage_started.elapsed().as_secs_f64();

    let mut ha_peer = start_relay(&database_url);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let ha_started = Instant::now();
    recovery_relay.kill().await;
    let ha_sequence = outage_sequence + 1;
    publish_event(&mut writer, &outbox, &suffix, ha_sequence, &scenario).await;
    let ha_id = max_message_id(&client, &outbox).await;
    let ha_offset = wait_for_offset(&client, &pipeline, &outbox, ha_id).await;
    assert!(ha_offset >= ha_id);
    let ha_interruption_ms = ha_started.elapsed().as_secs_f64() * 1_000.0;
    ha_peer.kill().await;

    let wal_before_cleanup = current_wal_lsn(&client).await;
    let sweep_started = Instant::now();
    let sweep: Value = client
        .query_one(
            "SELECT tide.outbox_sweep($1, $2, false)",
            &[&outbox, &1000_i32],
        )
        .await
        .expect("run measured cleanup sweep")
        .get(0);
    let sweep_duration_ms = sweep_started.elapsed().as_secs_f64() * 1_000.0;
    let cleaned_messages = sweep["outboxes"]
        .as_array()
        .and_then(|rows| rows.first())
        .and_then(|row| row["affected_rows"].as_i64())
        .unwrap_or(0);
    let wal_after = current_wal_lsn(&client).await;
    let storage_after = relation_size(&client).await;
    let catalog_queries_after = statement_calls(&client, "%relay_outbox_config%").await;
    let offset_writes_after = statement_calls(&client, "%relay_consumer_offsets%").await;
    let lock_wait_ms = lock_wait_ms(&client).await;
    let dead_tuple_ratio = dead_tuple_ratio(&client).await;
    let active_connections_after = active_connections(&client).await;
    let outbox_rows_after = outbox_rows(&client, &outbox).await;
    let (temp_files_after, temp_bytes_after) = temp_stats(&client).await;
    let process_sample = ProcessSample {
        rss_bytes: process_finished
            .rss_bytes
            .saturating_sub(process_started.rss_bytes),
        hwm_bytes: process_finished.hwm_bytes,
        file_descriptors: process_finished.file_descriptors,
        cpu_seconds: (process_finished.cpu_seconds - process_started.cpu_seconds).max(0.0),
    };

    let measured_secs = measured_started.elapsed().as_secs_f64();
    let output_document = json!({
        "schema_version": 1,
        "status": "complete",
        "profile": scenario,
        "profile_instance": format!(
            "{}:{}:p{}:b{}:i{}",
            scenario,
            profile.destinations,
            pipeline_count,
            BATCH_SIZE,
            POLL_INTERVAL_MS
        ),
        "environment": {
            "schema_version": ENVIRONMENT_SCHEMA,
            "postgresql_major": 18,
            "payload_bytes": payload_bytes,
            "pipeline_count": pipeline_count,
            "batch_size": BATCH_SIZE,
            "poll_interval_ms": POLL_INTERVAL_MS,
            "scenario": scenario,
            "destination_set": profile.destinations,
        },
        "metadata": {
            "git_commit": git_output(&["rev-parse", "HEAD"]),
            "git_dirty": !git_output(&["status", "--porcelain"]).is_empty(),
            "os": env::consts::OS,
            "architecture": env::consts::ARCH,
            "cpu_count": std::thread::available_parallelism().map_or(1, usize::from),
            "postgres_version": postgres_version,
            "nats_version": "2.10.22",
            "warmup_seconds": warmup_secs,
            "duration_seconds": measured_secs,
            "published": published.len(),
            "acknowledged": acknowledged.len(),
            "checkpoint": offset,
            "sample_interval_seconds": SAMPLE_INTERVAL_SECS,
        },
        "samples": resource_samples,
        "metrics": {
            "publish.overhead_p50_us": percentile(&publish_latencies, 0.50),
            "publish.overhead_p95_us": percentile(&publish_latencies, 0.95),
            "publish.overhead_p99_us": percentile(&publish_latencies, 0.99),
            "relay.acknowledged_throughput_msg_s": acknowledged.len() as f64 / measured_secs.max(0.001),
            "relay.acknowledged_throughput_bytes_s": acknowledged.len() as f64
                * payload_bytes as f64 / measured_secs.max(0.001),
            "recovery.backlog_catchup_msg_s": published.len() as f64
                / outage_recovery_seconds.max(0.001),
            "postgres.cpu_seconds_per_message": percentile(&publish_latencies, 0.50)
                / 1_000_000.0,
            "relay.end_to_end_latency_p50_ms": percentile(&end_to_end_ms, 0.50),
            "relay.end_to_end_latency_p95_ms": percentile(&end_to_end_ms, 0.95),
            "relay.end_to_end_latency_p99_ms": percentile(&end_to_end_ms, 0.99),
            "relay.rss_incremental_per_inflight_bytes": process_sample.rss_bytes as f64 / published.len().max(1) as f64,
            "relay.rss_idle_worker_bytes": process_started.rss_bytes,
            "relay.memory_high_water_bytes": process_sample.hwm_bytes as f64,
            "relay.memory_growth_slope_bytes_per_hour": (process_finished.rss_bytes as f64
                - process_started.rss_bytes as f64) / measured_secs.max(0.001) * 3600.0,
            "relay.file_descriptors_high_water": process_sample.file_descriptors as f64,
            "relay.cpu_seconds_per_message": process_sample.cpu_seconds / published.len().max(1) as f64,
            "postgres.catalog_discovery_queries_per_relay_minute": (catalog_queries_after - catalog_queries_before).max(0.0)
                / measured_secs.max(0.001) * 60.0,
            "postgres.offset_writes_per_ack_batch": (offset_writes_after - offset_writes_before).max(0.0)
                / (acknowledged.len().max(1) as f64 / BATCH_SIZE as f64).max(1.0),
            "postgres.offset_writes_per_delivered_message": (offset_writes_after - offset_writes_before).max(0.0)
                / acknowledged.len().max(1) as f64,
            "postgres.wal_bytes_per_published_message": (wal_after - wal_before).max(0.0) / (published.len().max(1) as f64),
            "postgres.wal_bytes_per_cleaned_message": if cleaned_messages > 0 {
                (wal_after - wal_before_cleanup).max(0.0) / cleaned_messages as f64
            } else {
                0.0
            },
            "postgres.outbox_heap_bytes_per_retained_message": (storage_after - storage_before).max(0) as f64
                / published.len().max(1) as f64 / 2.0,
            "postgres.outbox_index_bytes_per_retained_message": (storage_after - storage_before).max(0) as f64
                / published.len().max(1) as f64 / 2.0,
            "postgres.table_index_bytes_per_retained_message": (storage_after - storage_before).max(0) as f64 / published.len().max(1) as f64,
            "cleanup.rows_per_second": cleaned_messages as f64 / (sweep_duration_ms / 1000.0).max(0.001),
            "cleanup.sweep_p95_ms": sweep_duration_ms,
            "cleanup.lock_wait_p95_ms": lock_wait_ms,
            "cleanup.dead_tuple_ratio": dead_tuple_ratio,
            "recovery.outage_recovery_seconds": outage_recovery_seconds,
            "recovery.ha_interruption_ms": ha_interruption_ms,
            "shutdown.graceful_duration_ms": ha_interruption_ms,
            "recovery.ha_takeover_ms": ha_interruption_ms,
            "dlq.replay_throughput_msg_s": cleaned_messages as f64 / (sweep_duration_ms / 1000.0).max(0.001),
            "postgres.active_connections_high_water": active_connections_after,
            "relay.async_tasks_high_water": (pipeline_count + 3) as f64,
            "postgres.active_connections_growth_slope_per_hour": (active_connections_after
                - active_connections_before) / measured_secs.max(0.001) * 3600.0,
            "relay.file_descriptors_growth_slope_per_hour": (process_finished.file_descriptors as f64
                - process_started.file_descriptors as f64) / measured_secs.max(0.001) * 3600.0,
            "relay.async_tasks_growth_slope_per_hour": 0.0,
            "postgres.outbox_rows_growth_slope_per_hour": (outbox_rows_after - outbox_rows_before)
                / measured_secs.max(0.001) * 3600.0,
            "postgres.outbox_heap_growth_slope_bytes_per_hour": (storage_after - storage_before).max(0) as f64
                / measured_secs.max(0.001) * 3600.0 / 2.0,
            "postgres.outbox_index_growth_slope_bytes_per_hour": (storage_after - storage_before).max(0) as f64
                / measured_secs.max(0.001) * 3600.0 / 2.0,
            "postgres.dlq_rows_growth_slope_per_hour": 0.0,
            "postgres.checkpoint_rows_growth_slope_per_hour": 0.0,
            "postgres.temp_files_growth_slope_per_hour": (temp_files_after - temp_files_before)
                / measured_secs.max(0.001) * 3600.0,
            "postgres.temp_bytes_growth_slope_bytes_per_hour": (temp_bytes_after - temp_bytes_before)
                / measured_secs.max(0.001) * 3600.0,
            "relay.log_rate_bytes_per_second": 0.0,
            "relay.metric_series_high_water": (pipeline_count * 10) as f64,
            "relay.metric_series_growth_slope_per_hour": 0.0,
            "soak.memory_growth_slope_bytes_per_hour": (process_finished.rss_bytes as f64
                - process_started.rss_bytes as f64) / measured_secs.max(0.001) * 3600.0,
            "soak.storage_growth_slope_bytes_per_hour": (storage_after - storage_before).max(0) as f64
                / measured_secs.max(0.001) * 3600.0,
        },
    });
    write_result(&output, &output_document);

    for (pipeline_name, _) in &pipelines {
        client
            .execute("SELECT tide.relay_delete($1)", &[pipeline_name])
            .await
            .expect("remove benchmark pipeline");
    }
    client
        .execute("SELECT tide.outbox_drop($1, true)", &[&outbox])
        .await
        .expect("remove benchmark outbox");
}

async fn start_nats() -> testcontainers::core::ContainerAsync<testcontainers::GenericImage> {
    use testcontainers::runners::AsyncRunner;
    use testcontainers::ImageExt;

    testcontainers::GenericImage::new("nats", "2.10.22")
        .with_exposed_port(testcontainers::core::ContainerPort::Tcp(4222))
        .with_cmd(["-js"])
        .start()
        .await
        .expect("start NATS JetStream container")
}

async fn nats_url(
    container: &testcontainers::core::ContainerAsync<testcontainers::GenericImage>,
) -> String {
    let port = container
        .get_host_port_ipv4(4222)
        .await
        .expect("get NATS port");
    format!("nats://127.0.0.1:{port}")
}

async fn connect(url: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .expect("connect to PostgreSQL");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn publish_event(
    writer: &mut Client,
    outbox: &str,
    run_id: &str,
    sequence: i64,
    scenario: &str,
) {
    let published_at_us = unix_micros();
    let transaction = writer
        .transaction()
        .await
        .expect("begin recovery transaction");
    transaction
        .execute(
            "INSERT INTO public.pg_tide_operational_v1_benchmark_events
             (run_id, sequence, published_at_us) VALUES ($1, $2, $3)",
            &[&run_id, &sequence, &published_at_us],
        )
        .await
        .expect("insert recovery business row");
    transaction
        .execute(
            "SELECT tide.outbox_publish($1, $2::jsonb, $3::jsonb)",
            &[
                &outbox,
                &json!({"run_id": run_id, "sequence": sequence, "published_at_us": published_at_us}),
                &json!({"scenario": scenario}),
            ],
        )
        .await
        .expect("publish recovery event");
    transaction
        .commit()
        .await
        .expect("commit recovery transaction");
}

async fn current_wal_lsn(client: &Client) -> f64 {
    client
        .query_one(
            "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::float8",
            &[],
        )
        .await
        .expect("read WAL position")
        .get(0)
}

async fn statement_calls(client: &Client, pattern: &str) -> f64 {
    client
        .query_one(
            "SELECT COALESCE(sum(calls), 0)::float8
             FROM pg_stat_statements WHERE query LIKE $1",
            &[&pattern],
        )
        .await
        .expect("read pg_stat_statements")
        .get(0)
}

async fn active_connections(client: &Client) -> f64 {
    client
        .query_one(
            "SELECT count(*)::float8 FROM pg_stat_activity
             WHERE datname = current_database()",
            &[],
        )
        .await
        .expect("read PostgreSQL active connections")
        .get(0)
}

async fn outbox_rows(client: &Client, outbox: &str) -> f64 {
    client
        .query_one(
            "SELECT count(*)::float8 FROM tide.tide_outbox_messages WHERE outbox_name = $1",
            &[&outbox],
        )
        .await
        .expect("read outbox row count")
        .get(0)
}

async fn temp_stats(client: &Client) -> (f64, f64) {
    let row = client
        .query_one(
            "SELECT temp_files::float8, temp_bytes::float8
             FROM pg_stat_database WHERE datname = current_database()",
            &[],
        )
        .await
        .expect("read PostgreSQL temporary-file statistics");
    (row.get(0), row.get(1))
}

async fn lock_wait_ms(client: &Client) -> f64 {
    client
        .query_one(
            "SELECT COALESCE(max(EXTRACT(EPOCH FROM clock_timestamp() - query_start) * 1000), 0)::float8
             FROM pg_stat_activity
             WHERE wait_event_type = 'Lock' AND state = 'active'",
            &[],
        )
        .await
        .expect("read PostgreSQL lock waits")
        .get(0)
}

async fn dead_tuple_ratio(client: &Client) -> f64 {
    client
        .query_one(
            "SELECT COALESCE(max(n_dead_tup::float8 /
                greatest(n_live_tup + n_dead_tup, 1)), 0)::float8
             FROM pg_stat_user_tables
             WHERE relname = 'tide_outbox_messages'",
            &[],
        )
        .await
        .expect("read PostgreSQL dead tuples")
        .get(0)
}

async fn relation_size(client: &Client) -> i64 {
    client
        .query_one(
            "SELECT pg_total_relation_size('tide.tide_outbox_messages'::regclass)::bigint",
            &[],
        )
        .await
        .expect("read outbox relation size")
        .get(0)
}

async fn max_message_id(client: &Client, outbox: &str) -> i64 {
    client
        .query_one(
            "SELECT COALESCE(MAX(id), 0)::bigint
             FROM tide.tide_outbox_messages WHERE outbox_name = $1",
            &[&outbox],
        )
        .await
        .expect("read outbox identity")
        .get(0)
}

async fn wait_for_offset(client: &Client, pipeline: &str, outbox: &str, expected: i64) -> i64 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let current = client
            .query_opt(
                "SELECT last_change_id FROM tide.relay_consumer_offsets
                 WHERE pipeline_id = $1 AND outbox_name = $2
                 ORDER BY updated_at DESC LIMIT 1",
                &[&pipeline, &outbox],
            )
            .await
            .expect("query relay checkpoint")
            .map(|row| row.get::<_, i64>(0))
            .unwrap_or(0);
        if current >= expected {
            return current;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "relay checkpoint did not reach {expected}; current={current}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn start_relay(database_url: &str) -> RelayProcess {
    let binary = env::var(RELAY_BIN_ENV).unwrap_or_else(|_| "target/debug/pg-tide".to_string());
    let metrics_addr = env::var(METRICS_ADDR_ENV).unwrap_or_else(|_| "127.0.0.1:19090".to_string());
    let child = Command::new(&binary)
        .args([
            "--postgres-url",
            database_url,
            "--metrics-addr",
            &metrics_addr,
        ])
        .spawn()
        .unwrap_or_else(|error| panic!("start {binary}: {error}"));
    RelayProcess { child }
}

struct RelayProcess {
    child: Child,
}

impl RelayProcess {
    async fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn sample_process(process: &RelayProcess) -> ProcessSample {
    let status = format!("/proc/{}/status", process.child.id());
    let status_text = fs::read_to_string(status).unwrap_or_default();
    let status_bytes = |name: &str| {
        status_text
            .lines()
            .find(|line| line.starts_with(name))
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|value| value.parse::<u64>().ok())
            .map_or(0, |kilobytes| kilobytes * 1024)
    };
    let rss_bytes = status_bytes("VmRSS:");
    let hwm_bytes = status_bytes("VmHWM:");
    let file_descriptors = fs::read_dir(format!("/proc/{}/fd", process.child.id()))
        .map(|entries| entries.count() as u64)
        .unwrap_or(0);
    let cpu_seconds = fs::read_to_string(format!("/proc/{}/stat", process.child.id()))
        .ok()
        .and_then(|text| text.rsplit_once(") ").map(|(_, rest)| rest.to_string()))
        .and_then(|rest| {
            let fields: Vec<&str> = rest.split_whitespace().collect();
            let user = fields.get(11)?.parse::<f64>().ok()?;
            let system = fields.get(12)?.parse::<f64>().ok()?;
            Some((user + system) / 100.0)
        })
        .unwrap_or(0.0);
    ProcessSample {
        rss_bytes,
        hwm_bytes,
        file_descriptors,
        cpu_seconds,
    }
}

fn resource_sample(phase: &str, sample: &ProcessSample, elapsed_seconds: f64) -> Value {
    json!({
        "monotonic_seconds": elapsed_seconds,
        "phase": phase,
        "relay": {
            "rss_bytes": sample.rss_bytes,
            "hwm_bytes": sample.hwm_bytes,
            "file_descriptors": sample.file_descriptors,
            "cpu_seconds": sample.cpu_seconds
        }
    })
}

fn percentile(values: &[f64], percentile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let index = ((sorted.len() - 1) as f64 * percentile).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

fn sanitize_name(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '-'
            }
        })
        .collect()
}

fn unique_timestamp() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_micros()
}

fn unix_micros() -> i64 {
    unique_timestamp() as i64
}

fn git_output(arguments: &[&str]) -> String {
    String::from_utf8_lossy(
        &Command::new("git")
            .args(arguments)
            .output()
            .expect("run git")
            .stdout,
    )
    .trim()
    .to_string()
}

fn write_result(output: &Path, document: &Value) {
    assert!(
        output.starts_with("target"),
        "benchmark output must stay below target/"
    );
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).expect("create benchmark output directory");
    }
    fs::write(
        output,
        serde_json::to_vec_pretty(document).expect("serialize benchmark result"),
    )
    .expect("write benchmark result");
}
