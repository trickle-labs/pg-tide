//! Benchmarks for pg-tide-relay.
//!
//! Measures outbox message throughput, end-to-end delivery latency, and
//! inbox dedup overhead at scale.
//!
//! Run with:
//!
//! ```bash
//! just bench
//! # or
//! cargo bench --package pg-tide-relay
//! ```

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

// ── Payload construction ───────────────────────────────────────────────────

/// Build a realistic order-created payload.
fn order_payload(n: u64) -> serde_json::Value {
    serde_json::json!({
        "event_id":   format!("evt-{n:08}"),
        "event_type": "order.created",
        "order_id":   n,
        "customer_id": n % 1000,
        "total_cents": 9999_u64,
        "currency":   "USD",
        "items": [
            {"sku": "WIDGET-1", "qty": 2, "unit_price_cents": 4999_u64},
        ],
        "created_at": "2026-01-01T00:00:00Z",
    })
}

/// Build a payload of a specific size (approximate bytes).
fn sized_payload(n: u64, approx_bytes: usize) -> serde_json::Value {
    let pad = "x".repeat(approx_bytes.saturating_sub(80));
    serde_json::json!({
        "event_id":   format!("evt-{n:08}"),
        "event_type": "order.created",
        "order_id":   n,
        "data":       pad,
    })
}

// ── Serialisation benchmarks ───────────────────────────────────────────────

fn bench_payload_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("payload_serialization");

    for size in [1_u64, 10, 100, 1_000] {
        group.bench_with_input(
            BenchmarkId::new("serde_json::to_string", size),
            &size,
            |b, &n| {
                b.iter(|| {
                    let payload = order_payload(n);
                    serde_json::to_string(&payload).unwrap()
                });
            },
        );
    }

    group.finish();
}

// ── Batch construction ─────────────────────────────────────────────────────

/// Simulates the relay building a batch of envelope JSON objects before
/// publishing them to a sink. This is the hot path for high-throughput workloads.
fn bench_batch_construction(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_construction");

    for batch_size in [10_usize, 100, 500, 1_000] {
        group.bench_with_input(
            BenchmarkId::new("build_json_batch", batch_size),
            &batch_size,
            |b, &n| {
                b.iter(|| {
                    (0..n as u64)
                        .map(|i| {
                            serde_json::json!({
                                "id": i,
                                "outbox_name": "orders",
                                "payload": order_payload(i),
                                "headers": {},
                                "dedup_key": format!("evt-{i:08}"),
                            })
                        })
                        .collect::<Vec<_>>()
                });
            },
        );
    }

    group.finish();
}

// ── Subject rendering ──────────────────────────────────────────────────────

/// Measures the overhead of Handlebars-style subject template rendering.
/// A relay with thousands of messages per second renders one subject per message.
fn bench_subject_rendering(c: &mut Criterion) {
    let mut group = c.benchmark_group("subject_rendering");

    let outbox_name = "orders";
    let event_type = "order.created";

    group.bench_function("static_subject", |b| {
        b.iter(|| format!("{outbox_name}.events"));
    });

    group.bench_function("dynamic_subject", |b| {
        b.iter(|| format!("{outbox_name}.{event_type}"));
    });

    group.bench_function("template_replace", |b| {
        let template = "{outbox_name}.{event_type}";
        b.iter(|| {
            template
                .replace("{outbox_name}", outbox_name)
                .replace("{event_type}", event_type)
        });
    });

    group.finish();
}

// ── Dedup key hashing ──────────────────────────────────────────────────────

/// Benchmarks the cost of hashing event IDs for the inbox dedup index.
fn bench_dedup_hashing(c: &mut Criterion) {
    use std::collections::HashSet;

    let mut group = c.benchmark_group("dedup_hashing");

    for set_size in [100_usize, 1_000, 10_000] {
        group.bench_with_input(
            BenchmarkId::new("hashset_contains", set_size),
            &set_size,
            |b, &n| {
                let seen: HashSet<String> = (0..n).map(|i| format!("evt-{i:08}")).collect();
                b.iter(|| seen.contains("evt-00000042"));
            },
        );
    }

    group.finish();
}

// ── OutboxPollerSource poll simulation ────────────────────────────────────

/// Simulates the payload decode step of OutboxPollerSource::poll():
/// deserialise a batch of 1000 rows at varying payload sizes.
///
/// This isolates orchestration overhead from PostgreSQL I/O.
fn bench_outbox_poll_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("outbox_poll_decode");

    for &(label, payload_bytes) in &[
        ("1kb", 1_024_usize),
        ("10kb", 10_240_usize),
        ("100kb", 102_400_usize),
    ] {
        // Pre-build the raw JSON strings that would arrive from PostgreSQL.
        let rows: Vec<String> = (0..1_000_u64)
            .map(|i| {
                let p = sized_payload(i, payload_bytes);
                serde_json::to_string(&p).unwrap()
            })
            .collect();

        group.bench_with_input(
            BenchmarkId::new("decode_batch_1000", label),
            &rows,
            |b, rows| {
                b.iter(|| {
                    rows.iter()
                        .map(|s| serde_json::from_str::<serde_json::Value>(s).unwrap())
                        .collect::<Vec<_>>()
                });
            },
        );
    }

    group.finish();
}

// ── InboxSink batch UNNEST parameter building ─────────────────────────────

/// Simulates InboxSink::publish(): building the four UNNEST parameter Vecs
/// from a batch of messages.  This is the hot path for the pg-inbox sink.
fn bench_inbox_unnest_params(c: &mut Criterion) {
    let mut group = c.benchmark_group("inbox_unnest_params");

    for &batch_size in &[1_usize, 10, 100, 1_000] {
        group.bench_with_input(
            BenchmarkId::new("build_unnest_vecs", batch_size),
            &batch_size,
            |b, &n| {
                let messages: Vec<serde_json::Value> = (0..n as u64)
                    .map(|i| {
                        serde_json::json!({
                            "event_id": format!("evt-{i:08}"),
                            "source":   "orders",
                            "payload":  order_payload(i),
                            "headers":  {"event_type": "order.created"},
                        })
                    })
                    .collect();

                b.iter(|| {
                    let mut event_ids = Vec::with_capacity(n);
                    let mut sources = Vec::with_capacity(n);
                    let mut payloads = Vec::with_capacity(n);
                    let mut headers = Vec::with_capacity(n);
                    for msg in &messages {
                        event_ids.push(msg["event_id"].as_str().unwrap_or("").to_string());
                        sources.push(msg["source"].as_str().unwrap_or("").to_string());
                        payloads.push(serde_json::to_string(&msg["payload"]).unwrap());
                        headers.push(serde_json::to_string(&msg["headers"]).unwrap());
                    }
                    (event_ids, sources, payloads, headers)
                });
            },
        );
    }

    group.finish();
}

// ── Coordinator worker_inner orchestration mock ───────────────────────────

/// Simulates the coordinator's worker_inner() orchestration loop overhead
/// independently of PostgreSQL I/O.  Measures: poll_and_decode overhead,
/// routing decision, and batch aggregation for a mock source→sink path.
fn bench_worker_inner_orchestration(c: &mut Criterion) {
    use std::collections::HashMap;

    let mut group = c.benchmark_group("worker_inner_orchestration");

    // Simulate the per-message routing + envelope building overhead.
    for &batch_size in &[10_usize, 100, 1_000] {
        group.bench_with_input(
            BenchmarkId::new("routing_and_envelope", batch_size),
            &batch_size,
            |b, &n| {
                let messages: Vec<serde_json::Value> = (0..n as u64)
                    .map(|i| {
                        serde_json::json!({
                            "id":          i,
                            "outbox_name": "orders",
                            "payload":     order_payload(i),
                            "headers":     {},
                            "dedup_key":   format!("evt-{i:08}"),
                            "created_at":  "2026-01-01T00:00:00Z",
                        })
                    })
                    .collect();

                b.iter(|| {
                    // Simulate routing: partition messages by event_type.
                    let mut routed: HashMap<&str, Vec<&serde_json::Value>> = HashMap::new();
                    for msg in &messages {
                        let event_type = msg["payload"]["event_type"].as_str().unwrap_or("unknown");
                        routed.entry(event_type).or_default().push(msg);
                    }
                    // Simulate envelope wrapping for sink publish.
                    routed
                        .values()
                        .flat_map(|batch| {
                            batch.iter().map(|msg| {
                                serde_json::json!({
                                    "v": 1,
                                    "id": msg["id"],
                                    "subject": format!(
                                        "orders.{}",
                                        msg["payload"]["event_type"].as_str().unwrap_or("event")
                                    ),
                                    "payload": &msg["payload"],
                                })
                            })
                        })
                        .collect::<Vec<_>>()
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_payload_serialization,
    bench_batch_construction,
    bench_subject_rendering,
    bench_dedup_hashing,
    bench_outbox_poll_decode,
    bench_inbox_unnest_params,
    bench_worker_inner_orchestration,
);
criterion_main!(benches);
