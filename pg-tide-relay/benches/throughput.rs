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

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

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

criterion_group!(
    benches,
    bench_payload_serialization,
    bench_batch_construction,
    bench_subject_rendering,
    bench_dedup_hashing,
);
criterion_main!(benches);
