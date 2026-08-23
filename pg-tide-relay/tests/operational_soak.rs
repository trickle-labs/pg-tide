//! Sustained operational soak entry point.

#![recursion_limit = "256"]

use std::env;

#[path = "operational_benchmark.rs"]
mod benchmark;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires PostgreSQL 18, NATS JetStream, and the real pg-tide binary"]
async fn operational_soak() {
    let tier = env::var("PG_TIDE_BENCH_TIER").unwrap_or_else(|_| "nightly".to_string());
    assert!(matches!(tier.as_str(), "nightly" | "scheduled" | "release"));
    let duration = env::var("PG_TIDE_BENCH_DURATION_SECS")
        .unwrap_or_else(|_| "1800".to_string())
        .parse::<u64>()
        .expect("PG_TIDE_BENCH_DURATION_SECS must be an integer");
    assert!(duration > 0, "soak duration must be positive");
    benchmark::run_operational_benchmark().await;
}
