//! Sustained operational soak entry point.

#[path = "operational_benchmark.rs"]
mod benchmark;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires PostgreSQL 18, NATS JetStream, and the real pg-tide binary"]
async fn operational_soak() {
    benchmark::run_operational_benchmark().await;
}
