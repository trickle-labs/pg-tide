/// Prometheus metrics + health endpoint (RELAY-9).
use prometheus::{HistogramVec, IntCounterVec, IntGaugeVec, Registry, TextEncoder};
use std::sync::Arc;
use tokio::sync::RwLock;

// ── Metric name constants (used by Grafana dashboard codegen check) ───────────

pub const METRIC_MESSAGES_PUBLISHED: &str = "pg_tide_relay_messages_published_total";
pub const METRIC_MESSAGES_CONSUMED: &str = "pg_tide_relay_messages_consumed_total";
pub const METRIC_PUBLISH_ERRORS: &str = "pg_tide_relay_publish_errors_total";
pub const METRIC_DEDUP_SKIPPED: &str = "pg_tide_relay_dedup_skipped_total";
pub const METRIC_DLQ_ENTRIES_WRITTEN: &str = "pg_tide_relay_dlq_entries_written_total";
pub const METRIC_PIPELINE_HEALTHY: &str = "pg_tide_relay_pipeline_healthy";
pub const METRIC_CONSUMER_LAG: &str = "pg_tide_relay_consumer_lag";
pub const METRIC_DELIVERY_LATENCY: &str = "pg_tide_relay_delivery_latency_seconds";
/// v0.16.0: New coordinator metrics.
pub const METRIC_OWNED_PIPELINES: &str = "pg_tide_relay_owned_pipelines";
pub const METRIC_RECONCILE_DURATION: &str = "pg_tide_relay_reconcile_duration_seconds";
pub const METRIC_PIPELINE_ERRORS: &str = "pg_tide_relay_pipeline_errors_total";
/// v0.17.0: DLQ write error counter (permanent DLQ failures that pause the pipeline).
pub const METRIC_DLQ_WRITE_ERRORS: &str = "pg_tide_relay_dlq_write_errors_total";
/// v0.24.0: Per-sink publish latency histogram.
pub const METRIC_SINK_PUBLISH_DURATION: &str = "pg_tide_relay_sink_publish_duration_seconds";
/// v0.24.0: Connection pool state gauge.
pub const METRIC_POOL_CONNECTIONS: &str = "pg_tide_relay_pool_connections";
/// v0.24.0: Pool acquire duration histogram.
pub const METRIC_POOL_ACQUIRE_DURATION: &str = "pg_tide_relay_pool_acquire_duration_seconds";
/// v0.28.0: Delivery receipt write counter.
pub const METRIC_RECEIPTS_WRITTEN: &str = "pg_tide_relay_receipts_written_total";
/// v0.29.0: Fan-in source consumer lag gauge.
pub const METRIC_FANIN_SOURCE_LAG: &str = "pg_tide_relay_fanin_source_lag";
/// v0.29.0: Fan-in merged messages counter.
pub const METRIC_FANIN_MESSAGES_MERGED: &str = "pg_tide_relay_fanin_messages_merged_total";
pub const METRIC_DELIVERY_STAGE: &str = "pg_tide_relay_delivery_stage_total";
pub const METRIC_CHECKPOINT_ERRORS: &str = "pg_tide_relay_checkpoint_commit_errors_total";
pub const METRIC_OWNERSHIP_EVENTS: &str = "pg_tide_relay_ownership_events_total";
pub const METRIC_FORCED_SHUTDOWN: &str = "pg_tide_relay_forced_shutdown_total";
/// v0.43.0: Source poll query count for the operational cost contract.
pub const METRIC_SOURCE_POLL_QUERIES: &str = "pg_tide_relay_source_poll_queries_total";
/// v0.43.0: Durable source offset writes.
pub const METRIC_OFFSET_WRITES: &str = "pg_tide_relay_offset_writes_total";
/// v0.43.0: Coordinator catalog discovery query count.
pub const METRIC_CATALOG_DISCOVERY_QUERIES: &str = "pg_tide_relay_catalog_discovery_queries_total";

/// Shared relay metrics.
pub struct RelayMetrics {
    pub messages_published: IntCounterVec,
    pub messages_consumed: IntCounterVec,
    pub publish_errors: IntCounterVec,
    pub dedup_skipped: IntCounterVec,
    /// v0.13.0: DLQ entries written.
    pub dlq_entries_written: IntCounterVec,
    pub pipeline_healthy: IntGaugeVec,
    /// Pending messages in the outbox that haven't been consumed yet.
    pub consumer_lag: IntGaugeVec,
    /// End-to-end delivery latency in seconds (outbox publish → sink ack).
    pub delivery_latency_seconds: HistogramVec,
    /// v0.16.0: Number of pipeline workers currently owned by this coordinator.
    pub owned_pipelines: IntGaugeVec,
    /// v0.16.0: Duration of each reconcile loop iteration.
    pub reconcile_duration_seconds: HistogramVec,
    /// v0.16.0: Pipeline errors labelled by error class (transient/permanent).
    pub pipeline_errors_total: IntCounterVec,
    /// v0.17.0: DLQ write errors that caused a pipeline pause.
    pub dlq_write_errors: IntCounterVec,
    /// v0.24.0: Per-sink publish latency histogram (pipeline × sink_type).
    pub sink_publish_duration_seconds: HistogramVec,
    /// v0.24.0: Connection pool state gauge (idle/busy/waiting).
    pub pool_connections: IntGaugeVec,
    /// v0.24.0: Pool acquire duration histogram.
    pub pool_acquire_duration_seconds: HistogramVec,
    /// v0.28.0: Delivery receipts written counter (pipeline × sink_type).
    pub receipts_written: IntCounterVec,
    /// v0.29.0: Fan-in source consumer lag gauge (pipeline × outbox).
    pub fanin_source_lag: IntGaugeVec,
    /// v0.29.0: Fan-in merged messages counter (pipeline × outbox).
    pub fanin_messages_merged: IntCounterVec,
    /// v0.42.0: Fixed-cardinality delivery transition counter.
    pub delivery_stage_total: IntCounterVec,
    /// v0.42.0: Source checkpoint commit failures.
    pub checkpoint_commit_errors: IntCounterVec,
    /// v0.42.0: Ownership lifecycle transitions.
    pub ownership_events: IntCounterVec,
    /// v0.42.0: Forced shutdowns with in-flight work.
    pub forced_shutdown: IntCounterVec,
    pub source_poll_queries: IntCounterVec,
    pub offset_writes: IntCounterVec,
    pub catalog_discovery_queries: IntCounterVec,
    registry: Registry,
}

impl RelayMetrics {
    pub fn new() -> Result<Arc<Self>, prometheus::Error> {
        let registry = Registry::new();

        let messages_published = IntCounterVec::new(
            prometheus::opts!(
                METRIC_MESSAGES_PUBLISHED,
                "Total messages published to sink"
            ),
            &["pipeline", "direction", "tenant"],
        )?;

        let messages_consumed = IntCounterVec::new(
            prometheus::opts!(
                METRIC_MESSAGES_CONSUMED,
                "Total messages consumed from source"
            ),
            &["pipeline", "direction", "tenant"],
        )?;

        let publish_errors = IntCounterVec::new(
            prometheus::opts!(METRIC_PUBLISH_ERRORS, "Total publish errors"),
            &["pipeline", "direction", "tenant"],
        )?;

        let dedup_skipped = IntCounterVec::new(
            prometheus::opts!(
                METRIC_DEDUP_SKIPPED,
                "Total messages skipped due to deduplication"
            ),
            &["pipeline", "tenant"],
        )?;

        let dlq_entries_written = IntCounterVec::new(
            prometheus::opts!(
                METRIC_DLQ_ENTRIES_WRITTEN,
                "Total entries written to the dead-letter queue"
            ),
            &["pipeline", "direction", "tenant"],
        )?;

        let pipeline_healthy = IntGaugeVec::new(
            prometheus::opts!(
                METRIC_PIPELINE_HEALTHY,
                "1 if pipeline is healthy, 0 otherwise"
            ),
            &["pipeline", "tenant"],
        )?;

        let consumer_lag = IntGaugeVec::new(
            prometheus::opts!(
                METRIC_CONSUMER_LAG,
                "Pending messages in the outbox not yet consumed by the relay"
            ),
            &["pipeline", "tenant"],
        )?;

        let delivery_latency_seconds = HistogramVec::new(
            prometheus::HistogramOpts::new(
                METRIC_DELIVERY_LATENCY,
                "End-to-end latency from outbox publish to sink acknowledgement",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0]),
            &["pipeline", "tenant"],
        )?;

        // v0.16.0: New coordinator metrics.
        let owned_pipelines = IntGaugeVec::new(
            prometheus::opts!(
                METRIC_OWNED_PIPELINES,
                "Number of pipeline workers currently owned by this coordinator instance"
            ),
            &["relay_group"],
        )?;

        let reconcile_duration_seconds = HistogramVec::new(
            prometheus::HistogramOpts::new(
                METRIC_RECONCILE_DURATION,
                "Duration of coordinator reconcile loop iterations in seconds",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
            ]),
            &["relay_group"],
        )?;

        let pipeline_errors_total = IntCounterVec::new(
            prometheus::opts!(
                METRIC_PIPELINE_ERRORS,
                "Total pipeline errors labelled by error class"
            ),
            &["pipeline", "error_class"],
        )?;

        // v0.17.0: DLQ write errors counter.
        let dlq_write_errors = IntCounterVec::new(
            prometheus::opts!(
                METRIC_DLQ_WRITE_ERRORS,
                "Total DLQ write failures that caused a pipeline pause"
            ),
            &["pipeline"],
        )?;

        // v0.24.0: Per-sink publish latency histogram.
        let sink_publish_duration_seconds = HistogramVec::new(
            prometheus::HistogramOpts::new(
                METRIC_SINK_PUBLISH_DURATION,
                "Wall-clock time from Sink::publish() call entry to return, in seconds",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 30.0,
            ]),
            &["pipeline", "sink_type"],
        )?;

        // v0.24.0: Connection pool state gauge.
        let pool_connections = IntGaugeVec::new(
            prometheus::opts!(
                METRIC_POOL_CONNECTIONS,
                "Connection pool state counts (idle/busy/waiting)"
            ),
            &["state"],
        )?;

        // v0.24.0: Pool acquire duration histogram.
        let pool_acquire_duration_seconds = HistogramVec::new(
            prometheus::HistogramOpts::new(
                METRIC_POOL_ACQUIRE_DURATION,
                "Time to acquire a connection from the pool, in seconds",
            )
            .buckets(vec![0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
            &["relay_group"],
        )?;

        registry.register(Box::new(messages_published.clone()))?;
        registry.register(Box::new(messages_consumed.clone()))?;
        registry.register(Box::new(publish_errors.clone()))?;
        registry.register(Box::new(dedup_skipped.clone()))?;
        registry.register(Box::new(dlq_entries_written.clone()))?;
        registry.register(Box::new(pipeline_healthy.clone()))?;
        registry.register(Box::new(consumer_lag.clone()))?;
        registry.register(Box::new(delivery_latency_seconds.clone()))?;
        registry.register(Box::new(owned_pipelines.clone()))?;
        registry.register(Box::new(reconcile_duration_seconds.clone()))?;
        registry.register(Box::new(pipeline_errors_total.clone()))?;
        registry.register(Box::new(dlq_write_errors.clone()))?;
        registry.register(Box::new(sink_publish_duration_seconds.clone()))?;
        registry.register(Box::new(pool_connections.clone()))?;
        registry.register(Box::new(pool_acquire_duration_seconds.clone()))?;

        // v0.28.0: Delivery receipt writes.
        let receipts_written = IntCounterVec::new(
            prometheus::opts!(
                METRIC_RECEIPTS_WRITTEN,
                "Total delivery receipt rows written after confirmed sink publish"
            ),
            &["pipeline", "sink_type", "tenant"],
        )?;
        registry.register(Box::new(receipts_written.clone()))?;

        // v0.29.0: Fan-in source metrics.
        let fanin_source_lag = IntGaugeVec::new(
            prometheus::opts!(
                METRIC_FANIN_SOURCE_LAG,
                "Consumer lag per source outbox in fan-in pipelines"
            ),
            &["pipeline", "outbox", "tenant"],
        )?;
        registry.register(Box::new(fanin_source_lag.clone()))?;

        let fanin_messages_merged = IntCounterVec::new(
            prometheus::opts!(
                METRIC_FANIN_MESSAGES_MERGED,
                "Total messages merged from each source outbox in fan-in pipelines"
            ),
            &["pipeline", "outbox", "tenant"],
        )?;
        registry.register(Box::new(fanin_messages_merged.clone()))?;

        let delivery_stage_total = IntCounterVec::new(
            prometheus::opts!(METRIC_DELIVERY_STAGE, "Delivery state-machine transitions"),
            &["pipeline", "stage", "outcome"],
        )?;
        registry.register(Box::new(delivery_stage_total.clone()))?;

        let checkpoint_commit_errors = IntCounterVec::new(
            prometheus::opts!(
                METRIC_CHECKPOINT_ERRORS,
                "Source checkpoint commit failures"
            ),
            &["pipeline", "source"],
        )?;
        registry.register(Box::new(checkpoint_commit_errors.clone()))?;

        let ownership_events = IntCounterVec::new(
            prometheus::opts!(METRIC_OWNERSHIP_EVENTS, "Pipeline ownership transitions"),
            &["relay_group", "event"],
        )?;
        registry.register(Box::new(ownership_events.clone()))?;

        let forced_shutdown = IntCounterVec::new(
            prometheus::opts!(
                METRIC_FORCED_SHUTDOWN,
                "Workers aborted with in-flight work during shutdown"
            ),
            &["pipeline"],
        )?;
        registry.register(Box::new(forced_shutdown.clone()))?;

        let source_poll_queries = IntCounterVec::new(
            prometheus::opts!(
                METRIC_SOURCE_POLL_QUERIES,
                "Source poll queries issued by the relay"
            ),
            &["pipeline", "source"],
        )?;
        registry.register(Box::new(source_poll_queries.clone()))?;

        let offset_writes = IntCounterVec::new(
            prometheus::opts!(
                METRIC_OFFSET_WRITES,
                "Durable source offset writes issued by the relay"
            ),
            &["pipeline", "source"],
        )?;
        registry.register(Box::new(offset_writes.clone()))?;

        let catalog_discovery_queries = IntCounterVec::new(
            prometheus::opts!(
                METRIC_CATALOG_DISCOVERY_QUERIES,
                "Coordinator pipeline catalog discovery queries"
            ),
            &["relay_group"],
        )?;
        registry.register(Box::new(catalog_discovery_queries.clone()))?;

        Ok(Arc::new(Self {
            messages_published,
            messages_consumed,
            publish_errors,
            dedup_skipped,
            dlq_entries_written,
            pipeline_healthy,
            consumer_lag,
            delivery_latency_seconds,
            owned_pipelines,
            reconcile_duration_seconds,
            pipeline_errors_total,
            dlq_write_errors,
            sink_publish_duration_seconds,
            pool_connections,
            pool_acquire_duration_seconds,
            receipts_written,
            fanin_source_lag,
            fanin_messages_merged,
            delivery_stage_total,
            checkpoint_commit_errors,
            ownership_events,
            forced_shutdown,
            source_poll_queries,
            offset_writes,
            catalog_discovery_queries,
            registry,
        }))
    }

    pub fn render(&self) -> Result<String, prometheus::Error> {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = String::new();
        encoder.encode_utf8(&metric_families, &mut buffer)?;
        Ok(buffer)
    }
}

/// Health state for the relay process.
#[derive(Debug, Clone, Default)]
pub struct HealthState {
    pub healthy_pipelines: Vec<String>,
    pub unhealthy_pipelines: Vec<String>,
}

impl HealthState {
    pub fn is_healthy(&self) -> bool {
        self.unhealthy_pipelines.is_empty()
    }
}

/// Start the metrics + health HTTP server.
pub async fn start_metrics_server(
    addr: &str,
    metrics: Arc<RelayMetrics>,
    health: Arc<RwLock<HealthState>>,
) -> Result<(), crate::error::RelayError> {
    use axum::{extract::State, http::StatusCode, routing::get, Router};

    #[derive(Clone)]
    struct AppState {
        metrics: Arc<RelayMetrics>,
        health: Arc<RwLock<HealthState>>,
    }

    let state = AppState {
        metrics: Arc::clone(&metrics),
        health: Arc::clone(&health),
    };

    async fn health_handler(State(s): State<AppState>) -> (StatusCode, String) {
        let h = s.health.read().await;
        if h.is_healthy() {
            (StatusCode::OK, "healthy".to_string())
        } else {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("unhealthy: {:?}", h.unhealthy_pipelines),
            )
        }
    }

    let app = Router::new()
        .route(
            "/metrics",
            get(|State(s): State<AppState>| async move {
                match s.metrics.render() {
                    Ok(body) => (StatusCode::OK, body),
                    Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
                }
            }),
        )
        // v0.19.0: /healthz is the Kubernetes-standard liveness/readiness path;
        // /health is kept for backwards compatibility.
        .route("/health", get(health_handler))
        .route("/healthz", get(health_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(crate::error::RelayError::Io)?;

    tracing::info!("metrics server listening on {addr}");

    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!("metrics server error: {e}");
        }
    });

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_relay_metrics_creation() {
        let metrics = RelayMetrics::new().unwrap();
        metrics
            .messages_published
            .with_label_values(&["test-pipeline", "forward", "default"])
            .inc();
        // v0.16.0: touch coordinator metrics so they appear in render output.
        metrics
            .owned_pipelines
            .with_label_values(&["test-group"])
            .set(0);
        metrics
            .reconcile_duration_seconds
            .with_label_values(&["test-group"])
            .observe(0.001);
        metrics
            .pipeline_errors_total
            .with_label_values(&["test-pipeline", "transient"])
            .inc();
        metrics
            .source_poll_queries
            .with_label_values(&["test-pipeline", "outbox"])
            .inc();
        metrics
            .offset_writes
            .with_label_values(&["test-pipeline", "outbox"])
            .inc();
        metrics
            .catalog_discovery_queries
            .with_label_values(&["test-group"])
            .inc();
        let rendered = metrics.render().unwrap();
        assert!(rendered.contains(METRIC_MESSAGES_PUBLISHED));
        // v0.16.0: verify new coordinator metrics are registered.
        assert!(rendered.contains(METRIC_OWNED_PIPELINES));
        assert!(rendered.contains(METRIC_RECONCILE_DURATION));
        assert!(rendered.contains(METRIC_SOURCE_POLL_QUERIES));
        assert!(rendered.contains(METRIC_OFFSET_WRITES));
        assert!(rendered.contains(METRIC_CATALOG_DISCOVERY_QUERIES));
        // v0.17.0: verify DLQ write errors metric is registered.
        metrics
            .dlq_write_errors
            .with_label_values(&["test-pipeline"])
            .inc();
        let rendered2 = metrics.render().unwrap();
        assert!(rendered2.contains(METRIC_DLQ_WRITE_ERRORS));
    }

    #[test]
    fn test_metrics_tenant_label_dimension() {
        let metrics = RelayMetrics::new().unwrap();
        // v0.14.0: verify per-tenant label dimension works.
        metrics
            .messages_published
            .with_label_values(&["pipeline-a", "forward", "tenant-acme"])
            .inc_by(5);
        metrics
            .messages_published
            .with_label_values(&["pipeline-b", "forward", "tenant-beta"])
            .inc_by(3);
        let rendered = metrics.render().unwrap();
        assert!(
            rendered.contains("tenant-acme"),
            "tenant label must appear in metrics output"
        );
        assert!(
            rendered.contains("tenant-beta"),
            "second tenant label must appear"
        );
    }

    #[test]
    fn test_health_state_default_healthy() {
        let h = HealthState::default();
        assert!(h.is_healthy());
    }

    #[test]
    fn test_health_state_unhealthy() {
        let h = HealthState {
            healthy_pipelines: vec![],
            unhealthy_pipelines: vec!["broken-pipeline".to_string()],
        };
        assert!(!h.is_healthy());
    }
}
