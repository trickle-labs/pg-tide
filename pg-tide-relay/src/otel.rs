/// OpenTelemetry tracing integration (RELAY-P2-20).
///
/// Optional OTLP exporter for distributed tracing.  When enabled, the relay
/// exports spans to a configured OTLP endpoint (e.g. Jaeger, Grafana Tempo,
/// Datadog, Honeycomb).
///
/// Feature-gated: only compiled with `--features otel`.
///
/// Configuration is provided via `--otel-endpoint` CLI flag or
/// `PG_TIDE_OTEL_ENDPOINT` environment variable.  All tracing
/// instrumentation is done via the `tracing` crate using span macros —
/// the OpenTelemetry subscriber bridges `tracing` spans to OTLP.
///
/// Trace structure per poll cycle:
/// ```text
/// relay.poll_cycle            (root)
///   relay.source.poll         (time spent polling source)
///   relay.sink.publish        (time spent publishing batch)
///   relay.source.acknowledge  (time spent acknowledging)
/// ```
/// OpenTelemetry configuration.
#[derive(Debug, Clone, Default)]
pub struct OtelConfig {
    /// OTLP gRPC endpoint (e.g. `http://localhost:4317`).
    pub endpoint: Option<String>,
    /// Service name reported in traces.
    pub service_name: String,
    /// Sampling rate (1.0 = trace everything, 0.0 = trace nothing).
    pub sample_rate: f64,
}

impl OtelConfig {
    pub fn new(endpoint: impl Into<String>, service_name: impl Into<String>) -> Self {
        Self {
            endpoint: Some(endpoint.into()),
            service_name: service_name.into(),
            sample_rate: 1.0,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.endpoint.is_some()
    }
}

/// Initialise the OpenTelemetry tracing pipeline.
///
/// On success, the returned `OtelGuard` must be kept alive for the duration
/// of the process.  Dropping it flushes and shuts down the exporter.
///
/// This is a no-op when the `otel` feature is disabled.
#[cfg(feature = "otel")]
pub fn init_otel(config: &OtelConfig) -> Result<OtelGuard, crate::error::RelayError> {
    use opentelemetry::global;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::{runtime, trace as sdktrace, Resource};
    use tracing_opentelemetry::OpenTelemetryLayer;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    let endpoint = match &config.endpoint {
        Some(e) => e.clone(),
        None => return Ok(OtelGuard),
    };

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&endpoint)
        .build()
        .map_err(|e| crate::error::RelayError::other(format!("OTel exporter error: {e}")))?;

    let resource = Resource::new(vec![
        opentelemetry::KeyValue::new("service.name", config.service_name.clone()),
        opentelemetry::KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
    ]);

    let provider = sdktrace::TracerProvider::builder()
        .with_batch_exporter(exporter, runtime::Tokio)
        .with_resource(resource)
        .build();

    let tracer = provider.tracer("pg-tide");
    global::set_tracer_provider(provider);

    let otel_layer = OpenTelemetryLayer::new(tracer);

    tracing_subscriber::registry()
        .with(otel_layer)
        .try_init()
        .map_err(|e| crate::error::RelayError::other(format!("tracing init error: {e}")))?;

    tracing::info!(endpoint, "OpenTelemetry tracing enabled");
    Ok(OtelGuard)
}

/// Guard that shuts down the OTel exporter on drop.
#[cfg(feature = "otel")]
pub struct OtelGuard;

#[cfg(feature = "otel")]
impl Drop for OtelGuard {
    fn drop(&mut self) {
        opentelemetry::global::shutdown_tracer_provider();
    }
}

/// No-op guard when `otel` feature is disabled.
#[cfg(not(feature = "otel"))]
pub struct OtelGuard;

/// No-op initialiser when `otel` feature is disabled.
#[cfg(not(feature = "otel"))]
pub fn init_otel_noop(_config: &OtelConfig) -> OtelGuard {
    OtelGuard
}

/// Span names used throughout the relay for consistent telemetry.
pub mod spans {
    pub const POLL_CYCLE: &str = "relay.poll_cycle";
    pub const SOURCE_POLL: &str = "relay.source.poll";
    pub const SINK_PUBLISH: &str = "relay.sink.publish";
    pub const SOURCE_ACKNOWLEDGE: &str = "relay.source.acknowledge";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_otel_config_disabled_by_default() {
        let config = OtelConfig::default();
        assert!(!config.is_enabled());
    }

    #[test]
    fn test_otel_config_enabled_with_endpoint() {
        let config = OtelConfig::new("http://localhost:4317", "pg-tide-relay");
        assert!(config.is_enabled());
        assert_eq!(config.endpoint.as_deref(), Some("http://localhost:4317"));
        assert_eq!(config.service_name, "pg-tide-relay");
    }

    #[test]
    fn test_span_names_are_defined() {
        assert!(!spans::POLL_CYCLE.is_empty());
        assert!(!spans::SOURCE_POLL.is_empty());
        assert!(!spans::SINK_PUBLISH.is_empty());
        assert!(!spans::SOURCE_ACKNOWLEDGE.is_empty());
    }

    #[test]
    fn test_noop_guard_compiles() {
        // Verify the OtelGuard can be constructed and dropped without error.
        let _guard = OtelGuard;
    }
}
