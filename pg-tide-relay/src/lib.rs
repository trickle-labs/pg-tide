// pg-tide-relay: library target for integration tests and external consumers.
//
// This `lib.rs` re-exports the public API modules so that integration tests
// (in `tests/`) and downstream crates can access envelope types, error types,
// and sink implementations without depending on the binary entry point.
//
// Feature-gated modules conditionally compile optional backends; per-item
// `#[allow(dead_code)]` attributes are used in those modules as needed.

pub mod circuit_breaker;
pub mod config;
pub mod coordinator;
pub mod dlq;
pub mod envelope;
pub mod error;
pub mod jmespath_transform;
pub mod metrics;
pub mod otel;
pub mod pg_tls;
pub mod rate_limiter;
pub mod routing;
pub mod schema_evolution;
pub mod schema_registry;
pub mod sink;
pub mod source;
pub mod transforms;
pub mod wire_format;
