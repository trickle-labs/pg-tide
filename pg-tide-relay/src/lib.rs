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
pub mod descriptors;
pub mod dlq;
pub mod encryption;
pub mod envelope;
pub mod error;
pub mod failpoints;
pub mod http_util;
pub mod metrics;
pub mod pg_tls;
pub mod rate_limiter;
pub mod secret;
pub mod sink;
pub mod source;
pub mod transforms;
pub mod wire_format;
