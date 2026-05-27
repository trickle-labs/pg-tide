pub mod inbox;
pub mod pg_outbox;
/// Sink trait and implementations.
/// A Sink consumes RelayMessages published by a Source.
pub mod stdout;

#[cfg(feature = "nats")]
pub mod nats;

#[cfg(feature = "webhook")]
pub mod webhook;

#[cfg(feature = "kafka")]
pub mod kafka;

#[cfg(feature = "redis")]
pub mod redis;

#[cfg(feature = "sqs")]
pub mod sqs;

#[cfg(feature = "rabbitmq")]
pub mod rabbitmq;

// v0.5.0: Cloud provider parity & analytics backends
#[cfg(feature = "elasticsearch")]
pub mod elasticsearch;

#[cfg(feature = "pubsub")]
pub mod pubsub;

#[cfg(feature = "kinesis")]
pub mod kinesis;

#[cfg(feature = "servicebus")]
pub mod servicebus;

// v0.6.0: IoT and data lake backends
#[cfg(feature = "mqtt")]
pub mod mqtt;

#[cfg(feature = "eventhubs")]
pub mod eventhubs;

#[cfg(feature = "object-storage")]
pub mod object_storage;

// v0.8.0: Notification sinks + Arrow Flight
#[cfg(feature = "slack")]
pub mod slack;

#[cfg(feature = "discord")]
pub mod discord;

#[cfg(feature = "pagerduty")]
pub mod pagerduty;

#[cfg(feature = "arrow-flight")]
pub mod arrow_flight;

// v0.9.0: Connector ecosystem sinks
#[cfg(feature = "singer")]
pub mod singer;

#[cfg(feature = "airbyte")]
pub mod airbyte;

// v0.10.0: Analytics sinks
#[cfg(feature = "clickhouse")]
pub mod clickhouse;

#[cfg(feature = "mongodb")]
pub mod mongodb;

#[cfg(feature = "snowflake")]
pub mod snowflake;

#[cfg(feature = "bigquery")]
pub mod bigquery;

#[cfg(feature = "iceberg")]
pub mod iceberg;

#[cfg(feature = "delta")]
pub mod delta;

#[cfg(feature = "ducklake")]
pub mod ducklake;

// v0.37.0: RockLake PG-wire sidecar sink (bounded SQL subset).
// Enabled with --features rocklake.
// See plans/ecosystem/rocklake.md for design details.
#[cfg(feature = "rocklake")]
pub mod rocklake;

use async_trait::async_trait;

use crate::envelope::RelayMessage;
use crate::error::RelayError;

/// A Sink consumes batches of RelayMessages.
#[async_trait]
pub trait Sink: Send {
    /// Return the sink backend type name (for logging/metrics).
    fn name(&self) -> &str;

    /// Publish a batch of messages.
    /// Returns Ok(()) if all messages were published successfully.
    /// On error, the caller will retry or fail the pipeline.
    async fn publish(&mut self, messages: &[RelayMessage]) -> Result<(), RelayError>;

    /// Check if the sink is healthy (optional health check).
    async fn is_healthy(&mut self) -> bool;

    /// Gracefully close the sink (flush, disconnect, etc.).
    async fn close(&mut self) -> Result<(), RelayError>;
}
