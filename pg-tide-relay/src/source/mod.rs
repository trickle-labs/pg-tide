/// Source trait and implementations.
/// A Source produces RelayMessages that are forwarded to a Sink.
pub mod outbox;
pub mod stdin;

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

// v0.5.0: Cloud provider parity backends
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

// v0.9.0: Connector ecosystem sources
#[cfg(feature = "singer")]
pub mod singer;

#[cfg(feature = "airbyte")]
pub mod airbyte;

// v0.22.0: DuckLake reverse relay source
#[cfg(feature = "ducklake")]
pub mod ducklake;

// v0.37.0: RockLake reverse relay source (bounded SQL subset).
// Enabled with --features rocklake.
// See docs/archive/plans/rocklake.md for historical design details.
#[cfg(feature = "rocklake")]
pub mod rocklake;

// v0.32.0: WAL logical-replication source groundwork (feature-gated spike).
// Enabled only with --features wal-source; skipped in default CI.
// See docs/adr/adr-009-wal-logical-replication-source.md for design.
#[cfg(feature = "wal-source")]
pub mod pg_logical;

use async_trait::async_trait;

use crate::envelope::RelayMessage;
use crate::error::RelayError;

/// A Source yields batches of RelayMessages.
///
/// After a batch reaches a durable terminal disposition, the coordinator
/// calls `acknowledge()` with the checkpoint captured from the original poll.
/// Transforms and routing may remove messages but never change that checkpoint.
#[async_trait]
pub trait Source: Send {
    /// Return the source backend type name (for logging/metrics).
    fn name(&self) -> &str;

    /// Poll for the next batch of messages.
    /// Returns an empty Vec if there are no new messages.
    async fn poll(&mut self, batch_size: i64) -> Result<Vec<RelayMessage>, RelayError>;

    /// Acknowledge a durable terminal disposition for the polled batch.
    /// The Source should advance its committed offset to `checkpoint.ack_token`.
    async fn acknowledge(&mut self, last_message: &RelayMessage) -> Result<(), RelayError>;

    /// Configure a one-shot replay cursor without changing the live checkpoint.
    fn configure_replay(&mut self, _from_offset: i64) -> Result<(), RelayError> {
        Err(RelayError::InvalidConfig {
            name: self.name().to_string(),
            reason: "inline replay is supported only for native simple outbox sources".to_string(),
        })
    }

    /// Gracefully close the source (release resources, stop background tasks).
    async fn close(&mut self) -> Result<(), RelayError>;
}
