/// Source trait and implementations.
/// A Source produces RelayMessages that are forwarded to a Sink.
pub mod outbox;

use async_trait::async_trait;

use crate::envelope::RelayMessage;
use crate::error::RelayError;

/// A Source yields batches of RelayMessages.
///
/// After a batch reaches a durable terminal disposition, the coordinator
/// calls `acknowledge()` with the checkpoint captured from the original poll.
/// A sink may reject a batch, but the source checkpoint only advances after
/// the coordinator records a durable terminal disposition.
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
