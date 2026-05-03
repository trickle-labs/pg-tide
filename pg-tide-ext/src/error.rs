//! Error types for pg_tide.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum PgTideError {
    #[error("outbox already exists: {0}")]
    OutboxAlreadyExists(String),

    #[error("outbox not found: {0}")]
    OutboxNotFound(String),

    #[error("inbox already exists: {0}")]
    InboxAlreadyExists(String),

    #[error("inbox not found: {0}")]
    InboxNotFound(String),

    #[error("relay pipeline not found: {0}")]
    RelayNotFound(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("SPI error: {0}")]
    SpiError(String),
}
