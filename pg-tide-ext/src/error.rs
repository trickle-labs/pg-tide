//! Error types for pg_tide.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum PgTideError {
    #[error(
        "PGTIDE_CONFIG_UNSUPPORTED_SURFACE: {surface}; context={context}; \
         last_version=0.48.0; alternative={alternative}"
    )]
    UnsupportedSurface {
        surface: String,
        context: String,
        alternative: String,
    },

    #[error("PGTIDE_OUTBOX_ALREADY_EXISTS: outbox already exists: {0}")]
    OutboxAlreadyExists(String),

    #[error("PGTIDE_OUTBOX_NOT_FOUND: outbox not found: {0}")]
    OutboxNotFound(String),

    #[error("PGTIDE_INBOX_ALREADY_EXISTS: inbox already exists: {0}")]
    InboxAlreadyExists(String),

    #[error("PGTIDE_INBOX_NOT_FOUND: inbox not found: {0}")]
    InboxNotFound(String),

    #[error("PGTIDE_RELAY_NOT_FOUND: relay pipeline not found: {0}")]
    RelayNotFound(String),

    #[error("PGTIDE_INVALID_ARGUMENT: invalid argument: {0}")]
    InvalidArgument(String),

    #[error("PGTIDE_SWEEP_FAILED: outbox sweep failed for '{outbox}': {detail}")]
    SweepFailed { outbox: String, detail: String },

    #[error(
        "PGTIDE_PUBLISH_DENIED: role '{role}' is not authorized to publish to outbox '{outbox}'"
    )]
    PublishDenied { role: String, outbox: String },

    #[error(
        "PGTIDE_AUTHORIZATION_FAILED: authorization check failed for outbox '{outbox}': {detail}"
    )]
    AuthorizationError { outbox: String, detail: String },

    #[error("PGTIDE_SPI_ERROR: SPI error: {0}")]
    SpiError(String),
}

impl PgTideError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::UnsupportedSurface { .. } => "PGTIDE_CONFIG_UNSUPPORTED_SURFACE",
            Self::OutboxAlreadyExists(_) => "PGTIDE_OUTBOX_ALREADY_EXISTS",
            Self::OutboxNotFound(_) => "PGTIDE_OUTBOX_NOT_FOUND",
            Self::InboxAlreadyExists(_) => "PGTIDE_INBOX_ALREADY_EXISTS",
            Self::InboxNotFound(_) => "PGTIDE_INBOX_NOT_FOUND",
            Self::RelayNotFound(_) => "PGTIDE_RELAY_NOT_FOUND",
            Self::InvalidArgument(_) => "PGTIDE_INVALID_ARGUMENT",
            Self::SweepFailed { .. } => "PGTIDE_SWEEP_FAILED",
            Self::PublishDenied { .. } => "PGTIDE_PUBLISH_DENIED",
            Self::AuthorizationError { .. } => "PGTIDE_AUTHORIZATION_FAILED",
            Self::SpiError(_) => "PGTIDE_SPI_ERROR",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PgTideError;

    #[test]
    fn stable_prefixes_preserve_contextual_suffixes() {
        let cases = [
            (
                PgTideError::OutboxAlreadyExists("orders".into()),
                "PGTIDE_OUTBOX_ALREADY_EXISTS: ",
                "outbox already exists: orders",
            ),
            (
                PgTideError::RelayNotFound("orders".into()),
                "PGTIDE_RELAY_NOT_FOUND: ",
                "relay pipeline not found: orders",
            ),
            (
                PgTideError::PublishDenied {
                    role: "app".into(),
                    outbox: "orders".into(),
                },
                "PGTIDE_PUBLISH_DENIED: ",
                "role 'app' is not authorized to publish to outbox 'orders'",
            ),
        ];
        for (error, prefix, suffix) in cases {
            let rendered = error.to_string();
            assert_eq!(error.code(), prefix.trim_end_matches(": "));
            assert_eq!(rendered.strip_prefix(prefix), Some(suffix));
        }
    }
}
