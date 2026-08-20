/// RelayError — all errors that can occur in the relay binary.
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryClass {
    Transient,
    Permanent,
}

impl std::fmt::Display for RetryClass {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Transient => "transient",
            Self::Permanent => "permanent",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorFailureCode {
    Unavailable,
    Timeout,
    Throttled,
    Authentication,
    Authorization,
    TlsVerification,
    InvalidDestination,
    MessageTooLarge,
    ProtocolRejection,
    InvalidConfig,
    Shutdown,
    Unknown,
}

impl std::fmt::Display for ConnectorFailureCode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Unavailable => "unavailable",
            Self::Timeout => "timeout",
            Self::Throttled => "throttled",
            Self::Authentication => "authentication",
            Self::Authorization => "authorization",
            Self::TlsVerification => "tls_verification",
            Self::InvalidDestination => "invalid_destination",
            Self::MessageTooLarge => "message_too_large",
            Self::ProtocolRejection => "protocol_rejection",
            Self::InvalidConfig => "invalid_config",
            Self::Shutdown => "shutdown",
            Self::Unknown => "unknown",
        })
    }
}

#[derive(Debug, Error)]
pub enum RelayError {
    // Database errors
    #[error("postgres error: {0}")]
    Postgres(#[from] tokio_postgres::Error),

    #[error("postgres connection failed: {url}: {err}")]
    ConnectionFailed {
        url: String,
        err: tokio_postgres::Error,
    },

    // Configuration errors
    #[error("config error: {0}")]
    Config(String),

    #[error("invalid config for pipeline '{name}': {reason}")]
    InvalidConfig { name: String, reason: String },

    #[error("PGTIDE_CONFIG_UNSUPPORTED_SURFACE: {surface}; last_version=0.48.0; alternative={alternative}")]
    UnsupportedSurface {
        surface: String,
        alternative: String,
    },

    #[error("pipeline '{0}' not found")]
    PipelineNotFound(String),

    #[error("missing required config key '{key}' in pipeline '{pipeline}'")]
    MissingConfigKey { pipeline: String, key: String },

    // Payload errors
    #[error("unsupported outbox payload version: {0}")]
    UnsupportedPayloadVersion(i64),

    #[error("payload decode error in outbox '{outbox}' id={outbox_id}: {reason}")]
    PayloadDecode {
        outbox: String,
        outbox_id: i64,
        reason: String,
    },

    // Sink errors
    #[error("sink '{sink}' publish error: {source}")]
    SinkPublish {
        sink: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("connector '{connector}' failed ({code}, {retry_class}): {summary}")]
    ConnectorFailure {
        connector: String,
        code: ConnectorFailureCode,
        retry_class: RetryClass,
        summary: String,
    },

    #[error("sink '{sink}' unhealthy: {reason}")]
    SinkUnhealthy { sink: String, reason: String },

    // Source errors
    #[error("source '{src}' poll error: {inner}")]
    SourcePoll {
        src: String,
        inner: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("source '{src}' decode error: {reason}")]
    SourceDecode { src: String, reason: String },

    // IO errors
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    // JSON errors
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    // TOML errors
    #[error("toml error: {0}")]
    Toml(#[from] toml::de::Error),

    // Channel errors
    #[error("channel closed")]
    ChannelClosed,

    // Secret resolution errors (RELAY-SEC)
    #[error("secret token not found: {token}")]
    SecretNotFound { token: String },

    #[error("cannot read secret from file '{path}': {reason}")]
    SecretReadError { path: String, reason: String },

    #[error("invalid secret reference: {0}")]
    InvalidSecretToken(String),

    // Generic
    #[error("{0}")]
    Other(String),

    // TLS errors (v0.15.0 / v0.23.0)
    #[error("TLS required by sslmode=require but TLS backend not compiled in: {url}")]
    TlsRequired { url: String },

    #[error("PostgreSQL sslmode={mode} is rejected because it permits plaintext transport: {url}")]
    InsecureTransport { mode: String, url: String },

    #[error("TLS setup failed: {0}")]
    TlsSetup(String),

    // v0.35.0: KMS provider not yet implemented
    #[error("provider '{provider}' is not yet implemented: {message}")]
    NotImplemented { provider: String, message: String },
}

impl RelayError {
    pub fn config(msg: impl Into<String>) -> Self {
        Self::Config(msg.into())
    }

    pub fn other(msg: impl Into<String>) -> Self {
        Self::Other(msg.into())
    }

    /// v0.35.0: Construct a `NotImplemented` error for KMS providers.
    pub fn not_implemented(provider: impl Into<String>, message: impl Into<String>) -> Self {
        Self::NotImplemented {
            provider: provider.into(),
            message: message.into(),
        }
    }

    pub fn sink<E: std::error::Error + Send + Sync + 'static>(
        sink: impl Into<String>,
        source: E,
    ) -> Self {
        Self::SinkPublish {
            sink: sink.into(),
            source: Box::new(source),
        }
    }

    pub fn connector_failure(
        connector: impl Into<String>,
        code: ConnectorFailureCode,
        retry_class: RetryClass,
        summary: impl Into<String>,
    ) -> Self {
        Self::ConnectorFailure {
            connector: connector.into(),
            code,
            retry_class,
            summary: summary.into(),
        }
    }

    pub fn retry_class(&self) -> RetryClass {
        match self {
            Self::ConnectorFailure { retry_class, .. } => *retry_class,
            _ if self.is_transient() => RetryClass::Transient,
            _ => RetryClass::Permanent,
        }
    }

    pub fn connector_code(&self) -> Option<ConnectorFailureCode> {
        match self {
            Self::ConnectorFailure { code, .. } => Some(*code),
            _ => None,
        }
    }

    pub fn public_code(&self) -> ConnectorFailureCode {
        self.connector_code()
            .unwrap_or(ConnectorFailureCode::Unknown)
    }

    pub fn public_summary(&self) -> &str {
        match self {
            Self::ConnectorFailure { summary, .. } => summary,
            _ => "relay operation failed",
        }
    }

    pub fn owned_connector_failure(&self) -> Option<Self> {
        match self {
            Self::ConnectorFailure {
                connector,
                code,
                retry_class,
                summary,
            } => Some(Self::connector_failure(
                connector.clone(),
                *code,
                *retry_class,
                summary.clone(),
            )),
            _ => None,
        }
    }

    pub fn postgres_connector_failure(
        connector: impl Into<String>,
        error: &tokio_postgres::Error,
    ) -> Self {
        let (code, retry_class, summary) = match error.code().map(|state| state.code()) {
            Some("08001" | "08003" | "08006" | "08007" | "57P01") => (
                ConnectorFailureCode::Unavailable,
                RetryClass::Transient,
                "database unavailable",
            ),
            Some("57014") => (
                ConnectorFailureCode::Timeout,
                RetryClass::Transient,
                "database operation timed out",
            ),
            Some("28P01") => (
                ConnectorFailureCode::Authentication,
                RetryClass::Permanent,
                "database authentication rejected",
            ),
            Some("42501") => (
                ConnectorFailureCode::Authorization,
                RetryClass::Permanent,
                "database authorization rejected",
            ),
            Some("23505" | "42P01" | "42703" | "42883") => (
                ConnectorFailureCode::ProtocolRejection,
                RetryClass::Permanent,
                "database rejected the inbox write",
            ),
            _ => (
                ConnectorFailureCode::Unknown,
                RetryClass::Transient,
                "database operation failed",
            ),
        };
        Self::connector_failure(connector, code, retry_class, summary)
    }

    pub fn into_connector_failure(self, connector: impl Into<String>) -> Self {
        let connector = connector.into();
        match self {
            Self::ConnectionFailed { err, .. } | Self::Postgres(err) => {
                Self::postgres_connector_failure(connector, &err)
            }
            Self::TlsRequired { .. } | Self::InsecureTransport { .. } | Self::TlsSetup(_) => {
                Self::connector_failure(
                    connector,
                    ConnectorFailureCode::TlsVerification,
                    RetryClass::Permanent,
                    "connector TLS verification failed",
                )
            }
            other => other,
        }
    }

    /// Returns `true` if this error is transient (may succeed on retry).
    ///
    /// Permanent errors (bad credentials, schema mismatch, auth rejection,
    /// invalid config) should not trigger retry loops — they indicate that
    /// the pipeline must be paused and reviewed before retrying.
    pub fn is_transient(&self) -> bool {
        match self {
            // Permanent: configuration / auth / schema errors
            Self::Config(_)
            | Self::InvalidConfig { .. }
            | Self::PipelineNotFound(_)
            | Self::MissingConfigKey { .. }
            | Self::UnsupportedPayloadVersion(_)
            | Self::InvalidSecretToken(_)
            | Self::SecretNotFound { .. }
            | Self::SecretReadError { .. }
            | Self::TlsRequired { .. }
            | Self::TlsSetup(_)
            | Self::SourceDecode { .. }
            | Self::NotImplemented { .. } => false,
            Self::ConnectorFailure { retry_class, .. } => *retry_class == RetryClass::Transient,
            // Transient: network / I/O / temporary backend issues
            _ => true,
        }
    }

    pub fn source_poll<E: std::error::Error + Send + Sync + 'static>(
        source: impl Into<String>,
        inner: E,
    ) -> Self {
        Self::SourcePoll {
            src: source.into(),
            inner: Box::new(inner),
        }
    }

    /// If this error wraps a `tokio_postgres::Error` directly, return a
    /// reference to it.  Useful for inspecting `SQLSTATE` codes.
    pub fn as_postgres_error(&self) -> Option<&tokio_postgres::Error> {
        match self {
            Self::Postgres(e) => Some(e),
            _ => None,
        }
    }
}
