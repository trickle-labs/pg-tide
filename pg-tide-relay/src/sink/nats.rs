/// NATS JetStream sink (RELAY-6).
/// Feature-gated: only compiled with `--features nats`.
///
/// v0.40.0 (ADR-011 §13): The sink — not the source — owns NATS subject
/// rendering. It accepts either a fixed `subject` or a rendered
/// `subject_template`, publishes `Nats-Msg-Id` using the stable dedup key, and
/// waits for the JetStream publish acknowledgment before reporting success.
use crate::envelope::RelayMessage;
use crate::error::RelayError;

/// How the NATS sink derives the destination subject for each message.
#[derive(Debug, Clone)]
pub enum SubjectSpec {
    /// A fixed subject used verbatim for every message.
    Fixed(String),
    /// A template rendered per message from its metadata.
    ///
    /// Variables: `{outbox}` (logical outbox name), `{stream_table}`
    /// (legacy `outbox_<name>` form), `{op}`, `{outbox_id}`, and `{event_type}`
    /// (from a string `event_type` header; falls back to the literal `event`).
    Template(String),
}

impl SubjectSpec {
    /// Documented default when a NATS sink config supplies neither `subject`
    /// nor `subject_template`.
    pub fn default_template() -> Self {
        SubjectSpec::Template("{outbox}.{op}".to_string())
    }

    /// Build a `SubjectSpec` from the optional config values.
    ///
    /// A fixed `subject` takes precedence over `subject_template`. When both are
    /// absent, the documented default template applies.
    pub fn from_config(subject: Option<&str>, subject_template: Option<&str>) -> Self {
        match (subject, subject_template) {
            (Some(s), _) if !s.is_empty() => SubjectSpec::Fixed(s.to_string()),
            (_, Some(t)) if !t.is_empty() => SubjectSpec::Template(t.to_string()),
            _ => SubjectSpec::default_template(),
        }
    }

    /// Render the destination subject for a message.
    pub fn render(&self, msg: &RelayMessage) -> String {
        match self {
            SubjectSpec::Fixed(s) => s.clone(),
            SubjectSpec::Template(t) => render_subject(t, msg),
        }
    }

    pub fn validate(&self) -> Result<(), RelayError> {
        let subject = match self {
            Self::Fixed(value) | Self::Template(value) => value,
        };
        validate_subject(subject)
    }
}

pub fn validate_subject(subject: &str) -> Result<(), RelayError> {
    let tokens: Vec<&str> = subject.split('.').collect();
    if subject.is_empty() || tokens.iter().any(|token| token.is_empty()) {
        return Err(RelayError::config(
            "nats subject must not contain empty tokens",
        ));
    }
    for (index, token) in tokens.iter().enumerate() {
        if token
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
        {
            return Err(RelayError::config(
                "nats subject contains whitespace or control characters",
            ));
        }
        if token.contains('*') && *token != "*" {
            return Err(RelayError::config(
                "nats wildcard must occupy a complete token",
            ));
        }
        if token.contains('>') && (*token != ">" || index + 1 != tokens.len()) {
            return Err(RelayError::config(
                "nats terminal wildcard must be the final token",
            ));
        }
    }
    Ok(())
}

/// Render a NATS subject template from a message's metadata (ADR-011 §13).
pub fn render_subject(template: &str, msg: &RelayMessage) -> String {
    let outbox = msg.outbox_name.as_deref().unwrap_or("");
    let stream_table = if outbox.is_empty() {
        String::new()
    } else {
        format!("outbox_{outbox}")
    };
    let event_type = msg
        .headers
        .as_ref()
        .and_then(|h| h.get("event_type"))
        .and_then(|v| v.as_str())
        .unwrap_or("event");
    template
        .replace("{outbox}", outbox)
        .replace("{stream_table}", &stream_table)
        .replace("{op}", &msg.op)
        .replace(
            "{outbox_id}",
            &msg.outbox_id.map(|v| v.to_string()).unwrap_or_default(),
        )
        .replace("{event_type}", event_type)
}

#[cfg(feature = "nats")]
use async_nats::jetstream;

#[cfg(feature = "nats")]
fn connect_error(error: async_nats::ConnectError) -> RelayError {
    use crate::error::{ConnectorFailureCode, RetryClass};

    let (code, class, summary) = match error.kind() {
        async_nats::ConnectErrorKind::Authentication => (
            ConnectorFailureCode::Authentication,
            RetryClass::Permanent,
            "NATS authentication rejected",
        ),
        async_nats::ConnectErrorKind::AuthorizationViolation => (
            ConnectorFailureCode::Authorization,
            RetryClass::Permanent,
            "NATS authorization rejected",
        ),
        async_nats::ConnectErrorKind::Tls => (
            ConnectorFailureCode::TlsVerification,
            RetryClass::Permanent,
            "NATS TLS setup failed",
        ),
        async_nats::ConnectErrorKind::TimedOut => (
            ConnectorFailureCode::Timeout,
            RetryClass::Transient,
            "NATS connection timed out",
        ),
        async_nats::ConnectErrorKind::ServerParse => (
            ConnectorFailureCode::InvalidDestination,
            RetryClass::Permanent,
            "NATS server address is invalid",
        ),
        _ => (
            ConnectorFailureCode::Unavailable,
            RetryClass::Transient,
            "NATS is unavailable",
        ),
    };
    RelayError::connector_failure("nats", code, class, summary)
}

#[cfg(feature = "nats")]
fn publish_error(error: jetstream::context::PublishError) -> RelayError {
    use crate::error::{ConnectorFailureCode, RetryClass};

    let (code, class, summary) = match error.kind() {
        jetstream::context::PublishErrorKind::StreamNotFound => (
            ConnectorFailureCode::InvalidDestination,
            RetryClass::Permanent,
            "NATS JetStream stream was not found",
        ),
        jetstream::context::PublishErrorKind::TimedOut => (
            ConnectorFailureCode::Timeout,
            RetryClass::Transient,
            "NATS JetStream publish timed out",
        ),
        jetstream::context::PublishErrorKind::MaxAckPending => (
            ConnectorFailureCode::Throttled,
            RetryClass::Transient,
            "NATS JetStream acknowledgment capacity is full",
        ),
        jetstream::context::PublishErrorKind::WrongLastMessageId
        | jetstream::context::PublishErrorKind::WrongLastSequence => (
            ConnectorFailureCode::ProtocolRejection,
            RetryClass::Permanent,
            "NATS JetStream publish was rejected",
        ),
        _ => (
            ConnectorFailureCode::Unavailable,
            RetryClass::Transient,
            "NATS JetStream publish failed",
        ),
    };
    RelayError::connector_failure("nats", code, class, summary)
}

#[cfg(feature = "nats")]
pub struct NatsSink {
    js: jetstream::Context,
    subject: SubjectSpec,
}

#[cfg(feature = "nats")]
impl NatsSink {
    /// Create a NATS JetStream sink.
    ///
    /// `subject` is a fixed destination; `subject_template` is rendered per
    /// message. When neither is provided, the documented default template
    /// (`{outbox}.{op}`) applies.
    pub async fn new(
        url: &str,
        subject: Option<&str>,
        subject_template: Option<&str>,
    ) -> Result<Self, RelayError> {
        let client = async_nats::connect(url).await.map_err(connect_error)?;
        let js = jetstream::new(client);
        let subject = SubjectSpec::from_config(subject, subject_template);
        subject.validate()?;
        Ok(Self { js, subject })
    }
}
#[cfg(feature = "nats")]
#[async_trait::async_trait]
impl super::Sink for NatsSink {
    fn name(&self) -> &str {
        "nats"
    }

    async fn publish(&mut self, messages: &[RelayMessage]) -> Result<(), RelayError> {
        use async_nats::HeaderMap;

        for msg in messages {
            let payload = serde_json::to_vec(msg).map_err(RelayError::Json)?;

            // v0.40.0: The sink renders the configured subject from the message
            // metadata (ADR-011 §13) rather than trusting a source-rendered one.
            let subject = self.subject.render(msg);

            let mut headers = HeaderMap::new();
            // Stable deduplication identity — JetStream uses Nats-Msg-Id to
            // reject duplicates within the stream's dedup window.
            headers.insert("Nats-Msg-Id", msg.dedup_key.as_str());
            if msg.is_full_refresh {
                headers.insert("Pgtrickle-Full-Refresh", "true");
            }

            // Publish and wait for the JetStream acknowledgment before treating
            // the message as delivered. Both the publish call and the ack future
            // propagate their errors so an unacknowledged publish fails the batch.
            self.js
                .publish_with_headers(subject, headers, payload.into())
                .await
                .map_err(publish_error)?
                .await
                .map_err(publish_error)?;
        }
        Ok(())
    }

    async fn is_healthy(&mut self) -> bool {
        self.js.query_account().await.is_ok()
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::RelayMessage;

    fn msg_with(
        outbox: &str,
        op: &str,
        id: i64,
        headers: Option<serde_json::Value>,
    ) -> RelayMessage {
        let mut m = RelayMessage::new_forward(
            &format!("outbox_{outbox}"),
            id,
            0,
            op,
            serde_json::json!({"k": "v"}),
            false,
            None,
            "unused-subject",
        );
        m.outbox_name = Some(outbox.to_string());
        m.headers = headers;
        m
    }

    #[test]
    fn fixed_subject_is_verbatim() {
        let spec = SubjectSpec::from_config(Some("orders.created"), None);
        let m = msg_with("orders", "insert", 1, None);
        assert_eq!(spec.render(&m), "orders.created");
    }

    #[test]
    fn template_renders_outbox_and_op() {
        let spec = SubjectSpec::from_config(None, Some("{outbox}.{op}"));
        let m = msg_with("orders", "insert", 7, None);
        assert_eq!(spec.render(&m), "orders.insert");
    }

    #[test]
    fn template_legacy_stream_table_prefix() {
        let spec = SubjectSpec::from_config(None, Some("{stream_table}.{op}"));
        let m = msg_with("orders", "insert", 7, None);
        assert_eq!(spec.render(&m), "outbox_orders.insert");
    }

    #[test]
    fn event_type_from_header_or_fallback() {
        let spec = SubjectSpec::from_config(None, Some("events.{event_type}"));
        let with = msg_with(
            "orders",
            "insert",
            1,
            Some(serde_json::json!({"event_type": "order.created"})),
        );
        assert_eq!(spec.render(&with), "events.order.created");
        let without = msg_with("orders", "insert", 1, None);
        assert_eq!(spec.render(&without), "events.event");
        let non_string = msg_with(
            "orders",
            "insert",
            1,
            Some(serde_json::json!({"event_type": 5})),
        );
        assert_eq!(spec.render(&non_string), "events.event");
    }

    #[test]
    fn default_when_neither_provided() {
        let spec = SubjectSpec::from_config(None, None);
        let m = msg_with("payments", "delete", 3, None);
        assert_eq!(spec.render(&m), "payments.delete");
    }

    #[test]
    fn outbox_id_variable() {
        let spec = SubjectSpec::from_config(None, Some("o.{outbox_id}"));
        let m = msg_with("orders", "insert", 42, None);
        assert_eq!(spec.render(&m), "o.42");
    }

    #[test]
    fn rejects_invalid_subject_wildcards() {
        assert!(validate_subject("orders.*created").is_err());
        assert!(validate_subject("orders.*.created").is_ok());
        assert!(validate_subject("orders.>").is_ok());
        assert!(validate_subject("orders.>.created").is_err());
        assert!(validate_subject("orders.created").is_ok());
    }
}
