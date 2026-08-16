/// Dead-Letter Queue (DLQ) — stores relay messages that could not be delivered
/// after all retries, providing a recovery path via SQL functions.
///
/// Messages are written to `tide.relay_dlq` and can be inspected and retried
/// via the SQL API: `tide.relay_dlq_list()`, `tide.relay_dlq_retry(id)`, etc.
use std::sync::Arc;

use tokio_postgres::Client;

use crate::envelope::RelayMessage;
use crate::error::RelayError;

/// Classification of why a message ended up in the DLQ.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// The payload could not be decoded.
    Decode,
    /// The sink returned a permanent error (e.g. invalid credentials, schema mismatch).
    SinkPermanent,
    /// Inbox insertion failed permanently (constraint violation, etc.).
    InboxPermanent,
    /// Max retries exceeded for a transient error.
    MaxRetriesExceeded,
}

impl ErrorKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Decode => "decode",
            Self::SinkPermanent => "sink_permanent",
            Self::InboxPermanent => "inbox_permanent",
            Self::MaxRetriesExceeded => "max_retries_exceeded",
        }
    }
}

/// An entry ready to be written to `tide.relay_dlq`.
#[derive(Debug, Clone)]
pub struct DlqEntry {
    pub relay_mode: String,
    pub pipeline_name: String,
    pub source_name: String,
    pub sink_name: String,
    pub dedup_key: String,
    pub subject: Option<String>,
    pub payload: serde_json::Value,
    pub error_message: String,
    pub error_kind: ErrorKind,
}

impl DlqEntry {
    /// Build a DLQ entry from a failed relay message and context.
    pub fn from_message(
        relay_mode: &str,
        pipeline_name: &str,
        source_name: &str,
        sink_name: &str,
        msg: &RelayMessage,
        error: &str,
        kind: ErrorKind,
    ) -> Self {
        Self {
            relay_mode: relay_mode.to_string(),
            pipeline_name: pipeline_name.to_string(),
            source_name: source_name.to_string(),
            sink_name: sink_name.to_string(),
            dedup_key: msg.dedup_key.clone(),
            subject: Some(msg.subject.clone()),
            payload: msg.payload.clone(),
            error_message: error.to_string(),
            error_kind: kind,
        }
    }
}

/// Insert a batch of failed messages into `tide.relay_dlq`.
///
/// The complete batch is one durable disposition. A transaction makes a
/// connection or row failure roll back every insert, while `ON CONFLICT DO
/// NOTHING` keeps retries idempotent.
pub async fn insert_batch(db: &Arc<Client>, entries: &[DlqEntry]) -> Result<(), RelayError> {
    if entries.is_empty() {
        return Ok(());
    }

    db.batch_execute("BEGIN").await?;
    for entry in entries {
        if let Err(error) = insert_one(db, entry).await {
            if let Err(rollback_error) = db.batch_execute("ROLLBACK").await {
                return Err(RelayError::other(format!(
                    "DLQ insert failed: {error}; rollback failed: {rollback_error}"
                )));
            }
            return Err(error);
        }
    }
    db.batch_execute("COMMIT").await.map_err(RelayError::from)
}

/// Insert a single DLQ entry.
pub async fn insert_one(db: &Arc<Client>, entry: &DlqEntry) -> Result<(), RelayError> {
    db.execute(
        "INSERT INTO tide.relay_dlq
               (relay_mode, pipeline_name, source_name, sink_name,
                dedup_key, subject, payload, error_message, error_kind)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         ON CONFLICT DO NOTHING",
        &[
            &entry.relay_mode,
            &entry.pipeline_name,
            &entry.source_name,
            &entry.sink_name,
            &entry.dedup_key,
            &entry.subject,
            &entry.payload,
            &entry.error_message,
            &entry.error_kind.as_str(),
        ],
    )
    .await?;
    Ok(())
}

/// Configuration for DLQ behaviour in a pipeline.
#[derive(Debug, Clone)]
pub struct DlqConfig {
    /// Whether DLQ is enabled for this pipeline.
    pub enabled: bool,
    /// Maximum delivery attempts before routing to DLQ.
    pub max_retries: u32,
    /// Delay between retries in seconds.
    pub retry_delay_seconds: u64,
    /// How many days to retain resolved entries.
    pub retention_days: i32,
}

impl Default for DlqConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_retries: 5,
            retry_delay_seconds: 60,
            retention_days: 30,
        }
    }
}

impl DlqConfig {
    /// Parse DLQ config from a pipeline's JSON config object.
    pub fn from_pipeline_config(config: &serde_json::Value) -> Self {
        let dlq = match config.get("dlq") {
            Some(d) => d,
            None => return Self::default(),
        };

        Self {
            enabled: dlq
                .get("enabled")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            max_retries: dlq.get("max_retries").and_then(|v| v.as_u64()).unwrap_or(5) as u32,
            retry_delay_seconds: dlq
                .get("retry_delay_seconds")
                .and_then(|v| v.as_u64())
                .unwrap_or(60),
            retention_days: dlq
                .get("retention_days")
                .and_then(|v| v.as_i64())
                .unwrap_or(30) as i32,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_kind_str() {
        assert_eq!(ErrorKind::Decode.as_str(), "decode");
        assert_eq!(ErrorKind::SinkPermanent.as_str(), "sink_permanent");
        assert_eq!(ErrorKind::InboxPermanent.as_str(), "inbox_permanent");
        assert_eq!(
            ErrorKind::MaxRetriesExceeded.as_str(),
            "max_retries_exceeded"
        );
    }

    #[test]
    fn test_dlq_config_default() {
        let config = serde_json::json!({});
        let dlq = DlqConfig::from_pipeline_config(&config);
        assert!(!dlq.enabled);
        assert_eq!(dlq.max_retries, 5);
    }

    #[test]
    fn test_dlq_config_enabled() {
        let config = serde_json::json!({
            "dlq": {
                "enabled": true,
                "max_retries": 3,
                "retry_delay_seconds": 30,
                "retention_days": 14
            }
        });
        let dlq = DlqConfig::from_pipeline_config(&config);
        assert!(dlq.enabled);
        assert_eq!(dlq.max_retries, 3);
        assert_eq!(dlq.retry_delay_seconds, 30);
        assert_eq!(dlq.retention_days, 14);
    }

    #[test]
    fn test_dlq_entry_from_message() {
        let msg = crate::envelope::RelayMessage::new_reverse(
            "key-123",
            "order.created",
            serde_json::json!({"id": 1}),
        );
        let entry = DlqEntry::from_message(
            "forward",
            "orders-pipeline",
            "outbox:orders",
            "kafka",
            &msg,
            "connection refused",
            ErrorKind::SinkPermanent,
        );
        assert_eq!(entry.dedup_key, "key-123");
        assert_eq!(entry.error_kind.as_str(), "sink_permanent");
        assert_eq!(entry.pipeline_name, "orders-pipeline");
    }
}
