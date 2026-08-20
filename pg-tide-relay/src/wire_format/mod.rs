//! Native and CloudEvents wire formats for outbound pg_tide relay messages.

use std::collections::HashMap;

pub mod cloudevents;
pub mod native;

pub use cloudevents::CloudEventsFormat;
pub use native::NativePgTideFormat;

#[derive(Debug, Clone)]
pub struct RawMessage {
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub topic: String,
    pub headers: HashMap<String, String>,
}

impl RawMessage {
    pub fn new(topic: impl Into<String>, key: Option<Vec<u8>>, value: Option<Vec<u8>>) -> Self {
        Self {
            key,
            value,
            topic: topic.into(),
            headers: HashMap::new(),
        }
    }

    pub fn from_json(topic: impl Into<String>, value: &serde_json::Value) -> Self {
        Self::new(topic, None, serde_json::to_vec(value).ok())
    }

    pub fn tombstone(topic: impl Into<String>, key: Vec<u8>) -> Self {
        Self::new(topic, Some(key), None)
    }
}

#[derive(Debug, Clone)]
pub struct InboxRow {
    pub event_id: String,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub old_payload: Option<serde_json::Value>,
    pub op: String,
    pub commit_ts: Option<chrono::DateTime<chrono::Utc>>,
    pub source_position: Option<String>,
}

impl InboxRow {
    pub fn from_relay_message(msg: &crate::envelope::RelayMessage) -> Self {
        Self {
            event_id: msg.dedup_key.clone(),
            event_type: msg.subject.clone(),
            payload: msg.payload.clone(),
            old_payload: None,
            op: msg.op.clone(),
            commit_ts: None,
            source_position: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OutboxRow {
    pub outbox_id: i64,
    pub stream_table: String,
    pub database: String,
    pub schema_name: String,
    pub op: String,
    pub new_row: Option<serde_json::Value>,
    pub old_row: Option<serde_json::Value>,
    pub commit_ts: Option<chrono::DateTime<chrono::Utc>>,
    pub source_lsn: Option<i64>,
}

impl OutboxRow {
    pub fn from_relay_message(msg: &crate::envelope::RelayMessage) -> Self {
        Self {
            outbox_id: msg.outbox_id.unwrap_or(0),
            stream_table: msg.subject.clone(),
            database: "postgres".to_string(),
            schema_name: "public".to_string(),
            op: msg.op.clone(),
            new_row: (msg.op != "delete").then(|| msg.payload.clone()),
            old_row: (msg.op == "delete" || msg.op == "update").then(|| msg.payload.clone()),
            commit_ts: None,
            source_lsn: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EncodeContext {
    pub server_name: String,
    pub topic_template: String,
    pub emit_tombstones: bool,
    pub heartbeat_interval_ms: u64,
}

impl Default for EncodeContext {
    fn default() -> Self {
        Self {
            server_name: "pg-tide".to_string(),
            topic_template: "{server}.{schema}.{stream_table}".to_string(),
            emit_tombstones: true,
            heartbeat_interval_ms: 10_000,
        }
    }
}

impl EncodeContext {
    pub fn resolve_topic(&self, row: &OutboxRow) -> String {
        self.topic_template
            .replace("{server}", &self.server_name)
            .replace("{schema}", &row.schema_name)
            .replace("{stream_table}", &row.stream_table)
            .replace("{database}", &row.database)
    }
}

#[derive(Debug, Clone)]
pub struct EncodedMessage {
    pub topic: String,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct EncodedBatch {
    pub messages: Vec<EncodedMessage>,
}

impl EncodedBatch {
    pub fn single(message: EncodedMessage) -> Self {
        Self {
            messages: vec![message],
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("decode error in topic '{topic}': {reason}")]
    Decode { topic: String, reason: String },
    #[error("encode error for outbox_id={outbox_id}: {reason}")]
    Encode { outbox_id: i64, reason: String },
    #[error("unsupported operation '{op}' in topic '{topic}'")]
    UnsupportedOperation { op: String, topic: String },
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

impl WireError {
    pub fn decode(topic: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::Decode {
            topic: topic.into(),
            reason: reason.into(),
        }
    }

    pub fn encode(outbox_id: i64, reason: impl Into<String>) -> Self {
        Self::Encode {
            outbox_id,
            reason: reason.into(),
        }
    }
}

pub trait WireFormat: Send + Sync {
    fn name(&self) -> &'static str;
    fn decode(&self, raw: &RawMessage) -> Result<Option<InboxRow>, WireError>;
    fn encode(&self, row: &OutboxRow, ctx: &EncodeContext) -> Result<EncodedBatch, WireError>;
}

pub fn from_config(
    config: &serde_json::Value,
) -> Result<Box<dyn WireFormat>, crate::error::RelayError> {
    match config
        .get("wire_format")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("native")
    {
        "native" => Ok(Box::new(NativePgTideFormat::new())),
        "cloudevents" => Ok(Box::new(CloudEventsFormat::from_config(
            config
                .get("wire_config")
                .unwrap_or(&serde_json::Value::Null),
        ))),
        removed @ ("debezium" | "maxwell" | "canal" | "cdc_json") => {
            Err(crate::error::RelayError::UnsupportedSurface {
                surface: format!("wire_format={removed}"),
                alternative: "native or cloudevents".to_string(),
            })
        }
        other => Err(crate::error::RelayError::InvalidConfig {
            name: "wire_format".to_string(),
            reason: format!("unknown wire_format '{other}'"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_config_defaults_to_native() {
        assert_eq!(
            from_config(&serde_json::json!({})).unwrap().name(),
            "native"
        );
    }

    #[test]
    fn from_config_rejects_removed_format() {
        let error = match from_config(&serde_json::json!({"wire_format": "debezium"})) {
            Ok(_) => panic!("removed wire format must fail"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("PGTIDE_CONFIG_UNSUPPORTED_SURFACE"));
    }
}
