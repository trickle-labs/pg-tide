/// CloudEvents v1.0 wire format for pg-tide-relay (v0.14.0).
///
/// Encodes outbox messages as CloudEvents v1.0 JSON envelopes and decodes
/// incoming CloudEvents envelopes into pg_tide inbox rows.
///
/// Spec: https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md
///
/// # Forward path (outbox → CloudEvents)
///
/// Each `OutboxRow` is wrapped in a CloudEvents v1.0 JSON envelope:
/// ```json
/// {
///   "specversion": "1.0",
///   "type":        "io.pgtide.{op}",
///   "source":      "/pgtide/{server}/{schema}/{stream_table}",
///   "id":          "{outbox_id}",
///   "time":        "<commit_ts or now>",
///   "datacontenttype": "application/json",
///   "data":        { ...outbox payload... }
/// }
/// ```
///
/// # Reverse path (CloudEvents → inbox)
///
/// Incoming messages whose JSON root contains `"specversion": "1.0"` are
/// treated as CloudEvents. The `id` field maps to `event_id`, the `type`
/// field to `event_type`, and `data` to `payload`.
use chrono::Utc;
use serde_json::{json, Value};
use uuid::Uuid;

use super::{
    EncodeContext, EncodedBatch, EncodedMessage, InboxRow, OutboxRow, RawMessage, WireError,
    WireFormat,
};

/// Default CloudEvents source URI template.
const DEFAULT_SOURCE_TEMPLATE: &str = "/pgtide/{server}/{schema}/{stream_table}";

/// Configuration for the CloudEvents wire format.
#[derive(Debug, Clone)]
pub struct CloudEventsConfig {
    /// `source` URI template. Supports `{server}`, `{schema}`, `{stream_table}`.
    pub source_template: String,
    /// `type` prefix. Default: `"io.pgtide"`. Final type: `"{prefix}.{op}"`.
    pub type_prefix: String,
    /// Whether to include the pg_tide operation in `ce-op` extension attribute.
    pub include_op_extension: bool,
}

impl Default for CloudEventsConfig {
    fn default() -> Self {
        Self {
            source_template: DEFAULT_SOURCE_TEMPLATE.to_string(),
            type_prefix: "io.pgtide".to_string(),
            include_op_extension: true,
        }
    }
}

impl CloudEventsConfig {
    pub fn from_config(cfg: &Value) -> Self {
        let mut c = Self::default();
        if let Some(s) = cfg.get("source_template").and_then(|v| v.as_str()) {
            c.source_template = s.to_string();
        }
        if let Some(s) = cfg.get("type_prefix").and_then(|v| v.as_str()) {
            c.type_prefix = s.to_string();
        }
        if let Some(b) = cfg.get("include_op_extension").and_then(|v| v.as_bool()) {
            c.include_op_extension = b;
        }
        c
    }

    fn resolve_source(&self, server: &str, schema: &str, stream_table: &str) -> String {
        self.source_template
            .replace("{server}", server)
            .replace("{schema}", schema)
            .replace("{stream_table}", stream_table)
    }
}

/// CloudEvents v1.0 wire format implementation.
#[derive(Debug)]
pub struct CloudEventsFormat {
    config: CloudEventsConfig,
}

impl CloudEventsFormat {
    pub fn new() -> Self {
        Self {
            config: CloudEventsConfig::default(),
        }
    }

    pub fn from_config(cfg: &Value) -> Self {
        Self {
            config: CloudEventsConfig::from_config(cfg),
        }
    }
}

impl Default for CloudEventsFormat {
    fn default() -> Self {
        Self::new()
    }
}

impl WireFormat for CloudEventsFormat {
    fn name(&self) -> &'static str {
        "cloudevents"
    }

    fn decode(&self, raw: &RawMessage) -> Result<Option<InboxRow>, WireError> {
        let value_bytes = match &raw.value {
            Some(b) => b,
            None => return Ok(None), // tombstone — skip
        };

        let envelope: Value = serde_json::from_slice(value_bytes).map_err(|e| {
            WireError::decode(&raw.topic, format!("cloudevents: JSON parse failed: {e}"))
        })?;

        // Validate specversion.
        let specversion = envelope
            .get("specversion")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if specversion != "1.0" {
            return Err(WireError::decode(
                &raw.topic,
                format!(
                    "cloudevents: unsupported specversion '{}', expected '1.0'",
                    specversion
                ),
            ));
        }

        let event_id = envelope
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let event_id = if event_id.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            event_id
        };

        let event_type = envelope
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        // Extract op from the `ce-op` extension attribute or derive from `type`.
        let op = envelope
            .get("ce-op")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                // Try to derive op from type suffix (e.g. "io.pgtide.insert" → "insert").
                let t = &event_type;
                for suffix in &["insert", "update", "delete"] {
                    if t.ends_with(suffix) {
                        return suffix.to_string();
                    }
                }
                "event".to_string()
            });

        // `data` contains the application payload.
        let payload = envelope
            .get("data")
            .cloned()
            .unwrap_or(serde_json::Value::Object(Default::default()));

        // Commit timestamp from `time`.
        let commit_ts = envelope
            .get("time")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<chrono::DateTime<Utc>>().ok());

        Ok(Some(InboxRow {
            event_id,
            event_type,
            payload,
            old_payload: None,
            op,
            commit_ts,
            source_position: None,
        }))
    }

    fn encode(&self, row: &OutboxRow, ctx: &EncodeContext) -> Result<EncodedBatch, WireError> {
        let topic = ctx.resolve_topic(row);

        let source =
            self.config
                .resolve_source(&ctx.server_name, &row.schema_name, &row.stream_table);

        let ce_type = format!("{}.{}", self.config.type_prefix, row.op);

        let time = row
            .commit_ts
            .map(|ts| ts.to_rfc3339())
            .unwrap_or_else(|| Utc::now().to_rfc3339());

        let data = row
            .new_row
            .as_ref()
            .or(row.old_row.as_ref())
            .cloned()
            .unwrap_or(serde_json::Value::Null);

        let mut envelope = json!({
            "specversion":       "1.0",
            "type":              ce_type,
            "source":            source,
            "id":                row.outbox_id.to_string(),
            "time":              time,
            "datacontenttype":   "application/json",
            "data":              data,
        });

        // Add op as a CloudEvents extension attribute.
        if self.config.include_op_extension {
            envelope["ce-op"] = json!(row.op);
        }

        let key = Some(format!("{}:{}", row.stream_table, row.outbox_id).into_bytes());

        let encoded = EncodedMessage {
            topic,
            key,
            value: Some(
                serde_json::to_vec(&envelope)
                    .map_err(|e| WireError::encode(row.outbox_id, e.to_string()))?,
            ),
        };

        Ok(EncodedBatch::single(encoded))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_encode_produces_cloudevents_envelope() {
        let fmt = CloudEventsFormat::new();
        let ctx = EncodeContext::default();
        let row = OutboxRow {
            outbox_id: 42,
            stream_table: "orders".to_string(),
            database: "app".to_string(),
            schema_name: "public".to_string(),
            op: "insert".to_string(),
            new_row: Some(json!({"order_id": 1, "total": 99})),
            old_row: None,
            commit_ts: None,
            source_lsn: None,
        };

        let batch = fmt.encode(&row, &ctx).unwrap();
        assert_eq!(batch.messages.len(), 1);

        let msg = &batch.messages[0];
        let envelope: serde_json::Value =
            serde_json::from_slice(msg.value.as_ref().unwrap()).unwrap();

        assert_eq!(envelope["specversion"], "1.0");
        assert_eq!(envelope["id"], "42");
        assert_eq!(envelope["type"], "io.pgtide.insert");
        assert_eq!(envelope["datacontenttype"], "application/json");
        assert_eq!(envelope["data"]["order_id"], 1);
        assert_eq!(envelope["ce-op"], "insert");
    }

    #[test]
    fn test_decode_valid_cloudevents_envelope() {
        let fmt = CloudEventsFormat::new();
        let payload = json!({
            "specversion": "1.0",
            "id":          "evt-abc-123",
            "type":        "io.pgtide.update",
            "source":      "/pgtide/pg-tide/public/orders",
            "time":        "2024-01-01T12:00:00Z",
            "datacontenttype": "application/json",
            "ce-op":       "update",
            "data":        {"order_id": 5, "status": "shipped"},
        });

        let raw = RawMessage::from_json("orders", &payload);
        let row = fmt.decode(&raw).unwrap().unwrap();

        assert_eq!(row.event_id, "evt-abc-123");
        assert_eq!(row.event_type, "io.pgtide.update");
        assert_eq!(row.op, "update");
        assert_eq!(row.payload["order_id"], 5);
    }

    #[test]
    fn test_decode_wrong_specversion_errors() {
        let fmt = CloudEventsFormat::new();
        let payload = json!({
            "specversion": "0.3",
            "id": "old-evt",
            "type": "io.example.event",
        });
        let raw = RawMessage::from_json("test", &payload);
        let result = fmt.decode(&raw);
        assert!(result.is_err(), "unknown specversion must return an error");
    }

    #[test]
    fn test_decode_tombstone_returns_none() {
        let fmt = CloudEventsFormat::new();
        let raw = RawMessage::tombstone("orders", b"key".to_vec());
        let result = fmt.decode(&raw).unwrap();
        assert!(result.is_none(), "tombstone must return None");
    }

    #[test]
    fn test_op_derived_from_type_suffix() {
        let fmt = CloudEventsFormat::new();
        let payload = json!({
            "specversion": "1.0",
            "id": "x",
            "type": "com.example.db.delete",
            "source": "/db",
            "data": {},
        });
        let raw = RawMessage::from_json("test", &payload);
        let row = fmt.decode(&raw).unwrap().unwrap();
        assert_eq!(row.op, "delete");
    }

    #[test]
    fn test_from_config_respects_type_prefix() {
        let cfg = json!({"type_prefix": "com.example"});
        let fmt = CloudEventsFormat::from_config(&cfg);
        assert_eq!(fmt.config.type_prefix, "com.example");
    }
}
