/// Custom CDC JSON wire format with user-supplied JSONPath expressions.
///
/// Allows users to adapt any CDC source by providing JSONPath expressions
/// that map fields in the source message to the `InboxRow` fields expected
/// by pg_tide.
///
/// Feature-gated: only compiled with `--features cdc-json`.
///
/// Configuration example:
///
/// ```json
/// {
///   "wire_format": "cdc_json",
///   "wire_config": {
///     "op_path": "$.event_type",
///     "op_map": { "created": "insert", "modified": "update", "removed": "delete" },
///     "payload_path": "$.data",
///     "old_payload_path": "$.previous",
///     "event_id_path": "$.id",
///     "event_type_path": "$.resource",
///     "commit_ts_path": "$.occurred_at",
///     "commit_ts_format": "rfc3339",
///     "source_position_path": "$.sequence"
///   }
/// }
/// ```
///
/// All path fields are optional.  When absent, defaults are used:
/// - `op_path`: `"$.op"` (with values `insert`, `update`, `delete` expected)
/// - `payload_path`: `"$"` (entire message is the payload)
/// - `event_id_path`: auto-generated UUID
/// - `event_type_path`: topic name
///
/// ## JSONPath expressions
///
/// Uses simple dot-notation paths (e.g. `$.field.sub`) without array indexing.
/// This is intentionally minimal to avoid a heavy JSONPath dependency.
use serde_json::Value;
use uuid::Uuid;

use super::{
    EncodeContext, EncodedBatch, EncodedMessage, InboxRow, OutboxRow, RawMessage, WireError,
    WireFormat,
};

/// Timestamp format for the `commit_ts_path` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimestampFormat {
    /// RFC 3339 / ISO 8601 string.
    #[default]
    Rfc3339,
    /// Unix epoch in seconds (i64).
    UnixSeconds,
    /// Unix epoch in milliseconds (i64).
    UnixMillis,
}

impl TimestampFormat {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s {
            "unix_seconds" | "unix" => Self::UnixSeconds,
            "unix_millis" | "epoch_ms" => Self::UnixMillis,
            _ => Self::Rfc3339,
        }
    }
}

/// Configuration for the custom CDC JSON wire format.
#[derive(Debug, Clone)]
pub struct CdcJsonConfig {
    /// JSONPath to the operation field.
    pub op_path: String,
    /// Map from source op values to pg_tide op strings.
    pub op_map: std::collections::HashMap<String, String>,
    /// JSONPath to the payload (new row) field.
    pub payload_path: String,
    /// JSONPath to the old payload field (UPDATE/DELETE before-state).
    pub old_payload_path: Option<String>,
    /// JSONPath to the event_id field.
    pub event_id_path: Option<String>,
    /// JSONPath to the event_type field.
    pub event_type_path: Option<String>,
    /// JSONPath to the commit timestamp field.
    pub commit_ts_path: Option<String>,
    /// Format of the commit timestamp.
    pub commit_ts_format: TimestampFormat,
    /// JSONPath to the source position field.
    pub source_position_path: Option<String>,
}

impl Default for CdcJsonConfig {
    fn default() -> Self {
        let mut op_map = std::collections::HashMap::new();
        op_map.insert("insert".to_string(), "insert".to_string());
        op_map.insert("update".to_string(), "update".to_string());
        op_map.insert("delete".to_string(), "delete".to_string());
        op_map.insert("c".to_string(), "insert".to_string());
        op_map.insert("u".to_string(), "update".to_string());
        op_map.insert("d".to_string(), "delete".to_string());
        Self {
            op_path: "$.op".to_string(),
            op_map,
            payload_path: "$".to_string(),
            old_payload_path: None,
            event_id_path: None,
            event_type_path: None,
            commit_ts_path: None,
            commit_ts_format: TimestampFormat::Rfc3339,
            source_position_path: None,
        }
    }
}

impl CdcJsonConfig {
    pub fn from_config(config: &Value) -> Self {
        let mut cfg = Self::default();

        if let Some(p) = config.get("op_path").and_then(|v| v.as_str()) {
            cfg.op_path = p.to_string();
        }

        if let Some(map) = config.get("op_map").and_then(|v| v.as_object()) {
            cfg.op_map.clear();
            for (k, v) in map {
                if let Some(s) = v.as_str() {
                    cfg.op_map.insert(k.clone(), s.to_string());
                }
            }
        }

        if let Some(p) = config.get("payload_path").and_then(|v| v.as_str()) {
            cfg.payload_path = p.to_string();
        }

        cfg.old_payload_path = config
            .get("old_payload_path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        cfg.event_id_path = config
            .get("event_id_path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        cfg.event_type_path = config
            .get("event_type_path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        cfg.commit_ts_path = config
            .get("commit_ts_path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        if let Some(fmt) = config.get("commit_ts_format").and_then(|v| v.as_str()) {
            cfg.commit_ts_format = TimestampFormat::from_str(fmt);
        }

        cfg.source_position_path = config
            .get("source_position_path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        cfg
    }
}

/// Extract a value from a JSON document using a simple dot-path expression.
///
/// Supports `$.field.sub.sub2` notation.  Returns `None` if any path
/// segment is missing or the value is `null`.
pub fn extract_path<'a>(doc: &'a Value, path: &str) -> Option<&'a Value> {
    let path = path.trim_start_matches("$.");
    if path == "$" || path.is_empty() {
        return Some(doc);
    }
    let mut current = doc;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    if current.is_null() {
        None
    } else {
        Some(current)
    }
}

/// Custom CDC JSON format implementation.
#[derive(Debug)]
pub struct CdcJsonFormat {
    pub config: CdcJsonConfig,
}

impl CdcJsonFormat {
    pub fn new(config: CdcJsonConfig) -> Self {
        Self { config }
    }

    pub fn from_config(config: &Value) -> Self {
        Self::new(CdcJsonConfig::from_config(config))
    }

    fn resolve_op(&self, doc: &Value, topic: &str) -> Result<String, WireError> {
        let raw_op = extract_path(doc, &self.config.op_path)
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                WireError::decode(
                    topic,
                    format!("cdc_json: 'op' not found at path '{}'", self.config.op_path),
                )
            })?;

        self.config.op_map.get(raw_op).cloned().ok_or_else(|| {
            WireError::decode(topic, format!("cdc_json: unknown op value '{raw_op}'"))
        })
    }

    fn parse_commit_ts(&self, raw: &Value) -> Option<chrono::DateTime<chrono::Utc>> {
        match self.config.commit_ts_format {
            TimestampFormat::Rfc3339 => raw
                .as_str()
                .and_then(|s| s.parse::<chrono::DateTime<chrono::Utc>>().ok()),
            TimestampFormat::UnixSeconds => {
                use chrono::{TimeZone, Utc};
                raw.as_i64().and_then(|s| Utc.timestamp_opt(s, 0).single())
            }
            TimestampFormat::UnixMillis => {
                use chrono::{TimeZone, Utc};
                raw.as_i64()
                    .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
            }
        }
    }
}

impl WireFormat for CdcJsonFormat {
    fn name(&self) -> &'static str {
        "cdc_json"
    }

    fn decode(&self, raw: &RawMessage) -> Result<Option<InboxRow>, WireError> {
        let bytes = match &raw.value {
            Some(b) => b,
            None => return Ok(None),
        };

        let doc: Value = serde_json::from_slice(bytes).map_err(|e| {
            WireError::decode(&raw.topic, format!("cdc_json: JSON parse error: {e}"))
        })?;

        let op = self.resolve_op(&doc, &raw.topic)?;

        let payload = extract_path(&doc, &self.config.payload_path)
            .cloned()
            .unwrap_or(doc.clone());

        let old_payload = self
            .config
            .old_payload_path
            .as_deref()
            .and_then(|p| extract_path(&doc, p))
            .cloned();

        let event_id = self
            .config
            .event_id_path
            .as_deref()
            .and_then(|p| extract_path(&doc, p))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| {
                raw.key
                    .as_deref()
                    .and_then(|k| std::str::from_utf8(k).ok())
                    .map(|s| s.to_string())
            })
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        let event_type = self
            .config
            .event_type_path
            .as_deref()
            .and_then(|p| extract_path(&doc, p))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| raw.topic.clone());

        let commit_ts = self
            .config
            .commit_ts_path
            .as_deref()
            .and_then(|p| extract_path(&doc, p))
            .cloned()
            .and_then(|v| self.parse_commit_ts(&v));

        let source_position = self
            .config
            .source_position_path
            .as_deref()
            .and_then(|p| extract_path(&doc, p))
            .map(|v| v.to_string());

        Ok(Some(InboxRow {
            event_id,
            event_type,
            payload,
            old_payload,
            op,
            commit_ts,
            source_position,
        }))
    }

    fn encode(&self, row: &OutboxRow, ctx: &EncodeContext) -> Result<EncodedBatch, WireError> {
        // For the encode direction, emit a simple JSON document applying
        // the inverse of the configured paths where possible.
        let topic = ctx.resolve_topic(row);
        let op_key = self
            .config
            .op_map
            .iter()
            .find_map(|(k, v)| if v == &row.op { Some(k.clone()) } else { None })
            .unwrap_or_else(|| row.op.clone());

        let payload_val = row
            .new_row
            .as_ref()
            .or(row.old_row.as_ref())
            .cloned()
            .unwrap_or(Value::Null);

        let mut doc = serde_json::json!({
            "op": op_key,
            "data": payload_val,
        });

        if let Some(old) = &row.old_row {
            doc["previous"] = old.clone();
        }

        let key = Some(format!("{}:{}", row.stream_table, row.outbox_id).into_bytes());
        let value = serde_json::to_vec(&doc)
            .map_err(|e| WireError::encode(row.outbox_id, e.to_string()))?;

        Ok(EncodedBatch::single(EncodedMessage {
            topic,
            key,
            value: Some(value),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_extract_path_root() {
        let doc = json!({"a": {"b": 42}});
        let v = extract_path(&doc, "$");
        assert!(v.is_some());
    }

    #[test]
    fn test_extract_path_nested() {
        let doc = json!({"a": {"b": 42}});
        let v = extract_path(&doc, "$.a.b").unwrap();
        assert_eq!(v.as_i64().unwrap(), 42);
    }

    #[test]
    fn test_extract_path_missing_returns_none() {
        let doc = json!({"a": 1});
        assert!(extract_path(&doc, "$.x.y").is_none());
    }

    #[test]
    fn test_decode_default_config() {
        let fmt = CdcJsonFormat::from_config(&json!({}));
        let raw = RawMessage::from_json("orders", &json!({"op": "insert", "id": 1, "amount": 99}));
        let row = fmt.decode(&raw).unwrap().unwrap();
        assert_eq!(row.op, "insert");
        assert_eq!(row.event_type, "orders");
    }

    #[test]
    fn test_decode_custom_paths() {
        let fmt = CdcJsonFormat::from_config(&json!({
            "op_path": "$.event_type",
            "op_map": { "created": "insert", "deleted": "delete" },
            "payload_path": "$.data",
            "old_payload_path": "$.previous",
            "event_id_path": "$.id",
            "event_type_path": "$.resource",
            "commit_ts_path": "$.occurred_at",
            "commit_ts_format": "rfc3339",
            "source_position_path": "$.seq",
        }));

        let raw = RawMessage::from_json(
            "raw-topic",
            &json!({
                "event_type": "created",
                "id": "evt-123",
                "resource": "orders",
                "data": {"amount": 55},
                "occurred_at": "2024-01-15T10:00:00Z",
                "seq": "100"
            }),
        );

        let row = fmt.decode(&raw).unwrap().unwrap();
        assert_eq!(row.op, "insert");
        assert_eq!(row.event_id, "evt-123");
        assert_eq!(row.event_type, "orders");
        assert_eq!(row.payload["amount"], 55);
        assert!(row.commit_ts.is_some());
        assert!(row.source_position.is_some());
    }

    #[test]
    fn test_decode_unix_millis_timestamp() {
        let fmt = CdcJsonFormat::from_config(&json!({
            "commit_ts_path": "$.ts",
            "commit_ts_format": "unix_millis",
        }));
        let raw = RawMessage::from_json("topic", &json!({"op": "insert", "ts": 1714029482000_i64}));
        let row = fmt.decode(&raw).unwrap().unwrap();
        assert!(row.commit_ts.is_some());
    }

    #[test]
    fn test_decode_unknown_op_returns_error() {
        let fmt = CdcJsonFormat::from_config(&json!({}));
        let raw = RawMessage::from_json("topic", &json!({"op": "unknown_op"}));
        assert!(fmt.decode(&raw).is_err());
    }

    #[test]
    fn test_encode() {
        let fmt = CdcJsonFormat::from_config(&json!({}));
        let ctx = EncodeContext::default();
        let row = OutboxRow {
            outbox_id: 1,
            stream_table: "orders".to_string(),
            database: "app".to_string(),
            schema_name: "public".to_string(),
            op: "insert".to_string(),
            new_row: Some(json!({"id": 1})),
            old_row: None,
            commit_ts: None,
            source_lsn: None,
        };
        let batch = fmt.encode(&row, &ctx).unwrap();
        assert_eq!(batch.messages.len(), 1);
        let v: Value = serde_json::from_slice(batch.messages[0].value.as_ref().unwrap()).unwrap();
        assert!(v.get("op").is_some());
    }

    #[test]
    fn test_tombstone_returns_none() {
        let fmt = CdcJsonFormat::from_config(&json!({}));
        let raw = RawMessage::tombstone("topic", b"key".to_vec());
        assert!(fmt.decode(&raw).unwrap().is_none());
    }

    #[test]
    fn test_name() {
        let fmt = CdcJsonFormat::from_config(&json!({}));
        assert_eq!(fmt.name(), "cdc_json");
    }
}
