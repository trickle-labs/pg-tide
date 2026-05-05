/// Maxwell wire format decoder (decode-only).
///
/// Maxwell (https://maxwells-daemon.io) is a MySQL CDC tool that emits JSON
/// messages in a format slightly different from Debezium.  This decoder
/// handles the Maxwell JSON envelope and maps it into `InboxRow`.
///
/// Feature-gated: only compiled with `--features maxwell`.
///
/// Maxwell envelope shape:
///
/// ```json
/// {
///   "database": "mydb",
///   "table": "users",
///   "type": "insert",
///   "ts": 1714029482,
///   "xid": 12345,
///   "data": { "id": 7, "name": "alice" },
///   "old": { "name": "alice_old" }   // UPDATE only
/// }
/// ```
use serde_json::Value;
use uuid::Uuid;

use super::{EncodeContext, EncodedBatch, InboxRow, OutboxRow, RawMessage, WireError, WireFormat};

/// Configuration for the Maxwell wire format decoder.
#[derive(Debug, Clone, Default)]
pub struct MaxwellConfig {
    /// Whether to map Maxwell "bootstrap-insert" events as INSERT.
    pub treat_bootstrap_as_insert: bool,
}

impl MaxwellConfig {
    pub fn from_config(config: &Value) -> Self {
        let treat_bootstrap_as_insert = config
            .get("treat_bootstrap_as_insert")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        Self {
            treat_bootstrap_as_insert,
        }
    }
}

/// Maxwell JSON decoder (decode-only).
#[derive(Debug)]
pub struct MaxwellFormat {
    pub config: MaxwellConfig,
}

impl MaxwellFormat {
    pub fn new(config: MaxwellConfig) -> Self {
        Self { config }
    }

    pub fn from_config(config: &Value) -> Self {
        Self::new(MaxwellConfig::from_config(config))
    }

    fn maxwell_type_to_op(t: &str, bootstrap_as_insert: bool) -> Option<&'static str> {
        match t {
            "insert" => Some("insert"),
            "update" => Some("update"),
            "delete" => Some("delete"),
            "bootstrap-insert" if bootstrap_as_insert => Some("insert"),
            _ => None,
        }
    }
}

impl WireFormat for MaxwellFormat {
    fn name(&self) -> &'static str {
        "maxwell"
    }

    fn decode(&self, raw: &RawMessage) -> Result<Option<InboxRow>, WireError> {
        let bytes = match &raw.value {
            Some(b) => b,
            None => return Ok(None), // tombstone → skip
        };

        let envelope: Value = serde_json::from_slice(bytes)
            .map_err(|e| WireError::decode(&raw.topic, format!("Maxwell JSON parse error: {e}")))?;

        let event_type = envelope
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| WireError::decode(&raw.topic, "Maxwell envelope missing 'type'"))?;

        let op = match Self::maxwell_type_to_op(event_type, self.config.treat_bootstrap_as_insert) {
            Some(op) => op,
            None => {
                tracing::debug!(
                    topic = raw.topic,
                    event_type,
                    "Maxwell: skipping unsupported event type"
                );
                return Ok(None);
            }
        };

        let data = envelope.get("data").cloned().unwrap_or(Value::Null);

        let old_payload = envelope.get("old").cloned().filter(|v| !v.is_null());

        let table = envelope
            .get("table")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        let database = envelope
            .get("database")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        let commit_ts = envelope.get("ts").and_then(|v| v.as_i64()).and_then(|s| {
            use chrono::{TimeZone, Utc};
            Utc.timestamp_opt(s, 0).single()
        });

        let source_position = envelope
            .get("xid")
            .map(|v| v.to_string())
            .or_else(|| envelope.get("position").map(|v| v.to_string()));

        let event_id = raw
            .key
            .as_deref()
            .and_then(|k| std::str::from_utf8(k).ok())
            .map(|s| s.to_string())
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        let event_type_name = format!("{database}.{table}");

        Ok(Some(InboxRow {
            event_id,
            event_type: event_type_name,
            payload: data,
            old_payload,
            op: op.to_string(),
            commit_ts,
            source_position,
        }))
    }

    fn encode(&self, row: &OutboxRow, _ctx: &EncodeContext) -> Result<EncodedBatch, WireError> {
        // Maxwell is decode-only. Return an error to prevent accidental forward use.
        Err(WireError::UnsupportedOperation {
            op: "encode".to_string(),
            topic: row.stream_table.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn maxwell_msg(topic: &str, event_type: &str, data: Value, old: Option<Value>) -> RawMessage {
        let mut envelope = json!({
            "database": "mydb",
            "table": topic,
            "type": event_type,
            "ts": 1714029482_i64,
            "xid": 12345,
            "data": data,
        });
        if let Some(o) = old {
            envelope["old"] = o;
        }
        RawMessage::from_json(topic, &envelope)
    }

    #[test]
    fn test_decode_insert() {
        let fmt = MaxwellFormat::from_config(&json!({}));
        let raw = maxwell_msg("users", "insert", json!({"id": 7, "name": "alice"}), None);
        let row = fmt.decode(&raw).unwrap().unwrap();
        assert_eq!(row.op, "insert");
        assert_eq!(row.payload["id"], 7);
        assert_eq!(row.event_type, "mydb.users");
    }

    #[test]
    fn test_decode_update() {
        let fmt = MaxwellFormat::from_config(&json!({}));
        let raw = maxwell_msg(
            "users",
            "update",
            json!({"id": 7, "name": "alice2"}),
            Some(json!({"name": "alice"})),
        );
        let row = fmt.decode(&raw).unwrap().unwrap();
        assert_eq!(row.op, "update");
        assert_eq!(row.payload["name"], "alice2");
        assert_eq!(row.old_payload.as_ref().unwrap()["name"], "alice");
    }

    #[test]
    fn test_decode_delete() {
        let fmt = MaxwellFormat::from_config(&json!({}));
        let raw = maxwell_msg("users", "delete", json!({"id": 7}), None);
        let row = fmt.decode(&raw).unwrap().unwrap();
        assert_eq!(row.op, "delete");
    }

    #[test]
    fn test_decode_bootstrap_insert_as_insert() {
        let fmt = MaxwellFormat::from_config(&json!({"treat_bootstrap_as_insert": true}));
        let raw = maxwell_msg("users", "bootstrap-insert", json!({"id": 1}), None);
        let row = fmt.decode(&raw).unwrap().unwrap();
        assert_eq!(row.op, "insert");
    }

    #[test]
    fn test_decode_bootstrap_insert_skip_when_disabled() {
        let fmt = MaxwellFormat::from_config(&json!({"treat_bootstrap_as_insert": false}));
        let raw = maxwell_msg("users", "bootstrap-insert", json!({"id": 1}), None);
        let row = fmt.decode(&raw).unwrap();
        assert!(row.is_none());
    }

    #[test]
    fn test_decode_tombstone_returns_none() {
        let fmt = MaxwellFormat::from_config(&json!({}));
        let raw = RawMessage::tombstone("users", b"key".to_vec());
        assert!(fmt.decode(&raw).unwrap().is_none());
    }

    #[test]
    fn test_decode_commit_ts_extracted() {
        let fmt = MaxwellFormat::from_config(&json!({}));
        let raw = maxwell_msg("users", "insert", json!({"id": 1}), None);
        let row = fmt.decode(&raw).unwrap().unwrap();
        assert!(row.commit_ts.is_some());
    }

    #[test]
    fn test_decode_source_position_extracted() {
        let fmt = MaxwellFormat::from_config(&json!({}));
        let raw = maxwell_msg("users", "insert", json!({"id": 1}), None);
        let row = fmt.decode(&raw).unwrap().unwrap();
        assert!(row.source_position.is_some());
    }

    #[test]
    fn test_encode_returns_error() {
        let fmt = MaxwellFormat::from_config(&json!({}));
        let ctx = EncodeContext::default();
        let row = OutboxRow {
            outbox_id: 1,
            stream_table: "users".to_string(),
            database: "app".to_string(),
            schema_name: "public".to_string(),
            op: "insert".to_string(),
            new_row: Some(json!({"id": 1})),
            old_row: None,
            commit_ts: None,
            source_lsn: None,
        };
        assert!(fmt.encode(&row, &ctx).is_err());
    }

    #[test]
    fn test_name() {
        let fmt = MaxwellFormat::from_config(&json!({}));
        assert_eq!(fmt.name(), "maxwell");
    }
}
