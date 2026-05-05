/// Canal wire format decoder (decode-only).
///
/// Canal (https://github.com/alibaba/canal) is an Alibaba MySQL CDC tool that
/// emits JSON messages representing MySQL binlog events.  This decoder handles
/// the Canal JSON envelope and maps it into `InboxRow`.
///
/// Feature-gated: only compiled with `--features canal`.
///
/// Canal envelope shape:
///
/// ```json
/// {
///   "id": 1,
///   "database": "mydb",
///   "table": "users",
///   "pkNames": ["id"],
///   "isDdl": false,
///   "type": "INSERT",
///   "es": 1714029482000,
///   "ts": 1714029482000,
///   "sql": "",
///   "sqlType": { "id": 4 },
///   "mysqlType": { "id": "int" },
///   "data": [{ "id": "7", "name": "alice" }],
///   "old": [{ "name": "alice_old" }]
/// }
/// ```
///
/// Note: Canal serialises all column values as strings.
use serde_json::Value;
use uuid::Uuid;

use super::{EncodeContext, EncodedBatch, InboxRow, OutboxRow, RawMessage, WireError, WireFormat};

/// Configuration for the Canal wire format decoder.
#[derive(Debug, Clone, Default)]
pub struct CanalConfig {
    /// Whether to skip DDL events (ALTER TABLE, etc.). Default: true.
    pub skip_ddl: bool,
}

impl CanalConfig {
    pub fn from_config(config: &Value) -> Self {
        Self {
            skip_ddl: config
                .get("skip_ddl")
                .and_then(|v| v.as_bool())
                .unwrap_or(true),
        }
    }
}

/// Canal JSON decoder (decode-only).
#[derive(Debug)]
pub struct CanalFormat {
    pub config: CanalConfig,
}

impl CanalFormat {
    pub fn new(config: CanalConfig) -> Self {
        Self { config }
    }

    pub fn from_config(config: &Value) -> Self {
        Self::new(CanalConfig::from_config(config))
    }

    /// Map a Canal event type string to a pg_tide op string.
    fn canal_type_to_op(t: &str) -> Option<&'static str> {
        match t.to_uppercase().as_str() {
            "INSERT" => Some("insert"),
            "UPDATE" => Some("update"),
            "DELETE" => Some("delete"),
            _ => None,
        }
    }
}

impl WireFormat for CanalFormat {
    fn name(&self) -> &'static str {
        "canal"
    }

    fn decode(&self, raw: &RawMessage) -> Result<Option<InboxRow>, WireError> {
        let bytes = match &raw.value {
            Some(b) => b,
            None => return Ok(None), // tombstone → skip
        };

        let envelope: Value = serde_json::from_slice(bytes)
            .map_err(|e| WireError::decode(&raw.topic, format!("Canal JSON parse error: {e}")))?;

        // Skip DDL events.
        if self.config.skip_ddl {
            if let Some(true) = envelope.get("isDdl").and_then(|v| v.as_bool()) {
                tracing::debug!(topic = raw.topic, "Canal: skipping DDL event");
                return Ok(None);
            }
        }

        let event_type = envelope
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| WireError::decode(&raw.topic, "Canal envelope missing 'type'"))?;

        let op = match Self::canal_type_to_op(event_type) {
            Some(op) => op,
            None => {
                tracing::debug!(
                    topic = raw.topic,
                    event_type,
                    "Canal: skipping unsupported event type"
                );
                return Ok(None);
            }
        };

        let table = envelope
            .get("table")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        let database = envelope
            .get("database")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        // Canal always wraps data as an array.  Take the first element.
        let data_arr = envelope.get("data").and_then(|v| v.as_array());
        let payload = data_arr
            .and_then(|a| a.first())
            .cloned()
            .unwrap_or(Value::Null);

        // Old data (UPDATE before-state).
        let old_arr = envelope.get("old").and_then(|v| v.as_array());
        let old_payload = old_arr
            .and_then(|a| a.first())
            .cloned()
            .filter(|v| !v.is_null());

        // Timestamp: Canal uses `es` (event timestamp, ms) or `ts`.
        let commit_ts = envelope
            .get("es")
            .or_else(|| envelope.get("ts"))
            .and_then(|v| v.as_i64())
            .and_then(|ms| {
                use chrono::{TimeZone, Utc};
                Utc.timestamp_millis_opt(ms).single()
            });

        let source_position = envelope.get("id").map(|v| v.to_string());

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
            payload,
            old_payload,
            op: op.to_string(),
            commit_ts,
            source_position,
        }))
    }

    fn encode(&self, row: &OutboxRow, _ctx: &EncodeContext) -> Result<EncodedBatch, WireError> {
        // Canal is decode-only.
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

    fn canal_msg(
        topic: &str,
        event_type: &str,
        data: Vec<Value>,
        old: Option<Vec<Value>>,
        is_ddl: bool,
    ) -> RawMessage {
        let mut envelope = json!({
            "id": 1,
            "database": "mydb",
            "table": topic,
            "type": event_type,
            "isDdl": is_ddl,
            "es": 1714029482000_i64,
            "ts": 1714029482000_i64,
            "data": data,
        });
        if let Some(o) = old {
            envelope["old"] = json!(o);
        }
        RawMessage::from_json(topic, &envelope)
    }

    #[test]
    fn test_decode_insert() {
        let fmt = CanalFormat::from_config(&json!({}));
        let raw = canal_msg(
            "users",
            "INSERT",
            vec![json!({"id": "7", "name": "alice"})],
            None,
            false,
        );
        let row = fmt.decode(&raw).unwrap().unwrap();
        assert_eq!(row.op, "insert");
        assert_eq!(row.payload["id"], "7");
        assert_eq!(row.event_type, "mydb.users");
    }

    #[test]
    fn test_decode_update() {
        let fmt = CanalFormat::from_config(&json!({}));
        let raw = canal_msg(
            "users",
            "UPDATE",
            vec![json!({"id": "7", "name": "alice2"})],
            Some(vec![json!({"name": "alice"})]),
            false,
        );
        let row = fmt.decode(&raw).unwrap().unwrap();
        assert_eq!(row.op, "update");
        assert_eq!(row.payload["name"], "alice2");
        assert_eq!(row.old_payload.as_ref().unwrap()["name"], "alice");
    }

    #[test]
    fn test_decode_delete() {
        let fmt = CanalFormat::from_config(&json!({}));
        let raw = canal_msg("users", "DELETE", vec![json!({"id": "7"})], None, false);
        let row = fmt.decode(&raw).unwrap().unwrap();
        assert_eq!(row.op, "delete");
    }

    #[test]
    fn test_decode_ddl_skipped() {
        let fmt = CanalFormat::from_config(&json!({"skip_ddl": true}));
        let raw = canal_msg("users", "ALTER", vec![], None, true);
        let row = fmt.decode(&raw).unwrap();
        assert!(row.is_none());
    }

    #[test]
    fn test_decode_tombstone_returns_none() {
        let fmt = CanalFormat::from_config(&json!({}));
        let raw = RawMessage::tombstone("users", b"key".to_vec());
        assert!(fmt.decode(&raw).unwrap().is_none());
    }

    #[test]
    fn test_decode_commit_ts_extracted() {
        let fmt = CanalFormat::from_config(&json!({}));
        let raw = canal_msg("users", "INSERT", vec![json!({"id": "1"})], None, false);
        let row = fmt.decode(&raw).unwrap().unwrap();
        assert!(row.commit_ts.is_some());
    }

    #[test]
    fn test_encode_returns_error() {
        let fmt = CanalFormat::from_config(&json!({}));
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
        let fmt = CanalFormat::from_config(&json!({}));
        assert_eq!(fmt.name(), "canal");
    }
}
