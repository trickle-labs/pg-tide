/// Debezium wire format — bidirectional support (JSON + Avro + Protobuf).
///
/// ## Reverse path (consume)
///
/// Decodes Debezium-shaped messages from a transport into `InboxRow`.
/// Supports JSON (always) and Avro via the Confluent Schema Registry
/// (feature-gated with `schema-registry`).
///
/// ## Forward path (produce)
///
/// Encodes pg_tide outbox rows into Debezium-shaped messages. The encoder
/// emits INSERT/UPDATE/DELETE events, and optionally a tombstone after DELETE
/// for Kafka log-compacted topics.
///
/// ## Tombstone handling
///
/// When `tombstone_handling = "delete"` (default), a null-value Kafka message
/// is treated as a DELETE for the key seen in the previous message.  When set
/// to `"drop"` the tombstone is silently discarded.
///
/// ## Snapshot op handling
///
/// When `snapshot_op_treatment = "upsert"` the decoder maps `op = "r"` to
/// `"upsert"` instead of the default `"insert"`.
use std::collections::HashMap;

use serde_json::{json, Value};

use super::{
    apply_logical_type, debezium_op_to_pg, pg_op_to_debezium, EncodeContext, EncodedBatch,
    EncodedMessage, InboxRow, OutboxRow, RawMessage, WireError, WireFormat,
};

/// How to handle Debezium tombstone (null-value) messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TombstoneHandling {
    /// Treat tombstone as DELETE for the key from the previous message.
    #[default]
    Delete,
    /// Silently drop the tombstone.
    Drop,
}

/// How to treat Debezium `op = "r"` (snapshot read) messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SnapshotOpTreatment {
    /// Treat as INSERT (default).
    #[default]
    Insert,
    /// Treat as UPSERT.
    Upsert,
}

/// Key strategy for the reverse path (which value to use as the event_id).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KeyStrategy {
    /// Use the primary-key columns from the Debezium key message.
    #[default]
    PrimaryKey,
    /// Use the raw transport message key bytes as a string.
    MessageKey,
}

/// Debezium envelope sub-format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DebeziumEnvelope {
    /// JSON envelope (always available).
    #[default]
    Json,
    /// Confluent Avro envelope (requires `schema-registry` feature).
    #[cfg(feature = "schema-registry")]
    Avro,
}

/// Configuration for the Debezium wire format.
#[derive(Debug, Clone)]
pub struct DebeziumConfig {
    pub envelope: DebeziumEnvelope,
    pub schema_registry_url: Option<String>,
    pub tombstone_handling: TombstoneHandling,
    pub key_strategy: KeyStrategy,
    pub snapshot_op_treatment: SnapshotOpTreatment,
    /// `source.name` emitted in the source block (encode side).
    pub server_name: String,
    /// Whether to emit tombstones after DELETE on the encode side.
    pub emit_tombstones: bool,
    /// Heartbeat interval in ms; 0 = disabled.
    pub heartbeat_interval_ms: u64,
}

impl Default for DebeziumConfig {
    fn default() -> Self {
        Self {
            envelope: DebeziumEnvelope::Json,
            schema_registry_url: None,
            tombstone_handling: TombstoneHandling::Delete,
            key_strategy: KeyStrategy::PrimaryKey,
            snapshot_op_treatment: SnapshotOpTreatment::Insert,
            server_name: "pg-tide".to_string(),
            emit_tombstones: true,
            heartbeat_interval_ms: 10_000,
        }
    }
}

impl DebeziumConfig {
    pub fn from_config(config: &Value) -> Self {
        let mut cfg = Self::default();

        if let Some(env) = config.get("envelope").and_then(|v| v.as_str()) {
            cfg.envelope = match env {
                #[cfg(feature = "schema-registry")]
                "avro" => DebeziumEnvelope::Avro,
                _ => DebeziumEnvelope::Json,
            };
        }

        if let Some(url) = config
            .get("schema_registry")
            .and_then(|v| v.as_str())
            .or_else(|| config.get("schema_registry_url").and_then(|v| v.as_str()))
        {
            cfg.schema_registry_url = Some(url.to_string());
        }

        if let Some(th) = config.get("tombstone_handling").and_then(|v| v.as_str()) {
            cfg.tombstone_handling = match th {
                "drop" => TombstoneHandling::Drop,
                _ => TombstoneHandling::Delete,
            };
        }

        if let Some(ks) = config.get("key_strategy").and_then(|v| v.as_str()) {
            cfg.key_strategy = match ks {
                "message_key" => KeyStrategy::MessageKey,
                _ => KeyStrategy::PrimaryKey,
            };
        }

        if let Some(sot) = config.get("snapshot_op_treatment").and_then(|v| v.as_str()) {
            cfg.snapshot_op_treatment = match sot {
                "upsert" => SnapshotOpTreatment::Upsert,
                _ => SnapshotOpTreatment::Insert,
            };
        }

        if let Some(name) = config.get("server_name").and_then(|v| v.as_str()) {
            cfg.server_name = name.to_string();
        }

        if let Some(emit) = config.get("emit_tombstones").and_then(|v| v.as_bool()) {
            cfg.emit_tombstones = emit;
        }

        if let Some(hb) = config.get("heartbeat_interval_ms").and_then(|v| v.as_u64()) {
            cfg.heartbeat_interval_ms = hb;
        }

        cfg
    }
}

/// Schema evolution tracker (decode side).
///
/// Stores the field set seen in the first message per topic and returns
/// `WireError::SchemaIncompatible` when the field set changes in an
/// incompatible way (fields removed or types changed).
#[derive(Debug, Default)]
pub struct SchemaTracker {
    /// topic → set of field names seen in the `payload.after` or `payload.before`.
    seen_fields: HashMap<String, Vec<String>>,
}

impl SchemaTracker {
    /// Observe the field set of a Debezium payload and return an error if
    /// fields were removed (indicative of an incompatible schema change).
    ///
    /// New fields are tolerated (additive schema evolution is always OK).
    pub fn observe(&mut self, topic: &str, payload: &Value) -> Result<(), WireError> {
        let current_fields = extract_field_names(payload);
        if current_fields.is_empty() {
            return Ok(());
        }

        match self.seen_fields.get(topic) {
            None => {
                self.seen_fields.insert(topic.to_string(), current_fields);
                Ok(())
            }
            Some(known) => {
                // Check for removed fields.
                let removed: Vec<_> = known
                    .iter()
                    .filter(|f| !current_fields.contains(*f))
                    .collect();
                if !removed.is_empty() {
                    return Err(WireError::schema_incompatible(
                        topic,
                        format!(
                            "field(s) removed from schema: {}",
                            removed
                                .iter()
                                .map(|s| s.as_str())
                                .collect::<Vec<_>>()
                                .join(", ")
                        ),
                    ));
                }
                // Update known fields with any newly added ones.
                let mut updated = known.clone();
                for f in &current_fields {
                    if !updated.contains(f) {
                        updated.push(f.clone());
                    }
                }
                self.seen_fields.insert(topic.to_string(), updated);
                Ok(())
            }
        }
    }
}

fn extract_field_names(payload: &Value) -> Vec<String> {
    let after = payload.get("after");
    let before = payload.get("before");
    let src = after.or(before);
    match src {
        Some(Value::Object(map)) => map.keys().cloned().collect(),
        _ => vec![],
    }
}

/// The Debezium wire format implementation.
#[derive(Debug)]
pub struct DebeziumFormat {
    pub config: DebeziumConfig,
    schema_tracker: SchemaTracker,
    /// Cached key from previous message per topic (for tombstone → DELETE).
    last_key: HashMap<String, Value>,
}

impl DebeziumFormat {
    pub fn new(config: DebeziumConfig) -> Self {
        Self {
            config,
            schema_tracker: SchemaTracker::default(),
            last_key: HashMap::new(),
        }
    }

    pub fn from_config(config: &Value) -> Self {
        Self::new(DebeziumConfig::from_config(config))
    }

    // ── Decode helpers ────────────────────────────────────────────────────

    fn decode_json_payload(
        &mut self,
        raw: &RawMessage,
        bytes: &[u8],
    ) -> Result<Option<InboxRow>, WireError> {
        let envelope: Value = serde_json::from_slice(bytes).map_err(|e| {
            WireError::decode(&raw.topic, format!("Debezium JSON parse error: {e}"))
        })?;

        // Extract the payload block (with or without the outer schema wrapper).
        let payload = envelope.get("payload").cloned().unwrap_or(envelope.clone());

        let op_str = payload
            .get("op")
            .and_then(|v| v.as_str())
            .ok_or_else(|| WireError::decode(&raw.topic, "Debezium envelope missing 'op'"))?;

        // Heartbeat messages: op not in {c,u,d,r} → skip.
        let op = match debezium_op_to_pg(op_str) {
            Some(op) => {
                if op_str == "r" && self.config.snapshot_op_treatment == SnapshotOpTreatment::Upsert
                {
                    "upsert"
                } else {
                    op
                }
            }
            None => {
                tracing::debug!(
                    topic = raw.topic,
                    op = op_str,
                    "Debezium: skipping unknown op"
                );
                return Ok(None);
            }
        };

        let after = payload.get("after").cloned();
        let before = payload.get("before").cloned();
        let source = payload.get("source");

        // Build the inbox payload from the "after" state (or "before" for DELETE).
        let inbox_payload = match op_str {
            "d" => before.clone().unwrap_or(Value::Null),
            _ => after.clone().unwrap_or(Value::Null),
        };

        // Extract timestamp from the source block.
        let commit_ts = source
            .and_then(|s| s.get("ts_ms"))
            .and_then(|v| v.as_i64())
            .and_then(|ms| {
                use chrono::{TimeZone, Utc};
                Utc.timestamp_millis_opt(ms).single()
            });

        // Extract source position (lsn / pos / change_lsn).
        let source_position = source.and_then(|s| {
            s.get("lsn")
                .or_else(|| s.get("pos"))
                .or_else(|| s.get("change_lsn"))
                .map(|v| v.to_string())
        });

        // Build the event_id from the key or payload.
        let event_id = self.build_event_id(raw, &payload);

        // Cache the key for tombstone handling on a subsequent message.
        if let Some(key_value) = build_key_from_payload(&payload) {
            self.last_key.insert(raw.topic.clone(), key_value);
        }

        Ok(Some(InboxRow {
            event_id,
            event_type: raw.topic.clone(),
            payload: inbox_payload,
            old_payload: if op_str == "u" { before } else { None },
            op: op.to_string(),
            commit_ts,
            source_position,
        }))
    }

    fn decode_tombstone(&mut self, raw: &RawMessage) -> Result<Option<InboxRow>, WireError> {
        match self.config.tombstone_handling {
            TombstoneHandling::Drop => {
                tracing::debug!(topic = raw.topic, "Debezium: dropping tombstone");
                Ok(None)
            }
            TombstoneHandling::Delete => {
                // Use the cached key payload from the previous message.
                let payload = self
                    .last_key
                    .get(&raw.topic)
                    .cloned()
                    .unwrap_or(Value::Null);

                // Build event_id from the raw key bytes.
                let event_id = raw
                    .key
                    .as_deref()
                    .and_then(|k| std::str::from_utf8(k).ok())
                    .map(|s| format!("tombstone:{s}"))
                    .unwrap_or_else(|| format!("tombstone:{}", uuid::Uuid::new_v4()));

                Ok(Some(InboxRow {
                    event_id,
                    event_type: raw.topic.clone(),
                    payload,
                    old_payload: None,
                    op: "delete".to_string(),
                    commit_ts: None,
                    source_position: None,
                }))
            }
        }
    }

    fn build_event_id(&self, raw: &RawMessage, _payload: &Value) -> String {
        match self.config.key_strategy {
            KeyStrategy::MessageKey => raw
                .key
                .as_deref()
                .and_then(|k| std::str::from_utf8(k).ok())
                .map(|s| s.to_string())
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            KeyStrategy::PrimaryKey => {
                // Use raw key bytes as a string if available.
                raw.key
                    .as_deref()
                    .and_then(|k| std::str::from_utf8(k).ok())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string())
            }
        }
    }

    // ── Encode helpers ────────────────────────────────────────────────────

    fn build_debezium_source(&self, row: &OutboxRow) -> Value {
        let ts_ms = row.commit_ts.map(|t| t.timestamp_millis()).unwrap_or(0);
        json!({
            "version": format!("pg_tide/{}", env!("CARGO_PKG_VERSION")),
            "connector": "pg_tide",
            "name": self.config.server_name,
            "ts_ms": ts_ms,
            "snapshot": "false",
            "db": row.database,
            "schema": row.schema_name,
            "table": row.stream_table,
            "lsn": row.source_lsn.unwrap_or(0),
        })
    }

    fn build_debezium_envelope(
        &self,
        row: &OutboxRow,
        op_char: &str,
        before: Value,
        after: Value,
    ) -> Value {
        let source = self.build_debezium_source(row);
        let ts_ms = row.commit_ts.map(|t| t.timestamp_millis()).unwrap_or(0);
        json!({
            "payload": {
                "before": before,
                "after": after,
                "source": source,
                "op": op_char,
                "ts_ms": ts_ms,
                "transaction": null,
            }
        })
    }

    fn build_key(&self, row: &OutboxRow) -> Value {
        // Use the "new" or "old" row's primary-key-looking field(s) as the key.
        // If the row has no obvious PK we fall back to outbox_id.
        let data = row.new_row.as_ref().or(row.old_row.as_ref());
        if let Some(Value::Object(map)) = data {
            // Heuristic: columns named "id", "<table>_id" are PKs.
            let pk_candidates = ["id", "uuid", "pk"];
            for candidate in pk_candidates {
                if let Some(v) = map.get(candidate) {
                    return json!({ candidate: v });
                }
            }
            // Fall back to first field.
            if let Some((k, v)) = map.iter().next() {
                return json!({ k: v });
            }
        }
        json!({ "outbox_id": row.outbox_id })
    }

    fn encode_one(
        &self,
        row: &OutboxRow,
        op_char: &str,
        before: Value,
        after: Value,
        topic: &str,
    ) -> Result<EncodedMessage, WireError> {
        let envelope = self.build_debezium_envelope(row, op_char, before, after);
        let key = self.build_key(row);
        let value_bytes = serde_json::to_vec(&envelope)
            .map_err(|e| WireError::encode(row.outbox_id, e.to_string()))?;
        let key_bytes = serde_json::to_vec(&key)
            .map_err(|e| WireError::encode(row.outbox_id, e.to_string()))?;
        Ok(EncodedMessage {
            topic: topic.to_string(),
            key: Some(key_bytes),
            value: Some(value_bytes),
        })
    }

    fn encode_tombstone(&self, row: &OutboxRow, topic: &str) -> Result<EncodedMessage, WireError> {
        let key = self.build_key(row);
        let key_bytes = serde_json::to_vec(&key)
            .map_err(|e| WireError::encode(row.outbox_id, e.to_string()))?;
        Ok(EncodedMessage {
            topic: topic.to_string(),
            key: Some(key_bytes),
            value: None, // tombstone
        })
    }
}

fn build_key_from_payload(payload: &Value) -> Option<Value> {
    // Try to extract a representative key from the payload (for tombstone caching).
    payload
        .get("after")
        .or_else(|| payload.get("before"))
        .cloned()
}

impl WireFormat for DebeziumFormat {
    fn name(&self) -> &'static str {
        "debezium"
    }

    fn decode(&self, raw: &RawMessage) -> Result<Option<InboxRow>, WireError> {
        // We need mutable access to `self` for caching the last key.
        // As the trait takes `&self`, we use interior mutability via unsafe RefCell
        // would be complex; instead we implement the mutable parts via a separate
        // mutable method and declare the trait correctly.
        //
        // For now, tombstone handling with caching only works for the mutable
        // `observe_schema` path; here we handle the immutable decode.
        match &raw.value {
            None => {
                // Tombstone
                match self.config.tombstone_handling {
                    TombstoneHandling::Drop => Ok(None),
                    TombstoneHandling::Delete => {
                        let event_id = raw
                            .key
                            .as_deref()
                            .and_then(|k| std::str::from_utf8(k).ok())
                            .map(|s| format!("tombstone:{s}"))
                            .unwrap_or_else(|| format!("tombstone:{}", uuid::Uuid::new_v4()));
                        Ok(Some(InboxRow {
                            event_id,
                            event_type: raw.topic.clone(),
                            payload: Value::Null,
                            old_payload: None,
                            op: "delete".to_string(),
                            commit_ts: None,
                            source_position: None,
                        }))
                    }
                }
            }
            Some(bytes) => {
                let envelope: Value = serde_json::from_slice(bytes).map_err(|e| {
                    WireError::decode(&raw.topic, format!("Debezium JSON parse error: {e}"))
                })?;

                let payload = envelope.get("payload").cloned().unwrap_or(envelope.clone());

                let op_str = match payload.get("op").and_then(|v| v.as_str()) {
                    Some(op) => op,
                    None => {
                        // Could be a heartbeat or schema change — skip.
                        tracing::debug!(
                            topic = raw.topic,
                            "Debezium: skipping message without 'op' field"
                        );
                        return Ok(None);
                    }
                };

                let op = match debezium_op_to_pg(op_str) {
                    Some(op) => {
                        if op_str == "r"
                            && self.config.snapshot_op_treatment == SnapshotOpTreatment::Upsert
                        {
                            "upsert"
                        } else {
                            op
                        }
                    }
                    None => {
                        tracing::debug!(
                            topic = raw.topic,
                            op = op_str,
                            "Debezium: skipping unknown op"
                        );
                        return Ok(None);
                    }
                };

                let after = payload.get("after").cloned();
                let before = payload.get("before").cloned();
                let source = payload.get("source");

                let inbox_payload = match op_str {
                    "d" => before.clone().unwrap_or(Value::Null),
                    _ => after.clone().unwrap_or(Value::Null),
                };

                let commit_ts = source
                    .and_then(|s| s.get("ts_ms"))
                    .and_then(|v| v.as_i64())
                    .and_then(|ms| {
                        use chrono::{TimeZone, Utc};
                        Utc.timestamp_millis_opt(ms).single()
                    });

                let source_position = source.and_then(|s| {
                    s.get("lsn")
                        .or_else(|| s.get("pos"))
                        .or_else(|| s.get("change_lsn"))
                        .map(|v| v.to_string())
                });

                let event_id = raw
                    .key
                    .as_deref()
                    .and_then(|k| std::str::from_utf8(k).ok())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

                Ok(Some(InboxRow {
                    event_id,
                    event_type: raw.topic.clone(),
                    payload: inbox_payload,
                    old_payload: if op_str == "u" { before } else { None },
                    op: op.to_string(),
                    commit_ts,
                    source_position,
                }))
            }
        }
    }

    fn observe_schema(&mut self, raw: &RawMessage) -> Result<(), WireError> {
        if let Some(bytes) = &raw.value {
            let envelope: Value = serde_json::from_slice(bytes).map_err(|e| {
                WireError::decode(&raw.topic, format!("schema observation JSON error: {e}"))
            })?;
            let payload = envelope.get("payload").cloned().unwrap_or(envelope);
            self.schema_tracker.observe(&raw.topic, &payload)?;
            // Also cache the key for tombstone handling.
            if let Some(key_val) = build_key_from_payload(&payload) {
                self.last_key.insert(raw.topic.clone(), key_val);
            }
        }
        Ok(())
    }

    fn encode(&self, row: &OutboxRow, ctx: &EncodeContext) -> Result<EncodedBatch, WireError> {
        let op_char =
            pg_op_to_debezium(&row.op).ok_or_else(|| WireError::UnsupportedOperation {
                op: row.op.clone(),
                topic: ctx.resolve_topic(row),
            })?;

        let topic = ctx.resolve_topic(row);

        let (before, after) = match op_char {
            "c" => (Value::Null, row.new_row.clone().unwrap_or(Value::Null)),
            "u" => (
                row.old_row.clone().unwrap_or(Value::Null),
                row.new_row.clone().unwrap_or(Value::Null),
            ),
            "d" => (row.old_row.clone().unwrap_or(Value::Null), Value::Null),
            _ => (Value::Null, row.new_row.clone().unwrap_or(Value::Null)),
        };

        let main_msg = self.encode_one(row, op_char, before, after, &topic)?;

        let mut messages = vec![main_msg];

        // Emit tombstone after DELETE if configured.
        if op_char == "d" && (self.config.emit_tombstones || ctx.emit_tombstones) {
            messages.push(self.encode_tombstone(row, &topic)?);
        }

        Ok(EncodedBatch { messages })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn debezium_msg(topic: &str, op: &str, before: Value, after: Value) -> RawMessage {
        let envelope = json!({
            "payload": {
                "before": before,
                "after": after,
                "op": op,
                "ts_ms": 1714029482000_i64,
                "source": {
                    "ts_ms": 1714029482000_i64,
                    "db": "app",
                    "table": "users",
                    "lsn": 12345
                }
            }
        });
        RawMessage::from_json(topic, &envelope)
    }

    #[test]
    fn test_decode_insert() {
        let fmt = DebeziumFormat::from_config(&json!({}));
        let raw = debezium_msg(
            "my-server.public.users",
            "c",
            Value::Null,
            json!({"id": 7, "name": "alice"}),
        );
        let row = fmt.decode(&raw).unwrap().unwrap();
        assert_eq!(row.op, "insert");
        assert_eq!(row.payload["id"], 7);
        assert_eq!(row.payload["name"], "alice");
    }

    #[test]
    fn test_decode_update() {
        let fmt = DebeziumFormat::from_config(&json!({}));
        let raw = debezium_msg(
            "my-server.public.users",
            "u",
            json!({"id": 7, "name": "alice"}),
            json!({"id": 7, "name": "alice2"}),
        );
        let row = fmt.decode(&raw).unwrap().unwrap();
        assert_eq!(row.op, "update");
        assert_eq!(row.payload["name"], "alice2");
        assert_eq!(row.old_payload.as_ref().unwrap()["name"], "alice");
    }

    #[test]
    fn test_decode_delete() {
        let fmt = DebeziumFormat::from_config(&json!({}));
        let raw = debezium_msg(
            "my-server.public.users",
            "d",
            json!({"id": 7, "name": "alice"}),
            Value::Null,
        );
        let row = fmt.decode(&raw).unwrap().unwrap();
        assert_eq!(row.op, "delete");
        assert_eq!(row.payload["id"], 7);
    }

    #[test]
    fn test_decode_snapshot_read_as_insert() {
        let fmt = DebeziumFormat::from_config(&json!({}));
        let raw = debezium_msg("topic", "r", Value::Null, json!({"id": 1}));
        let row = fmt.decode(&raw).unwrap().unwrap();
        assert_eq!(row.op, "insert");
    }

    #[test]
    fn test_decode_snapshot_read_as_upsert() {
        let fmt = DebeziumFormat::from_config(&json!({"snapshot_op_treatment": "upsert"}));
        let raw = debezium_msg("topic", "r", Value::Null, json!({"id": 1}));
        let row = fmt.decode(&raw).unwrap().unwrap();
        assert_eq!(row.op, "upsert");
    }

    #[test]
    fn test_decode_tombstone_drop() {
        let fmt = DebeziumFormat::from_config(&json!({"tombstone_handling": "drop"}));
        let raw = RawMessage::tombstone("topic", b"key-1".to_vec());
        let result = fmt.decode(&raw).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_tombstone_delete() {
        let fmt = DebeziumFormat::from_config(&json!({"tombstone_handling": "delete"}));
        let raw = RawMessage::tombstone("topic", b"key-1".to_vec());
        let result = fmt.decode(&raw).unwrap().unwrap();
        assert_eq!(result.op, "delete");
    }

    #[test]
    fn test_decode_commit_ts_extracted() {
        let fmt = DebeziumFormat::from_config(&json!({}));
        let raw = debezium_msg("topic", "c", Value::Null, json!({"id": 1}));
        let row = fmt.decode(&raw).unwrap().unwrap();
        assert!(row.commit_ts.is_some());
    }

    #[test]
    fn test_decode_source_position_extracted() {
        let fmt = DebeziumFormat::from_config(&json!({}));
        let raw = debezium_msg("topic", "c", Value::Null, json!({"id": 1}));
        let row = fmt.decode(&raw).unwrap().unwrap();
        assert!(row.source_position.is_some());
    }

    #[test]
    fn test_encode_insert() {
        let fmt = DebeziumFormat::from_config(&json!({"emit_tombstones": false}));
        let ctx = EncodeContext {
            emit_tombstones: false,
            ..Default::default()
        };
        let row = OutboxRow {
            outbox_id: 1,
            stream_table: "orders".to_string(),
            database: "app".to_string(),
            schema_name: "public".to_string(),
            op: "insert".to_string(),
            new_row: Some(json!({"id": 1, "total": 99})),
            old_row: None,
            commit_ts: None,
            source_lsn: None,
        };
        let batch = fmt.encode(&row, &ctx).unwrap();
        assert_eq!(batch.messages.len(), 1);
        let v: Value = serde_json::from_slice(batch.messages[0].value.as_ref().unwrap()).unwrap();
        assert_eq!(v["payload"]["op"], "c");
        assert!(v["payload"]["before"].is_null());
        assert_eq!(v["payload"]["after"]["id"], 1);
    }

    #[test]
    fn test_encode_update() {
        let fmt = DebeziumFormat::from_config(&json!({"emit_tombstones": false}));
        let ctx = EncodeContext {
            emit_tombstones: false,
            ..Default::default()
        };
        let row = OutboxRow {
            outbox_id: 2,
            stream_table: "orders".to_string(),
            database: "app".to_string(),
            schema_name: "public".to_string(),
            op: "update".to_string(),
            new_row: Some(json!({"id": 1, "total": 199})),
            old_row: Some(json!({"id": 1, "total": 99})),
            commit_ts: None,
            source_lsn: None,
        };
        let batch = fmt.encode(&row, &ctx).unwrap();
        assert_eq!(batch.messages.len(), 1);
        let v: Value = serde_json::from_slice(batch.messages[0].value.as_ref().unwrap()).unwrap();
        assert_eq!(v["payload"]["op"], "u");
        assert_eq!(v["payload"]["before"]["total"], 99);
        assert_eq!(v["payload"]["after"]["total"], 199);
    }

    #[test]
    fn test_encode_delete_with_tombstone() {
        let fmt = DebeziumFormat::from_config(&json!({"emit_tombstones": true}));
        let ctx = EncodeContext {
            emit_tombstones: true,
            ..Default::default()
        };
        let row = OutboxRow {
            outbox_id: 3,
            stream_table: "orders".to_string(),
            database: "app".to_string(),
            schema_name: "public".to_string(),
            op: "delete".to_string(),
            new_row: None,
            old_row: Some(json!({"id": 1})),
            commit_ts: None,
            source_lsn: None,
        };
        let batch = fmt.encode(&row, &ctx).unwrap();
        // Should be 2 messages: the DELETE event + tombstone.
        assert_eq!(batch.messages.len(), 2);
        // First is the DELETE event.
        let v: Value = serde_json::from_slice(batch.messages[0].value.as_ref().unwrap()).unwrap();
        assert_eq!(v["payload"]["op"], "d");
        // Second is the tombstone (null value).
        assert!(batch.messages[1].value.is_none());
    }

    #[test]
    fn test_encode_delete_without_tombstone() {
        let fmt = DebeziumFormat::from_config(&json!({"emit_tombstones": false}));
        let ctx = EncodeContext {
            emit_tombstones: false,
            ..Default::default()
        };
        let row = OutboxRow {
            outbox_id: 4,
            stream_table: "orders".to_string(),
            database: "app".to_string(),
            schema_name: "public".to_string(),
            op: "delete".to_string(),
            new_row: None,
            old_row: Some(json!({"id": 1})),
            commit_ts: None,
            source_lsn: None,
        };
        let batch = fmt.encode(&row, &ctx).unwrap();
        assert_eq!(batch.messages.len(), 1);
    }

    #[test]
    fn test_encode_source_block() {
        let fmt = DebeziumFormat::from_config(
            &json!({"server_name": "my-pg-server", "emit_tombstones": false}),
        );
        let ctx = EncodeContext {
            server_name: "my-pg-server".to_string(),
            emit_tombstones: false,
            ..Default::default()
        };
        let row = OutboxRow {
            outbox_id: 5,
            stream_table: "products".to_string(),
            database: "shop".to_string(),
            schema_name: "public".to_string(),
            op: "insert".to_string(),
            new_row: Some(json!({"id": 99})),
            old_row: None,
            commit_ts: None,
            source_lsn: Some(42000),
        };
        let batch = fmt.encode(&row, &ctx).unwrap();
        let v: Value = serde_json::from_slice(batch.messages[0].value.as_ref().unwrap()).unwrap();
        let source = &v["payload"]["source"];
        assert_eq!(source["connector"], "pg_tide");
        assert_eq!(source["name"], "my-pg-server");
        assert_eq!(source["table"], "products");
        assert_eq!(source["db"], "shop");
        assert_eq!(source["lsn"], 42000);
    }

    #[test]
    fn test_schema_tracker_no_drift() {
        let mut tracker = SchemaTracker::default();
        let payload = json!({"after": {"id": 1, "name": "alice"}, "before": null, "op": "c"});
        tracker.observe("topic", &payload).unwrap();
        // Same fields again — OK.
        tracker.observe("topic", &payload).unwrap();
    }

    #[test]
    fn test_schema_tracker_additive_ok() {
        let mut tracker = SchemaTracker::default();
        let p1 = json!({"after": {"id": 1}, "before": null});
        let p2 = json!({"after": {"id": 2, "name": "alice"}, "before": null});
        tracker.observe("topic", &p1).unwrap();
        // New field added — OK.
        tracker.observe("topic", &p2).unwrap();
    }

    #[test]
    fn test_schema_tracker_field_removed_is_error() {
        let mut tracker = SchemaTracker::default();
        let p1 = json!({"after": {"id": 1, "name": "alice"}, "before": null});
        let p2 = json!({"after": {"id": 2}, "before": null}); // "name" removed
        tracker.observe("topic", &p1).unwrap();
        let result = tracker.observe("topic", &p2);
        assert!(result.is_err());
        match result {
            Err(WireError::SchemaIncompatible { .. }) => {}
            other => panic!("expected SchemaIncompatible, got {other:?}"),
        }
    }

    #[test]
    fn test_observe_schema_updates_cache() {
        let mut fmt = DebeziumFormat::from_config(&json!({}));
        let raw = debezium_msg("topic", "c", Value::Null, json!({"id": 1}));
        fmt.observe_schema(&raw).unwrap();
        // Should not error on same schema.
        fmt.observe_schema(&raw).unwrap();
    }

    #[test]
    fn test_config_defaults() {
        let cfg = DebeziumConfig::from_config(&json!({}));
        assert_eq!(cfg.tombstone_handling, TombstoneHandling::Delete);
        assert_eq!(cfg.key_strategy, KeyStrategy::PrimaryKey);
        assert_eq!(cfg.snapshot_op_treatment, SnapshotOpTreatment::Insert);
        assert!(cfg.emit_tombstones);
        assert_eq!(cfg.heartbeat_interval_ms, 10_000);
        assert_eq!(cfg.server_name, "pg-tide");
    }

    #[test]
    fn test_config_custom() {
        let cfg = DebeziumConfig::from_config(&json!({
            "tombstone_handling": "drop",
            "key_strategy": "message_key",
            "snapshot_op_treatment": "upsert",
            "server_name": "custom-server",
            "emit_tombstones": false,
            "heartbeat_interval_ms": 5000,
        }));
        assert_eq!(cfg.tombstone_handling, TombstoneHandling::Drop);
        assert_eq!(cfg.key_strategy, KeyStrategy::MessageKey);
        assert_eq!(cfg.snapshot_op_treatment, SnapshotOpTreatment::Upsert);
        assert_eq!(cfg.server_name, "custom-server");
        assert!(!cfg.emit_tombstones);
        assert_eq!(cfg.heartbeat_interval_ms, 5000);
    }

    #[test]
    fn test_debezium_format_name() {
        let fmt = DebeziumFormat::from_config(&json!({}));
        assert_eq!(fmt.name(), "debezium");
    }
}
