/// Pluggable wire format abstraction for pg-tide-relay (v0.11.0).
///
/// Decouples the transport layer (Kafka, NATS, Redis, etc.) from the envelope
/// format, enabling bidirectional Debezium support and other CDC formats.
///
/// # Architecture
///
/// The `WireFormat` trait is symmetric: it handles both the **reverse** path
/// (consume raw bytes from a transport → decode into `InboxRow`) and the
/// **forward** path (encode `OutboxRow` → raw bytes for a transport).
///
/// ## Reverse path (consume)
///
/// ```text
/// Source (Kafka / NATS / …)
///     └─▶ RawMessage
///         └─▶ WireFormat::decode  ─────▶ Option<InboxRow>
///                                              └─▶ inbox writer
/// ```
///
/// ## Forward path (produce)
///
/// ```text
/// outbox poller
///     └─▶ OutboxRow
///         └─▶ WireFormat::encode  ─────▶ EncodedBatch
///                                              └─▶ Sink
/// ```
use std::collections::HashMap;

pub mod debezium;
pub mod native;

#[cfg(feature = "maxwell")]
pub mod maxwell;

#[cfg(feature = "canal")]
pub mod canal;

#[cfg(feature = "cdc-json")]
pub mod cdc_json;

pub mod cloudevents;

// Re-export the canonical implementations for convenience.
pub use cloudevents::CloudEventsFormat;
pub use debezium::DebeziumFormat;
pub use native::NativePgTideFormat;

/// A raw, undecoded message from a transport backend.
#[derive(Debug, Clone)]
pub struct RawMessage {
    /// Message key bytes (may be None for keyless transports).
    pub key: Option<Vec<u8>>,
    /// Message value bytes. None represents a Debezium tombstone.
    pub value: Option<Vec<u8>>,
    /// Topic / subject / stream name from the transport.
    pub topic: String,
    /// Transport-level headers (e.g. Kafka headers).
    pub headers: HashMap<String, String>,
}

impl RawMessage {
    /// Convenience constructor for a keyed message.
    pub fn new(topic: impl Into<String>, key: Option<Vec<u8>>, value: Option<Vec<u8>>) -> Self {
        Self {
            key,
            value,
            topic: topic.into(),
            headers: HashMap::new(),
        }
    }

    /// Convenience constructor for a JSON-valued message.
    pub fn from_json(topic: impl Into<String>, value: &serde_json::Value) -> Self {
        Self {
            key: None,
            value: Some(serde_json::to_vec(value).unwrap_or_default()),
            topic: topic.into(),
            headers: HashMap::new(),
        }
    }

    /// Convenience constructor for a tombstone (null value).
    pub fn tombstone(topic: impl Into<String>, key: Vec<u8>) -> Self {
        Self {
            key: Some(key),
            value: None,
            topic: topic.into(),
            headers: HashMap::new(),
        }
    }
}

/// A decoded inbox row ready for insertion into a pg_tide inbox table.
#[derive(Debug, Clone)]
pub struct InboxRow {
    /// Dedup key for idempotent delivery.
    pub event_id: String,
    /// Event type / topic name used as the inbox event_type.
    pub event_type: String,
    /// The current (after) state of the row.
    pub payload: serde_json::Value,
    /// The previous (before) state, if present (UPDATE / DELETE).
    pub old_payload: Option<serde_json::Value>,
    /// Operation: "insert", "update", "delete".
    pub op: String,
    /// Commit timestamp from the source (UTC).
    pub commit_ts: Option<chrono::DateTime<chrono::Utc>>,
    /// Source position (LSN, binlog pos, etc.) as a string for tracing.
    pub source_position: Option<String>,
}

impl InboxRow {
    /// Build from a native pg_tide `RelayMessage`.
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

/// A row from the pg_tide outbox ready to be encoded.
#[derive(Debug, Clone)]
pub struct OutboxRow {
    /// Outbox row primary key.
    pub outbox_id: i64,
    /// Outbox table name (e.g. "orders").
    pub stream_table: String,
    /// The source database name.
    pub database: String,
    /// The source schema name (default "public").
    pub schema_name: String,
    /// Operation: "insert", "update", "delete".
    pub op: String,
    /// New row state (after). None for DELETE.
    pub new_row: Option<serde_json::Value>,
    /// Old row state (before). None for INSERT.
    pub old_row: Option<serde_json::Value>,
    /// Commit timestamp.
    pub commit_ts: Option<chrono::DateTime<chrono::Utc>>,
    /// PostgreSQL LSN (serialised as i64 for simplicity).
    pub source_lsn: Option<i64>,
}

impl OutboxRow {
    /// Build from a native pg_tide `RelayMessage`.
    pub fn from_relay_message(msg: &crate::envelope::RelayMessage) -> Self {
        Self {
            outbox_id: msg.outbox_id.unwrap_or(0),
            stream_table: msg.subject.clone(),
            database: "postgres".to_string(),
            schema_name: "public".to_string(),
            op: msg.op.clone(),
            new_row: if msg.op != "delete" {
                Some(msg.payload.clone())
            } else {
                None
            },
            old_row: if msg.op == "delete" || msg.op == "update" {
                Some(msg.payload.clone())
            } else {
                None
            },
            commit_ts: None,
            source_lsn: None,
        }
    }
}

/// Context passed to `WireFormat::encode` for the forward path.
#[derive(Debug, Clone)]
pub struct EncodeContext {
    /// Debezium `source.name` / server name (default "pg-tide").
    pub server_name: String,
    /// Topic template, e.g. `"{server}.{schema}.{stream_table}"`.
    pub topic_template: String,
    /// Whether to emit a tombstone (null-value) after a DELETE.
    pub emit_tombstones: bool,
    /// Heartbeat interval in ms (0 = disabled).
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
    /// Resolve the topic name for a given row using `topic_template`.
    pub fn resolve_topic(&self, row: &OutboxRow) -> String {
        self.topic_template
            .replace("{server}", &self.server_name)
            .replace("{schema}", &row.schema_name)
            .replace("{stream_table}", &row.stream_table)
            .replace("{database}", &row.database)
    }
}

/// A single encoded message ready to be written to a transport.
#[derive(Debug, Clone)]
pub struct EncodedMessage {
    /// Target topic / subject.
    pub topic: String,
    /// Message key bytes (routing key for Kafka, etc.).
    pub key: Option<Vec<u8>>,
    /// Message value bytes. None = tombstone.
    pub value: Option<Vec<u8>>,
}

/// A batch of encoded messages produced from one `OutboxRow`.
///
/// Most formats emit exactly one message per row, but Debezium with
/// `emit_tombstones = true` emits two messages for a DELETE (the event
/// plus a null-value tombstone).
#[derive(Debug, Clone)]
pub struct EncodedBatch {
    pub messages: Vec<EncodedMessage>,
}

impl EncodedBatch {
    pub fn single(msg: EncodedMessage) -> Self {
        Self {
            messages: vec![msg],
        }
    }

    pub fn empty() -> Self {
        Self { messages: vec![] }
    }
}

/// Errors that can occur during wire format encode / decode.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("decode error in topic '{topic}': {reason}")]
    Decode { topic: String, reason: String },

    #[error("encode error for outbox_id={outbox_id}: {reason}")]
    Encode { outbox_id: i64, reason: String },

    #[error("schema incompatible in topic '{topic}': {reason}")]
    SchemaIncompatible { topic: String, reason: String },

    #[error("schema registry error: {0}")]
    SchemaRegistry(String),

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

    pub fn schema_incompatible(topic: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::SchemaIncompatible {
            topic: topic.into(),
            reason: reason.into(),
        }
    }
}

/// The pluggable wire format trait — symmetric encode + decode.
///
/// Implementors handle both the reverse path (decode raw transport bytes into
/// `InboxRow`) and the forward path (encode `OutboxRow` into transport bytes).
pub trait WireFormat: Send + Sync {
    /// A short identifier for this format (e.g. `"debezium"`, `"native"`).
    fn name(&self) -> &'static str;

    // ── Reverse path (consume) ────────────────────────────────────────────

    /// Decode a single raw transport message into an inbox row.
    ///
    /// Returns `None` for messages the format chooses to skip (e.g. Debezium
    /// tombstones with `tombstone_handling = drop`, or schema-change events).
    fn decode(&self, raw: &RawMessage) -> Result<Option<InboxRow>, WireError>;

    /// Optional schema-evolution hook called on every inbound message.
    ///
    /// Default: no-op. Implementations may return `WireError::SchemaIncompatible`
    /// to halt the consumer when an incompatible schema change is detected.
    fn observe_schema(&mut self, _raw: &RawMessage) -> Result<(), WireError> {
        Ok(())
    }

    // ── Forward path (produce) ────────────────────────────────────────────

    /// Encode a pg_tide outbox row into transport bytes.
    ///
    /// Most rows produce exactly one message, but Debezium DELETE with
    /// `emit_tombstones = true` produces two.
    fn encode(&self, row: &OutboxRow, ctx: &EncodeContext) -> Result<EncodedBatch, WireError>;

    /// Optional registration hook called once per `(topic, schema)` pair
    /// before the first emit. Debezium-Avro registers schemas with the
    /// Confluent Schema Registry here.
    fn register_schema(&mut self, _topic: &str, _schema: &OutboxSchema) -> Result<(), WireError> {
        Ok(())
    }
}

/// Minimal schema descriptor for an outbox stream table.
#[derive(Debug, Clone)]
pub struct OutboxSchema {
    /// Stream table name.
    pub table: String,
    /// Column name → PG type string (e.g. "int4", "text", "timestamptz").
    pub columns: Vec<(String, String)>,
}

/// Build a `WireFormat` implementation from a pipeline's config JSON.
///
/// Reads `wire_format` (default `"native"`) and `wire_config` fields.
/// Returns a boxed trait object so the coordinator can hold it without
/// knowing the concrete type.
pub fn from_config(config: &serde_json::Value) -> Box<dyn WireFormat> {
    let format_name = config
        .get("wire_format")
        .and_then(|v| v.as_str())
        .unwrap_or("native");

    match format_name {
        "debezium" => {
            let wire_cfg = config.get("wire_config").cloned().unwrap_or_default();
            Box::new(DebeziumFormat::from_config(&wire_cfg))
        }
        "cloudevents" => {
            let wire_cfg = config.get("wire_config").cloned().unwrap_or_default();
            Box::new(CloudEventsFormat::from_config(&wire_cfg))
        }
        #[cfg(feature = "maxwell")]
        "maxwell" => {
            let wire_cfg = config.get("wire_config").cloned().unwrap_or_default();
            Box::new(maxwell::MaxwellFormat::from_config(&wire_cfg))
        }
        #[cfg(feature = "canal")]
        "canal" => {
            let wire_cfg = config.get("wire_config").cloned().unwrap_or_default();
            Box::new(canal::CanalFormat::from_config(&wire_cfg))
        }
        #[cfg(feature = "cdc-json")]
        "cdc_json" => {
            let wire_cfg = config.get("wire_config").cloned().unwrap_or_default();
            Box::new(cdc_json::CdcJsonFormat::from_config(&wire_cfg))
        }
        // Default / fallback: native pg_tide envelope.
        _ => Box::new(NativePgTideFormat::new()),
    }
}

// ── Type-coercion utilities (shared by multiple formats) ─────────────────────

/// Apply Debezium / common logical-type coercions to a JSON value in-place.
///
/// Handles the common Debezium logical types that appear in the `schema` block
/// and converts the companion `payload` value accordingly.  This is a best-
/// effort conversion; unknown types fall through as `text` with a warning.
pub fn apply_logical_type(value: &serde_json::Value, logical_type: &str) -> serde_json::Value {
    match logical_type {
        // Date: int days since Unix epoch → ISO-8601 date string.
        "io.debezium.time.Date" => {
            if let Some(days) = value.as_i64() {
                use chrono::NaiveDate;
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                if let Some(d) = epoch.checked_add_days(chrono::Days::new(days.unsigned_abs())) {
                    return serde_json::Value::String(d.to_string());
                }
            }
            value.clone()
        }
        // Timestamp (ms): int64 ms → ISO-8601.
        "io.debezium.time.Timestamp" => {
            if let Some(ms) = value.as_i64() {
                use chrono::{TimeZone, Utc};
                let ts = Utc
                    .timestamp_millis_opt(ms)
                    .single()
                    .map(|dt| dt.to_rfc3339());
                if let Some(s) = ts {
                    return serde_json::Value::String(s);
                }
            }
            value.clone()
        }
        // MicroTimestamp: int64 µs → ISO-8601.
        "io.debezium.time.MicroTimestamp" => {
            if let Some(us) = value.as_i64() {
                use chrono::{TimeZone, Utc};
                let ts = Utc.timestamp_micros(us).single().map(|dt| dt.to_rfc3339());
                if let Some(s) = ts {
                    return serde_json::Value::String(s);
                }
            }
            value.clone()
        }
        // NanoTimestamp: int64 ns → ISO-8601 (truncated to µs).
        "io.debezium.time.NanoTimestamp" => {
            if let Some(ns) = value.as_i64() {
                use chrono::{TimeZone, Utc};
                let ts = Utc
                    .timestamp_micros(ns / 1_000)
                    .single()
                    .map(|dt| dt.to_rfc3339());
                if let Some(s) = ts {
                    return serde_json::Value::String(s);
                }
            }
            value.clone()
        }
        // ZonedTimestamp: ISO-8601 string — pass through.
        "io.debezium.time.ZonedTimestamp" => value.clone(),
        // Json / Uuid / geometry: pass through as-is.
        "io.debezium.data.Json"
        | "io.debezium.data.Uuid"
        | "io.debezium.data.geometry.Geometry"
        | "io.debezium.data.geometry.Geography" => value.clone(),
        // Decimal: base64 bytes + scale → numeric string (best-effort).
        "io.debezium.data.Decimal" | "io.debezium.data.VariableScaleDecimal" => {
            // For JSON wire format the value is already a string representation.
            value.clone()
        }
        // Unknown logical type: fall through as text.
        _ => {
            tracing::warn!(
                logical_type,
                "unknown Debezium logical type; treating as text"
            );
            match value {
                serde_json::Value::String(_) => value.clone(),
                other => serde_json::Value::String(other.to_string()),
            }
        }
    }
}

/// Convert a Debezium `op` character to a pg_tide operation string.
pub fn debezium_op_to_pg(op: &str) -> Option<&'static str> {
    match op {
        "c" => Some("insert"),
        "u" => Some("update"),
        "d" => Some("delete"),
        "r" => Some("insert"), // snapshot read → treat as insert
        _ => None,
    }
}

/// Convert a pg_tide `op` string to a Debezium `op` character.
pub fn pg_op_to_debezium(op: &str) -> Option<&'static str> {
    match op {
        "insert" => Some("c"),
        "update" => Some("u"),
        "delete" => Some("d"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_op_conversions() {
        assert_eq!(debezium_op_to_pg("c"), Some("insert"));
        assert_eq!(debezium_op_to_pg("u"), Some("update"));
        assert_eq!(debezium_op_to_pg("d"), Some("delete"));
        assert_eq!(debezium_op_to_pg("r"), Some("insert"));
        assert_eq!(debezium_op_to_pg("x"), None);

        assert_eq!(pg_op_to_debezium("insert"), Some("c"));
        assert_eq!(pg_op_to_debezium("update"), Some("u"));
        assert_eq!(pg_op_to_debezium("delete"), Some("d"));
        assert_eq!(pg_op_to_debezium("event"), None);
    }

    #[test]
    fn test_encode_context_resolve_topic() {
        let ctx = EncodeContext {
            server_name: "my-server".to_string(),
            topic_template: "{server}.{schema}.{stream_table}".to_string(),
            ..Default::default()
        };
        let row = OutboxRow {
            outbox_id: 1,
            stream_table: "orders".to_string(),
            database: "app_db".to_string(),
            schema_name: "public".to_string(),
            op: "insert".to_string(),
            new_row: None,
            old_row: None,
            commit_ts: None,
            source_lsn: None,
        };
        assert_eq!(ctx.resolve_topic(&row), "my-server.public.orders");
    }

    #[test]
    fn test_logical_type_date() {
        // day 0 = 1970-01-01
        let v = apply_logical_type(&serde_json::json!(0_i64), "io.debezium.time.Date");
        assert_eq!(v.as_str().unwrap(), "1970-01-01");

        // day 1 = 1970-01-02
        let v = apply_logical_type(&serde_json::json!(1_i64), "io.debezium.time.Date");
        assert_eq!(v.as_str().unwrap(), "1970-01-02");
    }

    #[test]
    fn test_logical_type_timestamp_ms() {
        let v = apply_logical_type(&serde_json::json!(0_i64), "io.debezium.time.Timestamp");
        // Should be a string containing the epoch
        assert!(v.as_str().unwrap().contains("1970"));
    }

    #[test]
    fn test_from_config_native() {
        let cfg = serde_json::json!({});
        let fmt = from_config(&cfg);
        assert_eq!(fmt.name(), "native");
    }

    #[test]
    fn test_from_config_debezium() {
        let cfg = serde_json::json!({"wire_format": "debezium"});
        let fmt = from_config(&cfg);
        assert_eq!(fmt.name(), "debezium");
    }
}
