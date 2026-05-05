/// Schema Registry integration (RELAY-P2-12).
///
/// Supports serialising outgoing messages to Avro using the Confluent Schema
/// Registry wire format, and deserialising incoming Avro messages to JSON for
/// inbox insertion.
///
/// Feature-gated: only compiled with `--features schema-registry`.
///
/// Configuration in the pipeline's `config` JSONB column:
///
/// ```json
/// {
///   "schema_registry": {
///     "url": "http://localhost:8081",
///     "username": "user",
///     "password": "pass"
///   },
///   "serialization": {
///     "format": "avro",
///     "subject_name_strategy": "TopicName"
///   }
/// }
/// ```
/// Schema format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SerializationFormat {
    #[default]
    Json,
    #[cfg(feature = "schema-registry")]
    Avro,
}

impl SerializationFormat {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s {
            #[cfg(feature = "schema-registry")]
            "avro" => Self::Avro,
            _ => Self::Json,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Json => "json",
            #[cfg(feature = "schema-registry")]
            Self::Avro => "avro",
        }
    }
}

/// Subject name strategy for the schema registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SubjectNameStrategy {
    #[default]
    Topic,
    Record,
    TopicRecord,
}

impl SubjectNameStrategy {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s {
            "record_name" | "RecordName" => Self::Record,
            "topic_record_name" | "TopicRecordName" => Self::TopicRecord,
            _ => Self::Topic,
        }
    }

    /// Build the schema registry subject name for a given topic.
    pub fn subject(&self, topic: &str, record_name: &str, is_key: bool) -> String {
        let suffix = if is_key { "-key" } else { "-value" };
        match self {
            Self::Topic => format!("{topic}{suffix}"),
            Self::Record => format!("{record_name}{suffix}"),
            Self::TopicRecord => format!("{topic}-{record_name}{suffix}"),
        }
    }
}

/// Schema registry configuration parsed from pipeline config.
#[derive(Debug, Clone, Default)]
pub struct SchemaRegistryConfig {
    /// Schema Registry URL (e.g. `http://localhost:8081`).
    pub url: Option<String>,
    /// Optional HTTP Basic Auth username.
    pub username: Option<String>,
    /// Optional HTTP Basic Auth password.
    pub password: Option<String>,
    /// Serialisation format.
    pub format: SerializationFormat,
    /// Subject name strategy.
    pub subject_strategy: SubjectNameStrategy,
    /// Whether to auto-register schemas not found in the registry.
    pub auto_register: bool,
}

impl SchemaRegistryConfig {
    /// Parse schema registry config from a pipeline's JSON config object.
    pub fn from_pipeline_config(config: &serde_json::Value) -> Self {
        let sr = config.get("schema_registry");
        let ser = config.get("serialization");

        let url = sr
            .and_then(|s| s.get("url"))
            .and_then(|v| v.as_str())
            .map(String::from);

        let username = sr
            .and_then(|s| s.get("username"))
            .and_then(|v| v.as_str())
            .map(String::from);

        let password = sr
            .and_then(|s| s.get("password"))
            .and_then(|v| v.as_str())
            .map(String::from);

        let format = ser
            .and_then(|s| s.get("format"))
            .and_then(|v| v.as_str())
            .map(SerializationFormat::from_str)
            .unwrap_or_default();

        let subject_strategy = ser
            .and_then(|s| s.get("subject_name_strategy"))
            .and_then(|v| v.as_str())
            .map(SubjectNameStrategy::from_str)
            .unwrap_or_default();

        let auto_register = sr
            .and_then(|s| s.get("auto_register"))
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        Self {
            url,
            username,
            password,
            format,
            subject_strategy,
            auto_register,
        }
    }

    /// Whether schema registry is configured and active.
    pub fn is_active(&self) -> bool {
        self.url.is_some() && self.format != SerializationFormat::Json
    }
}

/// Confluent Schema Registry wire format prefix (5 bytes):
/// byte 0 = magic byte (0x00), bytes 1-4 = schema ID (big-endian i32).
pub const CONFLUENT_MAGIC: u8 = 0x00;
pub const CONFLUENT_HEADER_SIZE: usize = 5;

/// Encode a schema ID and payload into the Confluent wire format.
/// `[0x00][schema_id: 4 bytes BE][payload bytes]`
pub fn encode_confluent_wire(schema_id: i32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(CONFLUENT_HEADER_SIZE + payload.len());
    out.push(CONFLUENT_MAGIC);
    out.extend_from_slice(&schema_id.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Decode the Confluent wire format prefix from a byte slice.
/// Returns (schema_id, payload_bytes) or an error.
pub fn decode_confluent_wire(data: &[u8]) -> Result<(i32, &[u8]), String> {
    if data.len() < CONFLUENT_HEADER_SIZE {
        return Err(format!(
            "Confluent wire format: expected at least {} bytes, got {}",
            CONFLUENT_HEADER_SIZE,
            data.len()
        ));
    }
    if data[0] != CONFLUENT_MAGIC {
        return Err(format!(
            "Confluent wire format: unexpected magic byte 0x{:02x} (expected 0x00)",
            data[0]
        ));
    }
    let schema_id = i32::from_be_bytes(
        data[1..5]
            .try_into()
            .map_err(|_| "Confluent wire format: schema_id slice error".to_string())?,
    );
    Ok((schema_id, &data[CONFLUENT_HEADER_SIZE..]))
}

#[cfg(feature = "schema-registry")]
pub mod avro {
    //! Avro serialisation helpers for the Confluent Schema Registry wire format.
    use apache_avro::{from_value, to_avro_datum, Reader, Schema, Writer};
    use serde_json::Value;

    use crate::error::RelayError;

    /// Serialise a JSON value to Avro bytes using the provided schema string.
    /// The schema must be a valid Avro JSON schema.
    pub fn json_to_avro(json: &Value, schema_str: &str) -> Result<Vec<u8>, RelayError> {
        let schema = Schema::parse_str(schema_str)
            .map_err(|e| RelayError::other(format!("Avro schema parse error: {e}")))?;

        let avro_value = apache_avro::to_value(json)
            .map_err(|e| RelayError::other(format!("JSON→Avro conversion error: {e}")))?;

        let validated = avro_value
            .resolve(&schema)
            .map_err(|e| RelayError::other(format!("Avro schema validation error: {e}")))?;

        let mut writer = Writer::new(&schema, Vec::new());
        writer
            .append(validated)
            .map_err(|e| RelayError::other(format!("Avro append error: {e}")))?;

        writer
            .into_inner()
            .map_err(|e| RelayError::other(format!("Avro flush error: {e}")))
    }

    /// Deserialise Avro bytes to a JSON value using the provided schema string.
    pub fn avro_to_json(bytes: &[u8], schema_str: &str) -> Result<Value, RelayError> {
        let schema = Schema::parse_str(schema_str)
            .map_err(|e| RelayError::other(format!("Avro schema parse error: {e}")))?;

        let reader = Reader::with_schema(&schema, bytes)
            .map_err(|e| RelayError::other(format!("Avro reader error: {e}")))?;

        let mut values = Vec::new();
        for record in reader {
            let record =
                record.map_err(|e| RelayError::other(format!("Avro record read error: {e}")))?;
            let json: Value = from_value::<Value>(&record)
                .map_err(|e| RelayError::other(format!("Avro→JSON conversion error: {e}")))?;
            values.push(json);
        }

        match values.len() {
            0 => Ok(Value::Null),
            1 => Ok(values.remove(0)),
            _ => Ok(Value::Array(values)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_confluent_wire_encode_decode_roundtrip() {
        let schema_id = 42_i32;
        let payload = b"hello avro";

        let encoded = encode_confluent_wire(schema_id, payload);
        assert_eq!(encoded[0], CONFLUENT_MAGIC);
        assert_eq!(encoded.len(), CONFLUENT_HEADER_SIZE + payload.len());

        let (decoded_id, decoded_payload) = decode_confluent_wire(&encoded).unwrap();
        assert_eq!(decoded_id, schema_id);
        assert_eq!(decoded_payload, payload);
    }

    #[test]
    fn test_decode_too_short() {
        let err = decode_confluent_wire(&[0x00]).unwrap_err();
        assert!(err.contains("at least 5 bytes"));
    }

    #[test]
    fn test_decode_wrong_magic() {
        let data = [0x01, 0x00, 0x00, 0x00, 0x01, b'x'];
        let err = decode_confluent_wire(&data).unwrap_err();
        assert!(err.contains("magic byte"));
    }

    #[test]
    fn test_subject_name_strategies() {
        let tn = SubjectNameStrategy::Topic;
        assert_eq!(tn.subject("orders", "Order", false), "orders-value");
        assert_eq!(tn.subject("orders", "Order", true), "orders-key");

        let rn = SubjectNameStrategy::Record;
        assert_eq!(rn.subject("orders", "Order", false), "Order-value");

        let trn = SubjectNameStrategy::TopicRecord;
        assert_eq!(trn.subject("orders", "Order", false), "orders-Order-value");
    }

    #[test]
    fn test_format_from_str() {
        assert_eq!(SerializationFormat::from_str("json").as_str(), "json");
        assert_eq!(SerializationFormat::from_str("unknown").as_str(), "json");
    }

    #[test]
    fn test_config_parse() {
        let config = serde_json::json!({
            "schema_registry": {
                "url": "http://localhost:8081",
                "username": "user",
                "auto_register": false
            },
            "serialization": {
                "format": "json",
                "subject_name_strategy": "RecordName"
            }
        });
        let sr = SchemaRegistryConfig::from_pipeline_config(&config);
        assert_eq!(sr.url.as_deref(), Some("http://localhost:8081"));
        assert_eq!(sr.username.as_deref(), Some("user"));
        assert!(!sr.auto_register);
        assert_eq!(sr.subject_strategy, SubjectNameStrategy::Record);
    }

    #[test]
    fn test_inactive_when_no_url() {
        let config = SchemaRegistryConfig::default();
        assert!(!config.is_active());
    }
}
