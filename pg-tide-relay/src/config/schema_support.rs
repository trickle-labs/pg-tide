//! Canonical, versioned pipeline configuration types and schema support.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::descriptors;
use crate::error::RelayError;

pub const PIPELINE_SCHEMA_VERSION: u8 = 1;

/// The owned portion of a catalog pipeline row. Connector-specific nested
/// values are intentionally retained as JSON because factories own those
/// protocols; their surrounding object remains strict and versioned.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PipelineDocument {
    #[serde(default = "default_schema_version")]
    pub schema_version: u8,
    pub source_type: String,
    pub source: Map<String, Value>,
    pub sink_type: String,
    pub sink: Map<String, Value>,
    #[serde(default)]
    pub batch_size: Option<i64>,
    #[serde(default)]
    pub retry: Option<Map<String, Value>>,
    #[serde(default)]
    pub dlq: Option<Map<String, Value>>,
    #[serde(default)]
    pub transforms: Option<Vec<Value>>,
    #[serde(default)]
    pub encoding: Option<Map<String, Value>>,
}

fn default_schema_version() -> u8 {
    PIPELINE_SCHEMA_VERSION
}

impl PipelineDocument {
    pub fn parse(name: &str, raw: &Value) -> Result<Self, RelayError> {
        Self::parse_with_mode(name, raw, true)
    }

    /// Parse a catalog row for runtime compatibility. Known preview,
    /// experimental, and diagnostic connectors remain runnable but are not
    /// accepted as part of the frozen v1 schema.
    pub fn parse_runtime(name: &str, raw: &Value) -> Result<Self, RelayError> {
        Self::parse_with_mode(name, raw, false)
    }

    fn parse_with_mode(name: &str, raw: &Value, v1_only: bool) -> Result<Self, RelayError> {
        let object = raw
            .as_object()
            .ok_or_else(|| invalid(name, "pipeline config must be an object"))?;
        let mut normalized = object.clone();
        if !normalized.contains_key("sink") {
            if let Some(Value::Object(sink)) = normalized.remove("config") {
                normalized.insert("sink".to_string(), Value::Object(sink));
            }
        }
        if !normalized.contains_key("source_type") && normalized.contains_key("source") {
            normalized.insert(
                "source_type".to_string(),
                Value::String("outbox".to_string()),
            );
        }
        normalized
            .entry("schema_version".to_string())
            .or_insert(Value::from(PIPELINE_SCHEMA_VERSION));
        let value = Value::Object(normalized);
        let document: Self =
            serde_json::from_value(value).map_err(|error| invalid(name, error.to_string()))?;
        document.validate_with_mode(name, v1_only)?;
        Ok(document)
    }

    pub fn validate(&self, name: &str) -> Result<(), RelayError> {
        self.validate_with_mode(name, true)
    }

    fn validate_with_mode(&self, name: &str, v1_only: bool) -> Result<(), RelayError> {
        if self.schema_version != PIPELINE_SCHEMA_VERSION {
            return Err(invalid(
                name,
                format!("unsupported schema_version {}", self.schema_version),
            ));
        }
        if self.source_type.trim().is_empty() || self.sink_type.trim().is_empty() {
            return Err(invalid(name, "source_type and sink_type must not be empty"));
        }
        if self
            .batch_size
            .is_some_and(|size| !(1..=1_000_000).contains(&size))
        {
            return Err(invalid(name, "batch_size must be between 1 and 1000000"));
        }
        validate_connector(name, "source_type", &self.source_type, v1_only)?;
        validate_connector(name, "sink_type", &self.sink_type, v1_only)?;
        validate_connector_fields(name, "source", &self.source_type, &self.source)?;
        validate_connector_fields(name, "sink", &self.sink_type, &self.sink)?;
        validate_supported_source(name, &self.source_type, &self.source)?;
        validate_supported_sink(name, &self.sink_type, &self.sink)?;
        if let Some(size) = self.batch_size {
            if let Some(descriptor) = descriptors::sink_type_to_descriptor(&self.sink_type) {
                if let Some(capabilities) = descriptor.capabilities {
                    if size > i64::from(capabilities.max_batch_size) {
                        return Err(invalid(
                            name,
                            format!(
                                "batch_size {size} exceeds {} maximum for sink_type '{}'",
                                capabilities.max_batch_size, self.sink_type
                            ),
                        ));
                    }
                }
            }
        }
        if let Some(retry) = &self.retry {
            reject_keys(name, "retry", retry, &["max_retries", "backoff_ms"])?;
            if let Some(value) = retry.get("max_retries") {
                require_integer(name, "retry.max_retries", value, 0, 100_000)?;
            }
            if let Some(value) = retry.get("backoff_ms") {
                require_integer(name, "retry.backoff_ms", value, 0, i64::MAX)?;
            }
        }
        if let Some(dlq) = &self.dlq {
            reject_keys(name, "dlq", dlq, &["enabled", "max_retries"])?;
            if let Some(value) = dlq.get("enabled") {
                if !value.is_boolean() {
                    return Err(invalid(name, "dlq.enabled must be a boolean"));
                }
            }
            if let Some(value) = dlq.get("max_retries") {
                require_integer(name, "dlq.max_retries", value, 0, 100_000)?;
            }
        }
        if let Some(encoding) = &self.encoding {
            reject_keys(name, "encoding", encoding, &["format"])?;
            if let Some(value) = encoding.get("format") {
                let format = value
                    .as_str()
                    .ok_or_else(|| invalid(name, "encoding.format must be a string"))?;
                if !["json", "jsonl", "native", "avro", "protobuf"].contains(&format) {
                    return Err(invalid(
                        name,
                        format!("unsupported encoding.format '{format}'"),
                    ));
                }
            }
        }
        validate_secret_references(name, &Value::Object(self.source.clone()))?;
        validate_secret_references(name, &Value::Object(self.sink.clone()))?;
        Ok(())
    }

    pub fn canonical_json(&self) -> Result<Value, RelayError> {
        serde_json::to_value(self).map_err(RelayError::Json)
    }
}

fn invalid(name: &str, reason: impl Into<String>) -> RelayError {
    RelayError::InvalidConfig {
        name: name.to_string(),
        reason: reason.into(),
    }
}

fn validate_connector(
    name: &str,
    field: &str,
    connector: &str,
    v1_only: bool,
) -> Result<(), RelayError> {
    let accepted = if v1_only {
        match field {
            "source_type" => descriptors::V1_SUPPORTED_SOURCE_TYPES.contains(&connector),
            "sink_type" => descriptors::V1_SUPPORTED_SINK_TYPES.contains(&connector),
            _ => false,
        }
    } else {
        match field {
            "source_type" => descriptors::source_type_to_descriptor(connector).is_some(),
            "sink_type" => descriptors::sink_type_to_descriptor(connector).is_some(),
            _ => false,
        }
    };
    if accepted {
        return Ok(());
    }
    Err(invalid(
        name,
        format!("{field} '{connector}' is not supported by pipeline schema v1"),
    ))
}

pub fn connector_available(connector: &str) -> bool {
    (descriptors::V1_SUPPORTED_SOURCE_TYPES.contains(&connector)
        || descriptors::V1_SUPPORTED_SINK_TYPES.contains(&connector))
        && descriptors::source_type_to_descriptor(connector)
            .or_else(|| descriptors::sink_type_to_descriptor(connector))
            .is_some_and(descriptors::is_available)
}

fn validate_supported_source(
    name: &str,
    source_type: &str,
    values: &Map<String, Value>,
) -> Result<(), RelayError> {
    if source_type != "outbox" {
        return Ok(());
    }
    require_string_field(name, "source.outbox", values, true)?;
    optional_non_empty_string_field(name, "source.consumer_group", values)?;
    optional_non_empty_string_field(name, "source.consumer_id", values)?;
    if let Some(value) = values.get("subject_template") {
        validate_destination_text(name, "source.subject_template", value)?;
    }
    if let Some(value) = values.get("visibility_seconds") {
        require_integer(
            name,
            "source.visibility_seconds",
            value,
            1,
            i64::from(i32::MAX),
        )?;
    }
    Ok(())
}

const CONNECTOR_FIELDS: &[&str] = &[
    "access_token",
    "account",
    "addr",
    "allow_http",
    "atomic_lake_writes",
    "auth_token",
    "avatar_url",
    "batch_limit",
    "batch_size",
    "brokers",
    "bucket",
    "buffer_max_bytes",
    "buffer_max_rows",
    "buffer_max_seconds",
    "catalog_connection",
    "catalog_schema",
    "change_data_feed",
    "client_id",
    "collection_template",
    "component",
    "configured_catalog",
    "connection_string",
    "container",
    "data_path",
    "database",
    "dataset_id",
    "descriptor_path",
    "destination_args",
    "destination_command",
    "destination_config",
    "destination_image",
    "destination_name",
    "doc_id_field",
    "endpoint",
    "entity",
    "event_hub",
    "event_type",
    "exchange",
    "format",
    "group",
    "group_id",
    "icon_emoji",
    "inbox",
    "index_template",
    "inline_row_limit",
    "is_fifo",
    "iterator_type",
    "last_snapshot_id",
    "max_len",
    "max_messages",
    "namespace",
    "on_schema_change",
    "outbox",
    "partition",
    "partition_by_date",
    "partition_count",
    "partition_key_template",
    "password",
    "path",
    "prefix",
    "postgres_url",
    "project_id",
    "provider",
    "qos",
    "queue",
    "queue_url",
    "region",
    "root",
    "routing_key",
    "routing_key_template",
    "rows_per_file",
    "schema",
    "secret",
    "severity",
    "signing_algorithm",
    "signing_secret",
    "source",
    "source_args",
    "source_command",
    "source_config",
    "source_image",
    "source_name",
    "snapshot_poll_interval_ms",
    "ssrf_protection",
    "storage_provider",
    "stream",
    "stream_key",
    "stream_key_template",
    "stream_name",
    "stream_name_template",
    "subject",
    "subject_template",
    "subscription",
    "table",
    "table_path",
    "table_template",
    "tap_args",
    "tap_command",
    "tap_name",
    "target_args",
    "target_command",
    "target_name",
    "timeout_secs",
    "topic",
    "topic_template",
    "url",
    "user",
    "username",
    "visibility_seconds",
    "warehouse_path",
    "webhook_url",
    "write_concern",
    "write_mode",
];

fn validate_connector_fields(
    name: &str,
    field: &str,
    connector: &str,
    values: &Map<String, Value>,
) -> Result<(), RelayError> {
    if let Some(descriptor) = descriptors::source_type_to_descriptor(connector)
        .or_else(|| descriptors::sink_type_to_descriptor(connector))
    {
        if descriptor.capabilities.is_some() {
            return reject_keys(name, field, values, descriptor.config_fields);
        }
    }
    reject_keys(name, field, values, CONNECTOR_FIELDS)
}

fn validate_supported_sink(
    name: &str,
    sink_type: &str,
    values: &Map<String, Value>,
) -> Result<(), RelayError> {
    if !descriptors::is_supported_sink_type(sink_type) {
        return Ok(());
    }
    match sink_type {
        "inbox" => {
            require_string_field(name, "sink.inbox", values, true)?;
            optional_non_empty_string_field(name, "sink.postgres_url", values)?;
        }
        "pg_outbox" => {
            require_string_field(name, "sink.inbox", values, true)?;
            require_string_field(name, "sink.postgres_url", values, true)?;
        }
        "nats" => {
            require_string_field(name, "sink.url", values, true)?;
            optional_string_field(name, "sink.subject", values)?;
            optional_string_field(name, "sink.subject_template", values)?;
            if values.contains_key("subject") && values.contains_key("subject_template") {
                return Err(invalid(
                    name,
                    "sink.subject and sink.subject_template are mutually exclusive",
                ));
            }
            for field in ["subject", "subject_template"] {
                if let Some(value) = values.get(field) {
                    validate_destination_text(name, &format!("sink.{field}"), value)?;
                    #[cfg(feature = "nats")]
                    if let Some(subject) = value.as_str() {
                        crate::sink::nats::validate_subject(subject)
                            .map_err(|error| invalid(name, error.to_string()))?;
                    }
                }
            }
        }
        "kafka" => {
            require_string_field(name, "sink.brokers", values, true)?;
            if values.contains_key("topic") && values.contains_key("topic_template") {
                return Err(invalid(
                    name,
                    "sink.topic and sink.topic_template are mutually exclusive",
                ));
            }
            for field in ["topic", "topic_template"] {
                if let Some(value) = values.get(field) {
                    validate_destination_text(name, &format!("sink.{field}"), value)?;
                }
            }
        }
        "webhook" => {
            let url = require_string_field(name, "sink.url", values, true)?;
            let parsed = reqwest::Url::parse(url)
                .map_err(|_| invalid(name, "sink.url must be a valid URL"))?;
            let allow_http = values
                .get("allow_http")
                .map(|value| {
                    value
                        .as_bool()
                        .ok_or_else(|| invalid(name, "sink.allow_http must be a boolean"))
                })
                .transpose()?
                .unwrap_or(false);
            if parsed.scheme() != "https" && !(allow_http && parsed.scheme() == "http") {
                return Err(invalid(
                    name,
                    "sink.url must use HTTPS unless sink.allow_http is true",
                ));
            }
            if let Some(value) = values.get("timeout_secs") {
                let timeout = value
                    .as_i64()
                    .ok_or_else(|| invalid(name, "sink.timeout_secs must be an integer"))?;
                if !(1..=300).contains(&timeout) {
                    return Err(invalid(name, "sink.timeout_secs must be between 1 and 300"));
                }
            }
            optional_bool_field(name, "sink.ssrf_protection", values)?;
            optional_bool_field(name, "sink.allow_http", values)?;
            optional_secret_reference_field(name, "sink.signing_secret", values)?;
            if let Some(algorithm) = values.get("signing_algorithm") {
                let algorithm = algorithm
                    .as_str()
                    .ok_or_else(|| invalid(name, "sink.signing_algorithm must be a string"))?;
                if algorithm != "hmac-sha256" {
                    return Err(invalid(
                        name,
                        "sink.signing_algorithm must be 'hmac-sha256'",
                    ));
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn require_string_field<'a>(
    name: &str,
    field: &str,
    values: &'a Map<String, Value>,
    non_empty: bool,
) -> Result<&'a str, RelayError> {
    let key = field.rsplit('.').next().unwrap_or(field);
    let value = values
        .get(key)
        .ok_or_else(|| invalid(name, format!("{field} is required")))?;
    let text = value
        .as_str()
        .ok_or_else(|| invalid(name, format!("{field} must be a string")))?;
    if non_empty && text.trim().is_empty() {
        return Err(invalid(name, format!("{field} must not be empty")));
    }
    Ok(text)
}

fn optional_string_field(
    name: &str,
    field: &str,
    values: &Map<String, Value>,
) -> Result<(), RelayError> {
    let key = field.rsplit('.').next().unwrap_or(field);
    if let Some(value) = values.get(key) {
        value
            .as_str()
            .ok_or_else(|| invalid(name, format!("{field} must be a string")))?;
    }
    Ok(())
}

fn optional_non_empty_string_field(
    name: &str,
    field: &str,
    values: &Map<String, Value>,
) -> Result<(), RelayError> {
    let key = field.rsplit('.').next().unwrap_or(field);
    if let Some(value) = values.get(key) {
        let text = value
            .as_str()
            .ok_or_else(|| invalid(name, format!("{field} must be a string")))?;
        if text.trim().is_empty() {
            return Err(invalid(name, format!("{field} must not be empty")));
        }
    }
    Ok(())
}

fn optional_bool_field(
    name: &str,
    field: &str,
    values: &Map<String, Value>,
) -> Result<(), RelayError> {
    let key = field.rsplit('.').next().unwrap_or(field);
    if let Some(value) = values.get(key) {
        if !value.is_boolean() {
            return Err(invalid(name, format!("{field} must be a boolean")));
        }
    }
    Ok(())
}

fn optional_secret_reference_field(
    name: &str,
    field: &str,
    values: &Map<String, Value>,
) -> Result<(), RelayError> {
    let key = field.rsplit('.').next().unwrap_or(field);
    if let Some(value) = values.get(key) {
        let text = value
            .as_str()
            .ok_or_else(|| invalid(name, format!("{field} must be a string")))?;
        if !is_secret_reference(text) {
            return Err(invalid(
                name,
                format!("{field} must be an env/file secret reference"),
            ));
        }
    }
    Ok(())
}

fn is_secret_reference(text: &str) -> bool {
    let Some(rest) = text.strip_prefix("${") else {
        return false;
    };
    let Some(token) = rest.strip_suffix('}') else {
        return false;
    };
    let Some((kind, name)) = token.split_once(':') else {
        return false;
    };
    matches!(kind, "env" | "file") && !name.is_empty()
}

fn validate_destination_text(name: &str, field: &str, value: &Value) -> Result<(), RelayError> {
    let text = value
        .as_str()
        .ok_or_else(|| invalid(name, format!("{field} must be a string")))?;
    if text.trim().is_empty() || text.chars().any(char::is_control) {
        return Err(invalid(
            name,
            format!("{field} contains invalid destination text"),
        ));
    }
    Ok(())
}

fn reject_keys(
    name: &str,
    field: &str,
    values: &Map<String, Value>,
    allowed: &[&str],
) -> Result<(), RelayError> {
    if let Some(key) = values.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(invalid(
            name,
            format!("{field}.{key} is not a recognized field"),
        ));
    }

    Ok(())
}

fn require_integer(
    name: &str,
    field: &str,
    value: &Value,
    minimum: i64,
    maximum: i64,
) -> Result<(), RelayError> {
    let number = value
        .as_i64()
        .ok_or_else(|| invalid(name, format!("{field} must be an integer")))?;
    if !(minimum..=maximum).contains(&number) {
        return Err(invalid(
            name,
            format!("{field} must be between {minimum} and {maximum}"),
        ));
    }
    Ok(())
}

/// Secret references are accepted as strings and never resolved by schema
/// validation. This also rejects interpolation in ordinary fields.
pub fn validate_secret_references(name: &str, value: &Value) -> Result<(), RelayError> {
    match value {
        Value::String(text) if text.contains("${") => {
            let valid = text.split("${").skip(1).all(|part| {
                part.split_once('}').is_some_and(|(token, _)| {
                    token.starts_with("env:")
                        || token.starts_with("file:")
                        || token.starts_with("ENV:")
                })
            });
            if !valid {
                return Err(invalid(name, "invalid secret reference syntax"));
            }
        }
        Value::Array(values) => {
            for value in values {
                validate_secret_references(name, value)?;
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                validate_secret_references(name, value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Stable schema document used by the checked-in schema and regeneration check.
pub fn pipeline_schema() -> Value {
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://pg-tide.dev/schemas/pipeline-config-v1.schema.json",
        "title": "pg_tide pipeline configuration v1",
        "type": "object",
        "additionalProperties": false,
        "required": ["source_type", "source", "sink_type", "sink"],
        "properties": {
            "schema_version": {"const": 1, "default": 1, "type": "integer"},
            "source_type": {"type": "string", "enum": descriptors::V1_SUPPORTED_SOURCE_TYPES},
            "source": {"$ref": "#/$defs/source_outbox"},
            "sink_type": {"type": "string", "enum": descriptors::V1_SUPPORTED_SINK_TYPES},
            "sink": {"type": "object"},
            "batch_size": {"type": "integer", "minimum": 1, "maximum": 1000000},
            "retry": {"$ref": "#/$defs/retry"},
            "dlq": {"$ref": "#/$defs/dlq"},
            "transforms": {"type": "array"},
            "encoding": {"$ref": "#/$defs/encoding"}
        },
        "allOf": [
            {
                "if": {"properties": {"sink_type": {"const": "inbox"}}},
                "then": {
                    "properties": {
                        "sink": {"$ref": "#/$defs/sink_inbox"},
                        "batch_size": {"maximum": 1000}
                    }
                }
            },
            {
                "if": {"properties": {"sink_type": {"const": "pg_outbox"}}},
                "then": {
                    "properties": {
                        "sink": {"$ref": "#/$defs/sink_pg_outbox"},
                        "batch_size": {"maximum": 1000}
                    }
                }
            },
            {
                "if": {"properties": {"sink_type": {"const": "nats"}}},
                "then": {
                    "properties": {
                        "sink": {"$ref": "#/$defs/sink_nats"},
                        "batch_size": {"maximum": 100}
                    }
                }
            },
            {
                "if": {"properties": {"sink_type": {"const": "kafka"}}},
                "then": {
                    "properties": {
                        "sink": {"$ref": "#/$defs/sink_kafka"},
                        "batch_size": {"maximum": 100}
                    }
                }
            },
            {
                "if": {"properties": {"sink_type": {"const": "webhook"}}},
                "then": {
                    "properties": {
                        "sink": {"$ref": "#/$defs/sink_webhook"},
                        "batch_size": {"maximum": 100}
                    }
                }
            }
        ],
        "$defs": {
            "source_outbox": {
                "type": "object",
                "additionalProperties": false,
                "required": ["outbox"],
                "properties": {
                    "outbox": {"type": "string", "minLength": 1},
                    "subject_template": {
                        "type": "string",
                        "minLength": 1,
                        "default": "{outbox}.{op}"
                    },
                    "consumer_group": {"type": "string", "minLength": 1},
                    "consumer_id": {"type": "string", "minLength": 1, "default": "pg-tide"},
                    "visibility_seconds": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 2147483647,
                        "default": 30
                    }
                },
                "description": "Native PostgreSQL outbox source."
            },
            "sink_inbox": {
                "type": "object",
                "additionalProperties": false,
                "required": ["inbox"],
                "properties": {
                    "inbox": {"type": "string", "minLength": 1},
                    "postgres_url": {"type": "string", "minLength": 1}
                },
                "description": "PostgreSQL inbox in the local catalog; postgres_url selects a remote inbox."
            },
            "sink_pg_outbox": {
                "type": "object",
                "additionalProperties": false,
                "required": ["inbox", "postgres_url"],
                "properties": {
                    "inbox": {"type": "string", "minLength": 1},
                    "postgres_url": {"type": "string", "minLength": 1}
                },
                "description": "Deprecated PostgreSQL inbox alias; use sink_type 'inbox' with postgres_url."
            },
            "sink_nats": {
                "type": "object",
                "additionalProperties": false,
                "required": ["url"],
                "properties": {
                    "url": {"type": "string", "minLength": 1},
                    "subject": {"type": "string", "minLength": 1},
                    "subject_template": {
                        "type": "string",
                        "minLength": 1,
                        "default": "{outbox}.{op}"
                    }
                },
                "not": {"required": ["subject", "subject_template"]},
                "description": "NATS JetStream outbound sink."
            },
            "sink_kafka": {
                "type": "object",
                "additionalProperties": false,
                "required": ["brokers"],
                "properties": {
                    "brokers": {"type": "string", "minLength": 1},
                    "topic": {"type": "string", "minLength": 1},
                    "topic_template": {
                        "type": "string",
                        "minLength": 1,
                        "default": "{stream_table}"
                    }
                },
                "not": {"required": ["topic", "topic_template"]},
                "description": "Apache Kafka outbound sink."
            },
            "sink_webhook": {
                "type": "object",
                "additionalProperties": false,
                "required": ["url"],
                "properties": {
                    "url": {"type": "string", "minLength": 1},
                    "timeout_secs": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 300,
                        "default": 30
                    },
                    "allow_http": {"type": "boolean", "default": false},
                    "ssrf_protection": {"type": "boolean", "default": true},
                    "signing_secret": {
                        "type": "string",
                        "pattern": "^\\$\\{(?:env|file):[^}]+\\}$"
                    },
                    "signing_algorithm": {
                        "type": "string",
                        "enum": ["hmac-sha256"],
                        "default": "hmac-sha256"
                    }
                },
                "oneOf": [
                    {
                        "required": ["allow_http"],
                        "properties": {
                            "allow_http": {"const": true},
                            "url": {"pattern": "^https?://"}
                        }
                    },
                    {
                        "not": {"required": ["allow_http"]},
                        "properties": {"url": {"pattern": "^https://"}}
                    },
                    {
                        "required": ["allow_http"],
                        "properties": {
                            "allow_http": {"const": false},
                            "url": {"pattern": "^https://"}
                        }
                    }
                ],
                "description": "HTTPS webhook outbound sink."
            },
            "retry": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "max_retries": {"type": "integer", "minimum": 0, "maximum": 100000},
                    "backoff_ms": {"type": "integer", "minimum": 0}
                }
            },
            "dlq": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "enabled": {"type": "boolean"},
                    "max_retries": {"type": "integer", "minimum": 0, "maximum": 100000}
                }
            },
            "encoding": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "format": {"type": "string", "enum": ["json", "jsonl", "native", "avro", "protobuf"]}
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rejects_unknown_pipeline_and_connector_keys() {
        let err = PipelineDocument::parse(
            "orders",
            &json!({
                "source_type": "outbox",
                "source": {"outbox": "orders", "typo": true},
                "sink_type": "nats",
                "sink": {"url": "nats://localhost"}
            }),
        )
        .unwrap_err();
        assert!(err.to_string().contains("source.typo"));
    }

    #[test]
    fn rejects_invalid_numeric_bounds() {
        let err = PipelineDocument::parse(
            "orders",
            &json!({
                "source_type": "outbox",
                "source": {"outbox": "orders"},
                "sink_type": "stdout",
                "sink": {},
                "batch_size": 0
            }),
        )
        .unwrap_err();
        assert!(err.to_string().contains("batch_size"));
    }

    #[test]
    fn rejects_sink_batch_above_generated_capability() {
        let err = PipelineDocument::parse(
            "orders",
            &json!({
                "source_type": "outbox",
                "source": {"outbox": "orders"},
                "sink_type": "nats",
                "sink": {"url": "nats://localhost"},
                "batch_size": 101
            }),
        )
        .unwrap_err();
        assert!(err.to_string().contains("maximum"));
    }

    #[test]
    fn validates_canonical_postgres_alias_contracts() {
        let err = PipelineDocument::parse(
            "orders",
            &json!({
                "source_type": "outbox",
                "source": {"outbox": "orders"},
                "sink_type": "pg_outbox",
                "sink": {"inbox": "notifications"}
            }),
        )
        .unwrap_err();
        assert!(err.to_string().contains("postgres_url"));

        PipelineDocument::parse(
            "orders",
            &json!({
                "source_type": "outbox",
                "source": {"outbox": "orders"},
                "sink_type": "inbox",
                "sink": {"inbox": "notifications"}
            }),
        )
        .expect("canonical local inbox should not require a remote URL");
    }

    #[test]
    fn legacy_rows_normalize_to_v1_without_resolving_secrets() {
        let doc = PipelineDocument::parse(
            "orders",
            &json!({
                "source_type": "outbox",
                "source": {"outbox": "orders"},
                "sink_type": "nats",
                "sink": {"url": "${env:NATS_URL}"}
            }),
        )
        .unwrap();
        assert_eq!(doc.schema_version, 1);
        assert_eq!(
            doc.canonical_json().unwrap()["sink"]["url"],
            "${env:NATS_URL}"
        );
    }

    #[test]
    fn schema_is_draft_2020_and_strict_at_owned_objects() {
        let schema = pipeline_schema();
        assert_eq!(
            schema["$schema"],
            "https://json-schema.org/draft/2020-12/schema"
        );
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(
            schema["$defs"]["source_outbox"]["additionalProperties"],
            false
        );
        assert_eq!(
            schema["$defs"]["sink_webhook"]["additionalProperties"],
            false
        );
        assert_eq!(schema["$defs"]["retry"]["additionalProperties"], false);
        assert_eq!(
            schema["properties"]["source_type"]["enum"],
            serde_json::json!(["outbox"])
        );
        assert_eq!(
            schema["properties"]["sink_type"]["enum"],
            serde_json::json!(["inbox", "kafka", "nats", "pg_outbox", "webhook"])
        );
    }

    #[test]
    fn rejects_preview_and_experimental_connector_types() {
        for (field, value) in [("source_type", "nats"), ("sink_type", "redis")] {
            let mut config = json!({
                "source_type": "outbox",
                "source": {"outbox": "orders"},
                "sink_type": "nats",
                "sink": {"url": "nats://localhost"}
            });
            config[field] = json!(value);
            assert!(
                PipelineDocument::parse("orders", &config).is_err(),
                "{field}={value} must remain outside v1"
            );
        }
    }

    #[test]
    fn validates_supported_variant_fields_and_defaults() {
        PipelineDocument::parse(
            "orders",
            &json!({
                "schema_version": 1,
                "source_type": "outbox",
                "source": {"outbox": "orders", "visibility_seconds": 30},
                "sink_type": "webhook",
                "sink": {
                    "url": "https://example.test/hook",
                    "signing_secret": "${env:WEBHOOK_SECRET}"
                }
            }),
        )
        .expect("supported webhook config should validate");

        let err = PipelineDocument::parse(
            "orders",
            &json!({
                "source_type": "outbox",
                "source": {"outbox": "orders"},
                "sink_type": "kafka",
                "sink": {
                    "brokers": "localhost:9092",
                    "topic": "orders",
                    "topic_template": "{stream_table}"
                }
            }),
        )
        .unwrap_err();
        assert!(err.to_string().contains("mutually exclusive"));
    }
}
