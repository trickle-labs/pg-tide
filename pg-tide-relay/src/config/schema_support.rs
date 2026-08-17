//! Canonical, versioned pipeline configuration types and schema support.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

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
        document.validate(name)?;
        Ok(document)
    }

    pub fn validate(&self, name: &str) -> Result<(), RelayError> {
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
        validate_connector(name, "source_type", &self.source_type)?;
        validate_connector(name, "sink_type", &self.sink_type)?;
        validate_connector_fields(name, "source", &self.source)?;
        validate_connector_fields(name, "sink", &self.sink)?;
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

const BUILTIN_CONNECTORS: &[&str] = &[
    "airbyte",
    "bigquery",
    "clickhouse",
    "delta",
    "discord",
    "ducklake",
    "elasticsearch",
    "eventhubs",
    "fanin",
    "file",
    "iceberg",
    "inbox",
    "kafka",
    "kinesis",
    "mongodb",
    "mqtt",
    "nats",
    "object_storage",
    "outbox",
    "pagerduty",
    "pg_logical",
    "pg_outbox",
    "pg_trickle_outbox",
    "pubsub",
    "rabbitmq",
    "redis",
    "rocklake",
    "servicebus",
    "singer",
    "slack",
    "snowflake",
    "sqs",
    "stdin",
    "stdout",
    "webhook",
];

fn validate_connector(name: &str, field: &str, connector: &str) -> Result<(), RelayError> {
    if BUILTIN_CONNECTORS.contains(&connector) {
        return Ok(());
    }
    Err(invalid(
        name,
        format!("{field} '{connector}' is not a known connector"),
    ))
}

pub fn connector_available(connector: &str) -> bool {
    match connector {
        "outbox" | "pg_trickle_outbox" | "inbox" | "pg_outbox" | "stdin" | "fanin" => true,
        "stdout" | "file" => cfg!(feature = "stdout"),
        "nats" => cfg!(feature = "nats"),
        "webhook" => cfg!(feature = "webhook"),
        "kafka" => cfg!(feature = "kafka"),
        "redis" => cfg!(feature = "redis"),
        "sqs" => cfg!(feature = "sqs"),
        "rabbitmq" => cfg!(feature = "rabbitmq"),
        "pubsub" => cfg!(feature = "pubsub"),
        "kinesis" => cfg!(feature = "kinesis"),
        "servicebus" => cfg!(feature = "servicebus"),
        "mqtt" => cfg!(feature = "mqtt"),
        "eventhubs" => cfg!(feature = "eventhubs"),
        "singer" => cfg!(feature = "singer"),
        "airbyte" => cfg!(feature = "airbyte"),
        "rocklake" => cfg!(feature = "rocklake"),
        "pg_logical" => cfg!(feature = "wal-source"),
        "bigquery" | "clickhouse" | "discord" | "ducklake" | "elasticsearch" | "iceberg"
        | "mongodb" | "object_storage" | "pagerduty" | "slack" | "snowflake" | "delta" => false,
        _ => false,
    }
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
    values: &Map<String, Value>,
) -> Result<(), RelayError> {
    reject_keys(name, field, values, CONNECTOR_FIELDS)
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
    let mut schema = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://pg-tide.dev/schemas/pipeline-config-v1.schema.json",
        "title": "pg_tide pipeline configuration v1",
        "type": "object",
        "additionalProperties": false,
        "required": ["source_type", "source", "sink_type", "sink"],
        "properties": {
            "schema_version": {"const": 1, "type": "integer"},
            "source_type": {"type": "string", "enum": BUILTIN_CONNECTORS},
            "source": {"$ref": "#/$defs/connector"},
            "sink_type": {"type": "string", "enum": BUILTIN_CONNECTORS},
            "sink": {"$ref": "#/$defs/connector"},
            "batch_size": {"type": "integer", "minimum": 1, "maximum": 1000000},
            "retry": {"$ref": "#/$defs/retry"},
            "dlq": {"$ref": "#/$defs/dlq"},
            "transforms": {"type": "array"},
            "encoding": {"$ref": "#/$defs/encoding"}
        },
        "$defs": {
            "connector": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "access_token": {"type": "string"},
                    "account": {"type": "string"},
                    "addr": {"type": "string"},
                    "allow_http": {"type": "boolean"},
                    "auth_token": {"type": "string"},
                    "batch_size": {"type": "integer"},
                    "brokers": {"type": "string"},
                    "connection_string": {"type": "string"},
                    "consumer": {"type": "string"},
                    "consumer_group": {"type": "string"},
                    "consumer_id": {"type": "string"},
                    "event_type": {"type": "string"},
                    "format": {"type": "string"},
                    "group": {"type": "string"},
                    "outbox": {"type": "string"},
                    "password": {"type": "string"},
                    "path": {"type": "string"},
                    "project_id": {"type": "string"},
                    "queue": {"type": "string"},
                    "queue_url": {"type": "string"},
                    "schema": {"type": "string"},
                    "stream": {"type": "string"},
                    "subject": {"type": "string"},
                    "topic": {"type": "string"},
                    "url": {"type": "string"},
                    "username": {"type": "string"},
                    "webhook_url": {"type": "string"}
                },
                "description": "Connector-owned fields; values may contain secret references."
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
    });
    if let Some(properties) = schema["$defs"]["connector"]["properties"].as_object_mut() {
        for field in CONNECTOR_FIELDS {
            properties
                .entry((*field).to_string())
                .or_insert_with(|| serde_json::json!({}));
        }
        for field in [
            "access_token",
            "auth_token",
            "password",
            "secret",
            "signing_secret",
        ] {
            properties.insert(
                field.to_string(),
                serde_json::json!({
                    "type": "string",
                    "pattern": "^\\$\\{(?:env|file):[^}]+\\}$"
                }),
            );
        }
    }
    schema
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
        assert_eq!(schema["$defs"]["connector"]["additionalProperties"], false);
        assert_eq!(schema["$defs"]["retry"]["additionalProperties"], false);
    }
}
