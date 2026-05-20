/// Configuration types for the relay binary.
/// All pipeline definitions live in the PostgreSQL catalog tables.
/// This module only handles CLI/env/TOML configuration for the relay process itself.
use serde::{Deserialize, Serialize};

/// Top-level relay process configuration (not pipeline config — that lives in PG).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RelayConfig {
    /// PostgreSQL connection URL (required).
    /// Supports `${ENV:VAR_NAME}` substitution at load time (A30).
    pub postgres_url: String,

    /// Prometheus metrics + health endpoint address.
    pub metrics_addr: String,

    /// Log format: "text" or "json".
    pub log_format: LogFormat,

    /// Log level (e.g. "info", "debug", "warn", "error").
    pub log_level: String,

    /// Poll interval for pipeline discovery (seconds).
    pub discovery_interval_secs: u64,

    /// Default batch size when not specified per-pipeline.
    pub default_batch_size: i64,

    /// Relay group ID for advisory locks and offset namespacing.
    pub relay_group_id: String,

    /// A39: Maximum number of in-flight messages to the downstream sink
    /// before upstream polling is paused.  0 = unlimited (legacy behaviour).
    pub sink_max_inflight: usize,

    /// v0.15.0: Maximum number of pipeline workers this relay instance will
    /// own concurrently.  Each worker holds one PostgreSQL connection.
    /// `--max-pipelines` / `PG_TIDE_MAX_PIPELINES` / TOML `max_owned_pipelines`.
    pub max_owned_pipelines: usize,

    /// v0.15.0: Maximum number of connections in the coordinator connection pool.
    /// `--max-connections` / `PG_TIDE_MAX_CONNECTIONS` / TOML `max_connections`.
    pub max_connections: usize,

    /// v0.25.0: Tenant ID for multi-tenant relay groups.
    /// When set, the coordinator filters pipeline discovery to only own pipelines
    /// belonging to this tenant.  Advisory lock keys incorporate the tenant hash.
    /// `--tenant-id` / `PG_TIDE_TENANT_ID` / TOML `tenant_id`.
    pub tenant_id: Option<String>,

    /// v0.28.0: Configuration mode for pipeline discovery.
    /// `catalog_only` — reject TOML [[pipeline]] blocks that have no matching
    ///   row in tide.tide_outbox_config / tide.tide_inbox_config.
    /// `toml_allowed` (default) — emit a warning for TOML-only pipelines and
    ///   continue, preserving backward compatibility.
    pub config_mode: ConfigMode,
}

/// v0.28.0: Controls how the relay handles TOML-defined pipeline blocks that
/// are absent from the PostgreSQL catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ConfigMode {
    /// Allow TOML-defined pipelines that are not in the catalog (default).
    /// Emits a `tracing::warn!` for each orphaned TOML pipeline.
    #[default]
    TomlAllowed,
    /// Reject startup if any TOML [[pipeline]] block lacks a matching catalog row.
    CatalogOnly,
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            postgres_url: String::new(),
            metrics_addr: "0.0.0.0:9090".to_string(),
            log_format: LogFormat::Text,
            log_level: "info".to_string(),
            discovery_interval_secs: 30,
            default_batch_size: 100,
            relay_group_id: "default".to_string(),
            sink_max_inflight: 1_000,
            max_owned_pipelines: 50,
            max_connections: 52, // 2 coordinator + 50 workers by default
            tenant_id: None,
            config_mode: ConfigMode::TomlAllowed,
        }
    }
}

impl RelayConfig {
    /// A30: Expand `${ENV:VAR_NAME}` placeholders in a connection string using
    /// the current process environment.  Unknown variables are left as-is so
    /// callers can detect mis-configuration.
    ///
    /// # Security
    /// Only reads from the process environment — no eval or shell expansion.
    pub fn expand_env_vars(s: &str) -> String {
        let mut result = String::with_capacity(s.len());
        let mut rest = s;
        while let Some(start) = rest.find("${ENV:") {
            result.push_str(&rest[..start]);
            let after = &rest[start + 6..];
            if let Some(end) = after.find('}') {
                let var_name = &after[..end];
                match std::env::var(var_name) {
                    Ok(val) => result.push_str(&val),
                    Err(_) => {
                        // Leave the placeholder intact so the caller can detect the error.
                        result.push_str("${ENV:");
                        result.push_str(var_name);
                        result.push('}');
                    }
                }
                rest = &after[end + 1..];
            } else {
                // Malformed placeholder — pass through verbatim.
                result.push_str("${ENV:");
                rest = after;
            }
        }
        result.push_str(rest);
        result
    }

    /// Expand all connection string fields.
    pub fn resolve_env_vars(mut self) -> Self {
        self.postgres_url = Self::expand_env_vars(&self.postgres_url);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    #[default]
    Text,
    Json,
}

/// Pipeline configuration loaded from `relay_outbox_config` or `relay_inbox_config`.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Pipeline name (primary key in catalog table).
    pub name: String,
    /// "forward" or "reverse".
    pub direction: PipelineDirection,
    /// Whether the pipeline is enabled.
    pub enabled: bool,
    /// The full config JSONB from the catalog.
    pub config: serde_json::Value,
    /// v0.14.0: Tenant discriminator for multi-tenant relay groups.
    pub tenant_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineDirection {
    Forward,
    Reverse,
}

impl PipelineConfig {
    /// Extract a required string value from the pipeline config.
    pub fn require_str<'a>(&'a self, path: &[&str]) -> Result<&'a str, crate::error::RelayError> {
        let mut v = &self.config;
        for key in path {
            v = v
                .get(key)
                .ok_or_else(|| crate::error::RelayError::MissingConfigKey {
                    pipeline: self.name.clone(),
                    key: key.to_string(),
                })?;
        }
        v.as_str()
            .ok_or_else(|| crate::error::RelayError::InvalidConfig {
                name: self.name.clone(),
                reason: format!("{}: expected string", path.join(".")),
            })
    }

    /// Extract an optional string value from the pipeline config.
    pub fn opt_str<'a>(&'a self, path: &[&str]) -> Option<&'a str> {
        let mut v = &self.config;
        for key in path {
            v = v.get(key)?;
        }
        v.as_str()
    }

    /// Extract an optional i64 value from the pipeline config.
    pub fn opt_i64(&self, path: &[&str]) -> Option<i64> {
        let mut v = &self.config;
        for key in path {
            v = v.get(key)?;
        }
        v.as_i64()
    }

    /// Extract an optional bool value from the pipeline config.
    pub fn opt_bool(&self, path: &[&str]) -> Option<bool> {
        let mut v = &self.config;
        for key in path {
            v = v.get(key)?;
        }
        v.as_bool()
    }
}

// ── RELAY-SEC: Pipeline config secret resolution ───────────────────────────

/// v0.15.0: Returns a copy of `config` with any string that contains a secret
/// token pattern (`${env:…}` or `${file:…}`) replaced by `"[REDACTED]"`.
///
/// Use this when logging pipeline configuration to avoid leaking credentials
/// (OWASP A02:2021 Cryptographic Failures — logging sensitive data).
pub fn mask_secrets_for_logging(config: &serde_json::Value) -> serde_json::Value {
    mask_json_secrets(config.clone())
}

fn mask_json_secrets(v: serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::String(s) => {
            if s.contains("${env:") || s.contains("${file:") {
                serde_json::Value::String("[REDACTED]".to_string())
            } else {
                serde_json::Value::String(s)
            }
        }
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.into_iter()
                .map(|(k, v)| (k, mask_json_secrets(v)))
                .collect(),
        ),
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.into_iter().map(mask_json_secrets).collect())
        }
        other => other,
    }
}

/// v0.15.0: Validate a PostgreSQL identifier sourced from relay configuration.
///
/// Identifiers are always double-quoted in SQL (`"name"`), so the only
/// characters that can break out of double-quoting are `"` and the null byte.
///
/// Rules:
/// - Non-empty
/// - ≤ 63 bytes (PostgreSQL's `NAMEDATALEN - 1`)
/// - No double-quote characters (`"`) or null bytes
pub fn validate_relay_identifier(name: &str) -> Result<(), crate::error::RelayError> {
    if name.is_empty() {
        return Err(crate::error::RelayError::Config(
            "identifier must not be empty".to_string(),
        ));
    }
    if name.len() > 63 {
        return Err(crate::error::RelayError::Config(format!(
            "identifier '{name}' exceeds 63 bytes (PostgreSQL NAMEDATALEN limit)"
        )));
    }
    for c in name.chars() {
        if c == '"' || c == '\0' {
            return Err(crate::error::RelayError::Config(format!(
                "identifier '{name}' contains invalid character '{c}'"
            )));
        }
    }
    Ok(())
}

/// Recursively resolve `${env:VAR}` and `${file:/path}` tokens in every
/// string value within a pipeline config JSONB.///
/// On success returns the fully-resolved value.  On error (unknown env var,
/// missing file, invalid var name) returns `RelayError::SecretNotFound` or
/// similar so the coordinator can disable only the affected pipeline rather
/// than crashing the process.
pub fn resolve_pipeline_secrets(
    config: serde_json::Value,
) -> Result<serde_json::Value, crate::error::RelayError> {
    resolve_json_value(config)
}

fn resolve_json_value(v: serde_json::Value) -> Result<serde_json::Value, crate::error::RelayError> {
    match v {
        serde_json::Value::String(s) => Ok(serde_json::Value::String(resolve_secret_str(&s)?)),
        serde_json::Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, v) in map {
                out.insert(k, resolve_json_value(v)?);
            }
            Ok(serde_json::Value::Object(out))
        }
        serde_json::Value::Array(arr) => {
            let out: Result<Vec<_>, _> = arr.into_iter().map(resolve_json_value).collect();
            Ok(serde_json::Value::Array(out?))
        }
        other => Ok(other),
    }
}

fn resolve_secret_str(s: &str) -> Result<String, crate::error::RelayError> {
    let mut result = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = rest.find("${") {
        result.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        if let Some(end) = after.find('}') {
            let token = &after[..end];
            if let Some(var_name) = token.strip_prefix("env:") {
                validate_secret_var_name(var_name)?;
                match std::env::var(var_name) {
                    Ok(val) => result.push_str(&val),
                    Err(_) => {
                        return Err(crate::error::RelayError::SecretNotFound {
                            token: format!("${{{token}}}"),
                        });
                    }
                }
            } else if let Some(path) = token.strip_prefix("file:") {
                let val = std::fs::read_to_string(path).map_err(|e| {
                    crate::error::RelayError::SecretReadError {
                        path: path.to_string(),
                        reason: e.to_string(),
                    }
                })?;
                result.push_str(val.trim_end_matches('\n'));
            } else {
                // Unknown token type — pass through verbatim.
                result.push_str("${");
                result.push_str(token);
                result.push('}');
            }
            rest = &after[end + 1..];
        } else {
            // Malformed — no closing brace, pass through verbatim.
            result.push_str("${");
            rest = after;
        }
    }
    result.push_str(rest);
    Ok(result)
}

/// Only ASCII letters, digits, and underscores are allowed in variable names.
fn validate_secret_var_name(name: &str) -> Result<(), crate::error::RelayError> {
    if !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        Ok(())
    } else {
        Err(crate::error::RelayError::InvalidSecretToken(
            name.to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_pipeline(config: serde_json::Value) -> PipelineConfig {
        PipelineConfig {
            name: "test".to_string(),
            direction: PipelineDirection::Forward,
            enabled: true,
            config,
            tenant_name: "default".to_string(),
        }
    }

    #[test]
    fn test_require_str_nested() {
        let cfg = make_pipeline(json!({
            "source_type": "outbox",
            "source": { "outbox": "orders", "group": "relay-1" },
            "sink_type": "nats",
            "sink": { "type": "nats", "url": "nats://localhost:4222" }
        }));
        assert_eq!(cfg.require_str(&["source", "outbox"]).unwrap(), "orders");
        assert_eq!(
            cfg.require_str(&["sink", "url"]).unwrap(),
            "nats://localhost:4222"
        );
    }

    #[test]
    fn test_require_str_missing() {
        let cfg = make_pipeline(json!({"source_type": "outbox"}));
        assert!(cfg.require_str(&["source", "outbox"]).is_err());
    }

    #[test]
    fn test_opt_i64() {
        let cfg = make_pipeline(json!({"sink": {"batch_size": 500}}));
        assert_eq!(cfg.opt_i64(&["sink", "batch_size"]), Some(500));
        assert_eq!(cfg.opt_i64(&["sink", "missing"]), None);
    }

    #[test]
    fn test_relay_config_defaults() {
        let cfg = RelayConfig::default();
        assert_eq!(cfg.metrics_addr, "0.0.0.0:9090");
        assert_eq!(cfg.log_level, "info");
        assert_eq!(cfg.default_batch_size, 100);
    }

    #[test]
    fn test_relay_config_toml_roundtrip() {
        let cfg = RelayConfig {
            postgres_url: "postgres://localhost/test".to_string(),
            metrics_addr: "127.0.0.1:9091".to_string(),
            log_format: LogFormat::Json,
            log_level: "debug".to_string(),
            discovery_interval_secs: 60,
            default_batch_size: 200,
            relay_group_id: "prod-cluster-1".to_string(),
            sink_max_inflight: 500,
            max_owned_pipelines: 30,
            max_connections: 32,
            tenant_id: None,
            config_mode: ConfigMode::TomlAllowed,
        };
        let toml_str = toml::to_string(&cfg).unwrap();
        let decoded: RelayConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(decoded.postgres_url, cfg.postgres_url);
        assert_eq!(decoded.relay_group_id, cfg.relay_group_id);
        assert_eq!(decoded.sink_max_inflight, 500);
    }

    // ── A30: ENV variable expansion ───────────────────────────────────────

    #[test]
    fn test_expand_env_vars_no_placeholders() {
        let s = "postgres://localhost/mydb";
        assert_eq!(RelayConfig::expand_env_vars(s), s);
    }

    #[test]
    fn test_expand_env_vars_known_var() {
        // SAFETY: test-only; single-threaded cargo test with no parallel access.
        unsafe { std::env::set_var("PGTRICKLE_TEST_CONN_VAR", "secret_password") };
        let s = "postgres://user:${ENV:PGTRICKLE_TEST_CONN_VAR}@localhost/db";
        let result = RelayConfig::expand_env_vars(s);
        assert_eq!(result, "postgres://user:secret_password@localhost/db");
        // SAFETY: same as above.
        unsafe { std::env::remove_var("PGTRICKLE_TEST_CONN_VAR") };
    }

    #[test]
    fn test_expand_env_vars_unknown_var_preserved() {
        // Unknown variable placeholder must be left intact so callers can detect
        // mis-configuration rather than silently passing an empty password.
        // SAFETY: test-only; single-threaded cargo test.
        unsafe { std::env::remove_var("PGTRICKLE_DEFINITELY_NOT_SET_9XQ") };
        let s = "postgres://${ENV:PGTRICKLE_DEFINITELY_NOT_SET_9XQ}@host/db";
        let result = RelayConfig::expand_env_vars(s);
        assert_eq!(result, s, "unknown var placeholder should be preserved");
    }

    #[test]
    fn test_expand_env_vars_multiple_vars() {
        // SAFETY: test-only; single-threaded cargo test.
        unsafe {
            std::env::set_var("PGTRICKLE_TEST_USER", "alice");
            std::env::set_var("PGTRICKLE_TEST_DB", "analytics");
        }
        let s = "postgres://${ENV:PGTRICKLE_TEST_USER}@host/${ENV:PGTRICKLE_TEST_DB}";
        let result = RelayConfig::expand_env_vars(s);
        assert_eq!(result, "postgres://alice@host/analytics");
        // SAFETY: test-only; single-threaded cargo test.
        unsafe {
            std::env::remove_var("PGTRICKLE_TEST_USER");
            std::env::remove_var("PGTRICKLE_TEST_DB");
        }
    }

    #[test]
    fn test_expand_env_vars_malformed_unclosed() {
        // Malformed placeholder (no closing brace) is passed through verbatim.
        let s = "postgres://${ENV:UNCLOSED";
        let result = RelayConfig::expand_env_vars(s);
        assert_eq!(result, s);
    }
}
