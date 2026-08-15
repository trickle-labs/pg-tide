//! Relay Catalog API for pg_tide.
//!
//! Provides `tide.relay_set_outbox_v2()`, `tide.relay_set_inbox_v2()`,
//! `tide.relay_enable()`, `tide.relay_disable()`, `tide.relay_delete()`,
//! `tide.relay_list_configs()` in the `tide` schema.
//!
//! v0.36.0: The positional-parameter forms `relay_set_outbox()` (6 params)
//! and `relay_set_inbox()` (8 params) were removed as a breaking change.
//! Use `relay_set_outbox_v2(config JSONB)` and `relay_set_inbox_v2(config JSONB)`.
//!
//! The relay catalog stores pipeline configurations that the `pg-tide` binary
//! reads to set up source/sink connections.

use crate::error::PgTideError;
use pgrx::prelude::*;

fn relay_exists(name: &str) -> Result<bool, PgTideError> {
    let in_outbox = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM tide.relay_outbox_config WHERE name = $1)",
        &[name.into()],
    )
    .map(|r| r.unwrap_or(false))
    .map_err(|e| PgTideError::SpiError(format!("relay_exists SPI error: {e}")))?;
    let in_inbox = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM tide.relay_inbox_config WHERE name = $1)",
        &[name.into()],
    )
    .map(|r| r.unwrap_or(false))
    .map_err(|e| PgTideError::SpiError(format!("relay_exists SPI error: {e}")))?;
    Ok(in_outbox || in_inbox)
}

// ── TIDE-API: relay_set_outbox_v2 (v0.18.0) ──────────────────────────────

/// Configure a forward relay pipeline using a single JSONB config parameter.
///
/// v0.36.0: This is the only remaining form of `relay_set_outbox()`; the
/// 6-positional-parameter form was removed in v0.36.0.
///
/// v0.40.0 (ADR-011): Defaults to the **native** shared-table source. The named
/// outbox must already exist. pg_trickle compatibility is selected explicitly
/// via `source_mode`, never inferred from storage.
///
/// The config object accepts the following keys:
///
/// - `name`        TEXT  (required) Pipeline name.
/// - `outbox`      TEXT  (required) Source outbox name (must exist).
/// - `sink_type`   TEXT  (required) Sink backend type (e.g. `"nats"`, `"stdout"`).
/// - `config`      JSONB (default: `{}`) Sink-specific configuration.
/// - `source_mode` TEXT  (default: `"native"`) `"native"` or `"pg_trickle"`.
/// - `batch_size`  INT   (default: 100)
/// - `enabled`     BOOL  (default: true)
/// - `wire_format` TEXT  (default: `"native"`)
#[pg_extern(schema = "tide")]
pub fn relay_set_outbox_v2(p_config: pgrx::JsonB) {
    relay_set_outbox_v2_impl(p_config).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn relay_set_outbox_v2_impl(config: pgrx::JsonB) -> Result<(), PgTideError> {
    let obj = &config.0;

    let name = obj["name"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            PgTideError::InvalidArgument(
                r#"relay_set_outbox_v2: config must include a non-empty "name" key"#.to_string(),
            )
        })?;
    let outbox = obj["outbox"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            PgTideError::InvalidArgument(
                r#"relay_set_outbox_v2: config must include a non-empty "outbox" key"#.to_string(),
            )
        })?;
    let sink_type = obj["sink_type"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            PgTideError::InvalidArgument(
                r#"relay_set_outbox_v2: config must include a non-empty "sink_type" key"#
                    .to_string(),
            )
        })?;

    // v0.40.0 (ADR-011 §10): Native by default; pg_trickle is an explicit mode.
    let source_type = match obj.get("source_mode").and_then(|v| v.as_str()) {
        None | Some("native") => "outbox",
        Some("pg_trickle") => "pg_trickle_outbox",
        Some(other) => {
            return Err(PgTideError::InvalidArgument(format!(
                "relay_set_outbox_v2: unsupported source_mode '{other}' \
                 (expected 'native' or 'pg_trickle')"
            )))
        }
    };

    let sink_config = obj
        .get("config")
        .cloned()
        .unwrap_or(serde_json::Value::Object(Default::default()));
    let batch_size = obj["batch_size"].as_i64().unwrap_or(100) as i32;
    let enabled = obj["enabled"].as_bool().unwrap_or(true);

    crate::validation::validate_identifier(name)?;
    crate::validation::validate_identifier(outbox)?;

    // v0.40.0: Validate the named outbox exists before mutating the catalog.
    if !crate::outbox::outbox_exists(outbox)? {
        return Err(PgTideError::OutboxNotFound(outbox.to_string()));
    }

    let full_config = serde_json::json!({
        "source_type": source_type,
        "source": { "outbox": outbox },
        "sink_type": sink_type,
        "sink": sink_config,
        "batch_size": batch_size,
    });
    let full_str = serde_json::to_string(&full_config)
        .map_err(|e| PgTideError::SpiError(format!("serialize config: {e}")))?;

    Spi::run_with_args(
        "INSERT INTO tide.relay_outbox_config (name, enabled, config) \
         VALUES ($1, $2, $3::jsonb) \
         ON CONFLICT (name) DO UPDATE \
         SET enabled = EXCLUDED.enabled, config = EXCLUDED.config",
        &[name.into(), enabled.into(), full_str.as_str().into()],
    )
    .map_err(|e| PgTideError::SpiError(format!("UPSERT relay_outbox_config: {e}")))?;

    let _ = Spi::run_with_args("SELECT pg_notify('tide_relay_config', $1)", &[name.into()]);

    Ok(())
}

// ── TIDE-API: relay_set_inbox_v2 (v0.16.0) ───────────────────────────────

/// Configure a reverse relay pipeline using a single JSONB config parameter.
///
/// v0.36.0: This is the only remaining form of `relay_set_inbox()`; the
/// 8-positional-parameter form was removed in v0.36.0.
/// The config object accepts the following keys:
///
/// - `name`        TEXT  (required) Pipeline name.
/// - `inbox`       TEXT  (required) Target inbox name.
/// - `source`      TEXT  (default: `"stdout"`) Source backend type.
/// - `config`      JSONB (default: `{}`) Source-specific configuration.
/// - `batch_size`  INT   (default: 100)
/// - `enabled`     BOOL  (default: true)
/// - `max_retries` INT   (default: 3)
/// - `idempotent`  BOOL  (default: true)
#[pg_extern(schema = "tide")]
pub fn relay_set_inbox_v2(p_config: pgrx::JsonB) {
    relay_set_inbox_v2_impl(p_config).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn relay_set_inbox_v2_impl(config: pgrx::JsonB) -> Result<(), PgTideError> {
    let obj = &config.0;

    let name = obj["name"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            PgTideError::InvalidArgument(
                r#"relay_set_inbox_v2: config must include a non-empty "name" key"#.to_string(),
            )
        })?;
    let inbox = obj["inbox"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            PgTideError::InvalidArgument(
                r#"relay_set_inbox_v2: config must include a non-empty "inbox" key"#.to_string(),
            )
        })?;

    let source = obj["source"].as_str().unwrap_or("stdout");
    let src_config = obj
        .get("config")
        .cloned()
        .unwrap_or(serde_json::Value::Object(Default::default()));
    let batch_size = obj["batch_size"].as_i64().unwrap_or(100) as i32;
    let enabled = obj["enabled"].as_bool().unwrap_or(true);
    let max_retries = obj["max_retries"].as_i64().unwrap_or(3) as i32;
    let idempotent = obj["idempotent"].as_bool().unwrap_or(true);

    crate::validation::validate_identifier(name)?;
    crate::validation::validate_identifier(inbox)?;

    let full_config = serde_json::json!({
        "source_type": source,
        "source": src_config,
        "sink_type": "inbox",
        "sink": {
            "inbox": inbox,
            "max_retries": max_retries,
            "idempotent": idempotent,
        },
        "batch_size": batch_size,
    });
    let full_str = serde_json::to_string(&full_config)
        .map_err(|e| PgTideError::SpiError(format!("serialize config: {e}")))?;

    Spi::run_with_args(
        "INSERT INTO tide.relay_inbox_config (name, enabled, config) \
         VALUES ($1, $2, $3::jsonb) \
         ON CONFLICT (name) DO UPDATE \
         SET enabled = EXCLUDED.enabled, config = EXCLUDED.config",
        &[name.into(), enabled.into(), full_str.as_str().into()],
    )
    .map_err(|e| PgTideError::SpiError(format!("UPSERT relay_inbox_config: {e}")))?;

    let _ = Spi::run_with_args("SELECT pg_notify('tide_relay_config', $1)", &[name.into()]);

    Ok(())
}

// ── TIDE-API: relay_enable / relay_disable / relay_delete ─────────────────

/// Enable a relay pipeline.
///
/// Returns `TRUE` if a row was modified, `FALSE` if the pipeline did not exist.
/// Sends a `pg_notify('tide_relay_config')` to wake up any listening relay instances.
#[pg_extern(schema = "tide")]
pub fn relay_enable(p_name: &str) -> bool {
    relay_enable_impl(p_name).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn relay_enable_impl(name: &str) -> Result<bool, PgTideError> {
    if !relay_exists(name)? {
        return Ok(false); // pipeline not found — return false, no-op
    }
    let _ = Spi::run_with_args(
        "UPDATE tide.relay_outbox_config SET enabled = true WHERE name = $1",
        &[name.into()],
    );
    let _ = Spi::run_with_args(
        "UPDATE tide.relay_inbox_config SET enabled = true WHERE name = $1",
        &[name.into()],
    );
    let _ = Spi::run_with_args("SELECT pg_notify('tide_relay_config', $1)", &[name.into()]);
    Ok(true)
}

/// Disable a relay pipeline.
///
/// Returns `TRUE` if a row was modified, `FALSE` if the pipeline did not exist.
/// Sends a `pg_notify('tide_relay_config')` to wake up any listening relay instances.
#[pg_extern(schema = "tide")]
pub fn relay_disable(p_name: &str) -> bool {
    relay_disable_impl(p_name).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn relay_disable_impl(name: &str) -> Result<bool, PgTideError> {
    if !relay_exists(name)? {
        return Ok(false); // pipeline not found — return false, no-op
    }
    let _ = Spi::run_with_args(
        "UPDATE tide.relay_outbox_config SET enabled = false WHERE name = $1",
        &[name.into()],
    );
    let _ = Spi::run_with_args(
        "UPDATE tide.relay_inbox_config SET enabled = false WHERE name = $1",
        &[name.into()],
    );
    let _ = Spi::run_with_args("SELECT pg_notify('tide_relay_config', $1)", &[name.into()]);
    Ok(true)
}

/// Delete a relay pipeline.
#[pg_extern(schema = "tide")]
pub fn relay_delete(p_name: &str) {
    relay_delete_impl(p_name).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn relay_delete_impl(name: &str) -> Result<(), PgTideError> {
    if !relay_exists(name)? {
        return Err(PgTideError::RelayNotFound(name.to_string()));
    }
    let _ = Spi::run_with_args(
        "DELETE FROM tide.relay_outbox_config WHERE name = $1",
        &[name.into()],
    );
    let _ = Spi::run_with_args(
        "DELETE FROM tide.relay_inbox_config WHERE name = $1",
        &[name.into()],
    );
    Ok(())
}

// ── TIDE-API: relay_get_config / relay_list_configs ───────────────────────

/// Get the configuration for a single relay pipeline.
#[pg_extern(schema = "tide")]
pub fn relay_get_config(p_name: &str) -> pgrx::JsonB {
    relay_get_config_impl(p_name).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn relay_get_config_impl(name: &str) -> Result<pgrx::JsonB, PgTideError> {
    // Check outbox config first, then inbox.
    let config: Option<pgrx::JsonB> = Spi::get_one_with_args::<pgrx::JsonB>(
        "SELECT config FROM tide.relay_outbox_config WHERE name = $1",
        &[name.into()],
    )
    .unwrap_or(None);

    if let Some(c) = config {
        return Ok(c);
    }

    let config2: Option<pgrx::JsonB> = Spi::get_one_with_args::<pgrx::JsonB>(
        "SELECT config FROM tide.relay_inbox_config WHERE name = $1",
        &[name.into()],
    )
    .unwrap_or(None);

    config2.ok_or_else(|| PgTideError::RelayNotFound(name.to_string()))
}

/// List all relay pipeline configurations.
#[pg_extern(schema = "tide")]
pub fn relay_list_configs() -> pgrx::JsonB {
    relay_list_configs_impl().unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn relay_list_configs_impl() -> Result<pgrx::JsonB, PgTideError> {
    let outbox_rows = Spi::connect(|client| {
        let mut rows = Vec::new();
        let tup = client.select(
            "SELECT name, enabled, config FROM tide.relay_outbox_config ORDER BY name",
            None,
            &[],
        )?;
        for row in tup {
            let name: String = row.get(1)?.unwrap_or_default();
            let enabled: bool = row.get(2)?.unwrap_or(true);
            let config: serde_json::Value = row
                .get::<pgrx::JsonB>(3)?
                .map(|j| j.0)
                .unwrap_or(serde_json::Value::Object(Default::default()));
            rows.push(serde_json::json!({
                "name": name,
                "direction": "outbox",
                "enabled": enabled,
                "config": config,
            }));
        }
        Ok::<_, pgrx::spi::SpiError>(rows)
    })
    .map_err(|e| PgTideError::SpiError(format!("list outbox configs: {e}")))?;

    let inbox_rows = Spi::connect(|client| {
        let mut rows = Vec::new();
        let tup = client.select(
            "SELECT name, enabled, config FROM tide.relay_inbox_config ORDER BY name",
            None,
            &[],
        )?;
        for row in tup {
            let name: String = row.get(1)?.unwrap_or_default();
            let enabled: bool = row.get(2)?.unwrap_or(true);
            let config: serde_json::Value = row
                .get::<pgrx::JsonB>(3)?
                .map(|j| j.0)
                .unwrap_or(serde_json::Value::Object(Default::default()));
            rows.push(serde_json::json!({
                "name": name,
                "direction": "inbox",
                "enabled": enabled,
                "config": config,
            }));
        }
        Ok::<_, pgrx::spi::SpiError>(rows)
    })
    .map_err(|e| PgTideError::SpiError(format!("list inbox configs: {e}")))?;

    let all: Vec<_> = outbox_rows.into_iter().chain(inbox_rows).collect();
    Ok(pgrx::JsonB(serde_json::Value::Array(all)))
}

// ── TIDE-API: relay_set_tenant / relay_grant_tenant / relay_revoke_tenant ─

/// Assign a relay pipeline to a named tenant.
#[pg_extern(schema = "tide")]
pub fn relay_set_tenant(p_name: &str, p_tenant: &str) {
    relay_set_tenant_impl(p_name, p_tenant).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn relay_set_tenant_impl(name: &str, tenant: &str) -> Result<(), PgTideError> {
    crate::validation::validate_identifier(name)?;
    if !relay_exists(name)? {
        return Err(PgTideError::RelayNotFound(name.to_string()));
    }
    let _ = Spi::run_with_args(
        "UPDATE tide.relay_outbox_config SET tenant_name = $2 WHERE name = $1",
        &[name.into(), tenant.into()],
    );
    let _ = Spi::run_with_args(
        "UPDATE tide.relay_inbox_config SET tenant_name = $2 WHERE name = $1",
        &[name.into(), tenant.into()],
    );
    let _ = Spi::run_with_args(
        "UPDATE tide.relay_consumer_offsets SET tenant_name = $2 WHERE pipeline_id = $1",
        &[name.into(), tenant.into()],
    );
    Ok(())
}

/// Grant a role access to all pipelines in a named tenant.
#[pg_extern(schema = "tide")]
pub fn relay_grant_tenant(p_tenant: &str, p_role: &str) {
    relay_grant_tenant_impl(p_tenant, p_role).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn relay_grant_tenant_impl(tenant: &str, role: &str) -> Result<(), PgTideError> {
    Spi::run_with_args(
        "INSERT INTO tide.relay_tenant_grants (tenant_name, role_name)
         VALUES ($1, $2) ON CONFLICT DO NOTHING",
        &[tenant.into(), role.into()],
    )
    .map_err(|e| PgTideError::SpiError(format!("relay_grant_tenant: {e}")))?;
    Ok(())
}

/// Revoke a role's access to a tenant.
#[pg_extern(schema = "tide")]
pub fn relay_revoke_tenant(p_tenant: &str, p_role: &str) {
    relay_revoke_tenant_impl(p_tenant, p_role).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn relay_revoke_tenant_impl(tenant: &str, role: &str) -> Result<(), PgTideError> {
    Spi::run_with_args(
        "DELETE FROM tide.relay_tenant_grants WHERE tenant_name = $1 AND role_name = $2",
        &[tenant.into(), role.into()],
    )
    .map_err(|e| PgTideError::SpiError(format!("relay_revoke_tenant: {e}")))?;
    Ok(())
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    fn setup_outbox(name: &str) {
        let exists: bool = Spi::get_one_with_args::<bool>(
            "SELECT EXISTS(SELECT 1 FROM tide.tide_outbox_config WHERE outbox_name = $1)",
            &[name.into()],
        )
        .unwrap()
        .unwrap_or(false);
        if !exists {
            crate::outbox::outbox_create(name, 24, 10_000, "none");
        }
    }

    // ── relay_set_outbox_v2 / relay_set_inbox_v2 ───────────────────────────
    // v0.36.0: positional forms removed; all tests use v2 (JSONB) API.

    #[pg_test]
    fn test_relay_set_outbox_creates_config() {
        setup_outbox("relay-src-outbox");
        crate::relay::relay_set_outbox_v2(pgrx::JsonB(serde_json::json!({
            "name": "my-pipeline",
            "outbox": "relay-src-outbox",
            "sink_type": "nats",
            "config": {"url": "nats://localhost:4222"},
            "batch_size": 100,
            "enabled": true,
        })));
        let exists: bool = Spi::get_one(
            "SELECT EXISTS(SELECT 1 FROM tide.relay_outbox_config WHERE name = 'my-pipeline')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(exists, "relay_set_outbox_v2 must create a config row");
    }

    #[pg_test]
    fn test_relay_set_outbox_v2_defaults_to_native() {
        setup_outbox("v2-native-outbox");
        crate::relay::relay_set_outbox_v2(pgrx::JsonB(serde_json::json!({
            "name": "v2-native-pipeline",
            "outbox": "v2-native-outbox",
            "sink_type": "stdout",
        })));
        let source_type: String = Spi::get_one(
            "SELECT config ->> 'source_type' FROM tide.relay_outbox_config \
             WHERE name = 'v2-native-pipeline'",
        )
        .unwrap()
        .unwrap_or_default();
        assert_eq!(source_type, "outbox", "default source_mode must be native");
    }

    #[pg_test]
    fn test_relay_set_outbox_v2_pg_trickle_mode() {
        setup_outbox("v2-compat-outbox");
        crate::relay::relay_set_outbox_v2(pgrx::JsonB(serde_json::json!({
            "name": "v2-compat-pipeline",
            "outbox": "v2-compat-outbox",
            "sink_type": "stdout",
            "source_mode": "pg_trickle",
        })));
        let source_type: String = Spi::get_one(
            "SELECT config ->> 'source_type' FROM tide.relay_outbox_config \
             WHERE name = 'v2-compat-pipeline'",
        )
        .unwrap()
        .unwrap_or_default();
        assert_eq!(
            source_type, "pg_trickle_outbox",
            "explicit pg_trickle mode must select the compatibility source"
        );
    }

    #[pg_test]
    fn test_relay_set_outbox_v2_unknown_outbox_fails_before_mutation() {
        let result = crate::relay::relay_set_outbox_v2_impl(pgrx::JsonB(serde_json::json!({
            "name": "v2-orphan-pipeline",
            "outbox": "no-such-outbox",
            "sink_type": "stdout",
        })));
        assert!(result.is_err(), "unknown outbox must fail");
        // No config row may have been written.
        let exists: bool = Spi::get_one(
            "SELECT EXISTS(SELECT 1 FROM tide.relay_outbox_config WHERE name = 'v2-orphan-pipeline')",
        )
        .unwrap()
        .unwrap_or(true);
        assert!(
            !exists,
            "no config row may be written for an unknown outbox"
        );
    }

    #[pg_test]
    fn test_relay_set_outbox_v2_unsupported_mode_fails() {
        setup_outbox("v2-badmode-outbox");
        let result = crate::relay::relay_set_outbox_v2_impl(pgrx::JsonB(serde_json::json!({
            "name": "v2-badmode-pipeline",
            "outbox": "v2-badmode-outbox",
            "sink_type": "stdout",
            "source_mode": "bogus",
        })));
        assert!(
            matches!(result, Err(crate::error::PgTideError::InvalidArgument(_))),
            "unsupported source_mode must fail with InvalidArgument"
        );
    }

    #[pg_test]
    fn test_relay_set_inbox_creates_config() {
        crate::inbox::inbox_create("relay-dst-inbox", "tide", 3, 72, 0);
        crate::relay::relay_set_inbox_v2(pgrx::JsonB(serde_json::json!({
            "name": "my-reverse-pipeline",
            "inbox": "relay-dst-inbox",
            "source": "nats",
            "config": {"url": "nats://localhost:4222"},
            "batch_size": 100,
            "enabled": true,
            "max_retries": 3,
            "idempotent": true,
        })));
        let exists: bool = Spi::get_one(
            "SELECT EXISTS(SELECT 1 FROM tide.relay_inbox_config WHERE name = 'my-reverse-pipeline')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(exists, "relay_set_inbox_v2 must create a config row");
    }

    // ── relay_enable / relay_disable ───────────────────────────────────────

    #[pg_test]
    fn test_relay_enable_disable_roundtrip() {
        setup_outbox("toggle-relay-outbox");
        crate::relay::relay_set_outbox_v2(pgrx::JsonB(serde_json::json!({
            "name": "toggle-pipeline",
            "outbox": "toggle-relay-outbox",
            "sink_type": "stdout",
        })));

        crate::relay::relay_disable("toggle-pipeline");
        let enabled: bool = Spi::get_one(
            "SELECT enabled FROM tide.relay_outbox_config WHERE name = 'toggle-pipeline'",
        )
        .unwrap()
        .unwrap_or(true);
        assert!(!enabled, "pipeline should be disabled");

        crate::relay::relay_enable("toggle-pipeline");
        let enabled: bool = Spi::get_one(
            "SELECT enabled FROM tide.relay_outbox_config WHERE name = 'toggle-pipeline'",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(enabled, "pipeline should be re-enabled");
    }

    // ── relay_delete ───────────────────────────────────────────────────────

    #[pg_test]
    fn test_relay_delete_removes_config() {
        setup_outbox("del-relay-outbox");
        crate::relay::relay_set_outbox_v2(pgrx::JsonB(serde_json::json!({
            "name": "delete-me-pipeline",
            "outbox": "del-relay-outbox",
            "sink_type": "stdout",
        })));
        crate::relay::relay_delete("delete-me-pipeline");
        let exists: bool = Spi::get_one(
            "SELECT EXISTS(SELECT 1 FROM tide.relay_outbox_config WHERE name = 'delete-me-pipeline')",
        )
        .unwrap()
        .unwrap_or(true);
        assert!(!exists, "relay_delete must remove the config row");
    }

    // ── relay_get_config / relay_list_configs ─────────────────────────────

    #[pg_test]
    fn test_relay_get_config_returns_json() {
        setup_outbox("cfg-relay-outbox");
        crate::relay::relay_set_outbox_v2(pgrx::JsonB(serde_json::json!({
            "name": "cfg-pipeline",
            "outbox": "cfg-relay-outbox",
            "sink_type": "stdout",
            "config": {"key": "value"},
            "batch_size": 50,
        })));
        let cfg = crate::relay::relay_get_config("cfg-pipeline");
        assert_eq!(cfg.0["source_type"], "outbox");
        assert_eq!(cfg.0["source"]["outbox"], "cfg-relay-outbox");
        assert_eq!(cfg.0["sink_type"], "stdout");
    }

    #[pg_test]
    fn test_relay_list_configs_includes_pipeline() {
        setup_outbox("list-relay-outbox");
        crate::relay::relay_set_outbox_v2(pgrx::JsonB(serde_json::json!({
            "name": "list-pipeline",
            "outbox": "list-relay-outbox",
            "sink_type": "kafka",
        })));
        let list = crate::relay::relay_list_configs();
        let arr = list.0.as_array().expect("must be array");
        let found = arr.iter().any(|v| v["name"] == "list-pipeline");
        assert!(found, "relay_list_configs must include 'list-pipeline'");
    }

    // ── error paths ────────────────────────────────────────────────────────

    #[pg_test]
    fn test_relay_enable_unknown_pipeline_is_safe() {
        // Enabling a non-existent pipeline with our implementation is a no-op
        // (no rows updated). Verify the function does not panic.
        crate::relay::relay_enable("does-not-exist");
    }

    // ── relay_set_tenant / relay_grant_tenant ──────────────────────────────

    #[pg_test]
    fn test_relay_set_tenant_updates_config() {
        setup_outbox("tenant-relay-outbox");
        crate::relay::relay_set_outbox_v2(pgrx::JsonB(serde_json::json!({
            "name": "tenant-pipeline",
            "outbox": "tenant-relay-outbox",
            "sink_type": "stdout",
        })));
        crate::relay::relay_set_tenant("tenant-pipeline", "acme");
        let tenant: String = Spi::get_one(
            "SELECT tenant_name FROM tide.relay_outbox_config WHERE name = 'tenant-pipeline'",
        )
        .unwrap()
        .unwrap_or_default();
        assert_eq!(
            tenant, "acme",
            "relay_set_tenant must update the tenant_name column"
        );
    }

    #[pg_test]
    fn test_relay_grant_tenant_inserts_row() {
        crate::relay::relay_grant_tenant("acme", "acme_relay_role");
        let exists: bool = Spi::get_one(
            "SELECT EXISTS(SELECT 1 FROM tide.relay_tenant_grants
              WHERE tenant_name = 'acme' AND role_name = 'acme_relay_role')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(
            exists,
            "relay_grant_tenant must insert a row in relay_tenant_grants"
        );
    }

    #[pg_test]
    fn test_relay_revoke_tenant_removes_row() {
        crate::relay::relay_grant_tenant("revoke-tenant", "revoke_role");
        crate::relay::relay_revoke_tenant("revoke-tenant", "revoke_role");
        let exists: bool = Spi::get_one(
            "SELECT EXISTS(SELECT 1 FROM tide.relay_tenant_grants
              WHERE tenant_name = 'revoke-tenant' AND role_name = 'revoke_role')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(!exists, "relay_revoke_tenant must remove the row");
    }
}
