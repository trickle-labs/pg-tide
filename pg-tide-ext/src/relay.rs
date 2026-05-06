//! Relay Catalog API for pg_tide.
//!
//! Provides `tide.relay_set_outbox()`, `tide.relay_set_inbox()`,
//! `tide.relay_enable()`, `tide.relay_disable()`, `tide.relay_delete()`,
//! `tide.relay_list_configs()` in the `tide` schema.
//!
//! The relay catalog stores pipeline configurations that the `pg-tide` binary
//! reads to set up source/sink connections.

use crate::error::PgTideError;
use pgrx::prelude::*;

fn relay_exists(name: &str) -> bool {
    let in_outbox = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM tide.relay_outbox_config WHERE name = $1)",
        &[name.into()],
    )
    .unwrap_or(None)
    .unwrap_or(false);
    let in_inbox = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM tide.relay_inbox_config WHERE name = $1)",
        &[name.into()],
    )
    .unwrap_or(None)
    .unwrap_or(false);
    in_outbox || in_inbox
}

// ── TIDE-API: relay_set_outbox ────────────────────────────────────────────

/// Configure a forward relay pipeline (outbox → external sink).
#[pg_extern(schema = "tide")]
pub fn relay_set_outbox(
    p_name: &str,
    p_outbox: &str,
    p_sink: &str,
    p_config: default!(pgrx::JsonB, "'{}'::jsonb"),
    p_batch_size: default!(i32, 100),
    p_enabled: default!(bool, true),
) {
    relay_set_outbox_impl(p_name, p_outbox, p_sink, p_config, p_batch_size, p_enabled)
        .unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn relay_set_outbox_impl(
    name: &str,
    outbox: &str,
    sink: &str,
    config: pgrx::JsonB,
    batch_size: i32,
    enabled: bool,
) -> Result<(), PgTideError> {
    crate::validation::validate_identifier(name)?;
    crate::validation::validate_identifier(outbox)?;
    // Build combined config matching the relay runtime's expected shape:
    // source_type / source.outbox + sink_type / sink.* + batch_size.
    let full_config = serde_json::json!({
        "source_type": "outbox",
        "source": { "outbox": outbox },
        "sink_type": sink,
        "sink": config.0,
        "batch_size": batch_size,
    });
    let full_str = serde_json::to_string(&full_config)
        .map_err(|e| PgTideError::SpiError(format!("serialize full config: {e}")))?;

    Spi::run_with_args(
        "INSERT INTO tide.relay_outbox_config (name, enabled, config) \
         VALUES ($1, $2, $3::jsonb) \
         ON CONFLICT (name) DO UPDATE \
         SET enabled = EXCLUDED.enabled, config = EXCLUDED.config",
        &[name.into(), enabled.into(), full_str.as_str().into()],
    )
    .map_err(|e| PgTideError::SpiError(format!("UPSERT relay_outbox_config: {e}")))?;

    // Notify relay binary.
    let _ = Spi::run_with_args("SELECT pg_notify('tide_relay_config', $1)", &[name.into()]);

    Ok(())
}

// ── TIDE-API: relay_set_inbox ─────────────────────────────────────────────

/// Configure a reverse relay pipeline (external source → inbox).
#[pg_extern(schema = "tide")]
#[allow(clippy::too_many_arguments)]
pub fn relay_set_inbox(
    p_name: &str,
    p_inbox: &str,
    p_config: default!(pgrx::JsonB, "'{}'::jsonb"),
    p_batch_size: default!(i32, 100),
    p_source: default!(&str, "'stdout'"),
    p_enabled: default!(bool, true),
    p_max_retries: default!(i32, 3),
    p_idempotent: default!(bool, true),
) {
    relay_set_inbox_impl(
        p_name,
        p_inbox,
        p_config,
        p_batch_size,
        p_source,
        p_enabled,
        p_max_retries,
        p_idempotent,
    )
    .unwrap_or_else(|e| pgrx::error!("{}", e))
}

#[allow(clippy::too_many_arguments)]
fn relay_set_inbox_impl(
    name: &str,
    inbox: &str,
    config: pgrx::JsonB,
    batch_size: i32,
    source: &str,
    enabled: bool,
    max_retries: i32,
    idempotent: bool,
) -> Result<(), PgTideError> {
    crate::validation::validate_identifier(name)?;
    crate::validation::validate_identifier(inbox)?;
    // Build config matching the relay runtime's expected shape for reverse pipelines:
    // source_type / source.* + sink_type=inbox / sink.inbox + batch_size.
    let full_config = serde_json::json!({
        "source_type": source,
        "source": config.0,
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
#[pg_extern(schema = "tide")]
pub fn relay_enable(p_name: &str) {
    relay_enable_impl(p_name).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn relay_enable_impl(name: &str) -> Result<(), PgTideError> {
    if !relay_exists(name) {
        return Ok(()); // no-op — pipeline may have been deleted concurrently
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
    Ok(())
}

/// Disable a relay pipeline.
#[pg_extern(schema = "tide")]
pub fn relay_disable(p_name: &str) {
    relay_disable_impl(p_name).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn relay_disable_impl(name: &str) -> Result<(), PgTideError> {
    if !relay_exists(name) {
        return Ok(()); // no-op — pipeline may have been deleted concurrently
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
    Ok(())
}

/// Delete a relay pipeline.
#[pg_extern(schema = "tide")]
pub fn relay_delete(p_name: &str) {
    relay_delete_impl(p_name).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn relay_delete_impl(name: &str) -> Result<(), PgTideError> {
    if !relay_exists(name) {
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
    if !relay_exists(name) {
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
            crate::outbox::outbox_create(name, 24, 10_000);
        }
    }

    // ── relay_set_outbox / relay_set_inbox ─────────────────────────────────

    #[pg_test]
    fn test_relay_set_outbox_creates_config() {
        setup_outbox("relay-src-outbox");
        crate::relay::relay_set_outbox(
            "my-pipeline",
            "relay-src-outbox",
            "nats",
            pgrx::JsonB(serde_json::json!({"url": "nats://localhost:4222"})),
            100,
            true,
        );
        let exists: bool = Spi::get_one(
            "SELECT EXISTS(SELECT 1 FROM tide.relay_outbox_config WHERE name = 'my-pipeline')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(exists, "relay_set_outbox must create a config row");
    }

    #[pg_test]
    fn test_relay_set_inbox_creates_config() {
        crate::inbox::inbox_create("relay-dst-inbox", "tide", 3, 72, 0);
        crate::relay::relay_set_inbox(
            "my-reverse-pipeline",
            "relay-dst-inbox",
            pgrx::JsonB(serde_json::json!({"url": "nats://localhost:4222"})),
            100,
            "nats",
            true,
            3,
            true,
        );
        let exists: bool = Spi::get_one(
            "SELECT EXISTS(SELECT 1 FROM tide.relay_inbox_config WHERE name = 'my-reverse-pipeline')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(exists, "relay_set_inbox must create a config row");
    }

    // ── relay_enable / relay_disable ───────────────────────────────────────

    #[pg_test]
    fn test_relay_enable_disable_roundtrip() {
        setup_outbox("toggle-relay-outbox");
        crate::relay::relay_set_outbox(
            "toggle-pipeline",
            "toggle-relay-outbox",
            "stdout",
            pgrx::JsonB(serde_json::json!({})),
            100,
            true,
        );

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
        crate::relay::relay_set_outbox(
            "delete-me-pipeline",
            "del-relay-outbox",
            "stdout",
            pgrx::JsonB(serde_json::json!({})),
            100,
            true,
        );
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
        crate::relay::relay_set_outbox(
            "cfg-pipeline",
            "cfg-relay-outbox",
            "stdout",
            pgrx::JsonB(serde_json::json!({"key": "value"})),
            50,
            true,
        );
        let cfg = crate::relay::relay_get_config("cfg-pipeline");
        assert_eq!(cfg.0["source_type"], "outbox");
        assert_eq!(cfg.0["source"]["outbox"], "cfg-relay-outbox");
        assert_eq!(cfg.0["sink_type"], "stdout");
    }

    #[pg_test]
    fn test_relay_list_configs_includes_pipeline() {
        setup_outbox("list-relay-outbox");
        crate::relay::relay_set_outbox(
            "list-pipeline",
            "list-relay-outbox",
            "kafka",
            pgrx::JsonB(serde_json::json!({})),
            100,
            true,
        );
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
        crate::relay::relay_set_outbox(
            "tenant-pipeline",
            "tenant-relay-outbox",
            "stdout",
            pgrx::JsonB(serde_json::json!({})),
            100,
            true,
        );
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
