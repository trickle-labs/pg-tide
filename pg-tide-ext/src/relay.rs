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
    let config_str = serde_json::to_string(&config.0)
        .map_err(|e| PgTideError::SpiError(format!("serialize config: {e}")))?;

    // Build combined config with outbox + sink + batch.
    let full_config = serde_json::json!({
        "outbox": outbox,
        "sink": sink,
        "batch_size": batch_size,
        "params": config.0,
    });
    let full_str = serde_json::to_string(&full_config)
        .map_err(|e| PgTideError::SpiError(format!("serialize full config: {e}")))?;
    let _ = config_str; // suppress unused

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
    let full_config = serde_json::json!({
        "inbox": inbox,
        "source": source,
        "batch_size": batch_size,
        "max_retries": max_retries,
        "idempotent": idempotent,
        "params": config.0,
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
        return Err(PgTideError::RelayNotFound(name.to_string()));
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
        return Err(PgTideError::RelayNotFound(name.to_string()));
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
            rows.push(serde_json::json!({
                "name": name,
                "direction": "outbox",
                "enabled": enabled,
            }));
        }
        Ok::<_, pgrx::spi::SpiError>(rows)
    })
    .unwrap_or_default();

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
            rows.push(serde_json::json!({
                "name": name,
                "direction": "inbox",
                "enabled": enabled,
            }));
        }
        Ok::<_, pgrx::spi::SpiError>(rows)
    })
    .unwrap_or_default();

    let all: Vec<_> = outbox_rows.into_iter().chain(inbox_rows).collect();
    pgrx::JsonB(serde_json::Value::Array(all))
}
