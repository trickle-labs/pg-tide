//! Multi-Outbox Fan-In Pipeline API for pg_tide (v0.29.0).
//!
//! Provides Rust-backed helpers for fan-in pipeline management alongside
//! the PL/pgSQL `tide.relay_set_fanin()` SQL function.
//!
//! Fan-in pipelines combine messages from multiple outboxes into a single sink
//! using a configurable merge strategy (round_robin, priority, subject_hash).

use crate::error::PgTideError;
use pgrx::prelude::*;

// ── TIDE-API: relay_fanin_enable / relay_fanin_disable ────────────────────

/// Enable a fan-in pipeline (set enabled = TRUE).
#[pg_extern(schema = "tide")]
pub fn relay_fanin_enable(p_name: &str) {
    relay_fanin_set_enabled_impl(p_name, true).unwrap_or_else(|e| pgrx::error!("{}", e))
}

/// Disable a fan-in pipeline (set enabled = FALSE).
#[pg_extern(schema = "tide")]
pub fn relay_fanin_disable(p_name: &str) {
    relay_fanin_set_enabled_impl(p_name, false).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn relay_fanin_set_enabled_impl(name: &str, enabled: bool) -> Result<(), PgTideError> {
    crate::validation::validate_identifier(name)?;
    let updated: i64 = Spi::get_one_with_args::<i64>(
        "WITH u AS (UPDATE tide.relay_fanin_config SET enabled = $2, updated_at = now() \
         WHERE name = $1 RETURNING 1) SELECT COUNT(*)::bigint FROM u",
        &[name.into(), enabled.into()],
    )
    .unwrap_or(None)
    .unwrap_or(0);

    if updated == 0 {
        return Err(PgTideError::InvalidArgument(format!(
            "fan-in pipeline '{}' not found",
            name
        )));
    }
    pgrx::log!(
        "[pg_tide] relay_fanin_{}: '{name}'",
        if enabled { "enable" } else { "disable" }
    );
    Ok(())
}

// ── TIDE-API: relay_fanin_delete ──────────────────────────────────────────

/// Remove a fan-in pipeline from the catalog.
#[pg_extern(schema = "tide")]
pub fn relay_fanin_delete(p_name: &str) {
    relay_fanin_delete_impl(p_name).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn relay_fanin_delete_impl(name: &str) -> Result<(), PgTideError> {
    crate::validation::validate_identifier(name)?;
    let deleted: i64 = Spi::get_one_with_args::<i64>(
        "WITH d AS (DELETE FROM tide.relay_fanin_config WHERE name = $1 RETURNING 1) \
         SELECT COUNT(*)::bigint FROM d",
        &[name.into()],
    )
    .unwrap_or(None)
    .unwrap_or(0);

    if deleted == 0 {
        return Err(PgTideError::InvalidArgument(format!(
            "fan-in pipeline '{}' not found",
            name
        )));
    }
    pgrx::log!("[pg_tide] relay_fanin_delete: removed '{name}'");
    Ok(())
}

// ── TIDE-API: relay_fanin_list ────────────────────────────────────────────

/// Return all fan-in pipeline configurations as a JSON array.
#[pg_extern(schema = "tide")]
pub fn relay_fanin_list() -> pgrx::JsonB {
    relay_fanin_list_impl().unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn relay_fanin_list_impl() -> Result<pgrx::JsonB, PgTideError> {
    let result = Spi::get_one::<pgrx::JsonB>(
        "SELECT COALESCE( \
            (SELECT jsonb_agg(jsonb_build_object( \
                'name',           name, \
                'outbox_names',   to_jsonb(outbox_names), \
                'sink_type',      sink_type, \
                'merge_strategy', merge_strategy, \
                'enabled',        enabled, \
                'tenant_name',    tenant_name, \
                'updated_at',     updated_at \
             ) ORDER BY name) \
             FROM tide.relay_fanin_config), \
            '[]'::jsonb)",
    )
    .map_err(|e| PgTideError::SpiError(format!("relay_fanin_list SPI error: {e}")))?;

    Ok(result.unwrap_or_else(|| pgrx::JsonB(serde_json::json!([]))))
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

    #[pg_test]
    fn test_relay_fanin_list_empty() {
        let result = crate::fanin::relay_fanin_list();
        assert!(
            result.0.is_array(),
            "relay_fanin_list must return a JSON array"
        );
    }

    #[pg_test]
    fn test_relay_fanin_enable_disable() {
        setup_outbox("fanin-src-1");
        setup_outbox("fanin-src-2");

        // Insert via SQL (relay_set_fanin is PL/pgSQL)
        Spi::run_with_args(
            "SELECT tide.relay_set_fanin($1, ARRAY[$2, $3]::text[], $4)",
            &[
                "test-fanin-ed".into(),
                "fanin-src-1".into(),
                "fanin-src-2".into(),
                "stdout".into(),
            ],
        )
        .unwrap();

        crate::fanin::relay_fanin_disable("test-fanin-ed");
        let enabled: bool = Spi::get_one(
            "SELECT enabled FROM tide.relay_fanin_config WHERE name = 'test-fanin-ed'",
        )
        .unwrap()
        .unwrap_or(true);
        assert!(!enabled, "fan-in should be disabled");

        crate::fanin::relay_fanin_enable("test-fanin-ed");
        let enabled2: bool = Spi::get_one(
            "SELECT enabled FROM tide.relay_fanin_config WHERE name = 'test-fanin-ed'",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(enabled2, "fan-in should be re-enabled");
    }

    #[pg_test]
    fn test_relay_fanin_delete() {
        setup_outbox("fanin-del-src");
        Spi::run_with_args(
            "SELECT tide.relay_set_fanin($1, ARRAY[$2]::text[], $3)",
            &[
                "test-fanin-del".into(),
                "fanin-del-src".into(),
                "stdout".into(),
            ],
        )
        .unwrap();

        crate::fanin::relay_fanin_delete("test-fanin-del");
        let exists: bool = Spi::get_one(
            "SELECT EXISTS(SELECT 1 FROM tide.relay_fanin_config WHERE name = 'test-fanin-del')",
        )
        .unwrap()
        .unwrap_or(true);
        assert!(!exists, "fan-in should have been deleted");
    }

    #[pg_test]
    fn test_relay_fanin_enable_unknown_errors() {
        let result = crate::fanin::relay_fanin_set_enabled_impl("no-such-fanin", true);
        assert!(result.is_err(), "enabling nonexistent fan-in must fail");
    }
}
