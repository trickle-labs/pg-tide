//! Pipeline Template Library API for pg_tide (v0.29.0).
//!
//! Provides `tide.relay_template_list()` and `tide.relay_template_get()`
//! Rust-backed helpers alongside the SQL-defined CRUD functions.
//!
//! The core CRUD (`relay_template_create`, `relay_template_drop`,
//! `relay_template_validate`, `relay_set_outbox_from_template`,
//! `relay_set_inbox_from_template`) are pure PL/pgSQL in the migration file.
//! This module exposes the Rust-backed list helper used by `pg-tide template list`.

use crate::error::PgTideError;
use pgrx::prelude::*;

// ── TIDE-API: relay_template_list ─────────────────────────────────────────

/// Return all registered pipeline templates as a JSON array.
///
/// Each entry includes `name`, `description`, and `required_keys`.
#[pg_extern(schema = "tide")]
pub fn relay_template_list() -> pgrx::JsonB {
    relay_template_list_impl().unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn relay_template_list_impl() -> Result<pgrx::JsonB, PgTideError> {
    let result = Spi::get_one::<pgrx::JsonB>(
        "SELECT COALESCE( \
            (SELECT jsonb_agg(jsonb_build_object( \
                'name',          name, \
                'description',   description, \
                'required_keys', required_keys, \
                'updated_at',    updated_at \
             ) ORDER BY name) \
             FROM tide.relay_pipeline_templates), \
            '[]'::jsonb)",
    )
    .map_err(|e| PgTideError::SpiError(format!("relay_template_list SPI error: {e}")))?;

    Ok(result.unwrap_or_else(|| pgrx::JsonB(serde_json::json!([]))))
}

// ── TIDE-API: relay_template_get ──────────────────────────────────────────

/// Return the full config JSON for a named template, or NULL if not found.
#[pg_extern(schema = "tide")]
pub fn relay_template_get(p_name: &str) -> Option<pgrx::JsonB> {
    relay_template_get_impl(p_name).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn relay_template_get_impl(name: &str) -> Result<Option<pgrx::JsonB>, PgTideError> {
    let result = Spi::get_one_with_args::<pgrx::JsonB>(
        "SELECT jsonb_build_object( \
            'name',          name, \
            'description',   description, \
            'required_keys', required_keys, \
            'config',        config, \
            'created_at',    created_at, \
            'updated_at',    updated_at \
         ) FROM tide.relay_pipeline_templates WHERE name = $1",
        &[name.into()],
    )
    .map_err(|e| PgTideError::SpiError(format!("relay_template_get SPI error: {e}")))?;

    Ok(result)
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_relay_template_list_returns_array() {
        let result = crate::template::relay_template_list();
        assert!(
            result.0.is_array(),
            "relay_template_list must return a JSON array"
        );
        // At minimum the 5 built-in templates should be present.
        let arr = result.0.as_array().unwrap();
        assert!(
            arr.len() >= 5,
            "expected at least 5 built-in templates, got {}",
            arr.len()
        );
    }

    #[pg_test]
    fn test_relay_template_get_builtin() {
        let result = crate::template::relay_template_get("kafka-topic-mirror");
        assert!(
            result.is_some(),
            "kafka-topic-mirror built-in template must exist"
        );
        let json = result.unwrap();
        assert_eq!(json.0["name"], "kafka-topic-mirror");
        assert!(json.0["required_keys"].is_array());
    }

    #[pg_test]
    fn test_relay_template_get_nonexistent() {
        let result = crate::template::relay_template_get("no-such-template");
        assert!(result.is_none(), "missing template should return None");
    }
}
