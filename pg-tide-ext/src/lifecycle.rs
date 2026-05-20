//! Pipeline Lifecycle Management API for pg_tide (v0.29.0).
//!
//! Provides Rust-backed helpers for pipeline pause state management:
//! - `tide.relay_pipeline_state_upsert()` — write pause/resume state from relay
//! - `tide.relay_auto_resume_candidates()` — query pipelines eligible for auto-resume
//!
//! The view `relay_config_history()` and `relay_pipeline_pause_reason()` are
//! pure SQL in the migration file and do not need Rust wrappers.

use crate::error::PgTideError;
use pgrx::prelude::*;

// ── TIDE-API: relay_pipeline_state_upsert ────────────────────────────────

/// Upsert the runtime pause/resume state for a pipeline.
///
/// Called by the relay coordinator when a worker transitions to a paused state.
#[pg_extern(schema = "tide")]
pub fn relay_pipeline_state_upsert(
    p_name: &str,
    p_last_error: default!(Option<&str>, "NULL"),
    p_error_class: default!(Option<&str>, "NULL"),
    p_failure_count: default!(i32, 0_i32),
) {
    relay_pipeline_state_upsert_impl(p_name, p_last_error, p_error_class, p_failure_count)
        .unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn relay_pipeline_state_upsert_impl(
    name: &str,
    last_error: Option<&str>,
    error_class: Option<&str>,
    failure_count: i32,
) -> Result<(), PgTideError> {
    crate::validation::validate_identifier(name)?;

    // Validate error_class value when provided.
    if let Some(cls) = error_class {
        if cls != "transient" && cls != "permanent" {
            return Err(PgTideError::InvalidArgument(format!(
                "error_class must be 'transient' or 'permanent', got '{cls}'"
            )));
        }
    }

    Spi::run_with_args(
        "INSERT INTO tide.relay_pipeline_state \
            (name, last_error, error_class, pause_started_at, failure_count, updated_at) \
         VALUES ($1, $2, $3, CASE WHEN $4 > 0 THEN now() ELSE NULL END, $4, now()) \
         ON CONFLICT (name) DO UPDATE \
            SET last_error       = EXCLUDED.last_error, \
                error_class      = EXCLUDED.error_class, \
                pause_started_at = CASE \
                    WHEN EXCLUDED.failure_count > 0 AND tide.relay_pipeline_state.pause_started_at IS NULL \
                    THEN now() \
                    WHEN EXCLUDED.failure_count = 0 THEN NULL \
                    ELSE tide.relay_pipeline_state.pause_started_at \
                END, \
                failure_count    = EXCLUDED.failure_count, \
                updated_at       = now()",
        &[
            name.into(),
            last_error.into(),
            error_class.into(),
            failure_count.into(),
        ],
    )
    .map_err(|e| PgTideError::SpiError(format!("relay_pipeline_state_upsert: {e}")))?;

    Ok(())
}

// ── TIDE-API: relay_auto_resume_candidates ───────────────────────────────

/// Return a JSON array of pipeline names eligible for auto-resume.
///
/// A pipeline is eligible if:
///   - It has auto_resume_after set (non-NULL)
///   - The pipeline is currently disabled (enabled = FALSE)
///   - The pause duration has exceeded auto_resume_after
#[pg_extern(schema = "tide")]
pub fn relay_auto_resume_candidates() -> pgrx::JsonB {
    relay_auto_resume_candidates_impl().unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn relay_auto_resume_candidates_impl() -> Result<pgrx::JsonB, PgTideError> {
    let result = Spi::get_one::<pgrx::JsonB>(
        "SELECT COALESCE( \
            (SELECT jsonb_agg(jsonb_build_object( \
                'name',              rc.name, \
                'type',              'outbox', \
                'pause_started_at',  s.pause_started_at, \
                'auto_resume_after', c.auto_resume_after \
             )) \
             FROM tide.tide_outbox_config c \
             JOIN tide.relay_outbox_config rc ON rc.name = (c.outbox_name || '-pipeline') OR rc.name = c.outbox_name \
             JOIN tide.relay_pipeline_state s ON s.name = rc.name \
             WHERE c.auto_resume_after IS NOT NULL \
               AND rc.enabled = FALSE \
               AND s.pause_started_at IS NOT NULL \
               AND now() - s.pause_started_at > c.auto_resume_after \
            ), \
            '[]'::jsonb)",
    )
    .map_err(|e| PgTideError::SpiError(format!("relay_auto_resume_candidates: {e}")))?;

    Ok(result.unwrap_or_else(|| pgrx::JsonB(serde_json::json!([]))))
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_relay_pipeline_state_upsert_creates_row() {
        crate::lifecycle::relay_pipeline_state_upsert(
            "test-lc-pipeline",
            Some("connection refused"),
            Some("transient"),
            3,
        );

        let failure_count: i32 = Spi::get_one(
            "SELECT failure_count FROM tide.relay_pipeline_state WHERE name = 'test-lc-pipeline'",
        )
        .unwrap()
        .unwrap_or(0);
        assert_eq!(failure_count, 3);
    }

    #[pg_test]
    fn test_relay_pipeline_state_upsert_reset_on_zero() {
        crate::lifecycle::relay_pipeline_state_upsert(
            "test-lc-pipeline-reset",
            Some("some error"),
            Some("transient"),
            2,
        );
        crate::lifecycle::relay_pipeline_state_upsert("test-lc-pipeline-reset", None, None, 0);

        let failure_count: i32 = Spi::get_one(
            "SELECT failure_count FROM tide.relay_pipeline_state \
             WHERE name = 'test-lc-pipeline-reset'",
        )
        .unwrap()
        .unwrap_or(99);
        assert_eq!(failure_count, 0, "failure count should reset to 0");
    }

    #[pg_test]
    fn test_relay_pipeline_state_invalid_error_class() {
        let result = crate::lifecycle::relay_pipeline_state_upsert_impl(
            "test-lc-bad-class",
            Some("err"),
            Some("unknown"),
            1,
        );
        assert!(result.is_err(), "invalid error_class must fail");
    }

    #[pg_test]
    fn test_relay_auto_resume_candidates_returns_array() {
        let result = crate::lifecycle::relay_auto_resume_candidates();
        assert!(
            result.0.is_array(),
            "relay_auto_resume_candidates must return a JSON array"
        );
    }
}
