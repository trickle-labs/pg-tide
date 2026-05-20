//! Managed Backfill Jobs API for pg_tide (v0.14.0).
//!
//! Provides `tide.backfill_create()`, `tide.backfill_pause()`,
//! `tide.backfill_resume()`, `tide.backfill_status()` in the `tide` schema.
//!
//! Backfill jobs replay a range of outbox messages through a pipeline at a
//! configurable chunk size and throttle rate, without interfering with live
//! CDC pipelines.

use crate::error::PgTideError;
use pgrx::prelude::*;

// ── TIDE-API: backfill_create ────────────────────────────────────────────

/// Register a new cataloged backfill job.
///
/// Returns the new `job_id`.
#[pg_extern(schema = "tide")]
#[allow(clippy::too_many_arguments)]
pub fn backfill_create(
    p_job_name: &str,
    p_outbox_name: &str,
    p_pipeline_name: default!(Option<&str>, "NULL"),
    p_from_id: default!(i64, 0_i64),
    p_to_id: default!(i64, 9223372036854775807_i64),
    p_chunk_size: default!(i32, 500_i32),
    p_throttle_ms: default!(i32, 0_i32),
) -> i64 {
    backfill_create_impl(
        p_job_name,
        p_outbox_name,
        p_pipeline_name,
        p_from_id,
        p_to_id,
        p_chunk_size,
        p_throttle_ms,
    )
    .unwrap_or_else(|e| pgrx::error!("{}", e))
}

#[allow(clippy::too_many_arguments)]
fn backfill_create_impl(
    job_name: &str,
    outbox_name: &str,
    pipeline_name: Option<&str>,
    from_id: i64,
    to_id: i64,
    chunk_size: i32,
    throttle_ms: i32,
) -> Result<i64, PgTideError> {
    crate::validation::validate_identifier(job_name)?;
    crate::validation::validate_identifier(outbox_name)?;

    // Validate outbox exists.
    if !crate::outbox::outbox_exists(outbox_name)? {
        return Err(PgTideError::OutboxNotFound(outbox_name.to_string()));
    }

    // Estimate total rows.
    let rows_total: i64 = Spi::get_one_with_args::<i64>(
        "SELECT COUNT(*)::bigint FROM tide.tide_outbox_messages \
         WHERE outbox_name = $1 AND id BETWEEN $2 AND $3",
        &[outbox_name.into(), from_id.into(), to_id.into()],
    )
    .unwrap_or(None)
    .unwrap_or(0);

    let job_id: i64 = if let Some(pname) = pipeline_name {
        Spi::get_one_with_args::<i64>(
            "INSERT INTO tide.backfill_jobs \
             (job_name, outbox_name, pipeline_name, from_id, to_id, \
              chunk_size, rows_total, throttle_ms) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
             RETURNING job_id",
            &[
                job_name.into(),
                outbox_name.into(),
                pname.into(),
                from_id.into(),
                to_id.into(),
                chunk_size.into(),
                rows_total.into(),
                throttle_ms.into(),
            ],
        )
        .map_err(|e| PgTideError::SpiError(format!("INSERT backfill_jobs: {e}")))?
    } else {
        Spi::get_one_with_args::<i64>(
            "INSERT INTO tide.backfill_jobs \
             (job_name, outbox_name, from_id, to_id, \
              chunk_size, rows_total, throttle_ms) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             RETURNING job_id",
            &[
                job_name.into(),
                outbox_name.into(),
                from_id.into(),
                to_id.into(),
                chunk_size.into(),
                rows_total.into(),
                throttle_ms.into(),
            ],
        )
        .map_err(|e| PgTideError::SpiError(format!("INSERT backfill_jobs (no pipeline): {e}")))?
    }
    .unwrap_or(0);

    pgrx::log!("[pg_tide] backfill_create: job '{job_name}' registered with id {job_id}");
    Ok(job_id)
}

// ── TIDE-API: backfill_pause ─────────────────────────────────────────────

/// Pause a pending or running backfill job.
#[pg_extern(schema = "tide")]
pub fn backfill_pause(p_job_name: &str) {
    backfill_pause_impl(p_job_name).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn backfill_pause_impl(job_name: &str) -> Result<(), PgTideError> {
    let updated: i64 = Spi::get_one_with_args::<i64>(
        "WITH u AS ( \
            UPDATE tide.backfill_jobs \
            SET status = 'paused', paused_at = now() \
            WHERE job_name = $1 AND status IN ('pending', 'running') \
            RETURNING 1 \
         ) SELECT COUNT(*)::bigint FROM u",
        &[job_name.into()],
    )
    .unwrap_or(None)
    .unwrap_or(0);

    if updated == 0 {
        return Err(PgTideError::InvalidArgument(format!(
            "backfill job '{}' not found or not pauseable (status must be pending/running)",
            job_name
        )));
    }
    Ok(())
}

// ── TIDE-API: backfill_resume ────────────────────────────────────────────

/// Resume a paused backfill job.
#[pg_extern(schema = "tide")]
pub fn backfill_resume(p_job_name: &str) {
    backfill_resume_impl(p_job_name).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn backfill_resume_impl(job_name: &str) -> Result<(), PgTideError> {
    let updated: i64 = Spi::get_one_with_args::<i64>(
        "WITH u AS ( \
            UPDATE tide.backfill_jobs \
            SET status = 'pending', paused_at = NULL \
            WHERE job_name = $1 AND status = 'paused' \
            RETURNING 1 \
         ) SELECT COUNT(*)::bigint FROM u",
        &[job_name.into()],
    )
    .unwrap_or(None)
    .unwrap_or(0);

    if updated == 0 {
        return Err(PgTideError::InvalidArgument(format!(
            "backfill job '{}' not found or not paused",
            job_name
        )));
    }
    Ok(())
}

// ── TIDE-API: backfill_status ────────────────────────────────────────────

/// Get progress JSON for a backfill job, or fleet summary when called with NULL.
#[pg_extern(schema = "tide")]
pub fn backfill_status(p_job_name: default!(Option<&str>, "NULL")) -> pgrx::JsonB {
    backfill_status_impl(p_job_name).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn backfill_status_impl(job_name: Option<&str>) -> Result<pgrx::JsonB, PgTideError> {
    if let Some(name) = job_name {
        let row = Spi::get_one_with_args::<pgrx::JsonB>(
            "SELECT jsonb_build_object( \
                'job_id',         job_id, \
                'job_name',       job_name, \
                'outbox_name',    outbox_name, \
                'pipeline_name',  pipeline_name, \
                'status',         status, \
                'rows_processed', rows_processed, \
                'rows_total',     rows_total, \
                'pct_complete',   CASE WHEN rows_total > 0 \
                                  THEN ROUND(rows_processed::numeric / rows_total * 100, 2) \
                                  ELSE 0 END, \
                'chunk_size',     chunk_size, \
                'throttle_ms',    throttle_ms, \
                'created_at',     created_at, \
                'started_at',     started_at, \
                'paused_at',      paused_at, \
                'completed_at',   completed_at, \
                'error_message',  error_message \
             ) \
             FROM tide.backfill_jobs WHERE job_name = $1",
            &[name.into()],
        )
        .map_err(|e| PgTideError::SpiError(format!("backfill_status query: {e}")))?;

        row.ok_or_else(|| PgTideError::InvalidArgument(format!("backfill job '{name}' not found")))
    } else {
        // Fleet summary.
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT jsonb_build_object('jobs', COALESCE( \
                (SELECT jsonb_agg(jsonb_build_object( \
                    'job_id',         job_id, \
                    'job_name',       job_name, \
                    'outbox_name',    outbox_name, \
                    'status',         status, \
                    'rows_processed', rows_processed, \
                    'rows_total',     rows_total, \
                    'pct_complete',   CASE WHEN rows_total > 0 \
                                      THEN ROUND(rows_processed::numeric / rows_total * 100, 2) \
                                      ELSE 0 END, \
                    'created_at',     created_at \
                 ) ORDER BY job_id) FROM tide.backfill_jobs), '[]'::jsonb))",
        )
        .unwrap_or(None);

        Ok(result.unwrap_or_else(|| pgrx::JsonB(serde_json::json!({"jobs": []}))))
    }
}

// ── TIDE-API: backfill_cancel (v0.29.0) ──────────────────────────────────

/// Cancel a pending, running, or paused backfill job.
///
/// Sets `status = 'failed'` with `error_message = 'cancelled by operator'`.
/// Once cancelled, a job cannot be resumed.
///
/// NOTE: The PostgreSQL-facing function is defined in the migration SQL as a
/// plain PL/pgSQL function.  This Rust implementation is used internally by
/// the relay coordinator and by pgrx tests only.
pub fn backfill_cancel(p_job_name: &str) {
    backfill_cancel_impl(p_job_name).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn backfill_cancel_impl(job_name: &str) -> Result<(), PgTideError> {
    crate::validation::validate_identifier(job_name)?;
    let updated: i64 = Spi::get_one_with_args::<i64>(
        "WITH u AS ( \
            UPDATE tide.backfill_jobs \
            SET status = 'failed', \
                error_message = 'cancelled by operator', \
                completed_at = now() \
            WHERE job_name = $1 AND status IN ('pending', 'running', 'paused') \
            RETURNING 1 \
         ) SELECT COUNT(*)::bigint FROM u",
        &[job_name.into()],
    )
    .unwrap_or(None)
    .unwrap_or(0);

    if updated == 0 {
        return Err(PgTideError::InvalidArgument(format!(
            "backfill job '{}' not found or already completed/failed",
            job_name
        )));
    }
    pgrx::log!("[pg_tide] backfill_cancel: job '{job_name}' cancelled");
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

    #[pg_test]
    fn test_backfill_create_returns_job_id() {
        setup_outbox("backfill-outbox");
        let job_id = crate::backfill::backfill_create(
            "my-backfill",
            "backfill-outbox",
            None,
            0,
            i64::MAX,
            500,
            0,
        );
        assert!(job_id > 0, "backfill_create must return a positive job_id");
    }

    #[pg_test]
    fn test_backfill_pause_and_resume() {
        setup_outbox("pause-resume-outbox");
        crate::backfill::backfill_create(
            "pauseable-job",
            "pause-resume-outbox",
            None,
            0,
            i64::MAX,
            500,
            0,
        );

        crate::backfill::backfill_pause("pauseable-job");
        let status: String =
            Spi::get_one("SELECT status FROM tide.backfill_jobs WHERE job_name = 'pauseable-job'")
                .unwrap()
                .unwrap_or_default();
        assert_eq!(status, "paused");

        crate::backfill::backfill_resume("pauseable-job");
        let status2: String =
            Spi::get_one("SELECT status FROM tide.backfill_jobs WHERE job_name = 'pauseable-job'")
                .unwrap()
                .unwrap_or_default();
        assert_eq!(status2, "pending");
    }

    #[pg_test]
    fn test_backfill_status_returns_json() {
        setup_outbox("status-test-outbox");
        crate::backfill::backfill_create(
            "status-job",
            "status-test-outbox",
            None,
            0,
            i64::MAX,
            500,
            0,
        );
        let s = crate::backfill::backfill_status(Some("status-job"));
        assert_eq!(s.0["job_name"], "status-job");
        assert_eq!(s.0["status"], "pending");
    }

    #[pg_test]
    fn test_backfill_status_null_returns_fleet() {
        setup_outbox("fleet-backfill-outbox");
        crate::backfill::backfill_create(
            "fleet-job",
            "fleet-backfill-outbox",
            None,
            0,
            i64::MAX,
            500,
            0,
        );
        let s = crate::backfill::backfill_status(None);
        assert!(
            s.0.get("jobs").and_then(|a| a.as_array()).is_some(),
            "fleet summary must contain 'jobs' array"
        );
    }

    #[pg_test]
    fn test_backfill_create_unknown_outbox_errors() {
        // Should error because outbox doesn't exist.
        // We rely on the fact that pgrx::error! terminates the test with a panic.
        // Just test the internal impl returns an Err.
        let result = crate::backfill::backfill_create_impl(
            "bad-job",
            "nonexistent-outbox",
            None,
            0,
            i64::MAX,
            500,
            0,
        );
        assert!(
            result.is_err(),
            "creating backfill for nonexistent outbox must fail"
        );
    }

    #[pg_test]
    fn test_backfill_cancel() {
        setup_outbox("cancel-outbox");
        crate::backfill::backfill_create(
            "cancellable-job",
            "cancel-outbox",
            None,
            0,
            i64::MAX,
            500,
            0,
        );

        crate::backfill::backfill_cancel("cancellable-job");
        let status: String = Spi::get_one(
            "SELECT status FROM tide.backfill_jobs WHERE job_name = 'cancellable-job'",
        )
        .unwrap()
        .unwrap_or_default();
        assert_eq!(status, "failed");

        let err_msg: String = Spi::get_one(
            "SELECT error_message FROM tide.backfill_jobs WHERE job_name = 'cancellable-job'",
        )
        .unwrap()
        .unwrap_or_default();
        assert_eq!(err_msg, "cancelled by operator");
    }

    #[pg_test]
    fn test_backfill_cancel_unknown_errors() {
        let result = crate::backfill::backfill_cancel_impl("nonexistent-job-xyz");
        assert!(result.is_err(), "cancelling nonexistent job must fail");
    }
}
