//! Idempotent Inbox API for pg_tide.
//!
//! Provides `tide.inbox_create()`, `tide.inbox_mark_processed()`,
//! `tide.inbox_mark_failed()`, `tide.replay_inbox_messages()`, etc.

use crate::error::PgTideError;
use pgrx::prelude::*;

// ── Internal helpers ──────────────────────────────────────────────────────

fn inbox_exists(name: &str) -> bool {
    Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM tide.tide_inbox_config WHERE inbox_name = $1)",
        &[name.into()],
    )
    .unwrap_or(None)
    .unwrap_or(false)
}

// ── TIDE-API: inbox_create ────────────────────────────────────────────────

/// Create a named inbox.
///
/// Creates an entry in `tide.tide_inbox_config` and the message table
/// `tide.tide_inbox_messages` for this inbox.
#[pg_extern(schema = "tide")]
pub fn inbox_create(
    p_name: &str,
    p_schema: default!(&str, "'tide'"),
    p_max_retries: default!(i32, 3),
    p_processed_retention_hours: default!(i32, 72),
    p_dlq_retention_hours: default!(i32, 0),
) {
    inbox_create_impl(
        p_name,
        p_schema,
        p_max_retries,
        p_processed_retention_hours,
        p_dlq_retention_hours,
    )
    .unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn inbox_create_impl(
    name: &str,
    schema: &str,
    max_retries: i32,
    processed_retention_hours: i32,
    dlq_retention_hours: i32,
) -> Result<(), PgTideError> {
    if inbox_exists(name) {
        return Err(PgTideError::InboxAlreadyExists(name.to_string()));
    }

    Spi::run_with_args(
        "INSERT INTO tide.tide_inbox_config \
         (inbox_name, inbox_schema, max_retries, \
          processed_retention_hours, dlq_retention_hours) \
         VALUES ($1, $2, $3, $4, $5)",
        &[
            name.into(),
            schema.into(),
            max_retries.into(),
            processed_retention_hours.into(),
            dlq_retention_hours.into(),
        ],
    )
    .map_err(|e| PgTideError::SpiError(format!("INSERT tide_inbox_config: {e}")))?;

    // Create the inbox message table in the specified schema.
    let create_table = format!(
        r#"CREATE TABLE IF NOT EXISTS "{schema}"."{name}_inbox" (
            id             BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            event_id       TEXT        NOT NULL,
            source         TEXT,
            payload        JSONB,
            headers        JSONB,
            received_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            processed_at   TIMESTAMPTZ,
            retry_count    INT         NOT NULL DEFAULT 0,
            last_error     TEXT,
            CONSTRAINT uq_{name}_event_id UNIQUE (event_id)
        )"#
    );
    Spi::run(&create_table)
        .map_err(|e| PgTideError::SpiError(format!("CREATE inbox table: {e}")))?;

    pgrx::log!("[pg_tide] inbox_create: created inbox '{name}'");
    Ok(())
}

// ── TIDE-API: inbox_drop ──────────────────────────────────────────────────

/// Drop a named inbox and its message table.
#[pg_extern(schema = "tide")]
pub fn inbox_drop(p_name: &str, p_if_exists: default!(bool, false)) {
    inbox_drop_impl(p_name, p_if_exists).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn inbox_drop_impl(name: &str, if_exists: bool) -> Result<(), PgTideError> {
    if !inbox_exists(name) {
        if if_exists {
            return Ok(());
        }
        return Err(PgTideError::InboxNotFound(name.to_string()));
    }

    // Get the schema to drop the table from.
    let schema: String = Spi::get_one_with_args::<String>(
        "SELECT inbox_schema FROM tide.tide_inbox_config WHERE inbox_name = $1",
        &[name.into()],
    )
    .unwrap_or(None)
    .unwrap_or_else(|| "tide".to_string());

    let drop_table = format!(r#"DROP TABLE IF EXISTS "{schema}"."{name}_inbox" CASCADE"#);
    let _ = Spi::run(&drop_table);

    Spi::run_with_args(
        "DELETE FROM tide.tide_inbox_config WHERE inbox_name = $1",
        &[name.into()],
    )
    .map_err(|e| PgTideError::SpiError(format!("DELETE tide_inbox_config: {e}")))?;

    pgrx::log!("[pg_tide] inbox_drop: dropped inbox '{name}'");
    Ok(())
}

// ── TIDE-API: inbox_mark_processed ────────────────────────────────────────

/// Mark an inbox message as successfully processed.
#[pg_extern(schema = "tide")]
pub fn inbox_mark_processed(p_name: &str, p_event_id: &str) {
    inbox_mark_processed_impl(p_name, p_event_id).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn inbox_mark_processed_impl(name: &str, event_id: &str) -> Result<(), PgTideError> {
    if !inbox_exists(name) {
        return Err(PgTideError::InboxNotFound(name.to_string()));
    }

    let schema: String = Spi::get_one_with_args::<String>(
        "SELECT inbox_schema FROM tide.tide_inbox_config WHERE inbox_name = $1",
        &[name.into()],
    )
    .unwrap_or(None)
    .unwrap_or_else(|| "tide".to_string());

    let sql = format!(
        r#"UPDATE "{schema}"."{name}_inbox" \
           SET processed_at = now() \
           WHERE event_id = $1 AND processed_at IS NULL"#
    );
    Spi::run_with_args(&sql, &[event_id.into()])
        .map_err(|e| PgTideError::SpiError(format!("mark_processed: {e}")))?;

    Ok(())
}

// ── TIDE-API: inbox_mark_failed ───────────────────────────────────────────

/// Mark an inbox message as failed (increments retry_count, stores error).
#[pg_extern(schema = "tide")]
pub fn inbox_mark_failed(p_name: &str, p_event_id: &str, p_error: &str) {
    inbox_mark_failed_impl(p_name, p_event_id, p_error).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn inbox_mark_failed_impl(name: &str, event_id: &str, error: &str) -> Result<(), PgTideError> {
    if !inbox_exists(name) {
        return Err(PgTideError::InboxNotFound(name.to_string()));
    }

    let schema: String = Spi::get_one_with_args::<String>(
        "SELECT inbox_schema FROM tide.tide_inbox_config WHERE inbox_name = $1",
        &[name.into()],
    )
    .unwrap_or(None)
    .unwrap_or_else(|| "tide".to_string());

    let sql = format!(
        r#"UPDATE "{schema}"."{name}_inbox" \
           SET retry_count = retry_count + 1, \
               last_error  = $2 \
           WHERE event_id = $1"#
    );
    Spi::run_with_args(&sql, &[event_id.into(), error.into()])
        .map_err(|e| PgTideError::SpiError(format!("mark_failed: {e}")))?;

    Ok(())
}

// ── TIDE-API: inbox_status ────────────────────────────────────────────────

/// Get status summary for a named inbox as JSONB.
#[pg_extern(schema = "tide")]
pub fn inbox_status(p_name: default!(Option<&str>, "NULL")) -> pgrx::JsonB {
    inbox_status_impl(p_name).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn inbox_status_impl(name: Option<&str>) -> Result<pgrx::JsonB, PgTideError> {
    // If a specific name is given, return status for that inbox.
    // Otherwise return a summary of all inboxes.
    if let Some(n) = name {
        if !inbox_exists(n) {
            return Err(PgTideError::InboxNotFound(n.to_string()));
        }
        let schema: String = Spi::get_one_with_args::<String>(
            "SELECT inbox_schema FROM tide.tide_inbox_config WHERE inbox_name = $1",
            &[n.into()],
        )
        .unwrap_or(None)
        .unwrap_or_else(|| "tide".to_string());

        let pending: i64 = Spi::get_one_with_args::<i64>(
            &format!(r#"SELECT COUNT(*) FROM "{schema}"."{n}_inbox" WHERE processed_at IS NULL"#),
            &[],
        )
        .unwrap_or(None)
        .unwrap_or(0);

        let dlq_count: i64 = Spi::get_one_with_args::<i64>(
            &format!(
                r#"SELECT COUNT(*) FROM "{schema}"."{n}_inbox" \
                   WHERE processed_at IS NULL \
                     AND retry_count >= (SELECT max_retries FROM tide.tide_inbox_config WHERE inbox_name = $1)"#
            ),
            &[n.into()],
        )
        .unwrap_or(None)
        .unwrap_or(0);

        let status = serde_json::json!({
            "inbox_name": n,
            "pending": pending,
            "dlq_count": dlq_count,
        });
        return Ok(pgrx::JsonB(status));
    }

    // Summary of all inboxes.
    let all = serde_json::json!({"inboxes": []});
    Ok(pgrx::JsonB(all))
}

// ── TIDE-API: replay_inbox_messages ──────────────────────────────────────

/// Re-queue failed messages from DLQ back to the pending queue.
#[pg_extern(schema = "tide")]
pub fn replay_inbox_messages(p_name: &str, p_event_ids: Vec<String>) -> i64 {
    replay_inbox_messages_impl(p_name, p_event_ids).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn replay_inbox_messages_impl(name: &str, event_ids: Vec<String>) -> Result<i64, PgTideError> {
    if !inbox_exists(name) {
        return Err(PgTideError::InboxNotFound(name.to_string()));
    }

    let schema: String = Spi::get_one_with_args::<String>(
        "SELECT inbox_schema FROM tide.tide_inbox_config WHERE inbox_name = $1",
        &[name.into()],
    )
    .unwrap_or(None)
    .unwrap_or_else(|| "tide".to_string());

    let mut replayed: i64 = 0;
    for event_id in &event_ids {
        let sql = format!(
            r#"UPDATE "{schema}"."{name}_inbox" \
               SET retry_count = 0, last_error = NULL, processed_at = NULL \
               WHERE event_id = $1 AND processed_at IS NULL \
               RETURNING 1"#
        );
        let count: i64 = Spi::get_one_with_args::<i64>(
            &format!("WITH u AS ({sql}) SELECT COUNT(*) FROM u"),
            &[event_id.as_str().into()],
        )
        .unwrap_or(None)
        .unwrap_or(0);
        replayed += count;
    }
    Ok(replayed)
}
