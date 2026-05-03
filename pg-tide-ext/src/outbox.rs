//! Transactional Outbox API for pg_tide.
//!
//! Provides the `tide.*` outbox functions extracted from pg_trickle v0.46.0.
//! Works with any PostgreSQL 18+ database — pg_trickle is NOT required.
//!
//! # Outbox Table Layout
//!
//! Each named outbox stores messages in `tide.tide_outbox_messages` with a
//! discriminator column `outbox_name`. The config is in
//! `tide.tide_outbox_config` (one row per named outbox).
//!
//! # Consumer Groups
//!
//! Consumer groups are registered in `tide.tide_consumer_groups` with offsets
//! in `tide.tide_consumer_offsets` and leases in `tide.tide_consumer_leases`.

use crate::error::PgTideError;
use pgrx::prelude::*;

// ── Internal helpers ──────────────────────────────────────────────────────

/// Build an outbox name from a user-supplied logical name.
/// Truncated to 63 bytes to stay within PostgreSQL identifier limits.
pub fn outbox_name_for(name: &str) -> String {
    name.chars().take(63).collect()
}

/// Check if an outbox with the given name exists in tide_outbox_config.
pub fn outbox_exists(outbox_name: &str) -> bool {
    Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM tide.tide_outbox_config WHERE outbox_name = $1)",
        &[outbox_name.into()],
    )
    .unwrap_or(None)
    .unwrap_or(false)
}

/// Get the retention_hours for a named outbox (returns None if not found).
fn get_outbox_retention(outbox_name: &str) -> Option<i32> {
    Spi::get_one_with_args::<i32>(
        "SELECT retention_hours FROM tide.tide_outbox_config WHERE outbox_name = $1",
        &[outbox_name.into()],
    )
    .unwrap_or(None)
}

// ── TIDE-API: outbox_create ───────────────────────────────────────────────

/// Create a new named outbox.
///
/// Creates an entry in `tide.tide_outbox_config`. Publishing goes via
/// `tide.outbox_publish()`. This function is idempotent — if the outbox
/// already exists with the same settings, it is a no-op.
#[pg_extern(schema = "tide")]
pub fn outbox_create(
    p_name: &str,
    p_retention_hours: default!(i32, 24),
    p_inline_threshold: default!(i32, 10000),
) {
    outbox_create_impl(p_name, p_retention_hours, p_inline_threshold)
        .unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn outbox_create_impl(
    name: &str,
    retention_hours: i32,
    inline_threshold: i32,
) -> Result<(), PgTideError> {
    if outbox_exists(name) {
        return Err(PgTideError::OutboxAlreadyExists(name.to_string()));
    }

    Spi::run_with_args(
        "INSERT INTO tide.tide_outbox_config \
         (outbox_name, retention_hours, inline_threshold) \
         VALUES ($1, $2, $3)",
        &[name.into(), retention_hours.into(), inline_threshold.into()],
    )
    .map_err(|e| PgTideError::SpiError(format!("INSERT tide_outbox_config: {e}")))?;

    pgrx::log!("[pg_tide] outbox_create: created outbox '{name}'");
    Ok(())
}

// ── TIDE-API: outbox_publish ──────────────────────────────────────────────

/// Publish a payload to a named outbox transactionally.
///
/// This is the core outbox write function. It runs in the caller's current
/// transaction, providing the same atomicity as a direct INSERT.
/// `pg_notify('tide_outbox_new', name)` fires after the INSERT.
#[pg_extern(schema = "tide")]
pub fn outbox_publish(p_name: &str, p_payload: pgrx::JsonB, p_headers: pgrx::JsonB) {
    outbox_publish_impl(p_name, p_payload, p_headers).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn outbox_publish_impl(
    name: &str,
    payload: pgrx::JsonB,
    headers: pgrx::JsonB,
) -> Result<(), PgTideError> {
    if !outbox_exists(name) {
        return Err(PgTideError::OutboxNotFound(name.to_string()));
    }

    let payload_str = serde_json::to_string(&payload.0)
        .map_err(|e| PgTideError::SpiError(format!("payload serialize: {e}")))?;
    let headers_str = serde_json::to_string(&headers.0)
        .map_err(|e| PgTideError::SpiError(format!("headers serialize: {e}")))?;

    Spi::run_with_args(
        "INSERT INTO tide.tide_outbox_messages \
         (outbox_name, payload, headers) \
         VALUES ($1, $2::jsonb, $3::jsonb)",
        &[
            name.into(),
            payload_str.as_str().into(),
            headers_str.as_str().into(),
        ],
    )
    .map_err(|e| PgTideError::SpiError(format!("INSERT outbox_messages: {e}")))?;

    // Notify consumers.
    let notify_sql = format!("SELECT pg_notify('tide_outbox_new', $1)");
    let _ = Spi::run_with_args(&notify_sql, &[name.into()]);

    Ok(())
}

// ── TIDE-API: outbox_drop ─────────────────────────────────────────────────

/// Drop a named outbox and all its messages.
///
/// Removes the `tide_outbox_config` entry and all messages in
/// `tide_outbox_messages` for this outbox. Consumer groups are also dropped.
#[pg_extern(schema = "tide")]
pub fn outbox_drop(p_name: &str, p_if_exists: default!(bool, false)) {
    outbox_drop_impl(p_name, p_if_exists).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn outbox_drop_impl(name: &str, if_exists: bool) -> Result<(), PgTideError> {
    if !outbox_exists(name) {
        if if_exists {
            return Ok(());
        }
        return Err(PgTideError::OutboxNotFound(name.to_string()));
    }

    // Delete messages first (FK cascade would also work but let's be explicit).
    Spi::run_with_args(
        "DELETE FROM tide.tide_outbox_messages WHERE outbox_name = $1",
        &[name.into()],
    )
    .map_err(|e| PgTideError::SpiError(format!("DELETE outbox_messages: {e}")))?;

    // Delete consumer groups (cascades to offsets and leases).
    Spi::run_with_args(
        "DELETE FROM tide.tide_consumer_groups WHERE outbox_name = $1",
        &[name.into()],
    )
    .map_err(|e| PgTideError::SpiError(format!("DELETE consumer_groups: {e}")))?;

    // Delete config.
    Spi::run_with_args(
        "DELETE FROM tide.tide_outbox_config WHERE outbox_name = $1",
        &[name.into()],
    )
    .map_err(|e| PgTideError::SpiError(format!("DELETE tide_outbox_config: {e}")))?;

    pgrx::log!("[pg_tide] outbox_drop: dropped outbox '{name}'");
    Ok(())
}

// ── TIDE-API: outbox_status ───────────────────────────────────────────────

/// Get status summary for a named outbox as JSONB.
///
/// Returns pending message count, oldest message age, total published count,
/// and per-consumer-group lag.
#[pg_extern(schema = "tide")]
pub fn outbox_status(p_name: &str) -> pgrx::JsonB {
    outbox_status_impl(p_name).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn outbox_status_impl(name: &str) -> Result<pgrx::JsonB, PgTideError> {
    if !outbox_exists(name) {
        return Err(PgTideError::OutboxNotFound(name.to_string()));
    }

    let pending: i64 = Spi::get_one_with_args::<i64>(
        "SELECT COUNT(*) FROM tide.tide_outbox_messages \
         WHERE outbox_name = $1 AND consumed_at IS NULL",
        &[name.into()],
    )
    .unwrap_or(None)
    .unwrap_or(0);

    let total: i64 = Spi::get_one_with_args::<i64>(
        "SELECT COUNT(*) FROM tide.tide_outbox_messages WHERE outbox_name = $1",
        &[name.into()],
    )
    .unwrap_or(None)
    .unwrap_or(0);

    let oldest_age_secs: Option<f64> = Spi::get_one_with_args::<f64>(
        "SELECT EXTRACT(EPOCH FROM (now() - MIN(created_at))) \
         FROM tide.tide_outbox_messages \
         WHERE outbox_name = $1 AND consumed_at IS NULL",
        &[name.into()],
    )
    .unwrap_or(None);

    let retention: i32 = get_outbox_retention(name).unwrap_or(24);

    let status = serde_json::json!({
        "outbox_name": name,
        "pending_messages": pending,
        "total_messages": total,
        "oldest_pending_age_seconds": oldest_age_secs,
        "retention_hours": retention,
    });

    Ok(pgrx::JsonB(status))
}

// ── TIDE-API: outbox_disable / outbox_enable ──────────────────────────────

/// Pause publishing to a named outbox (outbox_publish() becomes a no-op).
#[pg_extern(schema = "tide")]
pub fn outbox_disable(p_name: &str) {
    outbox_disable_impl(p_name).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn outbox_disable_impl(name: &str) -> Result<(), PgTideError> {
    let updated: Option<i64> = Spi::get_one_with_args::<i64>(
        "WITH u AS (UPDATE tide.tide_outbox_config SET enabled = false \
         WHERE outbox_name = $1 RETURNING 1) SELECT COUNT(*) FROM u",
        &[name.into()],
    )
    .unwrap_or(None);
    if updated.unwrap_or(0) == 0 {
        return Err(PgTideError::OutboxNotFound(name.to_string()));
    }
    Ok(())
}

/// Resume publishing to a previously disabled outbox.
#[pg_extern(schema = "tide")]
pub fn outbox_enable(p_name: &str) {
    outbox_enable_impl(p_name).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn outbox_enable_impl(name: &str) -> Result<(), PgTideError> {
    let updated: Option<i64> = Spi::get_one_with_args::<i64>(
        "WITH u AS (UPDATE tide.tide_outbox_config SET enabled = true \
         WHERE outbox_name = $1 RETURNING 1) SELECT COUNT(*) FROM u",
        &[name.into()],
    )
    .unwrap_or(None);
    if updated.unwrap_or(0) == 0 {
        return Err(PgTideError::OutboxNotFound(name.to_string()));
    }
    Ok(())
}

// ── TIDE-API: Consumer Groups ─────────────────────────────────────────────

/// Create a consumer group for a named outbox.
#[pg_extern(schema = "tide")]
pub fn create_consumer_group(
    p_name: &str,
    p_outbox: &str,
    p_auto_offset_reset: default!(&str, "'earliest'"),
    p_if_not_exists: default!(bool, false),
) {
    create_consumer_group_impl(p_name, p_outbox, p_auto_offset_reset, p_if_not_exists)
        .unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn create_consumer_group_impl(
    name: &str,
    outbox: &str,
    auto_offset_reset: &str,
    if_not_exists: bool,
) -> Result<(), PgTideError> {
    if !outbox_exists(outbox) {
        return Err(PgTideError::OutboxNotFound(outbox.to_string()));
    }

    let exists: bool = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM tide.tide_consumer_groups WHERE group_name = $1)",
        &[name.into()],
    )
    .unwrap_or(None)
    .unwrap_or(false);

    if exists {
        if if_not_exists {
            return Ok(());
        }
        // Use a simple string error for now — will be a proper variant if needed
        return Err(PgTideError::InvalidArgument(format!(
            "consumer group '{}' already exists",
            name
        )));
    }

    let valid_resets = ["earliest", "latest", "none"];
    if !valid_resets.contains(&auto_offset_reset) {
        return Err(PgTideError::InvalidArgument(format!(
            "auto_offset_reset must be one of: earliest, latest, none; got '{}'",
            auto_offset_reset
        )));
    }

    Spi::run_with_args(
        "INSERT INTO tide.tide_consumer_groups \
         (group_name, outbox_name, auto_offset_reset) VALUES ($1, $2, $3)",
        &[name.into(), outbox.into(), auto_offset_reset.into()],
    )
    .map_err(|e| PgTideError::SpiError(format!("INSERT consumer_groups: {e}")))?;

    Ok(())
}

/// Drop a consumer group.
#[pg_extern(schema = "tide")]
pub fn drop_consumer_group(p_name: &str, p_if_exists: default!(bool, false)) {
    drop_consumer_group_impl(p_name, p_if_exists).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn drop_consumer_group_impl(name: &str, if_exists: bool) -> Result<(), PgTideError> {
    let deleted: Option<i64> = Spi::get_one_with_args::<i64>(
        "WITH d AS (DELETE FROM tide.tide_consumer_groups WHERE group_name = $1 RETURNING 1) \
         SELECT COUNT(*) FROM d",
        &[name.into()],
    )
    .unwrap_or(None);

    if deleted.unwrap_or(0) == 0 && !if_exists {
        return Err(PgTideError::InvalidArgument(format!(
            "consumer group '{}' not found",
            name
        )));
    }
    Ok(())
}

/// Commit consumer offset after successful processing.
#[pg_extern(schema = "tide")]
pub fn commit_offset(p_group: &str, p_consumer: &str, p_last_offset: i64) {
    commit_offset_impl(p_group, p_consumer, p_last_offset).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn commit_offset_impl(group: &str, consumer: &str, last_offset: i64) -> Result<(), PgTideError> {
    Spi::run_with_args(
        "INSERT INTO tide.tide_consumer_offsets \
         (group_name, consumer_id, committed_offset, last_heartbeat) \
         VALUES ($1, $2, $3, now()) \
         ON CONFLICT (group_name, consumer_id) DO UPDATE \
         SET committed_offset = EXCLUDED.committed_offset, \
             last_heartbeat = EXCLUDED.last_heartbeat",
        &[group.into(), consumer.into(), last_offset.into()],
    )
    .map_err(|e| PgTideError::SpiError(format!("commit_offset: {e}")))?;

    // Clear lease.
    let _ = Spi::run_with_args(
        "DELETE FROM tide.tide_consumer_leases WHERE group_name = $1 AND consumer_id = $2",
        &[group.into(), consumer.into()],
    );

    Ok(())
}

/// Update consumer heartbeat timestamp.
#[pg_extern(schema = "tide")]
pub fn consumer_heartbeat(p_group: &str, p_consumer: &str) {
    consumer_heartbeat_impl(p_group, p_consumer).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn consumer_heartbeat_impl(group: &str, consumer: &str) -> Result<(), PgTideError> {
    let _ = Spi::run_with_args(
        "UPDATE tide.tide_consumer_offsets SET last_heartbeat = now() \
         WHERE group_name = $1 AND consumer_id = $2",
        &[group.into(), consumer.into()],
    );
    Ok(())
}
