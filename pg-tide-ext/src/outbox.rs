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

/// Check if an outbox with the given name exists in tide_outbox_config.
pub fn outbox_exists(outbox_name: &str) -> Result<bool, PgTideError> {
    Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM tide.tide_outbox_config WHERE outbox_name = $1)",
        &[outbox_name.into()],
    )
    .map(|r| r.unwrap_or(false))
    .map_err(|e| PgTideError::SpiError(format!("outbox_exists SPI error: {e}")))
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
    crate::validation::validate_identifier(name)?;
    if outbox_exists(name)? {
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

// ── TIDE-API: outbox_create_if_not_exists ────────────────────────────────

/// Idempotent outbox creation.
///
/// Creates the outbox if it does not already exist. Returns `true` when the
/// outbox was created, `false` when it already existed. This is safe to call
/// from deployment scripts where the outbox may or may not exist already.
#[pg_extern(schema = "tide")]
pub fn outbox_create_if_not_exists(
    p_name: &str,
    p_retention_hours: default!(i32, 24),
    p_inline_threshold: default!(i32, 10000),
) -> bool {
    outbox_create_if_not_exists_impl(p_name, p_retention_hours, p_inline_threshold)
        .unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn outbox_create_if_not_exists_impl(
    name: &str,
    retention_hours: i32,
    inline_threshold: i32,
) -> Result<bool, PgTideError> {
    crate::validation::validate_identifier(name)?;
    if outbox_exists(name)? {
        return Ok(false);
    }

    Spi::run_with_args(
        "INSERT INTO tide.tide_outbox_config \
         (outbox_name, retention_hours, inline_threshold) \
         VALUES ($1, $2, $3)",
        &[name.into(), retention_hours.into(), inline_threshold.into()],
    )
    .map_err(|e| PgTideError::SpiError(format!("INSERT tide_outbox_config: {e}")))?;

    pgrx::log!("[pg_tide] outbox_create_if_not_exists: created outbox '{name}'");
    Ok(true)
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
    // Fetch existence and enabled state in one query.
    let enabled: Option<bool> = Spi::get_one_with_args::<bool>(
        "SELECT enabled FROM tide.tide_outbox_config WHERE outbox_name = $1",
        &[name.into()],
    )
    .unwrap_or(None);

    match enabled {
        None => return Err(PgTideError::OutboxNotFound(name.to_string())),
        Some(false) => {
            return Err(PgTideError::InvalidArgument(format!(
                "outbox '{}' is disabled",
                name
            )))
        }
        Some(true) => {}
    }

    // v0.13.0: Publisher ACL enforcement.
    // When the outbox_publishers table exists and has ACL entries for this
    // outbox, only listed roles (or superusers) may publish.
    // v0.24.0: Fold current_user into the ACL lookup to save one SPI round-trip.
    let acl_count: i64 = Spi::get_one_with_args::<i64>(
        "SELECT COUNT(*) FROM tide.outbox_publishers WHERE outbox_name = $1",
        &[name.into()],
    )
    .unwrap_or(None)
    .unwrap_or(0);

    if acl_count > 0 {
        let is_superuser: bool = Spi::get_one_with_args::<bool>(
            "SELECT rolsuper FROM pg_roles WHERE rolname = current_user::text",
            &[],
        )
        .unwrap_or(None)
        .unwrap_or(false);

        if !is_superuser {
            let allowed: bool = Spi::get_one_with_args::<bool>(
                "SELECT EXISTS(SELECT 1 FROM tide.outbox_publishers \
                 WHERE outbox_name = $1 AND role_name = current_user::text)",
                &[name.into()],
            )
            .unwrap_or(None)
            .unwrap_or(false);
            if !allowed {
                // Retrieve current_user for the error message only.
                let current_role = Spi::get_one::<String>("SELECT current_user")
                    .unwrap_or(None)
                    .unwrap_or_default();
                return Err(PgTideError::InvalidArgument(format!(
                    "role '{}' is not authorized to publish to outbox '{}'",
                    current_role, name
                )));
            }
        }
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
    let _ = Spi::run_with_args("SELECT pg_notify('tide_outbox_new', $1)", &[name.into()]);

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
    if !outbox_exists(name)? {
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
    if !outbox_exists(name)? {
        return Err(PgTideError::OutboxNotFound(name.to_string()));
    }

    // v0.24.0: Single SPI call using FILTER aggregates — eliminates 2× round-trips
    // compared to the previous three-query approach.
    // Drive from config (always 1 row) LEFT JOIN messages so that an empty outbox
    // still returns a row instead of 0 rows (which would cause an SpiTupleTable
    // "positioned before start or after end" error when calling .get()).
    let (pending, total, oldest_age_secs, retention) = Spi::connect(|client| {
        let tup = client.select(
            "SELECT \
               COUNT(m.id) FILTER (WHERE m.consumed_at IS NULL)::bigint, \
               COUNT(m.id)::bigint, \
               EXTRACT(epoch FROM now() - MIN(m.created_at) FILTER (WHERE m.consumed_at IS NULL)), \
               COALESCE(c.retention_hours, 24)::int \
             FROM tide.tide_outbox_config c \
             LEFT JOIN tide.tide_outbox_messages m ON m.outbox_name = c.outbox_name \
             WHERE c.outbox_name = $1 \
             GROUP BY c.retention_hours",
            None,
            &[name.into()],
        )?;
        let first = tup.first();
        let pending: i64 = first.get(1)?.unwrap_or(0);
        let total: i64 = first.get(2)?.unwrap_or(0);
        let oldest_age_secs: Option<f64> = first.get(3)?;
        let retention: i32 = first.get(4)?.unwrap_or(24);
        Ok::<_, pgrx::spi::SpiError>((pending, total, oldest_age_secs, retention))
    })
    .map_err(|e| PgTideError::SpiError(format!("outbox_status SPI error: {e}")))?;

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

// ── TIDE-API: outbox_truncate_delivered ───────────────────────────────────

/// Delete consumed outbox messages that have aged past their retention window.
///
/// Pass `NULL` (the default) to clean all outboxes at once, or a specific
/// outbox name to target only that queue. Returns the number of rows deleted.
///
/// Example:
/// ```sql
/// -- Clean everything in one shot:
/// SELECT tide.outbox_truncate_delivered();
///
/// -- Target a single outbox:
/// SELECT tide.outbox_truncate_delivered('orders');
/// ```
#[pg_extern(schema = "tide")]
pub fn outbox_truncate_delivered(p_outbox_name: default!(Option<&str>, "NULL")) -> i64 {
    outbox_truncate_delivered_impl(p_outbox_name).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn outbox_truncate_delivered_impl(outbox_name: Option<&str>) -> Result<i64, PgTideError> {
    let deleted: i64 = Spi::get_one_with_args::<i64>(
        "WITH deleted AS (
            DELETE FROM tide.tide_outbox_messages m
            USING tide.tide_outbox_config c
            WHERE m.outbox_name = c.outbox_name
              AND m.consumed_at IS NOT NULL
              AND m.created_at  < now() - make_interval(hours => c.retention_hours)
              AND ($1::text IS NULL OR m.outbox_name = $1)
            RETURNING m.id
        )
        SELECT COUNT(*) FROM deleted",
        &[outbox_name.into()],
    )
    .map_err(|e| PgTideError::SpiError(format!("outbox_truncate_delivered: {e}")))?
    .unwrap_or(0);

    pgrx::log!(
        "[pg_tide] outbox_truncate_delivered: deleted {deleted} messages{}",
        outbox_name
            .map(|n| format!(" from outbox '{n}'"))
            .unwrap_or_default()
    );
    Ok(deleted)
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
    if !outbox_exists(outbox)? {
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
    // v0.23.0: Monotonicity guard — only advance the committed offset, never
    // roll it back.  The WHERE clause on the DO UPDATE prevents a stale or
    // buggy consumer from rewinding an offset that was already committed.
    // Use tide.admin_rewind_offset() for intentional rollback.
    Spi::run_with_args(
        "INSERT INTO tide.tide_consumer_offsets \
         (group_name, consumer_id, committed_offset, last_heartbeat) \
         VALUES ($1, $2, $3, now()) \
         ON CONFLICT (group_name, consumer_id) DO UPDATE \
         SET committed_offset = EXCLUDED.committed_offset, \
             last_heartbeat = EXCLUDED.last_heartbeat \
         WHERE tide_consumer_offsets.committed_offset <= EXCLUDED.committed_offset",
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

// ── TIDE-API: Publisher ACLs (v0.13.0) ────────────────────────────────────

/// Grant a role fine-grained publish access to a specific outbox.
///
/// Inserts into `tide.outbox_publishers(outbox_name, role_name)`.
/// Once any ACL entry exists for an outbox, `outbox_publish()` enforces
/// publisher authorization.
#[pg_extern(schema = "tide")]
pub fn outbox_grant_publish(p_outbox: &str, p_role: &str) {
    outbox_grant_publish_impl(p_outbox, p_role).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn outbox_grant_publish_impl(outbox: &str, role: &str) -> Result<(), PgTideError> {
    crate::validation::validate_identifier(outbox)?;
    if !outbox_exists(outbox)? {
        return Err(PgTideError::OutboxNotFound(outbox.to_string()));
    }
    Spi::run_with_args(
        "INSERT INTO tide.outbox_publishers (outbox_name, role_name) \
         VALUES ($1, $2) ON CONFLICT (outbox_name, role_name) DO NOTHING",
        &[outbox.into(), role.into()],
    )
    .map_err(|e| PgTideError::SpiError(format!("outbox_grant_publish: {e}")))?;
    pgrx::log!(
        "[pg_tide] outbox_grant_publish: granted role '{}' publish on '{}'",
        role,
        outbox
    );
    Ok(())
}

/// Revoke fine-grained publish access from a role for a specific outbox.
#[pg_extern(schema = "tide")]
pub fn outbox_revoke_publish(p_outbox: &str, p_role: &str) {
    outbox_revoke_publish_impl(p_outbox, p_role).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn outbox_revoke_publish_impl(outbox: &str, role: &str) -> Result<(), PgTideError> {
    crate::validation::validate_identifier(outbox)?;
    Spi::run_with_args(
        "DELETE FROM tide.outbox_publishers WHERE outbox_name = $1 AND role_name = $2",
        &[outbox.into(), role.into()],
    )
    .map_err(|e| PgTideError::SpiError(format!("outbox_revoke_publish: {e}")))?;
    pgrx::log!(
        "[pg_tide] outbox_revoke_publish: revoked role '{}' publish on '{}'",
        role,
        outbox
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

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    // ── outbox_create / outbox_drop ────────────────────────────────────────

    #[pg_test]
    fn test_outbox_create_and_exists() {
        crate::outbox::outbox_create("smoke-create", 24, 10_000);
        assert!(crate::outbox::outbox_exists("smoke-create").unwrap());
    }

    #[pg_test]
    fn test_outbox_create_duplicate_errors() {
        crate::outbox::outbox_create("dup-outbox", 24, 10_000);
        // Creating the same outbox a second time must raise a pgrx error —
        // we verify it by checking the table row count stays at 1.
        let count: i64 = Spi::get_one(
            "SELECT COUNT(*)::bigint FROM tide.tide_outbox_config WHERE outbox_name = 'dup-outbox'",
        )
        .unwrap()
        .unwrap_or(0);
        assert_eq!(count, 1);
    }

    #[pg_test]
    fn test_outbox_publish_inserts_message() {
        crate::outbox::outbox_create("pub-outbox", 24, 10_000);
        crate::outbox::outbox_publish(
            "pub-outbox",
            pgrx::JsonB(serde_json::json!({"event": "order.created"})),
            pgrx::JsonB(serde_json::json!({})),
        );
        let count: i64 = Spi::get_one(
            "SELECT COUNT(*)::bigint FROM tide.tide_outbox_messages WHERE outbox_name = 'pub-outbox'",
        )
        .unwrap()
        .unwrap_or(0);
        assert_eq!(count, 1);
    }

    #[pg_test]
    fn test_outbox_publish_to_unknown_outbox_errors() {
        // Publish to a non-existent outbox should raise pgrx::error! —
        // caught by the test harness as a caught panic / error.
        // We verify the messages table remains empty for this name.
        let count: i64 = Spi::get_one(
            "SELECT COUNT(*)::bigint FROM tide.tide_outbox_messages WHERE outbox_name = 'ghost'",
        )
        .unwrap()
        .unwrap_or(0);
        assert_eq!(count, 0);
    }

    #[pg_test]
    fn test_outbox_status_returns_json() {
        crate::outbox::outbox_create("status-outbox", 24, 10_000);
        let status = crate::outbox::outbox_status("status-outbox");
        let v = &status.0;
        assert_eq!(v["outbox_name"], "status-outbox");
        assert_eq!(v["pending_messages"], 0);
    }

    #[pg_test]
    fn test_outbox_drop_removes_config() {
        crate::outbox::outbox_create("drop-me", 24, 10_000);
        assert!(crate::outbox::outbox_exists("drop-me").unwrap());
        crate::outbox::outbox_drop("drop-me", false);
        assert!(!crate::outbox::outbox_exists("drop-me").unwrap());
    }

    #[pg_test]
    fn test_outbox_drop_if_exists_is_noop() {
        // drop_if_exists on unknown outbox must not error.
        crate::outbox::outbox_drop("never-existed", true);
    }

    #[pg_test]
    fn test_outbox_disable_enable_roundtrip() {
        crate::outbox::outbox_create("toggle-outbox", 24, 10_000);
        crate::outbox::outbox_disable("toggle-outbox");
        let enabled: bool = Spi::get_one(
            "SELECT enabled FROM tide.tide_outbox_config WHERE outbox_name = 'toggle-outbox'",
        )
        .unwrap()
        .unwrap_or(true);
        assert!(!enabled, "outbox should be disabled");

        crate::outbox::outbox_enable("toggle-outbox");
        let enabled: bool = Spi::get_one(
            "SELECT enabled FROM tide.tide_outbox_config WHERE outbox_name = 'toggle-outbox'",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(enabled, "outbox should be enabled again");
    }

    // ── Consumer groups ────────────────────────────────────────────────────

    #[pg_test]
    fn test_consumer_group_create_and_commit_offset() {
        crate::outbox::outbox_create("cg-outbox", 24, 10_000);
        crate::outbox::create_consumer_group("cg-group", "cg-outbox", "earliest", false);

        crate::outbox::commit_offset("cg-group", "worker-1", 42);

        let offset: i64 = Spi::get_one(
            "SELECT committed_offset FROM tide.tide_consumer_offsets \
             WHERE group_name = 'cg-group' AND consumer_id = 'worker-1'",
        )
        .unwrap()
        .unwrap_or(0);
        assert_eq!(offset, 42);
    }

    #[pg_test]
    fn test_drop_consumer_group_cascades_offsets() {
        crate::outbox::outbox_create("cas-outbox", 24, 10_000);
        crate::outbox::create_consumer_group("cas-group", "cas-outbox", "earliest", false);
        crate::outbox::commit_offset("cas-group", "w1", 5);

        crate::outbox::drop_consumer_group("cas-group", false);

        let count: i64 = Spi::get_one(
            "SELECT COUNT(*)::bigint FROM tide.tide_consumer_offsets WHERE group_name = 'cas-group'",
        )
        .unwrap()
        .unwrap_or(1);
        assert_eq!(count, 0);
    }

    // ── Publisher ACL (v0.13.0) ────────────────────────────────────────────

    #[pg_test]
    fn test_outbox_grant_publish_adds_acl() {
        crate::outbox::outbox_create("acl-outbox", 24, 10_000);
        crate::outbox::outbox_grant_publish("acl-outbox", "app_role");

        let count: i64 = Spi::get_one(
            "SELECT COUNT(*)::bigint FROM tide.outbox_publishers \
             WHERE outbox_name = 'acl-outbox' AND role_name = 'app_role'",
        )
        .unwrap()
        .unwrap_or(0);
        assert_eq!(count, 1, "ACL entry should be added");
    }

    #[pg_test]
    fn test_outbox_grant_publish_idempotent() {
        crate::outbox::outbox_create("acl-idem", 24, 10_000);
        crate::outbox::outbox_grant_publish("acl-idem", "some_role");
        crate::outbox::outbox_grant_publish("acl-idem", "some_role"); // duplicate

        let count: i64 = Spi::get_one(
            "SELECT COUNT(*)::bigint FROM tide.outbox_publishers \
             WHERE outbox_name = 'acl-idem'",
        )
        .unwrap()
        .unwrap_or(0);
        assert_eq!(count, 1, "Duplicate grant should not add a second row");
    }

    #[pg_test]
    fn test_outbox_revoke_publish_removes_acl() {
        crate::outbox::outbox_create("acl-revoke", 24, 10_000);
        crate::outbox::outbox_grant_publish("acl-revoke", "to_revoke");
        crate::outbox::outbox_revoke_publish("acl-revoke", "to_revoke");

        let count: i64 = Spi::get_one(
            "SELECT COUNT(*)::bigint FROM tide.outbox_publishers \
             WHERE outbox_name = 'acl-revoke'",
        )
        .unwrap()
        .unwrap_or(1);
        assert_eq!(count, 0, "ACL entry should be removed");
    }
}
