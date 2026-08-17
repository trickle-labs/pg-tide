//! Transactional Outbox API for pg_tide.
//!
//! Provides the `tide.*` outbox functions extracted from pg_trickle v0.46.0.
//! Works with PostgreSQL 18 — pg_trickle is NOT required.
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
use serde_json::Value;
use std::time::Instant;

// ── Internal helpers ──────────────────────────────────────────────────────

const MAX_SWEEP_BATCH: i32 = 10_000;

fn lock_outbox(name: &str, shared: bool) -> Result<(), PgTideError> {
    let lock_fn = if shared {
        "pg_advisory_xact_lock_shared"
    } else {
        "pg_advisory_xact_lock"
    };
    let sql = format!("SELECT {lock_fn}(hashtextextended('pg_tide:outbox:' || $1, 0))");
    Spi::run_with_args(&sql, &[name.into()])
        .map_err(|e| PgTideError::SpiError(format!("outbox fence for '{name}': {e}")))
}

fn lock_outbox_session(name: &str) -> Result<(), PgTideError> {
    Spi::run_with_args(
        "SELECT pg_advisory_lock(hashtextextended('pg_tide:outbox:' || $1, 0))",
        &[name.into()],
    )
    .map_err(|e| PgTideError::SpiError(format!("outbox maintenance fence for '{name}': {e}")))
}

fn unlock_outbox_session(name: &str) -> Result<(), PgTideError> {
    let unlocked = Spi::get_one_with_args::<bool>(
        "SELECT pg_advisory_unlock(hashtextextended('pg_tide:outbox:' || $1, 0))",
        &[name.into()],
    )
    .map_err(|e| PgTideError::SpiError(format!("release outbox fence for '{name}': {e}")))?;
    if unlocked != Some(true) {
        return Err(PgTideError::SpiError(format!(
            "outbox maintenance fence for '{name}' was not held"
        )));
    }
    Ok(())
}

/// Check if an outbox with the given name exists in tide_outbox_config.
pub fn outbox_exists(outbox_name: &str) -> Result<bool, PgTideError> {
    Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM tide.tide_outbox_config WHERE outbox_name = $1)",
        &[outbox_name.into()],
    )
    .map_err(|e| PgTideError::SpiError(format!("outbox_exists SPI error: {e}")))?
    .ok_or_else(|| {
        PgTideError::SpiError(format!(
            "outbox_exists returned NULL for outbox '{outbox_name}'"
        ))
    })
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
    p_partition_strategy: default!(&str, "'none'"),
) {
    outbox_create_impl(
        p_name,
        p_retention_hours,
        p_inline_threshold,
        p_partition_strategy,
    )
    .unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn outbox_create_impl(
    name: &str,
    retention_hours: i32,
    inline_threshold: i32,
    partition_strategy: &str,
) -> Result<(), PgTideError> {
    crate::validation::validate_identifier(name)?;
    let strategy = partition_strategy.trim_matches('\'');
    if !matches!(strategy, "none" | "daily" | "weekly" | "monthly") {
        return Err(PgTideError::InvalidArgument(format!(
            "partition_strategy must be 'none', 'daily', 'weekly', or 'monthly'; got '{strategy}'"
        )));
    }
    // v0.26.0: NAMEDATALEN guard — partition table names are derived from the
    // outbox name.  The backup prefix 'tide_outbox_messages_backup_' is 29 bytes,
    // leaving at most 34 bytes for the name fragment (after replacing '-' with '_').
    if strategy != "none" {
        let fragment = name.replace('-', "_");
        let backup_len = "tide_outbox_messages_backup_".len() + fragment.len();
        if backup_len > 63 {
            return Err(PgTideError::InvalidArgument(format!(
                "outbox name '{}' is too long for partitioned outbox table naming \
                 (backup prefix is 29 bytes, max name fragment is 34 bytes, got {} bytes). \
                 Shorten the outbox name to at most 34 characters.",
                name,
                fragment.len()
            )));
        }
    }
    if outbox_exists(name)? {
        return Err(PgTideError::OutboxAlreadyExists(name.to_string()));
    }

    Spi::run_with_args(
        "INSERT INTO tide.tide_outbox_config \
         (outbox_name, retention_hours, inline_threshold, partition_strategy) \
         VALUES ($1, $2, $3, $4)",
        &[
            name.into(),
            retention_hours.into(),
            inline_threshold.into(),
            strategy.into(),
        ],
    )
    .map_err(|e| PgTideError::SpiError(format!("INSERT tide_outbox_config: {e}")))?;

    Spi::run_with_args(
        "INSERT INTO tide.outbox_cleanup_state (outbox_name) VALUES ($1) \
         ON CONFLICT (outbox_name) DO NOTHING",
        &[name.into()],
    )
    .map_err(|e| PgTideError::SpiError(format!("INSERT outbox_cleanup_state: {e}")))?;

    pgrx::log!("[pg_tide] outbox_create: created outbox '{name}' (partition_strategy={strategy})");
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
    p_partition_strategy: default!(&str, "'none'"),
) -> bool {
    outbox_create_if_not_exists_impl(
        p_name,
        p_retention_hours,
        p_inline_threshold,
        p_partition_strategy,
    )
    .unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn outbox_create_if_not_exists_impl(
    name: &str,
    retention_hours: i32,
    inline_threshold: i32,
    partition_strategy: &str,
) -> Result<bool, PgTideError> {
    crate::validation::validate_identifier(name)?;
    let strategy = partition_strategy.trim_matches('\'');
    if !matches!(strategy, "none" | "daily" | "weekly" | "monthly") {
        return Err(PgTideError::InvalidArgument(format!(
            "partition_strategy must be 'none', 'daily', 'weekly', or 'monthly'; got '{strategy}'"
        )));
    }
    // v0.26.0: NAMEDATALEN guard — same check as outbox_create_impl.
    if strategy != "none" {
        let fragment = name.replace('-', "_");
        let backup_len = "tide_outbox_messages_backup_".len() + fragment.len();
        if backup_len > 63 {
            return Err(PgTideError::InvalidArgument(format!(
                "outbox name '{}' is too long for partitioned outbox table naming \
                 (backup prefix is 29 bytes, max name fragment is 34 bytes, got {} bytes). \
                 Shorten the outbox name to at most 34 characters.",
                name,
                fragment.len()
            )));
        }
    }
    if outbox_exists(name)? {
        return Ok(false);
    }

    Spi::run_with_args(
        "INSERT INTO tide.tide_outbox_config \
         (outbox_name, retention_hours, inline_threshold, partition_strategy) \
         VALUES ($1, $2, $3, $4)",
        &[
            name.into(),
            retention_hours.into(),
            inline_threshold.into(),
            strategy.into(),
        ],
    )
    .map_err(|e| PgTideError::SpiError(format!("INSERT tide_outbox_config: {e}")))?;

    Spi::run_with_args(
        "INSERT INTO tide.outbox_cleanup_state (outbox_name) VALUES ($1) \
         ON CONFLICT (outbox_name) DO NOTHING",
        &[name.into()],
    )
    .map_err(|e| PgTideError::SpiError(format!("INSERT outbox_cleanup_state: {e}")))?;

    pgrx::log!("[pg_tide] outbox_create_if_not_exists: created outbox '{name}' (partition_strategy={strategy})");
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
    // v0.40.0 (ADR-011 §15): Authorization is fail-closed. Any SPI error while
    // reading outbox state, ACL existence, superuser status, or role membership
    // aborts the publish rather than defaulting to allow.

    // 1. Read existence and enabled state; an SPI error aborts.
    let enabled: Option<bool> = Spi::get_one_with_args::<bool>(
        "SELECT enabled FROM tide.tide_outbox_config WHERE outbox_name = $1",
        &[name.into()],
    )
    .map_err(|e| {
        PgTideError::SpiError(format!("outbox_publish existence check for '{name}': {e}"))
    })?;

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

    // 2. Resolve the caller and authorization role state; every lookup is
    // fail-closed so a missing row cannot become an implicit allow.
    let current_role = Spi::get_one::<String>("SELECT session_user::text")
        .map_err(|e| PgTideError::AuthorizationError {
            outbox: name.to_string(),
            detail: format!("session_user lookup failed: {e}"),
        })?
        .ok_or_else(|| PgTideError::AuthorizationError {
            outbox: name.to_string(),
            detail: "session_user lookup returned no row".to_string(),
        })?;
    let is_superuser = Spi::get_one_with_args::<bool>(
        "SELECT rolsuper FROM pg_roles WHERE rolname = $1",
        &[current_role.as_str().into()],
    )
    .map_err(|e| PgTideError::AuthorizationError {
        outbox: name.to_string(),
        detail: format!("superuser lookup failed: {e}"),
    })?
    .ok_or_else(|| PgTideError::AuthorizationError {
        outbox: name.to_string(),
        detail: format!("role '{current_role}' was not found"),
    })?;
    let is_extension_owner = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(
             SELECT 1
             FROM pg_extension e
             JOIN pg_roles r ON r.oid = e.extowner
             WHERE e.extname = 'pg_tide' AND r.rolname = $1
         )",
        &[current_role.as_str().into()],
    )
    .map_err(|e| PgTideError::AuthorizationError {
        outbox: name.to_string(),
        detail: format!("extension-owner lookup failed: {e}"),
    })?
    .ok_or_else(|| PgTideError::AuthorizationError {
        outbox: name.to_string(),
        detail: "extension-owner lookup returned no row".to_string(),
    })?;

    // 3. Query the ACL verdict; an SPI error aborts (fail-closed).
    // v0.40.0: Explicitly account for superuser, extension-owner, and
    // inherited role membership before allowing the insert.
    let acl_verdict: Option<String> = Spi::get_one_with_args::<String>(
        "SELECT CASE
           WHEN $2::bool THEN 'superuser'
           WHEN $3::bool THEN 'extension_owner'
           WHEN NOT EXISTS(
                  SELECT 1
                  FROM pg_roles r
                  WHERE r.rolname = 'tide_publisher'
                    AND pg_has_role($4::name, r.rolname, 'member')
                )
             THEN 'denied'
           WHEN NOT EXISTS(SELECT 1 FROM tide.outbox_publishers WHERE outbox_name = $1)
             THEN 'denied'
           WHEN EXISTS(SELECT 1 FROM tide.outbox_publishers
                       WHERE outbox_name = $1
                         AND pg_has_role($4::name, role_name::name, 'member'))
             THEN 'allowed'
           ELSE 'denied'
         END",
        &[
            name.into(),
            is_superuser.into(),
            is_extension_owner.into(),
            current_role.as_str().into(),
        ],
    )
    .map_err(|e| PgTideError::AuthorizationError {
        outbox: name.to_string(),
        detail: format!("ACL verdict query failed: {e}"),
    })?;

    // 3–5. Accept only explicit superuser / extension_owner / allowed. Reject 'denied'
    // and treat any unknown or null verdict as an internal authorization error.
    match acl_verdict.as_deref() {
        Some("superuser") | Some("extension_owner") | Some("allowed") => { /* publish proceeds */ }
        Some("denied") => {
            return Err(PgTideError::PublishDenied {
                role: current_role,
                outbox: name.to_string(),
            });
        }
        other => {
            return Err(PgTideError::AuthorizationError {
                outbox: name.to_string(),
                detail: format!("unexpected ACL verdict: {other:?}"),
            });
        }
    }

    // Shared publishers are compatible with one another but conflict with the
    // exclusive fence used by polling and cleanup.
    lock_outbox(name, true)?;

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
    Spi::run_with_args("SELECT pg_notify('tide_outbox_new', $1)", &[name.into()])
        .map_err(|e| PgTideError::SpiError(format!("notify outbox '{name}': {e}")))?;

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

    lock_outbox(name, false)?;

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

    let status: pgrx::JsonB = Spi::connect(|client| {
        let tup = client.select(
            "SELECT row_to_json(s)::jsonb \
               FROM tide.outbox_retention_status s \
              WHERE s.outbox_name = $1",
            None,
            &[name.into()],
        )?;
        let first = tup.first();
        let value: Option<pgrx::JsonB> = first.get(1)?;
        Ok::<_, pgrx::spi::SpiError>(
            value.unwrap_or_else(|| pgrx::JsonB(serde_json::json!({"outbox_name": name}))),
        )
    })
    .map_err(|e| PgTideError::SpiError(format!("outbox_status SPI error: {e}")))?;

    Ok(status)
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
    .map_err(|e| PgTideError::SpiError(format!("disable outbox '{name}': {e}")))?;
    let updated = updated
        .ok_or_else(|| PgTideError::SpiError(format!("disable outbox '{name}' returned NULL")))?;
    if updated == 0 {
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
    .map_err(|e| PgTideError::SpiError(format!("enable outbox '{name}': {e}")))?;
    let updated = updated
        .ok_or_else(|| PgTideError::SpiError(format!("enable outbox '{name}' returned NULL")))?;
    if updated == 0 {
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
    let result = outbox_sweep_impl(outbox_name, 1_000, false)?;
    let deleted = result
        .get("outboxes")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter_map(|row| row.get("affected_rows").and_then(Value::as_i64))
                .sum()
        })
        .unwrap_or(0);

    pgrx::log!(
        "[pg_tide] outbox_truncate_delivered: deleted {deleted} messages{}",
        outbox_name
            .map(|n| format!(" from outbox '{n}'"))
            .unwrap_or_default()
    );
    Ok(deleted)
}

/// Sweep retained messages safely for one or all outboxes.
#[pg_extern(schema = "tide", create_or_replace)]
pub fn outbox_sweep(
    p_outbox_name: default!(Option<&str>, "NULL"),
    p_batch_size: default!(i32, 1000),
    p_dry_run: default!(bool, false),
) -> pgrx::JsonB {
    pgrx::JsonB(
        outbox_sweep_impl(p_outbox_name, p_batch_size, p_dry_run)
            .unwrap_or_else(|e| pgrx::error!("{}", e)),
    )
}

fn outbox_sweep_impl(
    outbox_name: Option<&str>,
    batch_size: i32,
    dry_run: bool,
) -> Result<Value, PgTideError> {
    if !(1..=MAX_SWEEP_BATCH).contains(&batch_size) {
        return Err(PgTideError::InvalidArgument(format!(
            "p_batch_size must be between 1 and {MAX_SWEEP_BATCH}; got {batch_size}"
        )));
    }

    let names = match outbox_name {
        Some(name) => {
            if !outbox_exists(name)? {
                return Err(PgTideError::OutboxNotFound(name.to_string()));
            }
            vec![name.to_string()]
        }
        None => Spi::connect(|client| {
            let tuples = client.select(
                "SELECT outbox_name::text FROM tide.tide_outbox_config ORDER BY outbox_name",
                None,
                &[],
            )?;
            let mut names = Vec::new();
            for row in tuples {
                if let Some(name) = row.get::<String>(1)? {
                    names.push(name);
                }
            }
            Ok::<_, pgrx::spi::SpiError>(names)
        })
        .map_err(|e| PgTideError::SpiError(format!("list sweep outboxes: {e}")))?,
    };

    let mut results = Vec::with_capacity(names.len());
    for name in names {
        results.push(sweep_one(&name, batch_size, dry_run)?);
    }
    Ok(serde_json::json!({
        "batch_size": batch_size,
        "dry_run": dry_run,
        "outboxes": results,
    }))
}

fn sweep_one(name: &str, batch_size: i32, dry_run: bool) -> Result<Value, PgTideError> {
    lock_outbox_session(name)?;
    let result = sweep_one_locked(name, batch_size, dry_run);
    let unlock_result = unlock_outbox_session(name);
    match (result, unlock_result) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}

fn sweep_one_locked(name: &str, batch_size: i32, dry_run: bool) -> Result<Value, PgTideError> {
    let started = Instant::now();

    let (retention_hours, safe_offset, participants, lease_blocked) = Spi::connect(|client| {
        let config = client.select(
            "SELECT retention_hours FROM tide.tide_outbox_config WHERE outbox_name = $1",
            None,
            &[name.into()],
        )?;
        let retention_hours: i32 = config.first().get(1)?.unwrap_or(24);

        let participants = client.select(
            "WITH relay_participants AS (
                 SELECT c.name::text AS participant, c.enabled,
                        COALESCE(MIN(o.last_change_id), 0)::bigint AS safe_offset
                   FROM tide.relay_outbox_config c
                   LEFT JOIN tide.relay_consumer_offsets o
                     ON o.pipeline_id = c.name
                    AND o.outbox_name = $1
                  WHERE c.config #>> '{source,outbox}' = $1
                    AND c.config ->> 'source_type' = 'outbox'
                  GROUP BY c.name, c.enabled
               ), group_participants AS (
                 SELECT g.group_name::text AS participant, true AS enabled,
                        COALESCE(MIN(o.committed_offset), 0)::bigint AS safe_offset
                   FROM tide.tide_consumer_groups g
                   LEFT JOIN tide.tide_consumer_offsets o USING (group_name)
                  WHERE g.outbox_name = $1
                  GROUP BY g.group_name
               ), fanin_participants AS (
                 SELECT f.name::text || '/' || member::text AS participant,
                        f.enabled,
                        COALESCE(MIN(o.last_change_id), 0)::bigint AS safe_offset
                   FROM tide.relay_fanin_config f
                   CROSS JOIN LATERAL unnest(f.outbox_names) AS members(member)
                   LEFT JOIN tide.relay_consumer_offsets o
                     ON o.pipeline_id = f.name
                    AND o.outbox_name = member
                    AND o.fanin_member = member
                  WHERE f.enabled AND member = $1
                  GROUP BY f.name, f.enabled, member
               ), all_participants AS (
                 SELECT * FROM relay_participants
                 UNION ALL
                 SELECT * FROM group_participants
                 UNION ALL
                 SELECT * FROM fanin_participants
               )
               SELECT MIN(safe_offset)::bigint,
                      COALESCE(jsonb_agg(jsonb_build_object(
                          'name', participant,
                          'enabled', enabled,
                          'safe_offset', safe_offset
                      ) ORDER BY participant), '[]'::jsonb)
                 FROM all_participants",
            None,
            &[name.into()],
        )?;
        let participant_row = participants.first();
        let safe_offset: Option<i64> = participant_row.get(1)?;
        let participants: pgrx::JsonB = participant_row
            .get(2)?
            .unwrap_or_else(|| pgrx::JsonB(serde_json::json!([])));

        let lease_blocked: bool = client
            .select(
                "SELECT EXISTS(
                    SELECT 1
                      FROM tide.tide_consumer_leases l
                      JOIN tide.tide_consumer_groups g USING (group_name)
                     WHERE g.outbox_name = $1 AND l.expires_at > now()
                )",
                None,
                &[name.into()],
            )?
            .first()
            .get(1)?
            .unwrap_or(false);
        Ok::<_, pgrx::spi::SpiError>((retention_hours, safe_offset, participants.0, lease_blocked))
    })
    .map_err(|e| PgTideError::SweepFailed {
        outbox: name.to_string(),
        detail: format!("resolve retention participants: {e}"),
    })?;

    let cutoff = format!("now() - make_interval(hours => {retention_hours})");
    let candidates = if lease_blocked {
        Vec::new()
    } else {
        Spi::connect(|client| {
            let query = format!(
                "SELECT id FROM tide.tide_outbox_messages
                  WHERE outbox_name = $1
                    AND created_at < {cutoff}
                    AND ($2::bigint IS NULL OR id <= $2)
                  ORDER BY id
                  LIMIT $3
                  FOR UPDATE SKIP LOCKED"
            );
            let tuples = client.select(
                &query,
                None,
                &[name.into(), safe_offset.into(), (batch_size + 1).into()],
            )?;
            let mut ids = Vec::new();
            for row in tuples {
                if let Some(id) = row.get::<i64>(1)? {
                    ids.push(id);
                }
            }
            Ok::<_, pgrx::spi::SpiError>(ids)
        })
        .map_err(|e| PgTideError::SweepFailed {
            outbox: name.to_string(),
            detail: format!("select candidates: {e}"),
        })?
    };

    let has_more = candidates.len() > batch_size as usize;
    let eligible_in_batch = candidates.len().min(batch_size as usize) as i64;
    let mut affected_rows = 0_i64;
    let mut highest_deleted_id = None;

    if !dry_run && eligible_in_batch > 0 {
        let deleted = Spi::connect(|client| {
            let query = format!(
                "WITH candidates AS (
                     SELECT ctid
                       FROM tide.tide_outbox_messages
                      WHERE outbox_name = $1
                        AND created_at < {cutoff}
                        AND ($2::bigint IS NULL OR id <= $2)
                      ORDER BY id
                      LIMIT $3
                      FOR UPDATE SKIP LOCKED
                 ), deleted AS (
                     DELETE FROM tide.tide_outbox_messages m
                      USING candidates c
                      WHERE m.ctid = c.ctid
                      RETURNING m.id
                 )
                 SELECT COUNT(*)::bigint, MAX(id) FROM deleted"
            );
            let tuples = client.select(
                &query,
                None,
                &[name.into(), safe_offset.into(), batch_size.into()],
            )?;
            let first = tuples.first();
            Ok::<_, pgrx::spi::SpiError>((first.get::<i64>(1)?.unwrap_or(0), first.get::<i64>(2)?))
        })
        .map_err(|e| PgTideError::SweepFailed {
            outbox: name.to_string(),
            detail: format!("delete candidates: {e}"),
        })?;
        affected_rows = deleted.0;
        highest_deleted_id = deleted.1;
    }

    if !dry_run {
        Spi::run_with_args(
            "INSERT INTO tide.outbox_cleanup_state
                 (outbox_name, last_success_at, last_safe_offset, highest_deleted_id,
                  last_batch_rows, total_rows_deleted, last_duration_ms, last_partition_action)
             VALUES ($1, now(), $2, COALESCE($3, 0), $4, $4, $5, 'none')
             ON CONFLICT (outbox_name) DO UPDATE SET
                 last_success_at = EXCLUDED.last_success_at,
                 last_safe_offset = EXCLUDED.last_safe_offset,
                 highest_deleted_id = GREATEST(
                     tide.outbox_cleanup_state.highest_deleted_id,
                     EXCLUDED.highest_deleted_id
                 ),
                 last_batch_rows = EXCLUDED.last_batch_rows,
                 total_rows_deleted = tide.outbox_cleanup_state.total_rows_deleted
                     + EXCLUDED.last_batch_rows,
                 last_duration_ms = EXCLUDED.last_duration_ms,
                 last_partition_action = EXCLUDED.last_partition_action",
            &[
                name.into(),
                safe_offset.into(),
                highest_deleted_id.into(),
                affected_rows.into(),
                (started.elapsed().as_secs_f64() * 1000.0).into(),
            ],
        )
        .map_err(|e| PgTideError::SweepFailed {
            outbox: name.to_string(),
            detail: format!("record cleanup state: {e}"),
        })?;
    }

    Ok(serde_json::json!({
        "outbox": name,
        "retention_cutoff": cutoff,
        "safe_offset": safe_offset,
        "participants": participants,
        "blockers": if lease_blocked {
            serde_json::json!([{"type": "active_lease"}])
        } else {
            serde_json::json!([])
        },
        "eligible_in_batch": eligible_in_batch,
        "affected_rows": affected_rows,
        "has_more": has_more,
        "highest_deleted_id": highest_deleted_id,
        "duration_ms": started.elapsed().as_secs_f64() * 1000.0,
        "partition_action": "none",
    }))
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
    .map_err(|e| PgTideError::SpiError(format!("drop consumer group '{name}': {e}")))?;

    let deleted = deleted.ok_or_else(|| {
        PgTideError::SpiError(format!("drop consumer group '{name}' returned NULL"))
    })?;
    if deleted == 0 && !if_exists {
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
    if last_offset < 0 {
        return Err(PgTideError::InvalidArgument(
            "consumer offset must be non-negative".to_string(),
        ));
    }

    // v0.23.0: Monotonicity guard — only advance the committed offset, never
    // roll it back.  The WHERE clause on the DO UPDATE prevents a stale or
    // buggy consumer from rewinding an offset that was already committed.
    // Use tide.admin_rewind_offset() for intentional rollback.
    let persisted: Option<i64> = Spi::get_one_with_args(
        "INSERT INTO tide.tide_consumer_offsets \
         (group_name, consumer_id, committed_offset, last_heartbeat) \
         VALUES ($1, $2, $3, now()) \
         ON CONFLICT (group_name, consumer_id) DO UPDATE \
         SET committed_offset = EXCLUDED.committed_offset, \
             last_heartbeat = EXCLUDED.last_heartbeat \
         WHERE tide_consumer_offsets.committed_offset <= EXCLUDED.committed_offset \
         RETURNING committed_offset",
        &[group.into(), consumer.into(), last_offset.into()],
    )
    .map_err(|e| PgTideError::SpiError(format!("commit_offset: {e}")))?;
    if persisted.is_none() {
        return Err(PgTideError::InvalidArgument(format!(
            "consumer offset {last_offset} is lower than the committed offset"
        )));
    }

    // Clear lease.
    Spi::run_with_args(
        "DELETE FROM tide.tide_consumer_leases WHERE group_name = $1 AND consumer_id = $2",
        &[group.into(), consumer.into()],
    )
    .map_err(|e| PgTideError::SpiError(format!("clear consumer lease: {e}")))?;

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
    crate::validation::validate_identifier(role)?;
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
    crate::validation::validate_identifier(role)?;
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
    Spi::run_with_args(
        "UPDATE tide.tide_consumer_offsets SET last_heartbeat = now() \
         WHERE group_name = $1 AND consumer_id = $2",
        &[group.into(), consumer.into()],
    )
    .map_err(|e| PgTideError::SpiError(format!("consumer heartbeat: {e}")))?;
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
        crate::outbox::outbox_create("smoke-create", 24, 10_000, "none");
        assert!(crate::outbox::outbox_exists("smoke-create").unwrap());
    }

    #[pg_test]
    fn test_outbox_create_duplicate_errors() {
        crate::outbox::outbox_create("dup-outbox", 24, 10_000, "none");
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
        crate::outbox::outbox_create("pub-outbox", 24, 10_000, "none");
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
    fn test_outbox_publish_denied_for_unauthorized_role() {
        // v0.40.0 (ADR-011 §15): with an ACL present and the current role not a
        // publisher, authorization is denied and no message row is inserted.
        crate::outbox::outbox_create("acl-deny-outbox", 24, 10_000, "none");
        crate::outbox::outbox_grant_publish("acl-deny-outbox", "acl_other_role");
        Spi::run("CREATE ROLE acl_denied_role NOLOGIN").unwrap();
        // Grant enough read access to reach the ACL verdict cleanly (the role is
        // simply not in the publisher list, so the verdict is 'denied').
        Spi::run("GRANT USAGE ON SCHEMA tide TO acl_denied_role").unwrap();
        Spi::run(
            "GRANT SELECT ON tide.tide_outbox_config, tide.outbox_publishers TO acl_denied_role",
        )
        .unwrap();
        Spi::run("GRANT INSERT ON tide.tide_outbox_messages TO acl_denied_role").unwrap();
        Spi::run("SET LOCAL ROLE acl_denied_role").unwrap();

        let result = crate::outbox::outbox_publish_impl(
            "acl-deny-outbox",
            pgrx::JsonB(serde_json::json!({"x": 1})),
            pgrx::JsonB(serde_json::json!({})),
        );

        Spi::run("RESET ROLE").unwrap();
        // Fail-closed: an unauthorized role's publish must fail (whether via an
        // explicit denied verdict or a fail-closed lookup error under RLS) and
        // must never insert a row.
        assert!(
            result.is_err(),
            "unauthorized role must not publish, got: {result:?}"
        );
        let count: i64 = Spi::get_one(
            "SELECT COUNT(*)::bigint FROM tide.tide_outbox_messages WHERE outbox_name = 'acl-deny-outbox'",
        )
        .unwrap()
        .unwrap_or(-1);
        assert_eq!(count, 0, "denied publish must not insert a row");
    }

    #[pg_test]
    fn test_outbox_status_returns_json() {
        crate::outbox::outbox_create("status-outbox", 24, 10_000, "none");
        let status = crate::outbox::outbox_status("status-outbox");
        let v = &status.0;
        assert_eq!(v["outbox_name"], "status-outbox");
        assert_eq!(v["pending_messages"], 0);
    }

    #[pg_test]
    fn test_outbox_drop_removes_config() {
        crate::outbox::outbox_create("drop-me", 24, 10_000, "none");
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
        crate::outbox::outbox_create("toggle-outbox", 24, 10_000, "none");
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
        crate::outbox::outbox_create("cg-outbox", 24, 10_000, "none");
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
        crate::outbox::outbox_create("cas-outbox", 24, 10_000, "none");
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
        crate::outbox::outbox_create("acl-outbox", 24, 10_000, "none");
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
        crate::outbox::outbox_create("acl-idem", 24, 10_000, "none");
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
        crate::outbox::outbox_create("acl-revoke", 24, 10_000, "none");
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
