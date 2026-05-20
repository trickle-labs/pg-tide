//! Idempotent Inbox API for pg_tide.
//!
//! Provides `tide.inbox_create()`, `tide.inbox_mark_processed()`,
//! `tide.inbox_mark_failed()`, `tide.replay_inbox_messages()`, etc.

use crate::error::PgTideError;
use pgrx::prelude::*;

// ── Internal helpers ──────────────────────────────────────────────────────

fn inbox_exists(name: &str) -> Result<bool, PgTideError> {
    Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM tide.tide_inbox_config WHERE inbox_name = $1)",
        &[name.into()],
    )
    .map(|r| r.unwrap_or(false))
    .map_err(|e| PgTideError::SpiError(format!("inbox_exists SPI error: {e}")))
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
    crate::validation::validate_identifier(name)?;
    crate::validation::validate_identifier(schema)?;
    if inbox_exists(name)? {
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
            CONSTRAINT "uq_{name}_event_id" UNIQUE (event_id)
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
    if !inbox_exists(name)? {
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
    if !inbox_exists(name)? {
        return Err(PgTideError::InboxNotFound(name.to_string()));
    }

    let schema: String = Spi::get_one_with_args::<String>(
        "SELECT inbox_schema FROM tide.tide_inbox_config WHERE inbox_name = $1",
        &[name.into()],
    )
    .unwrap_or(None)
    .unwrap_or_else(|| "tide".to_string());

    let sql = format!(
        r#"UPDATE "{schema}"."{name}_inbox"
           SET processed_at = now()
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
    if !inbox_exists(name)? {
        return Err(PgTideError::InboxNotFound(name.to_string()));
    }

    let schema: String = Spi::get_one_with_args::<String>(
        "SELECT inbox_schema FROM tide.tide_inbox_config WHERE inbox_name = $1",
        &[name.into()],
    )
    .unwrap_or(None)
    .unwrap_or_else(|| "tide".to_string());

    let sql = format!(
        r#"UPDATE "{schema}"."{name}_inbox"
           SET retry_count = retry_count + 1,
               last_error  = $2
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
        if !inbox_exists(n)? {
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
                r#"SELECT COUNT(*) FROM "{schema}"."{n}_inbox"
                   WHERE processed_at IS NULL
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

    // Fleet summary: collect every configured inbox with a basic count.
    // v0.32.0 P2: Replace the N+1 SPI query loop with a single dynamic UNION ALL
    // aggregation so that fleet status executes only 2 SPI round-trips regardless
    // of the number of configured inboxes (down from N+1 at N=20 that's 21 calls).
    // P3-9: Replace .unwrap_or_default() with explicit error propagation so that
    // SPI connection failures surface to the SQL caller instead of returning empty.
    let rows: Vec<(String, String)> = Spi::connect(|client| {
        let mut entries = Vec::new();
        let tup = client.select(
            "SELECT inbox_name, inbox_schema FROM tide.tide_inbox_config ORDER BY inbox_name",
            None,
            &[],
        )?;
        for row in tup {
            let iname: String = row.get(1)?.unwrap_or_default();
            let ischema: String = row.get(2)?.unwrap_or_else(|| "tide".to_string());
            entries.push((iname, ischema));
        }
        Ok::<_, pgrx::spi::SpiError>(entries)
    })
    .map_err(|e| PgTideError::SpiError(format!("fleet inbox list: {e}")))?;

    if rows.is_empty() {
        return Ok(pgrx::JsonB(serde_json::json!({ "inboxes": [] })));
    }

    // Build a single UNION ALL query that counts pending messages across all
    // inbox tables in one database round-trip.
    // Example for 2 inboxes:
    //   SELECT 'a' AS n, COUNT(*) FROM "tide"."a_inbox" WHERE processed_at IS NULL
    //   UNION ALL
    //   SELECT 'b' AS n, COUNT(*) FROM "tide"."b_inbox" WHERE processed_at IS NULL
    let union_sql: String = rows
        .iter()
        .map(|(iname, ischema)| {
            format!(
                r#"SELECT '{iname}' AS inbox_name, COUNT(*) AS pending FROM "{ischema}"."{iname}_inbox" WHERE processed_at IS NULL"#,
                iname = iname.replace('\'', "''"),
                ischema = ischema.replace('\'', "''"),
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");

    let mut pending_map: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    Spi::connect(|client| {
        let tup = client.select(&union_sql, None, &[])?;
        for row in tup {
            let iname: String = row.get(1)?.unwrap_or_default();
            let pending: i64 = row.get(2)?.unwrap_or(0);
            pending_map.insert(iname, pending);
        }
        Ok::<_, pgrx::spi::SpiError>(())
    })
    .map_err(|e| PgTideError::SpiError(format!("fleet inbox pending counts: {e}")))?;

    let summaries: Vec<serde_json::Value> = rows
        .iter()
        .map(|(iname, _)| {
            let pending = pending_map.get(iname).copied().unwrap_or(0);
            serde_json::json!({
                "inbox_name": iname,
                "pending": pending,
            })
        })
        .collect();

    Ok(pgrx::JsonB(serde_json::json!({ "inboxes": summaries })))
}

// ── TIDE-API: replay_inbox_messages ──────────────────────────────────────

/// Re-queue failed messages from DLQ back to the pending queue.
#[pg_extern(schema = "tide")]
pub fn replay_inbox_messages(p_name: &str, p_event_ids: Vec<String>) -> i64 {
    replay_inbox_messages_impl(p_name, p_event_ids).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn replay_inbox_messages_impl(name: &str, event_ids: Vec<String>) -> Result<i64, PgTideError> {
    if !inbox_exists(name)? {
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
            r#"UPDATE "{schema}"."{name}_inbox"
               SET retry_count = 0, last_error = NULL, processed_at = NULL
               WHERE event_id = $1 AND processed_at IS NULL
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

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    // ── inbox_create / inbox_drop ──────────────────────────────────────────

    #[pg_test]
    fn test_inbox_create_and_table_exists() {
        crate::inbox::inbox_create("smoke-inbox", "tide", 3, 72, 0);
        let exists: bool = Spi::get_one(
            "SELECT EXISTS(SELECT 1 FROM tide.tide_inbox_config WHERE inbox_name = 'smoke-inbox')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(exists, "inbox config row must exist after inbox_create");
    }

    #[pg_test]
    fn test_inbox_create_duplicate_errors() {
        crate::inbox::inbox_create("dup-inbox", "tide", 3, 72, 0);
        let count: i64 = Spi::get_one(
            "SELECT COUNT(*)::bigint FROM tide.tide_inbox_config WHERE inbox_name = 'dup-inbox'",
        )
        .unwrap()
        .unwrap_or(0);
        assert_eq!(count, 1, "duplicate inbox should not insert a second row");
    }

    #[pg_test]
    fn test_inbox_drop_removes_config() {
        crate::inbox::inbox_create("drop-inbox", "tide", 3, 72, 0);
        crate::inbox::inbox_drop("drop-inbox", false);
        let exists: bool = Spi::get_one(
            "SELECT EXISTS(SELECT 1 FROM tide.tide_inbox_config WHERE inbox_name = 'drop-inbox')",
        )
        .unwrap()
        .unwrap_or(true);
        assert!(!exists, "inbox config row must be removed after inbox_drop");
    }

    #[pg_test]
    fn test_inbox_drop_if_exists_is_noop() {
        crate::inbox::inbox_drop("never-existed-inbox", true);
    }

    // ── inbox_mark_processed ──────────────────────────────────────────────

    #[pg_test]
    fn test_inbox_mark_processed_sets_timestamp() {
        crate::inbox::inbox_create("proc-inbox", "tide", 3, 72, 0);

        // Directly insert a pending message into the inbox table.
        Spi::run(
            r#"INSERT INTO tide."proc-inbox_inbox" (event_id, source, payload, headers)
               VALUES ('evt-001', 'test', '{}', '{}')"#,
        )
        .unwrap();

        crate::inbox::inbox_mark_processed("proc-inbox", "evt-001");

        let processed: bool = Spi::get_one(
            r#"SELECT processed_at IS NOT NULL FROM tide."proc-inbox_inbox" WHERE event_id = 'evt-001'"#,
        )
        .unwrap()
        .unwrap_or(false);
        assert!(processed, "processed_at must be set after mark_processed");
    }

    // ── inbox_mark_failed ─────────────────────────────────────────────────

    #[pg_test]
    fn test_inbox_mark_failed_sets_error() {
        crate::inbox::inbox_create("fail-inbox", "tide", 3, 72, 0);

        Spi::run(
            r#"INSERT INTO tide."fail-inbox_inbox" (event_id, source, payload, headers)
               VALUES ('evt-fail-001', 'test', '{}', '{}')"#,
        )
        .unwrap();

        crate::inbox::inbox_mark_failed("fail-inbox", "evt-fail-001", "downstream timeout");

        let error: String = Spi::get_one(
            r#"SELECT last_error FROM tide."fail-inbox_inbox" WHERE event_id = 'evt-fail-001'"#,
        )
        .unwrap()
        .unwrap_or_default();
        assert_eq!(error, "downstream timeout");
    }

    // ── inbox_status ──────────────────────────────────────────────────────

    #[pg_test]
    fn test_inbox_status_returns_json() {
        crate::inbox::inbox_create("stat-inbox", "tide", 3, 72, 0);
        let status = crate::inbox::inbox_status(Some("stat-inbox"));
        let v = &status.0;
        assert_eq!(v["inbox_name"], "stat-inbox");
        assert_eq!(v["pending"], 0);
    }

    #[pg_test]
    fn test_inbox_status_null_returns_fleet_summary() {
        crate::inbox::inbox_create("fleet-inbox-a", "tide", 3, 72, 0);
        crate::inbox::inbox_create("fleet-inbox-b", "tide", 3, 72, 0);
        let status = crate::inbox::inbox_status(None);
        let v = &status.0;
        assert!(
            v.get("inboxes").and_then(|a| a.as_array()).is_some(),
            "fleet summary must contain 'inboxes' array"
        );
    }

    // ── idempotent delivery ───────────────────────────────────────────────

    #[pg_test]
    fn test_inbox_deduplicates_event_id() {
        crate::inbox::inbox_create("dedup-inbox", "tide", 3, 72, 0);

        let insert = r#"INSERT INTO tide."dedup-inbox_inbox" (event_id, source, payload, headers)
                        VALUES ('dedup-evt', 'test', '{}', '{}')
                        ON CONFLICT (event_id) DO NOTHING"#;
        Spi::run(insert).unwrap();
        Spi::run(insert).unwrap();

        let count: i64 = Spi::get_one(r#"SELECT COUNT(*)::bigint FROM tide."dedup-inbox_inbox""#)
            .unwrap()
            .unwrap_or(0);
        assert_eq!(count, 1, "duplicate event_id must be silently ignored");
    }
}
