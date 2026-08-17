use crate::cli::OutputFormat;
/// `pg-tide status` — print a human-readable status table for all configured relay pipelines.
use pg_tide_relay::pg_tls;
use std::fmt;

#[derive(Debug)]
enum Value<T> {
    Known(T),
    Unknown,
    Stale,
}

impl<T: fmt::Display> fmt::Display for Value<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Known(value) => value.fmt(f),
            Self::Unknown => f.write_str("unknown"),
            Self::Stale => f.write_str("stale"),
        }
    }
}

/// Print a human-readable status table for all configured relay pipelines.
///
/// Columns:
///   PIPELINE | DIRECTION | ENABLED | LAST_OFFSET | CONSUMER_LAG
///
/// When `inbox_summary` is true, also calls `tide.inbox_status(NULL)` and
/// renders the fleet inbox summary table.
///
/// NOTE: `tide.inbox_status(NULL)` (fleet mode) executes a query proportional
/// to the number of configured inboxes and is intended for dashboards and
/// monitoring scripts, not for high-frequency application-level checks.
/// Without the `--inbox-summary` flag this call is omitted to keep default
/// output fast.
pub async fn run_status(
    url: &str,
    inbox_summary: bool,
    output_format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let (client, conn) = pg_tls::connect(url)
        .await
        .map_err(|e| format!("connection failed: {e}"))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let status_view_exists: bool = client
        .query_one(
            "SELECT to_regclass('tide.relay_pipeline_status') IS NOT NULL",
            &[],
        )
        .await?
        .get(0);
    let all_rows = if status_view_exists {
        client
            .query(
                "SELECT pipeline_id, direction, enabled, ownership, health,
                        consumer_lag, last_offset, last_checkpoint_success_at,
                        last_error_code, last_error_component, last_error_class,
                        last_error_at, retry_attempt, retry_state, next_retry_at,
                        unresolved_dlq_depth, last_state_update_at
                   FROM tide.relay_pipeline_status
                  ORDER BY pipeline_id, direction",
                &[],
            )
            .await?
    } else {
        compatibility_rows(&client).await?
    };

    if matches!(output_format, OutputFormat::Json) {
        let pipelines = all_rows
            .iter()
            .map(status_row_json)
            .collect::<Result<Vec<_>, _>>()?;
        crate::cmd::output::success(
            "status",
            serde_json::json!({ "pipelines": pipelines }),
            output_format,
        )?;
    } else {
        render_rows(&all_rows)?;
    }

    // v0.33.0: Optional inbox fleet summary.
    if inbox_summary && matches!(output_format, OutputFormat::Text) {
        print_inbox_fleet_summary(&client).await;
    }

    Ok(())
}

fn status_row_json(row: &tokio_postgres::Row) -> Result<serde_json::Value, tokio_postgres::Error> {
    let value_or_unknown = |value: Option<serde_json::Value>| {
        value.unwrap_or_else(|| serde_json::Value::String("unknown".to_string()))
    };
    let checkpoint =
        row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>("last_checkpoint_success_at")?;
    let checkpoint_value = checkpoint.map_or_else(
        || serde_json::Value::String("unknown".to_string()),
        |at| {
            if chrono::Utc::now() - at > chrono::Duration::minutes(5) {
                serde_json::Value::String("stale".to_string())
            } else {
                serde_json::Value::String(at.to_rfc3339())
            }
        },
    );
    let retry_attempt = row.try_get::<_, Option<i32>>("retry_attempt")?;
    let retry = retry_attempt.map(serde_json::Value::from);
    let retry = if retry.is_some()
        && row
            .try_get::<_, Option<chrono::DateTime<chrono::Utc>>>("last_state_update_at")?
            .is_some_and(|at| chrono::Utc::now() - at > chrono::Duration::minutes(5))
    {
        serde_json::Value::String("stale".to_string())
    } else {
        value_or_unknown(retry)
    };
    let pipeline_id: String = row.try_get("pipeline_id")?;
    let direction: String = row.try_get("direction")?;
    let enabled: bool = row.try_get("enabled")?;
    let ownership = row.try_get::<_, Option<String>>("ownership")?;
    let health = row.try_get::<_, Option<String>>("health")?;
    let consumer_lag = row.try_get::<_, Option<i64>>("consumer_lag")?;
    let last_offset = row.try_get::<_, Option<i64>>("last_offset")?;
    let last_error_code = row.try_get::<_, Option<String>>("last_error_code")?;
    let last_error_at = row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>("last_error_at")?;
    let retry_state = row.try_get::<_, Option<String>>("retry_state")?;
    let unresolved_dlq_depth = row.try_get::<_, Option<i64>>("unresolved_dlq_depth")?;
    Ok(serde_json::json!({
        "pipeline_id": pipeline_id,
        "direction": direction,
        "enabled": enabled,
        "ownership": value_or_unknown(ownership.map(serde_json::Value::String)),
        "health": value_or_unknown(health.map(serde_json::Value::String)),
        "consumer_lag": value_or_unknown(consumer_lag.map(serde_json::Value::from)),
        "last_offset": value_or_unknown(last_offset.map(serde_json::Value::from)),
        "last_checkpoint_success_at": checkpoint_value,
        "last_error_code": value_or_unknown(last_error_code.map(serde_json::Value::String)),
        "last_error_at": value_or_unknown(last_error_at.map(|at| serde_json::Value::String(at.to_rfc3339()))),
        "retry_attempt": retry,
        "retry_state": value_or_unknown(retry_state.map(serde_json::Value::String)),
        "unresolved_dlq_depth": value_or_unknown(unresolved_dlq_depth.map(serde_json::Value::from)),
    }))
}

async fn compatibility_rows(
    client: &tokio_postgres::Client,
) -> Result<Vec<tokio_postgres::Row>, tokio_postgres::Error> {
    let state_exists: bool = client
        .query_one(
            "SELECT to_regclass('tide.relay_pipeline_state') IS NOT NULL",
            &[],
        )
        .await?
        .get(0);
    let dlq_exists: bool = client
        .query_one("SELECT to_regclass('tide.relay_dlq') IS NOT NULL", &[])
        .await?
        .get(0);
    let dlq_forward = if dlq_exists {
        "(SELECT COUNT(*)::bigint FROM tide.relay_dlq d
           WHERE d.pipeline_name = roc.name AND d.resolved = false)"
    } else {
        "NULL::bigint"
    };
    let dlq_reverse = if dlq_exists {
        "(SELECT COUNT(*)::bigint FROM tide.relay_dlq d
           WHERE d.pipeline_name = ric.name AND d.resolved = false)"
    } else {
        "NULL::bigint"
    };
    let state_join = if state_exists {
        "LEFT JOIN tide.relay_pipeline_state s ON s.name = roc.name"
    } else {
        "LEFT JOIN (SELECT NULL::text AS name, NULL::int AS failure_count,
                          NULL::text AS last_error, NULL::text AS error_class,
                          NULL::timestamptz AS updated_at) s ON false"
    };
    let state_join_reverse = if state_exists {
        "LEFT JOIN tide.relay_pipeline_state s ON s.name = ric.name"
    } else {
        "LEFT JOIN (SELECT NULL::text AS name, NULL::int AS failure_count,
                          NULL::text AS last_error, NULL::text AS error_class,
                          NULL::timestamptz AS updated_at) s ON false"
    };
    client
        .query(
            &format!(
                "SELECT name::text AS pipeline_id, 'forward'::text AS direction, enabled,
                CASE WHEN NOT enabled THEN 'unowned' ELSE 'unknown' END AS ownership,
                CASE WHEN NOT enabled THEN 'disabled' ELSE 'unknown' END AS health,
                NULL::bigint AS consumer_lag, o.last_change_id AS last_offset,
                NULL::timestamptz AS last_checkpoint_success_at,
                CASE WHEN s.last_error IS NULL THEN NULL ELSE 'error_present' END AS last_error_code,
                NULL::text AS last_error_component, s.error_class AS last_error_class,
                CASE WHEN s.last_error IS NULL THEN NULL ELSE s.updated_at END AS last_error_at,
                s.failure_count AS retry_attempt,
                CASE WHEN s.failure_count > 0 THEN 'retrying' ELSE NULL END AS retry_state,
                NULL::timestamptz AS next_retry_at,
                {dlq_forward} AS unresolved_dlq_depth,
                s.updated_at AS last_state_update_at
           FROM tide.relay_outbox_config roc
           {state_join}
           LEFT JOIN LATERAL (
             SELECT last_change_id FROM tide.relay_consumer_offsets
              WHERE pipeline_id = roc.name
              ORDER BY updated_at DESC NULLS LAST LIMIT 1
           ) o ON true
         UNION ALL
         SELECT name::text, 'reverse'::text, enabled,
                CASE WHEN NOT enabled THEN 'unowned' ELSE 'unknown' END,
                CASE WHEN NOT enabled THEN 'disabled' ELSE 'unknown' END,
                NULL::bigint, o.last_change_id, NULL::timestamptz,
                CASE WHEN s.last_error IS NULL THEN NULL ELSE 'error_present' END,
                NULL::text, s.error_class,
                CASE WHEN s.last_error IS NULL THEN NULL ELSE s.updated_at END,
                s.failure_count,
                CASE WHEN s.failure_count > 0 THEN 'retrying' ELSE NULL END,
                NULL::timestamptz, {dlq_reverse}, s.updated_at
           FROM tide.relay_inbox_config ric
           {state_join_reverse}
           LEFT JOIN LATERAL (
             SELECT last_change_id FROM tide.relay_consumer_offsets
              WHERE pipeline_id = ric.name
              ORDER BY updated_at DESC NULLS LAST LIMIT 1
           ) o ON true
         ORDER BY name, direction"
            ),
            &[],
        )
        .await
}

fn render_rows(all_rows: &[tokio_postgres::Row]) -> Result<(), Box<dyn std::error::Error>> {
    if all_rows.is_empty() {
        println!("No pipelines configured.");
    } else {
        // Print header.
        println!(
            "{:<30} {:<10} {:<8} {:<14} {:<14}",
            "PIPELINE", "DIRECTION", "ENABLED", "OFFSET", "LAG"
        );
        println!("{}", "-".repeat(80));

        for row in all_rows {
            let name: String = row.try_get("pipeline_id")?;
            let direction: String = row.try_get("direction")?;
            let enabled: bool = row.try_get("enabled")?;
            let last_offset = row
                .try_get::<_, Option<i64>>("last_offset")
                .ok()
                .flatten()
                .map_or(Value::Unknown, Value::Known);
            let lag = if direction == "forward" {
                row.try_get::<_, Option<i64>>("consumer_lag")
                    .ok()
                    .flatten()
                    .map_or(Value::Unknown, Value::Known)
            } else {
                Value::Unknown
            };
            let retry = row
                .try_get::<_, Option<i32>>("retry_attempt")
                .ok()
                .flatten()
                .map_or(Value::Unknown, |n| {
                    let stale = row
                        .try_get::<_, Option<chrono::DateTime<chrono::Utc>>>("last_state_update_at")
                        .ok()
                        .flatten()
                        .map(|at| chrono::Utc::now() - at > chrono::Duration::minutes(5))
                        .unwrap_or(false);
                    if stale {
                        Value::Stale
                    } else {
                        Value::Known(n)
                    }
                });
            let error = row
                .try_get::<_, Option<String>>("last_error_code")
                .ok()
                .flatten()
                .map_or("none".to_string(), |_| "present".to_string());
            let ownership = row
                .try_get::<_, Option<String>>("ownership")
                .ok()
                .flatten()
                .map_or(Value::Unknown, Value::Known);
            let health = row
                .try_get::<_, Option<String>>("health")
                .ok()
                .flatten()
                .map_or(Value::Unknown, Value::Known);
            let success = row
                .try_get::<_, Option<chrono::DateTime<chrono::Utc>>>("last_checkpoint_success_at")
                .ok()
                .flatten()
                .map_or(Value::Unknown, |at| {
                    if chrono::Utc::now() - at > chrono::Duration::minutes(5) {
                        Value::Stale
                    } else {
                        Value::Known(at.to_rfc3339())
                    }
                });
            let error_at = row
                .try_get::<_, Option<chrono::DateTime<chrono::Utc>>>("last_error_at")
                .ok()
                .flatten()
                .map_or(Value::Unknown, |at| Value::Known(at.to_rfc3339()));
            let dlq = row
                .try_get::<_, Option<i64>>("unresolved_dlq_depth")
                .ok()
                .flatten()
                .map_or(Value::Unknown, Value::Known);

            println!(
                "{:<30} {:<10} {:<8} {:<14} {:<14}",
                name,
                direction,
                if enabled { "yes" } else { "no" },
                last_offset,
                lag,
            );
            println!(
                "  ownership={} health={} success={} error={} error_at={} retry={} dlq={}",
                ownership, health, success, error, error_at, retry, dlq
            );
        }

        println!("\n{} pipeline(s) configured.", all_rows.len());
    }

    Ok(())
}

/// Print the inbox fleet summary table from `tide.inbox_status(NULL)`.
async fn print_inbox_fleet_summary(client: &tokio_postgres::Client) {
    // Check whether the function exists (requires v0.14.0+).
    let has_fleet_fn: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.routines \
             WHERE routine_schema = 'tide' AND routine_name = 'inbox_status')",
            &[],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or(false);

    if !has_fleet_fn {
        println!("\n[inbox fleet] tide.inbox_status() not available (requires v0.14.0+)");
        return;
    }

    let rows = client
        .query("SELECT * FROM tide.inbox_status(NULL)", &[])
        .await;

    match rows {
        Err(e) => {
            println!("\n[inbox fleet] Error: {e}");
        }
        Ok(rows) if rows.is_empty() => {
            println!("\nNo inboxes configured.");
        }
        Ok(rows) => {
            println!(
                "\n{:<30} {:<12} {:<12} {:<28}",
                "INBOX", "TOTAL", "PENDING", "LAST_PROCESSED"
            );
            println!("{}", "-".repeat(86));
            for row in &rows {
                let name: String = row.try_get("inbox_name").unwrap_or_default();
                let total: i64 = row.try_get("total_messages").unwrap_or(0);
                let pending: i64 = row.try_get("pending_messages").unwrap_or(0);
                let last_processed: String = row
                    .try_get::<_, Option<chrono::DateTime<chrono::Utc>>>("last_processed_at")
                    .ok()
                    .flatten()
                    .map(|t| t.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                    .unwrap_or_else(|| "—".to_string());
                println!(
                    "{:<30} {:<12} {:<12} {:<28}",
                    name, total, pending, last_processed
                );
            }
            println!("\n{} inbox(es) in fleet summary.", rows.len());
        }
    }
}
