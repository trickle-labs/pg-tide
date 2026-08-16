/// `pg-tide status` — print a human-readable status table for all configured relay pipelines.
use pg_tide_relay::pg_tls;
use std::collections::HashMap;

#[derive(Debug, Default)]
struct PipelineLag {
    outbox_name: String,
    lag: i64,
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
pub async fn run_status(url: &str, inbox_summary: bool) -> Result<(), Box<dyn std::error::Error>> {
    let (client, conn) = pg_tls::connect(url)
        .await
        .map_err(|e| format!("connection failed: {e}"))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let lag_by_pipeline = load_pipeline_lags(&client).await?;

    // Forward pipelines. Lag comes from relay_pipeline_lag when available and
    // falls back to the same exact `id > last_change_id` predicate.
    let forward_rows = client
        .query(
            "SELECT
                roc.name::text AS name,
                'forward'::text AS direction,
                roc.enabled,
                (roc.config->'source'->>'outbox')::text AS outbox_name,
                COALESCE(rco.last_change_id, 0) AS last_offset
             FROM tide.relay_outbox_config roc
             LEFT JOIN LATERAL (
                 SELECT last_change_id
                   FROM tide.relay_consumer_offsets
                  WHERE pipeline_id = roc.name
                    AND outbox_name = (roc.config->'source'->>'outbox')
                  ORDER BY updated_at DESC NULLS LAST
                  LIMIT 1
             ) rco ON true
             ORDER BY roc.name",
            &[],
        )
        .await?;

    // Reverse pipelines.
    let reverse_rows = client
        .query(
            "SELECT
                ric.name::text AS name,
                'reverse'::text AS direction,
                ric.enabled,
                COALESCE(rco.last_change_id, 0) AS last_offset,
                0::bigint AS consumer_lag
             FROM tide.relay_inbox_config ric
             LEFT JOIN tide.relay_consumer_offsets rco
               ON rco.pipeline_id = ric.name
             ORDER BY ric.name",
            &[],
        )
        .await?;

    let all_rows: Vec<_> = forward_rows.iter().chain(reverse_rows.iter()).collect();

    if all_rows.is_empty() {
        println!("No pipelines configured.");
    } else {
        // Print header.
        println!(
            "{:<30} {:<10} {:<8} {:<14} {:<14}",
            "PIPELINE", "DIRECTION", "ENABLED", "LAST_OFFSET", "CONSUMER_LAG"
        );
        println!("{}", "-".repeat(80));

        for row in &all_rows {
            let name: String = row.get("name");
            let direction: String = row.get("direction");
            let enabled: bool = row.get("enabled");
            let last_offset: i64 = row.try_get("last_offset").unwrap_or(0);
            let consumer_lag = if direction == "forward" {
                let outbox_name: Option<String> = row.try_get("outbox_name").ok();
                match outbox_name {
                    Some(outbox) => match lag_by_pipeline.get(&name) {
                        Some(lag) if lag.outbox_name == outbox => lag.lag,
                        _ => exact_lag(&client, &outbox, last_offset).await?,
                    },
                    None => 0,
                }
            } else {
                0
            };

            println!(
                "{:<30} {:<10} {:<8} {:<14} {:<14}",
                name,
                direction,
                if enabled { "yes" } else { "no" },
                last_offset,
                consumer_lag,
            );
        }

        println!("\n{} pipeline(s) configured.", all_rows.len());
    }

    // v0.33.0: Optional inbox fleet summary.
    // NOTE: This query scales with the number of configured inboxes (O(n)).
    // Use on dashboards and monitoring scripts; avoid in tight polling loops.
    if inbox_summary {
        print_inbox_fleet_summary(&client).await;
    }

    Ok(())
}

async fn load_pipeline_lags(
    client: &tokio_postgres::Client,
) -> Result<HashMap<String, PipelineLag>, Box<dyn std::error::Error>> {
    let exists: bool = client
        .query_one(
            "SELECT to_regclass('tide.relay_pipeline_lag') IS NOT NULL",
            &[],
        )
        .await?
        .get(0);
    if !exists {
        return Ok(HashMap::new());
    }

    let mut lags = HashMap::new();
    for row in client
        .query(
            "SELECT DISTINCT ON (pipeline_id, outbox_name)
                    pipeline_id, outbox_name, lag
               FROM tide.relay_pipeline_lag
              ORDER BY pipeline_id, outbox_name, updated_at DESC NULLS LAST,
                       relay_group_id DESC",
            &[],
        )
        .await?
    {
        let pipeline = row
            .try_get::<_, String>("pipeline_id")
            .or_else(|_| row.try_get::<_, String>("pipeline_name"));
        let outbox = row.try_get::<_, String>("outbox_name");
        let lag = row
            .try_get::<_, i64>("lag")
            .or_else(|_| row.try_get::<_, i64>("lag_count"))
            .or_else(|_| row.try_get::<_, i64>("consumer_lag"));
        if let (Ok(pipeline), Ok(outbox), Ok(lag)) = (pipeline, outbox, lag) {
            lags.insert(
                pipeline,
                PipelineLag {
                    outbox_name: outbox,
                    lag,
                },
            );
        }
    }
    Ok(lags)
}

async fn exact_lag(
    client: &tokio_postgres::Client,
    outbox_name: &str,
    last_offset: i64,
) -> Result<i64, Box<dyn std::error::Error>> {
    Ok(client
        .query_one(
            "SELECT COUNT(*)::bigint
               FROM tide.tide_outbox_messages
              WHERE outbox_name = $1 AND id > $2",
            &[&outbox_name, &last_offset],
        )
        .await?
        .get(0))
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
