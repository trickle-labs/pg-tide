/// `pg-tide status` — print a human-readable status table for all configured relay pipelines.
use pg_tide_relay::pg_tls;

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

    // Forward pipelines.
    let forward_rows = client
        .query(
            "SELECT
                roc.name,
                'forward'::text AS direction,
                roc.enabled,
                COALESCE(rco.last_change_id, 0) AS last_offset,
                (SELECT COUNT(*) FROM tide.tide_outbox_messages tom
                 WHERE tom.outbox_name = (roc.config->>'source' ->> 'outbox')
                   AND tom.consumed_at IS NULL) AS consumer_lag
             FROM tide.relay_outbox_config roc
             LEFT JOIN tide.relay_consumer_offsets rco
               ON rco.pipeline_id = roc.name
             ORDER BY roc.name",
            &[],
        )
        .await
        .unwrap_or_default();

    // Reverse pipelines.
    let reverse_rows = client
        .query(
            "SELECT
                ric.name,
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
        .await
        .unwrap_or_default();

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
            let consumer_lag: i64 = row.try_get("consumer_lag").unwrap_or(0);

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
