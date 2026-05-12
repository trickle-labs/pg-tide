/// `pg-tide status` — print a human-readable status table for all configured relay pipelines.
use pg_tide_relay::pg_tls;

/// Print a human-readable status table for all configured relay pipelines.
///
/// Columns:
///   PIPELINE | DIRECTION | ENABLED | LAST_OFFSET | CONSUMER_LAG
pub async fn run_status(url: &str) -> Result<(), Box<dyn std::error::Error>> {
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
        return Ok(());
    }

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
    Ok(())
}
