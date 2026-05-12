/// `pg-tide sweep` — delete consumed outbox messages past their retention window.
use pg_tide_relay::pg_tls;

/// Delete consumed outbox messages past their retention window.
///
/// Calls `tide.outbox_truncate_delivered()` for each configured outbox (or
/// just the one named by `outbox_name`).  Run on a schedule via cron or a
/// Kubernetes CronJob to prevent unbounded growth of the outbox message table.
pub async fn run_sweep(
    url: &str,
    outbox_name: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("pg-tide sweep v{}", env!("CARGO_PKG_VERSION"));

    let (client, conn) = pg_tls::connect(url)
        .await
        .map_err(|e| format!("connection failed: {e}"))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let outboxes: Vec<String> = if let Some(name) = outbox_name {
        vec![name.to_string()]
    } else {
        client
            .query(
                "SELECT outbox_name FROM tide.tide_outbox_config ORDER BY outbox_name",
                &[],
            )
            .await?
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect()
    };

    if outboxes.is_empty() {
        println!("  [INFO] No outboxes configured — nothing to sweep.");
        return Ok(());
    }

    let mut total_deleted: i64 = 0;
    for name in &outboxes {
        let deleted: i64 = client
            .query_one("SELECT tide.outbox_truncate_delivered($1)", &[name])
            .await
            .map(|r| r.get::<_, i64>(0))
            .unwrap_or(0);
        println!("  [OK] Swept outbox '{name}': {deleted} rows deleted");
        total_deleted += deleted;
    }

    println!(
        "\npg-tide sweep: {total_deleted} total row(s) deleted from {} outbox(es).",
        outboxes.len()
    );
    Ok(())
}
