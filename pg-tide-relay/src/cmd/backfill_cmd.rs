/// `pg-tide backfill` — managed backfill job management commands.
use pg_tide_relay::pg_tls;

/// Pause a running or pending backfill job.
pub async fn run_backfill_pause(
    url: &str,
    job_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (client, conn) = pg_tls::connect(url)
        .await
        .map_err(|e| format!("connection failed: {e}"))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    client
        .execute("SELECT tide.backfill_pause($1)", &[&job_name])
        .await
        .map_err(|e| format!("backfill pause failed: {e}"))?;

    println!("Backfill job '{}' paused.", job_name);
    Ok(())
}

/// Resume a paused backfill job.
pub async fn run_backfill_resume(
    url: &str,
    job_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (client, conn) = pg_tls::connect(url)
        .await
        .map_err(|e| format!("connection failed: {e}"))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    client
        .execute("SELECT tide.backfill_resume($1)", &[&job_name])
        .await
        .map_err(|e| format!("backfill resume failed: {e}"))?;

    println!("Backfill job '{}' resumed.", job_name);
    Ok(())
}

/// Cancel a backfill job (cannot be undone).
pub async fn run_backfill_cancel(
    url: &str,
    job_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (client, conn) = pg_tls::connect(url)
        .await
        .map_err(|e| format!("connection failed: {e}"))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    client
        .execute("SELECT tide.backfill_cancel($1)", &[&job_name])
        .await
        .map_err(|e| format!("backfill cancel failed: {e}"))?;

    println!("Backfill job '{}' cancelled.", job_name);
    Ok(())
}

/// Print the progress of all backfill jobs, or a specific job.
pub async fn run_backfill_status(
    url: &str,
    job_name: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (client, conn) = pg_tls::connect(url)
        .await
        .map_err(|e| format!("connection failed: {e}"))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    if let Some(name) = job_name {
        // Single job progress.
        let rows = client
            .query(
                "SELECT rows_processed, total_rows, \
                        pct_complete::float8, \
                        estimated_completion, status \
                 FROM tide.backfill_progress($1)",
                &[&name],
            )
            .await
            .unwrap_or_default();

        if rows.is_empty() {
            eprintln!("Backfill job '{}' not found.", name);
            std::process::exit(1);
        }

        println!("Backfill job: {}", name);
        for row in &rows {
            let rows_processed: i64 = row.get("rows_processed");
            let total_rows: i64 = row.get("total_rows");
            let pct: f64 = row.try_get::<_, f64>("pct_complete").unwrap_or(0.0);
            let status: String = row.get("status");
            let eta: Option<chrono::DateTime<chrono::Utc>> =
                row.try_get("estimated_completion").ok().flatten();

            println!("  Status:          {}", status);
            println!("  Rows processed:  {} / {}", rows_processed, total_rows);
            println!("  Progress:        {:.1}%", pct);
            if let Some(eta_time) = eta {
                println!(
                    "  ETA:             {}",
                    eta_time.format("%Y-%m-%dT%H:%M:%SZ")
                );
            }
        }
    } else {
        // Fleet summary.
        let row = client
            .query_one("SELECT tide.backfill_status(NULL)", &[])
            .await
            .map_err(|e| format!("query failed: {e}"))?;

        let json: serde_json::Value = row.get(0);
        let jobs = json
            .get("jobs")
            .and_then(|j| j.as_array())
            .cloned()
            .unwrap_or_default();

        if jobs.is_empty() {
            println!("No backfill jobs found.");
            return Ok(());
        }

        println!(
            "{:<30} {:<15} {:<12} {:<8} {:<10}",
            "JOB_NAME", "OUTBOX", "STATUS", "PCT", "ROWS"
        );
        println!("{}", "-".repeat(80));

        for job in &jobs {
            let job_name = job.get("job_name").and_then(|v| v.as_str()).unwrap_or("");
            let outbox = job
                .get("outbox_name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let status = job.get("status").and_then(|v| v.as_str()).unwrap_or("");
            let pct = job
                .get("pct_complete")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let rows = job
                .get("rows_processed")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);

            println!(
                "{:<30} {:<15} {:<12} {:>7.1}% {:<10}",
                job_name, outbox, status, pct, rows
            );
        }
    }

    Ok(())
}
