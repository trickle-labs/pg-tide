/// `pg-tide replay` — preview outbox ranges and resolve DLQ entries.
use pg_tide_relay::pg_tls;

use crate::cli::ReplayCommands;

/// Dispatch replay workbench subcommands.
pub async fn run_replay_command(
    cmd: ReplayCommands,
    default_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match cmd {
        ReplayCommands::Preview {
            outbox,
            from_id,
            to_id,
            limit,
            postgres_url,
        } => {
            let url = postgres_url
                .or_else(|| std::env::var("PG_TIDE_POSTGRES_URL").ok())
                .unwrap_or_else(|| default_url.to_string());
            if url.is_empty() {
                eprintln!("error: --postgres-url is required for replay preview");
                std::process::exit(1);
            }
            run_replay_preview(&url, &outbox, from_id, to_id, limit).await
        }
        ReplayCommands::DlqResolve {
            pipeline,
            dedup_key,
            postgres_url,
        } => {
            let url = postgres_url
                .or_else(|| std::env::var("PG_TIDE_POSTGRES_URL").ok())
                .unwrap_or_else(|| default_url.to_string());
            if url.is_empty() {
                eprintln!("error: --postgres-url is required for dlq-resolve");
                std::process::exit(1);
            }
            run_dlq_resolve(&url, &pipeline, &dedup_key).await
        }
        ReplayCommands::DlqRequeue {
            pipeline,
            dedup_key,
            postgres_url,
        } => {
            let url = postgres_url
                .or_else(|| std::env::var("PG_TIDE_POSTGRES_URL").ok())
                .unwrap_or_else(|| default_url.to_string());
            if url.is_empty() {
                eprintln!("error: --postgres-url is required for dlq-requeue");
                std::process::exit(1);
            }
            run_dlq_requeue(&url, &pipeline, &dedup_key).await
        }
    }
}

/// `pg-tide replay preview` — print outbox messages in an ID range as JSONL.
async fn run_replay_preview(
    url: &str,
    outbox: &str,
    from_id: i64,
    to_id: i64,
    limit: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let (client, conn) = pg_tls::connect(url).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let rows = client
        .query(
            "SELECT id, outbox_name, payload, headers, created_at, consumed_at IS NOT NULL AS consumed
             FROM tide.tide_outbox_messages
             WHERE outbox_name = $1 AND id BETWEEN $2 AND $3
             ORDER BY id
             LIMIT $4",
            &[&outbox, &from_id, &to_id, &(limit as i64)],
        )
        .await?;

    for row in &rows {
        let id: i64 = row.get("id");
        let outbox_name: String = row.get("outbox_name");
        let payload: serde_json::Value = row.get("payload");
        let headers: serde_json::Value = row.get("headers");
        let created_at: chrono::DateTime<chrono::Utc> = row.get("created_at");
        let consumed: bool = row.get("consumed");
        let line = serde_json::json!({
            "id": id,
            "outbox_name": outbox_name,
            "payload": payload,
            "headers": headers,
            "created_at": created_at.to_rfc3339(),
            "consumed": consumed,
        });
        println!("{}", serde_json::to_string(&line)?);
    }

    eprintln!("{} message(s) previewed from outbox '{outbox}'", rows.len());
    Ok(())
}

/// `pg-tide replay dlq-resolve` — mark a DLQ entry as resolved.
async fn run_dlq_resolve(
    url: &str,
    pipeline: &str,
    dedup_key: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (client, conn) = pg_tls::connect(url).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let updated = client
        .execute(
            "UPDATE tide.relay_dlq
             SET resolved = true, resolved_at = now(), resolved_by = current_user
             WHERE pipeline_name = $1 AND dedup_key = $2 AND resolved = false",
            &[&pipeline, &dedup_key],
        )
        .await?;

    if updated == 0 {
        eprintln!(
            "warning: no active DLQ entry found for pipeline='{}' dedup_key='{}'",
            pipeline, dedup_key
        );
    } else {
        println!(
            "DLQ entry resolved: pipeline='{}' dedup_key='{}'",
            pipeline, dedup_key
        );
    }
    Ok(())
}

/// `pg-tide replay dlq-requeue` — requeue a DLQ entry for another attempt.
async fn run_dlq_requeue(
    url: &str,
    pipeline: &str,
    dedup_key: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (client, conn) = pg_tls::connect(url).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    // Mark the existing entry resolved.
    let updated = client
        .execute(
            "UPDATE tide.relay_dlq
             SET resolved = true, resolved_at = now(), resolved_by = current_user || ' (requeue)',
                 attempt_count = 0
             WHERE pipeline_name = $1 AND dedup_key = $2 AND resolved = false",
            &[&pipeline, &dedup_key],
        )
        .await?;

    if updated == 0 {
        eprintln!(
            "warning: no active DLQ entry found for pipeline='{}' dedup_key='{}'",
            pipeline, dedup_key
        );
        return Ok(());
    }

    // Re-insert as a fresh pending entry.
    client
        .execute(
            "INSERT INTO tide.relay_dlq (pipeline_name, dedup_key, payload, attempt_count, resolved)
             SELECT pipeline_name, dedup_key, payload, 0, false
             FROM tide.relay_dlq
             WHERE pipeline_name = $1 AND dedup_key = $2
               AND resolved = true
             ORDER BY id DESC
             LIMIT 1",
            &[&pipeline, &dedup_key],
        )
        .await?;

    println!(
        "DLQ entry requeued: pipeline='{}' dedup_key='{}'",
        pipeline, dedup_key
    );
    Ok(())
}
