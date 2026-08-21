/// `pg-tide replay` — preview outbox ranges and resolve DLQ entries.
use pg_tide_relay::{coordinator, pg_tls};

use crate::cli::ReplayCommands;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReplayRange {
    from_id: i64,
    to_id: i64,
    batch_size: i64,
}

fn validate_replay_range(from_id: i64, to_id: i64, batch_size: i64) -> Result<ReplayRange, String> {
    if from_id < 0 {
        return Err("from-id must be non-negative".to_string());
    }
    if to_id < from_id {
        return Err("to-id must be greater than or equal to from-id".to_string());
    }
    if !(1..=10_000).contains(&batch_size) {
        return Err("batch-size must be between 1 and 10000".to_string());
    }
    Ok(ReplayRange {
        from_id,
        to_id,
        batch_size,
    })
}

/// Dispatch replay workbench subcommands.
pub async fn run_replay_command(
    cmd: ReplayCommands,
    default_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match cmd {
        ReplayCommands::Execute {
            pipeline,
            from_id,
            to_id,
            batch_size,
            postgres_url,
        } => {
            let range = validate_replay_range(from_id, to_id, batch_size)
                .map_err(|reason| format!("invalid replay range: {reason}"))?;
            let url = postgres_url
                .or_else(|| std::env::var("PG_TIDE_POSTGRES_URL").ok())
                .unwrap_or_else(|| default_url.to_string());
            if url.is_empty() {
                eprintln!("error: --postgres-url is required for replay execute");
                std::process::exit(1);
            }
            run_replay_execute(&url, &pipeline, range).await
        }
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
    let (mut client, conn) = pg_tls::connect(url).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let transaction = client.transaction().await?;
    let updated = transaction
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
        transaction.rollback().await?;
        return Ok(());
    }

    // Re-insert as a fresh pending entry.
    transaction
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
    transaction.commit().await?;

    println!(
        "DLQ entry requeued: pipeline='{}' dedup_key='{}'",
        pipeline, dedup_key
    );
    Ok(())
}

/// `pg-tide replay execute` — run one bounded range through the normal worker.
async fn run_replay_execute(
    url: &str,
    pipeline: &str,
    range: ReplayRange,
) -> Result<(), Box<dyn std::error::Error>> {
    let (before, after) =
        coordinator::run_replay_once(url, pipeline, range.from_id, range.to_id, range.batch_size)
            .await?;
    println!(
        "bounded replay complete: pipeline='{pipeline}' from_id={} to_id={} batch_size={} live_checkpoint={} -> {}",
        range.from_id, range.to_id, range.batch_size, before, after
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_replay_range;

    #[test]
    fn replay_range_is_bounded() {
        assert!(validate_replay_range(0, 0, 1).is_ok());
        assert!(validate_replay_range(-1, 0, 1).is_err());
        assert!(validate_replay_range(2, 1, 1).is_err());
        assert!(validate_replay_range(0, 1, 0).is_err());
        assert!(validate_replay_range(0, 1, 10_001).is_err());
    }
}
