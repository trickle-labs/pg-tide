//! `pg-tide sweep` — run bounded, retention-aware outbox cleanup.

use pg_tide_relay::pg_tls;
use serde_json::Value;

#[derive(Debug, Default)]
struct SweepResult {
    affected_rows: i64,
    eligible_in_batch: i64,
    has_more: bool,
    highest_deleted_id: Option<i64>,
    safe_offset: Option<i64>,
    blockers: Vec<String>,
    partition_action: Option<String>,
}

impl SweepResult {
    fn from_json(outbox: &str, value: Value) -> Result<Self, String> {
        if let Some(results) = value.get("outboxes").and_then(Value::as_array) {
            let result = results
                .first()
                .cloned()
                .unwrap_or_else(|| serde_json::json!({"outbox": outbox}));
            return Self::from_json(outbox, result);
        }
        let object = value
            .as_object()
            .ok_or_else(|| format!("outbox '{outbox}' returned a non-object sweep result"))?;
        let number = |names: &[&str]| {
            names
                .iter()
                .find_map(|name| object.get(*name).and_then(Value::as_i64))
        };
        let blockers = object
            .get("blockers")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .map(|item| {
                        item.as_str()
                            .map(str::to_owned)
                            .unwrap_or_else(|| item.to_string())
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(Self {
            affected_rows: number(&["affected_rows", "deleted_rows", "rows_deleted"]).unwrap_or(0),
            eligible_in_batch: number(&["eligible_in_batch", "eligible_rows"]).unwrap_or(0),
            has_more: object
                .get("has_more")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            highest_deleted_id: number(&["highest_deleted_id", "highest_deleted"]),
            safe_offset: number(&["safe_offset", "safe_cleanup_offset"]),
            blockers,
            partition_action: object
                .get("partition_action")
                .or_else(|| object.get("partition"))
                .map(Value::to_string),
        })
    }
}

/// Backwards-compatible one-batch sweep entry point.
#[allow(dead_code)]
pub async fn run_sweep(
    url: &str,
    outbox_name: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    run_sweep_with_options(url, outbox_name, 1000, Some(1), false).await
}

/// Delete at most `batch_size` rows per transaction and outbox per iteration.
///
/// Each outbox gets independent transactions. A failed outbox is reported and
/// does not prevent unrelated outboxes from being attempted, but the command
/// returns an error so automation cannot mistake partial cleanup for success.
pub async fn run_sweep_with_options(
    url: &str,
    outbox_name: Option<&str>,
    batch_size: i32,
    max_batches: Option<u32>,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if !(1..=10_000).contains(&batch_size) {
        return Err("batch-size must be between 1 and 10000".into());
    }
    if max_batches == Some(0) {
        return Err("max-batches must be greater than zero".into());
    }

    println!("pg-tide sweep v{}", env!("CARGO_PKG_VERSION"));
    let (client, conn) = pg_tls::connect(url)
        .await
        .map_err(|e| format!("connection failed: {e}"))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let outboxes: Vec<String> = if let Some(name) = outbox_name {
        vec![name.to_owned()]
    } else {
        client
            .query(
                "SELECT outbox_name::text FROM tide.tide_outbox_config ORDER BY outbox_name",
                &[],
            )
            .await?
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect()
    };

    if outboxes.is_empty() {
        println!("  [INFO] No outboxes configured — nothing to sweep.");
        return Ok(());
    }

    let iterations = max_batches.unwrap_or(1);
    let mut total_deleted = 0_i64;
    let mut failures = Vec::new();

    for name in &outboxes {
        let mut outbox_deleted = 0_i64;
        let mut completed = 0_u32;
        let limit = if dry_run { 1 } else { iterations };

        for batch in 1..=limit {
            match call_sweep(&client, name, batch_size, dry_run).await {
                Ok(result) => {
                    outbox_deleted += result.affected_rows;
                    print_progress(name, batch, dry_run, &result);
                    completed = batch;
                    if dry_run || !result.has_more {
                        break;
                    }
                }
                Err(error) => {
                    let message = format!(
                        "outbox '{name}' sweep failed after {completed} batch(es): {error}; \
                         inspect tide.outbox_retention_status and retry"
                    );
                    println!("  [FAIL] {message}");
                    failures.push(message);
                    break;
                }
            }
        }

        total_deleted += outbox_deleted;
        println!("  [OK] Outbox '{name}': {outbox_deleted} row(s) affected");
    }

    if let Err(error) = maintain_partitions(&client, dry_run).await {
        let message = format!(
            "partition maintenance failed: {error}; \
             inspect tide.outbox_storage_config and retry"
        );
        println!("  [FAIL] {message}");
        failures.push(message);
    }

    println!(
        "\npg-tide sweep: {total_deleted} total row(s) affected across {} outbox(es).",
        outboxes.len()
    );
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; ").into())
    }
}

async fn call_sweep(
    client: &tokio_postgres::Client,
    outbox: &str,
    batch_size: i32,
    dry_run: bool,
) -> Result<SweepResult, Box<dyn std::error::Error>> {
    client.batch_execute("BEGIN").await?;
    let result = client
        .query_one(
            "SELECT tide.outbox_sweep($1, $2, $3)",
            &[&outbox, &batch_size, &dry_run],
        )
        .await;

    let result = match result {
        Ok(row) => {
            let json: Value = row.try_get(0)?;
            SweepResult::from_json(outbox, json).map_err(|error| error.into())
        }
        Err(error) => Err(error.into()),
    };

    match result {
        Ok(value) => {
            client.batch_execute("COMMIT").await?;
            Ok(value)
        }
        Err(error) => {
            let _ = client.batch_execute("ROLLBACK").await;
            Err(error)
        }
    }
}

async fn maintain_partitions(
    client: &tokio_postgres::Client,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let available: bool = client
        .query_one(
            "SELECT to_regprocedure('tide.outbox_maintain_partitions(integer,boolean)') IS NOT NULL",
            &[],
        )
        .await?
        .get(0);
    if !available {
        return Ok(());
    }

    client.batch_execute("BEGIN").await?;
    let result = client
        .query_one(
            "SELECT tide.outbox_maintain_partitions($1, $2)",
            &[&2_i32, &dry_run],
        )
        .await;
    match result {
        Ok(_) => {
            client.batch_execute("COMMIT").await?;
            Ok(())
        }
        Err(error) => {
            let _ = client.batch_execute("ROLLBACK").await;
            Err(error.into())
        }
    }
}

fn print_progress(outbox: &str, batch: u32, dry_run: bool, result: &SweepResult) {
    let mode = if dry_run { "dry-run" } else { "sweep" };
    println!(
        "  [OK] {mode} outbox '{outbox}' batch {batch}: eligible={}, affected={}, \
         has_more={}, safe_offset={}",
        result.eligible_in_batch,
        result.affected_rows,
        result.has_more,
        result
            .safe_offset
            .map_or_else(|| "—".to_string(), |value| value.to_string())
    );
    if let Some(highest) = result.highest_deleted_id {
        println!("       highest_deleted_id={highest}");
    }
    if !result.blockers.is_empty() {
        println!("       blockers={}", result.blockers.join(", "));
    }
    if let Some(action) = &result.partition_action {
        println!("       partition_action={action}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_bounded_sweep_result() {
        let result = SweepResult::from_json(
            "orders",
            serde_json::json!({
                "eligible_in_batch": 3,
                "affected_rows": 3,
                "has_more": true,
                "safe_offset": 12,
                "highest_deleted_id": 10,
                "blockers": [{"name": "slow", "offset": 4}],
                "partition_action": "none"
            }),
        )
        .expect("valid result");
        assert_eq!(result.affected_rows, 3);
        assert!(result.has_more);
        assert_eq!(result.safe_offset, Some(12));
        assert_eq!(result.blockers.len(), 1);
    }

    #[test]
    fn parses_extension_wrapper_result() {
        let result = SweepResult::from_json(
            "orders",
            serde_json::json!({
                "outboxes": [{
                    "affected_rows": 2,
                    "eligible_in_batch": 2,
                    "has_more": false
                }]
            }),
        )
        .expect("wrapped result");
        assert_eq!(result.affected_rows, 2);
    }

    #[test]
    fn rejects_non_object_sweep_result() {
        assert!(SweepResult::from_json("orders", serde_json::json!(null)).is_err());
    }
}
