/// `pg-tide history <pipeline>` — show pipeline config change history.
///
/// v0.36.0: Added `--output json|table` flag for machine-readable output.
use pg_tide_relay::pg_tls;

/// Print the config change history for a pipeline.
///
/// `output` is either `"table"` (default, human-readable) or `"json"` (machine-
/// readable NDJSON stream, one JSON object per history entry).
pub async fn run_history(
    url: &str,
    pipeline: &str,
    limit: i64,
    since: Option<&str>,
    output: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (client, conn) = pg_tls::connect(url)
        .await
        .map_err(|e| format!("connection failed: {e}"))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    // Build query with optional since filter.
    let rows = if let Some(since_ts) = since {
        client
            .query(
                "SELECT change_id, pipeline_name, pipeline_type, changed_at, changed_by, \
                        old_config, new_config \
                 FROM tide.relay_config_history($1) \
                 WHERE changed_at >= $2::timestamptz \
                 LIMIT $3",
                &[&pipeline, &since_ts, &limit],
            )
            .await
            .unwrap_or_default()
    } else {
        client
            .query(
                "SELECT change_id, pipeline_name, pipeline_type, changed_at, changed_by, \
                        old_config, new_config \
                 FROM tide.relay_config_history($1) \
                 LIMIT $2",
                &[&pipeline, &limit],
            )
            .await
            .unwrap_or_default()
    };

    if rows.is_empty() {
        if output == "json" {
            println!("[]");
        } else {
            println!("No config history found for pipeline '{}'.", pipeline);
        }
        return Ok(());
    }

    if output == "json" {
        // Emit a JSON array of history entries.
        let mut entries: Vec<serde_json::Value> = Vec::with_capacity(rows.len());
        for row in &rows {
            let change_id: i64 = row.get("change_id");
            let pipeline_name: String = row.get("pipeline_name");
            let pipeline_type: String = row.get("pipeline_type");
            let changed_at: chrono::DateTime<chrono::Utc> = row.get("changed_at");
            let changed_by: String = row.get("changed_by");
            let old_config: Option<serde_json::Value> = row.get("old_config");
            let new_config: Option<serde_json::Value> = row.get("new_config");
            entries.push(serde_json::json!({
                "change_id": change_id,
                "pipeline_name": pipeline_name,
                "pipeline_type": pipeline_type,
                "changed_at": changed_at.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                "changed_by": changed_by,
                "old_config": old_config,
                "new_config": new_config,
            }));
        }
        println!("{}", serde_json::to_string_pretty(&entries)?);
        return Ok(());
    }

    // Default: table output.
    println!(
        "{:<8} {:<30} {:<10} {:<28} {:<20}",
        "ID", "PIPELINE", "TYPE", "CHANGED_AT", "CHANGED_BY"
    );
    println!("{}", "-".repeat(100));

    for row in &rows {
        let change_id: i64 = row.get("change_id");
        let pipeline_name: String = row.get("pipeline_name");
        let pipeline_type: String = row.get("pipeline_type");
        let changed_at: chrono::DateTime<chrono::Utc> = row.get("changed_at");
        let changed_by: String = row.get("changed_by");

        println!(
            "{:<8} {:<30} {:<10} {:<28} {:<20}",
            change_id,
            pipeline_name,
            pipeline_type,
            changed_at.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            changed_by,
        );

        // Show a compact diff between old and new config.
        let old_config: Option<serde_json::Value> = row.get("old_config");
        let new_config: Option<serde_json::Value> = row.get("new_config");

        if let Some(new) = &new_config {
            if let Some(old) = &old_config {
                // Print keys that changed.
                if let (Some(old_map), Some(new_map)) = (old.as_object(), new.as_object()) {
                    let changed_keys: Vec<&str> = old_map
                        .iter()
                        .filter(|(k, v)| new_map.get(*k) != Some(v))
                        .map(|(k, _)| k.as_str())
                        .chain(
                            new_map
                                .keys()
                                .filter(|k| !old_map.contains_key(k.as_str()))
                                .map(|k| k.as_str()),
                        )
                        .collect();
                    if !changed_keys.is_empty() {
                        println!("  Changed keys: {}", changed_keys.join(", "));
                    }
                }
            } else {
                println!("  [new pipeline]");
            }
        }
    }

    Ok(())
}
