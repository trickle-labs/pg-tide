/// `pg-tide validate-config` — dry-run source and sink factories for a pipeline.
use std::sync::Arc;

use pg_tide_relay::pg_tls;

/// Dry-run source and sink factories for a named pipeline.
pub async fn run_validate_config(
    url: &str,
    pipeline: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use pg_tide_relay::config::{resolve_pipeline_secrets, PipelineConfig, PipelineDirection};

    println!("pg-tide validate-config — pipeline: {pipeline}");

    // v0.15.0: Use pg_tls::connect (honours sslmode from URL).
    let (client, conn) = pg_tls::connect(url)
        .await
        .map_err(|e| format!("connection failed: {e}"))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    // Load pipeline from catalog (outbox config first, then inbox).
    let row = client
        .query_opt(
            "SELECT config, 'forward'::text AS direction, enabled \
             FROM tide.relay_outbox_config WHERE name = $1
             UNION ALL
             SELECT config, 'reverse'::text, enabled \
             FROM tide.relay_inbox_config WHERE name = $1
             LIMIT 1",
            &[&pipeline],
        )
        .await?;

    let row = match row {
        Some(r) => r,
        None => {
            eprintln!("error: pipeline '{pipeline}' not found in catalog");
            std::process::exit(1);
        }
    };

    let config: serde_json::Value = row.get(0);
    let direction_str: String = row.get(1);
    let enabled: bool = row.get(2);

    if !enabled {
        println!("  [WARN] pipeline '{pipeline}' is disabled");
    }

    let direction = if direction_str == "forward" {
        PipelineDirection::Forward
    } else {
        PipelineDirection::Reverse
    };

    let pc = PipelineConfig {
        name: pipeline.to_string(),
        direction,
        enabled,
        config,
        tenant_name: "default".to_string(),
    };

    let resolved = resolve_pipeline_secrets(pc.config.clone())
        .map_err(|e| format!("secret resolution failed: {e}"))?;

    println!("  [OK] Secrets resolved");

    let resolved_pc = PipelineConfig {
        name: pc.name.clone(),
        direction: pc.direction,
        enabled: pc.enabled,
        config: resolved,
        tenant_name: pc.tenant_name.clone(),
    };

    // Try to build source.
    let (worker_client, worker_conn) = pg_tls::connect(url)
        .await
        .map_err(|e| format!("worker connection failed: {e}"))?;
    tokio::spawn(async move {
        let _ = worker_conn.await;
    });
    let db = Arc::new(worker_client);

    match pg_tide_relay::coordinator::build_source_for_validation(
        &resolved_pc,
        Arc::clone(&db),
        "validate",
    )
    .await
    {
        Ok(src) => println!("  [OK] Source '{}' instantiated", src.name()),
        Err(e) => {
            println!("  [FAIL] Source instantiation failed: {e}");
            std::process::exit(1);
        }
    }

    match pg_tide_relay::coordinator::build_sink_for_validation(&resolved_pc, Arc::clone(&db)).await
    {
        Ok(sink) => println!("  [OK] Sink '{}' instantiated", sink.name()),
        Err(e) => {
            println!("  [FAIL] Sink instantiation failed: {e}");
            std::process::exit(1);
        }
    }

    println!("\nvalidate-config: pipeline '{pipeline}' configuration is valid.");
    Ok(())
}
