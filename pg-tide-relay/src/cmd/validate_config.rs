/// `pg-tide validate-config` — dry-run source and sink factories for a pipeline.
use crate::cli::OutputFormat;
use pg_tide_relay::pg_tls;

/// Dry-run source and sink factories for a named pipeline.
pub async fn run_validate_config(
    url: &str,
    pipeline: &str,
    output_format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    use pg_tide_relay::config::{resolve_pipeline_secrets, PipelineConfig, PipelineDirection};

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

    let row = row.ok_or_else(|| format!("pipeline '{pipeline}' not found in catalog"))?;

    let config: serde_json::Value = row.get(0);
    let direction_str: String = row.get(1);
    let enabled: bool = row.get(2);

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

    let report = pg_tide_relay::config::preflight::validate_pipelines(std::slice::from_ref(&pc));
    if !report.is_valid() {
        let reasons = report
            .issues
            .iter()
            .filter(|issue| {
                matches!(
                    issue.severity,
                    pg_tide_relay::config::preflight::PreflightSeverity::Error
                )
            })
            .map(|issue| issue.reason.as_str())
            .collect::<Vec<_>>()
            .join("; ");
        return Err(format!("pipeline '{pipeline}' failed preflight: {reasons}").into());
    }

    // Validate secret references without returning resolved values.
    resolve_pipeline_secrets(pc.config.clone())
        .map_err(|error| format!("secret resolution failed: {error}"))?;

    let data = serde_json::json!({
        "pipeline": pipeline,
        "direction": direction_str,
        "enabled": enabled,
        "valid": true,
    });
    if matches!(output_format, OutputFormat::Json) {
        crate::cmd::output::success("config validate", data, output_format)?;
    } else {
        println!("validate-config: pipeline '{pipeline}' configuration is valid.");
    }
    Ok(())
}
