//! `pg-tide migrate-config` — inspect catalog rows before the v0.49.0 upgrade.

use pg_tide_relay::pg_tls;
use serde_json::Value;

const SUPPORTED_SINKS: &[&str] = &[
    "inbox",
    "pg_outbox",
    "nats",
    "kafka",
    "webhook",
    "stdout",
    "file",
];

fn removed_surface(config: &Value, reverse: bool) -> Option<String> {
    if reverse {
        return Some("reverse pipeline".to_string());
    }
    if config.get("source_mode").and_then(Value::as_str) == Some("pg_trickle") {
        return Some("source_mode=pg_trickle".to_string());
    }
    let source = config
        .get("source_type")
        .and_then(Value::as_str)
        .unwrap_or("outbox");
    if source != "outbox" {
        return Some(format!("source_type={source}"));
    }
    if let Some(sink) = config.get("sink_type").and_then(Value::as_str) {
        if !SUPPORTED_SINKS.contains(&sink) {
            return Some(format!("sink_type={sink}"));
        }
    }
    if let Some(format) = config.get("wire_format").and_then(Value::as_str) {
        if !matches!(format, "native" | "cloudevents") {
            return Some(format!("wire_format={format}"));
        }
    }
    None
}

/// Inventory every catalog row that requires operator action before upgrading.
pub async fn run_migrate_config(url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let (client, connection) = pg_tls::connect(url).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let mut affected = 0usize;
    for row in client
        .query(
            "SELECT name, enabled, config FROM tide.relay_outbox_config ORDER BY name",
            &[],
        )
        .await?
    {
        let name: String = row.get("name");
        let enabled: bool = row.get("enabled");
        let config: Value = row.get("config");
        if let Some(surface) = removed_surface(&config, false) {
            affected += 1;
            println!(
                "PGTIDE_CONFIG_UNSUPPORTED_SURFACE: pipeline='{name}' enabled={enabled} surface={surface}; last_version=0.48.0; alternative=export, disable, replace, or delete before upgrading"
            );
        }
    }

    for row in client
        .query(
            "SELECT name, enabled FROM tide.relay_inbox_config ORDER BY name",
            &[],
        )
        .await?
    {
        let name: String = row.get("name");
        let enabled: bool = row.get("enabled");
        affected += 1;
        println!(
            "PGTIDE_CONFIG_UNSUPPORTED_SURFACE: pipeline='{name}' enabled={enabled} surface=reverse pipeline; last_version=0.48.0; alternative=export, disable, replace, or delete before upgrading"
        );
    }

    if affected == 0 {
        println!("No v0.49.0-removed pipeline configuration found.");
    } else {
        println!(
            "Found {affected} affected pipeline(s). Export configuration, then disable, replace, or delete each row before retrying the upgrade."
        );
    }
    Ok(())
}
