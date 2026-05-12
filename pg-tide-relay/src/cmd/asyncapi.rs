/// `pg-tide asyncapi` — AsyncAPI 3.0 document generation from relay catalog.
use pg_tide_relay::pg_tls;

use crate::cli::AsyncapiCommands;

/// Dispatch asyncapi subcommands.
pub async fn run_asyncapi_command(
    cmd: AsyncapiCommands,
    default_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match cmd {
        AsyncapiCommands::Export {
            format,
            output,
            postgres_url,
        } => {
            let url = postgres_url
                .or_else(|| std::env::var("PG_TIDE_POSTGRES_URL").ok())
                .unwrap_or_else(|| default_url.to_string());
            if url.is_empty() {
                eprintln!("error: --postgres-url is required for asyncapi export");
                std::process::exit(1);
            }
            run_asyncapi_export(&url, &format, output.as_deref()).await
        }
    }
}

/// `pg-tide asyncapi export` — generate an AsyncAPI 3.0 document.
async fn run_asyncapi_export(
    url: &str,
    format: &str,
    output: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (client, conn) = pg_tls::connect(url).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    // Load all relay pipelines.
    let outbox_rows = client
        .query(
            "SELECT name, enabled, config FROM tide.relay_outbox_config ORDER BY name",
            &[],
        )
        .await?;

    let inbox_rows = client
        .query(
            "SELECT name, enabled, config FROM tide.relay_inbox_config ORDER BY name",
            &[],
        )
        .await?;

    // Build AsyncAPI 3.0 document.
    let mut channels = serde_json::Map::new();
    let mut operations = serde_json::Map::new();
    let mut messages = serde_json::Map::new();

    for row in &outbox_rows {
        let name: String = row.get(0);
        let _enabled: bool = row.get(1);
        let config: serde_json::Value = row.get(2);

        let sink_type = config
            .get("sink_type")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let outbox_name = config
            .pointer("/source/outbox")
            .and_then(|v| v.as_str())
            .unwrap_or(&name);
        let wire_format = config
            .get("wire_format")
            .and_then(|v| v.as_str())
            .unwrap_or("native");

        channels.insert(
            format!("forward/{name}"),
            serde_json::json!({
                "address": format!("{}/{}", sink_type, name),
                "description": format!("Forward relay: outbox '{}' → {}", outbox_name, sink_type),
                "messages": {
                    format!("{name}Message"): {
                        "$ref": format!("#/components/messages/{name}Message")
                    }
                }
            }),
        );

        messages.insert(
            format!("{name}Message"),
            serde_json::json!({
                "name": format!("{name}Message"),
                "contentType": "application/json",
                "payload": {
                    "type": "object",
                    "description": format!("pg_tide outbox message (wire_format: {})", wire_format)
                }
            }),
        );

        operations.insert(
            format!("send{}", to_pascal_case(&name)),
            serde_json::json!({
                "action": "send",
                "channel": { "$ref": format!("#/channels/forward~1{name}") },
                "description": format!("Publish messages from outbox '{}' to {}", outbox_name, sink_type)
            }),
        );
    }

    for row in &inbox_rows {
        let name: String = row.get(0);
        let _enabled: bool = row.get(1);
        let config: serde_json::Value = row.get(2);

        let source_type = config
            .get("source_type")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let inbox_name = config
            .pointer("/sink/inbox")
            .and_then(|v| v.as_str())
            .unwrap_or(&name);
        let wire_format = config
            .get("wire_format")
            .and_then(|v| v.as_str())
            .unwrap_or("native");

        channels.insert(
            format!("reverse/{name}"),
            serde_json::json!({
                "address": format!("{}/{}", source_type, name),
                "description": format!("Reverse relay: {} → inbox '{}'", source_type, inbox_name),
                "messages": {
                    format!("{name}InboxMessage"): {
                        "$ref": format!("#/components/messages/{name}InboxMessage")
                    }
                }
            }),
        );

        messages.insert(
            format!("{name}InboxMessage"),
            serde_json::json!({
                "name": format!("{name}InboxMessage"),
                "contentType": "application/json",
                "payload": {
                    "type": "object",
                    "description": format!("Inbound message for inbox '{}' (wire_format: {})", inbox_name, wire_format)
                }
            }),
        );

        operations.insert(
            format!("receive{}", to_pascal_case(&name)),
            serde_json::json!({
                "action": "receive",
                "channel": { "$ref": format!("#/channels/reverse~1{name}") },
                "description": format!("Consume messages from {} into inbox '{}'", source_type, inbox_name)
            }),
        );
    }

    let doc = serde_json::json!({
        "asyncapi": "3.0.0",
        "info": {
            "title": "pg-tide Relay AsyncAPI",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "Auto-generated AsyncAPI 3.0 document from pg-tide relay catalog metadata.",
        },
        "channels": channels,
        "operations": operations,
        "components": {
            "messages": messages,
        }
    });

    let content = match format {
        "json" => serde_json::to_string_pretty(&doc)?,
        _ => {
            format!(
                "# AsyncAPI 3.0 document — generated by pg-tide v{}\n# Format: JSON (YAML-compatible)\n{}",
                env!("CARGO_PKG_VERSION"),
                serde_json::to_string_pretty(&doc)?
            )
        }
    };

    match output {
        Some(path) => {
            tokio::fs::write(path, content).await?;
            eprintln!("AsyncAPI document written to '{path}'");
        }
        None => println!("{content}"),
    }

    Ok(())
}

/// Convert a kebab-case or snake_case string to PascalCase for AsyncAPI operation IDs.
fn to_pascal_case(s: &str) -> String {
    s.split(['-', '_'])
        .filter(|p| !p.is_empty())
        .map(|p| {
            let mut chars = p.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
            }
        })
        .collect()
}
