/// `pg-tide asyncapi` — AsyncAPI 3.0 document generation from relay catalog.
use pg_tide_relay::pg_tls;

use crate::cli::{AsyncapiCommands, Cli};

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
            full_schema,
        } => {
            let url = postgres_url
                .or_else(|| std::env::var("PG_TIDE_POSTGRES_URL").ok())
                .unwrap_or_else(|| default_url.to_string());
            if url.is_empty() {
                use clap::CommandFactory;
                Cli::command()
                    .error(
                        clap::error::ErrorKind::MissingRequiredArgument,
                        "--postgres-url (or $PG_TIDE_POSTGRES_URL) is required for `asyncapi export`",
                    )
                    .exit();
            }
            run_asyncapi_export(&url, &format, output.as_deref(), full_schema).await
        }
        AsyncapiCommands::Validate {
            spec_url,
            postgres_url,
        } => {
            let url = postgres_url
                .or_else(|| std::env::var("PG_TIDE_POSTGRES_URL").ok())
                .unwrap_or_else(|| default_url.to_string());
            if url.is_empty() {
                use clap::CommandFactory;
                Cli::command()
                    .error(
                        clap::error::ErrorKind::MissingRequiredArgument,
                        "--postgres-url (or $PG_TIDE_POSTGRES_URL) is required for `asyncapi validate`",
                    )
                    .exit();
            }
            run_asyncapi_validate(&url, &spec_url).await
        }
    }
}

/// `pg-tide asyncapi export` — generate an AsyncAPI 3.0 document.
async fn run_asyncapi_export(
    url: &str,
    format: &str,
    output: Option<&str>,
    full_schema: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (client, conn) = pg_tls::connect(url).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    // Load all relay pipelines.
    // v0.27.0: Include the optional `description` column from tide_outbox_config
    // (added in v0.27.0 migration) for AsyncAPI channel descriptions.
    // v0.30.0: Also fetch template_source to populate template-sourced channel
    // descriptions from the pipeline template library.
    let outbox_rows = client
        .query(
            "SELECT r.name, r.enabled, r.config, \
             COALESCE(o.description, '') as description, \
             COALESCE(r.config->>'template_source', '') as template_source \
             FROM tide.relay_outbox_config r \
             LEFT JOIN tide.tide_outbox_config o ON o.outbox_name = \
               COALESCE((r.config->'source'->>'outbox'), r.name) \
             ORDER BY r.name",
            &[],
        )
        .await
        .or_else(|_| {
            // Fallback for databases without the description column (pre-v0.27.0).
            Ok::<_, tokio_postgres::Error>(vec![])
        })
        .unwrap_or_default();

    // Re-query without description if the join query failed (schema not upgraded yet).
    let outbox_rows = if outbox_rows.is_empty() {
        client
            .query(
                "SELECT name, enabled, config FROM tide.relay_outbox_config ORDER BY name",
                &[],
            )
            .await?
    } else {
        outbox_rows
    };

    let inbox_rows = client
        .query(
            "SELECT name, enabled, config FROM tide.relay_inbox_config ORDER BY name",
            &[],
        )
        .await?;

    // v0.27.0: When --full-schema is set, sample up to 10 recent messages per
    // outbox to derive payload schema properties from observed JSON keys.
    let sample_schemas: std::collections::HashMap<String, Vec<String>> = if full_schema {
        let mut map = std::collections::HashMap::new();
        for row in &outbox_rows {
            let name: String = row.get(0);
            let config: serde_json::Value = row.get(2);
            let outbox_name = config
                .pointer("/source/outbox")
                .and_then(|v| v.as_str())
                .unwrap_or(&name)
                .to_string();
            // Try to sample payload keys from the shared outbox message table.
            let sample_query = "SELECT payload FROM tide.tide_outbox_messages \
                 WHERE stream_table = $1 ORDER BY id DESC LIMIT 10"
                .to_string();
            if let Ok(rows) = client.query(&sample_query, &[&outbox_name]).await {
                let mut keys: std::collections::HashSet<String> = std::collections::HashSet::new();
                for r in rows {
                    let payload: serde_json::Value = r.get(0);
                    if let Some(obj) = payload.as_object() {
                        keys.extend(obj.keys().cloned());
                    }
                }
                if !keys.is_empty() {
                    let mut sorted: Vec<String> = keys.into_iter().collect();
                    sorted.sort();
                    map.insert(name, sorted);
                }
            }
        }
        map
    } else {
        std::collections::HashMap::new()
    };

    // Build AsyncAPI 3.0 document.
    let mut channels = serde_json::Map::new();
    let mut operations = serde_json::Map::new();
    let mut messages = serde_json::Map::new();

    for row in &outbox_rows {
        let name: String = row.get(0);
        let _enabled: bool = row.get(1);
        let config: serde_json::Value = row.get(2);
        // v0.27.0: pick up description column when present (4th column).
        let db_description: Option<String> = row.try_get(3).ok().filter(|s: &String| !s.is_empty());
        // v0.30.0: template_source column for template-derived descriptions.
        let template_source: Option<String> =
            row.try_get(4).ok().filter(|s: &String| !s.is_empty());

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

        // v0.30.0: template-sourced description takes lowest priority;
        // explicit outbox description overrides it.
        let channel_description = db_description
            .or_else(|| {
                template_source.as_ref().map(|tmpl| {
                    format!(
                        "Forward relay: outbox '{}' → {} (instantiated from template '{}')",
                        outbox_name, sink_type, tmpl
                    )
                })
            })
            .unwrap_or_else(|| format!("Forward relay: outbox '{}' → {}", outbox_name, sink_type));

        // v0.30.0: fan-in pipelines list contributing outboxes in the message schema.
        let fan_in_sources: Vec<String> = config
            .pointer("/source/members")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let is_fan_in = !fan_in_sources.is_empty();

        channels.insert(
            format!("forward/{name}"),
            serde_json::json!({
                "address": format!("{}/{}", sink_type, name),
                "description": channel_description,
                "messages": {
                    format!("{name}Message"): {
                        "$ref": format!("#/components/messages/{name}Message")
                    }
                }
            }),
        );

        // v0.27.0: Include sampled payload property keys when --full-schema is set.
        let payload = if let Some(keys) = sample_schemas.get(&name) {
            let props: serde_json::Map<String, serde_json::Value> = keys
                .iter()
                .map(|k| {
                    (
                        k.clone(),
                        serde_json::json!({ "type": "string", "description": "sampled field" }),
                    )
                })
                .collect();
            serde_json::json!({
                "type": "object",
                "description": format!("pg_tide outbox message (wire_format: {}; schema sampled from recent messages)", wire_format),
                "properties": props
            })
        } else {
            serde_json::json!({
                "type": "object",
                "description": format!("pg_tide outbox message (wire_format: {})", wire_format)
            })
        };

        // v0.30.0: For fan-in pipelines emit a oneOf message schema that lists
        // each contributing outbox as a distinct source variant.
        let message_payload = if is_fan_in {
            let one_of: Vec<serde_json::Value> = fan_in_sources
                .iter()
                .map(|src| {
                    serde_json::json!({
                        "type": "object",
                        "title": format!("From outbox '{}'", src),
                        "description": format!(
                            "Message contributed by fan-in member outbox '{}' (wire_format: {})",
                            src, wire_format
                        )
                    })
                })
                .collect();
            serde_json::json!({
                "oneOf": one_of,
                "description": format!(
                    "Fan-in pipeline '{}': messages may originate from any of {} contributing outbox(es)",
                    name,
                    fan_in_sources.len()
                )
            })
        } else {
            payload
        };

        messages.insert(
            format!("{name}Message"),
            serde_json::json!({
                "name": format!("{name}Message"),
                "contentType": "application/json",
                "payload": message_payload
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

    // v0.30.0: Emit a delivery-receipts channel if the table exists.
    let has_delivery_receipts: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
             WHERE table_schema = 'tide' AND table_name = 'relay_delivery_receipts')",
            &[],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or(false);

    if has_delivery_receipts {
        // Emit one delivery-receipt channel per forward pipeline.
        let receipt_rows = client
            .query(
                "SELECT DISTINCT pipeline_name FROM tide.relay_delivery_receipts ORDER BY 1",
                &[],
            )
            .await
            .unwrap_or_default();

        for receipt_row in &receipt_rows {
            let pipeline: String = receipt_row.get(0);
            channels.insert(
                format!("tide/delivery-receipts/{pipeline}"),
                serde_json::json!({
                    "address": format!("tide/delivery-receipts/{}", pipeline),
                    "description": format!(
                        "Delivery receipt stream for pipeline '{}'. Each message confirms successful \
                         or failed delivery of an outbox event to the downstream sink.",
                        pipeline
                    ),
                    "messages": {
                        format!("{pipeline}ReceiptMessage"): {
                            "$ref": format!("#/components/messages/{}ReceiptMessage", pipeline)
                        }
                    }
                }),
            );
            messages.insert(
                format!("{pipeline}ReceiptMessage"),
                serde_json::json!({
                    "name": format!("{pipeline}ReceiptMessage"),
                    "contentType": "application/json",
                    "payload": {
                        "type": "object",
                        "description": format!(
                            "Delivery receipt for pipeline '{}' (tide.relay_delivery_receipts)",
                            pipeline
                        ),
                        "properties": {
                            "pipeline_name": { "type": "string" },
                            "outbox_message_id": { "type": "integer" },
                            "delivered_at": { "type": "string", "format": "date-time" },
                            "status": { "type": "string", "enum": ["delivered", "failed", "dlq"] },
                            "sink_type": { "type": "string" },
                            "error_message": { "type": "string" }
                        }
                    }
                }),
            );
        }
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

/// `pg-tide asyncapi validate` — compare the live relay catalog against an
/// external AsyncAPI spec (fetched by URL) and report channel mismatches.
///
/// v0.27.0: Checks that every channel in the spec exists as a configured
/// pipeline in the live catalog, and warns about undocumented pipelines.
///
/// v0.36.0: Exit code distinction:
///   - Exit 0: catalog fully matches the spec.
///   - Exit 1: channels declared in the spec are **missing** from the live
///     catalog (the spec expects pipelines that are not running).
///   - Exit 2: live pipelines are **not documented** in the spec (schema
///     mismatch — running pipelines absent from the spec).
///
/// When both conditions are true, exit code 1 takes precedence.
async fn run_asyncapi_validate(
    postgres_url: &str,
    spec_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::collections::HashSet;

    // Fetch the AsyncAPI spec from the provided URL.
    let response = reqwest::get(spec_url).await?;
    if !response.status().is_success() {
        return Err(format!(
            "Failed to fetch AsyncAPI spec from '{}': HTTP {}",
            spec_url,
            response.status()
        )
        .into());
    }
    let body = response.text().await?;
    let spec: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| format!("Failed to parse AsyncAPI spec as JSON: {e}"))?;

    let spec_channels: HashSet<String> = spec
        .get("channels")
        .and_then(|c| c.as_object())
        .map(|obj| obj.keys().cloned().collect())
        .unwrap_or_default();

    // Load live catalog channels.
    let (client, conn) = pg_tls::connect(postgres_url).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let rows = client
        .query(
            "SELECT name FROM tide.relay_outbox_config \
             UNION ALL \
             SELECT name FROM tide.relay_inbox_config \
             ORDER BY 1",
            &[],
        )
        .await?;

    let live_channels: HashSet<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();

    // Channels in spec but not in live catalog (exit code 1: missing channels).
    let mut spec_only: Vec<&str> = spec_channels
        .iter()
        .filter(|c| {
            let bare = c
                .trim_start_matches("forward/")
                .trim_start_matches("reverse/");
            !live_channels.contains(*c) && !live_channels.contains(bare)
        })
        .map(String::as_str)
        .collect();
    spec_only.sort();

    // Channels in live catalog but not in spec (exit code 2: schema mismatch).
    let mut live_only: Vec<&str> = live_channels
        .iter()
        .filter(|c| {
            !spec_channels.contains(*c)
                && !spec_channels.contains(&format!("forward/{c}"))
                && !spec_channels.contains(&format!("reverse/{c}"))
        })
        .map(String::as_str)
        .collect();
    live_only.sort();

    if spec_only.is_empty() && live_only.is_empty() {
        println!("OK: relay catalog matches AsyncAPI spec ({})", spec_url);
        return Ok(());
    }

    if !spec_only.is_empty() {
        // Exit 1: channels declared in the spec are missing from the live catalog.
        eprintln!("ERROR: channels declared in spec but missing from live catalog (exit 1):");
        for ch in &spec_only {
            eprintln!("  - {ch}");
        }
    }
    if !live_only.is_empty() {
        // Exit 2: live pipelines not documented in spec (schema mismatch).
        eprintln!(
            "WARNING: live pipelines not documented in spec (exit 2 if no missing channels):"
        );
        for ch in &live_only {
            eprintln!("  + {ch}");
        }
    }

    // Exit 1 takes precedence over exit 2.
    if !spec_only.is_empty() {
        std::process::exit(1);
    }
    std::process::exit(2);
}
