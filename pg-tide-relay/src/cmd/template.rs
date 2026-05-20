/// `pg-tide template` — pipeline template management commands.
use pg_tide_relay::pg_tls;
use serde_json::Value;

/// List all available pipeline templates.
pub async fn run_template_list(url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let (client, conn) = pg_tls::connect(url)
        .await
        .map_err(|e| format!("connection failed: {e}"))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let row = client
        .query_one("SELECT tide.relay_template_list()", &[])
        .await
        .map_err(|e| format!("query failed: {e}"))?;

    let json: Value = row.get::<_, serde_json::Value>(0);
    let templates = json.as_array().cloned().unwrap_or_default();

    if templates.is_empty() {
        println!("No pipeline templates found.");
        return Ok(());
    }

    println!("{:<30} {:<50} REQUIRED KEYS", "NAME", "DESCRIPTION");
    println!("{}", "-".repeat(110));

    for t in &templates {
        let name = t.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let desc = t.get("description").and_then(|v| v.as_str()).unwrap_or("");
        let keys: Vec<String> = t
            .get("required_keys")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|k| k.as_str())
                    .map(|s| s.to_string())
                    .collect()
            })
            .unwrap_or_default();

        println!(
            "{:<30} {:<50} {}",
            name,
            if desc.len() > 48 {
                format!("{}…", &desc[..47])
            } else {
                desc.to_string()
            },
            keys.join(", ")
        );
    }

    Ok(())
}

/// Show the full config JSON for a named template.
pub async fn run_template_show(url: &str, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let (client, conn) = pg_tls::connect(url)
        .await
        .map_err(|e| format!("connection failed: {e}"))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let row = client
        .query_one("SELECT tide.relay_template_get($1)", &[&name])
        .await
        .map_err(|e| format!("query failed: {e}"))?;

    let result: Option<serde_json::Value> = row.get(0);
    match result {
        None => {
            eprintln!("Template '{}' not found.", name);
            std::process::exit(1);
        }
        Some(json) => {
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
    }

    Ok(())
}

/// Instantiate a template as an outbox pipeline, applying key=value overrides.
pub async fn run_template_apply(
    url: &str,
    name: &str,
    outbox_name: &str,
    set_pairs: &[(String, String)],
) -> Result<(), Box<dyn std::error::Error>> {
    let (client, conn) = pg_tls::connect(url)
        .await
        .map_err(|e| format!("connection failed: {e}"))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    // Build overrides JSONB from --set key=value pairs.
    let mut overrides = serde_json::Map::new();
    for (k, v) in set_pairs {
        overrides.insert(k.clone(), serde_json::Value::String(v.clone()));
    }
    let overrides_json = serde_json::Value::Object(overrides);
    let overrides_str = overrides_json.to_string();

    let row = client
        .query_one(
            "SELECT tide.relay_set_outbox_from_template($1, $2, $3::jsonb)",
            &[&outbox_name, &name, &overrides_str],
        )
        .await
        .map_err(|e| format!("template apply failed: {e}"))?;

    let resolved: serde_json::Value = row.get(0);
    println!("Applied template '{}' for outbox '{}'.", name, outbox_name);
    println!("Resolved config:");
    println!("{}", serde_json::to_string_pretty(&resolved)?);

    Ok(())
}
