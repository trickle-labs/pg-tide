use pg_tide_relay::config::schema_support::PipelineDocument;
use pg_tide_relay::pg_tls;

pub async fn run_export(
    url: &str,
    pipeline: Option<&str>,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let (client, conn) = pg_tls::connect(url).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let rows = if let Some(name) = pipeline {
        client
            .query(
                "SELECT name::text, config, enabled, 'forward'::text AS direction
               FROM tide.relay_outbox_config WHERE name = $1
             UNION ALL
             SELECT name::text, config, enabled, 'reverse'::text AS direction
               FROM tide.relay_inbox_config WHERE name = $1
             ORDER BY name::text, direction",
                &[&name],
            )
            .await?
    } else {
        client
            .query(
                "SELECT name::text, config, enabled, 'forward'::text AS direction
                   FROM tide.relay_outbox_config
             UNION ALL
             SELECT name::text, config, enabled, 'reverse'::text AS direction
                   FROM tide.relay_inbox_config
             ORDER BY name::text, direction",
                &[],
            )
            .await?
    };
    let mut pipelines = Vec::with_capacity(rows.len());
    for row in rows {
        let name = row.get::<_, String>(0);
        let config = row.get::<_, serde_json::Value>(1);
        let document = PipelineDocument::parse(&name, &config)?;
        pipelines.push(serde_json::json!({
            "name": name,
            "direction": row.get::<_, String>(3),
            "enabled": row.get::<_, bool>(2),
            "config": document.canonical_json()?,
        }));
    }
    Ok(serde_json::json!({"schema_version": 1, "pipelines": pipelines}))
}
