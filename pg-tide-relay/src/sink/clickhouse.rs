/// ClickHouse analytics sink (v0.10.0 — RELAY-P3-CH).
///
/// Delivers relay messages to ClickHouse via its HTTP interface.
/// Each batch is inserted as a JSON Lines (JSONEachRow) payload using
/// ClickHouse's native HTTP API (`INSERT INTO … FORMAT JSONEachRow`).
///
/// Feature-gated: only compiled with `--features clickhouse`.
use crate::envelope::RelayMessage;
use crate::error::RelayError;

#[cfg(feature = "clickhouse")]
use reqwest::Client;

/// Configuration for the ClickHouse sink.
#[derive(Debug, Clone)]
pub struct ClickHouseConfig {
    /// ClickHouse HTTP endpoint, e.g. `http://localhost:8123`.
    pub url: String,
    /// Target database name.
    pub database: String,
    /// Table name template; `{stream_table}` is replaced with the message subject.
    pub table_template: String,
    /// Optional username (defaults to `default`).
    pub username: Option<String>,
    /// Optional password.
    pub password: Option<String>,
}

impl ClickHouseConfig {
    /// Resolve the table name for a given subject.
    pub fn table_for(&self, subject: &str) -> String {
        self.table_template.replace("{stream_table}", subject)
    }

    /// Build the ClickHouse INSERT query for a given table.
    pub fn insert_query(&self, table: &str) -> String {
        format!(
            "INSERT INTO `{}`.`{}` FORMAT JSONEachRow",
            self.database, table
        )
    }
}

#[cfg(feature = "clickhouse")]
pub struct ClickHouseSink {
    client: Client,
    config: ClickHouseConfig,
}

#[cfg(feature = "clickhouse")]
impl ClickHouseSink {
    /// Create a new ClickHouseSink.
    pub fn new(config: ClickHouseConfig) -> Result<Self, RelayError> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .map_err(|e| RelayError::sink("clickhouse", e))?;
        Ok(Self { client, config })
    }

    /// Build the NDJSON body for a batch of messages (JSONEachRow format).
    pub fn build_jsonl_body(&self, messages: &[&RelayMessage]) -> String {
        messages
            .iter()
            .map(|msg| {
                serde_json::to_string(&serde_json::json!({
                    "_dedup_key": msg.dedup_key,
                    "_subject":   msg.subject,
                    "_op":        msg.op,
                    "_outbox_id": msg.outbox_id,
                    "data":       msg.payload,
                }))
                .unwrap_or_default()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

#[cfg(feature = "clickhouse")]
#[async_trait::async_trait]
impl super::Sink for ClickHouseSink {
    fn name(&self) -> &str {
        "clickhouse"
    }

    async fn publish(&mut self, messages: &[RelayMessage]) -> Result<(), RelayError> {
        if messages.is_empty() {
            return Ok(());
        }

        // Group by resolved table name.
        let mut groups: std::collections::HashMap<String, Vec<&RelayMessage>> =
            std::collections::HashMap::new();
        for msg in messages {
            let table = self.config.table_for(&msg.subject);
            groups.entry(table).or_default().push(msg);
        }

        for (table, batch) in &groups {
            let query = self.config.insert_query(table);
            let body = self.build_jsonl_body(batch);

            let mut req = self
                .client
                .post(&self.config.url)
                .query(&[("query", &query)])
                .header("Content-Type", "application/x-ndjson")
                .body(body);

            if let Some(ref user) = self.config.username {
                req = req.header("X-ClickHouse-User", user);
            }
            if let Some(ref pass) = self.config.password {
                req = req.header("X-ClickHouse-Key", pass);
            }

            let resp = req
                .send()
                .await
                .map_err(|e| RelayError::sink("clickhouse", e))?;

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(RelayError::SinkPublish {
                    sink: "clickhouse".to_string(),
                    source: format!("HTTP {status}: {body}").into(),
                });
            }
        }

        Ok(())
    }

    async fn is_healthy(&mut self) -> bool {
        let resp = self
            .client
            .get(format!("{}/ping", self.config.url))
            .send()
            .await;
        matches!(resp, Ok(r) if r.status().is_success())
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}
