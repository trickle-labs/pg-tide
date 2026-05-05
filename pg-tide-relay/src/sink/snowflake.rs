/// Snowflake analytics sink (v0.10.0 — RELAY-P3-SF).
///
/// Delivers relay messages to Snowflake using the Snowflake Ingest REST API
/// (Snowpipe Streaming). Messages are batched and uploaded as NDJSON to
/// Snowflake's streaming ingest endpoint.
///
/// Authentication: JWT-based (RS256) using the account identifier and private key.
/// For simplicity in this implementation, a pre-generated bearer token can also
/// be supplied via `auth_token`.
///
/// Feature-gated: only compiled with `--features snowflake`.
use crate::envelope::RelayMessage;
use crate::error::RelayError;

#[cfg(feature = "snowflake")]
use reqwest::Client;

/// Configuration for the Snowflake sink.
#[derive(Debug, Clone)]
pub struct SnowflakeConfig {
    /// Snowflake account identifier, e.g. `myorg-myaccount`.
    pub account: String,
    /// Target Snowflake database.
    pub database: String,
    /// Target Snowflake schema.
    pub schema: String,
    /// Table name template; `{stream_table}` replaced with message subject.
    pub table_template: String,
    /// Snowflake user name.
    pub user: String,
    /// Pre-generated bearer token or JWT for Snowpipe Streaming.
    pub auth_token: String,
    /// Number of rows per INSERT batch (default: 16384).
    pub batch_size: usize,
}

impl SnowflakeConfig {
    pub fn table_for(&self, subject: &str) -> String {
        self.table_template.replace("{stream_table}", subject)
    }

    /// Build the Snowpipe Streaming endpoint URL.
    pub fn endpoint_url(&self) -> String {
        format!(
            "https://{}.snowflakecomputing.com/v1/streaming/channels/insertRows",
            self.account
        )
    }

    /// Build the row insert payload for the Snowpipe Streaming API.
    pub fn build_insert_rows_payload(
        &self,
        channel: &str,
        messages: &[&RelayMessage],
    ) -> serde_json::Value {
        let rows: Vec<serde_json::Value> = messages
            .iter()
            .map(|msg| {
                serde_json::json!({
                    "_DEDUP_KEY": msg.dedup_key,
                    "_SUBJECT":   msg.subject,
                    "_OP":        msg.op,
                    "_OUTBOX_ID": msg.outbox_id,
                    "DATA":       msg.payload.to_string(),
                })
            })
            .collect();

        serde_json::json!({
            "requestId": uuid::Uuid::new_v4().to_string(),
            "channelName": channel,
            "tableDefinition": {
                "database": self.database,
                "schema":   self.schema,
            },
            "rows": rows,
        })
    }
}

#[cfg(feature = "snowflake")]
pub struct SnowflakeSink {
    client: Client,
    config: SnowflakeConfig,
}

#[cfg(feature = "snowflake")]
impl SnowflakeSink {
    pub fn new(config: SnowflakeConfig) -> Result<Self, RelayError> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .map_err(|e| RelayError::sink("snowflake", e))?;
        Ok(Self { client, config })
    }
}

#[cfg(feature = "snowflake")]
#[async_trait::async_trait]
impl super::Sink for SnowflakeSink {
    fn name(&self) -> &str {
        "snowflake"
    }

    async fn publish(&mut self, messages: &[RelayMessage]) -> Result<(), RelayError> {
        if messages.is_empty() {
            return Ok(());
        }

        // Group messages by resolved table.
        let mut groups: std::collections::HashMap<String, Vec<&RelayMessage>> =
            std::collections::HashMap::new();
        for msg in messages {
            let table = self.config.table_for(&msg.subject);
            groups.entry(table).or_default().push(msg);
        }

        let url = self.config.endpoint_url();

        for (table, batch) in &groups {
            let channel = format!(
                "{}.{}.{}",
                self.config.database, self.config.schema, table
            );
            let payload = self.config.build_insert_rows_payload(&channel, batch);

            let resp = self
                .client
                .post(&url)
                .header("Authorization", format!("Bearer {}", self.config.auth_token))
                .header("Content-Type", "application/json")
                .json(&payload)
                .send()
                .await
                .map_err(|e| RelayError::sink("snowflake", e))?;

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(RelayError::SinkPublish {
                    sink: "snowflake".to_string(),
                    source: format!("HTTP {status}: {body}").into(),
                });
            }
        }

        Ok(())
    }

    async fn is_healthy(&mut self) -> bool {
        // Snowflake Snowpipe Streaming does not have a lightweight health endpoint;
        // return true optimistically (connection issues surface in publish).
        true
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}
