/// BigQuery analytics sink (v0.10.0 — RELAY-P3-BQ).
///
/// Delivers relay messages to Google BigQuery using the BigQuery Storage Write API
/// (via the legacy `tabledata.insertAll` REST endpoint for JSON streaming).
///
/// Authentication: Bearer token supplied directly. In production this would be
/// refreshed from a service-account credentials file or Workload Identity.
///
/// Feature-gated: only compiled with `--features bigquery`.
use crate::envelope::RelayMessage;
use crate::error::RelayError;

#[cfg(feature = "bigquery")]
use reqwest::Client;

/// Write mode for BigQuery.
#[derive(Debug, Clone, PartialEq)]
pub enum BigQueryWriteMode {
    /// Stream rows via `tabledata.insertAll` (default, always available).
    Streaming,
    /// Batch load via Storage Write API (lower cost, higher latency).
    Batch,
}

/// Configuration for the BigQuery sink.
#[derive(Debug, Clone)]
pub struct BigQueryConfig {
    /// GCP project ID.
    pub project_id: String,
    /// BigQuery dataset ID.
    pub dataset_id: String,
    /// Table name template; `{stream_table}` replaced with message subject.
    pub table_template: String,
    /// Write mode (default: Streaming).
    pub write_mode: BigQueryWriteMode,
    /// Bearer access token (e.g. from `gcloud auth print-access-token`).
    pub access_token: String,
}

impl BigQueryConfig {
    pub fn table_for(&self, subject: &str) -> String {
        self.table_template.replace("{stream_table}", subject)
    }

    /// Build the `tabledata.insertAll` endpoint URL.
    pub fn insert_all_url(&self, table: &str) -> String {
        format!(
            "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets/{}/tables/{}/insertAll",
            self.project_id, self.dataset_id, table
        )
    }

    /// Build the insertAll request body for a batch of messages.
    pub fn build_insert_all_payload(&self, messages: &[&RelayMessage]) -> serde_json::Value {
        let rows: Vec<serde_json::Value> = messages
            .iter()
            .map(|msg| {
                serde_json::json!({
                    "insertId": msg.dedup_key,
                    "json": {
                        "_dedup_key": msg.dedup_key,
                        "_subject":   msg.subject,
                        "_op":        msg.op,
                        "_outbox_id": msg.outbox_id,
                        "data":       msg.payload.to_string(),
                    }
                })
            })
            .collect();

        serde_json::json!({
            "skipInvalidRows": false,
            "ignoreUnknownValues": false,
            "rows": rows,
        })
    }
}

#[cfg(feature = "bigquery")]
pub struct BigQuerySink {
    client: Client,
    config: BigQueryConfig,
}

#[cfg(feature = "bigquery")]
impl BigQuerySink {
    pub fn new(config: BigQueryConfig) -> Result<Self, RelayError> {
        crate::http_util::validate_url("https://bigquery.googleapis.com", "bigquery", false, true)?;
        let client = crate::http_util::secure_client_for_url(
            "https://bigquery.googleapis.com",
            "bigquery",
            std::time::Duration::from_secs(60),
            false,
            true,
        )
        .map_err(|e| RelayError::sink("bigquery", e))?;
        Ok(Self { client, config })
    }
}

#[cfg(feature = "bigquery")]
#[async_trait::async_trait]
impl super::Sink for BigQuerySink {
    fn name(&self) -> &str {
        "bigquery"
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
            let url = self.config.insert_all_url(table);
            let payload = self.config.build_insert_all_payload(batch);

            let resp = self
                .client
                .post(&url)
                .header(
                    "Authorization",
                    format!("Bearer {}", self.config.access_token),
                )
                .header("Content-Type", "application/json")
                .json(&payload)
                .send()
                .await
                .map_err(|e| RelayError::sink("bigquery", e))?;

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(RelayError::SinkPublish {
                    sink: "bigquery".to_string(),
                    source: format!("HTTP {status}: {body}").into(),
                });
            }

            // Check for partial failures in the BigQuery response.
            let body: serde_json::Value = resp
                .json()
                .await
                .unwrap_or(serde_json::Value::Object(Default::default()));
            if let Some(errors) = body.get("insertErrors") {
                if !errors.as_array().map(|a| a.is_empty()).unwrap_or(true) {
                    return Err(RelayError::SinkPublish {
                        sink: "bigquery".to_string(),
                        source: format!("insertErrors: {errors}").into(),
                    });
                }
            }
        }

        Ok(())
    }

    async fn is_healthy(&mut self) -> bool {
        true
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}
