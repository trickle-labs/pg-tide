/// Elasticsearch / OpenSearch sink (RELAY-P2-4).
///
/// Uses the `_bulk` HTTP API for efficient batched indexing.
/// Compatible with both Elasticsearch 8.x and OpenSearch 2.x — the bulk API
/// is identical on both engines.
///
/// Feature-gated: only compiled with `--features elasticsearch`.
use crate::envelope::RelayMessage;
use crate::error::RelayError;

#[cfg(feature = "elasticsearch")]
use reqwest::Client;

#[cfg(feature = "elasticsearch")]
pub struct ElasticsearchSink {
    client: Client,
    base_url: String,
    index_template: String,
}

#[cfg(feature = "elasticsearch")]
impl ElasticsearchSink {
    /// Create a new Elasticsearch sink.
    ///
    /// `base_url`: e.g. `"http://localhost:9200"` or `"https://my-cluster.es.io"`
    /// `index_template`: e.g. `"pg-tide-{stream_table}"` — supports `{stream_table}`, `{op}`, `{outbox_id}`
    ///
    /// v0.18.0: Applies the shared SSRF validator to the Elasticsearch URL.
    /// Set `allow_http = true` and `ssrf_protection = false` for dev/test.
    pub fn new(
        base_url: impl Into<String>,
        index_template: impl Into<String>,
        allow_http: bool,
        ssrf_protection: bool,
    ) -> Result<Self, RelayError> {
        let base_url_str: String = base_url.into();
        // v0.18.0: SSRF guard — reject link-local, loopback, private-range URLs.
        crate::http_util::validate_url(
            &base_url_str,
            "elasticsearch",
            allow_http,
            ssrf_protection,
        )?;
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| RelayError::sink("elasticsearch", e))?;
        Ok(Self {
            client,
            base_url: base_url_str.trim_end_matches('/').to_string(),
            index_template: index_template.into(),
        })
    }
}

#[cfg(feature = "elasticsearch")]
#[async_trait::async_trait]
impl super::Sink for ElasticsearchSink {
    fn name(&self) -> &str {
        "elasticsearch"
    }

    /// Publish a batch via the `_bulk` API.
    ///
    /// Each message is indexed with `_id = dedup_key` for idempotent upserts.
    /// Delete operations (`op = "delete"`) emit a `delete` bulk action.
    async fn publish(&mut self, messages: &[RelayMessage]) -> Result<(), RelayError> {
        if messages.is_empty() {
            return Ok(());
        }

        let mut body = String::new();

        for msg in messages {
            let index = crate::envelope::render_subject(
                &self.index_template,
                &msg.subject,
                &msg.op,
                msg.outbox_id.unwrap_or(0),
                msg.refresh_id,
            );

            if msg.op == "delete" {
                // Delete by document ID.
                let action = serde_json::json!({
                    "delete": { "_index": index, "_id": msg.dedup_key }
                });
                body.push_str(&serde_json::to_string(&action).map_err(RelayError::Json)?);
                body.push('\n');
            } else {
                // Index (upsert) with _id = dedup_key.
                let action = serde_json::json!({
                    "index": { "_index": index, "_id": msg.dedup_key }
                });
                body.push_str(&serde_json::to_string(&action).map_err(RelayError::Json)?);
                body.push('\n');
                body.push_str(&serde_json::to_string(&msg.payload).map_err(RelayError::Json)?);
                body.push('\n');
            }
        }

        let url = format!("{base}/_bulk", base = self.base_url);
        let resp = self
            .client
            .post(&url)
            .header("Content-Type", "application/x-ndjson")
            .body(body)
            .send()
            .await
            .map_err(|e| RelayError::sink("elasticsearch", e))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(RelayError::SinkPublish {
                sink: "elasticsearch".to_string(),
                source: format!("HTTP {status}: {text}").into(),
            });
        }

        // Parse bulk response to check for per-item errors.
        let resp_body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| RelayError::sink("elasticsearch", e))?;

        if resp_body
            .get("errors")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            // Find the first error item and surface it.
            if let Some(items) = resp_body.get("items").and_then(|v| v.as_array()) {
                for item in items {
                    for action in &["index", "delete", "create", "update"] {
                        if let Some(result) = item.get(action) {
                            if let Some(err) = result.get("error") {
                                return Err(RelayError::SinkPublish {
                                    sink: "elasticsearch".to_string(),
                                    source: format!("bulk error: {err}").into(),
                                });
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn is_healthy(&mut self) -> bool {
        let url = format!("{base}/_cluster/health", base = self.base_url);
        self.client.get(&url).send().await.is_ok()
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}
