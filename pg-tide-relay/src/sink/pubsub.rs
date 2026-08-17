/// Google Cloud Pub/Sub sink (RELAY-P2-1).
///
/// Publishes messages to a GCP Pub/Sub topic via the REST API.
/// Supports both real GCP and the Pub/Sub emulator
/// (set `PUBSUB_EMULATOR_HOST=host:port`).
///
/// Authentication: uses the Bearer token from `PUBSUB_TOKEN` env-var,
/// or no authentication when `PUBSUB_EMULATOR_HOST` is set (emulator mode).
///
/// Feature-gated: only compiled with `--features pubsub`.
use crate::envelope::RelayMessage;
use crate::error::RelayError;

#[cfg(feature = "pubsub")]
use base64::Engine as _;
#[cfg(feature = "pubsub")]
use reqwest::Client;

#[cfg(feature = "pubsub")]
pub struct PubSubSink {
    client: Client,
    /// Full REST endpoint, e.g. `http://localhost:8085` or `https://pubsub.googleapis.com`
    endpoint: String,
    project_id: String,
    topic: String,
}

#[cfg(feature = "pubsub")]
impl PubSubSink {
    pub fn new(
        project_id: impl Into<String>,
        topic: impl Into<String>,
    ) -> Result<Self, RelayError> {
        let emulator = std::env::var("PUBSUB_EMULATOR_HOST").ok();
        let endpoint = emulator
            .as_deref()
            .map(|h| format!("http://{h}"))
            .unwrap_or_else(|| "https://pubsub.googleapis.com".to_string());
        crate::http_util::validate_url(
            &endpoint,
            "pubsub",
            emulator.is_some(),
            emulator.is_none(),
        )?;

        let client = crate::http_util::secure_client_for_url(
            &endpoint,
            "pubsub",
            std::time::Duration::from_secs(60),
            emulator.is_some(),
            emulator.is_none(),
        )
        .map_err(|e| RelayError::sink("pubsub", e))?;

        Ok(Self {
            client,
            endpoint,
            project_id: project_id.into(),
            topic: topic.into(),
        })
    }

    fn publish_url(&self) -> String {
        format!(
            "{endpoint}/v1/projects/{project}/topics/{topic}:publish",
            endpoint = self.endpoint,
            project = self.project_id,
            topic = self.topic,
        )
    }
}

#[cfg(feature = "pubsub")]
#[async_trait::async_trait]
impl super::Sink for PubSubSink {
    fn name(&self) -> &str {
        "pubsub"
    }

    async fn publish(&mut self, messages: &[RelayMessage]) -> Result<(), RelayError> {
        if messages.is_empty() {
            return Ok(());
        }

        // Build Pub/Sub message list — payload is base64-encoded data.
        let ps_messages: Vec<serde_json::Value> = messages
            .iter()
            .map(|msg| {
                let data_bytes = serde_json::to_vec(&msg.payload).unwrap_or_default();
                let data_b64 = base64::engine::general_purpose::STANDARD.encode(&data_bytes);
                serde_json::json!({
                    "data": data_b64,
                    "attributes": {
                        "pgt_dedup_key": msg.dedup_key,
                        "pgt_op": msg.op,
                        "pgt_subject": msg.subject,
                    }
                })
            })
            .collect();

        let body = serde_json::json!({ "messages": ps_messages });
        let url = self.publish_url();

        let mut req = self.client.post(&url).json(&body);

        // Attach Bearer token when running against real GCP.
        if let Ok(token) = std::env::var("PUBSUB_TOKEN") {
            req = req.header("Authorization", format!("Bearer {token}"));
        }

        let resp = req
            .send()
            .await
            .map_err(|e| RelayError::sink("pubsub", e))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(RelayError::SinkPublish {
                sink: "pubsub".to_string(),
                source: format!("HTTP {status}: {text}").into(),
            });
        }

        Ok(())
    }

    async fn is_healthy(&mut self) -> bool {
        let url = format!(
            "{endpoint}/v1/projects/{project}/topics/{topic}",
            endpoint = self.endpoint,
            project = self.project_id,
            topic = self.topic,
        );
        let mut req = self.client.get(&url);
        if let Ok(token) = std::env::var("PUBSUB_TOKEN") {
            req = req.header("Authorization", format!("Bearer {token}"));
        }
        req.send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}
