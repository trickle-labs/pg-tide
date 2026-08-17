/// Google Cloud Pub/Sub source (RELAY-P2-1) — pull subscription.
///
/// Pulls messages from a GCP Pub/Sub subscription via the REST API
/// and writes them to a pg-tide inbox.
/// Supports both real GCP and the Pub/Sub emulator
/// (set `PUBSUB_EMULATOR_HOST=host:port`).
///
/// Feature-gated: only compiled with `--features pubsub`.
use crate::envelope::{AckToken, RelayMessage};
use crate::error::RelayError;

#[cfg(feature = "pubsub")]
use base64::Engine as _;
#[cfg(feature = "pubsub")]
use reqwest::Client;

/// Ack IDs that need to be acknowledged after successful inbox write.
#[cfg(feature = "pubsub")]
#[allow(dead_code)]
struct PendingAck {
    ack_id: String,
}

#[cfg(feature = "pubsub")]
pub struct PubSubSource {
    client: Client,
    endpoint: String,
    project_id: String,
    subscription: String,
    event_type: String,
    /// Buffered ack IDs from the last poll, awaiting acknowledgement.
    pending_acks: Vec<String>,
}

#[cfg(feature = "pubsub")]
impl PubSubSource {
    pub fn new(
        project_id: impl Into<String>,
        subscription: impl Into<String>,
        event_type: impl Into<String>,
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
        .map_err(|e| RelayError::source_poll("pubsub", e))?;

        Ok(Self {
            client,
            endpoint,
            project_id: project_id.into(),
            subscription: subscription.into(),
            event_type: event_type.into(),
            pending_acks: Vec::new(),
        })
    }

    fn pull_url(&self) -> String {
        format!(
            "{endpoint}/v1/projects/{project}/subscriptions/{sub}:pull",
            endpoint = self.endpoint,
            project = self.project_id,
            sub = self.subscription,
        )
    }

    fn ack_url(&self) -> String {
        format!(
            "{endpoint}/v1/projects/{project}/subscriptions/{sub}:acknowledge",
            endpoint = self.endpoint,
            project = self.project_id,
            sub = self.subscription,
        )
    }

    fn add_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Ok(token) = std::env::var("PUBSUB_TOKEN") {
            req.header("Authorization", format!("Bearer {token}"))
        } else {
            req
        }
    }
}

#[cfg(feature = "pubsub")]
#[async_trait::async_trait]
impl super::Source for PubSubSource {
    fn name(&self) -> &str {
        "pubsub"
    }

    async fn poll(&mut self, batch_size: i64) -> Result<Vec<RelayMessage>, RelayError> {
        let max_messages = batch_size.min(1000) as u32;

        let body = serde_json::json!({ "maxMessages": max_messages });
        let url = self.pull_url();
        let req = self.add_auth(self.client.post(&url).json(&body));

        let resp = req
            .send()
            .await
            .map_err(|e| RelayError::source_poll("pubsub", e))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(RelayError::SourcePoll {
                src: "pubsub".to_string(),
                inner: format!("HTTP {status}: {text}").into(),
            });
        }

        let resp_body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| RelayError::source_poll("pubsub", e))?;

        let received = match resp_body.get("receivedMessages").and_then(|v| v.as_array()) {
            Some(msgs) => msgs.clone(),
            None => return Ok(Vec::new()),
        };

        let mut messages = Vec::new();
        self.pending_acks.clear();

        for item in &received {
            let ack_id = item
                .get("ackId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let ps_msg = match item.get("message") {
                Some(m) => m,
                None => continue,
            };

            // Decode base64 data.
            let data_b64 = ps_msg.get("data").and_then(|v| v.as_str()).unwrap_or("");

            let payload_bytes = base64::engine::general_purpose::STANDARD
                .decode(data_b64)
                .unwrap_or_default();

            let payload: serde_json::Value =
                serde_json::from_slice(&payload_bytes).unwrap_or(serde_json::Value::Null);

            // Use pgt_dedup_key attribute if present, otherwise use message ID.
            let dedup_key = ps_msg
                .get("attributes")
                .and_then(|a| a.get("pgt_dedup_key"))
                .and_then(|v| v.as_str())
                .or_else(|| ps_msg.get("messageId").and_then(|v| v.as_str()))
                .map(|s| format!("pubsub:{s}"))
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

            let event_type = ps_msg
                .get("attributes")
                .and_then(|a| a.get("pgt_event_type"))
                .and_then(|v| v.as_str())
                .unwrap_or(&self.event_type)
                .to_string();

            let mut relay_msg = RelayMessage::new_reverse(dedup_key, event_type, payload);
            relay_msg.ack_token = AckToken::None;

            self.pending_acks.push(ack_id);
            messages.push(relay_msg);
        }

        Ok(messages)
    }

    async fn acknowledge(&mut self, _last_message: &RelayMessage) -> Result<(), RelayError> {
        if self.pending_acks.is_empty() {
            return Ok(());
        }

        let ack_ids: Vec<&str> = self.pending_acks.iter().map(|s| s.as_str()).collect();
        let body = serde_json::json!({ "ackIds": ack_ids });
        let url = self.ack_url();
        let req = self.add_auth(self.client.post(&url).json(&body));

        let resp = req
            .send()
            .await
            .map_err(|e| RelayError::source_poll("pubsub", e))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            tracing::warn!(
                source = "pubsub",
                status = %status,
                body = %text,
                "acknowledge failed"
            );
        }

        self.pending_acks.clear();
        Ok(())
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}
