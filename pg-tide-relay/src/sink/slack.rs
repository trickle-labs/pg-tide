/// Slack notification sink (RELAY-P3-N1).
///
/// Delivers relay messages as Slack messages via the Incoming Webhooks API.
/// Each batch is formatted as a structured Block Kit message with one section
/// per relay message.  The webhook URL is obtained from Slack's App dashboard.
///
/// Feature-gated: only compiled with `--features slack`.
use crate::envelope::RelayMessage;
use crate::error::RelayError;

#[cfg(feature = "slack")]
use reqwest::Client;

#[cfg(feature = "slack")]
pub struct SlackSink {
    client: Client,
    webhook_url: String,
    /// Optional override for the bot username shown in Slack.
    username: Option<String>,
    /// Optional emoji icon override (e.g. `":robot_face:"`).
    icon_emoji: Option<String>,
    /// Maximum number of relay messages to include per Slack message.
    /// Excess messages in the same batch generate additional Slack messages.
    batch_limit: usize,
}

#[cfg(feature = "slack")]
impl SlackSink {
    pub fn new(
        webhook_url: impl Into<String>,
        username: Option<String>,
        icon_emoji: Option<String>,
        batch_limit: usize,
    ) -> Result<Self, RelayError> {
        let webhook_url = webhook_url.into();
        crate::http_util::validate_url(&webhook_url, "slack", false, true)?;
        let client = crate::http_util::secure_client_for_url(
            &webhook_url,
            "slack",
            std::time::Duration::from_secs(30),
            false,
            true,
        )
        .map_err(|e| RelayError::sink("slack", e))?;
        Ok(Self {
            client,
            webhook_url,
            username,
            icon_emoji,
            batch_limit: batch_limit.max(1),
        })
    }

    /// Build the Slack Incoming Webhook JSON body for a slice of messages.
    fn build_payload(&self, messages: &[RelayMessage]) -> serde_json::Value {
        let blocks: Vec<serde_json::Value> = messages
            .iter()
            .map(|msg| {
                let text = format!(
                    "*{}* — `{}` | op: `{}`\n```{}```",
                    msg.subject,
                    msg.dedup_key,
                    msg.op,
                    serde_json::to_string_pretty(&msg.payload)
                        .unwrap_or_else(|_| msg.payload.to_string()),
                );
                serde_json::json!({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": text
                    }
                })
            })
            .collect();

        let mut body = serde_json::json!({ "blocks": blocks });

        if let Some(ref username) = self.username {
            body["username"] = serde_json::Value::String(username.clone());
        }
        if let Some(ref icon) = self.icon_emoji {
            body["icon_emoji"] = serde_json::Value::String(icon.clone());
        }

        body
    }
}

#[cfg(feature = "slack")]
#[async_trait::async_trait]
impl super::Sink for SlackSink {
    fn name(&self) -> &str {
        "slack"
    }

    async fn publish(&mut self, messages: &[RelayMessage]) -> Result<(), RelayError> {
        if messages.is_empty() {
            return Ok(());
        }

        // Split large batches into smaller chunks that fit within Slack's
        // block limit (50 blocks per message).
        for chunk in messages.chunks(self.batch_limit) {
            let payload = self.build_payload(chunk);

            let resp = self
                .client
                .post(&self.webhook_url)
                .header("Content-Type", "application/json")
                .json(&payload)
                .send()
                .await
                .map_err(|e| RelayError::sink("slack", e))?;

            if !resp.status().is_success() {
                return Err(RelayError::SinkPublish {
                    sink: "slack".to_string(),
                    source: format!("HTTP {}", resp.status()).into(),
                });
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

#[cfg(all(test, feature = "slack"))]
mod tests {
    use super::*;
    use crate::envelope::RelayMessage;

    fn make_msg(op: &str, order_id: i64) -> RelayMessage {
        RelayMessage::new_forward(
            "orders",
            order_id,
            0,
            op,
            serde_json::json!({"order_id": order_id}),
            false,
            None,
            format!("orders.{op}"),
        )
    }

    #[test]
    fn test_build_payload_has_blocks() {
        let sink = SlackSink::new("https://hooks.slack.com/services/test", None, None, 50).unwrap();
        let msgs = vec![make_msg("insert", 1), make_msg("delete", 2)];
        let payload = sink.build_payload(&msgs);
        let blocks = payload["blocks"].as_array().unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0]["type"], "section");
    }

    #[test]
    fn test_build_payload_includes_username_and_icon() {
        let sink = SlackSink::new(
            "https://hooks.slack.com/services/test",
            Some("pg-tide".to_string()),
            Some(":robot_face:".to_string()),
            50,
        )
        .unwrap();
        let msgs = vec![make_msg("insert", 1)];
        let payload = sink.build_payload(&msgs);
        assert_eq!(payload["username"], "pg-tide");
        assert_eq!(payload["icon_emoji"], ":robot_face:");
    }

    #[test]
    fn test_build_payload_op_appears_in_block_text() {
        let sink = SlackSink::new("https://hooks.slack.com/services/test", None, None, 50).unwrap();
        let msgs = vec![make_msg("delete", 99)];
        let payload = sink.build_payload(&msgs);
        let text = payload["blocks"][0]["text"]["text"].as_str().unwrap();
        assert!(text.contains("delete"));
        assert!(text.contains("orders:99:0"));
    }

    #[test]
    fn test_batch_limit_is_at_least_one() {
        // batch_limit = 0 must be clamped to 1.
        let sink = SlackSink::new("https://hooks.slack.com/services/test", None, None, 0).unwrap();
        assert_eq!(sink.batch_limit, 1);
    }
}
