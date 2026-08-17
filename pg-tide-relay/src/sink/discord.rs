/// Discord notification sink (RELAY-P3-N2).
///
/// Delivers relay messages to a Discord channel via Discord's Webhook API.
/// Messages are formatted as Discord Embeds for rich presentation.
///
/// Feature-gated: only compiled with `--features discord`.
use crate::envelope::RelayMessage;
use crate::error::RelayError;

#[cfg(feature = "discord")]
use reqwest::Client;

#[cfg(feature = "discord")]
pub struct DiscordSink {
    client: Client,
    webhook_url: String,
    /// Optional bot username to display in Discord.
    username: Option<String>,
    /// Optional avatar URL for the bot.
    avatar_url: Option<String>,
    /// Maximum relay messages per Discord message (Discord allows up to 10 embeds).
    batch_limit: usize,
}

#[cfg(feature = "discord")]
impl DiscordSink {
    pub fn new(
        webhook_url: impl Into<String>,
        username: Option<String>,
        avatar_url: Option<String>,
        batch_limit: usize,
    ) -> Result<Self, RelayError> {
        let webhook_url = webhook_url.into();
        crate::http_util::validate_url(&webhook_url, "discord", false, true)?;
        let client = crate::http_util::secure_client_for_url(
            &webhook_url,
            "discord",
            std::time::Duration::from_secs(30),
            false,
            true,
        )
        .map_err(|e| RelayError::sink("discord", e))?;
        Ok(Self {
            client,
            webhook_url,
            username,
            avatar_url,
            // Discord allows max 10 embeds per message.
            batch_limit: batch_limit.clamp(1, 10),
        })
    }

    /// Build the Discord Webhook JSON body for a slice of messages.
    fn build_payload(&self, messages: &[RelayMessage]) -> serde_json::Value {
        // Discord embed colour: green for insert, red for delete, grey otherwise.
        let embeds: Vec<serde_json::Value> = messages
            .iter()
            .map(|msg| {
                let color: u32 = match msg.op.as_str() {
                    "insert" => 0x57_F2_87, // green
                    "delete" => 0xED_42_45, // red
                    _ => 0x99_AA_B5,        // grey
                };

                let payload_str = serde_json::to_string_pretty(&msg.payload)
                    .unwrap_or_else(|_| msg.payload.to_string());
                // Discord embed description is limited to 4096 chars.
                let description = if payload_str.len() > 3_950 {
                    format!("```json\n{}…```", &payload_str[..3_947])
                } else {
                    format!("```json\n{payload_str}\n```")
                };

                serde_json::json!({
                    "title": format!("{} — {}", msg.subject, msg.op),
                    "description": description,
                    "color": color,
                    "footer": {
                        "text": format!("dedup_key: {}", msg.dedup_key)
                    }
                })
            })
            .collect();

        let mut body = serde_json::json!({ "embeds": embeds });

        if let Some(ref username) = self.username {
            body["username"] = serde_json::Value::String(username.clone());
        }
        if let Some(ref avatar_url) = self.avatar_url {
            body["avatar_url"] = serde_json::Value::String(avatar_url.clone());
        }

        body
    }
}

#[cfg(feature = "discord")]
#[async_trait::async_trait]
impl super::Sink for DiscordSink {
    fn name(&self) -> &str {
        "discord"
    }

    async fn publish(&mut self, messages: &[RelayMessage]) -> Result<(), RelayError> {
        if messages.is_empty() {
            return Ok(());
        }

        for chunk in messages.chunks(self.batch_limit) {
            let payload = self.build_payload(chunk);

            let resp = self
                .client
                .post(&self.webhook_url)
                .header("Content-Type", "application/json")
                .json(&payload)
                .send()
                .await
                .map_err(|e| RelayError::sink("discord", e))?;

            if !resp.status().is_success() {
                return Err(RelayError::SinkPublish {
                    sink: "discord".to_string(),
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

#[cfg(all(test, feature = "discord"))]
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
    fn test_build_payload_has_embeds() {
        let sink =
            DiscordSink::new("https://discord.com/api/webhooks/test", None, None, 10).unwrap();
        let msgs = vec![make_msg("insert", 1), make_msg("delete", 2)];
        let payload = sink.build_payload(&msgs);
        let embeds = payload["embeds"].as_array().unwrap();
        assert_eq!(embeds.len(), 2);
    }

    #[test]
    fn test_insert_color_is_green() {
        let sink =
            DiscordSink::new("https://discord.com/api/webhooks/test", None, None, 10).unwrap();
        let msgs = vec![make_msg("insert", 1)];
        let payload = sink.build_payload(&msgs);
        // 0x57F287 = 5763719 decimal.
        assert_eq!(payload["embeds"][0]["color"], 5_763_719u64);
    }

    #[test]
    fn test_delete_color_is_red() {
        let sink =
            DiscordSink::new("https://discord.com/api/webhooks/test", None, None, 10).unwrap();
        let msgs = vec![make_msg("delete", 99)];
        let payload = sink.build_payload(&msgs);
        // 0xED4245 = 15548997 decimal.
        assert_eq!(payload["embeds"][0]["color"], 15_548_997u64);
    }

    #[test]
    fn test_embed_footer_contains_dedup_key() {
        let sink =
            DiscordSink::new("https://discord.com/api/webhooks/test", None, None, 10).unwrap();
        let msgs = vec![make_msg("insert", 42)];
        let payload = sink.build_payload(&msgs);
        let footer = payload["embeds"][0]["footer"]["text"].as_str().unwrap();
        assert!(footer.contains("orders:42:0"));
    }

    #[test]
    fn test_batch_limit_clamped_to_10() {
        // Discord allows max 10 embeds — batch_limit > 10 must be clamped.
        let sink =
            DiscordSink::new("https://discord.com/api/webhooks/test", None, None, 100).unwrap();
        assert_eq!(sink.batch_limit, 10);
    }

    #[test]
    fn test_includes_username_and_avatar() {
        let sink = DiscordSink::new(
            "https://discord.com/api/webhooks/test",
            Some("pg-tide-relay".to_string()),
            Some("https://example.com/avatar.png".to_string()),
            10,
        )
        .unwrap();
        let msgs = vec![make_msg("insert", 1)];
        let payload = sink.build_payload(&msgs);
        assert_eq!(payload["username"], "pg-tide-relay");
        assert_eq!(payload["avatar_url"], "https://example.com/avatar.png");
    }
}
