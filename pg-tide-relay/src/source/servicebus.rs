/// Azure Service Bus source (RELAY-P2-3).
///
/// Receives messages from an Azure Service Bus queue using PeekLock mode
/// via the Service Bus REST API. Messages are completed (deleted) only after
/// successful inbox write.
///
/// Feature-gated: only compiled with `--features servicebus`.
use crate::envelope::{AckToken, RelayMessage};
use crate::error::RelayError;

#[cfg(feature = "servicebus")]
use reqwest::Client;

/// Pending lock token for a received message.
#[cfg(feature = "servicebus")]
#[derive(Clone)]
struct PendingMessage {
    lock_location: String,
    message_id: String,
}

#[cfg(feature = "servicebus")]
pub struct ServiceBusSource {
    client: Client,
    namespace: String,
    entity: String,
    key_name: String,
    key_value: String,
    event_type: String,
    /// Lock locations awaiting completion after successful inbox write.
    pending: Vec<PendingMessage>,
}

#[cfg(feature = "servicebus")]
impl ServiceBusSource {
    /// Create a new Azure Service Bus source.
    ///
    /// `connection_string`: Standard Azure SB connection string.
    /// `entity`: Queue name.
    pub fn new(
        connection_string: &str,
        entity: impl Into<String>,
        event_type: impl Into<String>,
    ) -> Result<Self, RelayError> {
        let (namespace, key_name, key_value) =
            crate::sink::servicebus::parse_connection_string_pub(connection_string)?;

        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .map_err(|e| RelayError::source_poll("servicebus", e))?;

        Ok(Self {
            client,
            namespace,
            entity: entity.into(),
            key_name,
            key_value,
            event_type: event_type.into(),
            pending: Vec::new(),
        })
    }

    fn base_url(&self) -> String {
        format!(
            "https://{namespace}.servicebus.windows.net/{entity}",
            namespace = self.namespace,
            entity = self.entity,
        )
    }

    fn resource_uri(&self) -> String {
        crate::sink::servicebus::url_encode_pub(&format!(
            "{namespace}.servicebus.windows.net/{entity}",
            namespace = self.namespace,
            entity = self.entity,
        ))
    }

    fn sas_token(&self) -> Result<String, RelayError> {
        crate::sink::servicebus::generate_sas_token_pub(
            &self.resource_uri(),
            &self.key_name,
            &self.key_value,
            300,
        )
    }
}

#[cfg(feature = "servicebus")]
#[async_trait::async_trait]
impl super::Source for ServiceBusSource {
    fn name(&self) -> &str {
        "servicebus"
    }

    async fn poll(&mut self, batch_size: i64) -> Result<Vec<RelayMessage>, RelayError> {
        let url = format!("{}/messages/head", self.base_url());
        let mut messages = Vec::new();
        self.pending.clear();

        let count = batch_size.min(256) as usize;

        for _ in 0..count {
            let sas = self.sas_token()?;

            // POST to peek-lock a single message.
            let resp = self
                .client
                .post(&url)
                .header("Authorization", sas)
                .send()
                .await
                .map_err(|e| RelayError::source_poll("servicebus", e))?;

            // 204 No Content = no messages available.
            if resp.status() == reqwest::StatusCode::NO_CONTENT {
                break;
            }

            if !resp.status().is_success() {
                let status = resp.status();
                let text = resp.text().await.unwrap_or_default();
                return Err(RelayError::SourcePoll {
                    src: "servicebus".to_string(),
                    inner: format!("HTTP {status}: {text}").into(),
                });
            }

            // The Location header contains the lock URI for completing the message.
            let lock_location = resp
                .headers()
                .get("Location")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();

            let broker_props_str = resp
                .headers()
                .get("BrokerProperties")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("{}")
                .to_string();

            let broker_props: serde_json::Value = serde_json::from_str(&broker_props_str)
                .unwrap_or(serde_json::Value::Object(Default::default()));

            let message_id = broker_props
                .get("MessageId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let body = resp
                .text()
                .await
                .map_err(|e| RelayError::source_poll("servicebus", e))?;

            let payload: serde_json::Value =
                serde_json::from_str(&body).unwrap_or(serde_json::Value::String(body));

            let dedup_key = if message_id.is_empty() {
                uuid::Uuid::new_v4().to_string()
            } else {
                format!("servicebus:{message_id}")
            };

            let event_type = payload
                .get("event_type")
                .and_then(|v| v.as_str())
                .unwrap_or(&self.event_type)
                .to_string();

            let mut relay_msg = RelayMessage::new_reverse(dedup_key, event_type, payload);
            relay_msg.ack_token = AckToken::None;

            self.pending.push(PendingMessage {
                lock_location,
                message_id: message_id.clone(),
            });
            messages.push(relay_msg);
        }

        Ok(messages)
    }

    async fn acknowledge(&mut self, _last_message: &RelayMessage) -> Result<(), RelayError> {
        // Complete (delete) all pending locked messages.
        for pending in &self.pending {
            if pending.lock_location.is_empty() {
                continue;
            }

            let sas = self.sas_token()?;
            let resp = self
                .client
                .delete(&pending.lock_location)
                .header("Authorization", sas)
                .send()
                .await
                .map_err(|e| RelayError::source_poll("servicebus", e))?;

            if !resp.status().is_success() {
                tracing::warn!(
                    source = "servicebus",
                    message_id = %pending.message_id,
                    status = %resp.status(),
                    "failed to complete (delete) locked message",
                );
            }
        }

        self.pending.clear();
        Ok(())
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}
