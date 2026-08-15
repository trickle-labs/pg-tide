/// Azure Event Hubs source (v0.6.0 — RELAY-P3-4).
///
/// Reads events from Azure Event Hub partitions using the Event Hubs REST API.
/// Events are consumed partition-by-partition in a round-robin fashion.
///
/// The REST API uses `ReceiveAndDelete` semantics: events are removed from the
/// partition as they are received. Consumer group offset is tracked in-process.
///
/// Feature-gated: only compiled with `--features eventhubs`.
use crate::envelope::{AckToken, RelayMessage};
use crate::error::RelayError;

#[cfg(feature = "eventhubs")]
use reqwest::Client;

#[cfg(feature = "eventhubs")]
pub struct EventHubsSource {
    client: Client,
    namespace: String,
    event_hub: String,
    consumer_group: String,
    key_name: String,
    key_value: String,
    event_type: String,
    /// Number of partitions to read from (round-robin).
    partition_count: usize,
    /// Current partition index for round-robin.
    current_partition: usize,
}

#[cfg(feature = "eventhubs")]
impl EventHubsSource {
    /// Create a new Azure Event Hubs source.
    ///
    /// `connection_string`: Standard Azure Event Hubs connection string.
    /// `event_hub`: Event Hub name.
    /// `consumer_group`: Consumer group name (use `$Default` for the default group).
    /// `partition_count`: Number of partitions to read from.
    /// `event_type`: Set as `op` field on received RelayMessages.
    pub fn new(
        connection_string: &str,
        event_hub: impl Into<String>,
        consumer_group: impl Into<String>,
        partition_count: usize,
        event_type: impl Into<String>,
    ) -> Result<Self, RelayError> {
        let (namespace, key_name, key_value) =
            crate::sink::eventhubs::parse_eventhubs_connection_string(connection_string)?;

        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(65))
            .build()
            .map_err(|e| RelayError::source_poll("eventhubs", e))?;

        Ok(Self {
            client,
            namespace,
            event_hub: event_hub.into(),
            consumer_group: consumer_group.into(),
            key_name,
            key_value,
            event_type: event_type.into(),
            partition_count: partition_count.max(1),
            current_partition: 0,
        })
    }

    fn resource_uri_for_partition(&self, partition_id: usize) -> String {
        crate::sink::eventhubs::url_encode_eventhubs(&format!(
            "{ns}.servicebus.windows.net/{eh}/ConsumerGroups/{cg}/Partitions/{pid}",
            ns = self.namespace,
            eh = self.event_hub,
            cg = self.consumer_group,
            pid = partition_id,
        ))
    }

    fn receive_url(&self, partition_id: usize) -> String {
        format!(
            "https://{ns}.servicebus.windows.net/{eh}/ConsumerGroups/{cg}/Partitions/{pid}/Messages?timeout=1&api-version=2014-01",
            ns = self.namespace,
            eh = self.event_hub,
            cg = self.consumer_group,
            pid = partition_id,
        )
    }

    fn sas_token_for_partition(&self, partition_id: usize) -> Result<String, RelayError> {
        let resource_uri = self.resource_uri_for_partition(partition_id);
        crate::sink::eventhubs::generate_eventhubs_sas_token(
            &resource_uri,
            &self.key_name,
            &self.key_value,
            300,
        )
    }
}

#[cfg(feature = "eventhubs")]
#[async_trait::async_trait]
impl super::Source for EventHubsSource {
    fn name(&self) -> &str {
        "eventhubs"
    }

    async fn poll(&mut self, batch_size: i64) -> Result<Vec<RelayMessage>, RelayError> {
        let mut messages = Vec::new();
        let limit = batch_size as usize;

        // Round-robin over partitions to collect up to `batch_size` messages.
        for _ in 0..self.partition_count {
            if messages.len() >= limit {
                break;
            }

            let partition_id = self.current_partition;
            self.current_partition = (self.current_partition + 1) % self.partition_count;

            let url = self.receive_url(partition_id);
            let sas = self.sas_token_for_partition(partition_id)?;

            let resp = self
                .client
                .get(&url)
                .header("Authorization", sas)
                .send()
                .await
                .map_err(|e| RelayError::source_poll("eventhubs", e))?;

            // 204 No Content = no messages on this partition.
            if resp.status() == reqwest::StatusCode::NO_CONTENT {
                continue;
            }

            if !resp.status().is_success() {
                let status = resp.status();
                let text = resp.text().await.unwrap_or_default();
                return Err(RelayError::SourcePoll {
                    src: "eventhubs".to_string(),
                    inner: format!("HTTP {status}: {text}").into(),
                });
            }

            // Extract sequence number from response headers for dedup key.
            let sequence_number = resp
                .headers()
                .get("x-ms-sequence-number")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("0")
                .to_string();

            let body = resp
                .bytes()
                .await
                .map_err(|e| RelayError::source_poll("eventhubs", e))?;

            let payload: serde_json::Value = serde_json::from_slice(&body).unwrap_or_else(|_| {
                serde_json::json!({
                    "raw": std::str::from_utf8(&body).unwrap_or("")
                })
            });

            let dedup_key = format!(
                "eventhubs:{eh}:{cg}:{pid}:{seq}",
                eh = self.event_hub,
                cg = self.consumer_group,
                pid = partition_id,
                seq = sequence_number,
            );

            messages.push(RelayMessage {
                outbox_name: None,
                headers: None,
                created_at: None,
                subject: format!("{}/{}", self.event_hub, partition_id),
                op: self.event_type.clone(),
                dedup_key,
                outbox_id: None,
                refresh_id: None,
                is_full_refresh: false,
                payload,
                ack_token: AckToken::None,
            });
        }

        Ok(messages)
    }

    async fn acknowledge(&mut self, _last_message: &RelayMessage) -> Result<(), RelayError> {
        // Event Hubs REST API uses ReceiveAndDelete semantics — no explicit ack needed.
        Ok(())
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}
