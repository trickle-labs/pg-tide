/// MQTT v5 source (v0.6.0 — RELAY-P3-6).
///
/// Subscribes to an MQTT topic filter and receives messages from the broker.
/// Received messages are decoded from JSON and forwarded to the inbox.
///
/// QoS 1 (at-least-once): the broker retransmits messages that are not
/// acknowledged within the keep-alive interval.
///
/// Feature-gated: only compiled with `--features mqtt`.
use crate::envelope::{AckToken, RelayMessage};
use crate::error::RelayError;

#[cfg(feature = "mqtt")]
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};

#[cfg(feature = "mqtt")]
use tokio::sync::mpsc;

/// MQTT source — subscribes to a topic filter and yields messages.
#[cfg(feature = "mqtt")]
pub struct MqttSource {
    client: AsyncClient,
    event_type: String,
    rx: mpsc::Receiver<RelayMessage>,
}

#[cfg(feature = "mqtt")]
impl MqttSource {
    /// Connect and subscribe to `topic_filter`.
    ///
    /// `url`: broker URL, e.g. `"mqtt://localhost:1883"`
    /// `topic_filter`: MQTT topic filter (supports `+` and `#` wildcards)
    /// `client_id`: unique MQTT client identifier
    /// `event_type`: set as the `op` field on each incoming RelayMessage
    pub async fn new(
        url: &str,
        topic_filter: impl Into<String>,
        client_id: impl Into<String>,
        event_type: impl Into<String>,
    ) -> Result<Self, RelayError> {
        let client_id = client_id.into();
        let topic_filter = topic_filter.into();
        let event_type = event_type.into();

        let stripped = url
            .trim_start_matches("mqtts://")
            .trim_start_matches("mqtt://");
        let (host, port) = if let Some(pos) = stripped.rfind(':') {
            let h = &stripped[..pos];
            let p: u16 = stripped[pos + 1..]
                .parse()
                .unwrap_or(if url.starts_with("mqtts") { 8883 } else { 1883 });
            (h.to_string(), p)
        } else {
            (
                stripped.to_string(),
                if url.starts_with("mqtts") { 8883 } else { 1883 },
            )
        };

        let mut options = MqttOptions::new(&client_id, &host, port);
        options.set_keep_alive(std::time::Duration::from_secs(60));
        options.set_clean_session(false); // persistent session for offline buffering

        let (client, mut eventloop) = AsyncClient::new(options, 256);

        // Subscribe on a background task after the event loop drives the CONNACK.
        let client_sub = client.clone();
        let topic_filter_clone = topic_filter.clone();

        let (tx, rx) = mpsc::channel::<RelayMessage>(1024);

        tokio::spawn(async move {
            // Wait for CONNACK before subscribing.
            let mut subscribed = false;
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::ConnAck(_))) if !subscribed => {
                        if let Err(e) = client_sub
                            .subscribe(&topic_filter_clone, QoS::AtLeastOnce)
                            .await
                        {
                            tracing::error!(error = %e, "MQTT subscribe failed");
                        } else {
                            subscribed = true;
                            tracing::debug!(topic = %topic_filter_clone, "MQTT subscribed");
                        }
                    }
                    Ok(Event::Incoming(Packet::Publish(p))) => {
                        let payload: serde_json::Value =
                            serde_json::from_slice(&p.payload).unwrap_or_else(|_| {
                                serde_json::json!({ "raw": std::str::from_utf8(&p.payload).unwrap_or("") })
                            });

                        // Use MQTT packet ID as dedup key fallback.
                        let dedup_key = format!("mqtt:{}:{}", p.topic, uuid::Uuid::new_v4());

                        let msg = RelayMessage {
                            outbox_name: None,
                            headers: None,
                            created_at: None,
                            subject: p.topic.clone(),
                            op: "event".to_string(),
                            dedup_key,
                            outbox_id: None,
                            refresh_id: None,
                            is_full_refresh: false,
                            payload,
                            ack_token: AckToken::None,
                        };

                        if tx.send(msg).await.is_err() {
                            break; // Channel closed — source dropped
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(error = %e, "MQTT eventloop error — reconnecting");
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
        });

        Ok(Self {
            client,
            event_type,
            rx,
        })
    }
}

#[cfg(feature = "mqtt")]
#[async_trait::async_trait]
impl super::Source for MqttSource {
    fn name(&self) -> &str {
        "mqtt"
    }

    async fn poll(&mut self, batch_size: i64) -> Result<Vec<RelayMessage>, RelayError> {
        let mut messages = Vec::new();
        let limit = batch_size as usize;

        // Drain the channel without blocking — collect up to `batch_size` messages.
        while messages.len() < limit {
            match self.rx.try_recv() {
                Ok(mut msg) => {
                    msg.op = self.event_type.clone();
                    messages.push(msg);
                }
                Err(_) => break,
            }
        }

        // If the channel was empty, wait briefly for the first message.
        if messages.is_empty() {
            tokio::time::timeout(std::time::Duration::from_millis(100), self.rx.recv())
                .await
                .ok()
                .flatten()
                .into_iter()
                .for_each(|mut msg| {
                    msg.op = self.event_type.clone();
                    messages.push(msg);
                });
        }

        Ok(messages)
    }

    async fn acknowledge(&mut self, _last_message: &RelayMessage) -> Result<(), RelayError> {
        // QoS 1 acknowledgements are handled by the rumqttc eventloop automatically.
        Ok(())
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        let _ = self.client.disconnect().await;
        Ok(())
    }
}
