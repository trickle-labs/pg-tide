/// Azure Event Hubs sink (v0.6.0 — RELAY-P3-4).
///
/// Sends messages to an Azure Event Hub using the Event Hubs REST API
/// with Shared Access Signature (SAS) authentication.
///
/// The connection string format is the standard Azure Event Hubs connection
/// string: `Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<name>;SharedAccessKey=<key>`
///
/// **Note:** Azure Event Hubs and Azure Service Bus share the same namespace
/// infrastructure and SAS token mechanism. The Event Hubs REST send endpoint
/// is identical to the Service Bus endpoint format.
///
/// Feature-gated: only compiled with `--features eventhubs`.
use crate::envelope::RelayMessage;
use crate::error::RelayError;

#[cfg(feature = "eventhubs")]
use reqwest::Client;

#[cfg(feature = "eventhubs")]
pub struct EventHubsSink {
    client: Client,
    namespace: String,
    event_hub: String,
    key_name: String,
    key_value: String,
    partition_key_template: String,
}

#[cfg(feature = "eventhubs")]
impl EventHubsSink {
    /// Create a new Azure Event Hubs sink.
    ///
    /// `connection_string`: Standard Azure Event Hubs connection string.
    /// `event_hub`: Event Hub name.
    /// `partition_key_template`: Template for the partition key (for ordering within a partition).
    pub fn new(
        connection_string: &str,
        event_hub: impl Into<String>,
        partition_key_template: impl Into<String>,
    ) -> Result<Self, RelayError> {
        let (namespace, key_name, key_value) =
            parse_eventhubs_connection_string(connection_string)?;
        crate::http_util::validate_url(
            &format!("https://{namespace}.servicebus.windows.net"),
            "eventhubs",
            false,
            true,
        )?;

        let endpoint = format!("https://{namespace}.servicebus.windows.net");
        let client = crate::http_util::secure_client_for_url(
            &endpoint,
            "eventhubs",
            std::time::Duration::from_secs(30),
            false,
            true,
        )
        .map_err(|e| RelayError::sink("eventhubs", e))?;

        Ok(Self {
            client,
            namespace,
            event_hub: event_hub.into(),
            key_name,
            key_value,
            partition_key_template: partition_key_template.into(),
        })
    }

    fn send_url(&self) -> String {
        format!(
            "https://{ns}.servicebus.windows.net/{eh}/messages",
            ns = self.namespace,
            eh = self.event_hub,
        )
    }

    fn resource_uri(&self) -> String {
        url_encode_eventhubs(&format!(
            "{ns}.servicebus.windows.net/{eh}",
            ns = self.namespace,
            eh = self.event_hub,
        ))
    }

    fn sas_token(&self) -> Result<String, RelayError> {
        generate_eventhubs_sas_token(&self.resource_uri(), &self.key_name, &self.key_value, 300)
    }
}

#[cfg(feature = "eventhubs")]
#[async_trait::async_trait]
impl super::Sink for EventHubsSink {
    fn name(&self) -> &str {
        "eventhubs"
    }

    /// Publish a batch of messages to the Event Hub.
    ///
    /// Messages are sent individually. Each message includes a `BrokerProperties`
    /// header with `PartitionKey` set to the rendered partition key template.
    async fn publish(&mut self, messages: &[RelayMessage]) -> Result<(), RelayError> {
        if messages.is_empty() {
            return Ok(());
        }

        let url = self.send_url();

        for msg in messages {
            let sas = self.sas_token()?;

            let partition_key = crate::envelope::render_subject(
                &self.partition_key_template,
                &msg.subject,
                &msg.op,
                msg.outbox_id.unwrap_or(0),
                msg.refresh_id,
            );

            let body = serde_json::to_string(&msg.payload).map_err(RelayError::Json)?;

            let resp = self
                .client
                .post(&url)
                .header("Authorization", sas)
                .header("Content-Type", "application/json;charset=utf-8")
                .header(
                    "BrokerProperties",
                    serde_json::json!({
                        "PartitionKey": partition_key,
                        "MessageId": msg.dedup_key,
                    })
                    .to_string(),
                )
                .body(body)
                .send()
                .await
                .map_err(|e| RelayError::sink("eventhubs", e))?;

            if !resp.status().is_success() {
                let status = resp.status();
                let text = resp.text().await.unwrap_or_default();
                return Err(RelayError::SinkPublish {
                    sink: "eventhubs".to_string(),
                    source: format!("HTTP {status}: {text}").into(),
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

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Parse an Azure Event Hubs (or Service Bus) connection string.
/// Returns `(namespace, key_name, key_value)`.
#[cfg(feature = "eventhubs")]
pub(crate) fn parse_eventhubs_connection_string(
    cs: &str,
) -> Result<(String, String, String), RelayError> {
    let mut namespace = String::new();
    let mut key_name = String::new();
    let mut key_value = String::new();

    for part in cs.split(';') {
        if let Some(rest) = part.strip_prefix("Endpoint=sb://") {
            namespace = rest.trim_end_matches('/').to_string();
            if let Some(host) = namespace.strip_suffix(".servicebus.windows.net") {
                namespace = host.to_string();
            }
        } else if let Some(rest) = part.strip_prefix("SharedAccessKeyName=") {
            key_name = rest.to_string();
        } else if let Some(rest) = part.strip_prefix("SharedAccessKey=") {
            key_value = rest.to_string();
        }
    }

    if namespace.is_empty() || key_name.is_empty() || key_value.is_empty() {
        return Err(RelayError::config(
            "invalid Azure Event Hubs connection string: missing Endpoint, SharedAccessKeyName, or SharedAccessKey",
        ));
    }

    Ok((namespace, key_name, key_value))
}

/// Percent-encode a string for use in an Azure SAS resource URI.
#[cfg(feature = "eventhubs")]
pub(crate) fn url_encode_eventhubs(s: &str) -> String {
    let mut encoded = String::new();
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            b'/' => encoded.push('/'),
            b => encoded.push_str(&format!("%{b:02X}")),
        }
    }
    encoded
}

/// Generate an Azure Shared Access Signature (SAS) token for Event Hubs.
#[cfg(feature = "eventhubs")]
pub(crate) fn generate_eventhubs_sas_token(
    resource_uri: &str,
    key_name: &str,
    key_value: &str,
    validity_seconds: u64,
) -> Result<String, RelayError> {
    use base64::Engine as _;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    let expiry = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| RelayError::config(format!("system time error: {e}")))?
        .as_secs()
        + validity_seconds;

    let string_to_sign = format!("{resource_uri}\n{expiry}");

    let key_bytes = base64::engine::general_purpose::STANDARD
        .decode(key_value)
        .map_err(|e| RelayError::config(format!("invalid Event Hubs key (base64): {e}")))?;

    let mut mac = Hmac::<Sha256>::new_from_slice(&key_bytes)
        .map_err(|e| RelayError::config(format!("HMAC key error: {e}")))?;
    mac.update(string_to_sign.as_bytes());
    let signature = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());

    let sig_encoded = url_encode_eventhubs(&signature);

    Ok(format!(
        "SharedAccessSignature sr={resource_uri}&sig={sig_encoded}&se={expiry}&skn={key_name}"
    ))
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(all(test, feature = "eventhubs"))]
mod tests {
    use super::*;

    #[test]
    fn test_parse_eventhubs_connection_string() {
        let cs = "Endpoint=sb://myhub.servicebus.windows.net/;\
                  SharedAccessKeyName=RootManageSharedAccessKey;\
                  SharedAccessKey=dGVzdGtleWJhc2U2NA==";
        let (ns, kn, kv) = parse_eventhubs_connection_string(cs).unwrap();
        assert_eq!(ns, "myhub");
        assert_eq!(kn, "RootManageSharedAccessKey");
        assert_eq!(kv, "dGVzdGtleWJhc2U2NA==");
    }

    #[test]
    fn test_parse_missing_key_returns_error() {
        let cs = "Endpoint=sb://myhub.servicebus.windows.net/;SharedAccessKeyName=foo";
        assert!(parse_eventhubs_connection_string(cs).is_err());
    }
}
