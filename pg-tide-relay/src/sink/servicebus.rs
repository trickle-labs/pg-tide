/// Azure Service Bus sink (RELAY-P2-3).
///
/// Sends messages to an Azure Service Bus queue or topic using the
/// Service Bus REST API with Shared Access Signature (SAS) authentication.
///
/// The connection string format is the standard Azure Service Bus connection
/// string: `Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<name>;SharedAccessKey=<key>`
///
/// Feature-gated: only compiled with `--features servicebus`.
use crate::envelope::RelayMessage;
use crate::error::RelayError;

#[cfg(feature = "servicebus")]
use reqwest::Client;

#[cfg(feature = "servicebus")]
pub struct ServiceBusSink {
    client: Client,
    namespace: String,
    entity: String,
    key_name: String,
    key_value: String,
}

#[cfg(feature = "servicebus")]
impl ServiceBusSink {
    /// Create a new Azure Service Bus sink.
    ///
    /// `connection_string`: Standard Azure SB connection string.
    /// `entity`: Queue or topic name.
    pub fn new(connection_string: &str, entity: impl Into<String>) -> Result<Self, RelayError> {
        let (namespace, key_name, key_value) = parse_connection_string(connection_string)?;

        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| RelayError::sink("servicebus", e))?;

        Ok(Self {
            client,
            namespace,
            entity: entity.into(),
            key_name,
            key_value,
        })
    }

    fn base_url(&self) -> String {
        format!(
            "https://{namespace}.servicebus.windows.net/{entity}",
            namespace = self.namespace,
            entity = self.entity,
        )
    }
}

#[cfg(feature = "servicebus")]
#[async_trait::async_trait]
impl super::Sink for ServiceBusSink {
    fn name(&self) -> &str {
        "servicebus"
    }

    async fn publish(&mut self, messages: &[RelayMessage]) -> Result<(), RelayError> {
        if messages.is_empty() {
            return Ok(());
        }

        let url = format!("{}/messages", self.base_url());
        let resource_uri = url_encode(&format!(
            "{namespace}.servicebus.windows.net/{entity}",
            namespace = self.namespace,
            entity = self.entity,
        ));

        // Azure Service Bus REST API sends messages one at a time or as a
        // batch of up to 256 KB. We send one at a time for simplicity.
        for msg in messages {
            let sas = generate_sas_token(
                &resource_uri,
                &self.key_name,
                &self.key_value,
                300, // 5-minute token validity
            )?;

            let body = serde_json::to_string(&msg.payload).map_err(RelayError::Json)?;

            let resp = self
                .client
                .post(&url)
                .header("Authorization", sas)
                .header("Content-Type", "application/json;charset=utf-8")
                .header(
                    "BrokerProperties",
                    serde_json::json!({
                        "MessageId": msg.dedup_key,
                    })
                    .to_string(),
                )
                .body(body)
                .send()
                .await
                .map_err(|e| RelayError::sink("servicebus", e))?;

            if !resp.status().is_success() {
                let status = resp.status();
                let text = resp.text().await.unwrap_or_default();
                return Err(RelayError::SinkPublish {
                    sink: "servicebus".to_string(),
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

/// Parse an Azure Service Bus connection string.
/// Returns `(namespace, key_name, key_value)`.
#[cfg(feature = "servicebus")]
pub(crate) fn parse_connection_string_pub(
    cs: &str,
) -> Result<(String, String, String), RelayError> {
    parse_connection_string(cs)
}

/// Parse an Azure Service Bus connection string.
/// Returns `(namespace, key_name, key_value)`.
#[cfg(feature = "servicebus")]
fn parse_connection_string(cs: &str) -> Result<(String, String, String), RelayError> {
    let mut namespace = String::new();
    let mut key_name = String::new();
    let mut key_value = String::new();

    for part in cs.split(';') {
        if let Some(rest) = part.strip_prefix("Endpoint=sb://") {
            namespace = rest.trim_end_matches('/').to_string();
            // Strip the `.servicebus.windows.net` suffix if present.
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
            "invalid Azure Service Bus connection string: missing Endpoint, SharedAccessKeyName, or SharedAccessKey",
        ));
    }

    Ok((namespace, key_name, key_value))
}

/// Percent-encode a string for use in a SAS resource URI.
#[cfg(feature = "servicebus")]
pub(crate) fn url_encode_pub(s: &str) -> String {
    url_encode(s)
}

/// Percent-encode a string for use in a SAS resource URI.
#[cfg(feature = "servicebus")]
fn url_encode(s: &str) -> String {
    let mut encoded = String::new();
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            b'/' => encoded.push('/'), // keep slashes for URI paths
            b => encoded.push_str(&format!("%{b:02X}")),
        }
    }
    encoded
}

/// Generate a Shared Access Signature (SAS) token for Azure Service Bus.
#[cfg(feature = "servicebus")]
pub(crate) fn generate_sas_token_pub(
    resource_uri: &str,
    key_name: &str,
    key_value: &str,
    validity_seconds: u64,
) -> Result<String, RelayError> {
    generate_sas_token(resource_uri, key_name, key_value, validity_seconds)
}

/// Generate a Shared Access Signature (SAS) token for Azure Service Bus.
#[cfg(feature = "servicebus")]
fn generate_sas_token(
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
        .map_err(|e| RelayError::config(format!("invalid Service Bus key (base64): {e}")))?;

    let mut mac = Hmac::<Sha256>::new_from_slice(&key_bytes)
        .map_err(|e| RelayError::config(format!("HMAC key error: {e}")))?;
    mac.update(string_to_sign.as_bytes());
    let signature = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());

    let sig_encoded = url_encode(&signature);

    Ok(format!(
        "SharedAccessSignature sr={resource_uri}&sig={sig_encoded}&se={expiry}&skn={key_name}"
    ))
}
