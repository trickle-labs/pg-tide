/// HTTP webhook sink (RELAY-7).
/// POSTs RelayMessages to a webhook URL with idempotency key header.
/// Feature-gated: only compiled with `--features webhook`.
///
/// v0.13.0: SSRF guard — rejects link-local, loopback, and private-range URLs
/// when `ssrf_protection` is enabled (default: true).  Use `allow_http: true`
/// in dev mode to allow plain HTTP.
use crate::envelope::RelayMessage;
use crate::error::RelayError;

#[cfg(feature = "webhook")]
use reqwest::{Client, Url};

/// Check whether a URL target is safe (SSRF guard).
///
/// Rejects:
/// - non-HTTPS scheme (unless `allow_http = true`)
/// - loopback addresses (127.x.x.x, ::1)
/// - link-local (169.254.x.x, fe80::/10)
/// - private ranges (10.x, 172.16-31.x, 192.168.x) — allowed in dev mode
/// - metadata service addresses (169.254.169.254)
#[cfg(feature = "webhook")]
pub fn validate_webhook_url(
    url: &Url,
    allow_http: bool,
    ssrf_protection: bool,
) -> Result<(), RelayError> {
    crate::http_util::validate_url(url.as_str(), "webhook", allow_http, ssrf_protection)
}

#[cfg(feature = "webhook")]
pub struct WebhookSink {
    client: Client,
    url: Url,
    #[allow(dead_code)]
    timeout_secs: u64,
    allow_http: bool,
    ssrf_protection: bool,
}

#[cfg(feature = "webhook")]
impl WebhookSink {
    pub fn new(url: &str, timeout_secs: u64) -> Result<Self, RelayError> {
        Self::new_with_options(url, timeout_secs, false, true)
    }

    pub fn new_with_options(
        url: &str,
        timeout_secs: u64,
        allow_http: bool,
        ssrf_protection: bool,
    ) -> Result<Self, RelayError> {
        let parsed = Url::parse(url).map_err(|e| RelayError::config(e.to_string()))?;
        validate_webhook_url(&parsed, allow_http, ssrf_protection)?;
        let client = crate::http_util::secure_client_for_url(
            parsed.as_str(),
            "webhook",
            std::time::Duration::from_secs(timeout_secs),
            allow_http,
            ssrf_protection,
        )
        .map_err(|e| RelayError::sink("webhook", e))?;
        Ok(Self {
            client,
            url: parsed,
            timeout_secs,
            allow_http,
            ssrf_protection,
        })
    }
}

#[cfg(feature = "webhook")]
#[async_trait::async_trait]
impl super::Sink for WebhookSink {
    fn name(&self) -> &str {
        "webhook"
    }

    async fn publish(&mut self, messages: &[RelayMessage]) -> Result<(), RelayError> {
        if messages.is_empty() {
            return Ok(());
        }

        // Re-validate URL on each publish (URL could have been mutated via config reload).
        validate_webhook_url(&self.url, self.allow_http, self.ssrf_protection)?;

        // POST the full batch as a JSON array with idempotency key from the last message.
        let idempotency_key = messages
            .last()
            .map(|m| m.dedup_key.clone())
            .unwrap_or_default();

        let payload = serde_json::to_value(messages).map_err(RelayError::Json)?;

        let resp = self
            .client
            .post(self.url.clone())
            .header("Content-Type", "application/json")
            .header("Idempotency-Key", &idempotency_key)
            .json(&payload)
            .send()
            .await
            .map_err(|e| RelayError::sink("webhook", e))?;

        if !resp.status().is_success() {
            return Err(RelayError::SinkPublish {
                sink: "webhook".to_string(),
                source: format!("HTTP {}", resp.status()).into(),
            });
        }
        Ok(())
    }

    async fn is_healthy(&mut self) -> bool {
        true // checked via metrics endpoint
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}

#[cfg(all(test, feature = "webhook"))]
mod tests {
    use super::*;

    #[test]
    fn test_ssrf_blocks_loopback() {
        let url = Url::parse("https://127.0.0.1/callback").unwrap();
        assert!(validate_webhook_url(&url, false, true).is_err());
    }

    #[test]
    fn test_ssrf_blocks_metadata_service() {
        let url = Url::parse("https://169.254.169.254/latest/meta-data").unwrap();
        assert!(validate_webhook_url(&url, false, true).is_err());
    }

    #[test]
    fn test_ssrf_blocks_private_10() {
        let url = Url::parse("https://10.0.0.1/hook").unwrap();
        assert!(validate_webhook_url(&url, false, true).is_err());
    }

    #[test]
    fn test_ssrf_blocks_private_192_168() {
        let url = Url::parse("https://192.168.1.1/hook").unwrap();
        assert!(validate_webhook_url(&url, false, true).is_err());
    }

    #[test]
    fn test_ssrf_blocks_private_172() {
        let url = Url::parse("https://172.16.0.1/hook").unwrap();
        assert!(validate_webhook_url(&url, false, true).is_err());
    }

    #[test]
    fn test_ssrf_allows_public_https() {
        let url = Url::parse("https://webhook.example.com/callback").unwrap();
        assert!(validate_webhook_url(&url, false, true).is_ok());
    }

    #[test]
    fn test_ssrf_blocks_http_by_default() {
        let url = Url::parse("http://webhook.example.com/callback").unwrap();
        assert!(validate_webhook_url(&url, false, true).is_err());
    }

    #[test]
    fn test_ssrf_allows_http_when_explicitly_allowed() {
        let url = Url::parse("http://webhook.example.com/callback").unwrap();
        assert!(validate_webhook_url(&url, true, true).is_ok());
    }

    #[test]
    fn test_ssrf_disabled_allows_private() {
        let url = Url::parse("http://10.0.0.1/hook").unwrap();
        // ssrf_protection=false disables all checks
        assert!(validate_webhook_url(&url, true, false).is_ok());
    }

    #[test]
    fn test_ssrf_blocks_link_local() {
        let url = Url::parse("https://169.254.1.2/hook").unwrap();
        assert!(validate_webhook_url(&url, false, true).is_err());
    }
}
