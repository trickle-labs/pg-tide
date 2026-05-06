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
    if !allow_http && url.scheme() != "https" {
        return Err(RelayError::config(format!(
            "webhook URL must use HTTPS (got '{}'). Set allow_http=true to override.",
            url.scheme()
        )));
    }

    if !ssrf_protection {
        return Ok(());
    }

    let host = url.host_str().unwrap_or("");

    // Block loopback.
    if host == "localhost" || host == "127.0.0.1" || host == "::1" || host.starts_with("127.") {
        return Err(RelayError::config(format!(
            "SSRF guard: loopback target '{}' is not allowed in production",
            host
        )));
    }

    // Block link-local metadata service (AWS/GCP/Azure instance metadata).
    if host == "169.254.169.254" || host.starts_with("169.254.") {
        return Err(RelayError::config(format!(
            "SSRF guard: link-local/metadata target '{}' is blocked",
            host
        )));
    }

    // Block other link-local ranges.
    if host.starts_with("fe80:") || host.starts_with("[fe80:") {
        return Err(RelayError::config(format!(
            "SSRF guard: IPv6 link-local target '{}' is blocked",
            host
        )));
    }

    // Block private ranges (RFC 1918).
    if host.starts_with("10.") || host.starts_with("192.168.") || is_private_172(host) {
        return Err(RelayError::config(format!(
            "SSRF guard: private-range target '{}' is blocked. \
             Set ssrf_protection=false to allow private targets in dev mode.",
            host
        )));
    }

    Ok(())
}

/// Check whether an IP string falls in the 172.16.0.0/12 range.
#[cfg(feature = "webhook")]
fn is_private_172(host: &str) -> bool {
    if let Some(rest) = host.strip_prefix("172.") {
        if let Some(second_octet_str) = rest.split('.').next() {
            if let Ok(n) = second_octet_str.parse::<u8>() {
                return (16..=31).contains(&n);
            }
        }
    }
    false
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
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_secs))
            .build()
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
