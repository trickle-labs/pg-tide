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
use hmac::{Hmac, Mac};
#[cfg(feature = "webhook")]
use reqwest::{Client, Url};
#[cfg(feature = "webhook")]
use sha2::{Digest, Sha256};

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
    signing_secret: Option<Vec<u8>>,
}

#[cfg(feature = "webhook")]
impl WebhookSink {
    pub fn new(url: &str, timeout_secs: u64) -> Result<Self, RelayError> {
        Self::new_with_options(url, timeout_secs, false, true, None, "hmac-sha256")
    }

    pub fn new_with_options(
        url: &str,
        timeout_secs: u64,
        allow_http: bool,
        ssrf_protection: bool,
        signing_secret: Option<&str>,
        signing_algorithm: &str,
    ) -> Result<Self, RelayError> {
        if !(1..=300).contains(&timeout_secs) {
            return Err(RelayError::InvalidConfig {
                name: "webhook".to_string(),
                reason: "timeout_secs must be between 1 and 300".to_string(),
            });
        }
        if signing_algorithm != "hmac-sha256" {
            return Err(RelayError::config(
                "webhook: signing_algorithm must be 'hmac-sha256'",
            ));
        }
        if signing_secret.is_some_and(str::is_empty) {
            return Err(RelayError::config(
                "webhook: signing_secret must not be empty",
            ));
        }
        let parsed = Url::parse(url).map_err(|e| RelayError::config(e.to_string()))?;
        validate_webhook_url(&parsed, allow_http, ssrf_protection)?;
        let client = crate::http_util::secure_client_for_url(
            parsed.as_str(),
            "webhook",
            std::time::Duration::from_secs(timeout_secs),
            allow_http,
            ssrf_protection,
        )
        .map_err(|_| {
            RelayError::connector_failure(
                "webhook",
                crate::error::ConnectorFailureCode::TlsVerification,
                crate::error::RetryClass::Permanent,
                "webhook TLS client setup failed",
            )
        })?;
        Ok(Self {
            client,
            url: parsed,
            timeout_secs,
            allow_http,
            ssrf_protection,
            signing_secret: signing_secret.map(str::as_bytes).map(ToOwned::to_owned),
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

        let idempotency_key = idempotency_key(messages);
        let body = serde_json::to_vec(messages).map_err(RelayError::Json)?;

        let mut request = self
            .client
            .post(self.url.clone())
            .header("Content-Type", "application/json")
            .header("Idempotency-Key", &idempotency_key);
        if let Some(secret) = &self.signing_secret {
            let mut mac = Hmac::<Sha256>::new_from_slice(secret)
                .map_err(|_| RelayError::config("webhook: invalid signing secret"))?;
            mac.update(&body);
            request = request.header(
                "X-Pg-Tide-Signature",
                format!("sha256={}", hex::encode(mac.finalize().into_bytes())),
            );
        }
        let resp = request
            .body(body)
            .send()
            .await
            .map_err(webhook_request_error)?;

        if !resp.status().is_success() {
            return Err(webhook_status_error(resp.status().as_u16()));
        }
        Ok(())
    }

    async fn is_healthy(&mut self) -> bool {
        validate_webhook_url(&self.url, self.allow_http, self.ssrf_protection).is_ok()
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}

#[cfg(feature = "webhook")]
fn idempotency_key(messages: &[RelayMessage]) -> String {
    if messages.len() == 1 {
        return messages[0].dedup_key.clone();
    }
    let mut digest = Sha256::new();
    for message in messages {
        digest.update((message.dedup_key.len() as u64).to_be_bytes());
        digest.update(message.dedup_key.as_bytes());
    }
    format!("batch-{}", hex::encode(digest.finalize()))
}

#[cfg(feature = "webhook")]
fn webhook_request_error(error: reqwest::Error) -> RelayError {
    use crate::error::{ConnectorFailureCode, RetryClass};

    let (code, class, summary) = if error.is_timeout() {
        (
            ConnectorFailureCode::Timeout,
            RetryClass::Transient,
            "webhook request timed out",
        )
    } else if error.is_connect() {
        (
            ConnectorFailureCode::Unavailable,
            RetryClass::Transient,
            "webhook endpoint is unavailable",
        )
    } else if error.is_redirect() {
        (
            ConnectorFailureCode::InvalidDestination,
            RetryClass::Permanent,
            "webhook redirects are not allowed",
        )
    } else {
        (
            ConnectorFailureCode::Unknown,
            RetryClass::Transient,
            "webhook request failed",
        )
    };
    RelayError::connector_failure("webhook", code, class, summary)
}

#[cfg(feature = "webhook")]
fn webhook_status_error(status: u16) -> RelayError {
    use crate::error::{ConnectorFailureCode, RetryClass};

    let (code, class, summary) = match status {
        408 | 425 | 429 | 500..=599 => (
            ConnectorFailureCode::Throttled,
            RetryClass::Transient,
            "webhook endpoint requested retry",
        ),
        300..=399 => (
            ConnectorFailureCode::InvalidDestination,
            RetryClass::Permanent,
            "webhook redirects are not allowed",
        ),
        401 | 403 => (
            ConnectorFailureCode::Authentication,
            RetryClass::Permanent,
            "webhook authentication was rejected",
        ),
        400..=499 => (
            ConnectorFailureCode::ProtocolRejection,
            RetryClass::Permanent,
            "webhook request was rejected",
        ),
        _ => (
            ConnectorFailureCode::Unknown,
            RetryClass::Transient,
            "webhook response was invalid",
        ),
    };
    RelayError::connector_failure("webhook", code, class, summary)
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

    #[test]
    fn batch_idempotency_key_is_deterministic() {
        let first = RelayMessage::new_reverse("event-1", "orders", serde_json::json!({}));
        let second = RelayMessage::new_reverse("event-2", "orders", serde_json::json!({}));
        let left = idempotency_key(&[first.clone(), second.clone()]);
        let right = idempotency_key(&[first, second]);
        assert_eq!(left, right);
        assert_ne!(
            left,
            idempotency_key(&[RelayMessage::new_reverse(
                "event-3",
                "orders",
                serde_json::json!({}),
            )])
        );
    }

    #[test]
    fn retryable_webhook_statuses_are_typed() {
        let error = webhook_status_error(429);
        assert_eq!(error.retry_class(), crate::error::RetryClass::Transient);
        assert_eq!(
            error.connector_code(),
            Some(crate::error::ConnectorFailureCode::Throttled)
        );
        let error = webhook_status_error(403);
        assert_eq!(error.retry_class(), crate::error::RetryClass::Permanent);
        assert_eq!(
            error.connector_code(),
            Some(crate::error::ConnectorFailureCode::Authentication)
        );
    }
}
