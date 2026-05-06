/// HTTP webhook receiver source (RELAY-25) with signature verification (RELAY-P2-21).
///
/// Starts an axum HTTP server that accepts POST requests and converts them to RelayMessages.
/// Supports HMAC-SHA256, GitHub, Stripe, Svix, and Fivetran signature verification schemes.
///
/// Feature-gated: only compiled with `--features webhook`.
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::envelope::RelayMessage;
use crate::error::RelayError;

/// Webhook signature verification scheme.
#[derive(Debug, Clone, Default)]
pub enum SignatureScheme {
    /// No signature verification.
    #[default]
    None,
    /// Generic HMAC-SHA256: `HMAC(secret, body)` compared to the configured header.
    HmacSha256 { secret: String, header: String },
    /// GitHub webhook signature: `sha256=HMAC(secret, body)` in `X-Hub-Signature-256`.
    GitHub { secret: String },
    /// Stripe webhook signature: `Stripe-Signature` header with timestamp + HMAC.
    Stripe {
        secret: String,
        tolerance_seconds: u64,
    },
    /// Svix webhook signature scheme (used by many SaaS platforms).
    Svix { secret: String },
    /// Fivetran HVR webhook signature: `X-Fivetran-Signature` with `sha256=HMAC(secret, body)`.
    Fivetran { secret: String },
}

impl SignatureScheme {
    /// Parse signature scheme from a pipeline's JSON source config.
    pub fn from_config(config: &serde_json::Value) -> Self {
        let sig = match config.get("signature") {
            Some(s) => s,
            None => return Self::None,
        };

        let scheme = sig.get("scheme").and_then(|v| v.as_str()).unwrap_or("none");

        let secret = sig
            .get("secret")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        match scheme {
            "hmac-sha256" => {
                let header = sig
                    .get("header")
                    .and_then(|v| v.as_str())
                    .unwrap_or("X-Webhook-Signature")
                    .to_string();
                Self::HmacSha256 { secret, header }
            }
            "github" => Self::GitHub { secret },
            "stripe" => {
                let tolerance = sig
                    .get("tolerance_seconds")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(300);
                Self::Stripe {
                    secret,
                    tolerance_seconds: tolerance,
                }
            }
            "svix" => Self::Svix { secret },
            "fivetran" => Self::Fivetran { secret },
            _ => Self::None,
        }
    }

    /// Verify the signature on an incoming request.
    ///
    /// Returns `Ok(())` if the signature is valid or no verification is configured.
    /// Returns `Err(reason)` if the signature is missing or invalid.
    #[cfg(feature = "webhook")]
    pub fn verify(&self, body: &[u8], headers: &axum::http::HeaderMap) -> Result<(), String> {
        match self {
            Self::None => Ok(()),
            Self::HmacSha256 { secret, header } => {
                verify_hmac_sha256(body, headers, header, secret)
            }
            Self::GitHub { secret } => verify_github(body, headers, secret),
            Self::Stripe {
                secret,
                tolerance_seconds,
            } => verify_stripe(body, headers, secret, *tolerance_seconds),
            Self::Svix { secret } => verify_svix(body, headers, secret),
            Self::Fivetran { secret } => verify_fivetran(body, headers, secret),
        }
    }
}

/// Compute HMAC-SHA256 of `body` with `key`, returning a lowercase hex string.
#[cfg(feature = "webhook")]
fn hmac_sha256_hex(key: &str, body: &[u8]) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    // SAFETY: Hmac::new_from_slice accepts keys of any length per the HMAC spec.
    let mut mac =
        <Hmac<Sha256>>::new_from_slice(key.as_bytes()).expect("HMAC accepts any key size");
    mac.update(body);
    let result = mac.finalize();
    let bytes = result.into_bytes();
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Constant-time comparison to prevent timing attacks.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

#[cfg(feature = "webhook")]
fn verify_hmac_sha256(
    body: &[u8],
    headers: &axum::http::HeaderMap,
    header_name: &str,
    secret: &str,
) -> Result<(), String> {
    let sig_header = headers
        .get(header_name)
        .ok_or_else(|| format!("missing signature header: {header_name}"))?;
    let sig_str = sig_header
        .to_str()
        .map_err(|_| "signature header is not valid UTF-8".to_string())?;

    let expected = hmac_sha256_hex(secret, body);
    if !constant_time_eq(expected.as_bytes(), sig_str.as_bytes()) {
        return Err("HMAC-SHA256 signature mismatch".to_string());
    }
    Ok(())
}

#[cfg(feature = "webhook")]
fn verify_github(body: &[u8], headers: &axum::http::HeaderMap, secret: &str) -> Result<(), String> {
    let sig_header = headers
        .get("X-Hub-Signature-256")
        .ok_or("missing X-Hub-Signature-256 header")?;
    let sig_str = sig_header
        .to_str()
        .map_err(|_| "X-Hub-Signature-256 is not valid UTF-8")?;

    let sig_hex = sig_str
        .strip_prefix("sha256=")
        .ok_or("X-Hub-Signature-256 must start with 'sha256='")?;

    let expected = hmac_sha256_hex(secret, body);
    if !constant_time_eq(expected.as_bytes(), sig_hex.as_bytes()) {
        return Err("GitHub webhook HMAC-SHA256 signature mismatch".to_string());
    }
    Ok(())
}

#[cfg(feature = "webhook")]
fn verify_stripe(
    body: &[u8],
    headers: &axum::http::HeaderMap,
    secret: &str,
    tolerance_seconds: u64,
) -> Result<(), String> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    let sig_header = headers
        .get("Stripe-Signature")
        .ok_or("missing Stripe-Signature header")?;
    let sig_str = sig_header
        .to_str()
        .map_err(|_| "Stripe-Signature is not valid UTF-8")?;

    let mut timestamp: Option<&str> = None;
    let mut signatures: Vec<&str> = Vec::new();
    for part in sig_str.split(',') {
        if let Some(t) = part.strip_prefix("t=") {
            timestamp = Some(t);
        } else if let Some(s) = part.strip_prefix("v1=") {
            signatures.push(s);
        }
    }

    let ts = timestamp.ok_or("Stripe-Signature missing timestamp (t=)")?;

    if tolerance_seconds > 0 {
        let ts_val: u64 = ts
            .parse()
            .map_err(|_| "Stripe-Signature timestamp is not a number")?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if now.saturating_sub(ts_val) > tolerance_seconds {
            return Err(format!(
                "Stripe-Signature timestamp too old (tolerance: {tolerance_seconds}s)"
            ));
        }
    }

    let body_str = std::str::from_utf8(body).map_err(|_| "request body is not valid UTF-8")?;
    let signed_payload = format!("{ts}.{body_str}");

    let mut mac = <Hmac<Sha256>>::new_from_slice(secret.as_bytes())
        .map_err(|e| format!("Stripe HMAC key error: {e}"))?;
    mac.update(signed_payload.as_bytes());
    let expected_bytes = mac.finalize().into_bytes();
    let expected_hex: String = expected_bytes.iter().map(|b| format!("{b:02x}")).collect();

    for sig in &signatures {
        if constant_time_eq(expected_hex.as_bytes(), sig.as_bytes()) {
            return Ok(());
        }
    }
    Err("Stripe webhook HMAC-SHA256 signature mismatch".to_string())
}

#[cfg(feature = "webhook")]
fn verify_svix(body: &[u8], headers: &axum::http::HeaderMap, secret: &str) -> Result<(), String> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    let msg_id = headers
        .get("svix-id")
        .ok_or("missing svix-id header")?
        .to_str()
        .map_err(|_| "svix-id is not valid UTF-8")?;

    let timestamp = headers
        .get("svix-timestamp")
        .ok_or("missing svix-timestamp header")?
        .to_str()
        .map_err(|_| "svix-timestamp is not valid UTF-8")?;

    let sig_header = headers
        .get("svix-signature")
        .ok_or("missing svix-signature header")?
        .to_str()
        .map_err(|_| "svix-signature is not valid UTF-8")?;

    let body_str = std::str::from_utf8(body).map_err(|_| "request body is not valid UTF-8")?;
    let signed_payload = format!("{msg_id}.{timestamp}.{body_str}");

    // Svix secrets are base64-encoded; strip the "whsec_" prefix if present.
    let secret_b64 = secret.trim_start_matches("whsec_");
    let secret_bytes = simple_base64_decode(secret_b64)
        .map_err(|e| format!("Svix secret is not valid base64: {e}"))?;

    let mut mac = <Hmac<Sha256>>::new_from_slice(&secret_bytes)
        .map_err(|e| format!("Svix HMAC key error: {e}"))?;
    mac.update(signed_payload.as_bytes());
    let expected_bytes = mac.finalize().into_bytes();

    for part in sig_header.split(' ') {
        if let Some(b64) = part.strip_prefix("v1,") {
            if let Ok(sig_bytes) = simple_base64_decode(b64) {
                if constant_time_eq(expected_bytes.as_slice(), &sig_bytes) {
                    return Ok(());
                }
            }
        }
    }
    Err("Svix webhook signature mismatch".to_string())
}

/// Minimal base64 decoder supporting standard and URL-safe alphabets.
fn simple_base64_decode(s: &str) -> Result<Vec<u8>, String> {
    fn decode_char(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' | b'-' => Some(62),
            b'/' | b'_' => Some(63),
            _ => None,
        }
    }

    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity((bytes.len() / 4) * 3 + 3);
    let mut i = 0;
    while i + 1 < bytes.len() {
        let a = match decode_char(bytes[i]) {
            Some(v) => v,
            None => {
                i += 1;
                continue;
            }
        };
        let b = match decode_char(bytes[i + 1]) {
            Some(v) => v,
            None => {
                i += 2;
                continue;
            }
        };
        out.push((a << 2) | (b >> 4));

        if i + 2 < bytes.len() {
            if let Some(c) = decode_char(bytes[i + 2]) {
                out.push((b << 4) | (c >> 2));
                if i + 3 < bytes.len() {
                    if let Some(d) = decode_char(bytes[i + 3]) {
                        out.push((c << 6) | d);
                    }
                }
            }
        }
        i += 4;
    }
    Ok(out)
}

/// Verify a Fivetran HVR webhook signature.
///
/// Fivetran sends `X-Fivetran-Signature` with `sha256=<hex_hmac>`.
/// Reference: https://fivetran.com/docs/getting-started/hybrid-deployment
#[cfg(feature = "webhook")]
fn verify_fivetran(
    body: &[u8],
    headers: &axum::http::HeaderMap,
    secret: &str,
) -> Result<(), String> {
    let sig_header = headers
        .get("X-Fivetran-Signature")
        .ok_or("missing X-Fivetran-Signature header")?;
    let sig_str = sig_header
        .to_str()
        .map_err(|_| "X-Fivetran-Signature is not valid UTF-8")?;

    // Strip "sha256=" prefix if present.
    let sig_hex = sig_str.strip_prefix("sha256=").unwrap_or(sig_str);

    let expected = hmac_sha256_hex(secret, body);
    if !constant_time_eq(expected.as_bytes(), sig_hex.as_bytes()) {
        return Err("Fivetran webhook HMAC-SHA256 signature mismatch".to_string());
    }
    Ok(())
}

#[cfg(feature = "webhook")]
pub struct WebhookSource {
    rx: mpsc::Receiver<RelayMessage>,
    #[allow(dead_code)]
    event_type: String,
}

#[cfg(feature = "webhook")]
impl WebhookSource {
    pub async fn bind(addr: &str, event_type: impl Into<String>) -> Result<Self, RelayError> {
        Self::bind_with_signature(addr, event_type, SignatureScheme::None).await
    }

    pub async fn bind_with_signature(
        addr: &str,
        event_type: impl Into<String>,
        signature: SignatureScheme,
    ) -> Result<Self, RelayError> {
        use axum::{
            body::Bytes,
            http::{HeaderMap, StatusCode},
            routing::post,
            Router,
        };

        let (tx, rx) = mpsc::channel::<RelayMessage>(1024);
        let tx = Arc::new(tx);
        let event_type_clone = event_type.into();
        let sig = Arc::new(signature);

        let app = Router::new().route(
            "/",
            post({
                let tx = Arc::clone(&tx);
                let et = event_type_clone.clone();
                let sig = Arc::clone(&sig);
                move |headers: HeaderMap, body: Bytes| {
                    let tx = Arc::clone(&tx);
                    let et = et.clone();
                    let sig = Arc::clone(&sig);
                    async move {
                        if let Err(reason) = sig.verify(&body, &headers) {
                            tracing::warn!("webhook signature verification failed: {reason}");
                            return StatusCode::UNAUTHORIZED;
                        }

                        let payload: serde_json::Value = match serde_json::from_slice(&body) {
                            Ok(v) => v,
                            Err(e) => {
                                tracing::warn!("webhook body parse error: {e}");
                                return StatusCode::BAD_REQUEST;
                            }
                        };

                        let dedup_key = headers
                            .get("Idempotency-Key")
                            .and_then(|v| v.to_str().ok())
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                        let event_type = payload
                            .get("event_type")
                            .and_then(|v| v.as_str())
                            .unwrap_or(&et)
                            .to_string();
                        let msg = RelayMessage::new_reverse(dedup_key, event_type, payload);
                        let _ = tx.send(msg).await;
                        StatusCode::OK
                    }
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(RelayError::Io)?;

        tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                tracing::error!("webhook receiver error: {e}");
            }
        });

        Ok(Self {
            rx,
            event_type: event_type_clone,
        })
    }
}

#[cfg(feature = "webhook")]
#[async_trait::async_trait]
impl super::Source for WebhookSource {
    fn name(&self) -> &str {
        "webhook-receiver"
    }

    async fn poll(&mut self, batch_size: i64) -> Result<Vec<RelayMessage>, RelayError> {
        let mut messages = Vec::new();
        for _ in 0..batch_size {
            match self.rx.try_recv() {
                Ok(msg) => messages.push(msg),
                Err(_) => break,
            }
        }
        Ok(messages)
    }

    async fn acknowledge(&mut self, _last_message: &RelayMessage) -> Result<(), RelayError> {
        // HTTP webhook: response (200) was already sent synchronously in the handler.
        Ok(())
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scheme_parse_none_when_missing() {
        let config = serde_json::json!({});
        let scheme = SignatureScheme::from_config(&config);
        assert!(matches!(scheme, SignatureScheme::None));
    }

    #[test]
    fn test_scheme_parse_github() {
        let config = serde_json::json!({
            "signature": { "scheme": "github", "secret": "mysecret" }
        });
        let scheme = SignatureScheme::from_config(&config);
        assert!(matches!(scheme, SignatureScheme::GitHub { .. }));
    }

    #[test]
    fn test_scheme_parse_hmac() {
        let config = serde_json::json!({
            "signature": {
                "scheme": "hmac-sha256",
                "secret": "s",
                "header": "X-My-Sig"
            }
        });
        let scheme = SignatureScheme::from_config(&config);
        assert!(matches!(scheme, SignatureScheme::HmacSha256 { .. }));
    }

    #[test]
    fn test_scheme_parse_svix() {
        let config = serde_json::json!({
            "signature": { "scheme": "svix", "secret": "whsec_abc" }
        });
        let scheme = SignatureScheme::from_config(&config);
        assert!(matches!(scheme, SignatureScheme::Svix { .. }));
    }

    #[cfg(feature = "webhook")]
    #[test]
    fn test_none_scheme_always_passes() {
        let scheme = SignatureScheme::None;
        let headers = axum::http::HeaderMap::new();
        assert!(scheme.verify(b"hello", &headers).is_ok());
    }

    #[cfg(feature = "webhook")]
    #[test]
    fn test_hmac_sha256_valid_signature() {
        let body = b"test payload";
        let secret = "mysecret";
        let sig = hmac_sha256_hex(secret, body);
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::HeaderName::from_static("x-webhook-signature"),
            axum::http::HeaderValue::from_str(&sig).unwrap(),
        );
        let scheme = SignatureScheme::HmacSha256 {
            secret: secret.to_string(),
            header: "x-webhook-signature".to_string(),
        };
        assert!(scheme.verify(body, &headers).is_ok());
    }

    #[cfg(feature = "webhook")]
    #[test]
    fn test_hmac_sha256_invalid_signature() {
        let body = b"test payload";
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::HeaderName::from_static("x-webhook-signature"),
            axum::http::HeaderValue::from_static("wrong-sig"),
        );
        let scheme = SignatureScheme::HmacSha256 {
            secret: "mysecret".to_string(),
            header: "x-webhook-signature".to_string(),
        };
        assert!(scheme.verify(body, &headers).is_err());
    }

    #[cfg(feature = "webhook")]
    #[test]
    fn test_github_valid_signature() {
        let body = b"github payload";
        let secret = "github_secret";
        let sig = format!("sha256={}", hmac_sha256_hex(secret, body));
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::HeaderName::from_static("x-hub-signature-256"),
            axum::http::HeaderValue::from_str(&sig).unwrap(),
        );
        let scheme = SignatureScheme::GitHub {
            secret: secret.to_string(),
        };
        assert!(scheme.verify(body, &headers).is_ok());
    }

    #[cfg(feature = "webhook")]
    #[test]
    fn test_github_invalid_signature() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::HeaderName::from_static("x-hub-signature-256"),
            axum::http::HeaderValue::from_static("sha256=badhash"),
        );
        let scheme = SignatureScheme::GitHub {
            secret: "secret".to_string(),
        };
        assert!(scheme.verify(b"body", &headers).is_err());
    }
}
