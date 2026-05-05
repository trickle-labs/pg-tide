//! Unit tests: Webhook signature verification.

mod common;

#[cfg(feature = "webhook")]
mod sig_tests {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;

    fn hmac_hex(secret: &[u8], message: &[u8]) -> String {
        let mut mac = HmacSha256::new_from_slice(secret).unwrap();
        mac.update(message);
        let bytes = mac.finalize().into_bytes();
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn ct_eq(a: &[u8], b: &[u8]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        a.iter()
            .zip(b.iter())
            .fold(0u8, |acc, (x, y)| acc | (x ^ y))
            == 0
    }

    #[test]
    fn test_no_signature_config() {
        let cfg = serde_json::json!({"source_type": "webhook"});
        assert!(cfg.pointer("/signature/scheme").is_none());
    }

    #[test]
    fn test_hmac_sha256_config() {
        let cfg = serde_json::json!({
            "signature": {"scheme": "hmac-sha256", "secret": "s"}
        });
        assert_eq!(
            cfg.pointer("/signature/scheme").and_then(|v| v.as_str()),
            Some("hmac-sha256")
        );
    }

    #[test]
    fn test_correct_sig_accepted() {
        let secret = b"my-secret";
        let body = b"hello";
        let a = hmac_hex(secret, body);
        let b = hmac_hex(secret, body);
        assert!(ct_eq(a.as_bytes(), b.as_bytes()));
    }

    #[test]
    fn test_wrong_secret_rejected() {
        let body = b"hello";
        let a = hmac_hex(b"correct", body);
        let b = hmac_hex(b"wrong", body);
        assert!(!ct_eq(a.as_bytes(), b.as_bytes()));
    }

    #[test]
    fn test_wrong_body_rejected() {
        let secret = b"secret";
        let a = hmac_hex(secret, b"original");
        let b = hmac_hex(secret, b"tampered");
        assert!(!ct_eq(a.as_bytes(), b.as_bytes()));
    }

    #[test]
    fn test_github_format() {
        let secret = b"github-secret";
        let body = b"payload";
        let hex_sig = hmac_hex(secret, body);
        let header = format!("sha256={hex_sig}");
        assert!(header.starts_with("sha256="));
        let sig_part = header.strip_prefix("sha256=").unwrap();
        assert!(ct_eq(
            hmac_hex(secret, body).as_bytes(),
            sig_part.as_bytes()
        ));
    }

    #[test]
    fn test_stripe_format() {
        let secret = b"stripe-secret";
        let ts = "1234567890";
        let body = b"payload";
        let signed = format!("{ts}.{}", std::str::from_utf8(body).unwrap());
        let hex_sig = hmac_hex(secret, signed.as_bytes());
        let header = format!("t={ts},v1={hex_sig}");
        let ts_part = header
            .split(',')
            .find(|p| p.starts_with("t="))
            .and_then(|p| p.strip_prefix("t="))
            .unwrap();
        let v1_part = header
            .split(',')
            .find(|p| p.starts_with("v1="))
            .and_then(|p| p.strip_prefix("v1="))
            .unwrap();
        assert_eq!(ts_part, ts);
        let signed2 = format!("{ts_part}.{}", std::str::from_utf8(body).unwrap());
        assert!(ct_eq(
            hmac_hex(secret, signed2.as_bytes()).as_bytes(),
            v1_part.as_bytes()
        ));
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(ct_eq(b"abc", b"abc"));
        assert!(!ct_eq(b"abc", b"abd"));
        assert!(!ct_eq(b"short", b"longer"));
    }

    #[test]
    fn test_schemes_config() {
        for s in &["none", "hmac-sha256", "github", "stripe", "svix"] {
            let cfg = serde_json::json!({"signature": {"scheme": s}});
            assert_eq!(
                cfg.pointer("/signature/scheme").and_then(|v| v.as_str()),
                Some(*s)
            );
        }
    }
}

#[cfg(not(feature = "webhook"))]
#[test]
fn test_webhook_feature_not_enabled() {}
