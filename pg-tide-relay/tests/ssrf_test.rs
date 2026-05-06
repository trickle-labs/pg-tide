//! Integration tests: SSRF guard for webhook sinks (v0.13.0).
//!
//! Tests that the webhook sink correctly rejects unsafe URLs (loopback, link-local,
//! private ranges, HTTP when HTTPS-only is enforced) and allows safe public HTTPS URLs.

#[cfg(feature = "webhook")]
mod webhook_ssrf_tests {
    use pg_tide_relay::sink::webhook::validate_webhook_url;
    use reqwest::Url;

    #[test]
    fn test_ssrf_blocks_loopback_127() {
        let url = Url::parse("https://127.0.0.1/hook").unwrap();
        assert!(
            validate_webhook_url(&url, false, true).is_err(),
            "loopback 127.0.0.1 should be blocked"
        );
    }

    #[test]
    fn test_ssrf_blocks_loopback_localhost() {
        let url = Url::parse("https://localhost/hook").unwrap();
        assert!(
            validate_webhook_url(&url, false, true).is_err(),
            "localhost should be blocked"
        );
    }

    #[test]
    fn test_ssrf_blocks_aws_metadata() {
        let url = Url::parse("https://169.254.169.254/latest/meta-data/").unwrap();
        assert!(
            validate_webhook_url(&url, false, true).is_err(),
            "AWS/GCP metadata endpoint should be blocked"
        );
    }

    #[test]
    fn test_ssrf_blocks_link_local() {
        let url = Url::parse("https://169.254.0.1/hook").unwrap();
        assert!(
            validate_webhook_url(&url, false, true).is_err(),
            "link-local 169.254.x.x should be blocked"
        );
    }

    #[test]
    fn test_ssrf_blocks_private_10_range() {
        let url = Url::parse("https://10.0.0.1/hook").unwrap();
        assert!(
            validate_webhook_url(&url, false, true).is_err(),
            "private 10.x.x.x should be blocked"
        );
    }

    #[test]
    fn test_ssrf_blocks_private_192_168() {
        let url = Url::parse("https://192.168.1.100/hook").unwrap();
        assert!(
            validate_webhook_url(&url, false, true).is_err(),
            "private 192.168.x.x should be blocked"
        );
    }

    #[test]
    fn test_ssrf_blocks_private_172_16_to_31() {
        for second_octet in 16u8..=31 {
            let url = Url::parse(&format!("https://172.{second_octet}.0.1/hook")).unwrap();
            assert!(
                validate_webhook_url(&url, false, true).is_err(),
                "private 172.{second_octet}.x.x should be blocked"
            );
        }
    }

    #[test]
    fn test_ssrf_allows_172_outside_private_range() {
        // 172.32.x.x is NOT in the private range.
        let url = Url::parse("https://172.32.0.1/hook").unwrap();
        assert!(
            validate_webhook_url(&url, false, true).is_ok(),
            "172.32.x.x is public and should be allowed"
        );
    }

    #[test]
    fn test_ssrf_allows_public_https() {
        let url = Url::parse("https://hooks.slack.com/services/T00/B00/secret").unwrap();
        assert!(
            validate_webhook_url(&url, false, true).is_ok(),
            "public HTTPS should be allowed"
        );
    }

    #[test]
    fn test_ssrf_blocks_http_by_default() {
        let url = Url::parse("http://hooks.slack.com/services/T00/B00/secret").unwrap();
        assert!(
            validate_webhook_url(&url, false, true).is_err(),
            "plain HTTP should be rejected by default"
        );
    }

    #[test]
    fn test_ssrf_allows_http_when_explicitly_permitted() {
        let url = Url::parse("http://hooks.example.com/callback").unwrap();
        assert!(
            validate_webhook_url(&url, true, true).is_ok(),
            "HTTP should be allowed when allow_http=true"
        );
    }

    #[test]
    fn test_ssrf_disabled_bypasses_all_checks() {
        // ssrf_protection=false → allow any URL (dev mode).
        let private_url = Url::parse("http://10.0.0.1/hook").unwrap();
        assert!(
            validate_webhook_url(&private_url, true, false).is_ok(),
            "ssrf_protection=false should bypass all checks"
        );
    }
}

// When webhook feature is not enabled, ensure the module compiles without errors.
#[cfg(not(feature = "webhook"))]
#[test]
fn test_webhook_ssrf_feature_not_enabled() {
    // No-op — SSRF guard only exists when the webhook feature is compiled in.
}
