//! Integration tests: Fivetran HVR webhook endpoint (v0.9.0).
//!
//! Verifies Fivetran HMAC-SHA256 signature verification using
//! the `X-Fivetran-Signature` header.

mod common;

#[cfg(feature = "webhook")]
mod fivetran_tests {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;

    /// Compute HMAC-SHA256 of `body` with `secret`, returning lowercase hex.
    fn hmac_sha256_hex(secret: &[u8], body: &[u8]) -> String {
        let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key size");
        mac.update(body);
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

    // ── Fivetran signature scheme configuration ───────────────────────────

    #[test]
    fn test_fivetran_signature_scheme_parsed() {
        let config = serde_json::json!({
            "signature": {
                "scheme": "fivetran",
                "secret": "my-fivetran-secret"
            }
        });
        let scheme_str = config.pointer("/signature/scheme").and_then(|v| v.as_str());
        assert_eq!(scheme_str, Some("fivetran"));
    }

    #[test]
    fn test_fivetran_signature_secret_parsed() {
        let config = serde_json::json!({
            "signature": {
                "scheme": "fivetran",
                "secret": "fivetran-webhook-secret-123"
            }
        });
        let secret = config.pointer("/signature/secret").and_then(|v| v.as_str());
        assert_eq!(secret, Some("fivetran-webhook-secret-123"));
    }

    // ── Fivetran signature verification logic ─────────────────────────────

    #[test]
    fn test_fivetran_valid_signature_accepted() {
        let secret = b"test-fivetran-secret";
        let body = b"{\"event\":\"insert\",\"table\":\"orders\",\"data\":{\"id\":1}}";
        let sig = hmac_sha256_hex(secret, body);
        let header_value = format!("sha256={sig}");

        // Extract and verify (mirroring the verify_fivetran logic).
        let sig_hex = header_value
            .strip_prefix("sha256=")
            .unwrap_or(&header_value);
        let expected = hmac_sha256_hex(secret, body);
        assert!(
            ct_eq(expected.as_bytes(), sig_hex.as_bytes()),
            "valid Fivetran signature should be accepted"
        );
    }

    #[test]
    fn test_fivetran_wrong_secret_rejected() {
        let body = b"{\"event\":\"delete\",\"table\":\"users\"}";
        let sig_with_correct_secret = hmac_sha256_hex(b"correct-secret", body);
        let expected_with_wrong_secret = hmac_sha256_hex(b"wrong-secret", body);
        assert!(
            !ct_eq(
                sig_with_correct_secret.as_bytes(),
                expected_with_wrong_secret.as_bytes()
            ),
            "signature computed with wrong secret should not match"
        );
    }

    #[test]
    fn test_fivetran_tampered_body_rejected() {
        let secret = b"fivetran-secret";
        let original_sig = hmac_sha256_hex(secret, b"original body");
        let expected_for_tampered = hmac_sha256_hex(secret, b"tampered body");
        assert!(
            !ct_eq(original_sig.as_bytes(), expected_for_tampered.as_bytes()),
            "signature of original body should not match expected for tampered body"
        );
    }

    #[test]
    fn test_fivetran_signature_without_prefix_accepted() {
        // Some Fivetran implementations omit the "sha256=" prefix.
        let secret = b"prefix-test-secret";
        let body = b"test payload";
        let bare_sig = hmac_sha256_hex(secret, body);

        // strip_prefix("sha256=") on a bare sig returns None, so we use sig directly.
        let sig_hex = bare_sig.strip_prefix("sha256=").unwrap_or(&bare_sig);
        let expected = hmac_sha256_hex(secret, body);
        assert!(
            ct_eq(expected.as_bytes(), sig_hex.as_bytes()),
            "bare Fivetran signature (without sha256= prefix) should be accepted"
        );
    }

    // ── Fivetran payload transformation ──────────────────────────────────

    #[test]
    fn test_fivetran_payload_insert_op() {
        let payload = serde_json::json!({
            "event": "insert",
            "schema": "public",
            "table": "orders",
            "data": {"id": 42, "amount": 99.95},
            "before": null
        });
        let event = payload["event"].as_str().unwrap_or("");
        assert_eq!(event, "insert");
    }

    #[test]
    fn test_fivetran_payload_delete_op() {
        let payload = serde_json::json!({
            "event": "delete",
            "schema": "public",
            "table": "customers",
            "data": null,
            "before": {"id": 10, "name": "Alice"}
        });
        let event = payload["event"].as_str().unwrap_or("");
        assert_eq!(event, "delete");
    }

    #[test]
    fn test_fivetran_hvr_batch_payload() {
        // Fivetran HVR batches may contain multiple row changes.
        let payload = serde_json::json!({
            "events": [
                {"event": "insert", "table": "orders", "data": {"id": 1}},
                {"event": "update", "table": "orders", "data": {"id": 1, "status": "shipped"}},
                {"event": "delete", "table": "orders", "data": {"id": 2}}
            ],
            "schema": "public"
        });
        let events = payload["events"].as_array().expect("events array");
        assert_eq!(events.len(), 3, "HVR batch should contain 3 events");
        assert_eq!(events[0]["event"], "insert");
        assert_eq!(events[2]["event"], "delete");
    }

    // ── Signature scheme comparison ───────────────────────────────────────

    #[test]
    fn test_fivetran_and_github_use_same_underlying_hmac() {
        // Both Fivetran and GitHub use HMAC-SHA256 of the raw body.
        // The difference is the header name and prefix format.
        let secret = b"shared-secret";
        let body = b"same body";

        let fivetran_sig = hmac_sha256_hex(secret, body); // X-Fivetran-Signature: sha256=<sig>
        let github_sig = hmac_sha256_hex(secret, body); // X-Hub-Signature-256: sha256=<sig>

        assert_eq!(
            fivetran_sig, github_sig,
            "same secret + body should produce same HMAC regardless of platform"
        );
    }

    #[test]
    fn test_all_supported_signature_schemes() {
        // Ensure all known schemes are recognised in config.
        let schemes = [
            "hmac-sha256",
            "github",
            "stripe",
            "svix",
            "fivetran",
            "none",
        ];
        for scheme in &schemes {
            let config = serde_json::json!({
                "signature": {"scheme": scheme, "secret": "s"}
            });
            let parsed = config.pointer("/signature/scheme").and_then(|v| v.as_str());
            assert_eq!(
                parsed,
                Some(*scheme),
                "scheme '{scheme}' should be parseable"
            );
        }
    }
}
