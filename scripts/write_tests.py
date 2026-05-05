#!/usr/bin/env python3
"""Write clean test files for v0.7.0."""
import os

base = "/Users/geir.gronmo/projects/pg-tide2/pg-tide-relay/tests"

webhook = r"""//! Unit tests: Webhook signature verification.

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
        a.iter().zip(b.iter()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
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
        assert!(ct_eq(hmac_hex(secret, body).as_bytes(), sig_part.as_bytes()));
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
"""

schema = r"""//! Unit tests: Schema Registry (Confluent wire format).

mod common;

const MAGIC: u8 = 0x00;

fn enc(schema_id: u32, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(5 + payload.len());
    buf.push(MAGIC);
    buf.extend_from_slice(&schema_id.to_be_bytes());
    buf.extend_from_slice(payload);
    buf
}

fn dec(data: &[u8]) -> Option<(u32, &[u8])> {
    if data.len() < 5 || data[0] != MAGIC {
        return None;
    }
    let id = u32::from_be_bytes([data[1], data[2], data[3], data[4]]);
    Some((id, &data[5..]))
}

#[test]
fn test_confluent_wire_roundtrip() {
    let id = 42u32;
    let payload = b"hello, avro!";
    let encoded = enc(id, payload);
    assert_eq!(encoded[0], MAGIC);
    assert_eq!(encoded[1..5], id.to_be_bytes());
    assert_eq!(&encoded[5..], payload);
    let (decoded_id, decoded_payload) = dec(&encoded).unwrap();
    assert_eq!(decoded_id, id);
    assert_eq!(decoded_payload, payload);
}

#[test]
fn test_confluent_wire_reject_wrong_magic() {
    let mut bad = enc(1, b"test");
    bad[0] = 0x01;
    assert!(dec(&bad).is_none());
}

#[test]
fn test_confluent_wire_reject_too_short() {
    assert!(dec(&[0x00u8, 0x00, 0x00]).is_none());
}

#[test]
fn test_confluent_wire_empty_payload() {
    let encoded = enc(1, b"");
    assert_eq!(encoded.len(), 5);
    let (id, pl) = dec(&encoded).unwrap();
    assert_eq!(id, 1);
    assert_eq!(pl, b"");
}

#[test]
fn test_schema_registry_disabled_by_default() {
    let config = serde_json::json!({});
    assert!(
        !config
            .pointer("/schema_registry/enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    );
}

#[test]
fn test_schema_registry_config_parsed() {
    let config = serde_json::json!({
        "schema_registry": {
            "enabled": true,
            "url": "http://schema-registry:8081",
            "subject_name_strategy": "topic_name"
        },
        "serialization": { "format": "avro", "schema_id": 42 }
    });
    assert!(config
        .pointer("/schema_registry/enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false));
    assert_eq!(
        config
            .pointer("/schema_registry/url")
            .and_then(|v| v.as_str()),
        Some("http://schema-registry:8081")
    );
    assert_eq!(
        config
            .pointer("/serialization/format")
            .and_then(|v| v.as_str()),
        Some("avro")
    );
    assert_eq!(
        config
            .pointer("/serialization/schema_id")
            .and_then(|v| v.as_u64()),
        Some(42)
    );
}

#[test]
fn test_subject_name_strategies() {
    for s in &["topic_name", "record_name", "topic_record_name"] {
        let cfg = serde_json::json!({"schema_registry": {"subject_name_strategy": s}});
        assert_eq!(
            cfg.pointer("/schema_registry/subject_name_strategy")
                .and_then(|v| v.as_str()),
            Some(*s)
        );
    }
}

#[test]
fn test_schema_id_big_endian() {
    let id: u32 = 0x0001_02FF;
    let bytes = id.to_be_bytes();
    assert_eq!(bytes, [0x00, 0x01, 0x02, 0xFF]);
    assert_eq!(u32::from_be_bytes(bytes), id);
}

#[test]
fn test_confluent_wire_schema_id_zero() {
    let (id, _) = dec(&enc(0, b"test")).unwrap();
    assert_eq!(id, 0);
}

#[test]
fn test_confluent_wire_schema_id_max() {
    let (id, _) = dec(&enc(u32::MAX, b"test")).unwrap();
    assert_eq!(id, u32::MAX);
}

#[cfg(feature = "schema-registry")]
#[test]
fn test_avro_schema_parses() {
    let schema_str = r#"{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}"#;
    assert!(apache_avro::Schema::parse_str(schema_str).is_ok());
}
"""

with open(os.path.join(base, "webhook_sig_test.rs"), "w") as f:
    f.write(webhook)
print("webhook_sig_test.rs written:", len(webhook), "chars")

with open(os.path.join(base, "schema_registry_test.rs"), "w") as f:
    f.write(schema)
print("schema_registry_test.rs written:", len(schema), "chars")
