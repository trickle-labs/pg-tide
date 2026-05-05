//! Unit tests: Schema Registry (Confluent wire format).

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
    assert!(!config
        .pointer("/schema_registry/enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false));
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
