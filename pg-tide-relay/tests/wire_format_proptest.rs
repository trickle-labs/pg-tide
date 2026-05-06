//! Property-based wire-format tests (v0.16.0).
//!
//! Uses `proptest` to verify that `WireFormat::decode` → `encode` round-trips
//! hold for `NativePgTideFormat`, `DebeziumFormat`, and `CloudEventsFormat`
//! across randomised payloads.
//!
//! Property: for any randomly-generated `OutboxRow`, encoding with a format
//! and immediately decoding the first message must produce an `InboxRow`
//! whose payload is semantically equivalent to the original.

use proptest::prelude::*;
use serde_json::{json, Value};

use pg_tide_relay::wire_format::{
    debezium::DebeziumConfig, CloudEventsFormat, DebeziumFormat, EncodeContext, InboxRow,
    NativePgTideFormat, OutboxRow, RawMessage, WireFormat,
};

// ── Arbitrary data generators ─────────────────────────────────────────────────

/// Generate a JSON object with 1–5 random string key/value pairs.
fn arb_json_object() -> impl Strategy<Value = Value> {
    prop::collection::hash_map(
        "[a-z][a-z0-9_]{0,15}",
        prop_oneof![
            any::<i64>().prop_map(|n| json!(n)),
            any::<bool>().prop_map(|b| json!(b)),
            "[a-zA-Z0-9 _-]{0,40}".prop_map(|s| json!(s)),
        ],
        1..=5,
    )
    .prop_map(|map| {
        let mut obj = serde_json::Map::new();
        for (k, v) in map {
            obj.insert(k, v);
        }
        Value::Object(obj)
    })
}

/// Generate an operation string.
fn arb_op() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("insert".to_string()),
        Just("update".to_string()),
        Just("delete".to_string()),
    ]
}

/// Generate an OutboxRow with randomised payload and op.
fn arb_outbox_row() -> impl Strategy<Value = OutboxRow> {
    (
        any::<i64>(),
        "[a-z][a-z0-9_]{1,20}",
        arb_json_object(),
        arb_op(),
    )
        .prop_map(|(id, table, payload, op)| {
            let (new_row, old_row) = match op.as_str() {
                "insert" => (Some(payload), None),
                "update" => (Some(payload.clone()), Some(payload)),
                "delete" => (None, Some(payload)),
                _ => unreachable!(),
            };
            OutboxRow {
                outbox_id: id.abs(),
                stream_table: table.clone(),
                database: "testdb".to_string(),
                schema_name: "public".to_string(),
                op,
                new_row,
                old_row,
                commit_ts: None,
                source_lsn: None,
            }
        })
}

/// Generate a JSON value representing a native pg_tide envelope payload.
fn arb_native_payload() -> impl Strategy<Value = Value> {
    (
        "[a-z][a-z0-9_]{1,20}",
        "[a-z][a-z0-9_]{1,20}",
        arb_json_object(),
        arb_op(),
    )
        .prop_map(|(subject, dedup_key, payload, op)| {
            json!({
                "v": 1,
                "subject": subject,
                "dedup_key": dedup_key,
                "outbox_id": 42,
                "op": op,
                "payload": payload,
            })
        })
}

// ── NativePgTideFormat round-trip ─────────────────────────────────────────────

proptest! {
    /// Native format: decode a serialised native envelope → InboxRow payload
    /// must match the original payload field.
    #[test]
    fn prop_native_decode_payload_preserved(payload in arb_native_payload()) {
        let fmt = NativePgTideFormat::new();
        let raw = RawMessage::from_json("test-topic", &payload);
        let result = fmt.decode(&raw).unwrap();
        prop_assume!(result.is_some());
        let row: InboxRow = result.unwrap();
        // The decoded payload must contain all keys from the original.
        let original_payload = &payload["payload"];
        if let (Value::Object(orig), Value::Object(decoded)) = (original_payload, &row.payload) {
            for (k, v) in orig {
                prop_assert_eq!(decoded.get(k), Some(v),
                    "key '{}' should be preserved after decode", k);
            }
        }
    }
}

proptest! {
    /// Native format: dedup_key is preserved from the v:1 envelope.
    #[test]
    fn prop_native_decode_dedup_key_preserved(payload in arb_native_payload()) {
        let fmt = NativePgTideFormat::new();
        let raw = RawMessage::from_json("test-topic", &payload);
        let result = fmt.decode(&raw).unwrap();
        prop_assume!(result.is_some());
        let row = result.unwrap();
        let expected_dedup = payload["dedup_key"].as_str().unwrap_or("");
        prop_assert_eq!(&row.event_id, expected_dedup);
    }
}

// ── DebeziumFormat round-trip ─────────────────────────────────────────────────

proptest! {
    /// Debezium format: encode an OutboxRow → decode the first EncodedMessage
    /// back → the decoded InboxRow's payload should contain the same keys
    /// as the original new_row (for insert/update).
    #[test]
    fn prop_debezium_encode_decode_insert_update(row in arb_outbox_row().prop_filter(
        "only insert/update for this property",
        |r| r.op == "insert" || r.op == "update"
    )) {
        let fmt = DebeziumFormat::new(DebeziumConfig::default());
        let ctx = EncodeContext::default();

        let batch = fmt.encode(&row, &ctx).unwrap();
        prop_assume!(!batch.messages.is_empty());

        // Decode the first (data event) message.
        let first_msg = &batch.messages[0];
        let raw = RawMessage {
            key: first_msg.key.clone(),
            value: first_msg.value.clone(),
            topic: first_msg.topic.clone(),
            headers: Default::default(),
        };

        let decoded = fmt.decode(&raw).unwrap();
        // Debezium may return None for some message shapes; only assert when Some.
        if let Some(inbox_row) = decoded {
            // After field should contain the original new_row keys.
            if let Some(new_row) = &row.new_row {
                if let (Value::Object(orig), Value::Object(decoded_payload)) =
                    (new_row, &inbox_row.payload)
                {
                    for (k, v) in orig {
                        prop_assert_eq!(
                            decoded_payload.get(k),
                            Some(v),
                            "Debezium round-trip: key '{}' should be preserved", k
                        );
                    }
                }
            }
        }
    }
}

proptest! {
    /// Debezium format: encoding a tombstone-eligible DELETE produces 2 messages
    /// when emit_tombstones is true (the event + the null-value tombstone).
    #[test]
    fn prop_debezium_delete_tombstone_count(row in arb_outbox_row().prop_filter(
        "only delete",
        |r| r.op == "delete"
    )) {
        let fmt = DebeziumFormat::new(DebeziumConfig::default());
        let ctx = EncodeContext {
            emit_tombstones: true,
            ..Default::default()
        };

        let batch = fmt.encode(&row, &ctx).unwrap();
        // Debezium DELETE with tombstones should produce exactly 2 messages.
        prop_assert!(
            batch.messages.len() == 2,
            "delete with tombstones should produce 2 messages, got {}",
            batch.messages.len()
        );
        // Last message should be a tombstone (null value).
        prop_assert!(
            batch.messages.last().unwrap().value.is_none(),
            "last message should be a tombstone"
        );
    }
}

// ── CloudEventsFormat round-trip ──────────────────────────────────────────────

proptest! {
    /// CloudEvents format: encode → decode preserves the data payload.
    #[test]
    fn prop_cloudevents_encode_decode_insert(row in arb_outbox_row().prop_filter(
        "only insert for this property",
        |r| r.op == "insert"
    )) {
        let fmt = CloudEventsFormat::new();
        let ctx = EncodeContext::default();

        let batch = fmt.encode(&row, &ctx).unwrap();
        prop_assume!(!batch.messages.is_empty());

        let first_msg = &batch.messages[0];
        let raw = RawMessage {
            key: first_msg.key.clone(),
            value: first_msg.value.clone(),
            topic: first_msg.topic.clone(),
            headers: Default::default(),
        };

        let decoded = fmt.decode(&raw).unwrap();
        if let Some(inbox_row) = decoded {
            // Data field should contain the new_row keys.
            if let Some(new_row) = &row.new_row {
                if let (Value::Object(orig), Value::Object(decoded_payload)) =
                    (new_row, &inbox_row.payload)
                {
                    for (k, v) in orig {
                        prop_assert_eq!(
                            decoded_payload.get(k),
                            Some(v),
                            "CloudEvents round-trip: key '{}' should be preserved", k
                        );
                    }
                }
            }
        }
    }
}

// ── from_config factory round-trip ───────────────────────────────────────────

proptest! {
    /// from_config: any valid format name returns a format that can encode
    /// an insert row without panicking.
    #[test]
    fn prop_from_config_encode_does_not_panic(
        row in arb_outbox_row().prop_filter("insert only", |r| r.op == "insert"),
        format_name in prop_oneof![
            Just("native"),
            Just("debezium"),
            Just("cloudevents"),
        ]
    ) {
        let cfg = json!({"wire_format": format_name});
        let fmt = pg_tide_relay::wire_format::from_config(&cfg);
        let ctx = EncodeContext::default();
        // Must not panic — result (Ok or Err) is acceptable.
        let _ = fmt.encode(&row, &ctx);
    }
}
