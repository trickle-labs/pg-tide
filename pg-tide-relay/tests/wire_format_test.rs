//! Integration tests: Pluggable wire formats (v0.11.0).
//!
//! Tests the `WireFormat` abstraction end-to-end: the `WireFormat` trait,
//! the `NativePgTideFormat`, `DebeziumFormat`, and the optional Maxwell,
//! Canal, and custom CDC JSON formats.
//!
//! These tests use the common `PgTideTestDb` harness to verify that decoded
//! `InboxRow` values can be written to a real pg_tide inbox table, and that
//! encoded `EncodedBatch` values round-trip correctly through JSON.

mod common;

use common::PgTideTestDb;
use pg_tide_relay::wire_format::{
    self, DebeziumFormat, EncodeContext, InboxRow, NativePgTideFormat, OutboxRow, RawMessage,
    WireFormat,
};
use serde_json::{json, Value};

// ── Helper to deliver an InboxRow to the database ────────────────────────────

async fn deliver_inbox_row(db: &PgTideTestDb, inbox: &str, row: &InboxRow) {
    db.deliver_to_inbox(inbox, &row.event_id, &row.payload)
        .await;
}

// ── from_config factory ───────────────────────────────────────────────────────

#[test]
fn test_from_config_returns_native_by_default() {
    let cfg = json!({});
    let fmt = wire_format::from_config(&cfg);
    assert_eq!(fmt.name(), "native");
}

#[test]
fn test_from_config_returns_debezium() {
    let cfg = json!({"wire_format": "debezium"});
    let fmt = wire_format::from_config(&cfg);
    assert_eq!(fmt.name(), "debezium");
}

#[test]
fn test_from_config_returns_cloudevents() {
    let cfg = json!({"wire_format": "cloudevents"});
    let fmt = wire_format::from_config(&cfg);
    assert_eq!(fmt.name(), "cloudevents");
}

#[cfg(feature = "maxwell")]
#[test]
fn test_from_config_returns_maxwell() {
    let cfg = json!({"wire_format": "maxwell"});
    let fmt = wire_format::from_config(&cfg);
    assert_eq!(fmt.name(), "maxwell");
}

#[cfg(feature = "canal")]
#[test]
fn test_from_config_returns_canal() {
    let cfg = json!({"wire_format": "canal"});
    let fmt = wire_format::from_config(&cfg);
    assert_eq!(fmt.name(), "canal");
}

#[cfg(feature = "cdc-json")]
#[test]
fn test_from_config_returns_cdc_json() {
    let cfg = json!({"wire_format": "cdc_json"});
    let fmt = wire_format::from_config(&cfg);
    assert_eq!(fmt.name(), "cdc_json");
}

// ── Native format ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_native_decode_and_deliver_to_inbox() {
    let db = PgTideTestDb::start().await;
    db.setup_inbox("wire-native-inbox").await;

    let fmt = NativePgTideFormat::new();
    let raw = RawMessage::from_json(
        "wire-native-inbox",
        &json!({"op": "insert", "order_id": 42, "status": "created"}),
    );
    let row = fmt.decode(&raw).unwrap().unwrap();
    assert_eq!(row.op, "insert");

    deliver_inbox_row(&db, "wire-native-inbox", &row).await;
    db.assert_inbox_received("wire-native-inbox", 1).await;
}

#[test]
fn test_native_encode_roundtrip() {
    let fmt = NativePgTideFormat::new();
    let ctx = EncodeContext::default();
    let outbox_row = OutboxRow {
        outbox_id: 1,
        stream_table: "orders".to_string(),
        database: "app".to_string(),
        schema_name: "public".to_string(),
        op: "insert".to_string(),
        new_row: Some(json!({"id": 1, "total": 100})),
        old_row: None,
        commit_ts: None,
        source_lsn: None,
    };
    let batch = fmt.encode(&outbox_row, &ctx).unwrap();
    assert_eq!(batch.messages.len(), 1);
    let v: Value = serde_json::from_slice(batch.messages[0].value.as_ref().unwrap()).unwrap();
    assert_eq!(v["op"], "insert");
    assert_eq!(v["outbox_id"], 1);
}

// ── Debezium format ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_debezium_decode_insert_and_deliver_to_inbox() {
    let db = PgTideTestDb::start().await;
    db.setup_inbox("wire-debezium-inbox").await;

    let fmt = DebeziumFormat::from_config(&json!({}));
    let envelope = json!({
        "payload": {
            "before": null,
            "after": {"id": 7, "name": "alice"},
            "op": "c",
            "ts_ms": 1714029482000_i64,
            "source": {"ts_ms": 1714029482000_i64, "db": "app", "table": "users", "lsn": 1000}
        }
    });
    let raw = RawMessage {
        key: Some(b"{\"id\":7}".to_vec()),
        value: Some(serde_json::to_vec(&envelope).unwrap()),
        topic: "my-server.public.users".to_string(),
        headers: Default::default(),
    };

    let row = fmt.decode(&raw).unwrap().unwrap();
    assert_eq!(row.op, "insert");
    assert_eq!(row.payload["id"], 7);
    assert!(row.commit_ts.is_some());
    assert!(row.source_position.is_some());

    deliver_inbox_row(&db, "wire-debezium-inbox", &row).await;
    db.assert_inbox_received("wire-debezium-inbox", 1).await;
}

#[tokio::test]
async fn test_debezium_decode_update_and_deliver_to_inbox() {
    let db = PgTideTestDb::start().await;
    db.setup_inbox("wire-debezium-update-inbox").await;

    let fmt = DebeziumFormat::from_config(&json!({}));
    let envelope = json!({
        "payload": {
            "before": {"id": 7, "name": "alice"},
            "after": {"id": 7, "name": "alice2"},
            "op": "u",
            "ts_ms": 1714029482000_i64,
            "source": {"ts_ms": 1714029482000_i64, "db": "app", "table": "users"}
        }
    });
    let raw = RawMessage::from_json("my-server.public.users", &envelope);
    let row = fmt.decode(&raw).unwrap().unwrap();

    assert_eq!(row.op, "update");
    assert_eq!(row.payload["name"], "alice2");
    assert!(row.old_payload.is_some());

    deliver_inbox_row(&db, "wire-debezium-update-inbox", &row).await;
    db.assert_inbox_received("wire-debezium-update-inbox", 1)
        .await;
}

#[tokio::test]
async fn test_debezium_decode_delete_and_deliver_to_inbox() {
    let db = PgTideTestDb::start().await;
    db.setup_inbox("wire-debezium-delete-inbox").await;

    let fmt = DebeziumFormat::from_config(&json!({}));
    let envelope = json!({
        "payload": {
            "before": {"id": 7, "name": "alice"},
            "after": null,
            "op": "d",
            "ts_ms": 1714029482000_i64,
            "source": {"ts_ms": 1714029482000_i64, "db": "app", "table": "users"}
        }
    });
    let raw = RawMessage::from_json("my-server.public.users", &envelope);
    let row = fmt.decode(&raw).unwrap().unwrap();

    assert_eq!(row.op, "delete");
    assert_eq!(row.payload["id"], 7);

    deliver_inbox_row(&db, "wire-debezium-delete-inbox", &row).await;
    db.assert_inbox_received("wire-debezium-delete-inbox", 1)
        .await;
}

#[test]
fn test_debezium_encode_insert_no_tombstone() {
    let fmt = DebeziumFormat::from_config(&json!({"emit_tombstones": false}));
    let ctx = EncodeContext {
        server_name: "pg-tide-prod".to_string(),
        topic_template: "{server}.{schema}.{stream_table}".to_string(),
        emit_tombstones: false,
        ..Default::default()
    };
    let row = OutboxRow {
        outbox_id: 10,
        stream_table: "orders".to_string(),
        database: "app".to_string(),
        schema_name: "public".to_string(),
        op: "insert".to_string(),
        new_row: Some(json!({"id": 10, "total": 200})),
        old_row: None,
        commit_ts: None,
        source_lsn: None,
    };
    let batch = fmt.encode(&row, &ctx).unwrap();
    assert_eq!(batch.messages.len(), 1);
    let v: Value = serde_json::from_slice(batch.messages[0].value.as_ref().unwrap()).unwrap();
    assert_eq!(v["payload"]["op"], "c");
    assert_eq!(v["payload"]["after"]["id"], 10);
    assert!(v["payload"]["before"].is_null());
    assert_eq!(batch.messages[0].topic, "pg-tide-prod.public.orders");
}

#[test]
fn test_debezium_encode_delete_with_tombstone() {
    let fmt = DebeziumFormat::from_config(&json!({"emit_tombstones": true}));
    let ctx = EncodeContext {
        emit_tombstones: true,
        ..Default::default()
    };
    let row = OutboxRow {
        outbox_id: 11,
        stream_table: "orders".to_string(),
        database: "app".to_string(),
        schema_name: "public".to_string(),
        op: "delete".to_string(),
        new_row: None,
        old_row: Some(json!({"id": 11})),
        commit_ts: None,
        source_lsn: None,
    };
    let batch = fmt.encode(&row, &ctx).unwrap();
    // Event + tombstone.
    assert_eq!(batch.messages.len(), 2);
    // Tombstone has null value.
    assert!(batch.messages[1].value.is_none());
}

#[test]
fn test_debezium_tombstone_handling_drop() {
    let fmt = DebeziumFormat::from_config(&json!({"tombstone_handling": "drop"}));
    let raw = RawMessage::tombstone("topic", b"key-1".to_vec());
    let result = fmt.decode(&raw).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_debezium_tombstone_handling_delete() {
    let fmt = DebeziumFormat::from_config(&json!({"tombstone_handling": "delete"}));
    let raw = RawMessage::tombstone("topic", b"key-1".to_vec());
    let result = fmt.decode(&raw).unwrap().unwrap();
    assert_eq!(result.op, "delete");
}

#[test]
fn test_debezium_snapshot_upsert() {
    let fmt = DebeziumFormat::from_config(&json!({"snapshot_op_treatment": "upsert"}));
    let envelope = json!({
        "payload": {"before": null, "after": {"id": 1}, "op": "r", "ts_ms": 0_i64, "source": {}}
    });
    let raw = RawMessage::from_json("topic", &envelope);
    let row = fmt.decode(&raw).unwrap().unwrap();
    assert_eq!(row.op, "upsert");
}

#[test]
fn test_debezium_schema_evolution_detect_incompatible() {
    let mut fmt = DebeziumFormat::from_config(&json!({}));
    let env1 = json!({"payload": {"after": {"id": 1, "name": "alice"}, "before": null, "op": "c", "ts_ms": 0_i64, "source": {}}});
    let env2 = json!({"payload": {"after": {"id": 2}, "before": null, "op": "c", "ts_ms": 0_i64, "source": {}}}); // "name" removed
    let raw1 = RawMessage::from_json("topic", &env1);
    let raw2 = RawMessage::from_json("topic", &env2);
    fmt.observe_schema(&raw1).unwrap();
    let result = fmt.observe_schema(&raw2);
    assert!(result.is_err());
}

// ── Maxwell format ────────────────────────────────────────────────────────────

#[cfg(feature = "maxwell")]
mod maxwell_tests {
    use super::*;
    use pg_tide_relay::wire_format::maxwell::MaxwellFormat;

    #[tokio::test]
    async fn test_maxwell_decode_insert_and_deliver_to_inbox() {
        let db = PgTideTestDb::start().await;
        db.setup_inbox("wire-maxwell-inbox").await;

        let fmt = MaxwellFormat::from_config(&json!({}));
        let envelope = json!({
            "database": "mydb",
            "table": "users",
            "type": "insert",
            "ts": 1714029482_i64,
            "xid": 12345,
            "data": {"id": 7, "name": "alice"},
        });
        let raw = RawMessage::from_json("users", &envelope);
        let row = fmt.decode(&raw).unwrap().unwrap();
        assert_eq!(row.op, "insert");
        assert_eq!(row.event_type, "mydb.users");

        deliver_inbox_row(&db, "wire-maxwell-inbox", &row).await;
        db.assert_inbox_received("wire-maxwell-inbox", 1).await;
    }

    #[test]
    fn test_maxwell_encode_returns_error() {
        let fmt = MaxwellFormat::from_config(&json!({}));
        let ctx = EncodeContext::default();
        let row = OutboxRow {
            outbox_id: 1,
            stream_table: "users".to_string(),
            database: "app".to_string(),
            schema_name: "public".to_string(),
            op: "insert".to_string(),
            new_row: Some(json!({"id": 1})),
            old_row: None,
            commit_ts: None,
            source_lsn: None,
        };
        assert!(fmt.encode(&row, &ctx).is_err());
    }
}

// ── Canal format ──────────────────────────────────────────────────────────────

#[cfg(feature = "canal")]
mod canal_tests {
    use super::*;
    use pg_tide_relay::wire_format::canal::CanalFormat;

    #[tokio::test]
    async fn test_canal_decode_insert_and_deliver_to_inbox() {
        let db = PgTideTestDb::start().await;
        db.setup_inbox("wire-canal-inbox").await;

        let fmt = CanalFormat::from_config(&json!({}));
        let envelope = json!({
            "id": 1,
            "database": "mydb",
            "table": "orders",
            "type": "INSERT",
            "isDdl": false,
            "es": 1714029482000_i64,
            "data": [{"id": "5", "amount": "99.00"}],
        });
        let raw = RawMessage::from_json("orders", &envelope);
        let row = fmt.decode(&raw).unwrap().unwrap();
        assert_eq!(row.op, "insert");
        assert_eq!(row.event_type, "mydb.orders");

        deliver_inbox_row(&db, "wire-canal-inbox", &row).await;
        db.assert_inbox_received("wire-canal-inbox", 1).await;
    }

    #[test]
    fn test_canal_ddl_skipped() {
        let fmt = CanalFormat::from_config(&json!({}));
        let envelope = json!({
            "id": 2,
            "database": "mydb",
            "table": "orders",
            "type": "ALTER",
            "isDdl": true,
        });
        let raw = RawMessage::from_json("orders", &envelope);
        assert!(fmt.decode(&raw).unwrap().is_none());
    }
}

// ── Custom CDC JSON format ─────────────────────────────────────────────────────

#[cfg(feature = "cdc-json")]
mod cdc_json_tests {
    use super::*;
    use pg_tide_relay::wire_format::cdc_json::CdcJsonFormat;

    #[tokio::test]
    async fn test_cdc_json_decode_and_deliver_to_inbox() {
        let db = PgTideTestDb::start().await;
        db.setup_inbox("wire-cdc-json-inbox").await;

        let fmt = CdcJsonFormat::from_config(&json!({
            "op_path": "$.event_type",
            "op_map": {"created": "insert", "modified": "update", "removed": "delete"},
            "payload_path": "$.data",
            "event_id_path": "$.id",
            "event_type_path": "$.resource",
        }));

        let raw = RawMessage::from_json(
            "raw-topic",
            &json!({
                "event_type": "created",
                "id": "evt-abc",
                "resource": "orders",
                "data": {"order_id": 1, "total": 99},
            }),
        );

        let row = fmt.decode(&raw).unwrap().unwrap();
        assert_eq!(row.op, "insert");
        assert_eq!(row.event_id, "evt-abc");
        assert_eq!(row.event_type, "orders");
        assert_eq!(row.payload["order_id"], 1);

        deliver_inbox_row(&db, "wire-cdc-json-inbox", &row).await;
        db.assert_inbox_received("wire-cdc-json-inbox", 1).await;
    }

    #[test]
    fn test_cdc_json_encode_roundtrip() {
        let fmt = CdcJsonFormat::from_config(&json!({}));
        let ctx = EncodeContext::default();
        let row = OutboxRow {
            outbox_id: 1,
            stream_table: "orders".to_string(),
            database: "app".to_string(),
            schema_name: "public".to_string(),
            op: "insert".to_string(),
            new_row: Some(json!({"id": 1})),
            old_row: None,
            commit_ts: None,
            source_lsn: None,
        };
        let batch = fmt.encode(&row, &ctx).unwrap();
        assert_eq!(batch.messages.len(), 1);
        let v: Value = serde_json::from_slice(batch.messages[0].value.as_ref().unwrap()).unwrap();
        assert!(v.get("op").is_some());
    }
}

// ── Logical type coercion ─────────────────────────────────────────────────────

#[test]
fn test_logical_type_date_epoch() {
    use pg_tide_relay::wire_format::apply_logical_type;
    let v = apply_logical_type(&json!(0_i64), "io.debezium.time.Date");
    assert_eq!(v.as_str().unwrap(), "1970-01-01");
}

#[test]
fn test_logical_type_date_positive() {
    use pg_tide_relay::wire_format::apply_logical_type;
    // 365 days after epoch = 1971-01-01
    let v = apply_logical_type(&json!(365_i64), "io.debezium.time.Date");
    assert_eq!(v.as_str().unwrap(), "1971-01-01");
}

#[test]
fn test_logical_type_timestamp_ms() {
    use pg_tide_relay::wire_format::apply_logical_type;
    let v = apply_logical_type(&json!(0_i64), "io.debezium.time.Timestamp");
    assert!(v.as_str().unwrap().contains("1970"));
}

#[test]
fn test_logical_type_micro_timestamp() {
    use pg_tide_relay::wire_format::apply_logical_type;
    let v = apply_logical_type(&json!(0_i64), "io.debezium.time.MicroTimestamp");
    assert!(v.as_str().unwrap().contains("1970"));
}

#[test]
fn test_logical_type_unknown_falls_back_to_text() {
    use pg_tide_relay::wire_format::apply_logical_type;
    let v = apply_logical_type(&json!(42_i64), "io.unknown.Type");
    // Should produce a string.
    assert!(v.is_string());
}

// ── WireError types ───────────────────────────────────────────────────────────

#[test]
fn test_wire_error_decode_message() {
    use pg_tide_relay::wire_format::WireError;
    let e = WireError::decode("my-topic", "something broke");
    assert!(e.to_string().contains("my-topic"));
    assert!(e.to_string().contains("something broke"));
}

#[test]
fn test_wire_error_encode_message() {
    use pg_tide_relay::wire_format::WireError;
    let e = WireError::encode(42, "could not serialize");
    assert!(e.to_string().contains("42"));
    assert!(e.to_string().contains("could not serialize"));
}

#[test]
fn test_wire_error_schema_incompatible_message() {
    use pg_tide_relay::wire_format::WireError;
    let e = WireError::schema_incompatible("my-topic", "field 'name' was removed");
    assert!(e.to_string().contains("my-topic"));
    assert!(e.to_string().contains("field 'name' was removed"));
}

// ── CloudEvents wire format ───────────────────────────────────────────────────

#[test]
fn test_cloudevents_encode_insert_roundtrip() {
    use pg_tide_relay::wire_format::CloudEventsFormat;

    let fmt = CloudEventsFormat::from_config(&json!({}));
    let ctx = EncodeContext::default();
    let row = OutboxRow {
        outbox_id: 1,
        stream_table: "orders".to_string(),
        database: "app".to_string(),
        schema_name: "public".to_string(),
        op: "insert".to_string(),
        new_row: Some(json!({"id": 1, "total": 99})),
        old_row: None,
        commit_ts: None,
        source_lsn: None,
    };
    let batch = fmt.encode(&row, &ctx).unwrap();
    assert_eq!(batch.messages.len(), 1);
    let v: Value = serde_json::from_slice(batch.messages[0].value.as_ref().unwrap()).unwrap();
    assert_eq!(v["specversion"], "1.0");
    assert!(v.get("id").is_some());
    assert_eq!(v["type"], "io.pgtide.insert");
    assert_eq!(v["ce-op"], "insert");
    assert_eq!(v["data"]["id"], 1);
}

#[test]
fn test_cloudevents_decode_insert() {
    use pg_tide_relay::wire_format::CloudEventsFormat;

    let fmt = CloudEventsFormat::from_config(&json!({}));
    let envelope = json!({
        "specversion": "1.0",
        "id": "evt-123",
        "type": "app.orders.insert",
        "source": "/pg-tide/app/orders",
        "datacontenttype": "application/json",
        "ce-op": "insert",
        "data": {"id": 1, "total": 99},
    });
    let raw = RawMessage::from_json("app.orders", &envelope);
    let row = fmt.decode(&raw).unwrap().unwrap();
    assert_eq!(row.event_id, "evt-123");
    assert_eq!(row.event_type, "app.orders.insert");
    assert_eq!(row.op, "insert");
    assert_eq!(row.payload["id"], 1);
}

#[test]
fn test_cloudevents_wrong_specversion_errors() {
    use pg_tide_relay::wire_format::CloudEventsFormat;

    let fmt = CloudEventsFormat::from_config(&json!({}));
    let envelope = json!({
        "specversion": "0.3",
        "id": "evt-999",
        "type": "app.orders.insert",
        "source": "/pg-tide/app/orders",
        "data": {},
    });
    let raw = RawMessage::from_json("app.orders", &envelope);
    assert!(fmt.decode(&raw).is_err());
}

#[test]
fn test_from_config_returns_cloudevents_format_name() {
    let cfg = json!({"wire_format": "cloudevents"});
    let fmt = wire_format::from_config(&cfg);
    assert_eq!(fmt.name(), "cloudevents");
}
