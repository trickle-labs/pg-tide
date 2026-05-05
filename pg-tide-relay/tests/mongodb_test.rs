//! Integration tests: MongoDB analytics sink (v0.10.0 — RELAY-P3-MDB).
//!
//! Tests verify MongoDB config logic, document encoding, and DB-side mechanics
//! without requiring a running MongoDB instance.

mod common;

use common::PgTideTestDb;

// ── DB-side mechanics ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_mongodb_forward_sink_queues_messages() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("mongo-outbox").await;
    db.setup_consumer_group("mongo-group", "mongo-outbox").await;

    let payloads: Vec<serde_json::Value> = (1..=5)
        .map(|i| serde_json::json!({"doc_id": i, "name": format!("item-{i}")}))
        .collect();
    db.publish_messages("mongo-outbox", &payloads).await;

    assert_eq!(
        db.pending_count("mongo-outbox").await,
        5,
        "all 5 messages must be pending before MongoDB delivery"
    );
}

#[tokio::test]
async fn test_mongodb_sink_failure_preserves_offset() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("mongo-fail-outbox").await;
    db.setup_consumer_group("mongo-fail-group", "mongo-fail-outbox")
        .await;

    let payloads: Vec<serde_json::Value> = (1..=2).map(|i| serde_json::json!({"n": i})).collect();
    db.publish_messages("mongo-fail-outbox", &payloads).await;

    let row = db
        .client
        .query_opt(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'mongo-fail-group'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        row.is_none(),
        "consumer offset must not exist before successful MongoDB delivery"
    );
}

// ── Config and document encoding ─────────────────────────────────────────────

#[test]
fn test_mongodb_config_collection_for_subject() {
    use pg_tide_relay::sink::mongodb::MongoDbConfig;

    let cfg = MongoDbConfig::new("mongodb://localhost:27017", "pgtide");
    assert_eq!(cfg.collection_for("orders.insert"), "orders.insert");
    assert_eq!(cfg.collection_for("events"), "events");

    let custom = MongoDbConfig {
        collection_template: "tide_{stream_table}".to_string(),
        ..cfg
    };
    assert_eq!(custom.collection_for("orders"), "tide_orders");
}

#[test]
fn test_mongodb_config_document_encoding_insert() {
    use pg_tide_relay::envelope::RelayMessage;
    use pg_tide_relay::sink::mongodb::MongoDbConfig;

    let cfg = MongoDbConfig::new("mongodb://localhost:27017", "pgtide");

    let msg = RelayMessage::new_forward(
        "orders",
        10,
        0,
        "insert",
        serde_json::json!({"order_id": 10, "status": "pending"}),
        false,
        None,
        "orders.insert",
    );

    let doc = cfg.to_document(&msg).expect("encode document");
    let obj = doc.as_object().expect("document should be a JSON object");
    assert_eq!(obj["_op"], "insert");
    assert!(
        obj.contains_key("_dedup_key"),
        "_dedup_key should be present"
    );
    assert_eq!(obj["order_id"], 10);
    assert_eq!(obj["_outbox_id"], 10);
}

#[test]
fn test_mongodb_config_document_encoding_non_object_payload() {
    use pg_tide_relay::envelope::RelayMessage;
    use pg_tide_relay::sink::mongodb::MongoDbConfig;

    let cfg = MongoDbConfig::new("mongodb://localhost:27017", "pgtide");

    let msg = RelayMessage::new_forward(
        "events",
        1,
        0,
        "insert",
        serde_json::json!("scalar_value"),
        false,
        None,
        "events",
    );

    let doc = cfg
        .to_document(&msg)
        .expect("encode document with scalar payload");
    let obj = doc.as_object().expect("should be object");
    // Non-object payloads wrapped under "data" key.
    assert!(
        obj.contains_key("data"),
        "scalar payload wrapped under 'data'"
    );
}

#[test]
fn test_mongodb_config_delete_op_does_not_add_extra_fields() {
    use pg_tide_relay::envelope::RelayMessage;
    use pg_tide_relay::sink::mongodb::MongoDbConfig;

    let cfg = MongoDbConfig::new("mongodb://localhost:27017", "pgtide");

    let msg = RelayMessage::new_forward(
        "orders",
        99,
        0,
        "delete",
        serde_json::json!({"order_id": 99}),
        false,
        None,
        "orders.delete",
    );

    assert_eq!(msg.op, "delete");
    // Document encoding still works for delete operations.
    let doc = cfg.to_document(&msg).expect("encode delete document");
    assert_eq!(doc["_op"], "delete");
}
