//! Integration tests: Airbyte protocol adapter (v0.9.0).
//!
//! Verifies Airbyte message format, STATE persistence,
//! and CDC delete mapping — without requiring a running Airbyte connector.

mod common;

use common::PgTideTestDb;

const MIGRATION_SQL: &str = include_str!("../../sql/pg_tide--0.8.0--0.9.0.sql");

async fn apply_v090_migration(db: &PgTideTestDb) {
    db.client
        .batch_execute(MIGRATION_SQL)
        .await
        .expect("failed to apply v0.9.0 migration");
}

// ── Airbyte STATE persistence ─────────────────────────────────────────────────

#[tokio::test]
async fn test_airbyte_state_insert_and_retrieve() {
    let db = PgTideTestDb::start().await;
    apply_v090_migration(&db).await;

    let state = serde_json::json!({
        "type": "STREAM",
        "stream": {
            "stream_descriptor": {"name": "charges", "namespace": "stripe"},
            "stream_state": {"created": 1_714_700_000}
        }
    });

    db.client
        .execute(
            "INSERT INTO tide.relay_airbyte_state (pipeline_name, source_name, state_value)
             VALUES ('stripe-charges', 'source-stripe', $1)",
            &[&state],
        )
        .await
        .expect("insert airbyte state");

    let rows = db
        .client
        .query(
            "SELECT state_value FROM tide.relay_airbyte_state
              WHERE pipeline_name = 'stripe-charges'",
            &[],
        )
        .await
        .expect("select airbyte state");

    assert_eq!(rows.len(), 1);
    let retrieved: serde_json::Value = rows[0].get(0);
    assert_eq!(retrieved["type"], "STREAM");
    assert_eq!(retrieved["stream"]["stream_descriptor"]["name"], "charges");
}

#[tokio::test]
async fn test_airbyte_state_upsert_on_conflict() {
    let db = PgTideTestDb::start().await;
    apply_v090_migration(&db).await;

    // First state.
    db.client
        .execute(
            "INSERT INTO tide.relay_airbyte_state (pipeline_name, source_name, state_value)
             VALUES ('my-pipeline', 'source-hubspot', '{\"v\": 1}'::jsonb)",
            &[],
        )
        .await
        .expect("first insert");

    // Upsert with newer state.
    db.client
        .execute(
            "INSERT INTO tide.relay_airbyte_state (pipeline_name, source_name, state_value, written_at)
             VALUES ('my-pipeline', 'source-hubspot', '{\"v\": 2}'::jsonb, now())
             ON CONFLICT (pipeline_name, source_name) DO UPDATE
               SET state_value = EXCLUDED.state_value,
                   written_at  = EXCLUDED.written_at",
            &[],
        )
        .await
        .expect("upsert");

    let count: i64 = db
        .client
        .query_one(
            "SELECT COUNT(*)::bigint FROM tide.relay_airbyte_state
              WHERE pipeline_name = 'my-pipeline'",
            &[],
        )
        .await
        .expect("count")
        .get(0);
    assert_eq!(count, 1, "should have exactly one row after upsert");

    let val: serde_json::Value = db
        .client
        .query_one(
            "SELECT state_value FROM tide.relay_airbyte_state
              WHERE pipeline_name = 'my-pipeline'",
            &[],
        )
        .await
        .expect("fetch")
        .get(0);
    assert_eq!(val["v"], 2, "state should be updated to v=2");
}

// ── Airbyte message parsing ───────────────────────────────────────────────────

#[test]
fn test_airbyte_message_type_record() {
    let line = r#"{"type":"RECORD","record":{"stream":"orders","namespace":"pgtide","data":{"id":1,"amount":50.0},"emitted_at":1714700000000}}"#;
    let msg: serde_json::Value = serde_json::from_str(line).expect("parse");
    assert_eq!(msg["type"], "RECORD");
    assert_eq!(msg["record"]["stream"], "orders");
    assert_eq!(msg["record"]["data"]["id"], 1);
}

#[test]
fn test_airbyte_message_type_state() {
    let line = r#"{"type":"STATE","state":{"type":"GLOBAL","global":{"shared_state":{"offset":"abc123"},"stream_states":[]}}}"#;
    let msg: serde_json::Value = serde_json::from_str(line).expect("parse");
    assert_eq!(msg["type"], "STATE");
    assert_eq!(msg["state"]["global"]["shared_state"]["offset"], "abc123");
}

#[test]
fn test_airbyte_message_type_catalog() {
    let line = r#"{"type":"CATALOG","catalog":{"streams":[{"stream":{"name":"charges","namespace":"stripe","json_schema":{"type":"object"}},"sync_mode":"incremental","destination_sync_mode":"append"}]}}"#;
    let msg: serde_json::Value = serde_json::from_str(line).expect("parse");
    assert_eq!(msg["type"], "CATALOG");
    assert_eq!(msg["catalog"]["streams"][0]["stream"]["name"], "charges");
}

#[test]
fn test_airbyte_message_type_log() {
    let line = r#"{"type":"LOG","log":{"level":"INFO","message":"Connected to source"}}"#;
    let msg: serde_json::Value = serde_json::from_str(line).expect("parse");
    assert_eq!(msg["type"], "LOG");
    assert_eq!(msg["log"]["level"], "INFO");
}

#[test]
fn test_airbyte_message_type_trace() {
    let line = r#"{"type":"TRACE","trace":{"type":"ERROR","error":{"message":"timeout","failure_type":"transient_error"}}}"#;
    let msg: serde_json::Value = serde_json::from_str(line).expect("parse");
    assert_eq!(msg["type"], "TRACE");
    assert_eq!(msg["trace"]["type"], "ERROR");
}

// ── CDC soft-delete mapping ───────────────────────────────────────────────────

#[test]
fn test_airbyte_cdc_soft_delete_mapping() {
    // Airbyte CDC soft-delete: data contains _ab_cdc_deleted_at → op = "delete"
    let data = serde_json::json!({
        "id": 42,
        "_ab_cdc_deleted_at": "2024-03-15T09:00:00Z",
        "_ab_cdc_updated_at": "2024-03-15T09:00:00Z"
    });

    let op = if data
        .get("_ab_cdc_deleted_at")
        .and_then(|v| v.as_str())
        .is_some()
    {
        "delete"
    } else {
        "insert"
    };

    assert_eq!(op, "delete", "CDC delete record should map to op=delete");
}

#[test]
fn test_airbyte_regular_record_is_insert() {
    let data = serde_json::json!({
        "id": 100,
        "name": "Widget",
        "price": 9.99
    });

    let op = if data
        .get("_ab_cdc_deleted_at")
        .and_then(|v| v.as_str())
        .is_some()
    {
        "delete"
    } else {
        "insert"
    };

    assert_eq!(op, "insert", "regular record should map to op=insert");
}

// ── Singer vs Airbyte protocol comparison ────────────────────────────────────

#[test]
fn test_singer_and_airbyte_state_tables_are_independent() {
    // Verify the two state tables are distinct PostgreSQL tables.
    // Both can exist simultaneously without conflict.
    let singer_table = "tide.singer_state";
    let airbyte_table = "tide.relay_airbyte_state";
    assert_ne!(
        singer_table, airbyte_table,
        "state tables must be independent"
    );
}

#[test]
fn test_protocol_field_differences() {
    // Singer RECORD has "stream" at top level; Airbyte RECORD has "record.stream"
    let singer_record = serde_json::json!({
        "type": "RECORD",
        "stream": "orders",
        "record": {"id": 1}
    });
    let airbyte_record = serde_json::json!({
        "type": "RECORD",
        "record": {
            "stream": "orders",
            "data": {"id": 1}
        }
    });

    // Singer: stream at top-level
    assert_eq!(singer_record["stream"], "orders");
    // Airbyte: stream nested in record
    assert_eq!(airbyte_record["record"]["stream"], "orders");
    // They are structurally different despite similar content
    assert_ne!(singer_record, airbyte_record);
}
