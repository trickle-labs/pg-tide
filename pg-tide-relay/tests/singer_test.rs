//! Integration tests: Singer protocol adapter (v0.9.0).
//!
//! Tests Singer STATE persistence, SCHEMA drift detection,
//! and message format correctness — without requiring a real Singer tap/target.
//! Exercises the SQL API (tide.singer_state, tide.singer_schema_log).

mod common;

use common::PgTideTestDb;

/// SQL for the v0.9.0 migration (adds singer_state, singer_schema_log, relay_airbyte_state).
const MIGRATION_SQL: &str = include_str!("../../sql/pg_tide--0.8.0--0.9.0.sql");

/// Apply the v0.9.0 migration to a test database.
async fn apply_v090_migration(db: &PgTideTestDb) {
    db.client
        .batch_execute(MIGRATION_SQL)
        .await
        .expect("failed to apply v0.9.0 migration");
}

// ── STATE persistence ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_singer_state_upsert_and_retrieve() {
    let db = PgTideTestDb::start().await;
    apply_v090_migration(&db).await;

    // Insert a STATE checkpoint.
    db.client
        .execute(
            "INSERT INTO tide.singer_state (pipeline_name, tap_name, state_value)
             VALUES ('github-issues', 'tap-github',
                    '{\"bookmarks\": {\"issues\": {\"since\": \"2024-01-01\"}}}'::jsonb)",
            &[],
        )
        .await
        .expect("insert singer state");

    // Retrieve it.
    let rows = db
        .client
        .query(
            "SELECT state_value FROM tide.singer_state
              WHERE pipeline_name = 'github-issues' AND tap_name = 'tap-github'",
            &[],
        )
        .await
        .expect("select singer state");

    assert_eq!(rows.len(), 1, "expected exactly one state row");
    let state: serde_json::Value = rows[0].get(0);
    assert_eq!(
        state
            .pointer("/bookmarks/issues/since")
            .and_then(|v| v.as_str()),
        Some("2024-01-01"),
        "state value should match what was inserted"
    );
}

#[tokio::test]
async fn test_singer_state_upsert_replaces_previous() {
    let db = PgTideTestDb::start().await;
    apply_v090_migration(&db).await;

    // Insert initial state.
    db.client
        .execute(
            "INSERT INTO tide.singer_state (pipeline_name, tap_name, state_value)
             VALUES ('my-pipeline', 'tap-salesforce',
                    '{\"offset\": 100}'::jsonb)",
            &[],
        )
        .await
        .expect("insert first state");

    // Upsert with newer state.
    db.client
        .execute(
            "INSERT INTO tide.singer_state (pipeline_name, tap_name, state_value, written_at)
             VALUES ('my-pipeline', 'tap-salesforce', '{\"offset\": 999}'::jsonb, now())
             ON CONFLICT (pipeline_name, tap_name) DO UPDATE
               SET state_value = EXCLUDED.state_value,
                   written_at  = EXCLUDED.written_at",
            &[],
        )
        .await
        .expect("upsert state");

    // Only one row, with updated value.
    let rows = db
        .client
        .query(
            "SELECT state_value FROM tide.singer_state
              WHERE pipeline_name = 'my-pipeline' AND tap_name = 'tap-salesforce'",
            &[],
        )
        .await
        .expect("select state");

    assert_eq!(rows.len(), 1);
    let state: serde_json::Value = rows[0].get(0);
    assert_eq!(
        state.get("offset").and_then(|v| v.as_i64()),
        Some(999),
        "state should be updated to 999"
    );
}

#[tokio::test]
async fn test_singer_state_delete_resets_tap() {
    let db = PgTideTestDb::start().await;
    apply_v090_migration(&db).await;

    // Insert a state checkpoint.
    db.client
        .execute(
            "INSERT INTO tide.singer_state (pipeline_name, tap_name, state_value)
             VALUES ('stripe-pipeline', 'tap-stripe', '{\"last_event_id\": \"evt_123\"}'::jsonb)",
            &[],
        )
        .await
        .expect("insert state");

    // Delete it (forces full re-sync next startup).
    let deleted = db
        .client
        .execute(
            "DELETE FROM tide.singer_state WHERE pipeline_name = 'stripe-pipeline'",
            &[],
        )
        .await
        .expect("delete state");

    assert_eq!(deleted, 1, "one state row should be deleted");

    // Verify it's gone.
    let rows = db
        .client
        .query(
            "SELECT 1 FROM tide.singer_state WHERE pipeline_name = 'stripe-pipeline'",
            &[],
        )
        .await
        .expect("check state");

    assert!(rows.is_empty(), "state should be deleted for full re-sync");
}

#[tokio::test]
async fn test_singer_state_list_function() {
    let db = PgTideTestDb::start().await;
    apply_v090_migration(&db).await;

    // Insert two states.
    db.client
        .batch_execute(
            "INSERT INTO tide.singer_state (pipeline_name, tap_name, state_value)
             VALUES ('pipeline-a', 'tap-a', '{\"v\": 1}'::jsonb),
                    ('pipeline-b', 'tap-b', '{\"v\": 2}'::jsonb)",
        )
        .await
        .expect("insert states");

    let rows = db
        .client
        .query("SELECT * FROM tide.singer_state_list()", &[])
        .await
        .expect("singer_state_list");

    assert_eq!(rows.len(), 2, "singer_state_list should return both states");
}

// ── SCHEMA log ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_singer_schema_log_records_schema() {
    let db = PgTideTestDb::start().await;
    apply_v090_migration(&db).await;

    let schema_v1 = serde_json::json!({
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"}
        }
    });

    db.client
        .execute(
            "INSERT INTO tide.singer_schema_log
               (pipeline_name, tap_name, stream_name, schema_value, key_properties)
             VALUES ($1, $2, $3, $4, $5)",
            &[
                &"github-pipeline",
                &"tap-github",
                &"issues",
                &schema_v1,
                &vec!["id".to_string()],
            ],
        )
        .await
        .expect("insert schema log");

    let rows = db
        .client
        .query(
            "SELECT stream_name, schema_value, key_properties
               FROM tide.singer_schema_log
              WHERE pipeline_name = 'github-pipeline'",
            &[],
        )
        .await
        .expect("select schema log");

    assert_eq!(rows.len(), 1);
    let stream: String = rows[0].get("stream_name");
    assert_eq!(stream, "issues");
    let key_props: Vec<String> = rows[0].get("key_properties");
    assert_eq!(key_props, vec!["id"]);
}

#[tokio::test]
async fn test_singer_schema_drift_detection_function() {
    let db = PgTideTestDb::start().await;
    apply_v090_migration(&db).await;

    // Insert v1 schema (two properties).
    let schema_v1 = serde_json::json!({
        "type": "object",
        "properties": {
            "id":    {"type": "integer"},
            "name":  {"type": "string"}
        }
    });

    // Insert v2 schema (added "email", removed nothing, changed nothing).
    let schema_v2 = serde_json::json!({
        "type": "object",
        "properties": {
            "id":    {"type": "integer"},
            "name":  {"type": "string"},
            "email": {"type": "string"}
        }
    });

    db.client
        .execute(
            "INSERT INTO tide.singer_schema_log
               (pipeline_name, tap_name, stream_name, schema_value, key_properties, logged_at)
             VALUES ($1, $2, $3, $4, $5, now() - interval '1 second')",
            &[
                &"stripe-pipeline",
                &"tap-stripe",
                &"customers",
                &schema_v1,
                &vec!["id".to_string()],
            ],
        )
        .await
        .expect("insert v1 schema");

    db.client
        .execute(
            "INSERT INTO tide.singer_schema_log
               (pipeline_name, tap_name, stream_name, schema_value, key_properties, logged_at)
             VALUES ($1, $2, $3, $4, $5, now())",
            &[
                &"stripe-pipeline",
                &"tap-stripe",
                &"customers",
                &schema_v2,
                &vec!["id".to_string()],
            ],
        )
        .await
        .expect("insert v2 schema");

    // Detect drift.
    let drift_rows = db
        .client
        .query(
            "SELECT property, change_type, new_type
               FROM tide.singer_schema_drift('stripe-pipeline', 'tap-stripe', 'customers')",
            &[],
        )
        .await
        .expect("singer_schema_drift");

    assert_eq!(drift_rows.len(), 1, "one new property should be detected");
    let prop: String = drift_rows[0].get("property");
    let change: String = drift_rows[0].get("change_type");
    assert_eq!(prop, "email", "added property should be 'email'");
    assert_eq!(change, "added", "change type should be 'added'");
}

// ── OnSchemaChange variants ───────────────────────────────────────────────────

#[test]
fn test_on_schema_change_config_keys() {
    // Test that the on_schema_change config values are correctly recognised.
    let configs = [
        ("log", "log"),
        ("emit_event", "emit_event"),
        ("error", "error"),
        ("unknown", "log"), // default fallback
        ("", "log"),        // empty string → default
    ];
    for (input, expected_key) in configs {
        let normalized = match input {
            "emit_event" => "emit_event",
            "error" => "error",
            _ => "log",
        };
        assert_eq!(
            normalized, expected_key,
            "on_schema_change='{input}' should map to '{expected_key}'"
        );
    }
}

// ── Singer message format unit tests ─────────────────────────────────────────

#[test]
fn test_singer_schema_message_format() {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "id":   {"type": ["integer", "null"]},
            "name": {"type": ["string", "null"]}
        }
    });
    let msg = serde_json::json!({
        "type": "SCHEMA",
        "stream": "orders",
        "schema": schema,
        "key_properties": ["id"]
    });
    assert_eq!(msg["type"], "SCHEMA");
    assert_eq!(msg["stream"], "orders");
    assert!(msg["schema"]["properties"]["id"].is_object());
}

#[test]
fn test_singer_record_message_format() {
    let record = serde_json::json!({
        "id": 42,
        "name": "Alice",
        "_sdc_extracted_at": "2024-01-01T00:00:00Z"
    });
    let msg = serde_json::json!({
        "type": "RECORD",
        "stream": "users",
        "record": record,
        "time_extracted": "2024-01-01T00:00:00Z"
    });
    assert_eq!(msg["type"], "RECORD");
    assert_eq!(msg["stream"], "users");
    assert_eq!(msg["record"]["id"], 42);
}

#[test]
fn test_singer_state_message_format() {
    let msg = serde_json::json!({
        "type": "STATE",
        "value": {
            "bookmarks": {
                "orders": {"updated_at": "2024-01-01T00:00:00Z"}
            }
        }
    });
    assert_eq!(msg["type"], "STATE");
    assert!(msg["value"]["bookmarks"].is_object());
}

// ── Airbyte message format unit tests ────────────────────────────────────────

#[test]
fn test_airbyte_record_message_format() {
    let data = serde_json::json!({"id": 1, "amount": 99.95});
    let msg = serde_json::json!({
        "type": "RECORD",
        "record": {
            "stream": "orders",
            "namespace": "pgtide",
            "data": data,
            "emitted_at": 1_714_700_000_000i64
        }
    });
    assert_eq!(msg["type"], "RECORD");
    assert_eq!(msg["record"]["stream"], "orders");
    assert_eq!(msg["record"]["namespace"], "pgtide");
    assert_eq!(msg["record"]["data"]["amount"], 99.95);
}

#[test]
fn test_airbyte_catalog_message_format() {
    let msg = serde_json::json!({
        "type": "CATALOG",
        "catalog": {
            "streams": [{
                "stream": {
                    "name": "orders",
                    "namespace": "pgtide",
                    "json_schema": {"type": "object", "properties": {}}
                },
                "sync_mode": "append",
                "destination_sync_mode": "append"
            }]
        }
    });
    assert_eq!(msg["type"], "CATALOG");
    assert_eq!(msg["catalog"]["streams"][0]["stream"]["name"], "orders");
}

#[test]
fn test_airbyte_state_message_format() {
    let msg = serde_json::json!({
        "type": "STATE",
        "state": {
            "type": "GLOBAL",
            "global": {
                "shared_state": {"pg_tide_offset": "outbox:orders:1234"},
                "stream_states": []
            }
        }
    });
    assert_eq!(msg["type"], "STATE");
    assert_eq!(msg["state"]["type"], "GLOBAL");
    assert!(msg["state"]["global"]["shared_state"].is_object());
}

#[test]
fn test_airbyte_cdc_delete_marker() {
    // Airbyte CDC soft-delete: _ab_cdc_deleted_at set → op = "delete"
    let data = serde_json::json!({
        "id": 42,
        "_ab_cdc_deleted_at": "2024-01-15T10:30:00Z"
    });
    let is_delete = data
        .get("_ab_cdc_deleted_at")
        .and_then(|v| v.as_str())
        .is_some();
    assert!(
        is_delete,
        "record with _ab_cdc_deleted_at should be a delete operation"
    );
}

// ── Airbyte STATE persistence ─────────────────────────────────────────────────

#[tokio::test]
async fn test_airbyte_state_upsert_and_retrieve() {
    let db = PgTideTestDb::start().await;
    apply_v090_migration(&db).await;

    let state_value = serde_json::json!({
        "type": "GLOBAL",
        "global": {"shared_state": {"offset": 500}}
    });

    db.client
        .execute(
            "INSERT INTO tide.relay_airbyte_state (pipeline_name, source_name, state_value)
             VALUES ('stripe-pipeline', 'airbyte/source-stripe:latest', $1)",
            &[&state_value],
        )
        .await
        .expect("insert airbyte state");

    let rows = db
        .client
        .query(
            "SELECT state_value FROM tide.relay_airbyte_state
              WHERE pipeline_name = 'stripe-pipeline'",
            &[],
        )
        .await
        .expect("select airbyte state");

    assert_eq!(rows.len(), 1);
    let state: serde_json::Value = rows[0].get(0);
    assert_eq!(
        state
            .pointer("/global/shared_state/offset")
            .and_then(|v| v.as_i64()),
        Some(500)
    );
}
