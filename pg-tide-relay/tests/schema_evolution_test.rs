//! Integration tests: Schema evolution guardrails (v0.13.0).
//!
//! Tests fingerprinting, change classification, policy handling,
//! and database persistence of schema fingerprints.

mod common;

use common::PgTideTestDb;
use pg_tide_relay::schema_evolution::{
    classify_change, compute_fingerprint, extract_columns, OnSchemaChange, SchemaChangeKind,
    SchemaEvolutionGuard,
};
use std::sync::Arc;

// ── Unit tests (no DB) ────────────────────────────────────────────────────────

#[test]
fn test_fingerprint_is_order_independent() {
    let a = vec!["id".to_string(), "name".to_string(), "email".to_string()];
    let b = vec!["email".to_string(), "name".to_string(), "id".to_string()];
    assert_eq!(compute_fingerprint(&a), compute_fingerprint(&b));
}

#[test]
fn test_fingerprint_differs_for_different_column_sets() {
    let a = vec!["id".to_string(), "name".to_string()];
    let b = vec!["id".to_string(), "email".to_string()];
    assert_ne!(compute_fingerprint(&a), compute_fingerprint(&b));
}

#[test]
fn test_classify_no_change() {
    let cols = vec!["id".to_string(), "name".to_string()];
    assert_eq!(classify_change(&cols, &cols), SchemaChangeKind::NoChange);
}

#[test]
fn test_classify_additive_change() {
    let old = vec!["id".to_string(), "name".to_string()];
    let new = vec!["id".to_string(), "name".to_string(), "email".to_string()];
    assert_eq!(classify_change(&old, &new), SchemaChangeKind::Additive);
}

#[test]
fn test_classify_breaking_column_removal() {
    let old = vec!["id".to_string(), "name".to_string(), "email".to_string()];
    let new = vec!["id".to_string(), "name".to_string()]; // email removed
    assert_eq!(classify_change(&old, &new), SchemaChangeKind::Breaking);
}

#[test]
fn test_on_schema_change_roundtrip() {
    assert_eq!(OnSchemaChange::Warn.as_str(), "warn");
    assert_eq!(OnSchemaChange::Continue.as_str(), "continue");
    assert_eq!(OnSchemaChange::Pause.as_str(), "pause");
    assert_eq!(OnSchemaChange::Dlq.as_str(), "dlq");

    assert_eq!(OnSchemaChange::parse_config("warn"), OnSchemaChange::Warn);
    assert_eq!(OnSchemaChange::parse_config("pause"), OnSchemaChange::Pause);
    assert_eq!(
        OnSchemaChange::parse_config("unknown"),
        OnSchemaChange::Warn
    ); // default
}

#[test]
fn test_extract_columns_object() {
    let payload = serde_json::json!({"id": 1, "name": "test", "active": true});
    let mut cols = extract_columns(&payload);
    cols.sort();
    assert_eq!(cols, vec!["active", "id", "name"]);
}

#[test]
fn test_extract_columns_non_object_returns_empty() {
    assert_eq!(
        extract_columns(&serde_json::json!([1, 2, 3])),
        Vec::<String>::new()
    );
    assert_eq!(
        extract_columns(&serde_json::json!("string")),
        Vec::<String>::new()
    );
}

// ── Integration tests with DB ─────────────────────────────────────────────────

async fn make_guard(
    db: &PgTideTestDb,
    pipeline_name: &str,
    policy: OnSchemaChange,
) -> SchemaEvolutionGuard {
    let (client, conn) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
            db.host_port
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect for schema evolution guard");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    SchemaEvolutionGuard::new(pipeline_name, Arc::new(client), policy)
}

#[tokio::test]
async fn test_schema_evolution_guard_initial_observation() {
    let db = PgTideTestDb::start().await;
    let mut guard = make_guard(&db, "test-pipeline", OnSchemaChange::Warn).await;
    let cols = vec!["id".to_string(), "name".to_string(), "email".to_string()];

    let (kind, policy) = guard.observe("orders", &cols).await.expect("observe");
    assert_eq!(
        kind,
        SchemaChangeKind::Initial,
        "first observation is Initial"
    );
    assert_eq!(policy, OnSchemaChange::Warn);
}

#[tokio::test]
async fn test_schema_evolution_guard_no_change_on_repeat() {
    let db = PgTideTestDb::start().await;
    let mut guard = make_guard(&db, "repeat-pipeline", OnSchemaChange::Warn).await;
    let cols = vec!["id".to_string(), "name".to_string()];

    // First observation.
    let (first_kind, _) = guard
        .observe("payments", &cols)
        .await
        .expect("first observe");
    assert_eq!(first_kind, SchemaChangeKind::Initial);

    // Second observation with same columns.
    let (second_kind, _) = guard
        .observe("payments", &cols)
        .await
        .expect("second observe");
    assert_eq!(second_kind, SchemaChangeKind::NoChange);
}

#[tokio::test]
async fn test_schema_evolution_guard_detects_additive_change() {
    let db = PgTideTestDb::start().await;
    let mut guard = make_guard(&db, "additive-pipeline", OnSchemaChange::Warn).await;
    let old_cols = vec!["id".to_string(), "name".to_string()];
    let new_cols = vec!["id".to_string(), "name".to_string(), "email".to_string()];

    guard.observe("events", &old_cols).await.expect("initial");
    let (kind, _) = guard.observe("events", &new_cols).await.expect("additive");
    assert_eq!(kind, SchemaChangeKind::Additive);
}

#[tokio::test]
async fn test_schema_evolution_guard_detects_breaking_change() {
    let db = PgTideTestDb::start().await;
    let mut guard = make_guard(&db, "breaking-pipeline", OnSchemaChange::Pause).await;
    let old_cols = vec!["id".to_string(), "name".to_string(), "email".to_string()];
    let new_cols = vec!["id".to_string(), "name".to_string()]; // email removed

    guard.observe("users", &old_cols).await.expect("initial");
    let (kind, policy) = guard.observe("users", &new_cols).await.expect("breaking");
    assert_eq!(kind, SchemaChangeKind::Breaking);
    assert_eq!(policy, OnSchemaChange::Pause);
}
