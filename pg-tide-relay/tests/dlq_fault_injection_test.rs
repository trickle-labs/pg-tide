//! Integration test: DLQ fault-injection — RELAY-P2-TEST-DLQ.
//!
//! Verifies the DLQ write path under fault conditions:
//!   1. Normal DLQ write: entries are written when INSERT privilege is granted.
//!   2. Revoked INSERT: when the relay role loses INSERT on relay_dlq, the DLQ
//!      insert_batch() returns an error (total write failure).
//!   3. Empty batches are a no-op.
//!   4. ErrorKind classification: all ErrorKind variants round-trip through the
//!      as_str() representation correctly.

mod common;

use std::sync::Arc;

use common::PgTideTestDb;
use pg_tide_relay::dlq::{self, DlqEntry, ErrorKind};
use pg_tide_relay::envelope::RelayMessage;

fn make_entry(dedup_key: &str, kind: ErrorKind) -> DlqEntry {
    DlqEntry {
        relay_mode: "forward".to_string(),
        pipeline_name: "test-pipeline".to_string(),
        source_name: "outbox:orders".to_string(),
        sink_name: "kafka".to_string(),
        dedup_key: dedup_key.to_string(),
        subject: Some("orders.created".to_string()),
        payload: serde_json::json!({"order_id": 1}),
        error_message: "connection refused".to_string(),
        error_kind: kind,
    }
}

/// Helper to build a DlqEntry from a RelayMessage.
fn entry_from_message(msg: &RelayMessage, kind: ErrorKind) -> DlqEntry {
    DlqEntry::from_message(
        "forward",
        "test-pipeline",
        "outbox:orders",
        "kafka",
        msg,
        "sink returned permanent error",
        kind,
    )
}

// ── Test 1: Normal DLQ write succeeds ────────────────────────────────────────

#[tokio::test]
async fn test_dlq_fault_normal_write_succeeds() {
    let db = PgTideTestDb::start().await;

    let client = Arc::new(db.client);

    let entries = vec![
        make_entry("fi-key-001", ErrorKind::SinkPermanent),
        make_entry("fi-key-002", ErrorKind::Decode),
        make_entry("fi-key-003", ErrorKind::MaxRetriesExceeded),
    ];

    dlq::insert_batch(&client, &entries)
        .await
        .expect("DLQ batch insert should succeed");

    let count_row = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM tide.relay_dlq WHERE pipeline_name = 'test-pipeline'",
            &[],
        )
        .await
        .expect("count DLQ entries");
    let count: i64 = count_row.get(0);
    assert_eq!(count, 3, "all 3 DLQ entries should be inserted");
}

// ── Test 2: DLQ write failure when INSERT is revoked ─────────────────────────

#[tokio::test]
async fn test_dlq_fault_revoked_insert_returns_error() {
    let db = PgTideTestDb::start().await;

    // Create a restricted role that cannot INSERT into relay_dlq.
    db.client
        .batch_execute(
            "CREATE ROLE tide_restricted_role NOINHERIT;
             GRANT USAGE ON SCHEMA tide TO tide_restricted_role;
             -- Explicitly do NOT grant INSERT on relay_dlq",
        )
        .await
        .expect("create restricted role");

    // Connect as the restricted role.
    let restricted_url = format!(
        "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
        db.host_port
    );
    // We simulate the permission failure by using a connection that has had
    // INSERT revoked after the connection was established.  We use SET ROLE
    // to switch to the restricted role.
    let (restricted_client, conn) = tokio_postgres::connect(&restricted_url, tokio_postgres::NoTls)
        .await
        .expect("connect restricted client");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    // Revoke INSERT on relay_dlq from all roles (superuser can still insert,
    // but we simulate the error by inserting into a table that errors via a trigger).
    // Instead, we test the actual permission path by using a role that lacks INSERT.
    // Since we're connected as superuser (postgres), we simulate by creating
    // a per-test table and revoking access.

    // Actually: test the total-failure path by inserting entries with an Arc<Client>
    // that references a table that does not exist (simulates a real write failure).
    let client = Arc::new(restricted_client);

    // SET ROLE to restricted_role (which has no INSERT permission).
    client
        .execute("SET ROLE tide_restricted_role", &[])
        .await
        .expect("set role");

    let entries = vec![make_entry("fi-revoked-001", ErrorKind::SinkPermanent)];

    // The insert should fail because tide_restricted_role has no INSERT on relay_dlq.
    let result = dlq::insert_batch(&client, &entries).await;
    assert!(
        result.is_err(),
        "DLQ insert_batch should return Err when INSERT is denied; got Ok"
    );
}

// ── Test 3: Empty batch is a no-op ───────────────────────────────────────────

#[tokio::test]
async fn test_dlq_fault_empty_batch_is_noop() {
    let db = PgTideTestDb::start().await;

    let client = Arc::new(db.client);

    // An empty batch should return Ok without touching the DB.
    dlq::insert_batch(&client, &[])
        .await
        .expect("empty DLQ batch should be a no-op");
}

// ── Test 4: ErrorKind variants classify correctly ────────────────────────────

#[tokio::test]
async fn test_dlq_fault_error_kind_classification() {
    let db = PgTideTestDb::start().await;

    let client = Arc::new(db.client);

    let test_cases = [
        ("kind-decode", ErrorKind::Decode, "decode"),
        ("kind-sink-perm", ErrorKind::SinkPermanent, "sink_permanent"),
        (
            "kind-inbox-perm",
            ErrorKind::InboxPermanent,
            "inbox_permanent",
        ),
        (
            "kind-max-retries",
            ErrorKind::MaxRetriesExceeded,
            "max_retries_exceeded",
        ),
    ];

    for (key, kind, expected_str) in &test_cases {
        let entries = vec![make_entry(key, *kind)];
        dlq::insert_batch(&client, &entries)
            .await
            .expect("insert DLQ entry");

        let row = client
            .query_one(
                "SELECT error_kind FROM tide.relay_dlq WHERE dedup_key = $1",
                &[key],
            )
            .await
            .expect("query error_kind");
        let stored: String = row.get("error_kind");
        assert_eq!(
            &stored, expected_str,
            "error_kind for {key} should be '{expected_str}'"
        );
    }
}

// ── Test 5: from_message constructor populates all fields ────────────────────

#[tokio::test]
async fn test_dlq_fault_from_message_fields() {
    let db = PgTideTestDb::start().await;

    let client = Arc::new(db.client);

    let msg = RelayMessage::new_reverse(
        "from-msg-key-001",
        "orders.shipped",
        serde_json::json!({"order_id": 42}),
    );

    let entry = entry_from_message(&msg, ErrorKind::SinkPermanent);
    dlq::insert_batch(&client, &[entry])
        .await
        .expect("insert DLQ entry from message");

    let row = client
        .query_one(
            "SELECT pipeline_name, source_name, sink_name, dedup_key, subject, error_kind
             FROM tide.relay_dlq
             WHERE dedup_key = 'from-msg-key-001'",
            &[],
        )
        .await
        .expect("query DLQ entry");

    let pipeline: String = row.get("pipeline_name");
    let source: String = row.get("source_name");
    let sink: String = row.get("sink_name");
    let dedup: String = row.get("dedup_key");
    let subject: String = row.get("subject");
    let kind: String = row.get("error_kind");

    assert_eq!(pipeline, "test-pipeline");
    assert_eq!(source, "outbox:orders");
    assert_eq!(sink, "kafka");
    assert_eq!(dedup, "from-msg-key-001");
    assert_eq!(subject, "orders.shipped");
    assert_eq!(kind, "sink_permanent");
}
