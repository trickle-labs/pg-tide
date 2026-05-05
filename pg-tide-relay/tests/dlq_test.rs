//! Integration test: Dead-Letter Queue (DLQ) — RELAY-P2-11.
//!
//! Verifies that the DLQ table can be written to and queried via the SQL API.

mod common;

use common::PgTideTestDb;

/// The SQL for the DLQ migration (v0.7.0).
const DLQ_MIGRATION_SQL: &str = include_str!("../../sql/pg_tide--0.6.0--0.7.0.sql");

async fn setup_dlq(db: &PgTideTestDb) {
    db.client
        .batch_execute(DLQ_MIGRATION_SQL)
        .await
        .expect("failed to apply DLQ migration");
}

#[tokio::test]
async fn test_dlq_insert_and_list() {
    let db = PgTideTestDb::start().await;
    setup_dlq(&db).await;

    // Insert a DLQ entry directly.
    db.client
        .execute(
            "INSERT INTO tide.relay_dlq
               (relay_mode, pipeline_name, source_name, sink_name,
                dedup_key, subject, payload, error_message, error_kind)
             VALUES
               ('forward', 'test-pipeline', 'outbox:orders', 'kafka',
                'key-001', 'orders.created',
                '{\"id\": 1}'::jsonb, 'connection refused', 'sink_permanent')",
            &[],
        )
        .await
        .expect("insert DLQ entry");

    // List via SQL API.
    let rows = db
        .client
        .query("SELECT * FROM tide.relay_dlq_list()", &[])
        .await
        .expect("relay_dlq_list");

    assert_eq!(rows.len(), 1);
    let dedup_key: String = rows[0].get("dedup_key");
    assert_eq!(dedup_key, "key-001");

    let resolved: bool = rows[0].get("resolved");
    assert!(!resolved);
}

#[tokio::test]
async fn test_dlq_retry_marks_entry() {
    let db = PgTideTestDb::start().await;
    setup_dlq(&db).await;

    let row = db
        .client
        .query_one(
            "INSERT INTO tide.relay_dlq
               (relay_mode, pipeline_name, source_name, sink_name,
                dedup_key, payload, error_message, error_kind)
             VALUES ('forward', 'p', 's', 'k', 'key-002',
                     '{}'::jsonb, 'err', 'decode')
             RETURNING id",
            &[],
        )
        .await
        .expect("insert");
    let id: i64 = row.get(0);

    db.client
        .execute("SELECT tide.relay_dlq_retry($1)", &[&id])
        .await
        .expect("relay_dlq_retry");

    let row = db
        .client
        .query_one(
            "SELECT retried_at IS NOT NULL AS retried FROM tide.relay_dlq WHERE id = $1",
            &[&id],
        )
        .await
        .expect("select");
    let retried: bool = row.get("retried");
    assert!(retried, "retried_at should be set after relay_dlq_retry");
}

#[tokio::test]
async fn test_dlq_retry_all() {
    let db = PgTideTestDb::start().await;
    setup_dlq(&db).await;

    // Insert 3 entries.
    for i in 0..3_i32 {
        let key = format!("key-{i:03}");
        db.client
            .execute(
                "INSERT INTO tide.relay_dlq
                   (relay_mode, pipeline_name, source_name, sink_name,
                    dedup_key, payload, error_message, error_kind)
                 VALUES ('forward', 'p', 's', 'k', $1,
                         '{}'::jsonb, 'err', 'sink_permanent')",
                &[&key],
            )
            .await
            .unwrap();
    }

    let marked: i64 = db
        .client
        .query_one("SELECT tide.relay_dlq_retry_all()", &[])
        .await
        .expect("relay_dlq_retry_all")
        .get(0);

    assert_eq!(marked, 3);
}

#[tokio::test]
async fn test_dlq_purge_removes_resolved() {
    let db = PgTideTestDb::start().await;
    setup_dlq(&db).await;

    // Insert a resolved entry with an old timestamp.
    db.client
        .execute(
            "INSERT INTO tide.relay_dlq
               (relay_mode, pipeline_name, source_name, sink_name,
                dedup_key, payload, error_message, error_kind,
                resolved, last_failed_at)
             VALUES ('forward', 'p', 's', 'k', 'old-key',
                     '{}'::jsonb, 'err', 'decode',
                     true, now() - interval '60 days')",
            &[],
        )
        .await
        .expect("insert old resolved entry");

    // Insert a recent resolved entry (should NOT be purged).
    db.client
        .execute(
            "INSERT INTO tide.relay_dlq
               (relay_mode, pipeline_name, source_name, sink_name,
                dedup_key, payload, error_message, error_kind, resolved)
             VALUES ('forward', 'p', 's', 'k', 'recent-key',
                     '{}'::jsonb, 'err', 'decode', true)",
            &[],
        )
        .await
        .expect("insert recent resolved entry");

    let deleted: i64 = db
        .client
        .query_one("SELECT tide.relay_dlq_purge(30)", &[])
        .await
        .expect("relay_dlq_purge")
        .get(0);

    assert_eq!(deleted, 1, "only the 60-day old entry should be purged");

    let remaining: i64 = db
        .client
        .query_one("SELECT COUNT(*)::bigint FROM tide.relay_dlq", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(remaining, 1);
}

#[tokio::test]
async fn test_dlq_stats() {
    let db = PgTideTestDb::start().await;
    setup_dlq(&db).await;

    db.client
        .execute(
            "INSERT INTO tide.relay_dlq
               (relay_mode, pipeline_name, source_name, sink_name,
                dedup_key, payload, error_message, error_kind)
             VALUES
               ('forward', 'pipe-a', 's', 'k', 'k1', '{}'::jsonb, 'e', 'decode'),
               ('forward', 'pipe-a', 's', 'k', 'k2', '{}'::jsonb, 'e', 'decode'),
               ('forward', 'pipe-b', 's', 'k', 'k3', '{}'::jsonb, 'e', 'sink_permanent')",
            &[],
        )
        .await
        .expect("insert stats data");

    let rows = db
        .client
        .query("SELECT * FROM tide.relay_dlq_stats()", &[])
        .await
        .expect("relay_dlq_stats");

    assert_eq!(rows.len(), 2);

    let mut found_decode = false;
    for row in &rows {
        let pipeline: String = row.get("pipeline_name");
        let kind: String = row.get("error_kind");
        let total: i64 = row.get("total");
        if pipeline == "pipe-a" && kind == "decode" {
            assert_eq!(total, 2);
            found_decode = true;
        }
    }
    assert!(found_decode, "expected pipe-a/decode entry in stats");
}

#[tokio::test]
async fn test_dlq_resolve() {
    let db = PgTideTestDb::start().await;
    setup_dlq(&db).await;

    let row = db
        .client
        .query_one(
            "INSERT INTO tide.relay_dlq
               (relay_mode, pipeline_name, source_name, sink_name,
                dedup_key, payload, error_message, error_kind)
             VALUES ('forward', 'p', 's', 'k', 'res-key',
                     '{}'::jsonb, 'err', 'decode')
             RETURNING id",
            &[],
        )
        .await
        .expect("insert");
    let id: i64 = row.get(0);

    db.client
        .execute("SELECT tide.relay_dlq_resolve($1)", &[&id])
        .await
        .expect("relay_dlq_resolve");

    let resolved: bool = db
        .client
        .query_one("SELECT resolved FROM tide.relay_dlq WHERE id = $1", &[&id])
        .await
        .expect("select")
        .get(0);
    assert!(resolved);

    // Should not appear in list after resolving.
    let list = db
        .client
        .query(
            "SELECT * FROM tide.relay_dlq_list() WHERE dedup_key = 'res-key'",
            &[],
        )
        .await
        .expect("list");
    assert!(
        list.is_empty(),
        "resolved entry should not appear in dlq_list"
    );
}
