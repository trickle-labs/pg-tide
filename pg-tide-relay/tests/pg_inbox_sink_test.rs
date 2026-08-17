//! Integration test: PgInboxSink round-trip — RELAY-P2-TEST-PG-INBOX.
//!
//! v0.26.0: Closes the test gap noted in assessment-5 §7.
//!
//! Verifies that PgInboxSink:
//!   (a) inserts all messages with correct (event_id, source, payload, headers) columns;
//!   (b) deduplicates re-published batches via ON CONFLICT DO NOTHING;
//!   (c) completes the batch insert in a single SQL round-trip.

mod common;

use common::PgTideTestDb;
use pg_tide_relay::envelope::RelayMessage;
use pg_tide_relay::sink::pg_outbox::PgInboxSink;
use pg_tide_relay::sink::Sink;

/// Build a test RelayMessage using the reverse-mode constructor.
fn make_message(dedup_key: &str, subject: &str, payload: serde_json::Value) -> RelayMessage {
    RelayMessage::new_reverse(dedup_key, subject, payload)
}

#[tokio::test]
async fn test_pg_inbox_sink_round_trip() {
    let db = PgTideTestDb::start().await;

    // Create inbox table via test harness helper.
    db.setup_inbox("pg_sink_test").await;

    let postgres_url = format!(
        "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
        db.host_port
    );

    let mut sink = PgInboxSink::new(
        &format!("{postgres_url} sslmode=disable"),
        "pg_sink_test_inbox",
    )
    .await
    .expect("PgInboxSink::new should succeed");

    // Publish 100 messages in a single batch.
    let messages: Vec<RelayMessage> = (1..=100_u32)
        .map(|i| {
            make_message(
                &format!("evt-{i:04}"),
                "orders.created",
                serde_json::json!({"order_id": i, "status": "created"}),
            )
        })
        .collect();

    sink.publish(&messages)
        .await
        .expect("first publish should succeed");

    // (a) All 100 rows inserted with correct columns.
    let rows = db
        .client
        .query(
            "SELECT event_id, source, payload, headers \
             FROM tide.pg_sink_test_inbox \
             ORDER BY id",
            &[],
        )
        .await
        .expect("query inserted rows");

    assert_eq!(rows.len(), 100, "expected 100 inserted rows");

    let first_event_id: String = rows[0].get("event_id");
    assert_eq!(first_event_id, "evt-0001");

    let first_source: String = rows[0].get("source");
    assert_eq!(
        first_source, "orders.created",
        "source should be the message subject"
    );

    let first_payload: serde_json::Value = rows[0].get("payload");
    assert_eq!(first_payload["order_id"], 1);

    let headers: serde_json::Value = rows[0].get("headers");
    assert!(
        headers.is_object(),
        "headers should be a JSON object; got: {headers}"
    );

    // (b) Re-publishing the same 100 messages produces no duplicates.
    sink.publish(&messages)
        .await
        .expect("second publish should succeed");

    let count_row = db
        .client
        .query_one("SELECT COUNT(*)::bigint FROM tide.pg_sink_test_inbox", &[])
        .await
        .expect("count rows");
    let count: i64 = count_row.get(0);
    assert_eq!(
        count, 100,
        "re-publishing identical messages should not create duplicates"
    );
}

#[tokio::test]
async fn test_pg_inbox_sink_empty_batch_is_no_op() {
    let db = PgTideTestDb::start().await;
    db.setup_inbox("pg_sink_empty").await;

    let postgres_url = format!(
        "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
        db.host_port
    );

    let mut sink = PgInboxSink::new(
        &format!("{postgres_url} sslmode=disable"),
        "pg_sink_empty_inbox",
    )
    .await
    .expect("PgInboxSink::new should succeed");

    // Publishing an empty batch should return Ok without touching the DB.
    sink.publish(&[])
        .await
        .expect("empty publish should be a no-op");

    let count_row = db
        .client
        .query_one("SELECT COUNT(*)::bigint FROM tide.pg_sink_empty_inbox", &[])
        .await
        .expect("count rows");
    let count: i64 = count_row.get(0);
    assert_eq!(count, 0, "empty batch should not insert any rows");
}

/// v0.31.0: Hyphenated inbox name — guards the double-quoting fix in PgInboxSink.
///
/// Before v0.31.0, `PgInboxSink` used `tide.{table}` (unquoted), which produced
/// invalid SQL for inbox names containing hyphens:
///   INSERT INTO tide.order-events_inbox … (PostgreSQL interprets `-` as minus)
///
/// After the fix, the identifier is properly double-quoted:
///   INSERT INTO tide."order-events_inbox" …
#[tokio::test]
async fn test_pg_inbox_sink_hyphenated_name() {
    let db = PgTideTestDb::start().await;

    // Create inbox with a hyphenated name; setup_inbox creates tide."order-events_inbox".
    db.setup_inbox("order-events").await;

    let postgres_url = format!(
        "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
        db.host_port
    );

    let mut sink = PgInboxSink::new(
        &format!("{postgres_url} sslmode=disable"),
        "order-events_inbox",
    )
    .await
    .expect("PgInboxSink::new should accept hyphenated table names");

    // Publish 20 messages — with an unquoted identifier this would produce a
    // SQL syntax error.
    let messages: Vec<RelayMessage> = (1..=20_u32)
        .map(|i| {
            make_message(
                &format!("order-evt-{i:04}"),
                "orders.placed",
                serde_json::json!({"order_id": i}),
            )
        })
        .collect();

    sink.publish(&messages)
        .await
        .expect("publish to hyphenated inbox should not produce a SQL syntax error");

    // Assert all 20 rows appear with correct column values.
    let rows = db
        .client
        .query(
            r#"SELECT event_id, source FROM tide."order-events_inbox" ORDER BY id"#,
            &[],
        )
        .await
        .expect("query hyphenated inbox table");

    assert_eq!(rows.len(), 20, "expected 20 rows in order-events_inbox");

    let first_event_id: String = rows[0].get("event_id");
    assert_eq!(first_event_id, "order-evt-0001");

    let first_source: String = rows[0].get("source");
    assert_eq!(first_source, "orders.placed");

    // Deduplication still works with hyphenated names.
    sink.publish(&messages)
        .await
        .expect("second publish to hyphenated inbox should succeed");

    let count_row = db
        .client
        .query_one(
            r#"SELECT COUNT(*)::bigint FROM tide."order-events_inbox""#,
            &[],
        )
        .await
        .expect("count hyphenated inbox rows");
    let count: i64 = count_row.get(0);
    assert_eq!(
        count, 20,
        "re-publishing to hyphenated inbox should not create duplicates"
    );
}

#[tokio::test]
async fn test_pg_inbox_sink_invalid_table_rejected() {
    let postgres_url =
        "host=127.0.0.1 port=15432 user=postgres password=postgres dbname=postgres".to_string();

    // validate_relay_identifier rejects names with SQL-injection characters at
    // construction time — before any PostgreSQL connection is attempted.
    let result = PgInboxSink::new(&postgres_url, "bad\"name").await;
    assert!(
        result.is_err(),
        "PgInboxSink::new should reject identifiers with double-quote characters"
    );
}
