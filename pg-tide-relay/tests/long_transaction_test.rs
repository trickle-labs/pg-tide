//! v0.43 publisher/poller advisory-fence regression test.

mod common;

use common::PgTideTestDb;
use pg_tide_relay::envelope::AckToken;
use pg_tide_relay::source::outbox::OutboxPollerSource;
use pg_tide_relay::source::Source;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::NoTls;

async fn connect_source(db: &PgTideTestDb) -> Arc<tokio_postgres::Client> {
    let url = format!(
        "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
        db.host_port
    );
    let (client, connection) = tokio_postgres::connect(&url, NoTls)
        .await
        .expect("source connection");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Arc::new(client)
}

#[tokio::test]
async fn poll_waits_for_an_uncommitted_publisher_fence() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("orders").await;

    let mut writer = connect_source(&db).await;
    let writer = Arc::get_mut(&mut writer).expect("unique writer client");
    let tx = writer
        .transaction()
        .await
        .expect("begin publisher transaction");
    tx.query_one(
        "SELECT pg_advisory_xact_lock_shared(hashtextextended('pg_tide:outbox:orders', 0))",
        &[],
    )
    .await
    .expect("publisher fence");
    tx.execute(
        "INSERT INTO tide.tide_outbox_messages (outbox_name, payload, headers)
         VALUES ('orders', '{\"event\":\"a\"}', '{}'::jsonb)",
        &[],
    )
    .await
    .expect("uncommitted publish");

    db.publish_messages("orders", &[serde_json::json!({"event": "b"})])
        .await;
    let blocked_source = OutboxPollerSource::new_simple_native(
        connect_source(&db).await,
        "orders",
        "{outbox}.{op}",
        "test",
        "fence",
    )
    .await
    .expect("native source");

    let mut blocked_task = tokio::spawn(async move {
        let mut source = blocked_source;
        source.poll(10).await
    });
    let blocked = tokio::time::timeout(Duration::from_millis(100), &mut blocked_task).await;
    assert!(
        blocked.is_err(),
        "poll must wait while publisher fence is held"
    );
    blocked_task.abort();
    let _ = blocked_task.await;

    tx.commit().await.expect("commit publisher transaction");
    let mut source = OutboxPollerSource::new_simple_native(
        connect_source(&db).await,
        "orders",
        "{outbox}.{op}",
        "test",
        "fence-after",
    )
    .await
    .expect("native source after commit");
    let messages = source.poll(10).await.expect("poll after commit");
    assert_eq!(messages.len(), 2);
    let ids: Vec<i64> = messages.iter().filter_map(|m| m.outbox_id).collect();
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(matches!(
        messages.last().map(|m| &m.ack_token),
        Some(AckToken::OutboxOffset(_))
    ));
}
