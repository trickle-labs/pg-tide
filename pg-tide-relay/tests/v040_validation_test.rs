//! v0.40.0 (ADR-011) validation tests.
//!
//! Verifies the 0.39.0 → 0.40.0 migration and the native relay offset code:
//! the unconditional polling index, the outbox-scoped offset key, offset
//! backfill, monotonic offset writes through the relay code, fan-in quarantine,
//! message preservation, and explicit failure on ambiguous offset backfill.
//!
//! Requires Docker (testcontainers) for a real PostgreSQL 18 instance.

mod common;

use common::PgTideTestDb;
use std::sync::Arc;
use tokio_postgres::NoTls;

const V0_39_0_TO_0_40_0: &str = include_str!("../../sql/pg_tide--0.39.0--0.40.0.sql");

/// Apply the base schema plus every migration EXCEPT the final 0.39→0.40 one,
/// leaving the database at the v0.39.0 state.
async fn install_through_v0_39(client: &tokio_postgres::Client) {
    client
        .batch_execute("CREATE SCHEMA IF NOT EXISTS tide;")
        .await
        .expect("create schema");
    client
        .batch_execute(&common::strip_extension_comments(include_str!(
            "../../sql/pg_tide--0.1.0.sql"
        )))
        .await
        .expect("base schema");
    for (label, sql) in common::MIGRATIONS {
        if *label == "0.39.0 -> 0.40.0" {
            break;
        }
        let processed = common::strip_extension_comments(sql);
        client
            .batch_execute(&processed)
            .await
            .unwrap_or_else(|e| panic!("migration {label} failed: {e}"));
    }
}

#[tokio::test]
async fn test_v040_polling_index_is_unconditional() {
    let db = PgTideTestDb::start().await;
    let def: String = db
        .client
        .query_one(
            "SELECT indexdef FROM pg_indexes \
             WHERE schemaname = 'tide' AND indexname = 'idx_tide_outbox_messages_poll'",
            &[],
        )
        .await
        .expect("poll index must exist")
        .get(0);
    assert!(
        !def.to_lowercase().contains(" where "),
        "polling index must be unconditional (not partial): {def}"
    );
}

#[tokio::test]
async fn test_v040_offset_key_allows_same_pipeline_different_outbox() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("orders").await;
    db.setup_outbox("payments").await;
    db.client
        .execute(
            "INSERT INTO tide.relay_consumer_offsets \
             (relay_group_id, pipeline_id, outbox_name, last_change_id, worker_id) \
             VALUES ('g', 'p', 'orders', 1, 'w'), ('g', 'p', 'payments', 2, 'w')",
            &[],
        )
        .await
        .expect("same pipeline id for two outboxes must be allowed");
}

#[tokio::test]
async fn test_v040_offset_backfill_from_config_and_fanin() {
    let (client, _container) = raw_container().await;
    install_through_v0_39(&client).await;

    // Seed v0.39 state: pipeline config + a simple offset + a fanin offset + orphan.
    client
        .batch_execute(
            "INSERT INTO tide.tide_outbox_config(outbox_name) \
                 VALUES ('orders'), ('payments');
             INSERT INTO tide.relay_outbox_config(name, enabled, config) VALUES
                 ('orders-nats', true, '{\"source_type\":\"outbox\",\"source\":{\"outbox\":\"orders\"}}');
             INSERT INTO tide.relay_consumer_offsets(relay_group_id,pipeline_id,last_change_id,worker_id)
                 VALUES ('e2e-a','orders-nats',42,'w');
             INSERT INTO tide.relay_consumer_offsets(relay_group_id,pipeline_id,last_change_id,worker_id,fanin_member)
                 VALUES ('e2e-a','fanin-1',5,'w','payments');
             INSERT INTO tide.relay_consumer_offsets(relay_group_id,pipeline_id,last_change_id,worker_id)
                 VALUES ('e2e-a','ghost-pipeline',3,'w');",
        )
        .await
        .expect("seed v0.39 offsets");

    // Apply the 0.39 → 0.40 migration.
    client
        .batch_execute(&common::strip_extension_comments(V0_39_0_TO_0_40_0))
        .await
        .expect("0.39 → 0.40 migration must apply");

    // Simple offset backfilled from config source.outbox.
    let orders_outbox: String = client
        .query_one(
            "SELECT outbox_name FROM tide.relay_consumer_offsets \
             WHERE pipeline_id = 'orders-nats'",
            &[],
        )
        .await
        .expect("orders offset")
        .get(0);
    assert_eq!(orders_outbox, "orders");

    // Fan-in offset backfilled from fanin_member.
    let fanin_outbox: String = client
        .query_one(
            "SELECT outbox_name FROM tide.relay_consumer_offsets WHERE pipeline_id = 'fanin-1'",
            &[],
        )
        .await
        .expect("fanin offset")
        .get(0);
    assert_eq!(fanin_outbox, "payments");

    // Orphan pipeline offset removed.
    let ghost: i64 = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM tide.relay_consumer_offsets \
             WHERE pipeline_id = 'ghost-pipeline'",
            &[],
        )
        .await
        .expect("ghost count")
        .get(0);
    assert_eq!(ghost, 0, "orphan offset row must be removed");
}

#[tokio::test]
async fn test_v040_migration_fails_on_ambiguous_backfill() {
    let (client, _container) = raw_container().await;
    install_through_v0_39(&client).await;

    // A pipeline that exists in config but has no source.outbox → ambiguous.
    client
        .batch_execute(
            "INSERT INTO tide.relay_outbox_config(name, enabled, config) VALUES
                 ('weird', true, '{\"source_type\":\"outbox\",\"source\":{}}');
             INSERT INTO tide.relay_consumer_offsets(relay_group_id,pipeline_id,last_change_id,worker_id)
                 VALUES ('g','weird',9,'w');",
        )
        .await
        .expect("seed ambiguous offset");

    let result = client
        .batch_execute(&common::strip_extension_comments(V0_39_0_TO_0_40_0))
        .await;
    assert!(
        result.is_err(),
        "migration must fail explicitly on an ambiguous offset backfill"
    );
    let error = result.unwrap_err();
    let msg = error
        .as_db_error()
        .map(|db_error| db_error.message().to_owned())
        .unwrap_or_else(|| error.to_string());
    assert!(
        msg.contains("cannot be mapped to an outbox"),
        "error should explain the ambiguity, got: {msg}"
    );
}

#[tokio::test]
async fn test_v040_migration_preserves_messages_and_config() {
    let (client, _container) = raw_container().await;
    install_through_v0_39(&client).await;

    client
        .batch_execute(
            "INSERT INTO tide.tide_outbox_config(outbox_name) VALUES ('orders');
             INSERT INTO tide.tide_outbox_messages(outbox_name, payload, headers)
                 SELECT 'orders', jsonb_build_object('n', g), '{}'::jsonb
                 FROM generate_series(1, 25) g;
             INSERT INTO tide.relay_outbox_config(name, enabled, config) VALUES
                 ('orders-nats', true, '{\"source_type\":\"outbox\",\"source\":{\"outbox\":\"orders\"}}');",
        )
        .await
        .expect("seed messages");

    client
        .batch_execute(&common::strip_extension_comments(V0_39_0_TO_0_40_0))
        .await
        .expect("migration applies");

    let msg_count: i64 = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM tide.tide_outbox_messages WHERE outbox_name = 'orders'",
            &[],
        )
        .await
        .expect("msg count")
        .get(0);
    assert_eq!(msg_count, 25, "outbox messages must be preserved");

    let cfg_count: i64 = client
        .query_one("SELECT COUNT(*)::bigint FROM tide.relay_outbox_config", &[])
        .await
        .expect("cfg count")
        .get(0);
    assert_eq!(cfg_count, 1, "relay config must be preserved");
}

#[tokio::test]
async fn test_v040_offset_write_is_monotonic_through_relay_code() {
    use pg_tide_relay::envelope::{AckToken, RelayMessage};
    use pg_tide_relay::source::outbox::OutboxPollerSource;
    use pg_tide_relay::source::Source;

    let db = PgTideTestDb::start().await;
    db.setup_outbox("orders").await;

    // Standalone connection for the source (the source owns an Arc<Client>).
    let url = format!(
        "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
        db.host_port
    );
    let (client, connection) = tokio_postgres::connect(&url, NoTls)
        .await
        .expect("connect source client");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let mut source = OutboxPollerSource::new_simple_native(
        Arc::new(client),
        "orders",
        "{outbox}.{op}",
        "g-mono",
        "p-mono",
    )
    .await
    .expect("build native source");

    // Acknowledge a high offset, then a lower one — the stored value must not
    // rewind (monotonic GREATEST upsert in the relay code).
    let hi = ack_message(100);
    source.acknowledge(&hi).await.expect("ack high");
    let lo = ack_message(50);
    source.acknowledge(&lo).await.expect("ack low (no rewind)");

    let stored: i64 = db
        .client
        .query_one(
            "SELECT last_change_id FROM tide.relay_consumer_offsets \
             WHERE relay_group_id = 'g-mono' AND pipeline_id = 'p-mono' AND outbox_name = 'orders'",
            &[],
        )
        .await
        .expect("read offset")
        .get(0);
    assert_eq!(stored, 100, "a lower ack must not rewind the stored offset");

    fn ack_message(offset: i64) -> RelayMessage {
        let mut m = RelayMessage::new_forward(
            "outbox_orders",
            offset,
            0,
            "insert",
            serde_json::json!({}),
            false,
            None,
            "orders.insert",
        );
        m.ack_token = AckToken::OutboxOffset(offset);
        m
    }
}

/// Start a bare PostgreSQL 18 container and return a client + the container
/// guard (kept alive by the caller).
async fn raw_container() -> (
    tokio_postgres::Client,
    testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>,
) {
    use testcontainers::{runners::AsyncRunner, ImageExt};
    use testcontainers_modules::postgres::Postgres;

    let container = Postgres::default()
        .with_tag("18")
        .start()
        .await
        .expect("start postgres");
    let port = container.get_host_port_ipv4(5432).await.expect("get port");
    let url = format!("host=127.0.0.1 port={port} user=postgres password=postgres dbname=postgres");
    let mut attempt = 0;
    loop {
        match tokio_postgres::connect(&url, NoTls).await {
            Ok((client, connection)) => {
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                return (client, container);
            }
            Err(e) => {
                attempt += 1;
                if attempt >= 20 {
                    panic!("failed to connect: {e}");
                }
                tokio::time::sleep(std::time::Duration::from_millis(400)).await;
            }
        }
    }
}
