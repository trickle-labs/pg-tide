//! v0.42.0 validation for monotonic offsets and audited rewind APIs.
//!
//! Requires Docker (testcontainers) for PostgreSQL 18.

mod common;

use common::PgTideTestDb;

#[tokio::test]
async fn test_v042_installs_monotonic_triggers_and_rewind_functions() {
    let db = PgTideTestDb::start().await;

    let trigger_count: i64 = db
        .client
        .query_one(
            "SELECT COUNT(*)::bigint
               FROM pg_trigger
              WHERE tgname IN
                    ('tide_consumer_offsets_monotonic',
                     'relay_consumer_offsets_monotonic')",
            &[],
        )
        .await
        .expect("query trigger count")
        .get(0);
    assert_eq!(trigger_count, 2);

    let function_count: i64 = db
        .client
        .query_one(
            "SELECT COUNT(*)::bigint
               FROM pg_proc
              WHERE pronamespace = 'tide'::regnamespace
                AND proname IN
                    ('admin_rewind_offset', 'admin_rewind_relay_offset')",
            &[],
        )
        .await
        .expect("query rewind functions")
        .get(0);
    assert_eq!(function_count, 2);
}

#[tokio::test]
async fn test_v042_normal_offsets_cannot_rewind_but_admin_can() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("orders").await;
    db.setup_consumer_group("orders-group", "orders").await;

    db.client
        .execute(
            "INSERT INTO tide.tide_consumer_offsets
                    (group_name, consumer_id, committed_offset)
             VALUES ('orders-group', 'worker', 10)",
            &[],
        )
        .await
        .expect("seed consumer offset");
    db.client
        .execute(
            "INSERT INTO tide.relay_outbox_config
                    (name, enabled, config)
             VALUES ('orders-relay', false,
                     '{\"source_type\":\"outbox\",\"source\":{\"outbox\":\"orders\"}}')",
            &[],
        )
        .await
        .expect("seed disabled relay");
    db.client
        .execute(
            "INSERT INTO tide.relay_consumer_offsets
                    (relay_group_id, pipeline_id, outbox_name, last_change_id, worker_id)
             VALUES ('group-a', 'orders-relay', 'orders', 10, 'test')",
            &[],
        )
        .await
        .expect("seed relay offset");

    assert!(
        db.client
            .execute(
                "UPDATE tide.tide_consumer_offsets
                    SET committed_offset = 9
                  WHERE group_name = 'orders-group' AND consumer_id = 'worker'",
                &[],
            )
            .await
            .is_err(),
        "normal consumer writes must not rewind"
    );
    assert!(
        db.client
            .execute(
                "UPDATE tide.relay_consumer_offsets
                    SET last_change_id = 9
                  WHERE relay_group_id = 'group-a'
                    AND pipeline_id = 'orders-relay'
                    AND outbox_name = 'orders'",
                &[],
            )
            .await
            .is_err(),
        "normal relay writes must not rewind"
    );

    db.client
        .execute(
            "SELECT tide.admin_rewind_offset('orders-group', 'worker', 4, true)",
            &[],
        )
        .await
        .expect("admin consumer rewind");
    db.client
        .execute(
            "SELECT tide.admin_rewind_relay_offset(
                        'group-a', 'orders-relay', 'orders', 3, true)",
            &[],
        )
        .await
        .expect("admin relay rewind");

    let consumer_offset: i64 = db
        .client
        .query_one(
            "SELECT committed_offset
               FROM tide.tide_consumer_offsets
              WHERE group_name = 'orders-group' AND consumer_id = 'worker'",
            &[],
        )
        .await
        .expect("read consumer offset")
        .get(0);
    assert_eq!(consumer_offset, 4);

    let relay_offset: i64 = db
        .client
        .query_one(
            "SELECT last_change_id
               FROM tide.relay_consumer_offsets
              WHERE relay_group_id = 'group-a'
                AND pipeline_id = 'orders-relay'
                AND outbox_name = 'orders'",
            &[],
        )
        .await
        .expect("read relay offset")
        .get(0);
    assert_eq!(relay_offset, 3);

    let audit_count: i64 = db
        .client
        .query_one(
            "SELECT COUNT(*)::bigint
               FROM tide.tide_security_audit
              WHERE action IN ('ADMIN_REWIND_OFFSET', 'ADMIN_REWIND_RELAY_OFFSET')
                AND performed_by = session_user",
            &[],
        )
        .await
        .expect("read rewind audit")
        .get(0);
    assert_eq!(audit_count, 2);
}
