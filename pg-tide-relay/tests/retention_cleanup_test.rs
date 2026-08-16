//! v0.43 retention participant and exact-lag regression tests.

mod common;

use common::PgTideTestDb;

#[tokio::test]
async fn retention_status_uses_the_slowest_native_participant() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("orders").await;
    db.publish_messages(
        "orders",
        &(1..=5)
            .map(|id| serde_json::json!({"id": id}))
            .collect::<Vec<_>>(),
    )
    .await;

    for (pipeline, enabled, offset) in [("fast", true, 5_i64), ("slow", false, 2_i64)] {
        db.client
            .execute(
                "INSERT INTO tide.relay_outbox_config (name, enabled, config)
                 VALUES ($1, $2, '{\"source_type\":\"outbox\",\"source\":{\"outbox\":\"orders\"}}')
                 ON CONFLICT (name) DO UPDATE SET enabled = EXCLUDED.enabled,
                                                  config = EXCLUDED.config",
                &[&pipeline, &enabled],
            )
            .await
            .expect("insert relay pipeline");
        db.client
            .execute(
                "INSERT INTO tide.relay_consumer_offsets
                    (relay_group_id, pipeline_id, outbox_name, last_change_id, worker_id)
                 VALUES ('test', $1, 'orders', $2, 'test-worker')
                 ON CONFLICT (relay_group_id, pipeline_id, outbox_name)
                 DO UPDATE SET last_change_id = EXCLUDED.last_change_id",
                &[&pipeline, &offset],
            )
            .await
            .expect("insert relay offset");
    }

    let row = db
        .client
        .query_one(
            "SELECT safe_offset, blockers
               FROM tide.outbox_retention_status
              WHERE outbox_name = 'orders'",
            &[],
        )
        .await
        .expect("retention status");
    let safe_offset: Option<i64> = row.get(0);
    let blockers: serde_json::Value = row.get(1);
    assert_eq!(safe_offset, Some(2));
    assert_eq!(blockers.as_array().map(Vec::len), Some(2));
}

#[tokio::test]
async fn exact_lag_does_not_subtract_global_identity_gaps() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("orders").await;
    db.setup_outbox("payments").await;
    db.publish_messages("orders", &[serde_json::json!({"event": "order"})])
        .await;
    db.publish_messages("payments", &[serde_json::json!({"event": "payment"})])
        .await;

    db.client
        .execute(
            "INSERT INTO tide.relay_outbox_config (name, config)
             VALUES ('orders-pipeline',
                     '{\"source_type\":\"outbox\",\"source\":{\"outbox\":\"orders\"}}')",
            &[],
        )
        .await
        .expect("insert relay pipeline");
    db.client
        .execute(
            "INSERT INTO tide.relay_consumer_offsets
                (relay_group_id, pipeline_id, outbox_name, last_change_id)
             VALUES ('test', 'orders-pipeline', 'orders', 0)",
            &[],
        )
        .await
        .expect("insert relay offset");

    let lag: i64 = db
        .client
        .query_one(
            "SELECT lag
               FROM tide.relay_pipeline_lag
              WHERE pipeline_id = 'orders-pipeline'",
            &[],
        )
        .await
        .expect("lag view")
        .get(0);
    assert_eq!(lag, 1);
}
