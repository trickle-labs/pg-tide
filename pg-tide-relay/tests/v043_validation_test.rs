//! v0.43 fresh-schema relay surface validation.

mod common;

use common::PgTideTestDb;

#[tokio::test]
async fn v043_retention_surface_is_registered() {
    let db = PgTideTestDb::start().await;

    for relation in [
        "outbox_cleanup_state",
        "outbox_storage_config",
        "tide_partition_events",
        "relay_pipeline_lag",
        "outbox_retention_status",
    ] {
        let exists: bool = db
            .client
            .query_one(
                "SELECT to_regclass($1) IS NOT NULL",
                &[&format!("tide.{relation}")],
            )
            .await
            .expect("relation lookup")
            .get(0);
        assert!(exists, "missing tide.{relation}");
    }

    let function_count: i64 = db
        .client
        .query_one(
            "SELECT COUNT(*)::bigint
               FROM pg_proc p
               JOIN pg_namespace n ON n.oid = p.pronamespace
              WHERE n.nspname = 'tide'
                AND p.proname IN ('outbox_sweep', 'outbox_maintain_partitions')",
            &[],
        )
        .await
        .expect("function lookup")
        .get(0);
    assert_eq!(function_count, 2);

    let layout: String = db
        .client
        .query_one(
            "SELECT storage_layout FROM tide.outbox_storage_config WHERE singleton",
            &[],
        )
        .await
        .expect("storage layout")
        .get(0);
    assert_eq!(layout, "heap");
}
