//! v0.43 shared-parent storage and polling-index regression tests.

mod common;

use common::PgTideTestDb;

#[tokio::test]
async fn fresh_storage_is_heap_with_one_public_parent() {
    let db = PgTideTestDb::start().await;

    let layout: String = db
        .client
        .query_one(
            "SELECT storage_layout FROM tide.outbox_storage_config WHERE singleton",
            &[],
        )
        .await
        .expect("storage config")
        .get(0);
    assert_eq!(layout, "heap");

    let parent_count: i64 = db
        .client
        .query_one(
            "SELECT COUNT(*)::bigint
               FROM pg_class c
               JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = 'tide'
                AND c.relname = 'tide_outbox_messages'",
            &[],
        )
        .await
        .expect("parent relation")
        .get(0);
    assert_eq!(parent_count, 1);
}

#[tokio::test]
async fn canonical_polling_index_covers_outbox_and_id() {
    let db = PgTideTestDb::start().await;
    let index_count: i64 = db
        .client
        .query_one(
            "SELECT COUNT(*)::bigint
               FROM pg_indexes
              WHERE schemaname = 'tide'
                AND tablename = 'tide_outbox_messages'
                AND indexdef ILIKE '%outbox_name%'
                AND indexdef ILIKE '%id%'",
            &[],
        )
        .await
        .expect("polling index")
        .get(0);
    assert!(index_count > 0);
}
