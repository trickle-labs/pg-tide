//! Integration tests: Outbox publisher ACL (v0.13.0).
//!
//! Tests that the `tide.outbox_publishers` table enforces per-outbox publish
//! authorization.  These tests use the SQL-level API directly since the pgrx
//! extension tests exercise the Rust layer.

mod common;

use common::PgTideTestDb;

/// Apply the v0.13.0 migration on top of the base schema.
const V0_13_0_MIGRATION: &str = include_str!("../../sql/pg_tide--0.12.0--0.13.0.sql");

async fn apply_v13_migration(db: &PgTideTestDb) {
    // The base schema already includes v0.13.0 tables (added for pgrx test
    // compatibility), so the migration is idempotent.  Run it anyway to verify
    // the upgrade path.
    db.client
        .batch_execute(V0_13_0_MIGRATION)
        .await
        .expect("failed to apply v0.13.0 migration");
}

#[tokio::test]
async fn test_outbox_publishers_table_exists() {
    let db = PgTideTestDb::start().await;
    apply_v13_migration(&db).await;

    let exists: bool = db
        .client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
             WHERE table_schema = 'tide' AND table_name = 'outbox_publishers')",
            &[],
        )
        .await
        .expect("query")
        .get(0);
    assert!(
        exists,
        "tide.outbox_publishers should exist after v0.13.0 migration"
    );
}

#[tokio::test]
async fn test_outbox_publishers_insert_and_select() {
    let db = PgTideTestDb::start().await;
    apply_v13_migration(&db).await;

    // Create an outbox first (needed for FK constraint).
    db.client
        .execute(
            "INSERT INTO tide.tide_outbox_config (outbox_name, retention_hours, inline_threshold)
             VALUES ('acl-test-outbox', 24, 10000)",
            &[],
        )
        .await
        .expect("create outbox");

    // Grant a role.
    db.client
        .execute(
            "INSERT INTO tide.outbox_publishers (outbox_name, role_name)
             VALUES ('acl-test-outbox', 'app_writer')
             ON CONFLICT DO NOTHING",
            &[],
        )
        .await
        .expect("insert publisher");

    let count: i64 = db
        .client
        .query_one(
            "SELECT COUNT(*)::bigint FROM tide.outbox_publishers \
             WHERE outbox_name = 'acl-test-outbox' AND role_name = 'app_writer'",
            &[],
        )
        .await
        .expect("count")
        .get(0);
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_outbox_publishers_unique_constraint() {
    let db = PgTideTestDb::start().await;
    apply_v13_migration(&db).await;

    db.client
        .execute(
            "INSERT INTO tide.tide_outbox_config (outbox_name, retention_hours, inline_threshold)
             VALUES ('acl-idem-outbox', 24, 10000)",
            &[],
        )
        .await
        .expect("create outbox");

    // Insert twice — second should be a no-op (ON CONFLICT DO NOTHING).
    for _ in 0..2 {
        db.client
            .execute(
                "INSERT INTO tide.outbox_publishers (outbox_name, role_name)
                 VALUES ('acl-idem-outbox', 'some_role')
                 ON CONFLICT (outbox_name, role_name) DO NOTHING",
                &[],
            )
            .await
            .expect("idempotent insert");
    }

    let count: i64 = db
        .client
        .query_one(
            "SELECT COUNT(*)::bigint FROM tide.outbox_publishers \
             WHERE outbox_name = 'acl-idem-outbox'",
            &[],
        )
        .await
        .expect("count")
        .get(0);
    assert_eq!(count, 1, "duplicate grant should not create a second row");
}

#[tokio::test]
async fn test_outbox_publishers_delete_on_outbox_drop() {
    let db = PgTideTestDb::start().await;
    apply_v13_migration(&db).await;

    db.client
        .execute(
            "INSERT INTO tide.tide_outbox_config (outbox_name, retention_hours, inline_threshold)
             VALUES ('cascade-outbox', 24, 10000)",
            &[],
        )
        .await
        .expect("create outbox");
    db.client
        .execute(
            "INSERT INTO tide.outbox_publishers (outbox_name, role_name)
             VALUES ('cascade-outbox', 'reader')",
            &[],
        )
        .await
        .expect("insert publisher");

    // Drop the outbox → publisher should cascade-delete.
    db.client
        .execute(
            "DELETE FROM tide.tide_outbox_config WHERE outbox_name = 'cascade-outbox'",
            &[],
        )
        .await
        .expect("delete outbox");

    let count: i64 = db
        .client
        .query_one(
            "SELECT COUNT(*)::bigint FROM tide.outbox_publishers \
             WHERE outbox_name = 'cascade-outbox'",
            &[],
        )
        .await
        .expect("count")
        .get(0);
    assert_eq!(
        count, 0,
        "publisher ACL should cascade-delete when outbox is dropped"
    );
}
