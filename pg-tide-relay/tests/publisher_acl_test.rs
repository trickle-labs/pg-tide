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

// ── v0.40.0 (ADR-011 §15) fail-closed authorization tests ─────────────────
//
// These exercise real authorization behavior through the public
// `tide.outbox_publish()` SQL function on a real installed extension. Point
// `PG_TIDE_E2E_DATABASE_URL` at a PostgreSQL 18 server with `pg_tide` installed
// (`cargo pgrx install`); the tests skip when the variable is absent. Failure
// injections run inside transactions and restore catalog state before exit.

use tokio_postgres::NoTls;

const ACL_ENV: &str = "PG_TIDE_E2E_DATABASE_URL";

async fn acl_client(url: &str) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(url, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

async fn count_orders(c: &tokio_postgres::Client) -> i64 {
    c.query_one(
        "SELECT COUNT(*)::bigint FROM tide.tide_outbox_messages WHERE outbox_name = 'acl_orders'",
        &[],
    )
    .await
    .expect("count")
    .get(0)
}

#[tokio::test]
async fn acl_fail_closed_authorization_matrix() {
    let url = match std::env::var(ACL_ENV) {
        Ok(u) if !u.is_empty() => u,
        _ => {
            eprintln!("skipping acl_fail_closed_authorization_matrix: set {ACL_ENV}.");
            return;
        }
    };
    let admin = acl_client(&url).await;
    admin
        .batch_execute(
            "DROP EXTENSION IF EXISTS pg_tide CASCADE; CREATE EXTENSION pg_tide; \
             DROP ROLE IF EXISTS acl_pub; DROP ROLE IF EXISTS acl_nopub; \
             CREATE ROLE acl_pub LOGIN; CREATE ROLE acl_nopub LOGIN; \
             GRANT USAGE ON SCHEMA tide TO acl_pub, acl_nopub; \
             GRANT INSERT, SELECT ON tide.tide_outbox_messages TO acl_pub, acl_nopub; \
             GRANT SELECT ON tide.tide_outbox_config, tide.outbox_publishers TO acl_pub, acl_nopub;",
        )
        .await
        .expect("setup roles");
    admin
        .execute(
            "SELECT tide.outbox_create_if_not_exists('acl_orders', 24, 10000, 'none')",
            &[],
        )
        .await
        .expect("create outbox");

    // Case: no ACL entries → default behavior (publish succeeds as superuser).
    admin
        .execute(
            "SELECT tide.outbox_publish('acl_orders', '{\"a\":1}'::jsonb, '{}'::jsonb)",
            &[],
        )
        .await
        .expect("no-acl publish should succeed");
    assert_eq!(count_orders(&admin).await, 1);

    // Grant only acl_pub.
    admin
        .execute(
            "SELECT tide.outbox_grant_publish('acl_orders', 'acl_pub')",
            &[],
        )
        .await
        .expect("grant");

    // Case: authorized publisher succeeds.
    {
        let c = acl_client(&url).await;
        c.batch_execute("SET ROLE acl_pub").await.expect("set role");
        c.execute(
            "SELECT tide.outbox_publish('acl_orders', '{\"a\":2}'::jsonb, '{}'::jsonb)",
            &[],
        )
        .await
        .expect("authorized publish succeeds");
    }
    assert_eq!(count_orders(&admin).await, 2);

    // Case: unauthorized publisher fails; no new row.
    {
        let c = acl_client(&url).await;
        c.batch_execute("SET ROLE acl_nopub")
            .await
            .expect("set role");
        let r = c
            .execute(
                "SELECT tide.outbox_publish('acl_orders', '{\"a\":3}'::jsonb, '{}'::jsonb)",
                &[],
            )
            .await;
        assert!(r.is_err(), "unauthorized publish must fail");
    }
    assert_eq!(
        count_orders(&admin).await,
        2,
        "no row for unauthorized publish"
    );

    // Case: superuser publishes even with ACLs present.
    admin
        .execute(
            "SELECT tide.outbox_publish('acl_orders', '{\"a\":4}'::jsonb, '{}'::jsonb)",
            &[],
        )
        .await
        .expect("superuser publish succeeds");
    assert_eq!(count_orders(&admin).await, 3);

    // Case: ACL query permission denied → fail closed, no row.
    {
        let c = acl_client(&url).await;
        c.batch_execute(
            "SET ROLE acl_pub; \
             BEGIN; \
             SET LOCAL ROLE postgres; \
             REVOKE SELECT ON tide.outbox_publishers FROM acl_pub; \
             SET LOCAL ROLE acl_pub;",
        )
        .await
        .expect("revoke select in tx");
        let r = c
            .execute(
                "SELECT tide.outbox_publish('acl_orders', '{\"a\":5}'::jsonb, '{}'::jsonb)",
                &[],
            )
            .await;
        assert!(
            r.is_err(),
            "publish must fail closed when the ACL query is denied"
        );
        // Roll back to restore the grant.
        let _ = c.batch_execute("ROLLBACK").await;
    }
    assert_eq!(count_orders(&admin).await, 3, "no row when ACL query fails");

    // Case: ACL table unavailable → fail closed, no row.
    {
        let c = acl_client(&url).await;
        c.batch_execute(
            "BEGIN; ALTER TABLE tide.outbox_publishers RENAME TO outbox_publishers_hidden;",
        )
        .await
        .expect("hide table in tx");
        let r = c
            .execute(
                "SELECT tide.outbox_publish('acl_orders', '{\"a\":6}'::jsonb, '{}'::jsonb)",
                &[],
            )
            .await;
        assert!(
            r.is_err(),
            "publish must fail closed when the ACL table is missing"
        );
        let _ = c.batch_execute("ROLLBACK").await;
    }
    assert_eq!(
        count_orders(&admin).await,
        3,
        "no row when ACL table is unavailable"
    );

    // Cleanup.
    admin
        .batch_execute(
            "DROP EXTENSION IF EXISTS pg_tide CASCADE; \
             DROP ROLE IF EXISTS acl_pub; DROP ROLE IF EXISTS acl_nopub;",
        )
        .await
        .expect("cleanup");
}
