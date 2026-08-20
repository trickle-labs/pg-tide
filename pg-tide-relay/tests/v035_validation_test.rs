/// v0.35.0 SQL validation integration tests.
///
/// Tests:
///   1. `relay_provision_tenant()` rejects invalid role names (digit-leading, reserved, special chars).
///   2. `relay_pipeline_dep_add()` rejects invalid trigger_policy values via SIMILAR TO.
///   3. `relay_pipeline_dep_add()` accepts all valid trigger_policy values.
///   4. `relay_truncate_delivery_receipts()` deletes only receipts older than the retention interval.
mod common;

use std::time::Duration;
use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::NoTls;

async fn connect_retry(url: &str) -> tokio_postgres::Client {
    for _ in 0..20 {
        if let Ok((client, conn)) = tokio_postgres::connect(url, NoTls).await {
            tokio::spawn(async move {
                let _ = conn.await;
            });
            return client;
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    panic!("failed to connect to postgres");
}

/// Sets up a fresh database with the full migration chain through v0.35.0.
async fn setup_db() -> (
    tokio_postgres::Client,
    testcontainers::ContainerAsync<Postgres>,
) {
    let container = Postgres::default()
        .with_tag("18")
        .start()
        .await
        .expect("postgres container");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    let url = format!("host=127.0.0.1 port={port} user=postgres password=postgres dbname=postgres");
    let client = connect_retry(&url).await;
    common::install_full_schema(&client).await;
    (client, container)
}

// ── relay_provision_tenant() role-name validation ────────────────────────────

#[tokio::test]
async fn test_provision_tenant_valid_role() {
    let (client, _container) = setup_db().await;
    // A well-formed role name: starts with letter, only alnum+underscore.
    let result = client
        .execute(
            "SELECT tide.relay_provision_tenant('tenant-valid', 'valid_relay_role')",
            &[],
        )
        .await;
    assert!(
        result.is_ok(),
        "valid role name should not raise an error, got: {:?}",
        result
    );
}

#[tokio::test]
async fn test_provision_tenant_digit_leading_role_rejected() {
    let (client, _container) = setup_db().await;
    // Role name starts with a digit.
    let result = client
        .execute(
            "SELECT tide.relay_provision_tenant('tenant-invalid', '1bad_role')",
            &[],
        )
        .await;
    assert!(
        result.is_err(),
        "digit-leading role name should raise an error"
    );
    let err = result.unwrap_err();
    let err_str = format!("{err:?}");
    assert!(
        err_str.contains("role name must match"),
        "expected 'role name must match' in error, got: {err_str}"
    );
}

#[tokio::test]
async fn test_provision_tenant_dollar_sign_role_rejected() {
    let (client, _container) = setup_db().await;
    // Role name with $ sign.
    let result = client
        .execute(
            "SELECT tide.relay_provision_tenant('tenant-dollar', 'bad$role')",
            &[],
        )
        .await;
    assert!(result.is_err(), "role name with $ should raise an error");
}

#[tokio::test]
async fn test_provision_tenant_reserved_postgres_role_rejected() {
    let (client, _container) = setup_db().await;
    let result = client
        .execute(
            "SELECT tide.relay_provision_tenant('tenant-reserved', 'postgres')",
            &[],
        )
        .await;
    assert!(
        result.is_err(),
        "reserved role 'postgres' should raise an error"
    );
    let err = result.unwrap_err();
    let err_str = format!("{err:?}");
    assert!(
        err_str.contains("reserved role"),
        "expected 'reserved role' in error, got: {err_str}"
    );
}

#[tokio::test]
async fn test_provision_tenant_tide_admin_role_rejected() {
    let (client, _container) = setup_db().await;
    let result = client
        .execute(
            "SELECT tide.relay_provision_tenant('tenant-admin', 'tide_admin')",
            &[],
        )
        .await;
    assert!(
        result.is_err(),
        "reserved role 'tide_admin' should raise an error"
    );
}

// ── relay_pipeline_dep_add() trigger_policy validation ───────────────────────

#[tokio::test]
async fn test_trigger_policy_invalid_values_rejected() {
    let (client, _container) = setup_db().await;

    // relay_pipeline_deps has no FK to relay_outbox_config, so we can insert
    // edges for non-existent pipeline names to test the policy validation.
    let invalid_policies: &[&str] = &[
        "never",
        "on_offset_gte",
        "on_offset_gte()",
        "on_offset_gte(abc)",
        "on_offset_gte(-1)",
        "ON_IDLE",
        "ALWAYS",
        " always",
        "always ",
        "on_offset_gte(1 2 3)",
    ];

    for policy in invalid_policies {
        let result = client
            .execute(
                "SELECT tide.relay_pipeline_dep_add('pipe-a', 'pipe-b', $1)",
                &[policy],
            )
            .await;
        assert!(
            result.is_err(),
            "policy '{policy}' should be rejected but was accepted"
        );
        // Clean up any partial insert.
        let _ = client
            .execute(
                "DELETE FROM tide.relay_pipeline_deps \
                 WHERE upstream_pipeline = 'pipe-a' AND downstream_pipeline = 'pipe-b'",
                &[],
            )
            .await;
    }
}

#[tokio::test]
async fn test_trigger_policy_valid_values_accepted() {
    let (client, _container) = setup_db().await;

    let valid_policies: &[&str] = &[
        "always",
        "on_idle",
        "on_offset_gte(0)",
        "on_offset_gte(1)",
        "on_offset_gte(1000)",
        "on_offset_gte(999999999)",
    ];

    for policy in valid_policies {
        // Clean up previous edge before each insert.
        let _ = client
            .execute(
                "DELETE FROM tide.relay_pipeline_deps \
                 WHERE upstream_pipeline = 'pipe-x' AND downstream_pipeline = 'pipe-y'",
                &[],
            )
            .await;

        let result = client
            .execute(
                "SELECT tide.relay_pipeline_dep_add('pipe-x', 'pipe-y', $1)",
                &[policy],
            )
            .await;
        assert!(
            result.is_ok(),
            "policy '{policy}' should be accepted but was rejected: {:?}",
            result
        );
    }
}

// ── relay_truncate_delivery_receipts() ────────────────────────────────────────

#[tokio::test]
async fn test_relay_truncate_delivery_receipts() {
    let (client, _container) = setup_db().await;

    // Insert 10 old delivery receipt rows (> 1 day old).
    client
        .execute(
            "INSERT INTO tide.relay_delivery_receipts
                 (pipeline_name, message_id, outbox_name, delivered_at)
             SELECT 'sweep-pipe', gs, 'outbox_sweep', now() - interval '2 days'
             FROM generate_series(1, 10) gs",
            &[],
        )
        .await
        .expect("insert old receipts");

    // Insert one recent receipt that should NOT be deleted.
    client
        .execute(
            "INSERT INTO tide.relay_delivery_receipts
                 (pipeline_name, message_id, outbox_name, delivered_at)
             VALUES ('sweep-pipe', 99999, 'outbox_sweep', now())",
            &[],
        )
        .await
        .expect("insert recent receipt");

    // Call the sweep function with a 1-day retention window.
    let row = client
        .query_one(
            "SELECT tide.relay_truncate_delivery_receipts('1 day'::interval)",
            &[],
        )
        .await
        .expect("sweep function call");

    let deleted: i64 = row.get(0);
    assert_eq!(deleted, 10, "should have deleted exactly 10 old rows");

    // Verify the recent receipt is still present.
    let remaining: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM tide.relay_delivery_receipts \
             WHERE pipeline_name = 'sweep-pipe'",
            &[],
        )
        .await
        .expect("count remaining")
        .get(0);
    assert_eq!(remaining, 1, "recent receipt should remain after sweep");
}
