//! Integration tests: TLS/mTLS connection support (v0.13.0 / v0.15.0).
//!
//! Tests the `pg_tls` module's URL parsing, ssl mode detection,
//! URL manipulation helpers, and connection behaviour (v0.15.0).
//!
//! v0.15.0 adds:
//! - `test_pg_tls_connect_require_fails_closed` — verifies fail-closed behaviour
//!   when `sslmode=require` and no TLS backend is compiled in.
//! - `test_pg_tls_connect_succeeds_with_disable` — verifies that
//!   `sslmode=disable` works with a standard (no-TLS) PostgreSQL server.
//! - `test_pg_tls_connect_succeeds_with_prefer` — verifies that
//!   `sslmode=prefer` falls back to plaintext when the server does not offer TLS.

mod common;

use common::PgTideTestDb;
#[cfg(not(feature = "native-tls"))]
use pg_tide_relay::error::RelayError;
use pg_tide_relay::pg_tls::{parse_ssl_mode, with_ssl_mode, PgSslMode};

#[test]
fn test_pg_ssl_mode_require_from_dsn() {
    let mode = parse_ssl_mode("host=localhost user=postgres sslmode=require");
    assert_eq!(mode, PgSslMode::Require);
}

#[test]
fn test_pg_ssl_mode_disable_from_dsn() {
    let mode = parse_ssl_mode("host=localhost user=postgres sslmode=disable");
    assert_eq!(mode, PgSslMode::Disable);
}

#[test]
fn test_pg_ssl_mode_verify_full_is_require() {
    let mode = parse_ssl_mode("host=localhost sslmode=verify-full");
    assert_eq!(mode, PgSslMode::Require);
}

#[test]
fn test_pg_ssl_mode_verify_ca_is_require() {
    let mode = parse_ssl_mode("host=localhost sslmode=verify-ca");
    assert_eq!(mode, PgSslMode::Require);
}

#[test]
fn test_pg_ssl_mode_require_is_required_flag() {
    assert!(PgSslMode::Require.is_required());
}

#[test]
fn test_pg_ssl_mode_disable_not_required() {
    assert!(!PgSslMode::Disable.is_required());
}

#[test]
fn test_with_ssl_mode_appends_to_dsn() {
    let url = with_ssl_mode("host=localhost user=postgres", Some(PgSslMode::Require));
    assert!(
        url.contains("sslmode=require"),
        "expected sslmode=require in: {url}"
    );
}

#[test]
fn test_with_ssl_mode_appends_to_postgres_url() {
    let url = with_ssl_mode("postgres://localhost/mydb", Some(PgSslMode::Require));
    assert!(
        url.contains("sslmode=require"),
        "expected sslmode=require in: {url}"
    );
}

#[test]
fn test_with_ssl_mode_appends_to_url_with_existing_params() {
    let url = with_ssl_mode(
        "postgres://localhost/mydb?connect_timeout=5",
        Some(PgSslMode::Require),
    );
    assert!(
        url.contains("sslmode=require"),
        "expected sslmode=require in: {url}"
    );
}

#[test]
fn test_with_ssl_mode_none_is_noop() {
    let original = "host=localhost user=postgres";
    let result = with_ssl_mode(original, None);
    assert_eq!(result, original);
}

#[test]
fn test_with_ssl_mode_disable() {
    let url = with_ssl_mode("host=localhost", Some(PgSslMode::Disable));
    assert!(url.contains("sslmode=disable"), "got: {url}");
}

// ── v0.15.0: pg_tls::connect() integration tests ─────────────────────────

/// v0.15.0: Verify fail-closed behaviour — `sslmode=require` must return
/// `RelayError::TlsRequired` immediately without attempting a connection.
///
/// This test does NOT require a running PostgreSQL instance; the error is
/// returned before the connection is attempted.
///
/// When compiled with the `native-tls` feature, `sslmode=require` attempts a
/// real TLS connection instead of returning `TlsRequired`, so this test is
/// only meaningful without that feature.
#[cfg(not(feature = "native-tls"))]
#[tokio::test]
async fn test_pg_tls_connect_require_fails_closed() {
    // Use a URL with sslmode=require pointing to a non-existent host so that
    // even if the guard were bypassed we would not accidentally connect.
    let result =
        pg_tide_relay::pg_tls::connect("postgres://user:pass@127.0.0.1:9999/db?sslmode=require")
            .await;
    assert!(
        matches!(result, Err(RelayError::TlsRequired { .. })),
        "expected TlsRequired error for sslmode=require, got: {:?}",
        result.err()
    );
}

/// v0.15.0: Verify `sslmode=require` with DSN format also fails closed.
///
/// Only meaningful when compiled without the `native-tls` feature;
/// see `test_pg_tls_connect_require_fails_closed`.
#[cfg(not(feature = "native-tls"))]
#[tokio::test]
async fn test_pg_tls_connect_require_dsn_fails_closed() {
    let result = pg_tide_relay::pg_tls::connect("host=127.0.0.1 port=9999 sslmode=require").await;
    assert!(
        matches!(result, Err(RelayError::TlsRequired { .. })),
        "expected TlsRequired error for sslmode=require DSN, got: {:?}",
        result.err()
    );
}

/// v0.15.0: Verify `sslmode=disable` connects successfully to a standard
/// (no-TLS) PostgreSQL testcontainer.
#[tokio::test]
async fn test_pg_tls_connect_succeeds_with_sslmode_disable() {
    let db = PgTideTestDb::start().await;
    let url = format!(
        "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres sslmode=disable",
        db.host_port
    );
    let result = pg_tide_relay::pg_tls::connect(&url).await;
    assert!(
        result.is_ok(),
        "sslmode=disable should succeed with plaintext: {:?}",
        result.err()
    );
    if let Ok((client, conn)) = result {
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let row = client.query_one("SELECT 1::int", &[]).await.unwrap();
        let val: i32 = row.get(0);
        assert_eq!(val, 1);
    }
}

/// v0.15.0: Verify `sslmode=prefer` falls back to plaintext when the server
/// does not offer TLS (the standard testcontainer PostgreSQL image).
#[tokio::test]
async fn test_pg_tls_connect_succeeds_with_sslmode_prefer() {
    let db = PgTideTestDb::start().await;
    let url = format!(
        "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres sslmode=prefer",
        db.host_port
    );
    let result = pg_tide_relay::pg_tls::connect(&url).await;
    assert!(
        result.is_ok(),
        "sslmode=prefer should succeed (plaintext fallback): {:?}",
        result.err()
    );
    if let Ok((client, conn)) = result {
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let row = client.query_one("SELECT 42::int", &[]).await.unwrap();
        let val: i32 = row.get(0);
        assert_eq!(val, 42);
    }
}

/// v0.15.0: Verify that a URL with no sslmode (defaults to prefer/disable)
/// connects successfully.
#[tokio::test]
async fn test_pg_tls_connect_succeeds_with_no_sslmode() {
    let db = PgTideTestDb::start().await;
    let url = format!(
        "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
        db.host_port
    );
    let result = pg_tide_relay::pg_tls::connect(&url).await;
    assert!(
        result.is_ok(),
        "connection without sslmode should succeed: {:?}",
        result.err()
    );
}
