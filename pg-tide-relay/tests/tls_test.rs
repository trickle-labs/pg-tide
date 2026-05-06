//! Integration tests: TLS/mTLS connection support (v0.13.0).
//!
//! Tests the `pg_tls` module's URL parsing, ssl mode detection,
//! and URL manipulation helpers.

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
