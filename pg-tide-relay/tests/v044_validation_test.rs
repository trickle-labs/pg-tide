//! v0.44 release validation guards that do not require the v0.44 migration.
//!
//! The database-backed checks are intentionally deferred; this file provides
//! compile-time release-surface checks that remain useful without PostgreSQL.

#[test]
fn v044_upgrade_script_is_present_and_mentions_target_version() {
    const MIGRATION: &str = include_str!("../../sql/pg_tide--0.43.0--0.44.0.sql");
    assert!(!MIGRATION.trim().is_empty());
    assert!(MIGRATION.contains("0.44.0"));
}

#[test]
fn v044_security_surfaces_are_wired() {
    let roles = include_str!("../../deploy/postgres/pg_tide_roles.sql");
    let http = include_str!("../src/http_util.rs");
    let secret = include_str!("../src/secret.rs");
    let features = include_str!("../Cargo.toml");

    assert!(roles.contains("tide_admin"));
    assert!(roles.contains("NOBYPASSRLS"));
    assert!(http.contains("Policy::none()"));
    assert!(http.contains(".no_proxy()"));
    assert!(secret.contains("MAX_SECRET_FILE_BYTES"));
    assert!(secret.contains("custom_flags"));
    assert!(!features.contains("experimental-full = [\"kms\","));
    assert!(!features.contains("\"kms-aws\","));
    assert!(!features.contains("\"kms-gcp\","));
    assert!(!features.contains("\"kms-vault\","));
}
