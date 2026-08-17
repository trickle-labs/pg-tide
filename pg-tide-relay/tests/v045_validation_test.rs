//! v0.45 release-surface checks that do not require PostgreSQL.

#[test]
fn v045_forward_and_reverse_migrations_are_present() {
    let forward = include_str!("../../sql/pg_tide--0.44.0--0.45.0.sql");
    let reverse = include_str!("../../sql/pg_tide--0.45.0--0.44.0.sql");

    assert!(forward.contains("relay_runtime_status"));
    assert!(forward.contains("relay_pipeline_status"));
    assert!(forward.contains("v0.45.0"));
    assert!(reverse.contains("downgrade refused"));
    assert!(reverse.contains("relay_runtime_status"));
}

#[test]
fn v045_status_surface_is_observational_and_sanitized() {
    let migration = include_str!("../../sql/pg_tide--0.44.0--0.45.0.sql");

    assert!(migration.contains("advisory locks and durable offsets remain authoritative"));
    assert!(migration.contains("owner tokens and raw errors are intentionally omitted"));
    assert!(migration.contains("GRANT SELECT ON tide.relay_pipeline_status TO tide_reader"));
    assert!(migration.contains("GRANT SELECT ON tide.relay_pipeline_status TO tide_operator"));
}
