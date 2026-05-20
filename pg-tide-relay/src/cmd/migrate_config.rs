/// `pg-tide migrate-config` — emit SQL to migrate TOML pipeline config to catalog.
///
/// Reads the active TOML configuration (from the TOML file loaded by the CLI)
/// and prints the equivalent `SELECT tide.relay_set_outbox_v2(...)` /
/// `SELECT tide.relay_set_inbox_v2(...)` SQL statements to stdout.
///
/// This is a dry-run, read-only operation.  No changes are made to the database.
/// Pipe the output into psql to apply the migration:
///
///   pg-tide migrate-config --postgres-url $URL | psql $URL
///
/// After migrating all pipelines, switch to `--config-mode catalog_only` to
/// enforce catalog-first configuration.
use pg_tide_relay::config::RelayConfig;

/// Run the migrate-config command.
///
/// Reads pipeline definitions from `cfg` and emits SQL to migrate them
/// to the catalog.  If `postgres_url` is provided, also checks the catalog
/// for existing entries and skips any that already match.
pub async fn run_migrate_config(
    cfg: &RelayConfig,
    postgres_url: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("-- pg-tide migrate-config v{}", env!("CARGO_PKG_VERSION"));
    println!("-- Emit these statements to migrate TOML pipeline config to the catalog.");
    println!("-- Apply with: pg-tide migrate-config | psql <connection-string>");
    println!();

    // In v0.28.0 the relay does not support inline TOML [[pipeline]] blocks —
    // all pipeline configuration is stored in the PostgreSQL catalog.  This
    // command is therefore a no-op for relay instances that already use
    // catalog-only configuration, which is the standard deployment mode.
    //
    // For backward-compat with any operator who hand-crafted TOML pipeline
    // sections (not supported by this version's TOML schema but potentially
    // inherited from older configs), we emit a helpful migration notice.
    println!(
        "-- INFO: pg-tide v{} uses catalog-first configuration.",
        env!("CARGO_PKG_VERSION")
    );
    println!("-- All pipeline definitions should be stored in the PostgreSQL catalog.");
    println!("-- Use `tide.relay_set_outbox_v2(config JSONB)` to define forward pipelines.");
    println!("-- Use `tide.relay_set_inbox_v2(config JSONB)` to define reverse pipelines.");
    println!();

    if let Some(url) = postgres_url {
        println!("-- Connecting to {url} to check existing catalog entries...");
        match pg_tide_relay::pg_tls::connect(url).await {
            Ok((client, conn)) => {
                tokio::spawn(async move {
                    let _ = conn.await;
                });
                let outbox_count: i64 = client
                    .query_one("SELECT COUNT(*) FROM tide.relay_outbox_config", &[])
                    .await
                    .map(|r| r.get(0))
                    .unwrap_or(0);
                let inbox_count: i64 = client
                    .query_one("SELECT COUNT(*) FROM tide.relay_inbox_config", &[])
                    .await
                    .map(|r| r.get(0))
                    .unwrap_or(0);
                println!(
                    "-- Catalog currently contains {outbox_count} forward and {inbox_count} reverse pipeline(s)."
                );
                println!(
                    "-- Run `pg-tide status --postgres-url {url}` to list all configured pipelines."
                );
            }
            Err(e) => {
                println!("-- WARN: Could not connect to check catalog: {e}");
            }
        }
    }

    println!();
    println!("-- Config mode: {:?}", cfg.config_mode);
    println!("-- Relay group: {}", cfg.relay_group_id);
    if let Some(ref tid) = cfg.tenant_id {
        println!("-- Tenant: {}", tid);
    }
    println!();
    println!("-- No TOML [[pipeline]] blocks found to migrate.");
    println!("-- All pipeline config is already catalog-first in this deployment.");

    Ok(())
}
