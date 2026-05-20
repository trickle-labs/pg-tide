// pg-tide — relay entry point.
// The relay backends and traits are public API used by external consumers.
// Feature-gated modules have items that are conditionally compiled;
// targeted per-item allows are used in those modules directly.

// Re-use public modules from the library target.
use pg_tide_relay::config;
use pg_tide_relay::coordinator;
use pg_tide_relay::metrics;
use pg_tide_relay::pg_tls;

mod cli;
mod cmd;

use clap::{CommandFactory, Parser};
use futures_util::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::{mpsc, watch, RwLock};
use tracing_subscriber::EnvFilter;

use cli::{Cli, Commands};
use config::{LogFormat, RelayConfig};

/// v0.27.0: Emit a clap-formatted "missing required argument" error and exit
/// with code 2 when a PostgreSQL URL is absent for a command that requires it.
///
/// Using `Cli::command().error().exit()` produces the same structured output
/// as clap's own validation errors — consistent format, correct exit code, and
/// no bare `eprintln!` calls violating the project logging convention.
fn require_postgres_url(url: &str, for_cmd: &str) {
    if url.is_empty() {
        Cli::command()
            .error(
                clap::error::ErrorKind::MissingRequiredArgument,
                format!("--postgres-url (or $PG_TIDE_POSTGRES_URL) is required for `{for_cmd}`"),
            )
            .exit();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Load config from file if provided, then overlay CLI args.
    let mut cfg = if let Some(ref config_path) = cli.config {
        let content = tokio::fs::read_to_string(config_path).await?;
        toml::from_str::<RelayConfig>(&content)?
    } else {
        RelayConfig::default()
    };

    // CLI args take precedence over file config.
    if let Some(url) = cli.postgres_url.clone() {
        cfg.postgres_url = url;
    }
    // v0.18.0: --postgres-url-file takes precedence over --postgres-url.
    // Reads the URL from a file to avoid credential exposure in /proc/<pid>/cmdline.
    if let Some(ref url_file) = cli.postgres_url_file {
        let url = tokio::fs::read_to_string(url_file)
            .await
            .map_err(|e| format!("failed to read --postgres-url-file {url_file}: {e}"))?;
        cfg.postgres_url = url.trim().to_string();
    }
    cfg.metrics_addr = cli.metrics_addr.clone();
    cfg.log_level = cli.log_level.clone();
    cfg.relay_group_id = cli.relay_group_id.clone();
    cfg.log_format = match cli.log_format.as_str() {
        "json" => LogFormat::Json,
        _ => LogFormat::Text,
    };
    // v0.15.0: CLI overrides for max_pipelines and max_connections.
    if let Some(max) = cli.max_pipelines {
        cfg.max_owned_pipelines = max;
    }
    if let Some(max) = cli.max_connections {
        cfg.max_connections = max;
    }
    // v0.25.0: Tenant ID for multi-tenant relay groups.
    if let Some(ref tid) = cli.tenant_id {
        cfg.tenant_id = Some(tid.clone());
    }
    let drain_timeout = Duration::from_secs(cli.drain_timeout);

    // Expand ${ENV:VAR_NAME} placeholders in connection strings.
    cfg = cfg.resolve_env_vars();

    // Initialise tracing.
    init_tracing(&cfg);

    // Dispatch subcommands before checking postgres_url.
    match cli.command {
        Some(Commands::Doctor { postgres_url }) => {
            let url = postgres_url
                .or_else(|| std::env::var("PG_TIDE_POSTGRES_URL").ok())
                .unwrap_or_else(|| cfg.postgres_url.clone());
            require_postgres_url(&url, "doctor");
            return cmd::doctor::run_doctor(&url).await;
        }
        Some(Commands::ValidateConfig {
            pipeline,
            postgres_url,
        }) => {
            let url = postgres_url
                .or_else(|| std::env::var("PG_TIDE_POSTGRES_URL").ok())
                .unwrap_or_else(|| cfg.postgres_url.clone());
            require_postgres_url(&url, "validate-config");
            return cmd::validate_config::run_validate_config(&url, &pipeline).await;
        }
        Some(Commands::Replay(replay_cmd)) => {
            return cmd::replay::run_replay_command(replay_cmd, &cfg.postgres_url).await;
        }
        Some(Commands::Asyncapi(asyncapi_cmd)) => {
            return cmd::asyncapi::run_asyncapi_command(asyncapi_cmd, &cfg.postgres_url).await;
        }
        Some(Commands::Ducklake(ducklake_cmd)) => {
            return cmd::ducklake::run_ducklake_command(ducklake_cmd, &cfg.postgres_url).await;
        }
        Some(Commands::Sweep {
            outbox,
            postgres_url,
        }) => {
            let url = postgres_url
                .or_else(|| std::env::var("PG_TIDE_POSTGRES_URL").ok())
                .unwrap_or_else(|| cfg.postgres_url.clone());
            require_postgres_url(&url, "sweep");
            return cmd::sweep::run_sweep(&url, outbox.as_deref()).await;
        }
        Some(Commands::Status { postgres_url }) => {
            let url = postgres_url
                .or_else(|| std::env::var("PG_TIDE_POSTGRES_URL").ok())
                .unwrap_or_else(|| cfg.postgres_url.clone());
            require_postgres_url(&url, "status");
            return cmd::status::run_status(&url).await;
        }
        None => {}
    }

    // v0.25.0: Handle --self-test flag: verify connectivity, schema, and
    // advisory lock, then exit 0 on success or 1 on failure.
    if cli.self_test {
        require_postgres_url(&cfg.postgres_url, "--self-test");
        return cmd::self_test::run_self_test(&cfg.postgres_url).await;
    }

    require_postgres_url(&cfg.postgres_url, "relay daemon");

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        relay_group_id = %cfg.relay_group_id,
        "pg-tide starting"
    );

    // v0.15.0: Create a deadpool-postgres connection pool for coordinator
    // metadata operations.  Workers still create their own dedicated
    // connections via pg_tls::connect (one per pipeline).
    let pool = create_coordinator_pool(&cfg.postgres_url, cfg.max_connections)?;

    // Open a dedicated LISTEN connection for hot-reload notifications.
    // poll_message drives the connection I/O and surfaces Notification events;
    // spawning the connection as a background task would silently drop them.
    let (notif_tx, notif_rx) = mpsc::channel::<()>(32);
    // Clone before the async move so the original `notif_tx` remains available
    // for the SIGHUP handler registered further below.
    let notif_tx_pg = notif_tx.clone();
    let notify_url = cfg.postgres_url.clone();
    tokio::spawn(async move {
        // v0.15.0: Use pg_tls::connect to honour sslmode from the URL.
        let pair = pg_tls::connect(&notify_url).await;
        let (notif_client, notif_conn) = match pair {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("notification connection failed: {e}");
                return;
            }
        };
        if let Err(e) = notif_client.execute("LISTEN tide_relay_config", &[]).await {
            tracing::error!("LISTEN setup failed: {e}");
            return;
        }
        // Drive the connection manually so we can intercept notifications.
        let mut conn = notif_conn;
        let mut stream = std::pin::pin!(futures_util::stream::poll_fn(
            move |cx| conn.poll_message(cx)
        ));
        while let Some(msg) = stream.next().await {
            match msg {
                Ok(tokio_postgres::AsyncMessage::Notification(_)) => {
                    if notif_tx_pg.send(()).await.is_err() {
                        break; // coordinator stopped.
                    }
                }
                Ok(_) => {} // notices, etc.
                Err(e) => {
                    tracing::error!("notification connection error: {e}");
                    break;
                }
            }
        }
    });

    // Start metrics + health server.
    let relay_metrics = metrics::RelayMetrics::new()?;
    let health_state = Arc::new(RwLock::new(metrics::HealthState::default()));

    metrics::start_metrics_server(
        &cfg.metrics_addr,
        Arc::clone(&relay_metrics),
        Arc::clone(&health_state),
    )
    .await?;

    // Build coordinator.
    let mut coordinator = coordinator::Coordinator::new(
        pool,
        &cfg.relay_group_id,
        Arc::clone(&relay_metrics),
        Arc::clone(&health_state),
    );
    // v0.15.0: Apply max_owned_pipelines from config.
    coordinator.set_max_owned_pipelines(cfg.max_owned_pipelines);
    // v0.25.0: Apply tenant_id for multi-tenant relay groups.
    if let Some(ref tid) = cfg.tenant_id {
        coordinator.set_tenant_id(tid.clone());
    }

    // Shutdown watch channel: signal handler sends true when SIGTERM/Ctrl-C arrives.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    // SIGHUP channel: sends a reload notification to the coordinator.
    let notif_tx_sighup = notif_tx.clone();
    tokio::spawn(async move {
        wait_for_shutdown().await;
        let _ = shutdown_tx.send(true);
    });

    // SIGHUP handler: force a full config reload from the database.
    #[cfg(unix)]
    tokio::spawn(async move {
        let mut sighup = match signal::unix::signal(signal::unix::SignalKind::hangup()) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("failed to install SIGHUP handler: {e}");
                return;
            }
        };
        loop {
            sighup.recv().await;
            tracing::info!("received SIGHUP — forcing config reload");
            let _ = notif_tx_sighup.send(()).await;
        }
    });

    // Run the coordinator discovery loop (blocks until shutdown).
    coordinator
        .run(
            cfg.postgres_url.clone(),
            cfg.default_batch_size,
            Duration::from_secs(cfg.discovery_interval_secs),
            shutdown_rx,
            notif_rx,
        )
        .await?;

    tracing::info!(
        drain_timeout_secs = drain_timeout.as_secs(),
        "pg-tide shutting down — draining in-flight messages"
    );

    // Give active pipelines time to finish their current batch.
    if drain_timeout.as_secs() > 0 {
        tokio::time::timeout(drain_timeout, coordinator.drain())
            .await
            .unwrap_or_else(|_| {
                tracing::warn!(
                    "drain timeout ({} s) exceeded — forcing shutdown",
                    drain_timeout.as_secs()
                );
            });
    }

    coordinator.release_all_locks().await?;

    tracing::info!("pg-tide stopped");
    Ok(())
}

fn init_tracing(cfg: &RelayConfig) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&cfg.log_level));

    match cfg.log_format {
        LogFormat::Json => {
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(filter)
                .init();
        }
        LogFormat::Text => {
            tracing_subscriber::fmt().with_env_filter(filter).init();
        }
    }
}

async fn wait_for_shutdown() {
    let ctrl_c = async {
        // v0.23.0: Replace .expect() with graceful degradation so that
        // signal-registration failure on restricted seccomp profiles logs a
        // clear warning and lets the relay continue rather than panicking.
        if let Err(e) = signal::ctrl_c().await {
            tracing::warn!("Ctrl+C signal handler failed: {e}");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                tracing::warn!("failed to install SIGTERM handler: {e}");
                // Fall back to waiting forever on this branch so tokio::select!
                // can still fire on Ctrl+C.
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => { tracing::info!("received Ctrl+C") }
        _ = terminate => { tracing::info!("received SIGTERM") }
    }
}

// ── Connection pool helper ───────────────────────────────────────────────

/// v0.15.0: Create a deadpool-postgres connection pool for coordinator
/// metadata operations.
///
/// The pool is used for short-lived coordinator queries (pipeline discovery,
/// advisory lock management).  Workers create their own dedicated connections
/// via `pg_tls::connect`.
fn create_coordinator_pool(
    url: &str,
    max_size: usize,
) -> Result<deadpool_postgres::Pool, Box<dyn std::error::Error>> {
    let mut cfg = deadpool_postgres::Config::new();
    cfg.url = Some(url.to_string());
    cfg.pool = Some(deadpool_postgres::PoolConfig {
        max_size,
        ..Default::default()
    });
    let pool = cfg
        .create_pool(
            Some(deadpool_postgres::Runtime::Tokio1),
            tokio_postgres::NoTls,
        )
        .map_err(|e| format!("failed to create coordinator connection pool: {e}"))?;
    Ok(pool)
}
