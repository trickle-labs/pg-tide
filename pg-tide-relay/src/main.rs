// pg-tide — relay entry point.
// The relay backends and traits are public API used by external consumers.
// Dead code warnings are suppressed because many types are feature-gated or
// used only at runtime via trait objects rather than direct construction.
#![allow(dead_code, unused_imports)]

// Re-use public modules from the library target.
use pg_tide_relay::circuit_breaker;
use pg_tide_relay::config;
use pg_tide_relay::coordinator;
use pg_tide_relay::dlq;
use pg_tide_relay::envelope;
use pg_tide_relay::error;
use pg_tide_relay::jmespath_transform;
use pg_tide_relay::metrics;
use pg_tide_relay::otel;
use pg_tide_relay::rate_limiter;
use pg_tide_relay::routing;
use pg_tide_relay::schema_registry;
use pg_tide_relay::sink;
use pg_tide_relay::source;
use pg_tide_relay::transforms;

mod cli;

use clap::Parser;
use futures_util::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::{mpsc, watch, RwLock};
use tracing_subscriber::EnvFilter;

use cli::Cli;
use config::{LogFormat, RelayConfig};
use error::RelayError;

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
    if let Some(url) = cli.postgres_url {
        cfg.postgres_url = url;
    }
    cfg.metrics_addr = cli.metrics_addr;
    cfg.log_level = cli.log_level;
    cfg.relay_group_id = cli.relay_group_id;
    cfg.log_format = match cli.log_format.as_str() {
        "json" => LogFormat::Json,
        _ => LogFormat::Text,
    };
    let drain_timeout = Duration::from_secs(cli.drain_timeout);

    // Expand ${ENV:VAR_NAME} placeholders in connection strings.
    cfg = cfg.resolve_env_vars();

    // Initialise tracing.
    init_tracing(&cfg);

    if cfg.postgres_url.is_empty() {
        eprintln!("error: --postgres-url is required (or set PG_TIDE_POSTGRES_URL)");
        std::process::exit(1);
    }

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        relay_group_id = %cfg.relay_group_id,
        "pg-tide starting"
    );

    // A38: Connect to PostgreSQL (coordinator connection) with exponential backoff.
    let (db_client, db_conn) = connect_with_backoff(&cfg.postgres_url).await?;
    let db = Arc::new(db_client);

    // Spawn the coordinator connection driver.
    tokio::spawn(async move {
        if let Err(e) = db_conn.await {
            tracing::error!("coordinator DB connection error: {e}");
        }
    });

    // Open a dedicated LISTEN connection for hot-reload notifications.
    // poll_message drives the connection I/O and surfaces Notification events;
    // spawning the connection as a background task would silently drop them.
    let (notif_tx, notif_rx) = mpsc::channel::<()>(32);
    // Clone before the async move so the original `notif_tx` remains available
    // for the SIGHUP handler registered further below.
    let notif_tx_pg = notif_tx.clone();
    let notify_url = cfg.postgres_url.clone();
    tokio::spawn(async move {
        let pair = tokio_postgres::connect(&notify_url, tokio_postgres::NoTls).await;
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
        Arc::clone(&db),
        &cfg.relay_group_id,
        Arc::clone(&relay_metrics),
        Arc::clone(&health_state),
    );

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
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => { tracing::info!("received Ctrl+C") }
        _ = terminate => { tracing::info!("received SIGTERM") }
    }
}

/// A38: Connect to PostgreSQL with exponential backoff.
///
/// Retries with initial delay 100 ms, doubling each attempt up to 30 s,
/// with ±20 % jitter to avoid thundering-herd reconnects.
async fn connect_with_backoff(
    url: &str,
) -> Result<
    (
        tokio_postgres::Client,
        tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>,
    ),
    Box<dyn std::error::Error>,
> {
    const INITIAL_DELAY_MS: u64 = 100;
    const MAX_DELAY_MS: u64 = 30_000;
    const JITTER_PCT: f64 = 0.20;

    let mut delay_ms = INITIAL_DELAY_MS;
    let mut attempt = 0u32;

    loop {
        match tokio_postgres::connect(url, tokio_postgres::NoTls).await {
            Ok(pair) => return Ok(pair),
            Err(e) => {
                attempt += 1;
                // Apply ±20% jitter: seed from attempt number for determinism in tests.
                let jitter_range = (delay_ms as f64 * JITTER_PCT) as u64;
                let jitter = if jitter_range > 0 {
                    // Simple deterministic jitter: (attempt * 6364136223846793005) % range
                    let pseudo = attempt as u64 * 6_364_136_223_846_793_005_u64;
                    (pseudo % (jitter_range * 2)).saturating_sub(jitter_range)
                } else {
                    0
                };
                let sleep_ms = delay_ms.saturating_add(jitter);
                tracing::warn!(
                    attempt,
                    sleep_ms,
                    error = %e,
                    "PostgreSQL connection failed, retrying"
                );
                tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                delay_ms = (delay_ms * 2).min(MAX_DELAY_MS);
            }
        }
    }
}
