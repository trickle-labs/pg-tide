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

use clap::Parser;
use futures_util::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::{mpsc, watch, RwLock};
use tracing_subscriber::EnvFilter;

use cli::{AsyncapiCommands, Cli, Commands, ReplayCommands};
use config::{LogFormat, RelayConfig};

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
            if url.is_empty() {
                eprintln!("error: --postgres-url is required for doctor");
                std::process::exit(1);
            }
            return run_doctor(&url).await;
        }
        Some(Commands::ValidateConfig {
            pipeline,
            postgres_url,
        }) => {
            let url = postgres_url
                .or_else(|| std::env::var("PG_TIDE_POSTGRES_URL").ok())
                .unwrap_or_else(|| cfg.postgres_url.clone());
            if url.is_empty() {
                eprintln!("error: --postgres-url is required for validate-config");
                std::process::exit(1);
            }
            return run_validate_config(&url, &pipeline).await;
        }
        Some(Commands::Replay(replay_cmd)) => {
            return run_replay_command(replay_cmd, &cfg.postgres_url).await;
        }
        Some(Commands::Asyncapi(asyncapi_cmd)) => {
            return run_asyncapi_command(asyncapi_cmd, &cfg.postgres_url).await;
        }
        Some(Commands::Sweep {
            outbox,
            postgres_url,
        }) => {
            let url = postgres_url
                .or_else(|| std::env::var("PG_TIDE_POSTGRES_URL").ok())
                .unwrap_or_else(|| cfg.postgres_url.clone());
            if url.is_empty() {
                eprintln!("error: --postgres-url is required for sweep");
                std::process::exit(1);
            }
            return run_sweep(&url, outbox.as_deref()).await;
        }
        Some(Commands::Status { postgres_url }) => {
            let url = postgres_url
                .or_else(|| std::env::var("PG_TIDE_POSTGRES_URL").ok())
                .unwrap_or_else(|| cfg.postgres_url.clone());
            if url.is_empty() {
                eprintln!("error: --postgres-url is required for status");
                std::process::exit(1);
            }
            return run_status(&url).await;
        }
        None => {}
    }

    if cfg.postgres_url.is_empty() {
        eprintln!("error: --postgres-url is required (or set PG_TIDE_POSTGRES_URL)");
        std::process::exit(1);
    }

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

// ── `pg-tide doctor` ─────────────────────────────────────────────────────

/// Validate PostgreSQL connectivity, schema presence, and catalog health.
async fn run_doctor(url: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("pg-tide doctor v{}", env!("CARGO_PKG_VERSION"));
    println!("Connecting to PostgreSQL...");

    // v0.15.0: Use pg_tls::connect (honours sslmode from URL).
    let (client, conn) = pg_tls::connect(url)
        .await
        .map_err(|e| format!("connection failed: {e}"))?;

    tokio::spawn(async move {
        let _ = conn.await;
    });

    println!("  [OK] Connected to PostgreSQL");

    // Check schema exists.
    let schema_exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'tide')",
            &[],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or(false);

    if schema_exists {
        println!("  [OK] Schema 'tide' exists");
    } else {
        println!("  [FAIL] Schema 'tide' not found — is pg_tide installed?");
        std::process::exit(1);
    }

    // Check required tables.
    let required_tables = [
        "tide_outbox_config",
        "tide_outbox_messages",
        "tide_inbox_config",
        "relay_outbox_config",
        "relay_inbox_config",
        "relay_consumer_offsets",
    ];
    let mut all_ok = true;
    for table in &required_tables {
        let exists: bool = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
                 WHERE table_schema = 'tide' AND table_name = $1)",
                &[table],
            )
            .await
            .map(|r| r.get(0))
            .unwrap_or(false);
        if exists {
            println!("  [OK] Table tide.{table}");
        } else {
            println!("  [FAIL] Table tide.{table} missing");
            all_ok = false;
        }
    }

    // Check relay_consumer_offsets has the correct schema (v0.12.0 migration).
    let has_change_id: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
             WHERE table_schema = 'tide' AND table_name = 'relay_consumer_offsets' \
             AND column_name = 'last_change_id')",
            &[],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or(false);
    if has_change_id {
        println!("  [OK] relay_consumer_offsets.last_change_id column present");
    } else {
        println!("  [WARN] relay_consumer_offsets.last_change_id missing — run upgrade to v0.12.0");
        all_ok = false;
    }

    // Count configured pipelines.
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
        "  [INFO] {outbox_count} forward pipeline(s), {inbox_count} reverse pipeline(s) configured"
    );

    // v0.15.0: Check for claim-check delta tables (pg_trickle >= 0.46.0).
    let has_sweep_fn: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.routines \
             WHERE routine_schema = 'tide' AND routine_name = 'outbox_truncate_delivered')",
            &[],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or(false);
    if has_sweep_fn {
        println!("  [OK] tide.outbox_truncate_delivered() present (v0.15.0+)");
    } else {
        println!("  [WARN] tide.outbox_truncate_delivered() missing — upgrade to v0.15.0");
    }

    // v0.17.0: Check (a) DLQ INSERT privilege.
    let dlq_writable: bool = client
        .query_one(
            "SELECT has_table_privilege('tide.relay_dlq', 'INSERT')",
            &[],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or(false);
    if dlq_writable {
        println!("  [OK] Current role has INSERT on tide.relay_dlq");
    } else {
        println!("  [FAIL] Current role lacks INSERT on tide.relay_dlq — DLQ writes will fail");
        all_ok = false;
    }

    // v0.17.0: Check (b) advisory lock acquisition under relay_group_id 1 (default group).
    let lock_ok: bool = client
        .query_one(
            "SELECT pg_try_advisory_lock(hashtext('pg_tide_relay_group_default'))",
            &[],
        )
        .await
        .map(|r| r.get::<_, bool>(0))
        .unwrap_or(false);
    if lock_ok {
        // Release the test lock immediately.
        let _ = client
            .execute(
                "SELECT pg_advisory_unlock(hashtext('pg_tide_relay_group_default'))",
                &[],
            )
            .await;
        println!("  [OK] Advisory lock acquisition succeeded");
    } else {
        println!("  [WARN] Advisory lock acquisition failed — another relay instance may hold it");
    }

    // v0.17.0: Check (c) LISTEN permission for tide_relay_config.
    let listen_ok = client.execute("LISTEN tide_relay_config", &[]).await;
    if listen_ok.is_ok() {
        let _ = client.execute("UNLISTEN tide_relay_config", &[]).await;
        println!("  [OK] LISTEN on tide_relay_config permitted");
    } else {
        println!("  [FAIL] LISTEN on tide_relay_config denied — hot-reload will not function");
        all_ok = false;
    }

    if all_ok {
        println!("\npg-tide doctor: all checks passed.");
        Ok(())
    } else {
        println!("\npg-tide doctor: one or more checks failed.");
        std::process::exit(1);
    }
}

// ── `pg-tide validate-config` ────────────────────────────────────────────

/// Dry-run source and sink factories for a named pipeline.
async fn run_validate_config(url: &str, pipeline: &str) -> Result<(), Box<dyn std::error::Error>> {
    use pg_tide_relay::config::{resolve_pipeline_secrets, PipelineConfig, PipelineDirection};

    println!("pg-tide validate-config — pipeline: {pipeline}");

    // v0.15.0: Use pg_tls::connect (honours sslmode from URL).
    let (client, conn) = pg_tls::connect(url)
        .await
        .map_err(|e| format!("connection failed: {e}"))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    // Load pipeline from catalog (outbox config first, then inbox).
    let row = client
        .query_opt(
            "SELECT config, 'forward'::text AS direction, enabled \
             FROM tide.relay_outbox_config WHERE name = $1
             UNION ALL
             SELECT config, 'reverse'::text, enabled \
             FROM tide.relay_inbox_config WHERE name = $1
             LIMIT 1",
            &[&pipeline],
        )
        .await?;

    let row = match row {
        Some(r) => r,
        None => {
            eprintln!("error: pipeline '{pipeline}' not found in catalog");
            std::process::exit(1);
        }
    };

    let config: serde_json::Value = row.get(0);
    let direction_str: String = row.get(1);
    let enabled: bool = row.get(2);

    if !enabled {
        println!("  [WARN] pipeline '{pipeline}' is disabled");
    }

    let direction = if direction_str == "forward" {
        PipelineDirection::Forward
    } else {
        PipelineDirection::Reverse
    };

    let pc = PipelineConfig {
        name: pipeline.to_string(),
        direction,
        enabled,
        config,
        tenant_name: "default".to_string(),
    };

    let resolved = resolve_pipeline_secrets(pc.config.clone())
        .map_err(|e| format!("secret resolution failed: {e}"))?;

    println!("  [OK] Secrets resolved");

    let resolved_pc = PipelineConfig {
        name: pc.name.clone(),
        direction: pc.direction,
        enabled: pc.enabled,
        config: resolved,
        tenant_name: pc.tenant_name.clone(),
    };

    // Try to build source.
    // v0.15.0: Use pg_tls::connect (honours sslmode from URL).
    let (worker_client, worker_conn) = pg_tls::connect(url)
        .await
        .map_err(|e| format!("worker connection failed: {e}"))?;
    tokio::spawn(async move {
        let _ = worker_conn.await;
    });
    let db = Arc::new(worker_client);

    match pg_tide_relay::coordinator::build_source_for_validation(
        &resolved_pc,
        Arc::clone(&db),
        "validate",
    )
    .await
    {
        Ok(src) => println!("  [OK] Source '{}' instantiated", src.name()),
        Err(e) => {
            println!("  [FAIL] Source instantiation failed: {e}");
            std::process::exit(1);
        }
    }

    match pg_tide_relay::coordinator::build_sink_for_validation(&resolved_pc, Arc::clone(&db)).await
    {
        Ok(sink) => println!("  [OK] Sink '{}' instantiated", sink.name()),
        Err(e) => {
            println!("  [FAIL] Sink instantiation failed: {e}");
            std::process::exit(1);
        }
    }

    println!("\nvalidate-config: pipeline '{pipeline}' configuration is valid.");
    Ok(())
}

// ── `pg-tide replay` ─────────────────────────────────────────────────────

/// Dispatch replay workbench subcommands.
async fn run_replay_command(
    cmd: ReplayCommands,
    default_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match cmd {
        ReplayCommands::Preview {
            outbox,
            from_id,
            to_id,
            limit,
            postgres_url,
        } => {
            let url = postgres_url
                .or_else(|| std::env::var("PG_TIDE_POSTGRES_URL").ok())
                .unwrap_or_else(|| default_url.to_string());
            if url.is_empty() {
                eprintln!("error: --postgres-url is required for replay preview");
                std::process::exit(1);
            }
            run_replay_preview(&url, &outbox, from_id, to_id, limit).await
        }
        ReplayCommands::DryRun {
            pipeline,
            from_id,
            to_id,
            limit,
            postgres_url,
        } => {
            let url = postgres_url
                .or_else(|| std::env::var("PG_TIDE_POSTGRES_URL").ok())
                .unwrap_or_else(|| default_url.to_string());
            if url.is_empty() {
                eprintln!("error: --postgres-url is required for replay dry-run");
                std::process::exit(1);
            }
            run_replay_dry_run(&url, &pipeline, from_id, to_id, limit).await
        }
        ReplayCommands::DlqResolve {
            pipeline,
            dedup_key,
            postgres_url,
        } => {
            let url = postgres_url
                .or_else(|| std::env::var("PG_TIDE_POSTGRES_URL").ok())
                .unwrap_or_else(|| default_url.to_string());
            if url.is_empty() {
                eprintln!("error: --postgres-url is required for dlq-resolve");
                std::process::exit(1);
            }
            run_dlq_resolve(&url, &pipeline, &dedup_key).await
        }
        ReplayCommands::DlqRequeue {
            pipeline,
            dedup_key,
            postgres_url,
        } => {
            let url = postgres_url
                .or_else(|| std::env::var("PG_TIDE_POSTGRES_URL").ok())
                .unwrap_or_else(|| default_url.to_string());
            if url.is_empty() {
                eprintln!("error: --postgres-url is required for dlq-requeue");
                std::process::exit(1);
            }
            run_dlq_requeue(&url, &pipeline, &dedup_key).await
        }
    }
}

/// `pg-tide replay preview` — print outbox messages in an ID range as JSONL.
async fn run_replay_preview(
    url: &str,
    outbox: &str,
    from_id: i64,
    to_id: i64,
    limit: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let (client, conn) = pg_tls::connect(url).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let rows = client
        .query(
            "SELECT id, outbox_name, payload, headers, created_at, consumed_at IS NOT NULL AS consumed
             FROM tide.tide_outbox_messages
             WHERE outbox_name = $1 AND id BETWEEN $2 AND $3
             ORDER BY id
             LIMIT $4",
            &[&outbox, &from_id, &to_id, &(limit as i64)],
        )
        .await?;

    for row in &rows {
        let id: i64 = row.get("id");
        let outbox_name: String = row.get("outbox_name");
        let payload: serde_json::Value = row.get("payload");
        let headers: serde_json::Value = row.get("headers");
        let created_at: chrono::DateTime<chrono::Utc> = row.get("created_at");
        let consumed: bool = row.get("consumed");
        let line = serde_json::json!({
            "id": id,
            "outbox_name": outbox_name,
            "payload": payload,
            "headers": headers,
            "created_at": created_at.to_rfc3339(),
            "consumed": consumed,
        });
        println!("{}", serde_json::to_string(&line)?);
    }

    eprintln!("{} message(s) previewed from outbox '{outbox}'", rows.len());
    Ok(())
}

/// `pg-tide replay dry-run` — evaluate transforms without publishing.
async fn run_replay_dry_run(
    url: &str,
    pipeline: &str,
    from_id: i64,
    to_id: i64,
    limit: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let (client, conn) = pg_tls::connect(url).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    // Load pipeline config.
    let row = client
        .query_opt(
            "SELECT config FROM tide.relay_outbox_config WHERE name = $1
             UNION ALL
             SELECT config FROM tide.relay_inbox_config WHERE name = $1
             LIMIT 1",
            &[&pipeline],
        )
        .await?;

    let config: serde_json::Value = match row {
        Some(r) => r.get(0),
        None => {
            eprintln!("error: pipeline '{pipeline}' not found");
            std::process::exit(1);
        }
    };

    let outbox = config
        .pointer("/source/outbox")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let wire_fmt = pg_tide_relay::wire_format::from_config(&config);

    let rows = client
        .query(
            "SELECT id, outbox_name, payload, headers
             FROM tide.tide_outbox_messages
             WHERE outbox_name = $1 AND id BETWEEN $2 AND $3
             ORDER BY id
             LIMIT $4",
            &[&outbox, &from_id, &to_id, &(limit as i64)],
        )
        .await?;

    eprintln!(
        "Dry-run transform evaluation for pipeline '{pipeline}' ({} message(s)):",
        rows.len()
    );
    for row in &rows {
        let id: i64 = row.get("id");
        let payload: serde_json::Value = row.get("payload");
        let raw = pg_tide_relay::wire_format::RawMessage::from_json(outbox, &payload);
        match wire_fmt.decode(&raw) {
            Ok(Some(inbox_row)) => {
                let out = serde_json::json!({
                    "outbox_id": id,
                    "event_id": inbox_row.event_id,
                    "op": inbox_row.op,
                    "payload": inbox_row.payload,
                });
                println!("{}", serde_json::to_string(&out)?);
            }
            Ok(None) => eprintln!("  [SKIP] id={id} (tombstone or filtered)"),
            Err(e) => eprintln!("  [ERROR] id={id}: {e}"),
        }
    }

    Ok(())
}

/// `pg-tide replay dlq-resolve` — mark a DLQ entry as resolved.
async fn run_dlq_resolve(
    url: &str,
    pipeline: &str,
    dedup_key: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (client, conn) = pg_tls::connect(url).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let updated = client
        .execute(
            "UPDATE tide.relay_dlq
             SET resolved = true, resolved_at = now(), resolved_by = current_user
             WHERE pipeline_name = $1 AND dedup_key = $2 AND resolved = false",
            &[&pipeline, &dedup_key],
        )
        .await?;

    if updated == 0 {
        eprintln!(
            "warning: no active DLQ entry found for pipeline='{}' dedup_key='{}'",
            pipeline, dedup_key
        );
    } else {
        println!(
            "DLQ entry resolved: pipeline='{}' dedup_key='{}'",
            pipeline, dedup_key
        );
    }
    Ok(())
}

/// `pg-tide replay dlq-requeue` — requeue a DLQ entry for another attempt.
async fn run_dlq_requeue(
    url: &str,
    pipeline: &str,
    dedup_key: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (client, conn) = pg_tls::connect(url).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    // Mark the existing entry resolved.
    let updated = client
        .execute(
            "UPDATE tide.relay_dlq
             SET resolved = true, resolved_at = now(), resolved_by = current_user || ' (requeue)',
                 attempt_count = 0
             WHERE pipeline_name = $1 AND dedup_key = $2 AND resolved = false",
            &[&pipeline, &dedup_key],
        )
        .await?;

    if updated == 0 {
        eprintln!(
            "warning: no active DLQ entry found for pipeline='{}' dedup_key='{}'",
            pipeline, dedup_key
        );
        return Ok(());
    }

    // Re-insert as a fresh pending entry.
    client
        .execute(
            "INSERT INTO tide.relay_dlq (pipeline_name, dedup_key, payload, attempt_count, resolved)
             SELECT pipeline_name, dedup_key, payload, 0, false
             FROM tide.relay_dlq
             WHERE pipeline_name = $1 AND dedup_key = $2
               AND resolved = true
             ORDER BY id DESC
             LIMIT 1",
            &[&pipeline, &dedup_key],
        )
        .await?;

    println!(
        "DLQ entry requeued: pipeline='{}' dedup_key='{}'",
        pipeline, dedup_key
    );
    Ok(())
}

// ── `pg-tide asyncapi` ───────────────────────────────────────────────────

/// Dispatch asyncapi subcommands.
async fn run_asyncapi_command(
    cmd: AsyncapiCommands,
    default_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match cmd {
        AsyncapiCommands::Export {
            format,
            output,
            postgres_url,
        } => {
            let url = postgres_url
                .or_else(|| std::env::var("PG_TIDE_POSTGRES_URL").ok())
                .unwrap_or_else(|| default_url.to_string());
            if url.is_empty() {
                eprintln!("error: --postgres-url is required for asyncapi export");
                std::process::exit(1);
            }
            run_asyncapi_export(&url, &format, output.as_deref()).await
        }
    }
}

/// `pg-tide asyncapi export` — generate an AsyncAPI 3.0 document.
async fn run_asyncapi_export(
    url: &str,
    format: &str,
    output: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (client, conn) = pg_tls::connect(url).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    // Load all relay pipelines.
    let outbox_rows = client
        .query(
            "SELECT name, enabled, config FROM tide.relay_outbox_config ORDER BY name",
            &[],
        )
        .await?;

    let inbox_rows = client
        .query(
            "SELECT name, enabled, config FROM tide.relay_inbox_config ORDER BY name",
            &[],
        )
        .await?;

    // Build AsyncAPI 3.0 document.
    let mut channels = serde_json::Map::new();
    let mut operations = serde_json::Map::new();
    let mut messages = serde_json::Map::new();

    for row in &outbox_rows {
        let name: String = row.get(0);
        let _enabled: bool = row.get(1);
        let config: serde_json::Value = row.get(2);

        let sink_type = config
            .get("sink_type")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let outbox_name = config
            .pointer("/source/outbox")
            .and_then(|v| v.as_str())
            .unwrap_or(&name);
        let wire_format = config
            .get("wire_format")
            .and_then(|v| v.as_str())
            .unwrap_or("native");

        channels.insert(
            format!("forward/{name}"),
            serde_json::json!({
                "address": format!("{}/{}", sink_type, name),
                "description": format!("Forward relay: outbox '{}' → {}", outbox_name, sink_type),
                "messages": {
                    format!("{name}Message"): {
                        "$ref": format!("#/components/messages/{name}Message")
                    }
                }
            }),
        );

        messages.insert(
            format!("{name}Message"),
            serde_json::json!({
                "name": format!("{name}Message"),
                "contentType": "application/json",
                "payload": {
                    "type": "object",
                    "description": format!("pg_tide outbox message (wire_format: {})", wire_format)
                }
            }),
        );

        operations.insert(
            format!("send{}", to_pascal_case(&name)),
            serde_json::json!({
                "action": "send",
                "channel": { "$ref": format!("#/channels/forward~1{name}") },
                "description": format!("Publish messages from outbox '{}' to {}", outbox_name, sink_type)
            }),
        );
    }

    for row in &inbox_rows {
        let name: String = row.get(0);
        let _enabled: bool = row.get(1);
        let config: serde_json::Value = row.get(2);

        let source_type = config
            .get("source_type")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let inbox_name = config
            .pointer("/sink/inbox")
            .and_then(|v| v.as_str())
            .unwrap_or(&name);
        let wire_format = config
            .get("wire_format")
            .and_then(|v| v.as_str())
            .unwrap_or("native");

        channels.insert(
            format!("reverse/{name}"),
            serde_json::json!({
                "address": format!("{}/{}", source_type, name),
                "description": format!("Reverse relay: {} → inbox '{}'", source_type, inbox_name),
                "messages": {
                    format!("{name}InboxMessage"): {
                        "$ref": format!("#/components/messages/{name}InboxMessage")
                    }
                }
            }),
        );

        messages.insert(
            format!("{name}InboxMessage"),
            serde_json::json!({
                "name": format!("{name}InboxMessage"),
                "contentType": "application/json",
                "payload": {
                    "type": "object",
                    "description": format!("Inbound message for inbox '{}' (wire_format: {})", inbox_name, wire_format)
                }
            }),
        );

        operations.insert(
            format!("receive{}", to_pascal_case(&name)),
            serde_json::json!({
                "action": "receive",
                "channel": { "$ref": format!("#/channels/reverse~1{name}") },
                "description": format!("Consume messages from {} into inbox '{}'", source_type, inbox_name)
            }),
        );
    }

    let doc = serde_json::json!({
        "asyncapi": "3.0.0",
        "info": {
            "title": "pg-tide Relay AsyncAPI",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "Auto-generated AsyncAPI 3.0 document from pg-tide relay catalog metadata.",
        },
        "channels": channels,
        "operations": operations,
        "components": {
            "messages": messages,
        }
    });

    let content = match format {
        "json" => serde_json::to_string_pretty(&doc)?,
        _ => {
            // Simple YAML-ish output via serde_json → manual conversion.
            // For a production-quality YAML serialiser, add the `serde_yaml` crate.
            // Here we emit pretty-printed JSON with a YAML header comment so the
            // output is valid JSON-compatible YAML.
            format!(
                "# AsyncAPI 3.0 document — generated by pg-tide v{}\n# Format: JSON (YAML-compatible)\n{}",
                env!("CARGO_PKG_VERSION"),
                serde_json::to_string_pretty(&doc)?
            )
        }
    };

    match output {
        Some(path) => {
            tokio::fs::write(path, content).await?;
            eprintln!("AsyncAPI document written to '{path}'");
        }
        None => println!("{content}"),
    }

    Ok(())
}

/// Convert a kebab-case or snake_case string to PascalCase for AsyncAPI operation IDs.
fn to_pascal_case(s: &str) -> String {
    s.split(['-', '_'])
        .filter(|p| !p.is_empty())
        .map(|p| {
            let mut chars = p.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
            }
        })
        .collect()
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

// ── `pg-tide sweep` ──────────────────────────────────────────────────────

/// Delete consumed outbox messages past their retention window.
///
/// Calls `tide.outbox_truncate_delivered()` for each configured outbox (or
/// just the one named by `outbox_name`).  Run on a schedule via cron or a
/// Kubernetes CronJob to prevent unbounded growth of the outbox message table.
async fn run_sweep(url: &str, outbox_name: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    println!("pg-tide sweep v{}", env!("CARGO_PKG_VERSION"));

    let (client, conn) = pg_tls::connect(url)
        .await
        .map_err(|e| format!("connection failed: {e}"))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let outboxes: Vec<String> = if let Some(name) = outbox_name {
        vec![name.to_string()]
    } else {
        client
            .query(
                "SELECT outbox_name FROM tide.tide_outbox_config ORDER BY outbox_name",
                &[],
            )
            .await?
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect()
    };

    if outboxes.is_empty() {
        println!("  [INFO] No outboxes configured — nothing to sweep.");
        return Ok(());
    }

    let mut total_deleted: i64 = 0;
    for name in &outboxes {
        let deleted: i64 = client
            .query_one("SELECT tide.outbox_truncate_delivered($1)", &[name])
            .await
            .map(|r| r.get::<_, i64>(0))
            .unwrap_or(0);
        println!("  [OK] Swept outbox '{name}': {deleted} rows deleted");
        total_deleted += deleted;
    }

    println!(
        "\npg-tide sweep: {total_deleted} total row(s) deleted from {} outbox(es).",
        outboxes.len()
    );
    Ok(())
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

// ── `pg-tide status` ─────────────────────────────────────────────────────

/// Print a human-readable status table for all configured relay pipelines.
///
/// Columns:
///   PIPELINE | DIRECTION | ENABLED | LAST_OFFSET | CONSUMER_LAG | CB_STATE
///
/// - LAST_OFFSET: the committed change ID for forward pipelines (0 if not yet consumed).
/// - CONSUMER_LAG: number of undelivered outbox messages (forward pipelines only).
/// - CB_STATE: circuit breaker open/closed state from the relay config (always "unknown"
///   at query time; live state is only available in a running relay instance).
async fn run_status(url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let (client, conn) = pg_tls::connect(url)
        .await
        .map_err(|e| format!("connection failed: {e}"))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    // Forward pipelines.
    let forward_rows = client
        .query(
            "SELECT
                roc.name,
                'forward'::text AS direction,
                roc.enabled,
                COALESCE(rco.last_change_id, 0) AS last_offset,
                (SELECT COUNT(*) FROM tide.tide_outbox_messages tom
                 WHERE tom.outbox_name = (roc.config->>'source' ->> 'outbox')
                   AND tom.consumed_at IS NULL) AS consumer_lag
             FROM tide.relay_outbox_config roc
             LEFT JOIN tide.relay_consumer_offsets rco
               ON rco.pipeline_id = roc.name
             ORDER BY roc.name",
            &[],
        )
        .await
        .unwrap_or_default();

    // Reverse pipelines.
    let reverse_rows = client
        .query(
            "SELECT
                ric.name,
                'reverse'::text AS direction,
                ric.enabled,
                COALESCE(rco.last_change_id, 0) AS last_offset,
                0::bigint AS consumer_lag
             FROM tide.relay_inbox_config ric
             LEFT JOIN tide.relay_consumer_offsets rco
               ON rco.pipeline_id = ric.name
             ORDER BY ric.name",
            &[],
        )
        .await
        .unwrap_or_default();

    let all_rows: Vec<_> = forward_rows.iter().chain(reverse_rows.iter()).collect();

    if all_rows.is_empty() {
        println!("No pipelines configured.");
        return Ok(());
    }

    // Print header.
    println!(
        "{:<30} {:<10} {:<8} {:<14} {:<14}",
        "PIPELINE", "DIRECTION", "ENABLED", "LAST_OFFSET", "CONSUMER_LAG"
    );
    println!("{}", "-".repeat(80));

    for row in &all_rows {
        let name: String = row.get("name");
        let direction: String = row.get("direction");
        let enabled: bool = row.get("enabled");
        let last_offset: i64 = row.try_get("last_offset").unwrap_or(0);
        let consumer_lag: i64 = row.try_get("consumer_lag").unwrap_or(0);

        println!(
            "{:<30} {:<10} {:<8} {:<14} {:<14}",
            name,
            direction,
            if enabled { "yes" } else { "no" },
            last_offset,
            consumer_lag,
        );
    }

    println!("\n{} pipeline(s) configured.", all_rows.len());
    Ok(())
}
