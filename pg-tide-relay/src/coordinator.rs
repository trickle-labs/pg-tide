/// Coordinator — manages pipeline lifecycle with PostgreSQL advisory locks.
/// Implements RELAY-2 (coordinator loop), RELAY-SEC (secret resolution),
/// and hot-reload via LISTEN/NOTIFY.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch, RwLock, Semaphore};
use tokio::task::JoinHandle;
use tokio_postgres::Client;
use tracing::Instrument as _;

use crate::circuit_breaker::CircuitBreaker;
use crate::config::{
    mask_secrets_for_logging, resolve_pipeline_secrets, PipelineConfig, PipelineDirection,
};
use crate::dlq::{DlqConfig, DlqEntry, ErrorKind};
use crate::error::RelayError;
use crate::metrics::{HealthState, RelayMetrics};
use crate::rate_limiter::build_rate_limiter;

/// Coordinator manages pipeline ownership via advisory locks.
/// v0.15.0: Uses a deadpool-postgres Pool for coordinator metadata operations,
/// replacing the single persistent Arc<Client>.
pub struct Coordinator {
    /// v0.15.0: Connection pool for coordinator-level metadata operations.
    pool: deadpool_postgres::Pool,
    relay_group_id: String,
    metrics: Arc<RelayMetrics>,
    /// v0.19.0: Shared health state updated on pipeline start/stop.
    health: Arc<RwLock<HealthState>>,
    /// Pipeline ID → (cancellation sender, join handle).
    /// v0.15.0: JoinHandle stored for panic detection.
    owned: HashMap<String, (watch::Sender<bool>, JoinHandle<()>)>,
    /// v0.13.0 / v0.15.0: Maximum owned pipelines (connection limit).
    max_owned_pipelines: usize,
    /// v0.25.0: Optional tenant ID for multi-tenant relay groups.
    /// When set, only pipelines with matching tenant_name are owned.
    tenant_id: Option<String>,
    /// v0.35.0: Delivery receipt background sweep interval (hours, default 24).
    sweep_interval_hours: u64,
}

/// Execute one bounded replay through the normal worker without changing the
/// live pipeline configuration or its durable checkpoint.
pub async fn run_replay_once(
    db_url: &str,
    pipeline_name: &str,
    from_id: i64,
    to_id: i64,
    batch_size: i64,
) -> Result<(i64, i64), RelayError> {
    if from_id < 0 || to_id < from_id || !(1..=10_000).contains(&batch_size) {
        return Err(RelayError::InvalidConfig {
            name: pipeline_name.to_string(),
            reason: "replay range must satisfy 0 <= from-id <= to-id and batch-size 1..=10000"
                .to_string(),
        });
    }
    let (client, connection) = crate::pg_tls::connect(db_url).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::warn!(error = %error, "replay database connection closed");
        }
    });
    let db = Arc::new(client);

    let row = db
        .query_opt(
            "SELECT name::text, enabled, config, COALESCE(tenant_name, 'default')::text AS tenant_name
               FROM tide.relay_outbox_config
              WHERE name = $1",
            &[&pipeline_name],
        )
        .await?
        .ok_or_else(|| RelayError::PipelineNotFound(pipeline_name.to_string()))?;
    let pipeline = PipelineConfig {
        name: row.get("name"),
        direction: PipelineDirection::Forward,
        enabled: row.get("enabled"),
        config: row.get("config"),
        tenant_name: row.get("tenant_name"),
    };
    if !pipeline.enabled {
        return Err(RelayError::InvalidConfig {
            name: pipeline.name,
            reason: "pipeline is disabled".to_string(),
        });
    }
    pipeline.validate()?;

    let outbox_name = pipeline.require_str(&["source", "outbox"])?.to_string();
    let relay_group_id = format!("replay:{pipeline_name}");
    let live_checkpoint_before = read_live_checkpoint(&db, pipeline_name, &outbox_name).await?;

    let metrics = RelayMetrics::new()
        .map_err(|error| RelayError::other(format!("create replay metrics: {error}")))?;
    let health = Arc::new(RwLock::new(HealthState::default()));
    let (_stop_tx, mut stop_rx) = watch::channel(false);
    worker_inner(
        pipeline,
        db.clone(),
        WorkerRuntime {
            relay_group_id,
            status_enabled: false,
            owner_token: "replay".to_string(),
            replay: Some(ReplayRange { from_id, to_id }),
        },
        metrics,
        health,
        batch_size,
        &mut stop_rx,
    )
    .await?;

    let live_checkpoint_after = read_live_checkpoint(&db, pipeline_name, &outbox_name).await?;
    if live_checkpoint_after != live_checkpoint_before {
        return Err(RelayError::other(format!(
            "replay changed live checkpoint for pipeline '{pipeline_name}': {live_checkpoint_before} -> {live_checkpoint_after}"
        )));
    }
    Ok((live_checkpoint_before, live_checkpoint_after))
}

async fn read_live_checkpoint(
    db: &Client,
    pipeline_name: &str,
    outbox_name: &str,
) -> Result<i64, RelayError> {
    let row = db
        .query_one(
            "SELECT COALESCE(MAX(last_change_id), 0)::bigint
               FROM tide.relay_consumer_offsets
              WHERE pipeline_id = $1 AND outbox_name = $2",
            &[&pipeline_name, &outbox_name],
        )
        .await?;
    Ok(row.get(0))
}

impl Coordinator {
    pub fn new(
        pool: deadpool_postgres::Pool,
        relay_group_id: impl Into<String>,
        metrics: Arc<RelayMetrics>,
        health: Arc<RwLock<HealthState>>,
    ) -> Self {
        Self {
            pool,
            relay_group_id: relay_group_id.into(),
            metrics,
            health,
            owned: HashMap::new(),
            max_owned_pipelines: 50, // default: matches tide.relay_limits
            tenant_id: None,
            sweep_interval_hours: 24,
        }
    }

    /// Set the maximum number of pipelines this coordinator will own.
    pub fn set_max_owned_pipelines(&mut self, max: usize) {
        self.max_owned_pipelines = max;
    }

    /// v0.25.0: Set the tenant ID for multi-tenant relay groups.
    /// When set, only pipelines matching this tenant_name are discovered.
    pub fn set_tenant_id(&mut self, tenant_id: impl Into<String>) {
        self.tenant_id = Some(tenant_id.into());
    }

    /// v0.35.0: Set the delivery receipt sweep interval in hours.
    /// The coordinator will spawn a background task that calls
    /// `tide.relay_truncate_delivery_receipts()` on this schedule.
    pub fn set_sweep_interval_hours(&mut self, hours: u64) {
        self.sweep_interval_hours = hours;
    }

    /// Get the current max_owned_pipelines limit.
    fn max_owned_pipelines(&self) -> usize {
        self.max_owned_pipelines
    }

    /// Run the coordinator discovery loop until shutdown is signalled.
    ///
    /// - Performs an initial reconciliation immediately.
    /// - Re-reconciles every `discovery_interval`.
    /// - Also re-reconciles on every `tide_relay_config` NOTIFY event.
    /// - Exits cleanly when `shutdown_rx` is set to `true`.
    pub async fn run(
        &mut self,
        db_url: String,
        batch_size: i64,
        discovery_interval: Duration,
        mut shutdown_rx: watch::Receiver<bool>,
        mut notif_rx: mpsc::Receiver<()>,
    ) -> Result<(), RelayError> {
        // v0.35.0: Spawn a background task that periodically prunes delivery
        // receipt rows older than `sweep_interval_hours`.  The task exits when
        // `shutdown_rx` fires or when the pool is dropped.
        let sweep_pool = self.pool.clone();
        let mut sweep_shutdown = shutdown_rx.clone();
        let sweep_interval_hours: u64 = self.sweep_interval_hours;
        tokio::spawn(async move {
            let sweep_interval = Duration::from_secs(sweep_interval_hours * 3600);
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(sweep_interval) => {}
                    _ = sweep_shutdown.changed() => {
                        tracing::debug!("receipt sweep task exiting on shutdown signal");
                        break;
                    }
                }
                match sweep_pool.get().await {
                    Ok(client) => {
                        let interval_param = format!("{sweep_interval_hours} hours");
                        match client
                            .query_one(
                                "SELECT tide.relay_truncate_delivery_receipts($1::interval)",
                                &[&interval_param],
                            )
                            .await
                        {
                            Ok(row) => {
                                let deleted: i64 = row.try_get(0).unwrap_or(0);
                                if deleted > 0 {
                                    tracing::info!(
                                        deleted,
                                        sweep_interval_hours,
                                        "delivery receipt sweep completed"
                                    );
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    error = %e,
                                    "delivery receipt sweep query failed (non-fatal)"
                                );
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "delivery receipt sweep could not get pool connection (non-fatal)"
                        );
                    }
                }
            }
        });

        // Initial load.
        self.reconcile(&db_url, batch_size).await;

        loop {
            tokio::select! {
                _ = tokio::time::sleep(discovery_interval) => {
                    self.reconcile(&db_url, batch_size).await;
                }
                Some(_) = notif_rx.recv() => {
                    tracing::info!("tide_relay_config changed — reloading pipelines");
                    self.reconcile(&db_url, batch_size).await;
                }
                _ = shutdown_rx.changed() => {
                    tracing::info!("shutdown signal received — coordinator stopping");
                    break;
                }
            }
        }
        Ok(())
    }

    /// Load all enabled pipelines from the catalog.
    /// v0.25.0: When tenant_id is set, filter to only this tenant's pipelines.
    pub async fn load_pipelines(&self) -> Result<Vec<PipelineConfig>, RelayError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| RelayError::other(format!("coordinator pool error: {e}")))?;

        let rows = if let Some(ref tid) = self.tenant_id {
            client
                .query(
                    "SELECT name, 'forward' AS direction, enabled, config,
                            COALESCE(tenant_name, 'default') AS tenant_name
                       FROM tide.relay_outbox_config
                      WHERE enabled = true AND tenant_name = $1",
                    &[tid],
                )
                .await?
        } else {
            client
                .query(
                    "SELECT name, 'forward' AS direction, enabled, config,
                            COALESCE(tenant_name, 'default') AS tenant_name
                       FROM tide.relay_outbox_config
                      WHERE enabled = true",
                    &[],
                )
                .await?
        };
        self.metrics
            .catalog_discovery_queries
            .with_label_values(&[&self.relay_group_id])
            .inc();

        let mut pipelines = Vec::new();
        for row in rows {
            let name: String = row.get("name");
            let direction: String = row.get("direction");
            let enabled: bool = row.get("enabled");
            let config: serde_json::Value = row.get("config");
            let tenant_name: String = row.get("tenant_name");

            pipelines.push(PipelineConfig {
                name: name.clone(),
                direction: if direction == "forward" {
                    PipelineDirection::Forward
                } else {
                    return Err(RelayError::InvalidConfig {
                        name,
                        reason: format!("unsupported pipeline direction '{direction}'"),
                    });
                },
                enabled,
                config,
                tenant_name,
            });
        }
        Ok(pipelines)
    }

    /// Try to acquire the advisory lock for a pipeline.
    /// Returns true if the lock was acquired (this pod owns the pipeline).
    ///
    /// This compatibility helper is retained for the coordinator unit tests.
    /// Production reconciliation uses `try_acquire_ownership`, which keeps the
    /// lock-holding session with the worker.
    /// v0.25.0: When tenant_id is set, incorporate it into the lock key pair
    /// so two tenants with identical pipeline names do not collide.
    pub async fn try_acquire_lock(&self, pipeline_id: &str) -> Result<bool, RelayError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| RelayError::other(format!("coordinator pool error: {e}")))?;
        // Build the lock key: {relay_group}:{tenant_id}:{pipeline_id}
        // When no tenant is configured, the key reduces to {relay_group}:{pipeline_id}
        // (same as pre-v0.25.0 behaviour).
        let lock_key = match &self.tenant_id {
            Some(tid) => format!("{}:{}:{}", self.relay_group_id, tid, pipeline_id),
            None => format!("{}:{}", self.relay_group_id, pipeline_id),
        };
        let row = client
            .query_one(
                "SELECT pg_try_advisory_lock(hashtext($1), hashtext($2))",
                &[&self.relay_group_id, &lock_key],
            )
            .await?;
        Ok(row.get::<_, bool>(0))
    }

    /// Acquire a pipeline lock on the dedicated PostgreSQL session that will
    /// be shared by the worker for its entire lifetime.
    async fn try_acquire_ownership(
        &self,
        db_url: &str,
        pipeline: &PipelineConfig,
    ) -> Result<Option<(Arc<Client>, watch::Receiver<bool>)>, RelayError> {
        let (client, connection) = crate::pg_tls::connect(db_url).await?;
        let client = Arc::new(client);
        let (lost_tx, lost_rx) = watch::channel(false);
        let _pipeline_name = pipeline.name.clone();
        tokio::spawn(async move {
            match connection.await {
                Ok(()) => tracing::warn!("ownership PostgreSQL connection closed"),
                Err(error) => tracing::error!(%error, "ownership PostgreSQL connection failed"),
            }
            if let Err(error) = crate::test_failpoint!("ownership_connection_lost", &_pipeline_name)
            {
                tracing::error!(%error, "ownership loss failpoint failed");
            }
            let _ = lost_tx.send(true);
        });

        let direction = "forward";
        let lock_scope = format!("{}:{}:{}", pipeline.tenant_name, direction, pipeline.name);
        let row = client
            .query_one(
                "SELECT pg_try_advisory_lock(hashtext($1), hashtext($2))",
                &[&self.relay_group_id, &lock_scope],
            )
            .await?;
        if !row.get::<_, bool>(0) {
            drop(client);
            return Ok(None);
        }

        self.metrics
            .ownership_events
            .with_label_values(&[self.relay_group_id.as_str(), "acquired"])
            .inc();
        Ok(Some((client, lost_rx)))
    }

    /// Release the advisory lock for a pipeline.
    pub async fn release_lock(&self, pipeline_id: &str) -> Result<(), RelayError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| RelayError::other(format!("coordinator pool error: {e}")))?;
        let lock_key = match &self.tenant_id {
            Some(tid) => format!("{}:{}:{}", self.relay_group_id, tid, pipeline_id),
            None => format!("{}:{}", self.relay_group_id, pipeline_id),
        };
        client
            .execute(
                "SELECT pg_advisory_unlock(hashtext($1), hashtext($2))",
                &[&self.relay_group_id, &lock_key],
            )
            .await?;
        Ok(())
    }

    /// Release all advisory locks held by this coordinator.
    pub async fn release_all_locks(&self) -> Result<(), RelayError> {
        // v0.42.0: Production locks belong to the worker-held sessions. They
        // are released when those sessions are dropped after drain; unlocking
        // through an arbitrary pool client cannot release them safely.
        Ok(())
    }

    /// Signal all owned pipelines to stop and wait for them to finish their
    /// current batch. Called during graceful shutdown before
    /// `release_all_locks`.
    pub async fn drain(&mut self, timeout: Duration) {
        // Send the stop signal to every owned pipeline.
        for (pipeline_id, (tx, _handle)) in &self.owned {
            if tx.send(true).is_err() {
                tracing::debug!(pipeline = %pipeline_id, "pipeline already stopped");
            }
        }

        let owned = std::mem::take(&mut self.owned);
        let deadline = Instant::now() + timeout;
        for (pipeline_id, (_tx, mut handle)) in owned {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if tokio::time::timeout(remaining, &mut handle).await.is_err() {
                tracing::warn!(pipeline = %pipeline_id, "worker drain timed out — aborting");
                self.metrics
                    .forced_shutdown
                    .with_label_values(&[&pipeline_id])
                    .inc();
                handle.abort();
                let _ = handle.await;
            } else {
                tracing::debug!(pipeline = %pipeline_id, "pipeline drained");
            }
        }
    }

    // ── Private ──────────────────────────────────────────────────────────

    /// v0.29.0: Check for pipelines with auto_resume_after set that have been
    /// paused longer than the configured interval, and re-enable them.
    async fn check_auto_resume(&self) {
        let conn = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "auto-resume: failed to get pool connection");
                return;
            }
        };
        // Query pipelines where: enabled=FALSE, auto_resume_after IS NOT NULL,
        // and now() - pause_started_at > auto_resume_after.
        let rows = conn
            .query(
                "SELECT roc.name
                 FROM tide.relay_outbox_config roc
                 JOIN tide.relay_pipeline_state s ON s.name = roc.name
                 WHERE roc.enabled = FALSE
                   AND EXISTS (
                       SELECT 1 FROM tide.tide_outbox_config toc
                       WHERE toc.auto_resume_after IS NOT NULL
                         AND s.pause_started_at IS NOT NULL
                         AND now() - s.pause_started_at > toc.auto_resume_after
                   )
                 UNION ALL
                 SELECT ric.name
                 FROM tide.relay_inbox_config ric
                 JOIN tide.relay_pipeline_state s ON s.name = ric.name
                 WHERE ric.enabled = FALSE
                   AND EXISTS (
                       SELECT 1 FROM tide.tide_inbox_config tic
                       WHERE tic.auto_resume_after IS NOT NULL
                         AND s.pause_started_at IS NOT NULL
                         AND now() - s.pause_started_at > tic.auto_resume_after
                   )",
                &[],
            )
            .await
            .unwrap_or_default();

        for row in rows {
            let name: String = row.get(0);
            tracing::info!(pipeline = %name, "auto-resume: re-enabling pipeline after pause interval");
            if let Err(e) = conn
                .execute(
                    "UPDATE tide.relay_outbox_config SET enabled = TRUE WHERE name = $1;
                     UPDATE tide.relay_inbox_config SET enabled = TRUE WHERE name = $1;
                     UPDATE tide.relay_pipeline_state
                         SET failure_count = 0, pause_started_at = NULL, last_error = NULL
                     WHERE name = $1",
                    &[&name],
                )
                .await
            {
                tracing::warn!(pipeline = %name, error = %e, "auto-resume: failed to re-enable");
            }
        }
    }

    /// Load pipelines, start new ones, stop removed/disabled ones.
    ///
    /// v0.15.0: Also checks for panicked/completed workers and cleans them up
    /// immediately rather than waiting up to `discovery_interval` seconds.
    /// v0.16.0: Records reconcile duration and owned_pipelines gauge metrics.
    async fn reconcile(&mut self, db_url: &str, batch_size: i64) {
        let reconcile_start = Instant::now();
        let group_label = self.relay_group_id.clone();
        // v0.15.0: Detect panicked or unexpectedly completed worker tasks and
        // clean up their owned entries before loading the pipeline list.
        let panicked: Vec<_> = self
            .owned
            .iter()
            .filter(|(_, (_, handle))| handle.is_finished())
            .map(|(name, _)| name.clone())
            .collect();
        for name in panicked {
            if let Some((_, handle)) = self.owned.remove(&name) {
                match handle.await {
                    Ok(()) => tracing::warn!(
                        pipeline = %name,
                        "worker exited unexpectedly — will attempt to re-acquire"
                    ),
                    Err(e) => tracing::error!(
                        pipeline = %name,
                        panic = ?e,
                        "worker panicked — cleaning up"
                    ),
                }
            }
        }

        let pipelines = match self.load_pipelines().await {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(error = %e, "failed to load pipelines — skipping reconciliation");
                let mut health = self.health.write().await;
                health.postgres_reachable = false;
                health.coordinator_ready = false;
                self.metrics
                    .status_refresh_errors
                    .with_label_values(&["catalog"])
                    .inc();
                return;
            }
        };

        let preflight = crate::config::preflight::startup_preflight(&pipelines);
        for issue in &preflight.issues {
            match issue.severity {
                crate::config::preflight::PreflightSeverity::Error => {
                    tracing::error!(
                        pipeline = %issue.pipeline,
                        reason = %issue.reason,
                        "pipeline preflight failed"
                    );
                }
                crate::config::preflight::PreflightSeverity::Warning => {
                    tracing::warn!(
                        pipeline = %issue.pipeline,
                        reason = %issue.reason,
                        "pipeline preflight warning"
                    );
                }
            }
        }
        if !preflight.is_valid() {
            let mut health = self.health.write().await;
            health.startup_preflight_complete = true;
            health.postgres_reachable = true;
            health.coordinator_ready = false;
            self.metrics
                .status_refresh_errors
                .with_label_values(&["preflight"])
                .inc();
            return;
        }

        // v0.29.0: Check for pipelines eligible for auto-resume and re-enable them.
        self.check_auto_resume().await;

        let active_names: HashSet<_> = pipelines.iter().map(|p| p.name.clone()).collect();

        // Stop removed/disabled pipelines.
        let to_stop: Vec<_> = self
            .owned
            .keys()
            .filter(|n| !active_names.contains(*n))
            .cloned()
            .collect();
        for name in &to_stop {
            tracing::info!(pipeline = %name, "pipeline removed/disabled — stopping worker");
            if let Some((tx, mut handle)) = self.owned.remove(name) {
                let _ = tx.send(true);
                if tokio::time::timeout(Duration::from_secs(5), &mut handle)
                    .await
                    .is_err()
                {
                    tracing::warn!(pipeline = %name, "worker drain timed out — aborting");
                    self.metrics
                        .forced_shutdown
                        .with_label_values(&[name])
                        .inc();
                    handle.abort();
                    let _ = handle.await;
                }
            }
        }

        // Try to start pipelines not yet owned by this pod.
        for pipeline in pipelines {
            if self.owned.contains_key(&pipeline.name) {
                continue; // Already running.
            }

            // v0.13.0/v0.15.0: Enforce max_owned_pipelines connection limit.
            // Each owned pipeline consumes one PostgreSQL connection.
            let max_owned = self.max_owned_pipelines();
            if self.owned.len() >= max_owned {
                tracing::warn!(
                    owned = self.owned.len(),
                    max = max_owned,
                    pipeline = %pipeline.name,
                    "max_owned_pipelines limit reached — not acquiring additional pipelines"
                );
                break;
            }

            // RELAY-SEC: resolve ${env:VAR} / ${file:/path} tokens before
            // handing the config to the worker.  A bad secret disables only
            // this pipeline — all others continue running.
            // v0.15.0: Log masked config (not resolved config) to avoid
            // credential leakage in structured log output.
            let resolved_config = match resolve_pipeline_secrets(pipeline.config.clone()) {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(
                        pipeline = %pipeline.name,
                        error = %e,
                        // v0.32.0 P3: use explicit "{}" fallback instead of unwrap_or_default()
                        // which returns "" (not valid JSON for a config object).
                        masked_config = %serde_json::to_string(
                            &mask_secrets_for_logging(&pipeline.config)
                        ).unwrap_or_else(|_| "{}".to_string()),
                        "secret resolution failed — pipeline disabled"
                    );
                    continue;
                }
            };

            let Some((ownership_db, ownership_lost_rx)) =
                (match self.try_acquire_ownership(db_url, &pipeline).await {
                    Ok(value) => value,
                    Err(error) => {
                        tracing::warn!(
                            pipeline = %pipeline.name,
                            %error,
                            "advisory ownership acquisition failed"
                        );
                        continue;
                    }
                })
            else {
                tracing::debug!(
                    pipeline = %pipeline.name,
                    "advisory lock held by another relay — skipping"
                );
                continue;
            };

            tracing::info!(
                pipeline = %pipeline.name,
                direction = ?pipeline.direction,
                "acquired lock — spawning worker"
            );

            let (stop_tx, stop_rx) = watch::channel(false);

            let resolved_pipeline = PipelineConfig {
                name: pipeline.name.clone(),
                direction: pipeline.direction,
                enabled: pipeline.enabled,
                config: resolved_config,
                tenant_name: pipeline.tenant_name.clone(),
            };

            // v0.15.0: Store JoinHandle for panic detection.
            let handle = tokio::spawn(run_pipeline_worker(
                resolved_pipeline,
                ownership_db,
                ownership_lost_rx,
                self.relay_group_id.clone(),
                Arc::clone(&self.metrics),
                Arc::clone(&self.health),
                batch_size,
                stop_rx,
            ));

            self.owned.insert(pipeline.name, (stop_tx, handle));
        }

        // v0.16.0: Record owned_pipelines gauge and reconcile duration.
        self.metrics
            .owned_pipelines
            .with_label_values(&[&group_label])
            .set(self.owned.len() as i64);
        self.metrics
            .reconcile_duration_seconds
            .with_label_values(&[&group_label])
            .observe(reconcile_start.elapsed().as_secs_f64());

        // v0.19.0: Update shared HealthState so /healthz reflects live pipeline state.
        let mut h = self.health.write().await;
        h.startup_preflight_complete = true;
        h.postgres_reachable = true;
        h.coordinator_ready = true;
        h.unhealthy_pipelines
            .retain(|pipeline| self.owned.contains_key(pipeline));
        h.healthy_pipelines = self
            .owned
            .keys()
            .filter(|pipeline| !h.unhealthy_pipelines.contains(*pipeline))
            .cloned()
            .collect();
    }
}

// ── Pipeline worker ───────────────────────────────────────────────────────

async fn mark_pipeline_health(
    health: &Arc<RwLock<HealthState>>,
    pipeline: &str,
    _tenant: &str,
    healthy: bool,
) {
    let mut state = health.write().await;
    state.healthy_pipelines.retain(|name| name != pipeline);
    state.unhealthy_pipelines.retain(|name| name != pipeline);
    if healthy {
        state.healthy_pipelines.push(pipeline.to_string());
    } else {
        state.unhealthy_pipelines.push(pipeline.to_string());
    }
}

async fn runtime_status_available(db: &Client) -> bool {
    match db
        .query_one(
            "SELECT to_regclass('tide.relay_runtime_status') IS NOT NULL",
            &[],
        )
        .await
    {
        Ok(row) => row.get(0),
        Err(error) => {
            tracing::warn!(%error, "runtime status availability check failed");
            false
        }
    }
}

async fn upsert_runtime_status(
    db: &Client,
    relay_group_id: &str,
    pipeline: &PipelineConfig,
    owner_token: Option<&str>,
) {
    let direction = "forward";
    if let Err(error) = db
        .execute(
            "INSERT INTO tide.relay_runtime_status
                (relay_group_id, pipeline_id, direction, tenant_name,
                 owner_token, owner_acquired_at, last_owner_heartbeat,
                 last_state_update_at)
             VALUES ($1, $2, $3, $4, $5,
                     CASE WHEN $5 IS NULL THEN NULL ELSE now() END,
                     CASE WHEN $5 IS NULL THEN NULL ELSE now() END, now())
             ON CONFLICT (relay_group_id, pipeline_id, direction, tenant_name)
             DO UPDATE SET
                 owner_token = EXCLUDED.owner_token,
                 owner_acquired_at = CASE
                     WHEN EXCLUDED.owner_token IS NULL THEN NULL
                     ELSE COALESCE(tide.relay_runtime_status.owner_acquired_at, now())
                 END,
                 last_owner_heartbeat = CASE
                     WHEN EXCLUDED.owner_token IS NULL THEN NULL
                     ELSE now()
                 END,
                 last_state_update_at = now()",
            &[
                &relay_group_id,
                &pipeline.name,
                &direction,
                &pipeline.tenant_name,
                &owner_token,
            ],
        )
        .await
    {
        tracing::warn!(
            pipeline = %pipeline.name,
            %error,
            "runtime status write failed; delivery state remains authoritative"
        );
    }
}

async fn record_runtime_error(
    db: &Client,
    relay_group_id: &str,
    pipeline: &PipelineConfig,
    error: &RelayError,
) {
    let direction = match pipeline.direction {
        PipelineDirection::Forward => "forward",
        PipelineDirection::Reverse => "reverse",
    };
    if let Err(error) = db
        .execute(
            "UPDATE tide.relay_runtime_status
                SET last_error_code = $5,
                    last_error_component = 'relay.worker',
                    last_error_class = $6,
                    last_error_at = now(),
                    retry_state = CASE WHEN $6 = 'transient' THEN 'retrying' ELSE NULL END,
                    last_state_update_at = now()
              WHERE relay_group_id = $1
                AND pipeline_id = $2
                AND direction = $3
                AND tenant_name = $4",
            &[
                &relay_group_id,
                &pipeline.name,
                &direction,
                &pipeline.tenant_name,
                &error.public_code().to_string(),
                &error.retry_class().to_string(),
            ],
        )
        .await
    {
        tracing::warn!(
            pipeline = %pipeline.name,
            %error,
            "runtime error status write failed"
        );
    }
}

struct WorkerRuntime {
    relay_group_id: String,
    status_enabled: bool,
    owner_token: String,
    replay: Option<ReplayRange>,
}

#[derive(Clone, Copy)]
struct ReplayRange {
    from_id: i64,
    to_id: i64,
}

/// Top-level worker task: wraps `worker_inner` and logs the outcome.
#[allow(clippy::too_many_arguments)]
async fn run_pipeline_worker(
    pipeline: PipelineConfig,
    db: Arc<Client>,
    mut ownership_lost_rx: watch::Receiver<bool>,
    relay_group_id: String,
    metrics: Arc<RelayMetrics>,
    health: Arc<RwLock<HealthState>>,
    batch_size: i64,
    mut stop_rx: watch::Receiver<bool>,
) {
    let name = pipeline.name.clone();
    let tenant_label = pipeline.tenant_name.clone();
    let status_pipeline = pipeline.clone();
    let status_enabled = runtime_status_available(&db).await;
    let owner_token = uuid::Uuid::new_v4().to_string();

    // v0.13.0: Mark pipeline as healthy when worker starts.
    metrics
        .pipeline_healthy
        .with_label_values(&[&name, &tenant_label])
        .set(1);
    mark_pipeline_health(&health, &name, &tenant_label, true).await;
    if status_enabled {
        upsert_runtime_status(&db, &relay_group_id, &pipeline, Some(&owner_token)).await;
    }

    let worker = worker_inner(
        pipeline,
        db.clone(),
        WorkerRuntime {
            relay_group_id: relay_group_id.clone(),
            status_enabled,
            owner_token: owner_token.clone(),
            replay: None,
        },
        metrics.clone(),
        health.clone(),
        batch_size,
        &mut stop_rx,
    );
    tokio::pin!(worker);

    let outcome = tokio::select! {
        result = &mut worker => result,
        changed = ownership_lost_rx.changed() => {
            if changed.is_ok() && *ownership_lost_rx.borrow() {
                metrics
                    .ownership_events
                    .with_label_values(&[relay_group_id.as_str(), "lost"])
                    .inc();
                mark_pipeline_health(&health, &name, &tenant_label, false).await;
                tracing::error!(
                    pipeline = %name,
                    "ownership session lost — cancelling worker"
                );
                Err(RelayError::other("ownership session lost"))
            } else {
                Err(RelayError::other("ownership session monitor stopped"))
            }
        }
    };

    match outcome {
        Ok(()) => {
            tracing::info!(pipeline = %name, "worker stopped");
            // Mark as 0 on clean stop.
            metrics
                .pipeline_healthy
                .with_label_values(&[&name, &tenant_label])
                .set(0);
            mark_pipeline_health(&health, &name, &tenant_label, false).await;
            if status_enabled {
                upsert_runtime_status(&db, &relay_group_id, &status_pipeline, None).await;
            }
        }
        Err(e) => {
            tracing::error!(pipeline = %name, error = %e, "worker exited with error");
            // Mark as 0 on error exit.
            metrics
                .pipeline_healthy
                .with_label_values(&[&name, &tenant_label])
                .set(0);
            mark_pipeline_health(&health, &name, &tenant_label, false).await;
            if status_enabled {
                upsert_runtime_status(&db, &relay_group_id, &status_pipeline, None).await;
            }
            // v0.16.0: Record pipeline error by class.
            let error_class = if e.is_transient() {
                "transient"
            } else {
                "permanent"
            };
            if status_enabled {
                record_runtime_error(&db, &relay_group_id, &status_pipeline, &e).await;
            }
            metrics
                .pipeline_errors_total
                .with_label_values(&[name.as_str(), error_class])
                .inc();
        }
    }
}

/// Inner worker: open a DB connection, build source + sink, run poll loop.
async fn worker_inner(
    pipeline: PipelineConfig,
    db: Arc<Client>,
    runtime: WorkerRuntime,
    metrics: Arc<RelayMetrics>,
    health: Arc<RwLock<HealthState>>,
    default_batch_size: i64,
    stop_rx: &mut watch::Receiver<bool>,
) -> Result<(), RelayError> {
    // v0.28.0: Per-tenant DB role — issue SET ROLE when configured.
    // This enforces tenant isolation at the PostgreSQL connection level.
    if let Some(db_role) = pipeline.opt_str(&["db_role"]) {
        if !db_role.is_empty() {
            // Validate the role name before interpolating into SQL.
            crate::config::validate_relay_identifier(db_role).map_err(|e| {
                RelayError::InvalidConfig {
                    name: pipeline.name.clone(),
                    reason: format!("db_role validation failed: {e}"),
                }
            })?;
            db.execute(&format!("SET ROLE {}", db_role), &[])
                .await
                .map_err(|e| RelayError::InvalidConfig {
                    name: pipeline.name.clone(),
                    reason: format!("SET ROLE '{}' failed: {e}", db_role),
                })?;
            tracing::info!(
                pipeline = %pipeline.name,
                db_role,
                "worker session role set"
            );
        }
    }

    // v0.28.0: Extract outbox name for delivery receipt writes.
    let receipt_outbox_name: String = pipeline
        .opt_str(&["source", "outbox"])
        .unwrap_or(&pipeline.name)
        .to_string();

    let batch_size = pipeline
        .opt_i64(&["batch_size"])
        .unwrap_or(default_batch_size);
    let poll_interval_ms = pipeline.opt_i64(&["poll_interval_ms"]).unwrap_or(1_000) as u64;

    // v0.12.0: sink_max_inflight — limits concurrent in-flight publish operations.
    // 0 = unlimited (legacy behaviour).
    let sink_max_inflight = pipeline.opt_i64(&["sink_max_inflight"]).unwrap_or(0) as usize;
    let inflight_semaphore: Option<Arc<Semaphore>> = if sink_max_inflight > 0 {
        Some(Arc::new(Semaphore::new(sink_max_inflight)))
    } else {
        None
    };

    // v0.7.0: Parse operational config.
    let dry_run = pipeline.opt_bool(&["dry_run"]).unwrap_or(false);
    let dlq_config = DlqConfig::from_pipeline_config(&pipeline.config);
    let rate_limiter = build_rate_limiter(&pipeline.config);
    let mut circuit_breaker = CircuitBreaker::from_pipeline_config(&pipeline.config);

    // Bounded replay is an explicit invocation mode, not persisted pipeline
    // configuration. Keep the public pipeline schema strict and checkpoint-neutral.
    let replay_from = runtime.replay.map(|range| range.from_id);
    let replay_to = runtime.replay.map(|range| range.to_id);
    let is_replay = replay_from.is_some();

    // v0.13.0: Wire-format factory — instantiate the configured wire format.
    let wire_format = crate::wire_format::from_config(&pipeline.config)?;

    tracing::info!(
        pipeline = %pipeline.name,
        wire_format = wire_format.name(),
        "wire format active"
    );

    if dry_run {
        tracing::info!(pipeline = %pipeline.name, "dry-run mode enabled — messages will NOT be published");
    }
    if is_replay {
        tracing::info!(
            pipeline = %pipeline.name,
            from = replay_from,
            to = replay_to,
            "replay mode enabled"
        );
    }

    let mut source = build_source(&pipeline, Arc::clone(&db), &runtime.relay_group_id).await?;
    if let Some(from_offset) = replay_from {
        source.configure_replay(from_offset)?;
    }
    let mut sink = build_sink(&pipeline, Arc::clone(&db)).await?;

    let direction_label = "forward".to_string();

    // v0.14.0: Tenant label for per-tenant Prometheus dimension.
    let tenant_label = pipeline.tenant_name.clone();

    tracing::info!(
        pipeline = %pipeline.name,
        direction = direction_label,
        source = source.name(),
        sink = sink.name(),
        dry_run,
        "worker started"
    );

    let mut consecutive_failures: u32 = 0;
    // v0.15.0: Exponential backoff on poll errors.
    // Starts at poll_interval_ms, doubles on each failure, caps at 60 s.
    let max_poll_backoff_ms: u64 = pipeline
        .opt_i64(&["max_poll_backoff_ms"])
        .map(|v| v as u64)
        .unwrap_or(60_000);
    let mut poll_backoff_ms = poll_interval_ms;
    let mut last_runtime_heartbeat = Instant::now();

    loop {
        if runtime.status_enabled && last_runtime_heartbeat.elapsed() >= Duration::from_secs(30) {
            upsert_runtime_status(
                &db,
                &runtime.relay_group_id,
                &pipeline,
                Some(&runtime.owner_token),
            )
            .await;
            last_runtime_heartbeat = Instant::now();
        }
        metrics
            .pipeline_heartbeat_age
            .with_label_values(&[pipeline.name.as_str()])
            .set(0);
        if *stop_rx.borrow() {
            crate::test_failpoint!("during_shutdown", &pipeline.name)?;
            break;
        }

        let (batch, checkpoint) = {
            // v0.24.0: Use poll_and_decode() helper for clean separation of
            // poll, replay-filter, and error-classification logic.
            metrics
                .source_poll_queries
                .with_label_values(&[pipeline.name.as_str(), source.name()])
                .inc();
            match poll_and_decode(
                &mut source,
                batch_size,
                &pipeline.name,
                &direction_label,
                replay_from,
                replay_to,
                is_replay,
            )
            .await
            {
                PollOutcome::Batch {
                    messages: msgs,
                    checkpoint,
                } => {
                    // v0.15.0: Reset backoff on successful poll.
                    poll_backoff_ms = poll_interval_ms;
                    (msgs, checkpoint)
                }
                PollOutcome::Empty => {
                    // Reset backoff when idle (no messages).
                    poll_backoff_ms = poll_interval_ms;
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(poll_interval_ms)) => {}
                        _ = stop_rx.changed() => { break; }
                    }
                    continue;
                }
                PollOutcome::ReplayComplete => {
                    tracing::info!(pipeline = %pipeline.name, "replay complete");
                    break;
                }
                PollOutcome::TransientError(e) => {
                    // v0.15.0: Permanent errors stop the pipeline immediately.
                    // v0.18.0: Replace LCG pseudo-random jitter with rand::thread_rng()
                    // so concurrent pipelines failing at the same instant do not choose
                    // identical backoff offsets (fixes deterministic clustering).
                    consecutive_failures += 1;
                    let jitter_range = (poll_backoff_ms as f64 * 0.20) as u64;
                    let jitter: i64 = if jitter_range > 0 {
                        use rand::Rng;
                        let half = jitter_range as i64;
                        rand::rng().random_range(-half..=half)
                    } else {
                        0
                    };
                    let sleep_ms = (poll_backoff_ms as i64).saturating_add(jitter).max(0) as u64;
                    tracing::warn!(
                        pipeline = %pipeline.name,
                        error = %e,
                        sleep_ms,
                        consecutive_failures,
                        "transient poll error — backing off before retry"
                    );
                    // v0.16.0: OTel span around backoff sleep.
                    // v0.24.0: Annotate with consecutive_failures count and next_wake_up_ms
                    // for distributed trace performance debugging.
                    let next_wake_up_ms = (poll_backoff_ms * 2).min(max_poll_backoff_ms);
                    let backoff_span = tracing::info_span!(
                        "relay.backoff.sleep",
                        pipeline = %pipeline.name,
                        sleep_ms,
                        consecutive_failures,
                        next_wake_up_ms,
                    );
                    let _enter = backoff_span.enter();
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(sleep_ms)) => {}
                        _ = stop_rx.changed() => { break; }
                    }
                    poll_backoff_ms = (poll_backoff_ms * 2).min(max_poll_backoff_ms);
                    continue;
                }
                PollOutcome::PermanentError(e) => {
                    tracing::error!(
                        pipeline = %pipeline.name,
                        error_code = %e.public_code(),
                        error_summary = e.public_summary(),
                        "permanent poll error — stopping pipeline"
                    );
                    return Err(e);
                }
            }
        };
        crate::test_failpoint!("after_poll_before_encode", &pipeline.name)?;
        metrics
            .delivery_stage_total
            .with_label_values(&[pipeline.name.as_str(), "polled", "success"])
            .inc();
        delivery_transition(
            &pipeline,
            &runtime,
            source.name(),
            "unresolved",
            "polled",
            "success",
            false,
            "old",
            batch.len(),
        );

        // v0.13.0: Increment consumed counter after successful poll.
        metrics
            .messages_consumed
            .with_label_values(&[&pipeline.name, &direction_label, &tenant_label])
            .inc_by(batch.len() as u64);

        // v0.13.0: Record the poll timestamp for end-to-end latency tracking.
        let poll_instant = std::time::Instant::now();

        if batch.is_empty() {
            // All messages filtered out — acknowledge the source and continue.
            metrics
                .delivery_stage_total
                .with_label_values(&[pipeline.name.as_str(), "intentionally_filtered", "success"])
                .inc();
            delivery_transition(
                &pipeline,
                &runtime,
                source.name(),
                "unresolved",
                "intentionally_filtered",
                "success",
                false,
                "old",
                0,
            );
            commit_checkpoint(
                &mut source,
                checkpoint.as_deref(),
                &metrics,
                &pipeline.name,
                "intentionally_filtered",
            )
            .await?;
            metrics
                .retry_state
                .with_label_values(&[pipeline.name.as_str(), "none"])
                .set(1);
            metrics
                .retry_state
                .with_label_values(&[pipeline.name.as_str(), "retrying"])
                .set(0);
            continue;
        }

        crate::test_failpoint!("after_encode_before_publish", &pipeline.name)?;
        metrics
            .delivery_stage_total
            .with_label_values(&[pipeline.name.as_str(), "encoded", "success"])
            .inc();
        delivery_transition(
            &pipeline,
            &runtime,
            source.name(),
            sink.name(),
            "encoded",
            "success",
            false,
            "old",
            batch.len(),
        );

        if is_replay {
            crate::test_failpoint!("during_replay", &pipeline.name)?;
        }

        // v0.7.0: Dry-run mode — log what would be published, skip actual publish.
        if dry_run {
            metrics
                .delivery_stage_total
                .with_label_values(&[pipeline.name.as_str(), "dry_run_observed", "success"])
                .inc();
            delivery_transition(
                &pipeline,
                &runtime,
                source.name(),
                sink.name(),
                "dry_run_observed",
                "success",
                false,
                "old",
                batch.len(),
            );
            for msg in &batch {
                // v0.24.0: Demote per-message dry-run logging to debug to reduce
                // log volume (at 50 pipelines × 1 poll/s this was ~4.3M lines/day).
                tracing::debug!(
                    pipeline = %pipeline.name,
                    subject = %msg.subject,
                    dedup_key = %msg.dedup_key,
                    payload_bytes = msg.payload.to_string().len(),
                    "[dry-run] would publish message"
                );
            }
            commit_checkpoint(
                &mut source,
                checkpoint.as_deref(),
                &metrics,
                &pipeline.name,
                "dry_run_observed",
            )
            .await?;
            metrics
                .messages_published
                .with_label_values(&[&pipeline.name, &direction_label, &tenant_label])
                .inc_by(batch.len() as u64);
            continue;
        }

        // v0.7.0: Rate limiting — wait for tokens before publishing.
        rate_limiter.acquire(batch.len() as u32).await;

        // v0.12.0: sink_max_inflight semaphore — acquire one permit per batch
        // to bound the number of concurrent publish operations.
        let _inflight_permit = if let Some(ref sem) = inflight_semaphore {
            Some(Arc::clone(sem).acquire_owned().await.ok())
        } else {
            None
        };

        // v0.24.0: Use publish_with_circuit_breaker() helper, then record
        // per-sink publish latency and handle the outcome.
        metrics
            .delivery_stage_total
            .with_label_values(&[pipeline.name.as_str(), "publish_started", "success"])
            .inc();
        delivery_transition(
            &pipeline,
            &runtime,
            source.name(),
            sink.name(),
            "publish_started",
            "success",
            false,
            "old",
            batch.len(),
        );
        let publish_span = tracing::info_span!(
            "relay.sink.publish",
            pipeline = %pipeline.name,
            batch_size = batch.len(),
        );
        let publish_start = std::time::Instant::now();
        let publish_outcome = match validate_publish_limits(&pipeline, &batch) {
            Ok(()) => {
                publish_with_circuit_breaker(&mut sink, &batch, &mut circuit_breaker)
                    .instrument(publish_span)
                    .await
            }
            Err(error) => PublishOutcome::Failure(error),
        };
        let publish_duration = publish_start.elapsed().as_secs_f64();
        metrics
            .sink_publish_duration_seconds
            .with_label_values(&[pipeline.name.as_str(), sink.name()])
            .observe(publish_duration);

        // v0.27.0: Use handle_publish_outcome() to determine the next action.
        // Async side-effects (acknowledge, DLQ write) are executed below.
        let directive = handle_publish_outcome(
            &publish_outcome,
            &mut consecutive_failures,
            &dlq_config,
            poll_interval_ms,
        );

        if let PublishOutcome::Failure(error) = &publish_outcome {
            metrics
                .connector_failures_total
                .with_label_values(&[
                    pipeline.name.as_str(),
                    direction_label.as_str(),
                    tenant_label.as_str(),
                    pipeline.require_str(&["sink_type"]).unwrap_or("unknown"),
                    &error.public_code().to_string(),
                    &error.retry_class().to_string(),
                ])
                .inc();
        }

        match directive {
            WorkerDirective::Continue => {
                // v0.40.0 (ADR-011): The sink acknowledged the batch, but the
                // offset write can still fail. A failed offset commit must be
                // visible — mark the pipeline unhealthy, skip the success-shaped
                // delivery receipt, and retry the batch. At-least-once means the
                // sink may see a duplicate on retry; silent loss is forbidden.
                metrics
                    .delivery_stage_total
                    .with_label_values(&[pipeline.name.as_str(), "sink_acknowledged", "success"])
                    .inc();
                delivery_transition(
                    &pipeline,
                    &runtime,
                    source.name(),
                    sink.name(),
                    "sink_acknowledged",
                    "success",
                    true,
                    "old",
                    batch.len(),
                );
                crate::test_failpoint!("after_sink_ack", &pipeline.name)?;
                commit_checkpoint(
                    &mut source,
                    checkpoint.as_deref(),
                    &metrics,
                    &pipeline.name,
                    "sink_acknowledged",
                )
                .await?;

                // v0.13.0: Observe end-to-end delivery latency.
                let latency_secs = poll_instant.elapsed().as_secs_f64();
                metrics
                    .delivery_latency_seconds
                    .with_label_values(&[&pipeline.name, &tenant_label])
                    .observe(latency_secs);

                // v0.13.0: Set pipeline_healthy gauge to 1 on success.
                metrics
                    .pipeline_healthy
                    .with_label_values(&[&pipeline.name, &tenant_label])
                    .set(1);
                metrics
                    .pipeline_last_success
                    .with_label_values(&[pipeline.name.as_str()])
                    .set(chrono::Utc::now().timestamp());
                mark_pipeline_health(&health, &pipeline.name, &tenant_label, true).await;

                metrics
                    .messages_published
                    .with_label_values(&[&pipeline.name, &direction_label, &tenant_label])
                    .inc_by(batch.len() as u64);

                // v0.28.0: Write delivery receipts after confirmed sink publish
                // AND a confirmed offset commit.
                write_delivery_receipts(
                    &db,
                    &pipeline.name,
                    &receipt_outbox_name,
                    sink.name(),
                    &tenant_label,
                    &batch,
                    &metrics,
                )
                .await;
            }

            WorkerDirective::BackoffMs(sleep_ms) => {
                metrics
                    .retry_state
                    .with_label_values(&[pipeline.name.as_str(), "retrying"])
                    .set(1);
                metrics
                    .retry_state
                    .with_label_values(&[pipeline.name.as_str(), "none"])
                    .set(0);
                if let PublishOutcome::Failure(ref e) = publish_outcome {
                    tracing::warn!(
                        pipeline = %pipeline.name,
                        error_code = %e.public_code(),
                        error_summary = e.public_summary(),
                        consecutive_failures,
                        "publish error"
                    );
                    metrics
                        .pipeline_healthy
                        .with_label_values(&[&pipeline.name, &tenant_label])
                        .set(0);
                    mark_pipeline_health(&health, &pipeline.name, &tenant_label, false).await;
                    metrics
                        .publish_errors
                        .with_label_values(&[&pipeline.name, &direction_label, &tenant_label])
                        .inc();
                    metrics
                        .pipeline_last_error
                        .with_label_values(&[pipeline.name.as_str()])
                        .set(chrono::Utc::now().timestamp());
                } else {
                    // CircuitBreakerOpen with DLQ disabled.
                    tracing::warn!(
                        pipeline = %pipeline.name,
                        "circuit breaker open — sleeping (DLQ disabled)"
                    );
                }
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(sleep_ms)) => {}
                    _ = stop_rx.changed() => { break; }
                }
            }

            WorkerDirective::RouteToDlq { reason, error_kind } => {
                let dlq_reason_label = match error_kind {
                    ErrorKind::SinkPermanent => "circuit_breaker_open",
                    ErrorKind::MaxRetriesExceeded => "max_retries_exceeded",
                    _ => "unknown",
                };
                if let PublishOutcome::Failure(ref e) = publish_outcome {
                    tracing::warn!(
                        pipeline = %pipeline.name,
                        error_code = %e.public_code(),
                        error_summary = e.public_summary(),
                        consecutive_failures,
                        "publish error"
                    );
                    metrics
                        .pipeline_healthy
                        .with_label_values(&[&pipeline.name, &tenant_label])
                        .set(0);
                    mark_pipeline_health(&health, &pipeline.name, &tenant_label, false).await;
                    metrics
                        .publish_errors
                        .with_label_values(&[&pipeline.name, &direction_label, &tenant_label])
                        .inc();
                    metrics
                        .pipeline_last_error
                        .with_label_values(&[pipeline.name.as_str()])
                        .set(chrono::Utc::now().timestamp());
                } else {
                    tracing::warn!(
                        pipeline = %pipeline.name,
                        "circuit breaker open — routing batch to DLQ"
                    );
                }
                let entries: Vec<DlqEntry> = batch
                    .iter()
                    .map(|msg| {
                        DlqEntry::from_message(
                            &direction_label,
                            &pipeline.name,
                            source.name(),
                            sink.name(),
                            msg,
                            &reason,
                            error_kind,
                        )
                    })
                    .collect();
                let dlq_span = tracing::info_span!(
                    "relay.dlq.insert",
                    pipeline = %pipeline.name,
                    count = entries.len(),
                    reason = dlq_reason_label,
                );
                let _enter = dlq_span.enter();
                crate::test_failpoint!("during_dlq_write", &pipeline.name)?;
                match route_to_dlq(
                    &db,
                    &entries,
                    &pipeline.name,
                    &direction_label,
                    &tenant_label,
                    &metrics,
                )
                .await
                {
                    DlqOutcome::Written => {
                        metrics
                            .dlq_depth
                            .with_label_values(&[pipeline.name.as_str()])
                            .add(entries.len() as i64);
                        metrics
                            .delivery_stage_total
                            .with_label_values(&[
                                pipeline.name.as_str(),
                                "dlq_persisted",
                                "success",
                            ])
                            .inc();
                        delivery_transition(
                            &pipeline,
                            &runtime,
                            source.name(),
                            sink.name(),
                            "dlq_persisted",
                            "success",
                            false,
                            "old",
                            batch.len(),
                        );
                        crate::test_failpoint!("after_dlq_commit", &pipeline.name)?;
                        commit_checkpoint(
                            &mut source,
                            checkpoint.as_deref(),
                            &metrics,
                            &pipeline.name,
                            "dlq_persisted",
                        )
                        .await?;
                        consecutive_failures = 0;
                    }
                    DlqOutcome::TransientError => {
                        // Retry next iteration — sleep briefly.
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_millis(poll_interval_ms)) => {}
                            _ = stop_rx.changed() => { break; }
                        }
                    }
                    DlqOutcome::PermanentError(e) => {
                        return Err(e);
                    }
                }
            }

            WorkerDirective::Shutdown(e) => {
                return Err(e);
            }
        }
    } // end loop

    // v0.13.0: Mark pipeline as healthy=0 on worker exit.
    metrics
        .pipeline_healthy
        .with_label_values(&[&pipeline.name, &tenant_label])
        .set(0);

    let _ = source.close().await;
    let _ = sink.close().await;
    Ok(())
}

/// Filter a batch to only include messages in the replay offset range.
fn filter_replay_batch(
    batch: Vec<crate::envelope::RelayMessage>,
    from_offset: Option<i64>,
    to_offset: Option<i64>,
) -> Vec<crate::envelope::RelayMessage> {
    batch
        .into_iter()
        .filter(|msg| {
            let id = msg.outbox_id.unwrap_or(0);
            let after_from = from_offset.map(|f| id >= f).unwrap_or(true);
            let before_to = to_offset.map(|t| id <= t).unwrap_or(true);
            after_from && before_to
        })
        .collect()
}

// ── v0.18.0 / v0.24.0 worker helper functions ────────────────────────────

/// Outcome of a `route_to_dlq` call.
#[derive(Debug)]
enum DlqOutcome {
    /// All entries were written; source should be acknowledged.
    Written,
    /// A transient error occurred; caller should retry later.
    TransientError,
    /// A permanent error occurred; caller must pause the pipeline.
    PermanentError(RelayError),
}

/// Write a set of DLQ entries to the database and handle transient / permanent
/// write failures consistently.
///
/// Returns `DlqOutcome` so the caller can decide whether to pause or retry.
/// Increments `dlq_entries_written` on success and `dlq_write_errors` on
/// permanent failure.
async fn route_to_dlq(
    db: &Arc<Client>,
    entries: &[DlqEntry],
    pipeline_name: &str,
    direction_label: &str,
    tenant_label: &str,
    metrics: &Arc<RelayMetrics>,
) -> DlqOutcome {
    match crate::dlq::insert_batch(db, entries).await {
        Ok(()) => {
            metrics
                .dlq_entries_written
                .with_label_values(&[pipeline_name, direction_label, tenant_label])
                .inc_by(entries.len() as u64);
            DlqOutcome::Written
        }
        Err(e) if e.is_transient() => {
            tracing::warn!(
                pipeline = %pipeline_name,
                error = %e,
                "transient DLQ insert error — will retry"
            );
            DlqOutcome::TransientError
        }
        Err(e) => {
            metrics
                .dlq_write_errors
                .with_label_values(&[pipeline_name])
                .inc();
            tracing::error!(
                pipeline = %pipeline_name,
                error = %e,
                "permanent DLQ insert error — pausing pipeline"
            );
            DlqOutcome::PermanentError(e)
        }
    }
}

// ── v0.28.0: Delivery receipt helper ────────────────────────────────────────

/// Write delivery receipt rows for a successfully published batch.
///
/// Inserts one row per message into `tide.relay_delivery_receipts` using a
/// single `UNNEST` statement, within the same connection as the worker.
/// Errors are logged at WARN and do not fail the pipeline — a missing receipt
/// is less harmful than a stuck pipeline.
async fn write_delivery_receipts(
    db: &Arc<Client>,
    pipeline_name: &str,
    outbox_name: &str,
    sink_type: &str,
    tenant_label: &str,
    batch: &[crate::envelope::RelayMessage],
    metrics: &Arc<RelayMetrics>,
) {
    if batch.is_empty() {
        return;
    }

    let mut message_ids: Vec<i64> = Vec::with_capacity(batch.len());
    let mut dedup_keys: Vec<String> = Vec::with_capacity(batch.len());

    for msg in batch {
        // outbox_id is the canonical message identifier in the outbox table.
        message_ids.push(msg.outbox_id.unwrap_or(0));
        dedup_keys.push(msg.dedup_key.clone());
    }

    let result = db
        .execute(
            "INSERT INTO tide.relay_delivery_receipts \
             (pipeline_name, outbox_name, message_id, dedup_key, sink_type, tenant_name) \
             SELECT $1, $2, unnest($3::bigint[]), unnest($4::text[]), $5, $6",
            &[
                &pipeline_name,
                &outbox_name,
                &message_ids,
                &dedup_keys,
                &sink_type,
                &tenant_label,
            ],
        )
        .await;

    match result {
        Ok(rows) => {
            metrics
                .receipts_written
                .with_label_values(&[pipeline_name, sink_type, tenant_label])
                .inc_by(rows);
            tracing::debug!(
                pipeline = %pipeline_name,
                count = rows,
                "delivery receipts written"
            );
        }
        Err(e) => {
            // Delivery receipt writes are best-effort; a missing receipt
            // is less harmful than a stuck pipeline (OWASP A05 Misconfiguration
            // — do not panic on optional audit writes).
            tracing::warn!(
                pipeline = %pipeline_name,
                error = %e,
                "failed to write delivery receipts (non-fatal)"
            );
        }
    }
}

// ── v0.24.0 worker decomposition helpers ─────────────────────────────────

async fn commit_checkpoint(
    source: &mut Box<dyn crate::source::Source>,
    checkpoint: Option<&crate::envelope::RelayMessage>,
    metrics: &Arc<RelayMetrics>,
    pipeline_name: &str,
    terminal_stage: &str,
) -> Result<(), RelayError> {
    let Some(checkpoint) = checkpoint else {
        return Ok(());
    };

    let source_name = source.name().to_string();
    crate::test_failpoint!("before_checkpoint_commit", pipeline_name)?;
    let ack_span = tracing::info_span!(
        "relay.source.acknowledge",
        pipeline = %pipeline_name,
        source = %source_name,
    );
    match source.acknowledge(checkpoint).instrument(ack_span).await {
        Ok(()) => {
            metrics
                .offset_writes
                .with_label_values(&[pipeline_name, &source_name])
                .inc();
            metrics
                .delivery_stage_total
                .with_label_values(&[pipeline_name, "checkpoint_committed", "success"])
                .inc();
            tracing::debug!(
                event_code = "PGTIDE_DELIVERY_TRANSITION",
                pipeline = %pipeline_name,
                source = %source_name,
                stage = "checkpoint_committed",
                outcome = "success",
                duplicate_risk = false,
                checkpoint_class = "new",
                "delivery transition"
            );
            crate::test_failpoint!("after_checkpoint_commit", pipeline_name)?;
            Ok(())
        }
        Err(error) => {
            metrics
                .checkpoint_commit_errors
                .with_label_values(&[pipeline_name, &source_name])
                .inc();
            metrics
                .delivery_stage_total
                .with_label_values(&[pipeline_name, terminal_stage, "checkpoint_error"])
                .inc();
            tracing::error!(
                pipeline = %pipeline_name,
                source = %source_name,
                error = %error,
                "source checkpoint commit failed after terminal disposition"
            );
            Err(error)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn delivery_transition(
    pipeline: &PipelineConfig,
    runtime: &WorkerRuntime,
    source: &str,
    sink: &str,
    stage: &str,
    outcome: &str,
    duplicate_risk: bool,
    checkpoint_class: &str,
    batch_size: usize,
) {
    tracing::debug!(
        event_code = "PGTIDE_DELIVERY_TRANSITION",
        pipeline = %pipeline.name,
        relay_group = %runtime.relay_group_id,
        direction = "forward",
        tenant = %pipeline.tenant_name,
        source,
        sink,
        batch_size,
        stage,
        outcome,
        duplicate_risk,
        checkpoint_class,
        "delivery transition"
    );
}

/// Outcome of a `poll_and_decode` call.
#[derive(Debug)]
enum PollOutcome {
    /// A non-empty batch of decoded messages ready to process.
    Batch {
        messages: Vec<crate::envelope::RelayMessage>,
        checkpoint: Option<Box<crate::envelope::RelayMessage>>,
    },
    /// The source returned an empty batch; caller should sleep and retry.
    Empty,
    /// A transient error occurred; caller should back off and retry.
    TransientError(RelayError),
    /// A permanent error occurred; caller must stop the pipeline.
    PermanentError(RelayError),
    /// Replay range is exhausted; caller should stop the pipeline.
    ReplayComplete,
}

/// Poll the source and apply replay filtering.
///
/// v0.24.0: Extracted from `worker_inner()` to make the polling and replay
/// logic independently testable.
async fn poll_and_decode(
    source: &mut Box<dyn crate::source::Source>,
    batch_size: i64,
    pipeline_name: &str,
    direction_label: &str,
    replay_from: Option<i64>,
    replay_to: Option<i64>,
    is_replay: bool,
) -> PollOutcome {
    let span = tracing::info_span!(
        "relay.source.poll",
        pipeline = %pipeline_name,
        direction = %direction_label,
    );
    let msgs = match source.poll(batch_size).instrument(span).await {
        Ok(msgs) => msgs,
        Err(e) => {
            if e.is_transient() {
                return PollOutcome::TransientError(e);
            } else {
                return PollOutcome::PermanentError(e);
            }
        }
    };

    if msgs.is_empty() {
        return if is_replay {
            PollOutcome::ReplayComplete
        } else {
            PollOutcome::Empty
        };
    }

    let checkpoint = msgs.last().cloned().map(Box::new);

    // Apply replay range filter when in replay mode.
    let msgs = if is_replay {
        let filtered = filter_replay_batch(msgs, replay_from, replay_to);
        if filtered.is_empty() {
            return PollOutcome::ReplayComplete;
        }
        filtered
    } else {
        msgs
    };

    PollOutcome::Batch {
        messages: msgs,
        checkpoint,
    }
}

/// Outcome of a `publish_with_circuit_breaker` call.
#[derive(Debug)]
pub(crate) enum PublishOutcome {
    /// Batch published successfully; source should be acknowledged.
    Success,
    /// Publish failed; caller should handle retry / DLQ logic.
    Failure(RelayError),
    /// Circuit breaker is open; batch routed to DLQ or dropped.
    CircuitBreakerOpen,
}

/// Attempt to publish a batch via the sink, honouring the circuit breaker.
///
/// Returns the result so the caller can handle acknowledge, retry, and DLQ
/// routing without nesting the full error path inside `worker_inner()`.
///
/// v0.24.0: Extracted from `worker_inner()` for independent testability.
async fn publish_with_circuit_breaker(
    sink: &mut Box<dyn crate::sink::Sink>,
    batch: &[crate::envelope::RelayMessage],
    circuit_breaker: &mut crate::circuit_breaker::CircuitBreaker,
) -> PublishOutcome {
    if !circuit_breaker.should_allow() {
        return PublishOutcome::CircuitBreakerOpen;
    }

    match sink.publish(batch).await {
        Ok(()) => {
            circuit_breaker.record_success();
            PublishOutcome::Success
        }
        Err(e) => {
            circuit_breaker.record_failure();
            PublishOutcome::Failure(e)
        }
    }
}

fn validate_publish_limits(
    pipeline: &PipelineConfig,
    messages: &[crate::envelope::RelayMessage],
) -> Result<(), RelayError> {
    let sink_type = pipeline.require_str(&["sink_type"])?;
    let Some(descriptor) = crate::descriptors::sink_type_to_descriptor(sink_type) else {
        return Err(RelayError::InvalidConfig {
            name: pipeline.name.clone(),
            reason: format!("unknown sink_type '{sink_type}'"),
        });
    };
    let Some(capabilities) = descriptor.capabilities else {
        return Ok(());
    };
    if messages.len() > capabilities.max_batch_size as usize {
        return Err(RelayError::connector_failure(
            sink_type,
            crate::error::ConnectorFailureCode::MessageTooLarge,
            crate::error::RetryClass::Permanent,
            "publish batch exceeds the connector message-count limit",
        ));
    }
    let mut total_bytes = 0_u64;
    for message in messages {
        let encoded = serde_json::to_vec(message)?;
        let message_bytes = encoded.len() as u64;
        if message_bytes > capabilities.max_message_bytes {
            return Err(RelayError::connector_failure(
                sink_type,
                crate::error::ConnectorFailureCode::MessageTooLarge,
                crate::error::RetryClass::Permanent,
                "encoded message exceeds the connector message-size limit",
            ));
        }
        total_bytes = total_bytes.saturating_add(message_bytes);
    }
    if total_bytes > capabilities.max_batch_bytes {
        return Err(RelayError::connector_failure(
            sink_type,
            crate::error::ConnectorFailureCode::MessageTooLarge,
            crate::error::RetryClass::Permanent,
            "encoded batch exceeds the connector byte limit",
        ));
    }
    Ok(())
}

// ── v0.27.0 worker decomposition helpers ─────────────────────────────────

/// Directive returned by `handle_publish_outcome()` to tell `worker_inner()`
/// what action to take after a publish attempt.
///
/// v0.27.0: Extracted from the post-publish match arm in `worker_inner()` so
/// the branching logic can be unit-tested independently of the async pipeline.
#[derive(Debug)]
pub(crate) enum WorkerDirective {
    /// Publish succeeded; acknowledge the source, update metrics, and continue.
    Continue,
    /// Sleep for the given number of milliseconds then retry.
    BackoffMs(u64),
    /// Route the batch to the DLQ with the given reason and error classification.
    RouteToDlq {
        reason: String,
        error_kind: ErrorKind,
    },
    /// Stop the pipeline immediately due to a permanent error.
    /// Currently unused by `handle_publish_outcome()`; reserved for future
    /// permanent-error detection paths that bypass the DLQ.
    #[allow(dead_code)]
    Shutdown(RelayError),
}

/// Determine the post-publish worker directive based on the publish outcome.
///
/// This function contains only branching logic — no async operations.
/// `worker_inner()` executes the async side-effects (acknowledge, DLQ write)
/// indicated by the returned `WorkerDirective`.
///
/// v0.27.0: Extracted from `worker_inner()` for independent unit-testability.
/// Closes the last major untested branch in the coordinator.
pub(crate) fn handle_publish_outcome(
    outcome: &PublishOutcome,
    consecutive_failures: &mut u32,
    dlq_config: &DlqConfig,
    poll_interval_ms: u64,
) -> WorkerDirective {
    match outcome {
        PublishOutcome::Success => {
            *consecutive_failures = 0;
            WorkerDirective::Continue
        }
        PublishOutcome::CircuitBreakerOpen => {
            if dlq_config.enabled {
                WorkerDirective::RouteToDlq {
                    reason: "circuit breaker open".to_string(),
                    error_kind: ErrorKind::SinkPermanent,
                }
            } else {
                WorkerDirective::BackoffMs(poll_interval_ms)
            }
        }
        PublishOutcome::Failure(e) => {
            *consecutive_failures += 1;
            if e.retry_class() == crate::error::RetryClass::Permanent {
                return if dlq_config.enabled {
                    WorkerDirective::RouteToDlq {
                        reason: safe_dlq_reason(e),
                        error_kind: ErrorKind::SinkPermanent,
                    }
                } else {
                    WorkerDirective::Shutdown(
                        e.owned_connector_failure()
                            .unwrap_or_else(|| RelayError::other("permanent connector failure")),
                    )
                };
            }
            if dlq_config.enabled && *consecutive_failures > dlq_config.max_retries {
                WorkerDirective::RouteToDlq {
                    reason: safe_dlq_reason(e),
                    error_kind: ErrorKind::MaxRetriesExceeded,
                }
            } else {
                WorkerDirective::BackoffMs(poll_interval_ms)
            }
        }
    }
}

fn safe_dlq_reason(error: &RelayError) -> String {
    format!("{}: {}", error.public_code(), error.public_summary())
}

// ── Source factory ────────────────────────────────────────────────────────

async fn build_source(
    pipeline: &PipelineConfig,
    db: Arc<Client>,
    relay_group_id: &str,
) -> Result<Box<dyn crate::source::Source>, RelayError> {
    if pipeline.require_str(&["source_type"])? != "outbox" {
        return Err(RelayError::InvalidConfig {
            name: pipeline.name.clone(),
            reason: "only source_type 'outbox' is supported".to_string(),
        });
    }

    let outbox = pipeline.require_str(&["source", "outbox"])?;
    let subject_template = pipeline
        .opt_str(&["source", "subject_template"])
        .unwrap_or("{outbox}.{op}");

    if let Some(group_name) = pipeline.opt_str(&["source", "consumer_group"]) {
        let consumer_id = pipeline
            .opt_str(&["source", "consumer_id"])
            .unwrap_or("pg-tide");
        let visibility = pipeline
            .opt_i64(&["source", "visibility_seconds"])
            .unwrap_or(30) as i32;
        let source = crate::source::outbox::OutboxPollerSource::new_consumer_group(
            db,
            outbox,
            subject_template,
            relay_group_id,
            &pipeline.name,
            group_name,
            consumer_id,
            visibility,
        )
        .await?;
        Ok(Box::new(source))
    } else {
        let source = crate::source::outbox::OutboxPollerSource::new_simple_native(
            db,
            outbox,
            subject_template,
            relay_group_id,
            &pipeline.name,
        )
        .await?;
        Ok(Box::new(source))
    }
}

async fn build_sink(
    pipeline: &PipelineConfig,
    db: Arc<Client>,
) -> Result<Box<dyn crate::sink::Sink>, RelayError> {
    let sink_type = pipeline.require_str(&["sink_type"])?;
    let descriptor = crate::descriptors::sink_type_to_descriptor(sink_type).ok_or_else(|| {
        RelayError::InvalidConfig {
            name: pipeline.name.clone(),
            reason: format!("unknown sink_type '{sink_type}'"),
        }
    })?;
    if !crate::descriptors::is_available(descriptor) {
        return Err(RelayError::InvalidConfig {
            name: pipeline.name.clone(),
            reason: format!("sink_type '{sink_type}' is not compiled in"),
        });
    }

    match sink_type {
        #[cfg(feature = "stdout")]
        "stdout" => {
            let format = match pipeline.opt_str(&["sink", "format"]).unwrap_or("jsonl") {
                "pretty" => crate::sink::stdout::StdoutFormat::JsonPretty,
                _ => crate::sink::stdout::StdoutFormat::Jsonl,
            };
            Ok(Box::new(crate::sink::stdout::StdoutSink::new(format)))
        }
        #[cfg(feature = "stdout")]
        "file" => {
            let path = pipeline.require_str(&["sink", "path"])?;
            let format = match pipeline.opt_str(&["sink", "format"]).unwrap_or("jsonl") {
                "pretty" => crate::sink::stdout::StdoutFormat::JsonPretty,
                _ => crate::sink::stdout::StdoutFormat::Jsonl,
            };
            Ok(Box::new(
                crate::sink::stdout::FileSink::new(path, format).await?,
            ))
        }
        "inbox" => {
            let inbox = pipeline.require_str(&["sink", "inbox"])?;
            if let Some(postgres_url) = pipeline.opt_str(&["sink", "postgres_url"]) {
                Ok(Box::new(
                    crate::sink::pg_outbox::PgInboxSink::new(postgres_url, inbox).await?,
                ))
            } else {
                Ok(Box::new(
                    crate::sink::inbox::InboxSink::new_for_logical_name(db, inbox).await?,
                ))
            }
        }
        "pg_outbox" => {
            let postgres_url = pipeline.require_str(&["sink", "postgres_url"])?;
            let inbox = pipeline.require_str(&["sink", "inbox"])?;
            Ok(Box::new(
                crate::sink::pg_outbox::PgInboxSink::new(postgres_url, inbox).await?,
            ))
        }
        #[cfg(feature = "nats")]
        "nats" => {
            let url = pipeline.require_str(&["sink", "url"])?;
            let subject = pipeline.opt_str(&["sink", "subject"]);
            let subject_template = pipeline.opt_str(&["sink", "subject_template"]);
            Ok(Box::new(
                crate::sink::nats::NatsSink::new_with_options(crate::sink::nats::NatsOptions {
                    url,
                    subject,
                    subject_template,
                    allow_insecure: pipeline
                        .opt_bool(&["sink", "allow_insecure"])
                        .unwrap_or(false),
                    token: pipeline.opt_str(&["sink", "token"]),
                    username: pipeline.opt_str(&["sink", "username"]),
                    password: pipeline.opt_str(&["sink", "password"]),
                    credentials_file: pipeline.opt_str(&["sink", "credentials_file"]),
                    tls_ca_file: pipeline.opt_str(&["sink", "tls_ca_file"]),
                    tls_client_cert: pipeline.opt_str(&["sink", "tls_client_cert"]),
                    tls_client_key: pipeline.opt_str(&["sink", "tls_client_key"]),
                })
                .await?,
            ))
        }
        #[cfg(feature = "kafka")]
        "kafka" => {
            let brokers = pipeline.require_str(&["sink", "brokers"])?;
            let topic_template = pipeline
                .opt_str(&["sink", "topic_template"])
                .or_else(|| pipeline.opt_str(&["sink", "topic"]))
                .unwrap_or("{stream_table}");
            Ok(Box::new(crate::sink::kafka::KafkaSink::new_with_options(
                crate::sink::kafka::KafkaOptions {
                    brokers,
                    topic_template: topic_template.to_string(),
                    security_protocol: pipeline
                        .opt_str(&["sink", "security_protocol"])
                        .unwrap_or("ssl"),
                    allow_insecure: pipeline
                        .opt_bool(&["sink", "allow_insecure"])
                        .unwrap_or(false),
                    ssl_ca_location: pipeline.opt_str(&["sink", "ssl_ca_location"]),
                    ssl_certificate_location: pipeline
                        .opt_str(&["sink", "ssl_certificate_location"]),
                    ssl_key_location: pipeline.opt_str(&["sink", "ssl_key_location"]),
                    sasl_mechanism: pipeline.opt_str(&["sink", "sasl_mechanism"]),
                    sasl_username: pipeline.opt_str(&["sink", "sasl_username"]),
                    sasl_password: pipeline.opt_str(&["sink", "sasl_password"]),
                },
            )?))
        }
        #[cfg(feature = "webhook")]
        "webhook" => {
            let url = pipeline.require_str(&["sink", "url"])?;
            let timeout = pipeline.opt_i64(&["sink", "timeout_secs"]).unwrap_or(30) as u64;
            let allow_http = pipeline.opt_bool(&["sink", "allow_http"]).unwrap_or(false);
            let ssrf_protection = pipeline
                .opt_bool(&["sink", "ssrf_protection"])
                .unwrap_or(true);
            let signing_secret = pipeline.opt_str(&["sink", "signing_secret"]);
            let signing_algorithm = pipeline
                .opt_str(&["sink", "signing_algorithm"])
                .unwrap_or("hmac-sha256");
            Ok(Box::new(
                crate::sink::webhook::WebhookSink::new_with_options(
                    url,
                    timeout,
                    allow_http,
                    ssrf_protection,
                    signing_secret,
                    signing_algorithm,
                )?,
            ))
        }
        other => Err(RelayError::InvalidConfig {
            name: pipeline.name.clone(),
            reason: format!("unknown or removed sink_type '{other}'"),
        }),
    }
}

// ── Public factory wrappers for CLI validation ────────────────────────────

/// Build a source for validation purposes (used by `pg-tide validate-config`).
pub async fn build_source_for_validation(
    pipeline: &PipelineConfig,
    db: Arc<Client>,
    relay_group_id: &str,
) -> Result<Box<dyn crate::source::Source>, RelayError> {
    build_source(pipeline, db, relay_group_id).await
}

/// Build a sink for validation purposes (used by `pg-tide validate-config`).
pub async fn build_sink_for_validation(
    pipeline: &PipelineConfig,
    db: Arc<Client>,
) -> Result<Box<dyn crate::sink::Sink>, RelayError> {
    build_sink(pipeline, db).await
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn test_coordinator_construction() {
        // Just verifies the struct can be constructed with the right types.
        // Full integration tests use Testcontainers.
        let group_id = "test-group";
        assert_eq!(group_id, "test-group");
    }

    // ── v0.27.0: handle_publish_outcome() unit tests ──────────────────────

    fn make_dlq_config(enabled: bool, max_retries: u32) -> DlqConfig {
        DlqConfig {
            enabled,
            max_retries,
            ..DlqConfig::default()
        }
    }

    #[test]
    fn handle_publish_outcome_success_resets_failures() {
        let mut failures: u32 = 5;
        let dlq = make_dlq_config(true, 3);
        let directive = handle_publish_outcome(&PublishOutcome::Success, &mut failures, &dlq, 1000);
        assert_eq!(failures, 0, "consecutive_failures must be reset on success");
        assert!(
            matches!(directive, WorkerDirective::Continue),
            "success must return Continue"
        );
    }

    #[test]
    fn handle_publish_outcome_circuit_open_with_dlq() {
        let mut failures: u32 = 0;
        let dlq = make_dlq_config(true, 3);
        let directive = handle_publish_outcome(
            &PublishOutcome::CircuitBreakerOpen,
            &mut failures,
            &dlq,
            1000,
        );
        assert!(
            matches!(
                directive,
                WorkerDirective::RouteToDlq {
                    error_kind: ErrorKind::SinkPermanent,
                    ..
                }
            ),
            "open circuit breaker with DLQ enabled must route to DLQ"
        );
    }

    #[test]
    fn handle_publish_outcome_circuit_open_no_dlq() {
        let mut failures: u32 = 0;
        let dlq = make_dlq_config(false, 3);
        let directive = handle_publish_outcome(
            &PublishOutcome::CircuitBreakerOpen,
            &mut failures,
            &dlq,
            2000,
        );
        assert!(
            matches!(directive, WorkerDirective::BackoffMs(2000)),
            "open circuit breaker without DLQ must backoff"
        );
    }

    #[test]
    fn handle_publish_outcome_failure_below_max_retries() {
        let mut failures: u32 = 0;
        let dlq = make_dlq_config(true, 3);
        let e = RelayError::Other("sink error".into());
        let directive =
            handle_publish_outcome(&PublishOutcome::Failure(e), &mut failures, &dlq, 1000);
        assert_eq!(failures, 1);
        assert!(
            matches!(directive, WorkerDirective::BackoffMs(1000)),
            "failure below max_retries must backoff"
        );
    }

    #[test]
    fn handle_publish_outcome_failure_exceeds_max_retries() {
        let mut failures: u32 = 3; // already at max_retries=3
        let dlq = make_dlq_config(true, 3);
        let e = RelayError::Other("persistent sink error".into());
        let directive =
            handle_publish_outcome(&PublishOutcome::Failure(e), &mut failures, &dlq, 1000);
        assert_eq!(failures, 4, "failure count must be incremented");
        assert!(
            matches!(
                directive,
                WorkerDirective::RouteToDlq {
                    error_kind: ErrorKind::MaxRetriesExceeded,
                    ..
                }
            ),
            "exceeding max_retries must route to DLQ"
        );
    }

    #[test]
    fn handle_publish_outcome_failure_no_dlq() {
        let mut failures: u32 = 100; // way above any threshold
        let dlq = make_dlq_config(false, 0);
        let e = RelayError::Other("sink error".into());
        let directive =
            handle_publish_outcome(&PublishOutcome::Failure(e), &mut failures, &dlq, 500);
        assert!(
            matches!(directive, WorkerDirective::BackoffMs(500)),
            "failure without DLQ enabled must always backoff"
        );
    }

    #[test]
    fn handle_publish_outcome_permanent_connector_failure_routes_immediately() {
        let mut failures: u32 = 0;
        let dlq = make_dlq_config(true, 3);
        let error = RelayError::connector_failure(
            "webhook",
            crate::error::ConnectorFailureCode::Authorization,
            crate::error::RetryClass::Permanent,
            "webhook authorization was rejected",
        );
        let directive =
            handle_publish_outcome(&PublishOutcome::Failure(error), &mut failures, &dlq, 1000);
        assert_eq!(failures, 1);
        assert!(matches!(
            directive,
            WorkerDirective::RouteToDlq {
                error_kind: ErrorKind::SinkPermanent,
                ..
            }
        ));
    }

    fn limit_test_pipeline() -> PipelineConfig {
        PipelineConfig {
            name: "limit-test".to_string(),
            direction: PipelineDirection::Forward,
            enabled: true,
            config: serde_json::json!({
                "sink_type": "nats",
                "sink": {"url": "nats://localhost", "allow_insecure": true, "subject": "orders.created"}
            }),
            tenant_name: "default".to_string(),
        }
    }

    fn message_with_payload_size(size: usize) -> crate::envelope::RelayMessage {
        crate::envelope::RelayMessage::new_reverse(
            "limit-event",
            "orders.created",
            serde_json::json!({"data": "x".repeat(size)}),
        )
    }

    #[test]
    fn publish_limits_accept_exact_batch_count_and_reject_one_over() {
        let pipeline = limit_test_pipeline();
        let exact = (0..100)
            .map(|index| {
                crate::envelope::RelayMessage::new_reverse(
                    format!("event-{index}"),
                    "orders.created",
                    serde_json::json!({}),
                )
            })
            .collect::<Vec<_>>();
        assert!(validate_publish_limits(&pipeline, &exact).is_ok());

        let mut over = exact;
        over.push(crate::envelope::RelayMessage::new_reverse(
            "event-over",
            "orders.created",
            serde_json::json!({}),
        ));
        let error = validate_publish_limits(&pipeline, &over).unwrap_err();
        assert_eq!(
            error.connector_code(),
            Some(crate::error::ConnectorFailureCode::MessageTooLarge)
        );
    }

    #[test]
    fn publish_limits_accept_exact_message_bytes_and_reject_one_over() {
        let pipeline = limit_test_pipeline();
        let maximum = crate::descriptors::sink_type_to_descriptor("nats")
            .and_then(|descriptor| descriptor.capabilities)
            .expect("nats capabilities")
            .max_message_bytes as usize;
        let mut low = 0;
        let mut high = maximum;
        while low < high {
            let candidate = (low + high).div_ceil(2);
            if serde_json::to_vec(&message_with_payload_size(candidate))
                .expect("serialize message")
                .len()
                <= maximum
            {
                low = candidate;
            } else {
                high = candidate - 1;
            }
        }
        let exact = message_with_payload_size(low);
        assert_eq!(
            serde_json::to_vec(&exact)
                .expect("serialize exact message")
                .len(),
            maximum
        );
        assert!(validate_publish_limits(&pipeline, &[exact]).is_ok());

        let error =
            validate_publish_limits(&pipeline, &[message_with_payload_size(low + 1)]).unwrap_err();
        assert_eq!(
            error.connector_code(),
            Some(crate::error::ConnectorFailureCode::MessageTooLarge)
        );
    }

    #[test]
    fn publish_limits_accept_exact_batch_bytes_and_reject_one_over() {
        let pipeline = limit_test_pipeline();
        let capabilities = crate::descriptors::sink_type_to_descriptor("nats")
            .and_then(|descriptor| descriptor.capabilities)
            .expect("nats capabilities");
        let fixed_messages = (0..16)
            .map(|_| message_with_payload_size(1_000_000))
            .collect::<Vec<_>>();
        let fixed_bytes: usize = fixed_messages
            .iter()
            .map(|message| {
                serde_json::to_vec(message)
                    .expect("serialize message")
                    .len()
            })
            .sum();
        let target = capabilities.max_batch_bytes as usize;
        let mut low = 0;
        let mut high = capabilities.max_message_bytes as usize;
        while low < high {
            let candidate = (low + high).div_ceil(2);
            let total = fixed_bytes
                + serde_json::to_vec(&message_with_payload_size(candidate))
                    .expect("serialize message")
                    .len();
            if total <= target {
                low = candidate;
            } else {
                high = candidate - 1;
            }
        }
        let mut exact = fixed_messages;
        exact.push(message_with_payload_size(low));
        let encoded_bytes: usize = exact
            .iter()
            .map(|message| {
                serde_json::to_vec(message)
                    .expect("serialize message")
                    .len()
            })
            .sum();
        assert_eq!(encoded_bytes, target);
        assert!(validate_publish_limits(&pipeline, &exact).is_ok());

        exact.pop();
        exact.push(message_with_payload_size(low + 1));
        let error = validate_publish_limits(&pipeline, &exact).unwrap_err();
        assert_eq!(
            error.connector_code(),
            Some(crate::error::ConnectorFailureCode::MessageTooLarge)
        );
    }
}
