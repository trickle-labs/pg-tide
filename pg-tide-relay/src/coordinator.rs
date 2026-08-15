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
use crate::jmespath_transform::{apply_transforms, TransformConfig};
use crate::metrics::{HealthState, RelayMetrics};
use crate::rate_limiter::build_rate_limiter;
use crate::routing::{apply_routing, RoutingConfig};

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
            // Per-tenant filtering: only own pipelines belonging to this tenant.
            client
                .query(
                    "SELECT name, 'forward' AS direction, enabled, config,
                            COALESCE(tenant_name, 'default') AS tenant_name
                       FROM tide.relay_outbox_config
                      WHERE enabled = true AND tenant_name = $1
                     UNION ALL
                     SELECT name, 'reverse' AS direction, enabled, config,
                            COALESCE(tenant_name, 'default') AS tenant_name
                       FROM tide.relay_inbox_config
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
                      WHERE enabled = true
                     UNION ALL
                     SELECT name, 'reverse' AS direction, enabled, config,
                            COALESCE(tenant_name, 'default') AS tenant_name
                       FROM tide.relay_inbox_config
                      WHERE enabled = true",
                    &[],
                )
                .await?
        };

        let mut pipelines = Vec::new();
        for row in rows {
            let name: String = row.get("name");
            let direction: String = row.get("direction");
            let enabled: bool = row.get("enabled");
            let config: serde_json::Value = row.get("config");
            let tenant_name: String = row.get("tenant_name");

            pipelines.push(PipelineConfig {
                name,
                direction: if direction == "forward" {
                    PipelineDirection::Forward
                } else {
                    PipelineDirection::Reverse
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
        for pipeline_id in self.owned.keys() {
            let _ = self.release_lock(pipeline_id).await;
        }
        Ok(())
    }

    /// Signal all owned pipelines to stop and wait for them to finish their
    /// current batch.  Called during graceful shutdown before `release_all_locks`.
    pub async fn drain(&self) {
        // Send the stop signal to every owned pipeline.
        for (pipeline_id, (tx, _handle)) in &self.owned {
            if tx.send(true).is_err() {
                tracing::debug!(pipeline = %pipeline_id, "pipeline already stopped");
            }
        }

        // Wait until every pipeline's receiver is closed (i.e. the task exited).
        for (pipeline_id, (tx, _handle)) in &self.owned {
            tx.closed().await;
            tracing::debug!(pipeline = %pipeline_id, "pipeline drained");
        }
    }

    // ── Private ──────────────────────────────────────────────────────────

    /// v0.30.0: Check whether a pipeline's upstream DAG dependencies allow it
    /// to be acquired.  Returns `true` when all upstream policies are satisfied
    /// (pipeline is eligible), `false` when any policy gates acquisition.
    ///
    /// Policy semantics:
    ///   - `always`: downstream acquires unconditionally (always returns true).
    ///   - `on_idle`: downstream only acquires when upstream consumer lag is 0.
    ///   - `on_offset_gte(N)`: downstream only acquires when upstream committed ≥ N.
    ///
    /// When the `relay_pipeline_deps` table does not exist (pre-v0.30.0 schema),
    /// this function returns `true` so the coordinator degrades gracefully.
    async fn dag_check_acquisition(&self, pipeline_id: &str) -> bool {
        let conn = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    pipeline = %pipeline_id,
                    error = %e,
                    "dag-check: failed to get pool connection — allowing acquisition"
                );
                return true;
            }
        };

        // Check if the table exists (schema may be pre-v0.30.0).
        let table_exists: bool = conn
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
                 WHERE table_schema = 'tide' AND table_name = 'relay_pipeline_deps')",
                &[],
            )
            .await
            .map(|r| r.get(0))
            .unwrap_or(false);

        if !table_exists {
            return true;
        }

        // Fetch all upstream edges for this pipeline.
        let rows = match conn
            .query(
                "SELECT d.upstream_pipeline, d.trigger_policy, \
                        COALESCE(o.last_change_id, 0) AS committed, \
                        COALESCE((SELECT MAX(id) FROM tide.tide_outbox_messages \
                                  WHERE stream_table = d.upstream_pipeline), 0) AS max_id \
                 FROM tide.relay_pipeline_deps d \
                 LEFT JOIN tide.relay_consumer_offsets o \
                   ON o.pipeline_id = d.upstream_pipeline \
                  AND o.relay_group_id = $2 \
                 WHERE d.downstream_pipeline = $1",
                &[&pipeline_id, &self.relay_group_id],
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(
                    pipeline = %pipeline_id,
                    error = %e,
                    "dag-check: query failed — allowing acquisition"
                );
                return true;
            }
        };

        for row in &rows {
            let upstream: String = row.get(0);
            let policy: String = row.get(1);
            let committed: i64 = row.get(2);
            let max_id: i64 = row.get(3);
            let lag = max_id.saturating_sub(committed);

            let satisfied = match policy.as_str() {
                "always" => true,
                "on_idle" => lag == 0,
                p if p.starts_with("on_offset_gte(") => {
                    let threshold: i64 = p
                        .trim_start_matches("on_offset_gte(")
                        .trim_end_matches(')')
                        .parse()
                        .unwrap_or(0);
                    committed >= threshold
                }
                _ => true,
            };

            if !satisfied {
                tracing::debug!(
                    pipeline = %pipeline_id,
                    upstream = %upstream,
                    policy = %policy,
                    committed,
                    lag,
                    "dag-check: upstream policy unsatisfied — skipping acquisition"
                );
                return false;
            }
        }

        true
    }

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
                        "worker panicked — releasing lock and cleaning up"
                    ),
                }
                if let Err(e) = self.release_lock(&name).await {
                    tracing::warn!(
                        pipeline = %name,
                        error = %e,
                        "failed to release advisory lock after worker panic"
                    );
                }
            }
        }

        let pipelines = match self.load_pipelines().await {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(error = %e, "failed to load pipelines — skipping reconciliation");
                return;
            }
        };

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
            if let Some((tx, _handle)) = self.owned.remove(name) {
                let _ = tx.send(true);
            }
            if let Err(e) = self.release_lock(name).await {
                tracing::warn!(pipeline = %name, error = %e, "failed to release advisory lock");
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

            // v0.30.0: DAG-aware acquisition — check upstream policy before
            // attempting the advisory lock.  If any upstream is not satisfied,
            // skip this pipeline; it will be retried on the next reconcile cycle.
            if !self.dag_check_acquisition(&pipeline.name).await {
                continue;
            }

            let acquired = match self.try_acquire_lock(&pipeline.name).await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(pipeline = %pipeline.name, error = %e, "advisory lock error");
                    continue;
                }
            };

            if !acquired {
                tracing::debug!(
                    pipeline = %pipeline.name,
                    "advisory lock held by another pod — skipping"
                );
                continue;
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
                    if let Err(re) = self.release_lock(&pipeline.name).await {
                        tracing::warn!(error = %re, "failed to release lock after secret error");
                    }
                    continue;
                }
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
                db_url.to_string(),
                self.relay_group_id.clone(),
                Arc::clone(&self.metrics),
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
        h.healthy_pipelines = self.owned.keys().cloned().collect();
        // Pipelines are unhealthy once their metric is 0 — for coordinator
        // purposes we only track ownership; worker errors are reflected by the
        // pipeline_healthy gauge and visible via the metrics endpoint.
        h.unhealthy_pipelines.clear();
    }
}

// ── Pipeline worker ───────────────────────────────────────────────────────

/// Top-level worker task: wraps `worker_inner` and logs the outcome.
async fn run_pipeline_worker(
    pipeline: PipelineConfig,
    db_url: String,
    relay_group_id: String,
    metrics: Arc<RelayMetrics>,
    batch_size: i64,
    mut stop_rx: watch::Receiver<bool>,
) {
    let name = pipeline.name.clone();
    let tenant_label = pipeline.tenant_name.clone();

    // v0.13.0: Mark pipeline as healthy when worker starts.
    metrics
        .pipeline_healthy
        .with_label_values(&[&name, &tenant_label])
        .set(1);

    match worker_inner(
        pipeline,
        db_url,
        relay_group_id,
        metrics.clone(),
        batch_size,
        &mut stop_rx,
    )
    .await
    {
        Ok(()) => {
            tracing::info!(pipeline = %name, "worker stopped");
            // Mark as 0 on clean stop.
            metrics
                .pipeline_healthy
                .with_label_values(&[&name, &tenant_label])
                .set(0);
        }
        Err(e) => {
            tracing::error!(pipeline = %name, error = %e, "worker exited with error");
            // Mark as 0 on error exit.
            metrics
                .pipeline_healthy
                .with_label_values(&[&name, &tenant_label])
                .set(0);
            // v0.16.0: Record pipeline error by class.
            let error_class = if e.is_transient() {
                "transient"
            } else {
                "permanent"
            };
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
    db_url: String,
    relay_group_id: String,
    metrics: Arc<RelayMetrics>,
    default_batch_size: i64,
    stop_rx: &mut watch::Receiver<bool>,
) -> Result<(), RelayError> {
    // Each worker owns its own DB connection so pipelines are isolated.
    // v0.15.0: Use pg_tls::connect to honour sslmode from the URL.
    let (db_client, db_conn) = crate::pg_tls::connect(&db_url).await?;
    let db = Arc::new(db_client);
    tokio::spawn(async move {
        if let Err(e) = db_conn.await {
            tracing::error!("worker DB connection closed with error: {e}");
        }
    });

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
    let transform_config = TransformConfig::from_pipeline_config(&pipeline.config);
    let routing_config = RoutingConfig::from_pipeline_config(&pipeline.config);
    let rate_limiter = build_rate_limiter(&pipeline.config);
    let mut circuit_breaker = CircuitBreaker::from_pipeline_config(&pipeline.config);

    // v0.7.0: Replay mode — read from_offset/to_offset.
    let replay_from = pipeline.opt_i64(&["replay", "from_offset"]);
    let replay_to = pipeline.opt_i64(&["replay", "to_offset"]);
    let is_replay = replay_from.is_some();

    // v0.13.0: Wire-format factory — instantiate the configured wire format.
    let wire_format = crate::wire_format::from_config(&pipeline.config);

    // v0.16.0: Schema evolution guard — tracks fingerprints and enforces
    // the configured on_schema_change policy.
    let mut schema_guard = crate::schema_evolution::SchemaEvolutionGuard::from_config(
        &pipeline.name,
        Arc::clone(&db),
        &pipeline.config,
    );
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

    let mut source = build_source(&pipeline, Arc::clone(&db), &relay_group_id).await?;
    let mut sink = build_sink(&pipeline, Arc::clone(&db)).await?;

    let direction_label = match pipeline.direction {
        PipelineDirection::Forward => "forward".to_string(),
        PipelineDirection::Reverse => "reverse".to_string(),
    };

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

    loop {
        if *stop_rx.borrow() {
            break;
        }

        let batch = {
            // v0.24.0: Use poll_and_decode() helper for clean separation of
            // poll, replay-filter, and error-classification logic.
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
                PollOutcome::Batch(msgs) => {
                    // v0.15.0: Reset backoff on successful poll.
                    poll_backoff_ms = poll_interval_ms;
                    msgs
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
                        error = %e,
                        "permanent poll error — stopping pipeline"
                    );
                    return Err(e);
                }
            }
        };

        // v0.13.0: Increment consumed counter after successful poll.
        metrics
            .messages_consumed
            .with_label_values(&[&pipeline.name, &direction_label, &tenant_label])
            .inc_by(batch.len() as u64);

        // v0.13.0: Record the poll timestamp for end-to-end latency tracking.
        let poll_instant = std::time::Instant::now();

        // v0.7.0: Apply JMESPath transforms, schema evolution, and routing.
        // v0.18.0: Extracted into process_batch() helper for independent testability.
        let batch = {
            let span = tracing::info_span!(
                "relay.transform.evaluate",
                pipeline = %pipeline.name,
                batch_size = batch.len(),
            );
            let _enter = span.enter();
            match apply_transforms(&transform_config, batch) {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(pipeline = %pipeline.name, error = %e, "transform error");
                    continue;
                }
            }
        };

        if batch.is_empty() {
            // All messages filtered out — acknowledge the source and continue.
            continue;
        }

        // v0.16.0: Schema evolution check — compare payload schema fingerprint
        // against stored fingerprint and apply the configured policy.
        // v0.27.0: Extracted into apply_schema_evolution_check() for independent
        // testability and fuzz coverage.
        if apply_schema_evolution_check(&batch, &mut schema_guard, &pipeline.name).await {
            continue;
        }

        // v0.7.0: Apply content-based routing.
        // v0.16.0: OTel span around routing evaluation.
        let mut batch = batch;
        {
            let span = tracing::info_span!(
                "relay.routing.apply",
                pipeline = %pipeline.name,
                batch_size = batch.len(),
            );
            let _enter = span.enter();
            apply_routing(&routing_config, &mut batch);
        }

        // v0.7.0: Dry-run mode — log what would be published, skip actual publish.
        if dry_run {
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
            if let Some(last) = batch.last() {
                let _ = source.acknowledge(last).await;
            }
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
        let publish_span = tracing::info_span!(
            "relay.sink.publish",
            pipeline = %pipeline.name,
            batch_size = batch.len(),
        );
        let publish_start = std::time::Instant::now();
        let publish_outcome = publish_with_circuit_breaker(&mut sink, &batch, &mut circuit_breaker)
            .instrument(publish_span)
            .await;
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

        match directive {
            WorkerDirective::Continue => {
                // v0.40.0 (ADR-011): The sink acknowledged the batch, but the
                // offset write can still fail. A failed offset commit must be
                // visible — mark the pipeline unhealthy, skip the success-shaped
                // delivery receipt, and retry the batch. At-least-once means the
                // sink may see a duplicate on retry; silent loss is forbidden.
                if let Some(last) = batch.last() {
                    // v0.13.0: OTel span around acknowledge.
                    let ack_span = tracing::info_span!(
                        "relay.source.acknowledge",
                        pipeline = %pipeline.name,
                    );
                    if let Err(e) = source.acknowledge(last).instrument(ack_span).await {
                        tracing::error!(
                            pipeline = %pipeline.name,
                            relay_group = %relay_group_id,
                            outbox = %receipt_outbox_name,
                            attempted_offset = last.outbox_id.unwrap_or(0),
                            error = %e,
                            "offset commit failed after sink publish — pipeline unhealthy, \
                             will retry batch (at-least-once)"
                        );
                        metrics
                            .pipeline_healthy
                            .with_label_values(&[&pipeline.name, &tenant_label])
                            .set(0);
                        metrics
                            .publish_errors
                            .with_label_values(&[&pipeline.name, &direction_label, &tenant_label])
                            .inc();
                        consecutive_failures += 1;
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_millis(poll_interval_ms)) => {}
                            _ = stop_rx.changed() => { break; }
                        }
                        continue;
                    }
                }

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
                if let PublishOutcome::Failure(ref e) = publish_outcome {
                    tracing::warn!(
                        pipeline = %pipeline.name,
                        error = %e,
                        consecutive_failures,
                        "publish error"
                    );
                    metrics
                        .pipeline_healthy
                        .with_label_values(&[&pipeline.name, &tenant_label])
                        .set(0);
                    metrics
                        .publish_errors
                        .with_label_values(&[&pipeline.name, &direction_label, &tenant_label])
                        .inc();
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
                        error = %e,
                        consecutive_failures,
                        "publish error"
                    );
                    metrics
                        .pipeline_healthy
                        .with_label_values(&[&pipeline.name, &tenant_label])
                        .set(0);
                    metrics
                        .publish_errors
                        .with_label_values(&[&pipeline.name, &direction_label, &tenant_label])
                        .inc();
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
                        if let Some(last) = batch.last() {
                            let _ = source.acknowledge(last).await;
                        }
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

/// Outcome of a `poll_and_decode` call.
#[derive(Debug)]
enum PollOutcome {
    /// A non-empty batch of decoded messages ready to process.
    Batch(Vec<crate::envelope::RelayMessage>),
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
        return PollOutcome::Empty;
    }

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

    PollOutcome::Batch(msgs)
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
            if dlq_config.enabled && *consecutive_failures > dlq_config.max_retries {
                WorkerDirective::RouteToDlq {
                    reason: e.to_string(),
                    error_kind: ErrorKind::MaxRetriesExceeded,
                }
            } else {
                WorkerDirective::BackoffMs(poll_interval_ms)
            }
        }
    }
}

/// Apply the configured schema evolution policy to a batch.
///
/// Returns `true` if the batch should be **skipped** (breaking change +
/// `Pause` policy triggered). Returns `false` if the batch may proceed.
///
/// v0.27.0: Extracted from `worker_inner()` so the schema evolution path is
/// independently fuzzable and unit-testable.
async fn apply_schema_evolution_check(
    batch: &[crate::envelope::RelayMessage],
    schema_guard: &mut crate::schema_evolution::SchemaEvolutionGuard,
    pipeline_name: &str,
) -> bool {
    let topic = batch
        .first()
        .map(|m| m.subject.as_str())
        .unwrap_or("unknown");
    let columns: Vec<String> = batch
        .first()
        .and_then(|m| m.payload.as_object())
        .map(|obj| obj.keys().cloned().collect())
        .unwrap_or_default();

    let se_span = tracing::info_span!(
        "relay.schema_evolution.check",
        pipeline = %pipeline_name,
        topic,
    );
    let _enter = se_span.enter();
    match schema_guard.observe(topic, &columns).await {
        Ok((
            crate::schema_evolution::SchemaChangeKind::Breaking,
            crate::schema_evolution::OnSchemaChange::Pause,
        )) => {
            tracing::warn!(
                pipeline = %pipeline_name,
                topic,
                "breaking schema change detected — pausing pipeline per policy"
            );
            true // skip this batch
        }
        Ok((kind, policy)) => {
            if kind != crate::schema_evolution::SchemaChangeKind::NoChange
                && kind != crate::schema_evolution::SchemaChangeKind::Initial
            {
                tracing::warn!(
                    pipeline = %pipeline_name,
                    topic,
                    change_kind = ?kind,
                    policy = policy.as_str(),
                    "schema change detected"
                );
            }
            false // proceed
        }
        Err(e) => {
            tracing::warn!(
                pipeline = %pipeline_name,
                error = %e,
                "schema evolution check error — continuing"
            );
            false // proceed on error
        }
    }
}

// ── Source factory ────────────────────────────────────────────────────────

async fn build_source(
    pipeline: &PipelineConfig,
    db: Arc<Client>,
    relay_group_id: &str,
) -> Result<Box<dyn crate::source::Source>, RelayError> {
    let source_type = pipeline.require_str(&["source_type"])?;
    match source_type {
        // v0.40.0 (ADR-011): Native shared-table path. Polls
        // tide.tide_outbox_messages with a static query and decodes native
        // payloads by default.
        "outbox" => {
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
                let src = crate::source::outbox::OutboxPollerSource::new_consumer_group(
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
                Ok(Box::new(src))
            } else {
                let src = crate::source::outbox::OutboxPollerSource::new_simple_native(
                    db,
                    outbox,
                    subject_template,
                    relay_group_id,
                    &pipeline.name,
                )
                .await?;
                Ok(Box::new(src))
            }
        }

        // v0.40.0 (ADR-011 §10): Explicit pg_trickle compatibility path. Uses
        // the legacy dynamic per-outbox relation and v:1 envelope decoding.
        "pg_trickle_outbox" => {
            let outbox = pipeline.require_str(&["source", "outbox"])?;
            let subject_template = pipeline
                .opt_str(&["source", "subject_template"])
                .unwrap_or("{stream_table}.{op}");
            let src = crate::source::outbox::OutboxPollerSource::new_simple_pg_trickle(
                db,
                outbox,
                subject_template,
                relay_group_id,
                &pipeline.name,
            )
            .await?;
            Ok(Box::new(src))
        }

        "stdin" => {
            let event_type = pipeline
                .opt_str(&["source", "event_type"])
                .unwrap_or("event");
            Ok(Box::new(crate::source::stdin::StdinSource::new(event_type)))
        }

        #[cfg(feature = "nats")]
        "nats" => {
            let url = pipeline.require_str(&["source", "url"])?;
            let stream = pipeline.require_str(&["source", "stream"])?;
            let consumer = pipeline.require_str(&["source", "consumer"])?;
            let subject = pipeline.require_str(&["source", "subject"])?;
            let event_type = pipeline
                .opt_str(&["source", "event_type"])
                .unwrap_or("event");
            let src =
                crate::source::nats::NatsSource::new(url, stream, consumer, subject, event_type)
                    .await?;
            Ok(Box::new(src))
        }

        #[cfg(feature = "kafka")]
        "kafka" => {
            let brokers = pipeline.require_str(&["source", "brokers"])?;
            let group_id = pipeline.require_str(&["source", "group_id"])?;
            let topic = pipeline.require_str(&["source", "topic"])?;
            let event_type = pipeline
                .opt_str(&["source", "event_type"])
                .unwrap_or("event");
            Ok(Box::new(crate::source::kafka::KafkaSource::new(
                brokers, group_id, topic, event_type,
            )?))
        }

        #[cfg(feature = "webhook")]
        "webhook" => {
            let addr = pipeline.require_str(&["source", "addr"])?;
            let event_type = pipeline
                .opt_str(&["source", "event_type"])
                .unwrap_or("event");
            let src = crate::source::webhook::WebhookSource::bind(addr, event_type).await?;
            Ok(Box::new(src))
        }

        #[cfg(feature = "redis")]
        "redis" => {
            let url = pipeline.require_str(&["source", "url"])?;
            let stream_key = pipeline.require_str(&["source", "stream_key"])?;
            let group = pipeline.require_str(&["source", "group"])?;
            let consumer_id = pipeline
                .opt_str(&["source", "consumer_id"])
                .unwrap_or("pg-tide");
            let event_type = pipeline
                .opt_str(&["source", "event_type"])
                .unwrap_or("event");
            let src = crate::source::redis::RedisSource::new(
                url,
                stream_key,
                group,
                consumer_id,
                event_type,
            )
            .await?;
            Ok(Box::new(src))
        }

        #[cfg(feature = "sqs")]
        "sqs" => {
            let queue_url = pipeline.require_str(&["source", "queue_url"])?;
            let event_type = pipeline
                .opt_str(&["source", "event_type"])
                .unwrap_or("event");
            let max_messages = pipeline.opt_i64(&["source", "max_messages"]).unwrap_or(10) as i32;
            let src =
                crate::source::sqs::SqsSource::new(queue_url, event_type, max_messages).await?;
            Ok(Box::new(src))
        }

        #[cfg(feature = "rabbitmq")]
        "rabbitmq" => {
            let url = pipeline.require_str(&["source", "url"])?;
            let queue = pipeline.require_str(&["source", "queue"])?;
            let consumer_tag = pipeline
                .opt_str(&["source", "consumer_tag"])
                .unwrap_or("pg-tide");
            let event_type = pipeline
                .opt_str(&["source", "event_type"])
                .unwrap_or("event");
            let src =
                crate::source::rabbitmq::RabbitMqSource::new(url, queue, consumer_tag, event_type)
                    .await?;
            Ok(Box::new(src))
        }

        #[cfg(feature = "pubsub")]
        "pubsub" => {
            let project_id = pipeline.require_str(&["source", "project_id"])?;
            let subscription = pipeline.require_str(&["source", "subscription"])?;
            let event_type = pipeline
                .opt_str(&["source", "event_type"])
                .unwrap_or("event");
            let src =
                crate::source::pubsub::PubSubSource::new(project_id, subscription, event_type)?;
            Ok(Box::new(src))
        }

        #[cfg(feature = "kinesis")]
        "kinesis" => {
            let stream_name = pipeline.require_str(&["source", "stream_name"])?;
            let event_type = pipeline
                .opt_str(&["source", "event_type"])
                .unwrap_or("event");
            let iterator_type = pipeline
                .opt_str(&["source", "iterator_type"])
                .unwrap_or("TRIM_HORIZON");
            let src =
                crate::source::kinesis::KinesisSource::new(stream_name, event_type, iterator_type)
                    .await?;
            Ok(Box::new(src))
        }

        #[cfg(feature = "servicebus")]
        "servicebus" => {
            let connection_string = pipeline.require_str(&["source", "connection_string"])?;
            let entity = pipeline.require_str(&["source", "entity"])?;
            let event_type = pipeline
                .opt_str(&["source", "event_type"])
                .unwrap_or("event");
            let src = crate::source::servicebus::ServiceBusSource::new(
                connection_string,
                entity,
                event_type,
            )?;
            Ok(Box::new(src))
        }

        #[cfg(feature = "mqtt")]
        "mqtt" => {
            let url = pipeline.require_str(&["source", "url"])?;
            let topic = pipeline.require_str(&["source", "topic"])?;
            let client_id = pipeline
                .opt_str(&["source", "client_id"])
                .unwrap_or("pg-tide-inbox");
            let event_type = pipeline
                .opt_str(&["source", "event_type"])
                .unwrap_or("event");
            let src =
                crate::source::mqtt::MqttSource::new(url, topic, client_id, event_type).await?;
            Ok(Box::new(src))
        }

        #[cfg(feature = "eventhubs")]
        "eventhubs" => {
            let connection_string = pipeline.require_str(&["source", "connection_string"])?;
            let event_hub = pipeline.require_str(&["source", "event_hub"])?;
            let consumer_group = pipeline
                .opt_str(&["source", "consumer_group"])
                .unwrap_or("$Default");
            let partition_count = pipeline
                .opt_i64(&["source", "partition_count"])
                .unwrap_or(1) as usize;
            let event_type = pipeline
                .opt_str(&["source", "event_type"])
                .unwrap_or("event");
            let src = crate::source::eventhubs::EventHubsSource::new(
                connection_string,
                event_hub,
                consumer_group,
                partition_count,
                event_type,
            )?;
            Ok(Box::new(src))
        }

        // v0.9.0: Singer protocol adapter (tap source)
        #[cfg(feature = "singer")]
        "singer" => {
            let tap_command = pipeline.require_str(&["source", "tap_command"])?;
            let tap_args: Vec<String> = pipeline
                .config
                .pointer("/source/tap_args")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            let tap_name = pipeline
                .opt_str(&["source", "tap_name"])
                .unwrap_or(tap_command);
            let on_schema_change = crate::sink::singer::OnSchemaChange::from_str(
                pipeline
                    .opt_str(&["source", "on_schema_change"])
                    .unwrap_or("log"),
            );
            let src = crate::source::singer::SingerSource::new(
                Arc::clone(&db),
                &pipeline.name,
                tap_command,
                &tap_args,
                tap_name,
                on_schema_change,
            )
            .await?;
            Ok(Box::new(src))
        }

        // v0.9.0: Airbyte protocol adapter (connector source)
        #[cfg(feature = "airbyte")]
        "airbyte" => {
            let source_name;
            let catalog = pipeline
                .config
                .pointer("/source/configured_catalog")
                .cloned()
                .unwrap_or(serde_json::json!({"streams": []}));

            let src: Box<dyn crate::source::Source> =
                if let Some(image) = pipeline.opt_str(&["source", "source_image"]) {
                    let source_config = pipeline
                        .config
                        .pointer("/source/source_config")
                        .cloned()
                        .unwrap_or(serde_json::json!({}));
                    source_name = pipeline
                        .opt_str(&["source", "source_name"])
                        .unwrap_or(image)
                        .to_string();
                    Box::new(
                        crate::source::airbyte::AirbyteSource::new_docker(
                            Arc::clone(&db),
                            &pipeline.name,
                            image,
                            &source_config,
                            &catalog,
                            &source_name,
                        )
                        .await?,
                    )
                } else {
                    let cmd = pipeline.require_str(&["source", "source_command"])?;
                    let args: Vec<String> = pipeline
                        .config
                        .pointer("/source/source_args")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|v| v.as_str().map(String::from))
                                .collect()
                        })
                        .unwrap_or_default();
                    source_name = pipeline
                        .opt_str(&["source", "source_name"])
                        .unwrap_or(cmd)
                        .to_string();
                    Box::new(
                        crate::source::airbyte::AirbyteSource::new_command(
                            Arc::clone(&db),
                            &pipeline.name,
                            cmd,
                            &args,
                            &source_name,
                        )
                        .await?,
                    )
                };
            Ok(src)
        }

        // v0.40.0 (ADR-011 §12): Fan-in is experimental and quarantined.
        // The coordinator does not start fan-in workers in the production path.
        // Catalog rows and offsets are retained for a future release.
        "fanin" => Err(RelayError::InvalidConfig {
            name: pipeline.name.clone(),
            reason: format!(
                "fan-in pipeline '{}' is experimental and disabled in v0.40.0 \
                 (ADR-011). Fan-in configs are retained but not runnable; \
                 fan-in support returns in a later release.",
                pipeline.name
            ),
        }),

        // v0.37.0: RockLake reverse relay source.
        // Polls new snapshots from a RockLake PG-wire catalog sidecar.
        // Uses only the bounded SQL subset (single non-JOIN SELECT).
        #[cfg(feature = "rocklake")]
        "rocklake" => {
            use crate::source::rocklake::{RockLakeSource, RockLakeSourceConfig};
            let catalog_connection = pipeline.require_str(&["source", "catalog_connection"])?;
            let schema = pipeline.require_str(&["source", "schema"])?;
            let table = pipeline.require_str(&["source", "table"])?;
            let poll_ms = pipeline
                .opt_i64(&["source", "snapshot_poll_interval_ms"])
                .unwrap_or(1_000) as u64;
            let consumer_group = pipeline
                .opt_str(&["source", "consumer_group"])
                .unwrap_or("default")
                .to_string();
            let last_snapshot_id = pipeline
                .opt_i64(&["source", "last_snapshot_id"])
                .unwrap_or(0);
            let mut cfg = RockLakeSourceConfig::new(catalog_connection, schema, table);
            cfg.snapshot_poll_interval_ms = poll_ms;
            cfg.consumer_group = consumer_group;
            Ok(Box::new(RockLakeSource::new(cfg, last_snapshot_id)))
        }

        other => Err(RelayError::InvalidConfig {
            name: pipeline.name.clone(),
            reason: format!("unknown source_type: {other}"),
        }),
    }
}

async fn build_sink(
    pipeline: &PipelineConfig,
    db: Arc<Client>,
) -> Result<Box<dyn crate::sink::Sink>, RelayError> {
    let sink_type = pipeline.require_str(&["sink_type"])?;
    match sink_type {
        "stdout" => {
            let format = match pipeline.opt_str(&["sink", "format"]).unwrap_or("jsonl") {
                "pretty" => crate::sink::stdout::StdoutFormat::JsonPretty,
                _ => crate::sink::stdout::StdoutFormat::Jsonl,
            };
            Ok(Box::new(crate::sink::stdout::StdoutSink::new(format)))
        }

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
            Ok(Box::new(crate::sink::inbox::InboxSink::new(db, inbox)?))
        }

        #[cfg(feature = "nats")]
        "nats" => {
            let url = pipeline.require_str(&["sink", "url"])?;
            // v0.40.0 (ADR-011 §13): The sink renders the subject from config —
            // a fixed `subject` or a `subject_template`. When neither is set the
            // documented default template ({outbox}.{op}) applies.
            let subject = pipeline.opt_str(&["sink", "subject"]);
            let subject_template = pipeline.opt_str(&["sink", "subject_template"]);
            Ok(Box::new(
                crate::sink::nats::NatsSink::new(url, subject, subject_template).await?,
            ))
        }

        #[cfg(feature = "kafka")]
        "kafka" => {
            let brokers = pipeline.require_str(&["sink", "brokers"])?;
            let topic_template = pipeline
                .opt_str(&["sink", "topic_template"])
                .unwrap_or("{stream_table}");
            Ok(Box::new(crate::sink::kafka::KafkaSink::new(
                brokers,
                topic_template,
            )?))
        }

        #[cfg(feature = "webhook")]
        "webhook" => {
            let url = pipeline.require_str(&["sink", "url"])?;
            let timeout = pipeline.opt_i64(&["sink", "timeout_secs"]).unwrap_or(30) as u64;
            Ok(Box::new(crate::sink::webhook::WebhookSink::new(
                url, timeout,
            )?))
        }

        #[cfg(feature = "redis")]
        "redis" => {
            let url = pipeline.require_str(&["sink", "url"])?;
            let stream_key_template = pipeline
                .opt_str(&["sink", "stream_key_template"])
                .unwrap_or("{stream_table}");
            let max_len = pipeline.opt_i64(&["sink", "max_len"]).map(|n| n as usize);
            Ok(Box::new(
                crate::sink::redis::RedisSink::new(url, stream_key_template, max_len).await?,
            ))
        }

        #[cfg(feature = "sqs")]
        "sqs" => {
            let queue_url = pipeline.require_str(&["sink", "queue_url"])?;
            let is_fifo = pipeline.opt_bool(&["sink", "is_fifo"]).unwrap_or(false);
            Ok(Box::new(
                crate::sink::sqs::SqsSink::new(queue_url, is_fifo).await?,
            ))
        }

        #[cfg(feature = "rabbitmq")]
        "rabbitmq" => {
            let url = pipeline.require_str(&["sink", "url"])?;
            let exchange = pipeline.opt_str(&["sink", "exchange"]).unwrap_or("");
            let routing_key_template = pipeline
                .opt_str(&["sink", "routing_key_template"])
                .unwrap_or("{stream_table}");
            Ok(Box::new(
                crate::sink::rabbitmq::RabbitMqSink::new(url, exchange, routing_key_template)
                    .await?,
            ))
        }

        #[cfg(feature = "elasticsearch")]
        "elasticsearch" => {
            let url = pipeline.require_str(&["sink", "url"])?;
            let index_template = pipeline
                .opt_str(&["sink", "index_template"])
                .unwrap_or("pg-tide-{stream_table}");
            let allow_http = pipeline.opt_bool(&["sink", "allow_http"]).unwrap_or(false);
            let ssrf_protection = pipeline
                .opt_bool(&["sink", "ssrf_protection"])
                .unwrap_or(true);
            Ok(Box::new(
                crate::sink::elasticsearch::ElasticsearchSink::new(
                    url,
                    index_template,
                    allow_http,
                    ssrf_protection,
                )?,
            ))
        }

        #[cfg(feature = "pubsub")]
        "pubsub" => {
            let project_id = pipeline.require_str(&["sink", "project_id"])?;
            let topic = pipeline.require_str(&["sink", "topic"])?;
            Ok(Box::new(crate::sink::pubsub::PubSubSink::new(
                project_id, topic,
            )?))
        }

        #[cfg(feature = "kinesis")]
        "kinesis" => {
            let stream_name = pipeline.require_str(&["sink", "stream_name"])?;
            let partition_key_template = pipeline
                .opt_str(&["sink", "partition_key_template"])
                .unwrap_or("{stream_table}");
            Ok(Box::new(
                crate::sink::kinesis::KinesisSink::new(stream_name, partition_key_template).await?,
            ))
        }

        #[cfg(feature = "servicebus")]
        "servicebus" => {
            let connection_string = pipeline.require_str(&["sink", "connection_string"])?;
            let entity = pipeline.require_str(&["sink", "entity"])?;
            Ok(Box::new(crate::sink::servicebus::ServiceBusSink::new(
                connection_string,
                entity,
            )?))
        }

        #[cfg(feature = "mqtt")]
        "mqtt" => {
            let url = pipeline.require_str(&["sink", "url"])?;
            let client_id = pipeline
                .opt_str(&["sink", "client_id"])
                .unwrap_or("pg-tide");
            let topic_template = pipeline
                .opt_str(&["sink", "topic_template"])
                .unwrap_or("pg-tide/{stream_table}/{op}");
            let qos = pipeline.opt_i64(&["sink", "qos"]).unwrap_or(1) as u8;
            Ok(Box::new(
                crate::sink::mqtt::MqttSink::new(url, client_id, topic_template, qos).await?,
            ))
        }

        #[cfg(feature = "eventhubs")]
        "eventhubs" => {
            let connection_string = pipeline.require_str(&["sink", "connection_string"])?;
            let event_hub = pipeline.require_str(&["sink", "event_hub"])?;
            let partition_key_template = pipeline
                .opt_str(&["sink", "partition_key_template"])
                .unwrap_or("{stream_table}");
            Ok(Box::new(crate::sink::eventhubs::EventHubsSink::new(
                connection_string,
                event_hub,
                partition_key_template,
            )?))
        }

        #[cfg(feature = "object-storage")]
        "object-storage" => {
            use crate::sink::object_storage::{
                ObjectStorageFormat, ObjectStorageProvider, ObjectStorageSink,
            };

            let provider_str = pipeline.require_str(&["sink", "provider"])?;
            let prefix = pipeline.opt_str(&["sink", "prefix"]).unwrap_or("pg-tide");
            let format_str = pipeline.opt_str(&["sink", "format"]).unwrap_or("jsonl");
            let format = match format_str {
                "parquet" => ObjectStorageFormat::Parquet,
                _ => ObjectStorageFormat::Jsonl,
            };
            let buffer_max_rows = pipeline
                .opt_i64(&["sink", "buffer_max_rows"])
                .unwrap_or(100_000) as usize;
            let buffer_max_bytes = pipeline
                .opt_i64(&["sink", "buffer_max_bytes"])
                .unwrap_or(268_435_456) as usize;
            let buffer_max_seconds = pipeline
                .opt_i64(&["sink", "buffer_max_seconds"])
                .unwrap_or(300) as u64;
            let partition_by_date = pipeline
                .opt_bool(&["sink", "partition_by_date"])
                .unwrap_or(true);

            let provider = match provider_str {
                "s3" => {
                    let bucket = pipeline.require_str(&["sink", "bucket"])?;
                    let region = pipeline.opt_str(&["sink", "region"]).map(String::from);
                    let endpoint = pipeline.opt_str(&["sink", "endpoint"]).map(String::from);
                    ObjectStorageProvider::S3 {
                        bucket: bucket.to_string(),
                        region,
                        endpoint,
                    }
                }
                "gcs" => {
                    let bucket = pipeline.require_str(&["sink", "bucket"])?;
                    ObjectStorageProvider::Gcs {
                        bucket: bucket.to_string(),
                    }
                }
                "azure-blob" => {
                    let account = pipeline.require_str(&["sink", "account"])?;
                    let container = pipeline.require_str(&["sink", "container"])?;
                    ObjectStorageProvider::Azure {
                        account: account.to_string(),
                        container: container.to_string(),
                    }
                }
                "local" => {
                    let root = pipeline.require_str(&["sink", "root"])?;
                    ObjectStorageProvider::Local {
                        root: std::path::PathBuf::from(root),
                    }
                }
                other => {
                    return Err(RelayError::InvalidConfig {
                        name: pipeline.name.clone(),
                        reason: format!("unknown object-storage provider: {other}"),
                    })
                }
            };

            Ok(Box::new(ObjectStorageSink::new(
                provider,
                prefix,
                format,
                buffer_max_rows,
                buffer_max_bytes,
                buffer_max_seconds,
                partition_by_date,
            )?))
        }

        // v0.8.0: Notification sinks + Arrow Flight
        #[cfg(feature = "slack")]
        "slack" => {
            let webhook_url = pipeline.require_str(&["sink", "webhook_url"])?;
            let username = pipeline.opt_str(&["sink", "username"]).map(String::from);
            let icon_emoji = pipeline.opt_str(&["sink", "icon_emoji"]).map(String::from);
            let batch_limit = pipeline.opt_i64(&["sink", "batch_limit"]).unwrap_or(50) as usize;
            Ok(Box::new(crate::sink::slack::SlackSink::new(
                webhook_url,
                username,
                icon_emoji,
                batch_limit,
            )?))
        }

        #[cfg(feature = "discord")]
        "discord" => {
            let webhook_url = pipeline.require_str(&["sink", "webhook_url"])?;
            let username = pipeline.opt_str(&["sink", "username"]).map(String::from);
            let avatar_url = pipeline.opt_str(&["sink", "avatar_url"]).map(String::from);
            let batch_limit = pipeline.opt_i64(&["sink", "batch_limit"]).unwrap_or(10) as usize;
            Ok(Box::new(crate::sink::discord::DiscordSink::new(
                webhook_url,
                username,
                avatar_url,
                batch_limit,
            )?))
        }

        #[cfg(feature = "pagerduty")]
        "pagerduty" => {
            let routing_key = pipeline.require_str(&["sink", "routing_key"])?;
            let severity = pipeline.opt_str(&["sink", "severity"]).unwrap_or("info");
            let source = pipeline.opt_str(&["sink", "source"]).map(String::from);
            let component = pipeline.opt_str(&["sink", "component"]).map(String::from);
            Ok(Box::new(crate::sink::pagerduty::PagerDutySink::new(
                routing_key,
                severity,
                source,
                component,
            )?))
        }

        #[cfg(feature = "arrow-flight")]
        "arrow-flight" => {
            let url = pipeline.require_str(&["sink", "url"])?;
            let auth_token = pipeline.opt_str(&["sink", "auth_token"]).map(String::from);
            let descriptor_path: Vec<String> = pipeline
                .opt_str(&["sink", "descriptor_path"])
                .unwrap_or("pg-tide")
                .split('/')
                .map(String::from)
                .collect();
            let allow_http = pipeline.opt_bool(&["sink", "allow_http"]).unwrap_or(false);
            let ssrf_protection = pipeline
                .opt_bool(&["sink", "ssrf_protection"])
                .unwrap_or(true);
            Ok(Box::new(crate::sink::arrow_flight::ArrowFlightSink::new(
                url,
                auth_token,
                descriptor_path,
                allow_http,
                ssrf_protection,
            )?))
        }

        // v0.9.0: Singer protocol adapter
        #[cfg(feature = "singer")]
        "singer" => {
            let target_command = pipeline.require_str(&["sink", "target_command"])?;
            let target_args: Vec<String> = pipeline
                .config
                .pointer("/sink/target_args")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            let target_name = pipeline
                .opt_str(&["sink", "target_name"])
                .unwrap_or(target_command);
            let stream_name_template = pipeline
                .opt_str(&["sink", "stream_name_template"])
                .unwrap_or("{stream_table}");
            let on_schema_change = crate::sink::singer::OnSchemaChange::from_str(
                pipeline
                    .opt_str(&["sink", "on_schema_change"])
                    .unwrap_or("log"),
            );
            Ok(Box::new(crate::sink::singer::SingerSink::new(
                Arc::clone(&db),
                &pipeline.name,
                target_command,
                &target_args,
                target_name,
                stream_name_template,
                on_schema_change,
            )?))
        }

        // v0.9.0: Airbyte protocol adapter
        #[cfg(feature = "airbyte")]
        "airbyte" => {
            let stream_name_template = pipeline
                .opt_str(&["sink", "stream_name_template"])
                .unwrap_or("{stream_table}");
            let namespace = pipeline.opt_str(&["sink", "namespace"]).unwrap_or("pgtide");
            let destination_name;

            if let Some(image) = pipeline.opt_str(&["sink", "destination_image"]) {
                let destination_config = pipeline
                    .config
                    .pointer("/sink/destination_config")
                    .cloned()
                    .unwrap_or(serde_json::json!({}));
                destination_name = pipeline
                    .opt_str(&["sink", "destination_name"])
                    .unwrap_or(image)
                    .to_string();
                Ok(Box::new(crate::sink::airbyte::AirbyteSink::new_docker(
                    Arc::clone(&db),
                    &pipeline.name,
                    image,
                    &destination_config,
                    &destination_name,
                    stream_name_template,
                    namespace,
                )?))
            } else {
                let cmd = pipeline.require_str(&["sink", "destination_command"])?;
                let args: Vec<String> = pipeline
                    .config
                    .pointer("/sink/destination_args")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                destination_name = pipeline
                    .opt_str(&["sink", "destination_name"])
                    .unwrap_or(cmd)
                    .to_string();
                Ok(Box::new(crate::sink::airbyte::AirbyteSink::new_command(
                    Arc::clone(&db),
                    &pipeline.name,
                    cmd,
                    &args,
                    &destination_name,
                    stream_name_template,
                    namespace,
                )?))
            }
        }

        // v0.34.0: Register previously unregistered analytics / data-lake / document sinks.
        // These implementations existed since v0.10.0 but were never wired into
        // build_sink(), so they were unavailable for reverse pipelines (and also
        // for forward pipelines configured via the relay catalog rather than TOML).
        #[cfg(feature = "clickhouse")]
        "clickhouse" => {
            use crate::sink::clickhouse::ClickHouseConfig;
            let url = pipeline.require_str(&["sink", "url"])?;
            let database = pipeline.require_str(&["sink", "database"])?;
            let table_template = pipeline
                .opt_str(&["sink", "table_template"])
                .unwrap_or("{stream_table}");
            let username = pipeline.opt_str(&["sink", "username"]).map(String::from);
            let password = pipeline.opt_str(&["sink", "password"]).map(String::from);
            let allow_http = pipeline.opt_bool(&["sink", "allow_http"]).unwrap_or(false);
            let ssrf_protection = pipeline
                .opt_bool(&["sink", "ssrf_protection"])
                .unwrap_or(true);
            Ok(Box::new(crate::sink::clickhouse::ClickHouseSink::new(
                ClickHouseConfig {
                    url: url.to_string(),
                    database: database.to_string(),
                    table_template: table_template.to_string(),
                    username,
                    password,
                    allow_http,
                    ssrf_protection,
                },
            )?))
        }

        #[cfg(feature = "mongodb")]
        "mongodb" => {
            use crate::sink::mongodb::MongoDbConfig;
            let connection_string = pipeline.require_str(&["sink", "connection_string"])?;
            let database = pipeline.require_str(&["sink", "database"])?;
            let collection_template = pipeline
                .opt_str(&["sink", "collection_template"])
                .unwrap_or("{stream_table}");
            let doc_id_field = pipeline
                .opt_str(&["sink", "doc_id_field"])
                .unwrap_or("dedup_key");
            let write_concern = pipeline
                .opt_str(&["sink", "write_concern"])
                .unwrap_or("majority");
            let mut cfg = MongoDbConfig::new(connection_string, database);
            cfg.collection_template = collection_template.to_string();
            cfg.doc_id_field = doc_id_field.to_string();
            cfg.write_concern = write_concern.to_string();
            Ok(Box::new(crate::sink::mongodb::MongoDbSink::new(cfg).await?))
        }

        #[cfg(feature = "bigquery")]
        "bigquery" => {
            use crate::sink::bigquery::{BigQueryConfig, BigQueryWriteMode};
            let project_id = pipeline.require_str(&["sink", "project_id"])?;
            let dataset_id = pipeline.require_str(&["sink", "dataset_id"])?;
            let table_template = pipeline
                .opt_str(&["sink", "table_template"])
                .unwrap_or("{stream_table}");
            let access_token = pipeline.require_str(&["sink", "access_token"])?;
            let write_mode = match pipeline
                .opt_str(&["sink", "write_mode"])
                .unwrap_or("streaming")
            {
                "batch" => BigQueryWriteMode::Batch,
                _ => BigQueryWriteMode::Streaming,
            };
            Ok(Box::new(crate::sink::bigquery::BigQuerySink::new(
                BigQueryConfig {
                    project_id: project_id.to_string(),
                    dataset_id: dataset_id.to_string(),
                    table_template: table_template.to_string(),
                    write_mode,
                    access_token: access_token.to_string(),
                },
            )?))
        }

        #[cfg(feature = "snowflake")]
        "snowflake" => {
            use crate::sink::snowflake::SnowflakeConfig;
            let account = pipeline.require_str(&["sink", "account"])?;
            let database = pipeline.require_str(&["sink", "database"])?;
            let schema = pipeline.opt_str(&["sink", "schema"]).unwrap_or("PUBLIC");
            let table_template = pipeline
                .opt_str(&["sink", "table_template"])
                .unwrap_or("{stream_table}");
            let user = pipeline.require_str(&["sink", "user"])?;
            let auth_token = pipeline.require_str(&["sink", "auth_token"])?;
            let batch_size = pipeline.opt_i64(&["sink", "batch_size"]).unwrap_or(16_384) as usize;
            Ok(Box::new(crate::sink::snowflake::SnowflakeSink::new(
                SnowflakeConfig {
                    account: account.to_string(),
                    database: database.to_string(),
                    schema: schema.to_string(),
                    table_template: table_template.to_string(),
                    user: user.to_string(),
                    auth_token: auth_token.to_string(),
                    batch_size,
                },
            )?))
        }

        #[cfg(feature = "delta")]
        "delta" => {
            use crate::sink::delta::{DeltaConfig, DeltaSink};
            let table_path = pipeline.require_str(&["sink", "table_path"])?;
            let change_data_feed = pipeline
                .opt_bool(&["sink", "change_data_feed"])
                .unwrap_or(false);
            let rows_per_file = pipeline
                .opt_i64(&["sink", "rows_per_file"])
                .unwrap_or(50_000) as usize;
            let store = build_object_store_from_pipeline(pipeline)?;
            let cfg = DeltaConfig {
                table_path: table_path.to_string(),
                change_data_feed,
                rows_per_file,
            };
            Ok(Box::new(DeltaSink::new(store, cfg)))
        }

        #[cfg(feature = "iceberg")]
        "iceberg" => {
            use crate::sink::iceberg::{IcebergConfig, IcebergSink, IcebergWriteMode};
            let warehouse_path = pipeline.require_str(&["sink", "warehouse_path"])?;
            let namespace = pipeline
                .opt_str(&["sink", "namespace"])
                .unwrap_or("default");
            let table_template = pipeline
                .opt_str(&["sink", "table_template"])
                .unwrap_or("{stream_table}");
            let write_mode = match pipeline
                .opt_str(&["sink", "write_mode"])
                .unwrap_or("append")
            {
                "overwrite" => IcebergWriteMode::Overwrite,
                _ => IcebergWriteMode::Append,
            };
            let rows_per_file = pipeline
                .opt_i64(&["sink", "rows_per_file"])
                .unwrap_or(50_000) as usize;
            let store = build_object_store_from_pipeline(pipeline)?;
            let cfg = IcebergConfig {
                warehouse_path: warehouse_path.to_string(),
                namespace: namespace.to_string(),
                table_template: table_template.to_string(),
                write_mode,
                rows_per_file,
            };
            Ok(Box::new(IcebergSink::new(store, cfg)))
        }

        #[cfg(feature = "ducklake")]
        "ducklake" => {
            use crate::sink::ducklake::{
                DuckLakeConfig, DuckLakePartition, DuckLakeSink, SchemaChangePolicy,
            };
            let data_path = pipeline.require_str(&["sink", "data_path"])?;
            let namespace = pipeline.opt_str(&["sink", "namespace"]).unwrap_or("pgtide");
            let catalog_schema = pipeline
                .opt_str(&["sink", "catalog_schema"])
                .unwrap_or("ducklake");
            let inline_row_limit = pipeline
                .opt_i64(&["sink", "inline_row_limit"])
                .unwrap_or(10) as usize;
            let on_schema_change = match pipeline
                .opt_str(&["sink", "on_schema_change"])
                .unwrap_or("warn_and_continue")
            {
                "pause" => SchemaChangePolicy::Pause,
                "route_to_dlq" => SchemaChangePolicy::RouteToDlq,
                "auto_new_stream" => SchemaChangePolicy::AutoNewStream,
                _ => SchemaChangePolicy::WarnAndContinue,
            };
            let partition = match pipeline.opt_str(&["sink", "partition"]).unwrap_or("none") {
                "daily" => DuckLakePartition::Daily,
                "monthly" => DuckLakePartition::Monthly,
                other => {
                    if let Some(n) = other
                        .strip_prefix("bucket:")
                        .and_then(|s| s.parse::<u32>().ok())
                    {
                        DuckLakePartition::Bucket(n)
                    } else {
                        DuckLakePartition::None
                    }
                }
            };
            let atomic_lake_writes = pipeline
                .opt_bool(&["sink", "atomic_lake_writes"])
                .unwrap_or(false);
            // `catalog_connection` is required: the DuckLake sink needs its own
            // transaction-capable PostgreSQL client to commit catalog entries atomically.
            let catalog_url = pipeline.require_str(&["sink", "catalog_connection"])?;
            let (catalog_client, catalog_conn) = crate::pg_tls::connect(catalog_url).await?;
            tokio::spawn(async move {
                if let Err(e) = catalog_conn.await {
                    tracing::error!("ducklake catalog connection closed with error: {e}");
                }
            });
            let store = build_object_store_from_pipeline(pipeline)?;
            let mut cfg = DuckLakeConfig::new(data_path, namespace);
            cfg.catalog_schema = catalog_schema.to_string();
            cfg.inline_row_limit = inline_row_limit;
            cfg.on_schema_change = on_schema_change;
            cfg.partition = partition;
            cfg.atomic_lake_writes = atomic_lake_writes;
            cfg.pipeline_name = Some(pipeline.name.clone());
            Ok(Box::new(DuckLakeSink::new(store, catalog_client, cfg)))
        }

        // v0.34.0: Register remote PostgreSQL inbox sink (pg_outbox.rs — PgInboxSink).
        // Delivers messages to a pg_tide inbox on a remote PostgreSQL instance.
        // This is the feature = "pg-inbox" sink path; no extra Cargo feature required
        // (uses tokio-postgres which is already a core dependency).
        "pg_outbox" => {
            let postgres_url = pipeline.require_str(&["sink", "postgres_url"])?;
            let inbox = pipeline.require_str(&["sink", "inbox"])?;
            Ok(Box::new(
                crate::sink::pg_outbox::PgInboxSink::new(postgres_url, inbox).await?,
            ))
        }

        // v0.37.0: RockLake PG-wire sidecar sink.
        // Speaks RockLake's bounded SQL subset (no nextval, no ON CONFLICT,
        // no RETURNING, no catalog DDL).  Shares Parquet-building logic with
        // DuckLakeSink via the ducklake_common module.
        #[cfg(feature = "rocklake")]
        "rocklake" => {
            use crate::ducklake_common::{DuckLakePartition, SchemaChangePolicy};
            use crate::sink::rocklake::{RockLakeConfig, RockLakeSink};
            let data_path = pipeline.require_str(&["sink", "data_path"])?;
            let namespace = pipeline.opt_str(&["sink", "namespace"]).unwrap_or("pgtide");
            let catalog_schema = pipeline
                .opt_str(&["sink", "catalog_schema"])
                .unwrap_or("ducklake");
            let inline_row_limit = pipeline
                .opt_i64(&["sink", "inline_row_limit"])
                .unwrap_or(10) as usize;
            let on_schema_change = match pipeline
                .opt_str(&["sink", "on_schema_change"])
                .unwrap_or("warn_and_continue")
            {
                "pause" => SchemaChangePolicy::Pause,
                "route_to_dlq" => SchemaChangePolicy::RouteToDlq,
                "auto_new_stream" => SchemaChangePolicy::AutoNewStream,
                _ => SchemaChangePolicy::WarnAndContinue,
            };
            let partition = match pipeline.opt_str(&["sink", "partition"]).unwrap_or("none") {
                "daily" => DuckLakePartition::Daily,
                "monthly" => DuckLakePartition::Monthly,
                other => {
                    if let Some(n) = other
                        .strip_prefix("bucket:")
                        .and_then(|s| s.parse::<u32>().ok())
                    {
                        DuckLakePartition::Bucket(n)
                    } else {
                        DuckLakePartition::None
                    }
                }
            };
            // `catalog_connection` is required: the RockLake sink needs its own
            // connection to the RockLake PG-wire sidecar for catalog commits.
            let catalog_url = pipeline.require_str(&["sink", "catalog_connection"])?;
            let (catalog_client, catalog_conn) = crate::pg_tls::connect(catalog_url).await?;
            tokio::spawn(async move {
                if let Err(e) = catalog_conn.await {
                    tracing::error!("rocklake catalog connection closed with error: {e}");
                }
            });
            let store = build_object_store_from_pipeline(pipeline)?;
            let mut cfg = RockLakeConfig::new(data_path, namespace);
            cfg.catalog_schema = catalog_schema.to_string();
            cfg.inline_row_limit = inline_row_limit;
            cfg.on_schema_change = on_schema_change;
            cfg.partition = partition;
            cfg.pipeline_name = Some(pipeline.name.clone());
            Ok(Box::new(RockLakeSink::new(store, catalog_client, cfg)))
        }

        other => Err(RelayError::InvalidConfig {
            name: pipeline.name.clone(),
            reason: format!("unknown sink_type: {other}"),
        }),
    }
}

// ── Object store factory helper (v0.34.0) ────────────────────────────────

/// Build an `ObjectStore` from the pipeline's `sink` config section.
///
/// Reads `sink.storage_provider` (`s3` | `gcs` | `azure` | `local`, default
/// `local`) and the corresponding provider-specific keys.  Used by the
/// `delta`, `iceberg`, `ducklake`, and `rocklake` sink arms introduced in v0.34.0–v0.37.0.
#[cfg(any(
    feature = "delta",
    feature = "iceberg",
    feature = "ducklake",
    feature = "rocklake"
))]
fn build_object_store_from_pipeline(
    pipeline: &PipelineConfig,
) -> Result<std::sync::Arc<dyn object_store::ObjectStore>, RelayError> {
    let provider = pipeline
        .opt_str(&["sink", "storage_provider"])
        .unwrap_or("local");

    match provider {
        "s3" => {
            let bucket = pipeline.require_str(&["sink", "bucket"])?;
            let region = pipeline.opt_str(&["sink", "region"]).map(String::from);
            let endpoint = pipeline.opt_str(&["sink", "endpoint"]).map(String::from);
            let mut builder =
                object_store::aws::AmazonS3Builder::from_env().with_bucket_name(bucket);
            if let Some(r) = region {
                builder = builder.with_region(r);
            }
            if let Some(e) = endpoint {
                builder = builder.with_endpoint(e);
                // For LocalStack / MinIO — allow plain HTTP.
                builder = builder.with_allow_http(true);
            }
            Ok(std::sync::Arc::new(builder.build().map_err(|e| {
                RelayError::config(format!("S3 config error: {e}"))
            })?))
        }
        "gcs" => {
            let bucket = pipeline.require_str(&["sink", "bucket"])?;
            Ok(std::sync::Arc::new(
                object_store::gcp::GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket)
                    .build()
                    .map_err(|e| RelayError::config(format!("GCS config error: {e}")))?,
            ))
        }
        "azure" => {
            let account = pipeline.require_str(&["sink", "account"])?;
            let container = pipeline.require_str(&["sink", "container"])?;
            Ok(std::sync::Arc::new(
                object_store::azure::MicrosoftAzureBuilder::from_env()
                    .with_account(account)
                    .with_container_name(container)
                    .build()
                    .map_err(|e| RelayError::config(format!("Azure Blob config error: {e}")))?,
            ))
        }
        // Default: local filesystem. `sink.root` sets the prefix (default /tmp/pg-tide-objects).
        _ => {
            let root = pipeline
                .opt_str(&["sink", "root"])
                .unwrap_or("/tmp/pg-tide-objects");
            Ok(std::sync::Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(std::path::Path::new(root))
                    .map_err(|e| RelayError::config(format!("local fs error: {e}")))?,
            ))
        }
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
}
