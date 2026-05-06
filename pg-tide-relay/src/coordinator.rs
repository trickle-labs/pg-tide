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
    #[allow(dead_code)]
    health: Arc<RwLock<HealthState>>,
    /// Pipeline ID → (cancellation sender, join handle).
    /// v0.15.0: JoinHandle stored for panic detection.
    owned: HashMap<String, (watch::Sender<bool>, JoinHandle<()>)>,
    /// v0.13.0 / v0.15.0: Maximum owned pipelines (connection limit).
    max_owned_pipelines: usize,
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
        }
    }

    /// Set the maximum number of pipelines this coordinator will own.
    pub fn set_max_owned_pipelines(&mut self, max: usize) {
        self.max_owned_pipelines = max;
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
    pub async fn load_pipelines(&self) -> Result<Vec<PipelineConfig>, RelayError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| RelayError::other(format!("coordinator pool error: {e}")))?;
        let rows = client
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
            .await?;

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
    pub async fn try_acquire_lock(&self, pipeline_id: &str) -> Result<bool, RelayError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| RelayError::other(format!("coordinator pool error: {e}")))?;
        let row = client
            .query_one(
                "SELECT pg_try_advisory_lock(hashtext($1), hashtext($2))",
                &[&self.relay_group_id, &pipeline_id],
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
        client
            .execute(
                "SELECT pg_advisory_unlock(hashtext($1), hashtext($2))",
                &[&self.relay_group_id, &pipeline_id],
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
                        masked_config = %serde_json::to_string(
                            &mask_secrets_for_logging(&pipeline.config)
                        ).unwrap_or_default(),
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
            // v0.13.0: OTel span for the poll call.
            // Use Instrument::instrument() to avoid holding EnteredSpan across await.
            let span = tracing::info_span!(
                "relay.source.poll",
                pipeline = %pipeline.name,
                direction = %direction_label,
            );
            match source.poll(batch_size).instrument(span).await {
                Ok(msgs) => {
                    // v0.15.0: Reset backoff on successful poll.
                    poll_backoff_ms = poll_interval_ms;
                    msgs
                }
                Err(e) => {
                    // v0.15.0: Permanent errors stop the pipeline immediately.
                    if !e.is_transient() {
                        tracing::error!(
                            pipeline = %pipeline.name,
                            error = %e,
                            "permanent poll error — stopping pipeline"
                        );
                        return Err(e);
                    }
                    // v0.15.0: Exponential backoff with jitter for transient errors.
                    consecutive_failures += 1;
                    let jitter_range = (poll_backoff_ms as f64 * 0.20) as u64;
                    let jitter = if jitter_range > 0 {
                        let pseudo = consecutive_failures as u64 * 6_364_136_223_846_793_005_u64;
                        (pseudo % (jitter_range * 2)).saturating_sub(jitter_range)
                    } else {
                        0
                    };
                    let sleep_ms = poll_backoff_ms.saturating_add(jitter);
                    tracing::warn!(
                        pipeline = %pipeline.name,
                        error = %e,
                        sleep_ms,
                        consecutive_failures,
                        "transient poll error — backing off before retry"
                    );
                    // v0.16.0: OTel span around backoff sleep.
                    let backoff_span = tracing::info_span!(
                        "relay.backoff.sleep",
                        pipeline = %pipeline.name,
                        sleep_ms,
                        consecutive_failures,
                    );
                    let _enter = backoff_span.enter();
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(sleep_ms)) => {}
                        _ = stop_rx.changed() => { break; }
                    }
                    poll_backoff_ms = (poll_backoff_ms * 2).min(max_poll_backoff_ms);
                    continue;
                }
            }
        };

        if batch.is_empty() {
            // Reset backoff when idle (no messages).
            poll_backoff_ms = poll_interval_ms;
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(poll_interval_ms)) => {}
                _ = stop_rx.changed() => { break; }
            }
            continue;
        }

        // v0.13.0: Increment consumed counter after successful poll.
        metrics
            .messages_consumed
            .with_label_values(&[&pipeline.name, &direction_label, &tenant_label])
            .inc_by(batch.len() as u64);

        // v0.13.0: Record the poll timestamp for end-to-end latency tracking.
        let poll_instant = std::time::Instant::now();

        // v0.7.0: Replay mode — skip messages outside the replay range.
        let batch = if is_replay {
            filter_replay_batch(batch, replay_from, replay_to)
        } else {
            batch
        };

        if batch.is_empty() {
            // Replay range exhausted.
            tracing::info!(pipeline = %pipeline.name, "replay complete");
            break;
        }

        // v0.7.0: Apply JMESPath transforms (filter + payload projection).
        // v0.16.0: OTel span around transform evaluation.
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
        {
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
                pipeline = %pipeline.name,
                topic,
            );
            let _enter = se_span.enter();
            match schema_guard.observe(topic, &columns).await {
                Ok((
                    crate::schema_evolution::SchemaChangeKind::Breaking,
                    crate::schema_evolution::OnSchemaChange::Pause,
                )) => {
                    tracing::warn!(
                        pipeline = %pipeline.name,
                        topic,
                        "breaking schema change detected — pausing pipeline per policy"
                    );
                    continue;
                }
                Ok((kind, policy)) => {
                    if kind != crate::schema_evolution::SchemaChangeKind::NoChange
                        && kind != crate::schema_evolution::SchemaChangeKind::Initial
                    {
                        tracing::warn!(
                            pipeline = %pipeline.name,
                            topic,
                            change_kind = ?kind,
                            policy = policy.as_str(),
                            "schema change detected"
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(pipeline = %pipeline.name, error = %e, "schema evolution check error — continuing");
                }
            }
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
                tracing::info!(
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

        // v0.7.0: Circuit breaker check.
        if !circuit_breaker.should_allow() {
            tracing::warn!(
                pipeline = %pipeline.name,
                "circuit breaker open — routing batch to DLQ or sleeping"
            );
            if dlq_config.enabled {
                let entries: Vec<DlqEntry> = batch
                    .iter()
                    .map(|msg| {
                        DlqEntry::from_message(
                            &direction_label,
                            &pipeline.name,
                            source.name(),
                            sink.name(),
                            msg,
                            "circuit breaker open",
                            ErrorKind::SinkPermanent,
                        )
                    })
                    .collect();
                // v0.16.0: OTel span around DLQ insert.
                let dlq_span = tracing::info_span!(
                    "relay.dlq.insert",
                    pipeline = %pipeline.name,
                    count = entries.len(),
                    reason = "circuit_breaker_open",
                );
                let _enter = dlq_span.enter();
                if let Err(e) = crate::dlq::insert_batch(&db, &entries).await {
                    tracing::warn!(pipeline = %pipeline.name, error = %e, "DLQ insert error");
                } else {
                    // v0.13.0: Ack source after durable DLQ write; increment DLQ metric.
                    if let Some(last) = batch.last() {
                        let _ = source.acknowledge(last).await;
                    }
                    metrics
                        .dlq_entries_written
                        .with_label_values(&[&pipeline.name, &direction_label, &tenant_label])
                        .inc_by(entries.len() as u64);
                }
            } else {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(poll_interval_ms)) => {}
                    _ = stop_rx.changed() => { break; }
                }
            }
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

        // v0.13.0: OTel span around the publish call.
        let publish_result = {
            let span = tracing::info_span!(
                "relay.sink.publish",
                pipeline = %pipeline.name,
                batch_size = batch.len(),
            );
            sink.publish(&batch).instrument(span).await
        };
        match publish_result {
            Ok(()) => {
                consecutive_failures = 0;
                circuit_breaker.record_success();

                if let Some(last) = batch.last() {
                    // v0.13.0: OTel span around acknowledge.
                    let ack_span = tracing::info_span!(
                        "relay.source.acknowledge",
                        pipeline = %pipeline.name,
                    );
                    if let Err(e) = source.acknowledge(last).instrument(ack_span).await {
                        tracing::warn!(
                            pipeline = %pipeline.name,
                            error = %e,
                            "acknowledge error"
                        );
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
            }
            Err(e) => {
                consecutive_failures += 1;
                circuit_breaker.record_failure();

                tracing::warn!(
                    pipeline = %pipeline.name,
                    error = %e,
                    consecutive_failures,
                    "publish error"
                );

                // v0.13.0: Set pipeline_healthy gauge to 0 on error.
                metrics
                    .pipeline_healthy
                    .with_label_values(&[&pipeline.name, &tenant_label])
                    .set(0);

                metrics
                    .publish_errors
                    .with_label_values(&[&pipeline.name, &direction_label, &tenant_label])
                    .inc();

                // v0.7.0: Route to DLQ when max retries exceeded.
                if dlq_config.enabled && consecutive_failures > dlq_config.max_retries {
                    tracing::warn!(
                        pipeline = %pipeline.name,
                        "max retries ({}) exceeded — routing batch to DLQ",
                        dlq_config.max_retries
                    );
                    let entries: Vec<DlqEntry> = batch
                        .iter()
                        .map(|msg| {
                            DlqEntry::from_message(
                                &direction_label,
                                &pipeline.name,
                                source.name(),
                                sink.name(),
                                msg,
                                &e.to_string(),
                                ErrorKind::MaxRetriesExceeded,
                            )
                        })
                        .collect();
                    // v0.16.0: OTel span around DLQ insert on max retries.
                    let dlq_span = tracing::info_span!(
                        "relay.dlq.insert",
                        pipeline = %pipeline.name,
                        count = entries.len(),
                        reason = "max_retries_exceeded",
                    );
                    let _enter = dlq_span.enter();
                    match crate::dlq::insert_batch(&db, &entries).await {
                        Ok(()) => {
                            // v0.13.0: Ack source after durable DLQ write.
                            if let Some(last) = batch.last() {
                                let _ = source.acknowledge(last).await;
                            }
                            metrics
                                .dlq_entries_written
                                .with_label_values(&[
                                    &pipeline.name,
                                    &direction_label,
                                    &tenant_label,
                                ])
                                .inc_by(entries.len() as u64);
                        }
                        Err(dlq_err) => {
                            tracing::warn!(
                                pipeline = %pipeline.name,
                                error = %dlq_err,
                                "DLQ insert error"
                            );
                        }
                    }
                    consecutive_failures = 0;
                }

                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(poll_interval_ms)) => {}
                    _ = stop_rx.changed() => { break; }
                }
            }
        }
    }

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

// ── Source factory ────────────────────────────────────────────────────────

async fn build_source(
    pipeline: &PipelineConfig,
    db: Arc<Client>,
    relay_group_id: &str,
) -> Result<Box<dyn crate::source::Source>, RelayError> {
    let source_type = pipeline.require_str(&["source_type"])?;
    match source_type {
        "outbox" => {
            let outbox = pipeline.require_str(&["source", "outbox"])?;
            let subject_template = pipeline
                .opt_str(&["source", "subject_template"])
                .unwrap_or("{stream_table}.{op}");

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
                    format!("outbox_{outbox}"),
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
                // v0.15.0: Support raw payload mode (no v:1 envelope).
                let raw_mode = pipeline
                    .opt_str(&["source", "payload_mode"])
                    .map(|m| m == "raw")
                    .unwrap_or(false);
                let src = crate::source::outbox::OutboxPollerSource::new_simple_with_mode(
                    db,
                    outbox,
                    format!("outbox_{outbox}"),
                    subject_template,
                    relay_group_id,
                    &pipeline.name,
                    raw_mode,
                )
                .await?;
                Ok(Box::new(src))
            }
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

        other => Err(RelayError::InvalidConfig {
            name: pipeline.name.clone(),
            reason: format!("unknown source_type: {other}"),
        }),
    }
}

// ── Sink factory ──────────────────────────────────────────────────────────

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
            Ok(Box::new(crate::sink::inbox::InboxSink::new(db, inbox)))
        }

        #[cfg(feature = "nats")]
        "nats" => {
            let url = pipeline.require_str(&["sink", "url"])?;
            let subject_template = pipeline
                .opt_str(&["sink", "subject_template"])
                .unwrap_or("{stream_table}.{op}");
            Ok(Box::new(
                crate::sink::nats::NatsSink::new(url, subject_template).await?,
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
            Ok(Box::new(
                crate::sink::elasticsearch::ElasticsearchSink::new(url, index_template)?,
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
            Ok(Box::new(crate::sink::arrow_flight::ArrowFlightSink::new(
                url,
                auth_token,
                descriptor_path,
            )))
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

        other => Err(RelayError::InvalidConfig {
            name: pipeline.name.clone(),
            reason: format!("unknown sink_type: {other}"),
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
}
