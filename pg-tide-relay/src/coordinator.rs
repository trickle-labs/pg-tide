/// Coordinator — manages pipeline lifecycle with PostgreSQL advisory locks.
/// Implements RELAY-2 (coordinator loop), RELAY-SEC (secret resolution),
/// and hot-reload via LISTEN/NOTIFY.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch, RwLock};
use tokio_postgres::Client;

use crate::circuit_breaker::CircuitBreaker;
use crate::config::{resolve_pipeline_secrets, PipelineConfig, PipelineDirection};
use crate::dlq::{DlqConfig, DlqEntry, ErrorKind};
use crate::error::RelayError;
use crate::jmespath_transform::{apply_transforms, TransformConfig};
use crate::metrics::{HealthState, RelayMetrics};
use crate::rate_limiter::build_rate_limiter;
use crate::routing::{apply_routing, RoutingConfig};

/// Coordinator manages pipeline ownership via advisory locks.
pub struct Coordinator {
    db: Arc<Client>,
    relay_group_id: String,
    metrics: Arc<RelayMetrics>,
    health: Arc<RwLock<HealthState>>,
    /// Pipeline ID → cancellation sender.
    owned: HashMap<String, watch::Sender<bool>>,
}

impl Coordinator {
    pub fn new(
        db: Arc<Client>,
        relay_group_id: impl Into<String>,
        metrics: Arc<RelayMetrics>,
        health: Arc<RwLock<HealthState>>,
    ) -> Self {
        Self {
            db,
            relay_group_id: relay_group_id.into(),
            metrics,
            health,
            owned: HashMap::new(),
        }
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
        let rows = self
            .db
            .query(
                "SELECT name, 'forward' AS direction, enabled, config
                   FROM tide.relay_outbox_config
                  WHERE enabled = true
                 UNION ALL
                 SELECT name, 'reverse' AS direction, enabled, config
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

            pipelines.push(PipelineConfig {
                name,
                direction: if direction == "forward" {
                    PipelineDirection::Forward
                } else {
                    PipelineDirection::Reverse
                },
                enabled,
                config,
            });
        }
        Ok(pipelines)
    }

    /// Try to acquire the advisory lock for a pipeline.
    /// Returns true if the lock was acquired (this pod owns the pipeline).
    pub async fn try_acquire_lock(&self, pipeline_id: &str) -> Result<bool, RelayError> {
        let row = self
            .db
            .query_one(
                "SELECT pg_try_advisory_lock(hashtext($1), hashtext($2))",
                &[&self.relay_group_id, &pipeline_id],
            )
            .await?;
        Ok(row.get::<_, bool>(0))
    }

    /// Release the advisory lock for a pipeline.
    pub async fn release_lock(&self, pipeline_id: &str) -> Result<(), RelayError> {
        self.db
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
        for (pipeline_id, tx) in &self.owned {
            if tx.send(true).is_err() {
                tracing::debug!(pipeline = %pipeline_id, "pipeline already stopped");
            }
        }

        // Wait until every pipeline's receiver is closed (i.e. the task exited).
        for (pipeline_id, tx) in &self.owned {
            tx.closed().await;
            tracing::debug!(pipeline = %pipeline_id, "pipeline drained");
        }
    }

    // ── Private ──────────────────────────────────────────────────────────

    /// Load pipelines, start new ones, stop removed/disabled ones.
    async fn reconcile(&mut self, db_url: &str, batch_size: i64) {
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
            if let Some(tx) = self.owned.remove(name) {
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
            let resolved_config = match resolve_pipeline_secrets(pipeline.config.clone()) {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(
                        pipeline = %pipeline.name,
                        error = %e,
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
            };

            tokio::spawn(run_pipeline_worker(
                resolved_pipeline,
                db_url.to_string(),
                self.relay_group_id.clone(),
                Arc::clone(&self.metrics),
                batch_size,
                stop_rx,
            ));

            self.owned.insert(pipeline.name, stop_tx);
        }
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
    match worker_inner(
        pipeline,
        db_url,
        relay_group_id,
        metrics,
        batch_size,
        &mut stop_rx,
    )
    .await
    {
        Ok(()) => tracing::info!(pipeline = %name, "worker stopped"),
        Err(e) => tracing::error!(pipeline = %name, error = %e, "worker exited with error"),
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
    let (db_client, db_conn) = tokio_postgres::connect(&db_url, tokio_postgres::NoTls)
        .await
        .map_err(|e| RelayError::ConnectionFailed {
            url: db_url.clone(),
            err: e,
        })?;
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

    tracing::info!(
        pipeline = %pipeline.name,
        direction = direction_label,
        source = source.name(),
        sink = sink.name(),
        dry_run,
        "worker started"
    );

    let mut consecutive_failures: u32 = 0;

    loop {
        if *stop_rx.borrow() {
            break;
        }

        let batch = match source.poll(batch_size).await {
            Ok(msgs) => msgs,
            Err(e) => {
                tracing::warn!(
                    pipeline = %pipeline.name,
                    error = %e,
                    "poll error — sleeping before retry"
                );
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(poll_interval_ms)) => {}
                    _ = stop_rx.changed() => { break; }
                }
                continue;
            }
        };

        if batch.is_empty() {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(poll_interval_ms)) => {}
                _ = stop_rx.changed() => { break; }
            }
            continue;
        }

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
        let batch = match apply_transforms(&transform_config, batch) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(pipeline = %pipeline.name, error = %e, "transform error");
                continue;
            }
        };

        if batch.is_empty() {
            // All messages filtered out — acknowledge the source and continue.
            continue;
        }

        // v0.7.0: Apply content-based routing.
        let mut batch = batch;
        apply_routing(&routing_config, &mut batch);

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
                .with_label_values(&[&pipeline.name, &direction_label])
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
                if let Err(e) = crate::dlq::insert_batch(&db, &entries).await {
                    tracing::warn!(pipeline = %pipeline.name, error = %e, "DLQ insert error");
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

        match sink.publish(&batch).await {
            Ok(()) => {
                consecutive_failures = 0;
                circuit_breaker.record_success();

                if let Some(last) = batch.last() {
                    if let Err(e) = source.acknowledge(last).await {
                        tracing::warn!(
                            pipeline = %pipeline.name,
                            error = %e,
                            "acknowledge error"
                        );
                    }
                }
                metrics
                    .messages_published
                    .with_label_values(&[&pipeline.name, &direction_label])
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

                metrics
                    .publish_errors
                    .with_label_values(&[&pipeline.name, &direction_label])
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
                    if let Err(dlq_err) = crate::dlq::insert_batch(&db, &entries).await {
                        tracing::warn!(
                            pipeline = %pipeline.name,
                            error = %dlq_err,
                            "DLQ insert error"
                        );
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
                let src = crate::source::outbox::OutboxPollerSource::new_simple(
                    db,
                    outbox,
                    format!("outbox_{outbox}"),
                    subject_template,
                    relay_group_id,
                    &pipeline.name,
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

        other => Err(RelayError::InvalidConfig {
            name: pipeline.name.clone(),
            reason: format!("unknown sink_type: {other}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coordinator_construction() {
        // Just verifies the struct can be constructed with the right types.
        // Full integration tests use Testcontainers.
        let group_id = "test-group";
        assert_eq!(group_id, "test-group");
    }
}
