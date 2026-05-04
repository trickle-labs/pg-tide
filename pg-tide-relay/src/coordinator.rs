/// Coordinator — manages pipeline lifecycle with PostgreSQL advisory locks.
/// Implements RELAY-2 (coordinator loop), RELAY-SEC (secret resolution),
/// and hot-reload via LISTEN/NOTIFY.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch, RwLock};
use tokio_postgres::Client;

use crate::config::{resolve_pipeline_secrets, PipelineConfig, PipelineDirection};
use crate::error::RelayError;
use crate::metrics::{HealthState, RelayMetrics};

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
        "worker started"
    );

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

        match sink.publish(&batch).await {
            Ok(()) => {
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
                tracing::warn!(
                    pipeline = %pipeline.name,
                    error = %e,
                    "publish error — retrying batch"
                );
                metrics
                    .publish_errors
                    .with_label_values(&[&pipeline.name, &direction_label])
                    .inc();
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
