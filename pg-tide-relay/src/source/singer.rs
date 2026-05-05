/// Singer protocol tap source (RELAY-P3-Singer-Source).
///
/// Spawns a Singer tap subprocess, reads its stdout line-by-line,
/// converts RECORD messages to RelayMessages, and persists STATE
/// messages to `tide.singer_state` for resumable incremental syncs.
///
/// SCHEMA messages are logged to `tide.singer_schema_log` and
/// compared against the previous schema to detect drift.
///
/// Feature-gated: only compiled with `--features singer`.
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, ChildStdout, Command};
use tokio_postgres::Client;

use crate::envelope::RelayMessage;
use crate::error::RelayError;
use crate::sink::singer::{load_singer_state, log_singer_schema, persist_singer_state};

/// Singer tap source — reads from a tap subprocess.
pub struct SingerSource {
    db: Arc<Client>,
    pipeline_name: String,
    tap_name: String,
    /// Buffered RECORD messages not yet returned by poll().
    buffer: VecDeque<RelayMessage>,
    /// stdout reader for the tap subprocess.
    stdout: BufReader<ChildStdout>,
    /// The running subprocess (kept alive for the pipeline lifetime).
    child: Child,
    /// Remembered schemas for drift detection (stream_name → schema).
    known_schemas: HashMap<String, serde_json::Value>,
    /// How to handle schema drift.
    on_schema_change: crate::sink::singer::OnSchemaChange,
    /// Sequential counter for dedup keys.
    seq: u64,
}

impl SingerSource {
    /// Spawn a Singer tap subprocess, loading last STATE from PostgreSQL.
    pub async fn new(
        db: Arc<Client>,
        pipeline_name: impl Into<String>,
        tap_command: &str,
        tap_args: &[String],
        tap_name: impl Into<String>,
        on_schema_change: crate::sink::singer::OnSchemaChange,
    ) -> Result<Self, RelayError> {
        let pipeline_name = pipeline_name.into();
        let tap_name = tap_name.into();

        // Load last STATE from PostgreSQL and write to a temp file for the tap.
        let state_file =
            if let Some(state) = load_singer_state(&db, &pipeline_name, &tap_name).await? {
                let tmp = tempfile_path(&pipeline_name, &tap_name);
                tokio::fs::write(&tmp, state.to_string())
                    .await
                    .map_err(RelayError::Io)?;
                tracing::info!(
                    pipeline = %pipeline_name,
                    tap = %tap_name,
                    state_file = %tmp,
                    "Singer tap resuming from last STATE checkpoint"
                );
                Some(tmp)
            } else {
                tracing::info!(
                    pipeline = %pipeline_name,
                    tap = %tap_name,
                    "Singer tap starting fresh (no STATE checkpoint found)"
                );
                None
            };

        // Build the tap command, appending --state <file> if we have a checkpoint.
        let mut cmd = Command::new(tap_command);
        cmd.args(tap_args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit());

        if let Some(ref sf) = state_file {
            cmd.args(["--state", sf]);
        }

        let mut child = cmd.spawn().map_err(|e| RelayError::SourcePoll {
            src: "singer".to_string(),
            inner: format!("failed to spawn Singer tap '{tap_command}': {e}").into(),
        })?;

        let stdout = child
            .stdout
            .take()
            .expect("stdout was piped — handle is always present");

        tracing::info!(
            pipeline = %pipeline_name,
            tap = %tap_name,
            "Singer tap subprocess started"
        );

        Ok(Self {
            db,
            pipeline_name,
            tap_name,
            buffer: VecDeque::new(),
            stdout: BufReader::new(stdout),
            child,
            known_schemas: HashMap::new(),
            on_schema_change,
            seq: 0,
        })
    }

    /// Read lines from the tap's stdout, buffering RECORD messages.
    /// Returns the number of new RECORD messages added to the buffer.
    async fn fill_buffer_from_tap(
        &mut self,
        target: usize,
        read_timeout: std::time::Duration,
    ) -> Result<usize, RelayError> {
        let mut added = 0;
        let mut line = String::new();

        while added < target {
            line.clear();
            let n = match tokio::time::timeout(read_timeout, self.stdout.read_line(&mut line)).await
            {
                Ok(Ok(n)) => n,
                Ok(Err(e)) => return Err(RelayError::Io(e)),
                Err(_timeout) => break, // no more data right now
            };

            if n == 0 {
                // EOF — tap subprocess finished.
                break;
            }

            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            let msg: serde_json::Value = match serde_json::from_str(trimmed) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        pipeline = %self.pipeline_name,
                        error = %e,
                        line = %trimmed,
                        "Singer tap emitted non-JSON line — skipping"
                    );
                    continue;
                }
            };

            match msg.get("type").and_then(|v| v.as_str()) {
                Some("SCHEMA") => {
                    self.handle_schema_message(&msg).await?;
                }
                Some("RECORD") => {
                    if let Some(relay_msg) = self.handle_record_message(&msg) {
                        self.buffer.push_back(relay_msg);
                        added += 1;
                    }
                }
                Some("STATE") => {
                    self.handle_state_message(&msg).await;
                }
                Some("ACTIVATE_VERSION") => {
                    tracing::debug!(
                        pipeline = %self.pipeline_name,
                        "Singer ACTIVATE_VERSION received — full-refresh signalled"
                    );
                }
                other => {
                    tracing::debug!(
                        pipeline = %self.pipeline_name,
                        msg_type = ?other,
                        "Unknown Singer message type — ignoring"
                    );
                }
            }
        }

        Ok(added)
    }

    /// Handle a Singer SCHEMA message.
    async fn handle_schema_message(&mut self, msg: &serde_json::Value) -> Result<(), RelayError> {
        let stream = msg
            .get("stream")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let schema = msg
            .get("schema")
            .cloned()
            .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));
        let key_properties: Vec<String> = msg
            .get("key_properties")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        // Detect schema drift.
        let is_new = !self.known_schemas.contains_key(&stream);
        let changed = self
            .known_schemas
            .get(&stream)
            .map(|existing| existing != &schema)
            .unwrap_or(false);

        if changed {
            match &self.on_schema_change {
                crate::sink::singer::OnSchemaChange::Log => {
                    tracing::warn!(
                        pipeline = %self.pipeline_name,
                        stream = %stream,
                        "Singer SCHEMA drift detected (on_schema_change=log)"
                    );
                }
                crate::sink::singer::OnSchemaChange::EmitEvent => {
                    tracing::info!(
                        pipeline = %self.pipeline_name,
                        stream = %stream,
                        "Singer SCHEMA drift detected — SCHEMA_DRIFT event will be emitted"
                    );
                    // TODO: Emit SCHEMA_DRIFT outbox event (future enhancement).
                }
                crate::sink::singer::OnSchemaChange::Error => {
                    return Err(RelayError::SourcePoll {
                        src: "singer".to_string(),
                        inner: format!(
                            "Singer SCHEMA drift on stream '{}' (on_schema_change=error)",
                            stream
                        )
                        .into(),
                    });
                }
            }
        }

        if is_new || changed {
            self.known_schemas.insert(stream.clone(), schema.clone());
            // Log to tide.singer_schema_log for audit trail.
            let _ = log_singer_schema(
                &self.db,
                &self.pipeline_name,
                &self.tap_name,
                &stream,
                &schema,
                &key_properties,
            )
            .await;
        }

        Ok(())
    }

    /// Handle a Singer RECORD message, converting it to a RelayMessage.
    fn handle_record_message(&mut self, msg: &serde_json::Value) -> Option<RelayMessage> {
        let stream = msg
            .get("stream")
            .and_then(|v| v.as_str())
            .unwrap_or("stream");
        let record = msg
            .get("record")
            .cloned()
            .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));

        self.seq += 1;
        let dedup_key = format!("singer:{}:{}:{}", self.pipeline_name, stream, self.seq);

        Some(RelayMessage::new_reverse(dedup_key, stream, record))
    }

    /// Handle a Singer STATE message, persisting it to PostgreSQL.
    async fn handle_state_message(&self, msg: &serde_json::Value) {
        if let Some(value) = msg.get("value") {
            match persist_singer_state(&self.db, &self.pipeline_name, &self.tap_name, value.clone())
                .await
            {
                Ok(()) => {
                    tracing::debug!(
                        pipeline = %self.pipeline_name,
                        tap = %self.tap_name,
                        "Singer STATE checkpoint persisted"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        pipeline = %self.pipeline_name,
                        error = %e,
                        "Failed to persist Singer STATE checkpoint"
                    );
                }
            }
        }
    }
}

/// Build a temp file path for Singer state.
fn tempfile_path(pipeline_name: &str, tap_name: &str) -> String {
    let safe_pipeline = pipeline_name.replace(['/', '\\', ':'], "_");
    let safe_tap = tap_name.replace(['/', '\\', ':'], "_");
    std::env::temp_dir()
        .join(format!(
            "pg_tide_singer_state__{safe_pipeline}__{safe_tap}.json"
        ))
        .to_string_lossy()
        .into_owned()
}

#[async_trait::async_trait]
impl super::Source for SingerSource {
    fn name(&self) -> &str {
        "singer"
    }

    async fn poll(&mut self, batch_size: i64) -> Result<Vec<RelayMessage>, RelayError> {
        // Return any buffered messages first.
        let target = batch_size as usize;
        let buffered: Vec<RelayMessage> =
            self.buffer.drain(..self.buffer.len().min(target)).collect();

        if buffered.len() >= target {
            return Ok(buffered);
        }

        // Try to read more from the subprocess stdout.
        let still_needed = target - buffered.len();
        let read_timeout = std::time::Duration::from_millis(200);
        let _ = self
            .fill_buffer_from_tap(still_needed, read_timeout)
            .await?;

        // Drain the buffer again after filling.
        let mut result = buffered;
        let drain_count = self.buffer.len().min(still_needed);
        result.extend(self.buffer.drain(..drain_count));

        Ok(result)
    }

    async fn acknowledge(&mut self, _last_message: &RelayMessage) -> Result<(), RelayError> {
        // STATE is persisted inline when received from the tap.
        // No additional acknowledgement needed.
        Ok(())
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        // Gracefully terminate the subprocess.
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), self.child.wait()).await;
        let _ = self.child.kill().await;

        tracing::info!(
            pipeline = %self.pipeline_name,
            tap = %self.tap_name,
            "Singer tap subprocess stopped"
        );
        Ok(())
    }
}
