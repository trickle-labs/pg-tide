/// Airbyte protocol source (RELAY-P3-9).
///
/// Spawns an Airbyte source connector subprocess (Docker image or bare command),
/// reads its AirbyteMessage output, converts RECORD messages to RelayMessages,
/// and persists STATE messages to `tide.relay_airbyte_state`.
///
/// Feature-gated: only compiled with `--features airbyte`.
use std::collections::VecDeque;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, ChildStdout, Command};
use tokio_postgres::Client;

use crate::envelope::RelayMessage;
use crate::error::RelayError;
use crate::sink::airbyte::{load_airbyte_state, persist_airbyte_state};

/// Airbyte source — reads from a source connector subprocess.
pub struct AirbyteSource {
    db: Arc<Client>,
    pipeline_name: String,
    source_name: String,
    /// Buffered RECORD messages.
    buffer: VecDeque<RelayMessage>,
    /// stdout reader for the source subprocess.
    stdout: BufReader<ChildStdout>,
    /// The running subprocess.
    child: Child,
    /// Sequential counter for dedup keys.
    seq: u64,
}

impl AirbyteSource {
    /// Create an Airbyte source using a Docker image.
    pub async fn new_docker(
        db: Arc<Client>,
        pipeline_name: impl Into<String>,
        source_image: &str,
        source_config: &serde_json::Value,
        configured_catalog: &serde_json::Value,
        source_name: impl Into<String>,
    ) -> Result<Self, RelayError> {
        let pipeline_name = pipeline_name.into();
        let source_name = source_name.into();

        let config_json = source_config.to_string();
        let catalog_json = configured_catalog.to_string();

        let tmp_config = write_temp_file(&config_json, "airbyte_src_config").map_err(|e| {
            RelayError::SourcePoll {
                src: "airbyte".to_string(),
                inner: format!("failed to write Airbyte config: {e}").into(),
            }
        })?;
        let tmp_catalog = write_temp_file(&catalog_json, "airbyte_src_catalog").map_err(|e| {
            RelayError::SourcePoll {
                src: "airbyte".to_string(),
                inner: format!("failed to write Airbyte catalog: {e}").into(),
            }
        })?;

        // Load last STATE for resumable syncs.
        let state_args =
            if let Some(state) = load_airbyte_state(&db, &pipeline_name, &source_name).await? {
                let tmp_state =
                    write_temp_file(&state.to_string(), "airbyte_src_state").map_err(|e| {
                        RelayError::SourcePoll {
                            src: "airbyte".to_string(),
                            inner: format!("failed to write Airbyte state: {e}").into(),
                        }
                    })?;
                tracing::info!(
                    pipeline = %pipeline_name,
                    source = %source_name,
                    "Airbyte source resuming from last STATE checkpoint"
                );
                vec!["--state".to_string(), "/state.json".to_string(), tmp_state]
            } else {
                tracing::info!(
                    pipeline = %pipeline_name,
                    source = %source_name,
                    "Airbyte source starting fresh (no STATE checkpoint)"
                );
                vec![]
            };

        let mut docker_args = vec![
            "run".to_string(),
            "--rm".to_string(),
            "-i".to_string(),
            "-v".to_string(),
            format!("{tmp_config}:/config.json"),
            "-v".to_string(),
            format!("{tmp_catalog}:/catalog.json"),
        ];

        // Mount state file if present.
        if state_args.len() == 3 {
            docker_args.push("-v".to_string());
            docker_args.push(format!("{}:/state.json", state_args[2]));
        }

        docker_args.push(source_image.to_string());
        docker_args.extend([
            "read".to_string(),
            "--config".to_string(),
            "/config.json".to_string(),
            "--catalog".to_string(),
            "/catalog.json".to_string(),
        ]);

        if state_args.len() == 3 {
            docker_args.extend(["--state".to_string(), "/state.json".to_string()]);
        }

        let mut cmd = Command::new("docker");
        cmd.args(&docker_args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit());

        Self::spawn_from_command(db, pipeline_name, source_name, cmd).await
    }

    /// Create an Airbyte source using a bare connector command.
    pub async fn new_command(
        db: Arc<Client>,
        pipeline_name: impl Into<String>,
        source_command: &str,
        source_args: &[String],
        source_name: impl Into<String>,
    ) -> Result<Self, RelayError> {
        let pipeline_name = pipeline_name.into();
        let source_name = source_name.into();

        let mut cmd = Command::new(source_command);
        cmd.args(source_args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit());

        Self::spawn_from_command(db, pipeline_name, source_name, cmd).await
    }

    async fn spawn_from_command(
        db: Arc<Client>,
        pipeline_name: String,
        source_name: String,
        mut cmd: Command,
    ) -> Result<Self, RelayError> {
        let mut child = cmd.spawn().map_err(|e| RelayError::SourcePoll {
            src: "airbyte".to_string(),
            inner: format!("failed to spawn Airbyte source '{source_name}': {e}").into(),
        })?;

        let stdout = child
            .stdout
            .take()
            .expect("stdout was piped — handle is always present");

        tracing::info!(
            pipeline = %pipeline_name,
            source = %source_name,
            "Airbyte source subprocess started"
        );

        Ok(Self {
            db,
            pipeline_name,
            source_name,
            buffer: VecDeque::new(),
            stdout: BufReader::new(stdout),
            child,
            seq: 0,
        })
    }

    /// Read AirbyteMessages from the subprocess stdout, buffering RECORDs.
    async fn fill_buffer(
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
                Err(_) => break, // timeout
            };

            if n == 0 {
                break; // EOF
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
                        "Airbyte source emitted non-JSON line — skipping"
                    );
                    continue;
                }
            };

            match msg.get("type").and_then(|v| v.as_str()) {
                Some("RECORD") => {
                    if let Some(relay_msg) = self.convert_record(&msg) {
                        self.buffer.push_back(relay_msg);
                        added += 1;
                    }
                }
                Some("CATALOG") => {
                    tracing::debug!(
                        pipeline = %self.pipeline_name,
                        "Airbyte CATALOG received"
                    );
                }
                Some("STATE") => {
                    self.handle_state(&msg).await;
                }
                Some("LOG") => {
                    let level = msg
                        .pointer("/log/level")
                        .and_then(|v| v.as_str())
                        .unwrap_or("INFO");
                    let message = msg
                        .pointer("/log/message")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    tracing::debug!(
                        pipeline = %self.pipeline_name,
                        airbyte_log_level = %level,
                        "{}", message
                    );
                }
                Some("TRACE") => {
                    tracing::trace!(
                        pipeline = %self.pipeline_name,
                        "Airbyte TRACE: {trimmed}"
                    );
                }
                other => {
                    tracing::debug!(
                        pipeline = %self.pipeline_name,
                        msg_type = ?other,
                        "Unknown Airbyte message type — ignoring"
                    );
                }
            }
        }

        Ok(added)
    }

    /// Convert an AirbyteMessage RECORD to a RelayMessage.
    fn convert_record(&mut self, msg: &serde_json::Value) -> Option<RelayMessage> {
        let record = msg.get("record")?;
        let stream = record
            .get("stream")
            .and_then(|v| v.as_str())
            .unwrap_or("stream");
        let data = record
            .get("data")
            .cloned()
            .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));

        self.seq += 1;
        let dedup_key = format!("airbyte:{}:{}:{}", self.pipeline_name, stream, self.seq);

        // Check for Airbyte CDC soft-delete marker.
        let op = if data
            .get("_ab_cdc_deleted_at")
            .and_then(|v| v.as_str())
            .is_some()
        {
            "delete"
        } else {
            "insert"
        };

        let mut relay_msg = RelayMessage::new_reverse(dedup_key, stream, data);
        relay_msg.op = op.to_string();
        Some(relay_msg)
    }

    /// Handle an Airbyte STATE message.
    async fn handle_state(&self, msg: &serde_json::Value) {
        if let Some(state) = msg.get("state") {
            match persist_airbyte_state(
                &self.db,
                &self.pipeline_name,
                &self.source_name,
                state.clone(),
            )
            .await
            {
                Ok(()) => {
                    tracing::debug!(
                        pipeline = %self.pipeline_name,
                        source = %self.source_name,
                        "Airbyte STATE checkpoint persisted"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        pipeline = %self.pipeline_name,
                        error = %e,
                        "Failed to persist Airbyte STATE checkpoint"
                    );
                }
            }
        }
    }
}

/// Write content to a temp file, returning the path.
fn write_temp_file(content: &str, name: &str) -> Result<String, std::io::Error> {
    let path = std::env::temp_dir()
        .join(format!("pg_tide_{name}_{}.json", uuid::Uuid::new_v4()))
        .to_string_lossy()
        .into_owned();
    std::fs::write(&path, content)?;
    Ok(path)
}

#[async_trait::async_trait]
impl super::Source for AirbyteSource {
    fn name(&self) -> &str {
        "airbyte"
    }

    async fn poll(&mut self, batch_size: i64) -> Result<Vec<RelayMessage>, RelayError> {
        let target = batch_size as usize;

        // Return any buffered messages.
        let buffered: Vec<RelayMessage> =
            self.buffer.drain(..self.buffer.len().min(target)).collect();
        if buffered.len() >= target {
            return Ok(buffered);
        }

        let still_needed = target - buffered.len();
        let read_timeout = std::time::Duration::from_millis(200);
        let _ = self.fill_buffer(still_needed, read_timeout).await?;

        let mut result = buffered;
        let drain_count = self.buffer.len().min(still_needed);
        result.extend(self.buffer.drain(..drain_count));
        Ok(result)
    }

    async fn acknowledge(&mut self, _last_message: &RelayMessage) -> Result<(), RelayError> {
        // STATE is persisted inline when received from the connector.
        Ok(())
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), self.child.wait()).await;
        let _ = self.child.kill().await;

        tracing::info!(
            pipeline = %self.pipeline_name,
            source = %self.source_name,
            "Airbyte source subprocess stopped"
        );
        Ok(())
    }
}
