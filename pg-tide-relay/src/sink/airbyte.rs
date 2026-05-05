/// Airbyte protocol destination sink (RELAY-P3-9).
///
/// Spawns an Airbyte destination connector subprocess (Docker image or bare command)
/// and writes pg-tide delta messages as AirbyteMessage (CATALOG + RECORD + STATE).
/// Reads STATE/LOG/TRACE responses from the connector's stdout.
///
/// Supports Docker mode (`destination_image`) and bare command mode (`destination_command`).
///
/// Feature-gated: only compiled with `--features airbyte`.
use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio_postgres::Client;

use crate::envelope::RelayMessage;
use crate::error::RelayError;

/// Airbyte destination sink.
pub struct AirbyteSink {
    db: Arc<Client>,
    pipeline_name: String,
    destination_name: String,
    stream_name_template: String,
    namespace: String,
    /// stdin handle for the running subprocess.
    stdin: Option<ChildStdin>,
    /// stdout reader for STATE/LOG/TRACE responses.
    stdout: Option<BufReader<ChildStdout>>,
    /// The running subprocess.
    child: Option<Child>,
    /// Schemas emitted per stream for change detection.
    emitted_catalogs: HashMap<String, serde_json::Value>,
    /// Sequential counter for `emitted_at` timestamps.
    seq: u64,
}

impl AirbyteSink {
    /// Create an Airbyte sink using a Docker image.
    pub fn new_docker(
        db: Arc<Client>,
        pipeline_name: impl Into<String>,
        destination_image: &str,
        destination_config: &serde_json::Value,
        destination_name: impl Into<String>,
        stream_name_template: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Result<Self, RelayError> {
        let config_json = destination_config.to_string();
        let tmp_config = write_temp_config(&config_json, "airbyte_dest_config").map_err(|e| {
            RelayError::SinkPublish {
                sink: "airbyte".to_string(),
                source: format!("failed to write Airbyte config file: {e}").into(),
            }
        })?;

        let mut cmd = Command::new("docker");
        cmd.args([
            "run",
            "--rm",
            "-i",
            "-v",
            &format!("{tmp_config}:/config.json"),
            destination_image,
            "write",
            "--config",
            "/config.json",
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit());

        Self::spawn_from_command(
            db,
            pipeline_name,
            destination_name,
            stream_name_template,
            namespace,
            cmd,
        )
    }

    /// Create an Airbyte sink using a bare connector command.
    pub fn new_command(
        db: Arc<Client>,
        pipeline_name: impl Into<String>,
        destination_command: &str,
        destination_args: &[String],
        destination_name: impl Into<String>,
        stream_name_template: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Result<Self, RelayError> {
        let mut cmd = Command::new(destination_command);
        cmd.args(destination_args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit());

        Self::spawn_from_command(
            db,
            pipeline_name,
            destination_name,
            stream_name_template,
            namespace,
            cmd,
        )
    }

    fn spawn_from_command(
        db: Arc<Client>,
        pipeline_name: impl Into<String>,
        destination_name: impl Into<String>,
        stream_name_template: impl Into<String>,
        namespace: impl Into<String>,
        mut cmd: Command,
    ) -> Result<Self, RelayError> {
        let pipeline_name = pipeline_name.into();
        let destination_name = destination_name.into();

        let mut child = cmd.spawn().map_err(|e| RelayError::SinkPublish {
            sink: "airbyte".to_string(),
            source: format!("failed to spawn Airbyte destination '{destination_name}': {e}").into(),
        })?;

        let stdin = child.stdin.take();
        let stdout = child.stdout.take().map(BufReader::new);

        tracing::info!(
            pipeline = %pipeline_name,
            destination = %destination_name,
            "Airbyte destination subprocess started"
        );

        Ok(Self {
            db,
            pipeline_name,
            destination_name,
            stream_name_template: stream_name_template.into(),
            namespace: namespace.into(),
            stdin,
            stdout,
            child: Some(child),
            emitted_catalogs: HashMap::new(),
            seq: 0,
        })
    }

    /// Resolve stream name from a RelayMessage.
    fn stream_name(&self, msg: &RelayMessage) -> String {
        self.stream_name_template
            .replace("{stream_table}", &msg.subject)
            .replace("{subject}", &msg.subject)
    }

    /// Infer a JSON Schema from a RelayMessage payload.
    fn infer_json_schema(payload: &serde_json::Value) -> serde_json::Value {
        let mut properties = serde_json::Map::new();
        if let Some(obj) = payload.as_object() {
            for (key, val) in obj {
                let json_type = match val {
                    serde_json::Value::Bool(_) => "boolean",
                    serde_json::Value::Number(n) if n.is_i64() || n.is_u64() => "integer",
                    serde_json::Value::Number(_) => "number",
                    serde_json::Value::Array(_) => "array",
                    serde_json::Value::Object(_) => "object",
                    _ => "string",
                };
                properties.insert(key.clone(), serde_json::json!({"type": json_type}));
            }
        }
        // Airbyte CDC metadata columns.
        properties.insert(
            "_airbyte_emitted_at".to_string(),
            serde_json::json!({"type": "integer"}),
        );
        properties.insert(
            "_ab_cdc_deleted_at".to_string(),
            serde_json::json!({"type": ["string", "null"]}),
        );
        serde_json::json!({
            "type": "object",
            "properties": properties,
            "$schema": "http://json-schema.org/draft-07/schema#"
        })
    }

    /// Emit an AirbyteMessage CATALOG to the subprocess stdin.
    async fn emit_catalog(
        &mut self,
        stream: &str,
        schema: serde_json::Value,
    ) -> Result<(), RelayError> {
        let namespace = self.namespace.clone();
        let msg = serde_json::json!({
            "type": "CATALOG",
            "catalog": {
                "streams": [{
                    "stream": {
                        "name": stream,
                        "namespace": namespace,
                        "json_schema": schema,
                        "supported_sync_modes": ["append"],
                        "source_defined_cursor": false,
                        "default_cursor_field": []
                    },
                    "sync_mode": "append",
                    "destination_sync_mode": "append"
                }]
            }
        });
        self.write_line(&msg.to_string()).await
    }

    /// Emit an AirbyteMessage RECORD to the subprocess stdin.
    async fn emit_record(
        &mut self,
        stream: &str,
        data: serde_json::Value,
        emitted_at: i64,
    ) -> Result<(), RelayError> {
        let namespace = self.namespace.clone();
        let msg = serde_json::json!({
            "type": "RECORD",
            "record": {
                "stream": stream,
                "namespace": namespace,
                "data": data,
                "emitted_at": emitted_at
            }
        });
        self.write_line(&msg.to_string()).await
    }

    /// Emit an AirbyteMessage STATE checkpoint.
    async fn emit_state(&mut self, offset: &str) -> Result<(), RelayError> {
        let msg = serde_json::json!({
            "type": "STATE",
            "state": {
                "type": "GLOBAL",
                "global": {
                    "shared_state": {
                        "pg_tide_offset": offset
                    },
                    "stream_states": []
                }
            }
        });
        self.write_line(&msg.to_string()).await
    }

    /// Write a line to the subprocess stdin.
    async fn write_line(&mut self, line: &str) -> Result<(), RelayError> {
        if let Some(stdin) = &mut self.stdin {
            let bytes = format!("{line}\n");
            stdin
                .write_all(bytes.as_bytes())
                .await
                .map_err(RelayError::Io)?;
        }
        Ok(())
    }

    /// Drain responses (STATE/LOG/TRACE) from subprocess stdout.
    async fn drain_stdout_responses(&mut self) {
        let timeout = std::time::Duration::from_millis(50);
        let mut line = String::new();

        if let Some(stdout) = &mut self.stdout {
            loop {
                line.clear();
                match tokio::time::timeout(timeout, stdout.read_line(&mut line)).await {
                    Ok(Ok(0)) => break,
                    Ok(Ok(_)) => {
                        let trimmed = line.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        if let Ok(msg) = serde_json::from_str::<serde_json::Value>(trimmed) {
                            match msg.get("type").and_then(|v| v.as_str()) {
                                Some("STATE") => {
                                    if let Some(state) = msg.get("state") {
                                        let _ = persist_airbyte_state(
                                            &self.db,
                                            &self.pipeline_name,
                                            &self.destination_name,
                                            state.clone(),
                                        )
                                        .await;
                                    }
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
                                _ => {}
                            }
                        }
                    }
                    _ => break,
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl super::Sink for AirbyteSink {
    fn name(&self) -> &str {
        "airbyte"
    }

    async fn publish(&mut self, messages: &[RelayMessage]) -> Result<(), RelayError> {
        if messages.is_empty() {
            return Ok(());
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        for msg in messages {
            let stream = self.stream_name(msg);
            let schema = Self::infer_json_schema(&msg.payload);

            // Emit CATALOG if schema changed or first time.
            let emit_catalog = self
                .emitted_catalogs
                .get(&stream)
                .map(|s| s != &schema)
                .unwrap_or(true);

            if emit_catalog {
                self.emit_catalog(&stream, schema.clone()).await?;
                self.emitted_catalogs.insert(stream.clone(), schema);
            }

            // Build the Airbyte RECORD payload.
            let mut data = msg.payload.clone();
            if let Some(obj) = data.as_object_mut() {
                obj.insert("_airbyte_emitted_at".to_string(), serde_json::json!(now_ms));
                if msg.op == "delete" {
                    let ts = chrono::Utc::now().to_rfc3339();
                    obj.insert("_ab_cdc_deleted_at".to_string(), serde_json::json!(ts));
                }
            }

            self.emit_record(&stream, data, now_ms).await?;
            self.seq += 1;
        }

        // Emit STATE checkpoint.
        if let Some(last) = messages.last() {
            self.emit_state(&last.dedup_key).await?;
        }

        // Flush stdin.
        if let Some(stdin) = &mut self.stdin {
            stdin.flush().await.map_err(RelayError::Io)?;
        }

        // Drain responses.
        self.drain_stdout_responses().await;

        Ok(())
    }

    async fn is_healthy(&mut self) -> bool {
        if let Some(child) = &mut self.child {
            matches!(child.try_wait(), Ok(None))
        } else {
            false
        }
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        drop(self.stdin.take());

        if let Some(mut child) = self.child.take() {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), child.wait()).await;
            let _ = child.kill().await;
        }

        tracing::info!(
            pipeline = %self.pipeline_name,
            destination = %self.destination_name,
            "Airbyte destination subprocess stopped"
        );
        Ok(())
    }
}

/// Write a JSON string to a temp file and return the path.
fn write_temp_config(content: &str, name: &str) -> Result<String, std::io::Error> {
    let path = std::env::temp_dir()
        .join(format!("pg_tide_{name}_{}.json", uuid::Uuid::new_v4()))
        .to_string_lossy()
        .into_owned();
    std::fs::write(&path, content)?;
    Ok(path)
}

/// Persist an Airbyte STATE to `tide.relay_airbyte_state`.
pub(crate) async fn persist_airbyte_state(
    db: &Client,
    pipeline_name: &str,
    source_name: &str,
    state_value: serde_json::Value,
) -> Result<(), RelayError> {
    db.execute(
        "INSERT INTO tide.relay_airbyte_state (pipeline_name, source_name, state_value, written_at)
         VALUES ($1, $2, $3, now())
         ON CONFLICT (pipeline_name, source_name) DO UPDATE
           SET state_value = EXCLUDED.state_value,
               written_at  = EXCLUDED.written_at",
        &[&pipeline_name, &source_name, &state_value],
    )
    .await?;
    Ok(())
}

/// Load the last Airbyte STATE from `tide.relay_airbyte_state`.
pub(crate) async fn load_airbyte_state(
    db: &Client,
    pipeline_name: &str,
    source_name: &str,
) -> Result<Option<serde_json::Value>, RelayError> {
    let rows = db
        .query(
            "SELECT state_value FROM tide.relay_airbyte_state
              WHERE pipeline_name = $1 AND source_name = $2",
            &[&pipeline_name, &source_name],
        )
        .await?;
    Ok(rows.first().map(|r| r.get::<_, serde_json::Value>(0)))
}
