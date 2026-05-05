/// Singer protocol target sink (RELAY-P3-Singer-Sink).
///
/// Spawns a Singer target subprocess and writes pg-tide delta messages
/// as Singer SCHEMA + RECORD + STATE messages to its stdin.
/// Reads STATE acknowledgements from the target's stdout and persists
/// them to `tide.singer_state` for crash-recovery.
///
/// Feature-gated: only compiled with `--features singer`.
use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio_postgres::Client;

use crate::envelope::RelayMessage;
use crate::error::RelayError;

/// How to react when a Singer SCHEMA message indicates column drift.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum OnSchemaChange {
    /// Log the drift and keep forwarding (default).
    #[default]
    Log,
    /// Emit a `SCHEMA_DRIFT` event to the outbox (for downstream handling).
    EmitEvent,
    /// Return an error, stopping the pipeline.
    Error,
}

impl OnSchemaChange {
    pub fn from_str(s: &str) -> Self {
        match s {
            "emit_event" => Self::EmitEvent,
            "error" => Self::Error,
            _ => Self::Log,
        }
    }
}

/// Singer target sink — pipes pg-tide messages to a target subprocess.
pub struct SingerSink {
    db: Arc<Client>,
    pipeline_name: String,
    target_name: String,
    stream_name_template: String,
    on_schema_change: OnSchemaChange,
    /// stdin handle for the running subprocess.
    stdin: Option<ChildStdin>,
    /// stdout reader for STATE responses from the target.
    stdout: Option<BufReader<ChildStdout>>,
    /// The running subprocess (kept alive for the pipeline lifetime).
    child: Option<Child>,
    /// JSON Schema sent for each stream (stream_name → schema JSON).
    emitted_schemas: HashMap<String, serde_json::Value>,
}

impl SingerSink {
    pub fn new(
        db: Arc<Client>,
        pipeline_name: impl Into<String>,
        target_command: &str,
        target_args: &[String],
        target_name: impl Into<String>,
        stream_name_template: impl Into<String>,
        on_schema_change: OnSchemaChange,
    ) -> Result<Self, RelayError> {
        let pipeline_name = pipeline_name.into();
        let target_name = target_name.into();

        let mut cmd = Command::new(target_command);
        cmd.args(target_args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit());

        let mut child = cmd.spawn().map_err(|e| RelayError::SinkPublish {
            sink: "singer".to_string(),
            source: format!("failed to spawn Singer target '{target_command}': {e}").into(),
        })?;

        let stdin = child.stdin.take();
        let stdout = child.stdout.take().map(BufReader::new);

        tracing::info!(
            pipeline = %pipeline_name,
            target = %target_name,
            "Singer target subprocess started"
        );

        Ok(Self {
            db,
            pipeline_name,
            target_name,
            stream_name_template: stream_name_template.into(),
            on_schema_change,
            stdin,
            stdout,
            child: Some(child),
            emitted_schemas: HashMap::new(),
        })
    }

    /// Resolve the Singer stream name from a RelayMessage.
    fn stream_name(&self, msg: &RelayMessage) -> String {
        self.stream_name_template
            .replace("{stream_table}", &msg.subject)
            .replace("{subject}", &msg.subject)
            .replace("{op}", &msg.op)
    }

    /// Build a JSON Schema from the relay message payload.
    fn infer_schema(payload: &serde_json::Value) -> serde_json::Value {
        let mut properties = serde_json::Map::new();
        if let Some(obj) = payload.as_object() {
            for (key, val) in obj {
                let json_type = match val {
                    serde_json::Value::Bool(_) => "boolean",
                    serde_json::Value::Number(_) => "number",
                    serde_json::Value::Array(_) => "array",
                    serde_json::Value::Object(_) => "object",
                    _ => "string",
                };
                properties.insert(
                    key.clone(),
                    serde_json::json!({"type": [json_type, "null"]}),
                );
            }
        }
        // Always add Singer Data Capture metadata columns.
        properties.insert(
            "_sdc_extracted_at".to_string(),
            serde_json::json!({"type": ["string", "null"], "format": "date-time"}),
        );
        properties.insert(
            "_sdc_batched_at".to_string(),
            serde_json::json!({"type": ["string", "null"], "format": "date-time"}),
        );
        properties.insert(
            "_sdc_deleted_at".to_string(),
            serde_json::json!({"type": ["string", "null"], "format": "date-time"}),
        );
        serde_json::json!({
            "type": "object",
            "properties": properties,
            "additionalProperties": false
        })
    }

    /// Emit a Singer SCHEMA message to the subprocess stdin.
    async fn emit_schema(
        &mut self,
        stream: &str,
        schema: serde_json::Value,
    ) -> Result<(), RelayError> {
        let msg = serde_json::json!({
            "type": "SCHEMA",
            "stream": stream,
            "schema": schema,
            "key_properties": ["_sdc_extracted_at"]
        });
        self.write_line(&msg.to_string()).await
    }

    /// Emit a Singer RECORD message to the subprocess stdin.
    async fn emit_record(
        &mut self,
        stream: &str,
        record: serde_json::Value,
        time_extracted: &str,
    ) -> Result<(), RelayError> {
        let msg = serde_json::json!({
            "type": "RECORD",
            "stream": stream,
            "record": record,
            "time_extracted": time_extracted
        });
        self.write_line(&msg.to_string()).await
    }

    /// Emit a Singer STATE message to the subprocess stdin.
    async fn emit_state(&mut self, value: serde_json::Value) -> Result<(), RelayError> {
        let msg = serde_json::json!({
            "type": "STATE",
            "value": value
        });
        self.write_line(&msg.to_string()).await
    }

    /// Write a line (with newline) to the subprocess stdin.
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

    /// Drain any STATE messages from subprocess stdout and persist them.
    async fn drain_stdout_states(&mut self) {
        let mut line = String::new();
        let timeout = std::time::Duration::from_millis(50);

        if let Some(stdout) = &mut self.stdout {
            loop {
                line.clear();
                match tokio::time::timeout(timeout, stdout.read_line(&mut line)).await {
                    Ok(Ok(0)) => break, // EOF
                    Ok(Ok(_)) => {
                        let trimmed = line.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        if let Ok(msg) = serde_json::from_str::<serde_json::Value>(trimmed) {
                            if msg.get("type").and_then(|v| v.as_str()) == Some("STATE") {
                                if let Some(state_val) = msg.get("value") {
                                    let _ = persist_singer_state(
                                        &self.db,
                                        &self.pipeline_name,
                                        &self.target_name,
                                        state_val.clone(),
                                    )
                                    .await;
                                }
                            }
                        }
                    }
                    _ => break, // timeout or error
                }
            }
        }
    }

    /// Check if the schema for a stream has changed.
    fn schema_changed(&self, stream: &str, new_schema: &serde_json::Value) -> bool {
        match self.emitted_schemas.get(stream) {
            Some(existing) => existing != new_schema,
            None => true, // first time — treat as change
        }
    }
}

#[async_trait::async_trait]
impl super::Sink for SingerSink {
    fn name(&self) -> &str {
        "singer"
    }

    async fn publish(&mut self, messages: &[RelayMessage]) -> Result<(), RelayError> {
        if messages.is_empty() {
            return Ok(());
        }

        let now = chrono::Utc::now().to_rfc3339();

        for msg in messages {
            let stream = self.stream_name(msg);
            let schema = Self::infer_schema(&msg.payload);

            // Emit SCHEMA message if schema changed or not yet emitted.
            if self.schema_changed(&stream, &schema) {
                match &self.on_schema_change {
                    OnSchemaChange::Error if self.emitted_schemas.contains_key(&stream) => {
                        return Err(RelayError::SinkPublish {
                            sink: "singer".to_string(),
                            source: format!(
                                "Singer SCHEMA drift detected for stream '{}' (on_schema_change=error)",
                                stream
                            )
                            .into(),
                        });
                    }
                    OnSchemaChange::Log if self.emitted_schemas.contains_key(&stream) => {
                        tracing::warn!(
                            pipeline = %self.pipeline_name,
                            stream = %stream,
                            "Singer SCHEMA drift detected"
                        );
                    }
                    _ => {}
                }

                self.emit_schema(&stream, schema.clone()).await?;
                self.emitted_schemas.insert(stream.clone(), schema);
            }

            // Build the Singer RECORD payload.
            let mut record = msg.payload.clone();
            if let Some(obj) = record.as_object_mut() {
                obj.insert("_sdc_extracted_at".to_string(), serde_json::json!(now));
                obj.insert("_sdc_batched_at".to_string(), serde_json::json!(now));
                if msg.op == "delete" {
                    obj.insert("_sdc_deleted_at".to_string(), serde_json::json!(now));
                }
            }

            self.emit_record(&stream, record, &now).await?;
        }

        // Emit STATE with the last message's dedup key as checkpoint.
        if let Some(last) = messages.last() {
            let state = serde_json::json!({"bookmarks": {"pg_tide_offset": last.dedup_key}});
            self.emit_state(state).await?;
        }

        // Flush stdin.
        if let Some(stdin) = &mut self.stdin {
            stdin.flush().await.map_err(RelayError::Io)?;
        }

        // Drain any STATE responses from target stdout.
        self.drain_stdout_states().await;

        Ok(())
    }

    async fn is_healthy(&mut self) -> bool {
        if let Some(child) = &mut self.child {
            // Check if the subprocess is still running.
            matches!(child.try_wait(), Ok(None))
        } else {
            false
        }
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        // Close stdin to signal EOF to the target.
        drop(self.stdin.take());

        // Give the subprocess a moment to flush and exit.
        if let Some(mut child) = self.child.take() {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), child.wait()).await;
            // Force-kill if still running.
            let _ = child.kill().await;
        }

        tracing::info!(
            pipeline = %self.pipeline_name,
            target = %self.target_name,
            "Singer target subprocess stopped"
        );

        Ok(())
    }
}

/// Persist a Singer STATE message to `tide.singer_state`.
pub(crate) async fn persist_singer_state(
    db: &Client,
    pipeline_name: &str,
    tap_name: &str,
    state_value: serde_json::Value,
) -> Result<(), RelayError> {
    db.execute(
        "INSERT INTO tide.singer_state (pipeline_name, tap_name, state_value, written_at)
         VALUES ($1, $2, $3, now())
         ON CONFLICT (pipeline_name, tap_name) DO UPDATE
           SET state_value = EXCLUDED.state_value,
               written_at  = EXCLUDED.written_at",
        &[&pipeline_name, &tap_name, &state_value],
    )
    .await?;
    Ok(())
}

/// Load the last Singer STATE from `tide.singer_state`.
pub(crate) async fn load_singer_state(
    db: &Client,
    pipeline_name: &str,
    tap_name: &str,
) -> Result<Option<serde_json::Value>, RelayError> {
    let rows = db
        .query(
            "SELECT state_value FROM tide.singer_state
              WHERE pipeline_name = $1 AND tap_name = $2",
            &[&pipeline_name, &tap_name],
        )
        .await?;
    Ok(rows.first().map(|r| r.get::<_, serde_json::Value>(0)))
}

/// Persist a Singer SCHEMA message to `tide.singer_schema_log`.
pub(crate) async fn log_singer_schema(
    db: &Client,
    pipeline_name: &str,
    tap_name: &str,
    stream_name: &str,
    schema_value: &serde_json::Value,
    key_properties: &[String],
) -> Result<(), RelayError> {
    db.execute(
        "INSERT INTO tide.singer_schema_log
           (pipeline_name, tap_name, stream_name, schema_value, key_properties, logged_at)
         VALUES ($1, $2, $3, $4, $5, now())",
        &[
            &pipeline_name,
            &tap_name,
            &stream_name,
            schema_value,
            &key_properties,
        ],
    )
    .await?;
    Ok(())
}
