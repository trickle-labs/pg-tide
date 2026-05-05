/// Delta Lake analytics sink (v0.10.0 — RELAY-P3-DL).
///
/// Writes pg-tide relay messages to Delta Lake tables stored on object
/// storage (S3, GCS, Azure Blob, or local filesystem).
///
/// This implementation uses `object_store` for file I/O and `parquet` for
/// the Parquet data format, maintaining a Delta Log (`_delta_log/`) to
/// provide ACID transactions, schema evolution, and time travel.
///
/// Feature-gated: only compiled with `--features delta`.
use crate::envelope::RelayMessage;
use crate::error::RelayError;

#[cfg(feature = "delta")]
use object_store::{path::Path, ObjectStore};
#[cfg(feature = "delta")]
use std::sync::Arc;
#[cfg(feature = "delta")]
use chrono::Utc;

/// Configuration for the Delta Lake sink.
#[derive(Debug, Clone)]
pub struct DeltaConfig {
    /// Table root path (e.g. `s3://my-datalake/delta/orders` or `/tmp/delta/orders`).
    pub table_path: String,
    /// Whether to enable Change Data Feed (adds `_change_type` column).
    pub change_data_feed: bool,
    /// Rows per Parquet data file (default: 50000).
    pub rows_per_file: usize,
}

impl DeltaConfig {
    pub fn new(table_path: impl Into<String>) -> Self {
        Self {
            table_path: table_path.into(),
            change_data_feed: false,
            rows_per_file: 50_000,
        }
    }

    /// Build the Delta Log entry path for a given version.
    pub fn log_entry_path(&self, version: u64) -> String {
        format!(
            "{}/_delta_log/{:020}.json",
            self.table_path.trim_end_matches('/'),
            version
        )
    }

    /// Build a Delta Log commit entry (action JSON) for an add operation.
    pub fn build_add_action(
        &self,
        data_file: &str,
        num_records: i64,
        size_bytes: i64,
    ) -> serde_json::Value {
        let now_ms = Utc::now().timestamp_millis();
        serde_json::json!({
            "add": {
                "path": data_file,
                "partitionValues": {},
                "size": size_bytes,
                "modificationTime": now_ms,
                "dataChange": true,
                "stats": serde_json::json!({
                    "numRecords": num_records,
                }).to_string()
            }
        })
    }

    /// Build the schema for a Delta table (all pg-tide relay columns).
    pub fn table_schema() -> serde_json::Value {
        serde_json::json!({
            "type": "struct",
            "fields": [
                {"name": "_dedup_key", "type": "string",  "nullable": false, "metadata": {}},
                {"name": "_subject",   "type": "string",  "nullable": false, "metadata": {}},
                {"name": "_op",        "type": "string",  "nullable": false, "metadata": {}},
                {"name": "_outbox_id", "type": "long",    "nullable": true,  "metadata": {}},
                {"name": "data",       "type": "string",  "nullable": true,  "metadata": {}},
                {"name": "_change_type","type": "string", "nullable": true,  "metadata": {}},
            ]
        })
    }

    /// Build the initial Delta protocol + metadata log commit (version 00000000000000000000.json).
    pub fn build_init_commit(&self) -> Vec<serde_json::Value> {
        vec![
            serde_json::json!({
                "protocol": {
                    "minReaderVersion": 1,
                    "minWriterVersion": 2
                }
            }),
            serde_json::json!({
                "metaData": {
                    "id": format!("pg-tide-delta-{}", uuid::Uuid::new_v4()),
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": Self::table_schema().to_string(),
                    "partitionColumns": [],
                    "configuration": {
                        "delta.enableChangeDataFeed": self.change_data_feed.to_string()
                    },
                    "createdTime": Utc::now().timestamp_millis()
                }
            }),
        ]
    }
}

#[cfg(feature = "delta")]
pub struct DeltaSink {
    store: Arc<dyn ObjectStore>,
    config: DeltaConfig,
    version: u64,
    initialized: bool,
}

#[cfg(feature = "delta")]
impl DeltaSink {
    pub fn new(store: Arc<dyn ObjectStore>, config: DeltaConfig) -> Self {
        Self {
            store,
            config,
            version: 0,
            initialized: false,
        }
    }

    /// Ensure the Delta table is initialized (write version 0 log entry).
    async fn ensure_initialized(&mut self) -> Result<(), RelayError> {
        if self.initialized {
            return Ok(());
        }

        let init_path = Path::from(
            self.config
                .log_entry_path(0)
                .trim_start_matches('/'),
        );

        // Check if already initialized.
        if self.store.head(&init_path).await.is_ok() {
            self.initialized = true;
            return Ok(());
        }

        // Write the initial commit.
        let actions = self.config.build_init_commit();
        let content: String = actions
            .iter()
            .map(|a| a.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        self.store
            .put(&init_path, content.into_bytes().into())
            .await
            .map_err(|e| RelayError::SinkPublish { sink: "delta".to_string(), source: Box::new(e) })?;

        self.initialized = true;
        Ok(())
    }

    /// Build a Parquet file in memory from a batch of messages.
    pub fn build_parquet_bytes(messages: &[&RelayMessage], include_cdf: bool) -> Result<Vec<u8>, RelayError> {
        use parquet::basic::{LogicalType, Repetition, Type as PhysicalType};
        use parquet::data_type::{ByteArray, ByteArrayType, Int64Type};
        use parquet::file::properties::WriterProperties;
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::types::Type;

        let mut fields = vec![
            Arc::new(
                Type::primitive_type_builder("_dedup_key", PhysicalType::BYTE_ARRAY)
                    .with_logical_type(Some(LogicalType::String))
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .map_err(|e| RelayError::SinkPublish { sink: "delta".to_string(), source: Box::new(e) })?,
            ),
            Arc::new(
                Type::primitive_type_builder("_subject", PhysicalType::BYTE_ARRAY)
                    .with_logical_type(Some(LogicalType::String))
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .map_err(|e| RelayError::SinkPublish { sink: "delta".to_string(), source: Box::new(e) })?,
            ),
            Arc::new(
                Type::primitive_type_builder("_op", PhysicalType::BYTE_ARRAY)
                    .with_logical_type(Some(LogicalType::String))
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .map_err(|e| RelayError::SinkPublish { sink: "delta".to_string(), source: Box::new(e) })?,
            ),
            Arc::new(
                Type::primitive_type_builder("_outbox_id", PhysicalType::INT64)
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .map_err(|e| RelayError::SinkPublish { sink: "delta".to_string(), source: Box::new(e) })?,
            ),
            Arc::new(
                Type::primitive_type_builder("data", PhysicalType::BYTE_ARRAY)
                    .with_logical_type(Some(LogicalType::String))
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .map_err(|e| RelayError::SinkPublish { sink: "delta".to_string(), source: Box::new(e) })?,
            ),
        ];

        if include_cdf {
            fields.push(Arc::new(
                Type::primitive_type_builder("_change_type", PhysicalType::BYTE_ARRAY)
                    .with_logical_type(Some(LogicalType::String))
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .map_err(|e| RelayError::SinkPublish { sink: "delta".to_string(), source: Box::new(e) })?,
            ));
        }

        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(fields)
                .build()
                .map_err(|e| RelayError::SinkPublish { sink: "delta".to_string(), source: Box::new(e) })?,
        );

        let props = Arc::new(WriterProperties::builder().build());
        let mut buf: Vec<u8> = Vec::new();
        let cursor = std::io::Cursor::new(&mut buf);
        let mut writer = SerializedFileWriter::new(cursor, schema, props)
            .map_err(|e| RelayError::SinkPublish { sink: "delta".to_string(), source: Box::new(e) })?;

        let n = messages.len();
        let mut dedup_keys: Vec<ByteArray> = Vec::with_capacity(n);
        let mut subjects: Vec<ByteArray> = Vec::with_capacity(n);
        let mut ops: Vec<ByteArray> = Vec::with_capacity(n);
        let mut outbox_ids: Vec<i64> = Vec::with_capacity(n);
        let mut outbox_def: Vec<i16> = Vec::with_capacity(n);
        let mut data_vals: Vec<ByteArray> = Vec::with_capacity(n);
        let mut cdf_vals: Vec<ByteArray> = Vec::with_capacity(n);

        for msg in messages {
            dedup_keys.push(ByteArray::from(msg.dedup_key.as_str()));
            subjects.push(ByteArray::from(msg.subject.as_str()));
            ops.push(ByteArray::from(msg.op.as_str()));
            if let Some(id) = msg.outbox_id {
                outbox_ids.push(id);
                outbox_def.push(1);
            } else {
                outbox_ids.push(0);
                outbox_def.push(0);
            }
            let data_str = serde_json::to_string(&msg.payload).unwrap_or_else(|_| "null".to_string());
            data_vals.push(ByteArray::from(data_str.as_str()));
            if include_cdf {
                let ct = match msg.op.as_str() {
                    "delete" => "delete",
                    "insert" => "insert",
                    _ => "update_postimage",
                };
                cdf_vals.push(ByteArray::from(ct));
            }
        }

        let mut row_group = writer.next_row_group()
            .map_err(|e| RelayError::SinkPublish { sink: "delta".to_string(), source: Box::new(e) })?;

        macro_rules! write_ba_col {
            ($vals:expr) => {{
                let mut cw = row_group.next_column().unwrap().unwrap();
                cw.typed::<ByteArrayType>().write_batch(&$vals, None, None)
                    .map_err(|e| RelayError::SinkPublish { sink: "delta".to_string(), source: Box::new(e) })?;
                cw.close().map_err(|e| RelayError::SinkPublish { sink: "delta".to_string(), source: Box::new(e) })?;
            }};
        }

        write_ba_col!(dedup_keys);
        write_ba_col!(subjects);
        write_ba_col!(ops);

        {
            let mut cw = row_group.next_column().unwrap().unwrap();
            cw.typed::<Int64Type>()
                .write_batch(&outbox_ids, Some(&outbox_def), None)
                .map_err(|e| RelayError::SinkPublish { sink: "delta".to_string(), source: Box::new(e) })?;
            cw.close().map_err(|e| RelayError::SinkPublish { sink: "delta".to_string(), source: Box::new(e) })?;
        }

        write_ba_col!(data_vals);
        if include_cdf {
            write_ba_col!(cdf_vals);
        }

        row_group.close()
            .map_err(|e| RelayError::SinkPublish { sink: "delta".to_string(), source: Box::new(e) })?;
        writer.close()
            .map_err(|e| RelayError::SinkPublish { sink: "delta".to_string(), source: Box::new(e) })?;

        Ok(buf)
    }
}

#[cfg(feature = "delta")]
#[async_trait::async_trait]
impl super::Sink for DeltaSink {
    fn name(&self) -> &str {
        "delta"
    }

    async fn publish(&mut self, messages: &[RelayMessage]) -> Result<(), RelayError> {
        if messages.is_empty() {
            return Ok(());
        }

        self.ensure_initialized().await?;

        self.version += 1;
        let version = self.version;
        let now_ms = Utc::now().timestamp_millis();

        let file_name = format!("part-{:05}-{}.snappy.parquet", version, now_ms);
        let data_file_path = format!(
            "{}/{}",
            self.config.table_path.trim_end_matches('/'),
            file_name
        );

        let batch: Vec<&RelayMessage> = messages.iter().collect();
        let parquet_bytes = Self::build_parquet_bytes(&batch, self.config.change_data_feed)?;
        let size_bytes = parquet_bytes.len() as i64;

        // Write Parquet data file.
        let parquet_obj_path = Path::from(data_file_path.trim_start_matches('/'));
        self.store
            .put(&parquet_obj_path, parquet_bytes.into())
            .await
            .map_err(|e| RelayError::SinkPublish { sink: "delta".to_string(), source: Box::new(e) })?;

        // Write Delta Log entry.
        let add_action =
            self.config
                .build_add_action(&file_name, messages.len() as i64, size_bytes);
        let commit_content = add_action.to_string();
        let log_path = Path::from(
            self.config
                .log_entry_path(version)
                .trim_start_matches('/'),
        );
        self.store
            .put(&log_path, commit_content.into_bytes().into())
            .await
            .map_err(|e| RelayError::SinkPublish { sink: "delta".to_string(), source: Box::new(e) })?;

        Ok(())
    }

    async fn is_healthy(&mut self) -> bool {
        true
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}
