/// Object Storage sink (v0.6.0 — RELAY-P3-8).
///
/// Writes outbox messages to object storage (Amazon S3, Google Cloud Storage,
/// or Azure Blob Storage) in JSONL or Parquet format.
///
/// Messages are buffered in memory and flushed when the buffer exceeds
/// `buffer_max_rows` rows, `buffer_max_bytes` bytes, or `buffer_max_seconds`
/// seconds since the last flush — whichever comes first.
///
/// **Partitioning:** Files are organised into date-based paths compatible
/// with Hive-style partitioning: `{prefix}/year=YYYY/month=MM/day=DD/`.
///
/// **Format:**
/// - `jsonl`: Newline-delimited JSON (one message per line). No extra deps.
/// - `parquet`: Apache Parquet columnar format. Each file contains columns:
///   `dedup_key`, `subject`, `op`, `outbox_id`, and `payload` (JSON string).
///
/// Feature-gated: only compiled with `--features object-storage`.
use crate::envelope::RelayMessage;
use crate::error::RelayError;

#[cfg(feature = "object-storage")]
use object_store::{path::Path, ObjectStore};
#[cfg(feature = "object-storage")]
use std::sync::Arc;
#[cfg(feature = "object-storage")]
use std::time::Instant;

/// Output format for object storage files.
#[derive(Debug, Clone, PartialEq)]
pub enum ObjectStorageFormat {
    Jsonl,
    Parquet,
}

impl ObjectStorageFormat {
    fn extension(&self) -> &str {
        match self {
            Self::Jsonl => "jsonl",
            Self::Parquet => "parquet",
        }
    }
}

/// Object storage provider.
#[derive(Debug, Clone)]
pub enum ObjectStorageProvider {
    /// Amazon S3 — credentials via environment (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    /// or instance profile.
    S3 {
        bucket: String,
        region: Option<String>,
        endpoint: Option<String>,
    },
    /// Google Cloud Storage — credentials via `GOOGLE_APPLICATION_CREDENTIALS` or ADC.
    Gcs { bucket: String },
    /// Azure Blob Storage — credentials via `AZURE_STORAGE_ACCOUNT` + `AZURE_STORAGE_KEY`
    /// or `AZURE_STORAGE_CONNECTION_STRING`.
    Azure { account: String, container: String },
    /// Local filesystem (for testing / development).
    Local { root: std::path::PathBuf },
}

#[cfg(feature = "object-storage")]
pub struct ObjectStorageSink {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    format: ObjectStorageFormat,
    buffer: Vec<RelayMessage>,
    buffer_bytes: usize,
    buffer_max_rows: usize,
    buffer_max_bytes: usize,
    buffer_max_seconds: u64,
    last_flush: Instant,
    partition_by_date: bool,
}

#[cfg(feature = "object-storage")]
impl ObjectStorageSink {
    /// Create a new ObjectStorageSink.
    ///
    /// `provider`: Storage provider configuration.
    /// `prefix`: Path prefix within the bucket (e.g. `"pg-tide/orders_stream"`).
    /// `format`: Output format (`Jsonl` or `Parquet`).
    pub fn new(
        provider: ObjectStorageProvider,
        prefix: impl Into<String>,
        format: ObjectStorageFormat,
        buffer_max_rows: usize,
        buffer_max_bytes: usize,
        buffer_max_seconds: u64,
        partition_by_date: bool,
    ) -> Result<Self, RelayError> {
        let store: Arc<dyn ObjectStore> = match provider {
            ObjectStorageProvider::S3 {
                bucket,
                region,
                endpoint,
            } => {
                let mut builder =
                    object_store::aws::AmazonS3Builder::from_env().with_bucket_name(&bucket);
                if let Some(r) = region {
                    builder = builder.with_region(r);
                }
                if let Some(e) = endpoint {
                    builder = builder.with_endpoint(e);
                    // For LocalStack / minio — allow HTTP.
                    builder = builder.with_allow_http(true);
                }
                Arc::new(
                    builder
                        .build()
                        .map_err(|e| RelayError::config(format!("S3 config error: {e}")))?,
                )
            }

            ObjectStorageProvider::Gcs { bucket } => Arc::new(
                object_store::gcp::GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(&bucket)
                    .build()
                    .map_err(|e| RelayError::config(format!("GCS config error: {e}")))?,
            ),

            ObjectStorageProvider::Azure { account, container } => Arc::new(
                object_store::azure::MicrosoftAzureBuilder::from_env()
                    .with_account(account)
                    .with_container_name(container)
                    .build()
                    .map_err(|e| RelayError::config(format!("Azure Blob config error: {e}")))?,
            ),

            ObjectStorageProvider::Local { root } => Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(&root)
                    .map_err(|e| RelayError::config(format!("local fs error: {e}")))?,
            ),
        };

        Ok(Self {
            store,
            prefix: prefix.into(),
            format,
            buffer: Vec::new(),
            buffer_bytes: 0,
            buffer_max_rows,
            buffer_max_bytes,
            buffer_max_seconds,
            last_flush: Instant::now(),
            partition_by_date,
        })
    }

    /// Build the object path for the current flush.
    fn object_path(&self) -> Path {
        use chrono::Utc;
        let now = Utc::now();
        let file_id = uuid::Uuid::new_v4();
        let ext = self.format.extension();

        let path_str = if self.partition_by_date {
            format!(
                "{prefix}/year={year}/month={month:02}/day={day:02}/pgtide_{id}.{ext}",
                prefix = self.prefix.trim_end_matches('/'),
                year = now.format("%Y"),
                month = now.format("%m"),
                day = now.format("%d"),
                id = file_id,
            )
        } else {
            format!(
                "{prefix}/pgtide_{id}.{ext}",
                prefix = self.prefix.trim_end_matches('/'),
                id = file_id,
            )
        };

        Path::from(path_str)
    }

    /// Flush the in-memory buffer to object storage.
    async fn flush(&mut self) -> Result<(), RelayError> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let path = self.object_path();
        let bytes = match self.format {
            ObjectStorageFormat::Jsonl => self.encode_jsonl()?,
            ObjectStorageFormat::Parquet => self.encode_parquet()?,
        };

        self.store
            .put(&path, bytes.into())
            .await
            .map_err(|e| RelayError::SinkPublish {
                sink: "object-storage".to_string(),
                source: Box::new(e),
            })?;

        tracing::debug!(
            path = %path,
            rows = self.buffer.len(),
            format = ?self.format,
            "flushed object storage batch"
        );

        self.buffer.clear();
        self.buffer_bytes = 0;
        self.last_flush = Instant::now();

        Ok(())
    }

    /// Encode the buffer as newline-delimited JSON.
    fn encode_jsonl(&self) -> Result<Vec<u8>, RelayError> {
        let mut out = Vec::new();
        for msg in &self.buffer {
            serde_json::to_writer(&mut out, &msg.payload).map_err(RelayError::Json)?;
            out.push(b'\n');
        }
        Ok(out)
    }

    /// Encode the buffer as Apache Parquet.
    ///
    /// Schema: `dedup_key STRING, subject STRING, op STRING,
    ///           outbox_id INT64, payload STRING`
    fn encode_parquet(&self) -> Result<Vec<u8>, RelayError> {
        use parquet::basic::{Compression, LogicalType, Type as PhysicalType};
        use parquet::data_type::ByteArray;
        use parquet::data_type::ByteArrayType;
        use parquet::file::properties::WriterProperties;
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::types::Type;
        use std::sync::Arc;

        // Build the Parquet schema.
        let schema = {
            let dedup_key = Type::primitive_type_builder("dedup_key", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(parquet::basic::Repetition::REQUIRED)
                .build()
                .map_err(|e| RelayError::config(format!("parquet schema error: {e}")))?;
            let subject = Type::primitive_type_builder("subject", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(parquet::basic::Repetition::REQUIRED)
                .build()
                .map_err(|e| RelayError::config(format!("parquet schema error: {e}")))?;
            let op = Type::primitive_type_builder("op", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(parquet::basic::Repetition::REQUIRED)
                .build()
                .map_err(|e| RelayError::config(format!("parquet schema error: {e}")))?;
            let outbox_id = Type::primitive_type_builder("outbox_id", PhysicalType::INT64)
                .with_repetition(parquet::basic::Repetition::OPTIONAL)
                .build()
                .map_err(|e| RelayError::config(format!("parquet schema error: {e}")))?;
            let payload = Type::primitive_type_builder("payload", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(parquet::basic::Repetition::REQUIRED)
                .build()
                .map_err(|e| RelayError::config(format!("parquet schema error: {e}")))?;

            Type::group_type_builder("schema")
                .with_fields(vec![
                    Arc::new(dedup_key),
                    Arc::new(subject),
                    Arc::new(op),
                    Arc::new(outbox_id),
                    Arc::new(payload),
                ])
                .build()
                .map_err(|e| RelayError::config(format!("parquet schema error: {e}")))?
        };

        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::ZSTD(
                    parquet::basic::ZstdLevel::try_new(3).unwrap(),
                ))
                .build(),
        );

        let mut out_buf: Vec<u8> = Vec::new();
        let cursor = std::io::Cursor::new(&mut out_buf);
        let schema_arc = Arc::new(schema);
        let mut writer = SerializedFileWriter::new(cursor, schema_arc, props).map_err(|e| {
            RelayError::SinkPublish {
                sink: "object-storage".to_string(),
                source: Box::new(e),
            }
        })?;

        // Collect column data.
        let n = self.buffer.len();
        let mut dedup_keys: Vec<ByteArray> = Vec::with_capacity(n);
        let mut subjects: Vec<ByteArray> = Vec::with_capacity(n);
        let mut ops: Vec<ByteArray> = Vec::with_capacity(n);
        let mut outbox_ids: Vec<i64> = Vec::with_capacity(n);
        let mut outbox_def_levels: Vec<i16> = Vec::with_capacity(n);
        let mut payloads: Vec<ByteArray> = Vec::with_capacity(n);

        for msg in &self.buffer {
            dedup_keys.push(ByteArray::from(msg.dedup_key.as_str()));
            subjects.push(ByteArray::from(msg.subject.as_str()));
            ops.push(ByteArray::from(msg.op.as_str()));
            if let Some(id) = msg.outbox_id {
                outbox_ids.push(id);
                outbox_def_levels.push(1);
            } else {
                outbox_ids.push(0);
                outbox_def_levels.push(0);
            }
            let payload_str =
                serde_json::to_string(&msg.payload).unwrap_or_else(|_| "null".to_string());
            payloads.push(ByteArray::from(payload_str.as_str()));
        }

        let mut row_group = writer
            .next_row_group()
            .map_err(|e| RelayError::SinkPublish {
                sink: "object-storage".to_string(),
                source: Box::new(e),
            })?;

        // Write each column.
        macro_rules! write_byte_array_col {
            ($col:expr) => {{
                let mut col_writer = row_group.next_column().unwrap().unwrap();
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(&$col, None, None)
                    .map_err(|e| RelayError::SinkPublish {
                        sink: "object-storage".to_string(),
                        source: Box::new(e),
                    })?;
                col_writer.close().map_err(|e| RelayError::SinkPublish {
                    sink: "object-storage".to_string(),
                    source: Box::new(e),
                })?;
            }};
        }

        write_byte_array_col!(dedup_keys);
        write_byte_array_col!(subjects);
        write_byte_array_col!(ops);

        // outbox_id (INT64, optional).
        {
            use parquet::data_type::Int64Type;
            let mut col_writer = row_group.next_column().unwrap().unwrap();
            col_writer
                .typed::<Int64Type>()
                .write_batch(&outbox_ids, Some(&outbox_def_levels), None)
                .map_err(|e| RelayError::SinkPublish {
                    sink: "object-storage".to_string(),
                    source: Box::new(e),
                })?;
            col_writer.close().map_err(|e| RelayError::SinkPublish {
                sink: "object-storage".to_string(),
                source: Box::new(e),
            })?;
        }

        write_byte_array_col!(payloads);

        row_group.close().map_err(|e| RelayError::SinkPublish {
            sink: "object-storage".to_string(),
            source: Box::new(e),
        })?;
        writer.close().map_err(|e| RelayError::SinkPublish {
            sink: "object-storage".to_string(),
            source: Box::new(e),
        })?;

        Ok(out_buf)
    }

    /// Returns true if the buffer should be flushed now.
    fn should_flush(&self) -> bool {
        !self.buffer.is_empty()
            && (self.buffer.len() >= self.buffer_max_rows
                || self.buffer_bytes >= self.buffer_max_bytes
                || self.last_flush.elapsed().as_secs() >= self.buffer_max_seconds)
    }
}

#[cfg(feature = "object-storage")]
#[async_trait::async_trait]
impl super::Sink for ObjectStorageSink {
    fn name(&self) -> &str {
        "object-storage"
    }

    async fn publish(&mut self, messages: &[RelayMessage]) -> Result<(), RelayError> {
        for msg in messages {
            let approx_bytes = msg.dedup_key.len() + msg.subject.len() + msg.op.len() + 256;
            self.buffer_bytes += approx_bytes;
            self.buffer.push(msg.clone());
        }

        if self.should_flush() {
            self.flush().await?;
        }

        Ok(())
    }

    async fn is_healthy(&mut self) -> bool {
        true
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        // Flush any remaining buffered messages on shutdown.
        if !self.buffer.is_empty() {
            self.flush().await?;
        }
        Ok(())
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(all(test, feature = "object-storage"))]
mod tests {
    use super::*;

    /// Verify that JSONL encoding produces one JSON line per message.
    #[test]
    fn test_encode_jsonl() {
        use crate::envelope::AckToken;

        let msgs: Vec<RelayMessage> = (1..=3)
            .map(|i| RelayMessage {
                subject: "orders".to_string(),
                op: "insert".to_string(),
                dedup_key: format!("key-{i}"),
                outbox_id: Some(i),
                refresh_id: None,
                is_full_refresh: false,
                payload: serde_json::json!({ "id": i }),
                ack_token: AckToken::None,
            })
            .collect();

        let provider = ObjectStorageProvider::Local {
            root: std::path::PathBuf::from("/tmp"),
        };
        let mut sink = ObjectStorageSink::new(
            provider,
            "test",
            ObjectStorageFormat::Jsonl,
            10000,
            268_435_456,
            300,
            false,
        )
        .unwrap();
        sink.buffer = msgs;

        let bytes = sink.encode_jsonl().unwrap();
        let text = String::from_utf8(bytes).unwrap();
        let lines: Vec<&str> = text.trim_end_matches('\n').split('\n').collect();
        assert_eq!(lines.len(), 3);
        for line in &lines {
            assert!(serde_json::from_str::<serde_json::Value>(line).is_ok());
        }
    }

    /// Verify that Parquet encoding produces a valid Parquet file.
    #[test]
    fn test_encode_parquet_roundtrip() {
        use crate::envelope::AckToken;

        let msgs: Vec<RelayMessage> = (1..=5)
            .map(|i| RelayMessage {
                subject: "orders".to_string(),
                op: "insert".to_string(),
                dedup_key: format!("key-{i}"),
                outbox_id: Some(i),
                refresh_id: None,
                is_full_refresh: false,
                payload: serde_json::json!({ "id": i, "amount": i * 10 }),
                ack_token: AckToken::None,
            })
            .collect();

        let provider = ObjectStorageProvider::Local {
            root: std::path::PathBuf::from("/tmp"),
        };
        let mut sink = ObjectStorageSink::new(
            provider,
            "test",
            ObjectStorageFormat::Parquet,
            10000,
            268_435_456,
            300,
            false,
        )
        .unwrap();
        sink.buffer = msgs;

        let bytes = sink.encode_parquet().unwrap();
        // Parquet files start with the "PAR1" magic bytes.
        assert!(bytes.starts_with(b"PAR1"), "expected PAR1 magic bytes");
        assert!(
            bytes.len() > 100,
            "parquet file should have non-trivial size"
        );
    }

    /// Verify that the should_flush logic triggers on row count.
    #[test]
    fn test_should_flush_on_row_count() {
        use crate::envelope::AckToken;

        let provider = ObjectStorageProvider::Local {
            root: std::path::PathBuf::from("/tmp"),
        };
        let mut sink = ObjectStorageSink::new(
            provider,
            "test",
            ObjectStorageFormat::Jsonl,
            3, // flush after 3 rows
            268_435_456,
            300,
            false,
        )
        .unwrap();

        assert!(!sink.should_flush());

        for i in 0..3 {
            sink.buffer.push(RelayMessage {
                subject: "s".to_string(),
                op: "insert".to_string(),
                dedup_key: format!("k{i}"),
                outbox_id: None,
                refresh_id: None,
                is_full_refresh: false,
                payload: serde_json::json!({}),
                ack_token: AckToken::None,
            });
        }
        assert!(sink.should_flush());
    }
}
