/// DuckLake analytics sink (v0.10.0 — RELAY-P3-DKL).
///
/// Writes pg-tide relay messages to a DuckLake — a lightweight open data lake
/// format from the DuckDB team that combines Parquet files (on object storage)
/// with a SQL catalog in PostgreSQL.
///
/// This implementation:
/// 1. Writes Parquet files to object storage (`object_store`).
/// 2. Records file metadata in a PostgreSQL-hosted DuckLake catalog
///    (`tide.ducklake_files` and `tide.ducklake_snapshots`).
///
/// The catalog tables are created automatically on first use (no migration needed
/// for the relay side; the schema is owned by the relay).
///
/// Feature-gated: only compiled with `--features ducklake`.
use crate::envelope::RelayMessage;
use crate::error::RelayError;

#[cfg(feature = "ducklake")]
use chrono::Utc;
#[cfg(feature = "ducklake")]
use object_store::{path::Path, ObjectStore};
#[cfg(feature = "ducklake")]
use std::sync::Arc;

/// Configuration for the DuckLake sink.
#[derive(Debug, Clone)]
pub struct DuckLakeConfig {
    /// Object storage root path for Parquet files (e.g. `s3://my-lake/pgtide/` or `/tmp/ducklake/`).
    pub data_path: String,
    /// Logical namespace (schema) within the DuckLake catalog.
    pub namespace: String,
    /// Table name template; `{stream_table}` replaced with message subject.
    pub table_template: String,
    /// Parquet compression codec (default: Snappy).
    pub compression: DuckLakeCompression,
}

/// Compression codec for Parquet files.
#[derive(Debug, Clone, PartialEq)]
pub enum DuckLakeCompression {
    Snappy,
    Zstd,
    None,
}

impl DuckLakeConfig {
    pub fn new(data_path: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            data_path: data_path.into(),
            namespace: namespace.into(),
            table_template: "{stream_table}".to_string(),
            compression: DuckLakeCompression::Snappy,
        }
    }

    pub fn table_for(&self, subject: &str) -> String {
        self.table_template.replace("{stream_table}", subject)
    }

    pub fn parquet_path(&self, table: &str, snapshot_id: i64) -> String {
        format!(
            "{}/{}/{}/snap_{}.parquet",
            self.data_path.trim_end_matches('/'),
            self.namespace,
            table,
            snapshot_id,
        )
    }
}

#[cfg(feature = "ducklake")]
pub struct DuckLakeSink {
    store: Arc<dyn ObjectStore>,
    db: Arc<tokio_postgres::Client>,
    config: DuckLakeConfig,
    catalog_ready: bool,
}

#[cfg(feature = "ducklake")]
impl DuckLakeSink {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        db: Arc<tokio_postgres::Client>,
        config: DuckLakeConfig,
    ) -> Self {
        Self {
            store,
            db,
            config,
            catalog_ready: false,
        }
    }

    /// Create the DuckLake catalog tables if they don't already exist.
    async fn ensure_catalog(&mut self) -> Result<(), RelayError> {
        if self.catalog_ready {
            return Ok(());
        }

        self.db
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS tide.ducklake_snapshots (
                    id             BIGSERIAL    PRIMARY KEY,
                    namespace      TEXT         NOT NULL,
                    table_name     TEXT         NOT NULL,
                    parquet_path   TEXT         NOT NULL,
                    num_records    BIGINT       NOT NULL DEFAULT 0,
                    file_size_bytes BIGINT      NOT NULL DEFAULT 0,
                    schema_json    JSONB        NOT NULL DEFAULT '{}',
                    committed_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
                );
                CREATE INDEX IF NOT EXISTS ducklake_snapshots_table_idx
                    ON tide.ducklake_snapshots (namespace, table_name, committed_at DESC);",
            )
            .await?;

        self.catalog_ready = true;
        Ok(())
    }

    /// Build a Parquet file in memory from a batch of messages.
    pub fn build_parquet_bytes(
        messages: &[&RelayMessage],
        compression: &DuckLakeCompression,
    ) -> Result<Vec<u8>, RelayError> {
        use parquet::basic::{
            Compression as PqCompression, LogicalType, Repetition, Type as PhysicalType, ZstdLevel,
        };
        use parquet::data_type::{ByteArray, ByteArrayType, Int64Type};
        use parquet::file::properties::WriterProperties;
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::types::Type;

        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![
                    Arc::new(
                        Type::primitive_type_builder("_dedup_key", PhysicalType::BYTE_ARRAY)
                            .with_logical_type(Some(LogicalType::String))
                            .with_repetition(Repetition::REQUIRED)
                            .build()
                            .map_err(|e| RelayError::SinkPublish {
                                sink: "ducklake".to_string(),
                                source: Box::new(e),
                            })?,
                    ),
                    Arc::new(
                        Type::primitive_type_builder("_subject", PhysicalType::BYTE_ARRAY)
                            .with_logical_type(Some(LogicalType::String))
                            .with_repetition(Repetition::REQUIRED)
                            .build()
                            .map_err(|e| RelayError::SinkPublish {
                                sink: "ducklake".to_string(),
                                source: Box::new(e),
                            })?,
                    ),
                    Arc::new(
                        Type::primitive_type_builder("_op", PhysicalType::BYTE_ARRAY)
                            .with_logical_type(Some(LogicalType::String))
                            .with_repetition(Repetition::REQUIRED)
                            .build()
                            .map_err(|e| RelayError::SinkPublish {
                                sink: "ducklake".to_string(),
                                source: Box::new(e),
                            })?,
                    ),
                    Arc::new(
                        Type::primitive_type_builder("_outbox_id", PhysicalType::INT64)
                            .with_repetition(Repetition::OPTIONAL)
                            .build()
                            .map_err(|e| RelayError::SinkPublish {
                                sink: "ducklake".to_string(),
                                source: Box::new(e),
                            })?,
                    ),
                    Arc::new(
                        Type::primitive_type_builder("data", PhysicalType::BYTE_ARRAY)
                            .with_logical_type(Some(LogicalType::String))
                            .with_repetition(Repetition::REQUIRED)
                            .build()
                            .map_err(|e| RelayError::SinkPublish {
                                sink: "ducklake".to_string(),
                                source: Box::new(e),
                            })?,
                    ),
                ])
                .build()
                .map_err(|e| RelayError::SinkPublish {
                    sink: "ducklake".to_string(),
                    source: Box::new(e),
                })?,
        );

        let pq_compression = match compression {
            DuckLakeCompression::Snappy => PqCompression::SNAPPY,
            DuckLakeCompression::Zstd => PqCompression::ZSTD(ZstdLevel::try_new(3).unwrap()),
            DuckLakeCompression::None => PqCompression::UNCOMPRESSED,
        };

        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(pq_compression)
                .build(),
        );
        let mut buf: Vec<u8> = Vec::new();
        let cursor = std::io::Cursor::new(&mut buf);
        let mut writer = SerializedFileWriter::new(cursor, schema, props).map_err(|e| {
            RelayError::SinkPublish {
                sink: "ducklake".to_string(),
                source: Box::new(e),
            }
        })?;

        let n = messages.len();
        let mut dedup_keys: Vec<ByteArray> = Vec::with_capacity(n);
        let mut subjects: Vec<ByteArray> = Vec::with_capacity(n);
        let mut ops: Vec<ByteArray> = Vec::with_capacity(n);
        let mut outbox_ids: Vec<i64> = Vec::with_capacity(n);
        let mut outbox_def: Vec<i16> = Vec::with_capacity(n);
        let mut data_vals: Vec<ByteArray> = Vec::with_capacity(n);

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
            let data_str =
                serde_json::to_string(&msg.payload).unwrap_or_else(|_| "null".to_string());
            data_vals.push(ByteArray::from(data_str.as_str()));
        }

        let mut row_group = writer
            .next_row_group()
            .map_err(|e| RelayError::SinkPublish {
                sink: "ducklake".to_string(),
                source: Box::new(e),
            })?;

        macro_rules! write_ba_col {
            ($vals:expr) => {{
                let mut cw = row_group.next_column().unwrap().unwrap();
                cw.typed::<ByteArrayType>()
                    .write_batch(&$vals, None, None)
                    .map_err(|e| RelayError::SinkPublish {
                        sink: "ducklake".to_string(),
                        source: Box::new(e),
                    })?;
                cw.close().map_err(|e| RelayError::SinkPublish {
                    sink: "ducklake".to_string(),
                    source: Box::new(e),
                })?;
            }};
        }

        write_ba_col!(dedup_keys);
        write_ba_col!(subjects);
        write_ba_col!(ops);

        {
            let mut cw = row_group.next_column().unwrap().unwrap();
            cw.typed::<Int64Type>()
                .write_batch(&outbox_ids, Some(&outbox_def), None)
                .map_err(|e| RelayError::SinkPublish {
                    sink: "ducklake".to_string(),
                    source: Box::new(e),
                })?;
            cw.close().map_err(|e| RelayError::SinkPublish {
                sink: "ducklake".to_string(),
                source: Box::new(e),
            })?;
        }

        write_ba_col!(data_vals);

        row_group.close().map_err(|e| RelayError::SinkPublish {
            sink: "ducklake".to_string(),
            source: Box::new(e),
        })?;
        writer.close().map_err(|e| RelayError::SinkPublish {
            sink: "ducklake".to_string(),
            source: Box::new(e),
        })?;

        Ok(buf)
    }
}

#[cfg(feature = "ducklake")]
#[async_trait::async_trait]
impl super::Sink for DuckLakeSink {
    fn name(&self) -> &str {
        "ducklake"
    }

    async fn publish(&mut self, messages: &[RelayMessage]) -> Result<(), RelayError> {
        if messages.is_empty() {
            return Ok(());
        }

        self.ensure_catalog().await?;

        // Group by table name.
        let mut groups: std::collections::HashMap<String, Vec<&RelayMessage>> =
            std::collections::HashMap::new();
        for msg in messages {
            let table = self.config.table_for(&msg.subject);
            groups.entry(table).or_default().push(msg);
        }

        for (table, batch) in &groups {
            let now_ms = Utc::now().timestamp_millis();
            let parquet_path = self.config.parquet_path(table, now_ms);

            // Build + write Parquet file.
            let parquet_bytes = Self::build_parquet_bytes(batch, &self.config.compression)?;
            let file_size = parquet_bytes.len() as i64;
            let num_records = batch.len() as i64;

            let obj_path = Path::from(parquet_path.trim_start_matches('/'));
            self.store
                .put(&obj_path, parquet_bytes.into())
                .await
                .map_err(|e| RelayError::SinkPublish {
                    sink: "ducklake".to_string(),
                    source: Box::new(e),
                })?;

            // Record snapshot in PostgreSQL catalog.
            let schema_json = serde_json::json!({
                "columns": [
                    {"name": "_dedup_key", "type": "VARCHAR"},
                    {"name": "_subject",   "type": "VARCHAR"},
                    {"name": "_op",        "type": "VARCHAR"},
                    {"name": "_outbox_id", "type": "BIGINT"},
                    {"name": "data",       "type": "VARCHAR"},
                ]
            });

            self.db
                .execute(
                    "INSERT INTO tide.ducklake_snapshots
                         (namespace, table_name, parquet_path, num_records, file_size_bytes, schema_json)
                     VALUES ($1, $2, $3, $4, $5, $6)",
                    &[
                        &self.config.namespace,
                        table,
                        &parquet_path,
                        &num_records,
                        &file_size,
                        &schema_json,
                    ],
                )
                .await?;
        }

        Ok(())
    }

    async fn is_healthy(&mut self) -> bool {
        self.db.execute("SELECT 1", &[]).await.is_ok()
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}
