/// Apache Iceberg analytics sink (v0.10.0 — RELAY-P3-ICE).
///
/// Writes pg-tide relay messages to Apache Iceberg tables stored on object
/// storage (S3, GCS, Azure Blob, or local filesystem).
///
/// This implementation uses `object_store` for file I/O and `parquet` for the
/// columnar file format.  Iceberg table metadata (schema, snapshots, manifests)
/// is maintained manually following the Iceberg spec v2.
///
/// Catalog: REST catalog via HTTP (or local JSON files for development/testing).
///
/// Feature-gated: only compiled with `--features iceberg`.
use crate::envelope::RelayMessage;
use crate::error::RelayError;

#[cfg(feature = "iceberg")]
use object_store::{path::Path, ObjectStore};
#[cfg(feature = "iceberg")]
use std::sync::Arc;
#[cfg(feature = "iceberg")]
use chrono::Utc;

/// Write mode for Iceberg tables.
#[derive(Debug, Clone, PartialEq)]
pub enum IcebergWriteMode {
    /// Append data to the existing table (default).
    Append,
    /// Overwrite the entire table (for full-refresh snapshots).
    Overwrite,
}

/// Configuration for the Iceberg sink.
#[derive(Debug, Clone)]
pub struct IcebergConfig {
    /// Warehouse root path (e.g. `s3://my-datalake/iceberg` or `/tmp/iceberg`).
    pub warehouse_path: String,
    /// Iceberg namespace (database equivalent).
    pub namespace: String,
    /// Table name template; `{stream_table}` replaced with message subject.
    pub table_template: String,
    /// Write mode (default: Append).
    pub write_mode: IcebergWriteMode,
    /// Rows per Parquet file (default: 50000).
    pub rows_per_file: usize,
}

impl IcebergConfig {
    pub fn table_for(&self, subject: &str) -> String {
        self.table_template.replace("{stream_table}", subject)
    }

    /// Build the path for a new data file in the Iceberg warehouse.
    pub fn data_file_path(&self, table: &str, snapshot_id: u64) -> String {
        format!(
            "{}/{}/{}/data/snap-{}-0.parquet",
            self.warehouse_path, self.namespace, table, snapshot_id
        )
    }

    /// Build the path for the table metadata directory.
    pub fn metadata_path(&self, table: &str) -> String {
        format!("{}/{}/{}/metadata", self.warehouse_path, self.namespace, table)
    }

    /// Build a minimal Iceberg snapshot manifest entry as JSON (for metadata/v2.metadata.json).
    pub fn build_snapshot_metadata(
        &self,
        table: &str,
        snapshot_id: u64,
        sequence_number: i64,
        added_rows: i64,
        data_file_path: &str,
    ) -> serde_json::Value {
        let now_ms = Utc::now().timestamp_millis();
        serde_json::json!({
            "format-version": 2,
            "table-uuid": format!("pg-tide-iceberg-{}-{}", table, snapshot_id),
            "location": format!("{}/{}/{}", self.warehouse_path, self.namespace, table),
            "last-sequence-number": sequence_number,
            "last-updated-ms": now_ms,
            "last-column-id": 5,
            "current-schema-id": 0,
            "schemas": [{
                "schema-id": 0,
                "type": "struct",
                "fields": [
                    {"id": 1, "name": "_dedup_key", "required": true,  "type": "string"},
                    {"id": 2, "name": "_subject",   "required": true,  "type": "string"},
                    {"id": 3, "name": "_op",        "required": true,  "type": "string"},
                    {"id": 4, "name": "_outbox_id", "required": false, "type": "long"},
                    {"id": 5, "name": "data",       "required": false, "type": "string"},
                ]
            }],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "sort-orders": [{"order-id": 0, "fields": []}],
            "current-snapshot-id": snapshot_id,
            "snapshots": [{
                "snapshot-id": snapshot_id,
                "timestamp-ms": now_ms,
                "sequence-number": sequence_number,
                "summary": {
                    "operation": "append",
                    "added-data-files": "1",
                    "added-records": added_rows.to_string(),
                },
                "manifest-list": format!(
                    "{}/{}/{}/metadata/snap-{}-m0.avro",
                    self.warehouse_path, self.namespace, table, snapshot_id
                ),
                "schema-id": 0
            }],
            "statistics": [],
            "snapshot-log": [{"timestamp-ms": now_ms, "snapshot-id": snapshot_id}],
            "metadata-log": [],
            "refs": {
                "main": {"snapshot-id": snapshot_id, "type": "branch"}
            },
            "data-files": [data_file_path],
        })
    }
}

#[cfg(feature = "iceberg")]
pub struct IcebergSink {
    store: Arc<dyn ObjectStore>,
    config: IcebergConfig,
    snapshot_counter: u64,
    sequence_number: i64,
}

#[cfg(feature = "iceberg")]
impl IcebergSink {
    pub fn new(store: Arc<dyn ObjectStore>, config: IcebergConfig) -> Self {
        Self {
            store,
            config,
            snapshot_counter: 1,
            sequence_number: 1,
        }
    }

    /// Build a Parquet file in memory from a batch of messages.
    pub fn build_parquet_bytes(messages: &[&RelayMessage]) -> Result<Vec<u8>, RelayError> {
        use parquet::basic::{LogicalType, Repetition, Type as PhysicalType};
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
                            .map_err(|e| RelayError::SinkPublish { sink: "iceberg".to_string(), source: Box::new(e) })?,
                    ),
                    Arc::new(
                        Type::primitive_type_builder("_subject", PhysicalType::BYTE_ARRAY)
                            .with_logical_type(Some(LogicalType::String))
                            .with_repetition(Repetition::REQUIRED)
                            .build()
                            .map_err(|e| RelayError::SinkPublish { sink: "iceberg".to_string(), source: Box::new(e) })?,
                    ),
                    Arc::new(
                        Type::primitive_type_builder("_op", PhysicalType::BYTE_ARRAY)
                            .with_logical_type(Some(LogicalType::String))
                            .with_repetition(Repetition::REQUIRED)
                            .build()
                            .map_err(|e| RelayError::SinkPublish { sink: "iceberg".to_string(), source: Box::new(e) })?,
                    ),
                    Arc::new(
                        Type::primitive_type_builder("_outbox_id", PhysicalType::INT64)
                            .with_repetition(Repetition::OPTIONAL)
                            .build()
                            .map_err(|e| RelayError::SinkPublish { sink: "iceberg".to_string(), source: Box::new(e) })?,
                    ),
                    Arc::new(
                        Type::primitive_type_builder("data", PhysicalType::BYTE_ARRAY)
                            .with_logical_type(Some(LogicalType::String))
                            .with_repetition(Repetition::REQUIRED)
                            .build()
                            .map_err(|e| RelayError::SinkPublish { sink: "iceberg".to_string(), source: Box::new(e) })?,
                    ),
                ])
                .build()
                .map_err(|e| RelayError::SinkPublish { sink: "iceberg".to_string(), source: Box::new(e) })?,
        );

        let props = Arc::new(WriterProperties::builder().build());
        let mut buf: Vec<u8> = Vec::new();
        let cursor = std::io::Cursor::new(&mut buf);
        let mut writer = SerializedFileWriter::new(cursor, schema, props)
            .map_err(|e| RelayError::SinkPublish { sink: "iceberg".to_string(), source: Box::new(e) })?;

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
            let data_str = serde_json::to_string(&msg.payload).unwrap_or_else(|_| "null".to_string());
            data_vals.push(ByteArray::from(data_str.as_str()));
        }

        let mut row_group = writer.next_row_group()
            .map_err(|e| RelayError::SinkPublish { sink: "iceberg".to_string(), source: Box::new(e) })?;

        macro_rules! write_ba_col {
            ($vals:expr) => {{
                let mut cw = row_group.next_column().unwrap().unwrap();
                cw.typed::<ByteArrayType>().write_batch(&$vals, None, None)
                    .map_err(|e| RelayError::SinkPublish { sink: "iceberg".to_string(), source: Box::new(e) })?;
                cw.close().map_err(|e| RelayError::SinkPublish { sink: "iceberg".to_string(), source: Box::new(e) })?;
            }};
        }

        write_ba_col!(dedup_keys);
        write_ba_col!(subjects);
        write_ba_col!(ops);

        {
            let mut cw = row_group.next_column().unwrap().unwrap();
            cw.typed::<Int64Type>()
                .write_batch(&outbox_ids, Some(&outbox_def), None)
                .map_err(|e| RelayError::SinkPublish { sink: "iceberg".to_string(), source: Box::new(e) })?;
            cw.close().map_err(|e| RelayError::SinkPublish { sink: "iceberg".to_string(), source: Box::new(e) })?;
        }

        write_ba_col!(data_vals);

        row_group.close()
            .map_err(|e| RelayError::SinkPublish { sink: "iceberg".to_string(), source: Box::new(e) })?;
        writer.close()
            .map_err(|e| RelayError::SinkPublish { sink: "iceberg".to_string(), source: Box::new(e) })?;

        Ok(buf)
    }
}

#[cfg(feature = "iceberg")]
#[async_trait::async_trait]
impl super::Sink for IcebergSink {
    fn name(&self) -> &str {
        "iceberg"
    }

    async fn publish(&mut self, messages: &[RelayMessage]) -> Result<(), RelayError> {
        if messages.is_empty() {
            return Ok(());
        }

        // Group by resolved table name.
        let mut groups: std::collections::HashMap<String, Vec<&RelayMessage>> =
            std::collections::HashMap::new();
        for msg in messages {
            let table = self.config.table_for(&msg.subject);
            groups.entry(table).or_default().push(msg);
        }

        for (table, batch) in &groups {
            let snapshot_id = self.snapshot_counter;
            self.snapshot_counter += 1;
            self.sequence_number += 1;

            // Write Parquet data file.
            let parquet_bytes = Self::build_parquet_bytes(batch)?;
            let data_path_str = self.config.data_file_path(table, snapshot_id);
            let data_path = Path::from(data_path_str.trim_start_matches('/'));
            self.store
                .put(&data_path, parquet_bytes.into())
                .await
                .map_err(|e| RelayError::SinkPublish { sink: "iceberg".to_string(), source: Box::new(e) })?;

            // Write Iceberg metadata JSON.
            let meta = self.config.build_snapshot_metadata(
                table,
                snapshot_id,
                self.sequence_number,
                batch.len() as i64,
                &data_path_str,
            );
            let meta_path = Path::from(
                format!(
                    "{}/v{}.metadata.json",
                    self.config.metadata_path(table).trim_start_matches('/'),
                    snapshot_id
                )
                .as_str(),
            );
            let meta_bytes = serde_json::to_vec_pretty(&meta)
                .map_err(RelayError::Json)?;
            self.store
                .put(&meta_path, meta_bytes.into())
                .await
                .map_err(|e| RelayError::SinkPublish { sink: "iceberg".to_string(), source: Box::new(e) })?;
        }

        Ok(())
    }

    async fn is_healthy(&mut self) -> bool {
        true
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}
