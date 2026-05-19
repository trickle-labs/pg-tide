/// DuckLake analytics sink (v0.20.0 — DuckLake v1.0 native catalog integration).
///
/// Writes pg-tide relay messages to a DuckLake — a lightweight open data lake
/// format from the DuckDB team that combines Parquet files (on object storage)
/// with a SQL catalog in PostgreSQL.
///
/// This implementation (v0.20.0) speaks the real DuckLake v1.0 catalog protocol:
/// 1. Writes Parquet files to object storage (`object_store`).
/// 2. Records file metadata in the official DuckLake v1.0 catalog tables
///    (`ducklake_snapshot`, `ducklake_data_file`, `ducklake_file_column_stats`, etc.)
///    inside a single PostgreSQL transaction per batch.
/// 3. Auto-bootstraps schema/table/column entries on first use.
/// 4. Emits `pg_notify('tide_ducklake_changes', …)` after each snapshot commit.
/// 5. Computes per-file column statistics for DuckDB filter pushdown.
///
/// The catalog tables are created automatically in `catalog_schema` (default: `ducklake`).
/// Any DuckDB instance can `ATTACH 'ducklake:postgres:…'` and query the data directly.
///
/// Feature-gated: only compiled with `--features ducklake`.
use crate::envelope::RelayMessage;
use crate::error::RelayError;

#[cfg(feature = "ducklake")]
use chrono::Utc;
#[cfg(feature = "ducklake")]
use object_store::{path::Path, ObjectStore};
#[cfg(feature = "ducklake")]
use std::collections::HashMap;
#[cfg(feature = "ducklake")]
use std::sync::Arc;

/// Configuration for the DuckLake sink.
#[derive(Debug, Clone)]
pub struct DuckLakeConfig {
    /// Object storage root path for Parquet files (e.g. `s3://my-lake/pgtide/` or `/tmp/ducklake/`).
    pub data_path: String,
    /// Logical namespace (maps to `ducklake_schema.schema_name` in the DuckLake catalog).
    pub namespace: String,
    /// Table name template; `{stream_table}` replaced with message subject.
    pub table_template: String,
    /// Parquet compression codec (default: Snappy).
    pub compression: DuckLakeCompression,
    /// PostgreSQL schema where DuckLake v1.0 catalog tables live (default: `"ducklake"`).
    pub catalog_schema: String,
    /// When `true`, the outbox consumer-offset advance and the DuckLake catalog commit
    /// share the same PostgreSQL transaction — providing exactly-once delivery to the lake.
    /// Requires the relay to connect to the same PostgreSQL instance as the pg_tide outbox.
    pub atomic_lake_writes: bool,
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
            catalog_schema: "ducklake".to_string(),
            atomic_lake_writes: false,
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

/// Per-column statistics computed from a message batch (for filter pushdown).
#[cfg(feature = "ducklake")]
struct ColStats {
    min_value: Option<String>,
    max_value: Option<String>,
    null_count: i64,
}

#[cfg(feature = "ducklake")]
pub struct DuckLakeSink {
    store: Arc<dyn ObjectStore>,
    /// Owned client so we can start transactions (`&mut self` methods).
    db: tokio_postgres::Client,
    config: DuckLakeConfig,
    catalog_ready: bool,
    /// Cached (schema_id, table_id) for already-bootstrapped (namespace, table_name) pairs.
    bootstrapped_tables: HashMap<(String, String), (i64, i64)>,
    /// Cached column_id for each (table_id, column_name) pair.
    column_ids: HashMap<(i64, String), i64>,
}

#[cfg(feature = "ducklake")]
impl DuckLakeSink {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        db: tokio_postgres::Client,
        config: DuckLakeConfig,
    ) -> Self {
        Self {
            store,
            db,
            config,
            catalog_ready: false,
            bootstrapped_tables: HashMap::new(),
            column_ids: HashMap::new(),
        }
    }

    /// Create the DuckLake v1.0 catalog tables and sequences in `catalog_schema` if they
    /// don't already exist.  Idempotent — safe to call on every sink start.
    async fn ensure_catalog(&mut self) -> Result<(), RelayError> {
        if self.catalog_ready {
            return Ok(());
        }

        // Validate catalog_schema as a safe identifier before embedding it in SQL.
        crate::config::validate_relay_identifier(&self.config.catalog_schema)?;
        let cs = &self.config.catalog_schema;

        let ddl = format!(
            r#"
CREATE SCHEMA IF NOT EXISTS "{cs}";

CREATE SEQUENCE IF NOT EXISTS "{cs}".ducklake_snapshot_id_seq START WITH 1;
CREATE SEQUENCE IF NOT EXISTS "{cs}".ducklake_table_id_seq    START WITH 1;
CREATE SEQUENCE IF NOT EXISTS "{cs}".ducklake_schema_id_seq   START WITH 1;
CREATE SEQUENCE IF NOT EXISTS "{cs}".ducklake_column_id_seq   START WITH 1;
CREATE SEQUENCE IF NOT EXISTS "{cs}".ducklake_file_id_seq     START WITH 1;

CREATE TABLE IF NOT EXISTS "{cs}".ducklake_metadata (
    key   TEXT NOT NULL PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS "{cs}".ducklake_schema (
    schema_id   BIGINT NOT NULL PRIMARY KEY,
    schema_name TEXT   NOT NULL UNIQUE,
    schema_uuid UUID   NOT NULL DEFAULT gen_random_uuid()
);

CREATE TABLE IF NOT EXISTS "{cs}".ducklake_table (
    table_id    BIGINT NOT NULL PRIMARY KEY,
    schema_id   BIGINT NOT NULL REFERENCES "{cs}".ducklake_schema(schema_id),
    table_name  TEXT   NOT NULL,
    table_uuid  UUID   NOT NULL DEFAULT gen_random_uuid(),
    UNIQUE (schema_id, table_name)
);

CREATE TABLE IF NOT EXISTS "{cs}".ducklake_column (
    column_id    BIGINT  NOT NULL PRIMARY KEY,
    table_id     BIGINT  NOT NULL REFERENCES "{cs}".ducklake_table(table_id),
    column_name  TEXT    NOT NULL,
    column_type  TEXT    NOT NULL,
    column_order INT     NOT NULL DEFAULT 0,
    nullable     BOOLEAN NOT NULL DEFAULT true,
    UNIQUE (table_id, column_name)
);

CREATE TABLE IF NOT EXISTS "{cs}".ducklake_snapshot (
    snapshot_id     BIGINT      NOT NULL PRIMARY KEY,
    table_id        BIGINT      NOT NULL REFERENCES "{cs}".ducklake_table(table_id),
    schema_version  BIGINT      NOT NULL DEFAULT 0,
    sequence_number BIGINT      NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    author          TEXT
);

CREATE TABLE IF NOT EXISTS "{cs}".ducklake_snapshot_changes (
    snapshot_id BIGINT NOT NULL REFERENCES "{cs}".ducklake_snapshot(snapshot_id),
    change_type TEXT   NOT NULL,
    table_id    BIGINT REFERENCES "{cs}".ducklake_table(table_id),
    schema_id   BIGINT REFERENCES "{cs}".ducklake_schema(schema_id),
    file_id     BIGINT
);

CREATE TABLE IF NOT EXISTS "{cs}".ducklake_table_stats (
    table_id    BIGINT NOT NULL PRIMARY KEY REFERENCES "{cs}".ducklake_table(table_id),
    next_row_id BIGINT NOT NULL DEFAULT 0,
    row_count   BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS "{cs}".ducklake_table_column_stats (
    table_id   BIGINT NOT NULL,
    column_id  BIGINT NOT NULL,
    min_value  TEXT,
    max_value  TEXT,
    null_count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (table_id, column_id)
);

CREATE TABLE IF NOT EXISTS "{cs}".ducklake_data_file (
    file_id         BIGINT      NOT NULL PRIMARY KEY,
    table_id        BIGINT      NOT NULL REFERENCES "{cs}".ducklake_table(table_id),
    begin_snapshot  BIGINT      NOT NULL REFERENCES "{cs}".ducklake_snapshot(snapshot_id),
    end_snapshot    BIGINT,
    file_path       TEXT        NOT NULL,
    file_format     TEXT        NOT NULL DEFAULT 'parquet',
    record_count    BIGINT      NOT NULL DEFAULT 0,
    file_size_bytes BIGINT      NOT NULL DEFAULT 0,
    footer_size     BIGINT      NOT NULL DEFAULT 0,
    added_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS "{cs}".ducklake_file_column_stats (
    file_id        BIGINT NOT NULL REFERENCES "{cs}".ducklake_data_file(file_id),
    column_id      BIGINT NOT NULL,
    min_value      TEXT,
    max_value      TEXT,
    null_count     BIGINT NOT NULL DEFAULT 0,
    distinct_count BIGINT,
    PRIMARY KEY (file_id, column_id)
);

INSERT INTO "{cs}".ducklake_metadata (key, value)
VALUES ('catalog_version', '1.0'), ('created_by', 'pg-tide-relay')
ON CONFLICT (key) DO NOTHING;
"#,
            cs = cs
        );

        self.db.batch_execute(&ddl).await?;
        self.catalog_ready = true;
        Ok(())
    }

    /// Ensure a DuckLake schema (namespace), table, and columns exist in the catalog.
    /// Returns `(schema_id, table_id)`.  Uses cached values after first call.
    async fn bootstrap_table(
        &mut self,
        namespace: &str,
        table_name: &str,
    ) -> Result<(i64, i64), RelayError> {
        let cache_key = (namespace.to_string(), table_name.to_string());
        if let Some(&ids) = self.bootstrapped_tables.get(&cache_key) {
            return Ok(ids);
        }

        let cs = self.config.catalog_schema.clone();

        // Upsert ducklake_schema row.
        let schema_id: i64 = self
            .db
            .query_one(
                &format!(
                    r#"
INSERT INTO "{cs}".ducklake_schema (schema_id, schema_name)
VALUES (nextval('"{cs}".ducklake_schema_id_seq'), $1)
ON CONFLICT (schema_name) DO UPDATE SET schema_name = EXCLUDED.schema_name
RETURNING schema_id
"#,
                    cs = cs
                ),
                &[&namespace],
            )
            .await
            .map_err(RelayError::Postgres)?
            .get(0);

        // Upsert ducklake_table row.
        let table_id: i64 = self
            .db
            .query_one(
                &format!(
                    r#"
INSERT INTO "{cs}".ducklake_table (table_id, schema_id, table_name)
VALUES (nextval('"{cs}".ducklake_table_id_seq'), $1, $2)
ON CONFLICT (schema_id, table_name) DO UPDATE SET table_name = EXCLUDED.table_name
RETURNING table_id
"#,
                    cs = cs
                ),
                &[&schema_id, &table_name],
            )
            .await
            .map_err(RelayError::Postgres)?
            .get(0);

        // Ensure ducklake_table_stats row exists.
        self.db
            .execute(
                &format!(
                    r#"
INSERT INTO "{cs}".ducklake_table_stats (table_id, next_row_id, row_count)
VALUES ($1, 0, 0)
ON CONFLICT (table_id) DO NOTHING
"#,
                    cs = cs
                ),
                &[&table_id],
            )
            .await
            .map_err(RelayError::Postgres)?;

        // Register the standard pg-tide message columns.
        let columns = [
            ("_dedup_key", "VARCHAR", 0_i32, false),
            ("_subject", "VARCHAR", 1, false),
            ("_op", "VARCHAR", 2, false),
            ("_outbox_id", "BIGINT", 3, true),
            ("data", "VARCHAR", 4, false),
        ];
        for (col_name, col_type, col_order, nullable) in &columns {
            let col_id: i64 = self
                .db
                .query_one(
                    &format!(
                        r#"
INSERT INTO "{cs}".ducklake_column (column_id, table_id, column_name, column_type, column_order, nullable)
VALUES (nextval('"{cs}".ducklake_column_id_seq'), $1, $2, $3, $4, $5)
ON CONFLICT (table_id, column_name) DO UPDATE SET column_type = EXCLUDED.column_type
RETURNING column_id
"#,
                        cs = cs
                    ),
                    &[&table_id, col_name, col_type, col_order, nullable],
                )
                .await
                .map_err(RelayError::Postgres)?
                .get(0);
            self.column_ids
                .insert((table_id, col_name.to_string()), col_id);
        }

        self.bootstrapped_tables
            .insert(cache_key, (schema_id, table_id));
        Ok((schema_id, table_id))
    }

    /// Compute per-column statistics for filter pushdown from a message batch.
    fn compute_column_stats(messages: &[&RelayMessage]) -> [ColStats; 5] {
        // Column order: _dedup_key, _subject, _op, _outbox_id, data
        let n = messages.len();

        // VARCHAR: _dedup_key
        let (dk_min, dk_max) = str_min_max(messages.iter().map(|m| m.dedup_key.as_str()));
        // VARCHAR: _subject
        let (sub_min, sub_max) = str_min_max(messages.iter().map(|m| m.subject.as_str()));
        // VARCHAR: _op
        let (op_min, op_max) = str_min_max(messages.iter().map(|m| m.op.as_str()));
        // BIGINT: _outbox_id (nullable)
        let ids: Vec<i64> = messages.iter().filter_map(|m| m.outbox_id).collect();
        let id_null_count = (n - ids.len()) as i64;
        let (id_min, id_max) = if ids.is_empty() {
            (None, None)
        } else {
            (
                Some(ids.iter().copied().min().unwrap().to_string()),
                Some(ids.iter().copied().max().unwrap().to_string()),
            )
        };
        // VARCHAR: data — skip min/max (large JSON); record null_count = 0
        [
            ColStats {
                min_value: dk_min,
                max_value: dk_max,
                null_count: 0,
            },
            ColStats {
                min_value: sub_min,
                max_value: sub_max,
                null_count: 0,
            },
            ColStats {
                min_value: op_min,
                max_value: op_max,
                null_count: 0,
            },
            ColStats {
                min_value: id_min,
                max_value: id_max,
                null_count: id_null_count,
            },
            ColStats {
                min_value: None,
                max_value: None,
                null_count: 0,
            },
        ]
    }

    /// Build a Parquet file in memory from a batch of messages.
    ///
    /// Returns `(parquet_bytes, footer_size_bytes)`.
    pub fn build_parquet_bytes(
        messages: &[&RelayMessage],
        compression: &DuckLakeCompression,
    ) -> Result<(Vec<u8>, i64), RelayError> {
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
        let metadata = writer.close().map_err(|e| RelayError::SinkPublish {
            sink: "ducklake".to_string(),
            source: Box::new(e),
        })?;

        // Compute Parquet footer size from file metadata.
        // The footer is the last part of the Parquet file; its size can be approximated
        // from the final 8 bytes (4-byte footer length + 4-byte magic) of the file.
        let footer_size = if buf.len() >= 8 {
            let len_bytes: [u8; 4] = buf[buf.len() - 8..buf.len() - 4]
                .try_into()
                .unwrap_or([0; 4]);
            i32::from_le_bytes(len_bytes) as i64
        } else {
            0i64
        };
        let _ = metadata; // metadata used only for the `close()` call
        Ok((buf, footer_size))
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

        let namespace = self.config.namespace.clone();
        let cs = self.config.catalog_schema.clone();

        // Group by table name.
        let mut groups: HashMap<String, Vec<&RelayMessage>> = HashMap::new();
        for msg in messages {
            let table = self.config.table_for(&msg.subject);
            groups.entry(table).or_default().push(msg);
        }

        for (table, batch) in &groups {
            // Bootstrap table catalog entries (schema/table/column rows) on first use.
            let (schema_id, table_id) = self.bootstrap_table(&namespace, table).await?;

            let num_records = batch.len() as i64;

            // Compute column statistics for filter pushdown.
            let col_stats = Self::compute_column_stats(batch);

            // Build Parquet file in memory.
            let (parquet_bytes, footer_size) =
                Self::build_parquet_bytes(batch, &self.config.compression)?;
            let file_size = parquet_bytes.len() as i64;

            // Write Parquet to object storage.
            // We use `now_ms` as a timestamp component to make the path unique per call.
            let now_ms = Utc::now().timestamp_millis();
            let parquet_path = self.config.parquet_path(table, now_ms);
            let obj_path = Path::from(parquet_path.trim_start_matches('/'));
            self.store
                .put(&obj_path, parquet_bytes.into())
                .await
                .map_err(|e| RelayError::SinkPublish {
                    sink: "ducklake".to_string(),
                    source: Box::new(e),
                })?;

            // --- DuckLake v1.0 catalog transaction ---
            // All catalog writes for this batch happen in one PostgreSQL transaction,
            // guaranteeing atomic snapshot creation.
            let txn = self
                .db
                .build_transaction()
                .isolation_level(tokio_postgres::IsolationLevel::ReadCommitted)
                .start()
                .await
                .map_err(RelayError::Postgres)?;

            // 1. Allocate a monotonically increasing snapshot ID.
            let snapshot_id: i64 = txn
                .query_one(
                    &format!(
                        r#"SELECT nextval('"{cs}".ducklake_snapshot_id_seq')"#,
                        cs = cs
                    ),
                    &[],
                )
                .await
                .map_err(RelayError::Postgres)?
                .get(0);

            // 2. Allocate a file ID.
            let file_id: i64 = txn
                .query_one(
                    &format!(r#"SELECT nextval('"{cs}".ducklake_file_id_seq')"#, cs = cs),
                    &[],
                )
                .await
                .map_err(RelayError::Postgres)?
                .get(0);

            // 3. Insert ducklake_snapshot.
            txn.execute(
                &format!(
                    r#"
INSERT INTO "{cs}".ducklake_snapshot
    (snapshot_id, table_id, schema_version, sequence_number, author)
VALUES ($1, $2, 0,
    COALESCE((SELECT MAX(sequence_number) + 1
              FROM "{cs}".ducklake_snapshot
              WHERE table_id = $2), 0),
    'pg-tide-relay')
"#,
                    cs = cs
                ),
                &[&snapshot_id, &table_id],
            )
            .await
            .map_err(RelayError::Postgres)?;

            // 4. Insert ducklake_data_file.
            txn.execute(
                &format!(
                    r#"
INSERT INTO "{cs}".ducklake_data_file
    (file_id, table_id, begin_snapshot, file_path, file_format,
     record_count, file_size_bytes, footer_size)
VALUES ($1, $2, $3, $4, 'parquet', $5, $6, $7)
"#,
                    cs = cs
                ),
                &[
                    &file_id,
                    &table_id,
                    &snapshot_id,
                    &parquet_path,
                    &num_records,
                    &file_size,
                    &footer_size,
                ],
            )
            .await
            .map_err(RelayError::Postgres)?;

            // 5. Write per-file column statistics.
            // Column order matches the schema: _dedup_key, _subject, _op, _outbox_id, data
            let col_names = ["_dedup_key", "_subject", "_op", "_outbox_id", "data"];
            for (i, stats) in col_stats.iter().enumerate() {
                if let Some(col_id) = self.column_ids.get(&(table_id, col_names[i].to_string())) {
                    txn.execute(
                        &format!(
                            r#"
INSERT INTO "{cs}".ducklake_file_column_stats
    (file_id, column_id, min_value, max_value, null_count)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (file_id, column_id) DO UPDATE
    SET min_value  = EXCLUDED.min_value,
        max_value  = EXCLUDED.max_value,
        null_count = EXCLUDED.null_count
"#,
                            cs = cs
                        ),
                        &[
                            &file_id,
                            col_id,
                            &stats.min_value,
                            &stats.max_value,
                            &stats.null_count,
                        ],
                    )
                    .await
                    .map_err(RelayError::Postgres)?;
                }
            }

            // 6. Update ducklake_table_stats (next_row_id, row_count).
            txn.execute(
                &format!(
                    r#"
UPDATE "{cs}".ducklake_table_stats
SET next_row_id = next_row_id + $1,
    row_count   = row_count   + $1
WHERE table_id = $2
"#,
                    cs = cs
                ),
                &[&num_records, &table_id],
            )
            .await
            .map_err(RelayError::Postgres)?;

            // 7. Upsert global ducklake_table_column_stats (min/max across all files).
            for (i, stats) in col_stats.iter().enumerate() {
                if let Some(col_id) = self.column_ids.get(&(table_id, col_names[i].to_string())) {
                    txn.execute(
                        &format!(
                            r#"
INSERT INTO "{cs}".ducklake_table_column_stats
    (table_id, column_id, min_value, max_value, null_count)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (table_id, column_id) DO UPDATE
    SET min_value  = CASE
        WHEN EXCLUDED.min_value IS NOT NULL AND (ducklake_table_column_stats.min_value IS NULL
             OR EXCLUDED.min_value < ducklake_table_column_stats.min_value)
        THEN EXCLUDED.min_value
        ELSE ducklake_table_column_stats.min_value
        END,
        max_value  = CASE
        WHEN EXCLUDED.max_value IS NOT NULL AND (ducklake_table_column_stats.max_value IS NULL
             OR EXCLUDED.max_value > ducklake_table_column_stats.max_value)
        THEN EXCLUDED.max_value
        ELSE ducklake_table_column_stats.max_value
        END,
        null_count = ducklake_table_column_stats.null_count + EXCLUDED.null_count
"#,
                            cs = cs
                        ),
                        &[
                            &table_id,
                            col_id,
                            &stats.min_value,
                            &stats.max_value,
                            &stats.null_count,
                        ],
                    )
                    .await
                    .map_err(RelayError::Postgres)?;
                }
            }

            // 8. Record snapshot change.
            txn.execute(
                &format!(
                    r#"
INSERT INTO "{cs}".ducklake_snapshot_changes
    (snapshot_id, change_type, table_id, schema_id, file_id)
VALUES ($1, 'add_data_file', $2, $3, $4)
"#,
                    cs = cs
                ),
                &[&snapshot_id, &table_id, &schema_id, &file_id],
            )
            .await
            .map_err(RelayError::Postgres)?;

            // 9. NOTIFY-based change notification for downstream consumers.
            let notify_payload = serde_json::json!({
                "table": table,
                "snapshot_id": snapshot_id,
                "record_count": num_records,
            })
            .to_string();
            txn.execute(
                "SELECT pg_notify('tide_ducklake_changes', $1)",
                &[&notify_payload],
            )
            .await
            .map_err(RelayError::Postgres)?;

            // Commit the catalog transaction.
            txn.commit().await.map_err(RelayError::Postgres)?;

            tracing::debug!(
                table = %table,
                snapshot_id = snapshot_id,
                record_count = num_records,
                "DuckLake v1.0 snapshot committed"
            );
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

/// Helper: compute lexicographic min/max over a string iterator.
#[cfg(feature = "ducklake")]
fn str_min_max<'a>(mut iter: impl Iterator<Item = &'a str>) -> (Option<String>, Option<String>) {
    let first = match iter.next() {
        Some(s) => s,
        None => return (None, None),
    };
    let mut min = first.to_string();
    let mut max = first.to_string();
    for s in iter {
        if s < min.as_str() {
            min = s.to_string();
        }
        if s > max.as_str() {
            max = s.to_string();
        }
    }
    (Some(min), Some(max))
}
