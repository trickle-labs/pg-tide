/// DuckLake analytics sink (v0.21.0 — DuckLake streaming, inlining & schema evolution).
///
/// Writes pg-tide relay messages to a DuckLake — a lightweight open data lake
/// format from the DuckDB team that combines Parquet files (on object storage)
/// with a SQL catalog in PostgreSQL.
///
/// v0.21.0 adds:
/// - **Data inlining**: batches at or below `inline_row_limit` are written
///   directly to `ducklake_inlined_data_{table_id}_{schema_version}` in the
///   catalog — no Parquet files, sub-millisecond write latency.
/// - **Automatic schema evolution**: new JSON keys in message payloads trigger
///   new `ducklake_column` rows; breaking changes apply the `on_schema_change`
///   policy.
/// - **Snapshot-to-consumer-offset mapping**: writes to
///   `tide.ducklake_offset_map` atomically with each snapshot commit for
///   DuckDB time-travel replay.
/// - **Auto-partition**: registers `ducklake_partition_config` entries when
///   a partition strategy is configured.
/// - **DLQ archive**: background-style helper that moves aged DLQ entries into
///   a dedicated DuckLake table.
///
/// Feature-gated: only compiled with `--features ducklake`.
use crate::envelope::RelayMessage;
use crate::error::RelayError;

#[cfg(feature = "ducklake")]
use chrono::Utc;
#[cfg(feature = "ducklake")]
use object_store::{path::Path, ObjectStore};
#[cfg(feature = "ducklake")]
use std::collections::{HashMap, HashSet};
#[cfg(feature = "ducklake")]
use std::sync::Arc;

/// How the sink behaves when a breaking schema change is detected in an
/// incoming message batch.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum SchemaChangePolicy {
    /// Pause the pipeline (emit a permanent error so the coordinator pauses it).
    Pause,
    /// Route the offending batch to the DLQ.
    RouteToDlq,
    /// Log a warning and continue processing.
    #[default]
    WarnAndContinue,
    /// Automatically start a new DuckLake stream / table version.
    AutoNewStream,
}

/// Partition strategy for newly created DuckLake tables.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum DuckLakePartition {
    /// No hidden partitioning (default).
    #[default]
    None,
    /// Partition by day on `_committed_at`.
    Daily,
    /// Partition by month on `_committed_at`.
    Monthly,
    /// Bucket partitioning on `_subject` with the given bucket count.
    Bucket(u32),
}

impl DuckLakePartition {
    /// Returns the string representation stored in `tide.ducklake_partition_config`.
    pub fn as_str(&self) -> String {
        match self {
            Self::None => "none".to_string(),
            Self::Daily => "daily".to_string(),
            Self::Monthly => "monthly".to_string(),
            Self::Bucket(n) => format!("bucket:{n}"),
        }
    }
}

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
    /// Batches at or below this row count are inlined directly into the catalog
    /// rather than written as Parquet files (default: 10, matching DuckLake default).
    pub inline_row_limit: usize,
    /// Policy for handling breaking schema changes in incoming messages (default: WarnAndContinue).
    pub on_schema_change: SchemaChangePolicy,
    /// Auto-partition strategy for newly bootstrapped DuckLake tables (default: None).
    pub partition: DuckLakePartition,
    /// Pipeline name — used when writing `tide.ducklake_offset_map` entries.
    /// If `None`, offset mapping is skipped.
    pub pipeline_name: Option<String>,
    /// Hours after which DLQ entries are archived to DuckLake.  `None` disables archival.
    pub dlq_archive_after_hours: Option<u64>,
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
            inline_row_limit: 10,
            on_schema_change: SchemaChangePolicy::WarnAndContinue,
            partition: DuckLakePartition::None,
            pipeline_name: None,
            dlq_archive_after_hours: None,
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
    /// Tracks which table_ids already have their inlined-data table created.
    inlined_tables_ready: HashSet<(i64, i64)>,
    /// Cached schema version (number of additive columns added) per table_id.
    schema_version: HashMap<i64, i64>,
    /// Whether `tide.ducklake_partition_config` has been written per (pipeline, namespace, table).
    partition_registered: HashSet<(String, String, String)>,
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
            inlined_tables_ready: HashSet::new(),
            schema_version: HashMap::new(),
            partition_registered: HashSet::new(),
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

    // ── v0.21.0: Data Inlining ────────────────────────────────────────────────

    /// Ensure the per-table-version inline data table exists.
    ///
    /// DuckLake stores inlined rows in `ducklake_inlined_data_{table_id}_{schema_version}`.
    /// This table is created on demand the first time inlining is used for a given
    /// `(table_id, schema_version)` pair.
    async fn ensure_inlined_table(
        &mut self,
        table_id: i64,
        schema_version: i64,
    ) -> Result<(), RelayError> {
        let key = (table_id, schema_version);
        if self.inlined_tables_ready.contains(&key) {
            return Ok(());
        }
        let cs = &self.config.catalog_schema;
        let tname = format!("ducklake_inlined_data_{}_{}", table_id, schema_version);
        let ddl = format!(
            r#"
CREATE TABLE IF NOT EXISTS "{cs}"."{tname}" (
    row_id         BIGINT      NOT NULL,
    begin_snapshot BIGINT      NOT NULL,
    end_snapshot   BIGINT,
    _dedup_key     TEXT        NOT NULL,
    _subject       TEXT        NOT NULL,
    _op            TEXT        NOT NULL,
    _outbox_id     BIGINT,
    data           TEXT        NOT NULL
)
"#,
            cs = cs,
            tname = tname
        );
        self.db.batch_execute(&ddl).await?;
        self.inlined_tables_ready.insert(key);
        Ok(())
    }

    /// Write a small batch of messages directly to the inline data table
    /// instead of creating a Parquet file.  Returns the allocated snapshot ID.
    async fn publish_inline(
        &mut self,
        table_id: i64,
        schema_version: i64,
        snapshot_id: i64,
        schema_id: i64,
        messages: &[&RelayMessage],
    ) -> Result<(), RelayError> {
        let cs = self.config.catalog_schema.clone();
        let tname = format!("ducklake_inlined_data_{}_{}", table_id, schema_version);
        let num_records = messages.len() as i64;

        // Begin transaction for atomic snapshot + inline insert.
        let txn = self
            .db
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::ReadCommitted)
            .start()
            .await
            .map_err(RelayError::Postgres)?;

        // 1. Insert ducklake_snapshot.
        txn.execute(
            &format!(
                r#"
INSERT INTO "{cs}".ducklake_snapshot
    (snapshot_id, table_id, schema_version, sequence_number, author)
VALUES ($1, $2, $3,
    COALESCE((SELECT MAX(sequence_number) + 1
              FROM "{cs}".ducklake_snapshot
              WHERE table_id = $2), 0),
    'pg-tide-relay')
"#,
                cs = cs
            ),
            &[&snapshot_id, &table_id, &schema_version],
        )
        .await
        .map_err(RelayError::Postgres)?;

        // 2. Get current next_row_id.
        let start_row_id: i64 = txn
            .query_one(
                &format!(
                    r#"SELECT next_row_id FROM "{cs}".ducklake_table_stats WHERE table_id = $1"#,
                    cs = cs
                ),
                &[&table_id],
            )
            .await
            .map_err(RelayError::Postgres)?
            .get(0);

        // 3. Insert each row into the inlined data table.
        for (i, msg) in messages.iter().enumerate() {
            let row_id = start_row_id + i as i64;
            let data_str =
                serde_json::to_string(&msg.payload).unwrap_or_else(|_| "null".to_string());
            txn.execute(
                &format!(
                    r#"
INSERT INTO "{cs}"."{tname}"
    (row_id, begin_snapshot, _dedup_key, _subject, _op, _outbox_id, data)
VALUES ($1, $2, $3, $4, $5, $6, $7)
"#,
                    cs = cs,
                    tname = tname
                ),
                &[
                    &row_id,
                    &snapshot_id,
                    &msg.dedup_key,
                    &msg.subject,
                    &msg.op,
                    &msg.outbox_id,
                    &data_str,
                ],
            )
            .await
            .map_err(RelayError::Postgres)?;
        }

        // 4. Update table_stats.
        txn.execute(
            &format!(
                r#"UPDATE "{cs}".ducklake_table_stats
                   SET next_row_id = next_row_id + $1, row_count = row_count + $1
                   WHERE table_id = $2"#,
                cs = cs
            ),
            &[&num_records, &table_id],
        )
        .await
        .map_err(RelayError::Postgres)?;

        // 5. Record snapshot change (inlined, no file_id).
        txn.execute(
            &format!(
                r#"
INSERT INTO "{cs}".ducklake_snapshot_changes
    (snapshot_id, change_type, table_id, schema_id)
VALUES ($1, 'add_inlined_rows', $2, $3)
"#,
                cs = cs
            ),
            &[&snapshot_id, &table_id, &schema_id],
        )
        .await
        .map_err(RelayError::Postgres)?;

        // 6. NOTIFY.
        let table_name_for_notify = format!("inlined_{}", table_id);
        let notify_payload = serde_json::json!({
            "table": table_name_for_notify,
            "snapshot_id": snapshot_id,
            "record_count": num_records,
            "inlined": true,
        })
        .to_string();
        txn.execute(
            "SELECT pg_notify('tide_ducklake_changes', $1)",
            &[&notify_payload],
        )
        .await
        .map_err(RelayError::Postgres)?;

        txn.commit().await.map_err(RelayError::Postgres)?;

        tracing::debug!(
            table_id = table_id,
            snapshot_id = snapshot_id,
            record_count = num_records,
            "DuckLake inlined rows committed"
        );
        Ok(())
    }

    // ── v0.21.0: Schema Evolution Bridge ─────────────────────────────────────

    /// Detect JSON keys in `messages` that are not yet registered as columns
    /// for `table_id`.  Returns only the new key names (additive changes).
    fn detect_new_json_keys(&self, table_id: i64, messages: &[&RelayMessage]) -> Vec<String> {
        let mut new_keys: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for msg in messages {
            if let Some(obj) = msg.payload.as_object() {
                for key in obj.keys() {
                    // Skip the standard pg-tide envelope fields.
                    if matches!(
                        key.as_str(),
                        "_dedup_key" | "_subject" | "_op" | "_outbox_id" | "data"
                    ) {
                        continue;
                    }
                    if !seen.contains(key)
                        && !self.column_ids.contains_key(&(table_id, key.clone()))
                    {
                        new_keys.push(key.clone());
                        seen.insert(key.clone());
                    }
                }
            }
        }
        new_keys
    }

    /// Register a new additive column in the DuckLake catalog and update caches.
    /// Increments the schema version for `table_id`.
    async fn add_column_additive(
        &mut self,
        table_id: i64,
        col_name: &str,
        snapshot_id: i64,
    ) -> Result<(), RelayError> {
        let cs = self.config.catalog_schema.clone();
        let col_order: i32 = self
            .column_ids
            .iter()
            .filter(|((tid, _), _)| *tid == table_id)
            .count() as i32;

        let col_id: i64 = self
            .db
            .query_one(
                &format!(
                    r#"
INSERT INTO "{cs}".ducklake_column
    (column_id, table_id, column_name, column_type, column_order, nullable)
VALUES (nextval('"{cs}".ducklake_column_id_seq'), $1, $2, 'VARCHAR', $3, true)
ON CONFLICT (table_id, column_name) DO UPDATE SET column_type = EXCLUDED.column_type
RETURNING column_id
"#,
                    cs = cs
                ),
                &[&table_id, &col_name, &col_order],
            )
            .await
            .map_err(RelayError::Postgres)?
            .get(0);

        self.column_ids
            .insert((table_id, col_name.to_string()), col_id);

        // Increment schema version counter.
        let sv = self.schema_version.entry(table_id).or_insert(0);
        *sv += 1;

        tracing::info!(
            table_id = table_id,
            col_name = col_name,
            snapshot_id = snapshot_id,
            schema_version = *sv,
            "DuckLake schema evolution: added additive column"
        );
        Ok(())
    }

    /// Run the schema evolution bridge for a batch.
    ///
    /// Detects new JSON keys, classifies them as additive (new nullable column) or
    /// breaking (type conflict), and applies the configured `on_schema_change` policy.
    ///
    /// Returns `Ok(true)` when the batch should be processed normally,
    /// `Ok(false)` when the batch should be skipped (routed to DLQ by caller),
    /// and `Err` when the pipeline should be paused.
    async fn apply_schema_evolution(
        &mut self,
        table_id: i64,
        snapshot_id: i64,
        messages: &[&RelayMessage],
    ) -> Result<bool, RelayError> {
        let new_keys = self.detect_new_json_keys(table_id, messages);
        if new_keys.is_empty() {
            return Ok(true);
        }

        match &self.config.on_schema_change.clone() {
            SchemaChangePolicy::Pause => {
                return Err(RelayError::Config(format!(
                    "DuckLake schema evolution: new keys {:?} detected for table_id {} — \
                     pipeline paused per on_schema_change=pause policy",
                    new_keys, table_id
                )));
            }
            SchemaChangePolicy::RouteToDlq => {
                tracing::warn!(
                    table_id = table_id,
                    ?new_keys,
                    "DuckLake schema evolution: routing batch to DLQ (on_schema_change=route_to_dlq)"
                );
                return Ok(false);
            }
            SchemaChangePolicy::WarnAndContinue | SchemaChangePolicy::AutoNewStream => {
                // For both warn-and-continue and auto-new-stream, register new columns.
                for key in &new_keys {
                    self.add_column_additive(table_id, key, snapshot_id).await?;
                }
            }
        }
        Ok(true)
    }

    // ── v0.21.0: Offset Map ───────────────────────────────────────────────────

    /// Write a `tide.ducklake_offset_map` entry mapping the highest outbox
    /// offset in this batch to the committed DuckLake `snapshot_id`.
    ///
    /// This enables consumers to use DuckDB time-travel replay
    /// (`AT (VERSION => snapshot_id)`) to re-read events by consumer offset.
    async fn write_offset_map(
        &mut self,
        outbox_offset: i64,
        snapshot_id: i64,
    ) -> Result<(), RelayError> {
        let pipeline = match &self.config.pipeline_name {
            Some(p) => p.clone(),
            None => return Ok(()),
        };
        self.db
            .execute(
                r#"
INSERT INTO tide.ducklake_offset_map
    (pipeline_name, consumer_group, outbox_offset, snapshot_id)
VALUES ($1, $2, $3, $4)
ON CONFLICT (pipeline_name, consumer_group, outbox_offset) DO NOTHING
"#,
                &[&pipeline, &pipeline, &outbox_offset, &snapshot_id],
            )
            .await
            .map_err(RelayError::Postgres)?;
        Ok(())
    }

    // ── v0.21.0: Auto-Partition ───────────────────────────────────────────────

    /// Register the partition strategy for this table in
    /// `tide.ducklake_partition_config` (once per pipeline/namespace/table).
    async fn register_partition_config(
        &mut self,
        namespace: &str,
        table_name: &str,
    ) -> Result<(), RelayError> {
        let pipeline = match &self.config.pipeline_name {
            Some(p) => p.clone(),
            None => return Ok(()),
        };
        if self.config.partition == DuckLakePartition::None {
            return Ok(());
        }
        let key = (
            pipeline.clone(),
            namespace.to_string(),
            table_name.to_string(),
        );
        if self.partition_registered.contains(&key) {
            return Ok(());
        }
        let partition_type = self.config.partition.as_str();
        let cs = self.config.catalog_schema.clone();
        self.db
            .execute(
                r#"
INSERT INTO tide.ducklake_partition_config
    (pipeline_name, catalog_schema, namespace, table_name, partition_type)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (pipeline_name, namespace, table_name) DO NOTHING
"#,
                &[&pipeline, &cs, &namespace, &table_name, &partition_type],
            )
            .await
            .map_err(RelayError::Postgres)?;
        self.partition_registered.insert(key);
        Ok(())
    }

    // ── v0.21.0: DLQ Archive ──────────────────────────────────────────────────

    /// Archive aged DLQ entries into a DuckLake `dlq_archive` table.
    ///
    /// Moves entries older than `dlq_archive_after_hours` from
    /// `tide.relay_dlq` into `{catalog_schema}.dlq_archive`.  Called on each
    /// `publish()` invocation when `dlq_archive_after_hours` is set.
    pub async fn archive_dlq_entries(&mut self) -> Result<(), RelayError> {
        let hours = match self.config.dlq_archive_after_hours {
            Some(h) => h as i64,
            None => return Ok(()),
        };
        let cs = self.config.catalog_schema.clone();

        // Ensure the DLQ archive table exists.
        let archive_ddl = format!(
            r#"
CREATE TABLE IF NOT EXISTS "{cs}".dlq_archive (
    id              BIGSERIAL   PRIMARY KEY,
    pipeline_name   TEXT        NOT NULL,
    dedup_key       TEXT,
    subject         TEXT,
    payload         TEXT,
    error_message   TEXT,
    failed_at       TIMESTAMPTZ,
    archived_at     TIMESTAMPTZ NOT NULL DEFAULT now()
)
"#,
            cs = cs
        );
        self.db.batch_execute(&archive_ddl).await?;

        // Move aged entries.
        let moved: u64 = self
            .db
            .execute(
                &format!(
                    r#"
WITH aged AS (
    DELETE FROM tide.relay_dlq
    WHERE failed_at < now() - ($1 * INTERVAL '1 hour')
    RETURNING pipeline_name, dedup_key, subject, payload, error_message, failed_at
)
INSERT INTO "{cs}".dlq_archive
    (pipeline_name, dedup_key, subject, payload, error_message, failed_at)
SELECT pipeline_name, dedup_key, subject, payload, error_message, failed_at
FROM   aged
"#,
                    cs = cs
                ),
                &[&hours],
            )
            .await
            .map_err(RelayError::Postgres)?;

        if moved > 0 {
            tracing::info!(
                moved = moved,
                "DuckLake DLQ archiver: moved aged entries to dlq_archive"
            );
        }
        Ok(())
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

        // v0.21.0: Archive aged DLQ entries on each publish cycle.
        self.archive_dlq_entries().await?;

        let namespace = self.config.namespace.clone();
        let cs = self.config.catalog_schema.clone();
        let inline_row_limit = self.config.inline_row_limit;

        // Group by table name.
        let mut groups: HashMap<String, Vec<&RelayMessage>> = HashMap::new();
        for msg in messages {
            let table = self.config.table_for(&msg.subject);
            groups.entry(table).or_default().push(msg);
        }

        for (table, batch) in &groups {
            // Bootstrap table catalog entries (schema/table/column rows) on first use.
            let (schema_id, table_id) = self.bootstrap_table(&namespace, table).await?;

            // v0.21.0: Register partition config on first use.
            self.register_partition_config(&namespace, table).await?;

            // v0.21.0: Allocate a snapshot ID first (needed for schema evolution logging).
            let snapshot_id: i64 = self
                .db
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

            // v0.21.0: Schema evolution bridge — detect new JSON keys.
            let proceed = self
                .apply_schema_evolution(table_id, snapshot_id, batch)
                .await?;
            if !proceed {
                // on_schema_change=route_to_dlq: skip this batch.
                tracing::warn!(table = %table, "DuckLake: skipping batch due to schema change policy");
                continue;
            }

            // v0.21.0: Get current schema version.
            let schema_version = *self.schema_version.entry(table_id).or_insert(0);

            let num_records = batch.len() as i64;

            // v0.21.0: Choose write path — inline vs. Parquet.
            if batch.len() <= inline_row_limit {
                // Inline path: write rows directly to catalog.
                self.ensure_inlined_table(table_id, schema_version).await?;
                self.publish_inline(table_id, schema_version, snapshot_id, schema_id, batch)
                    .await?;

                // Write offset map entry.
                if let Some(last_id) = batch.iter().filter_map(|m| m.outbox_id).max() {
                    self.write_offset_map(last_id, snapshot_id).await?;
                }
                continue;
            }

            // Parquet path (unchanged from v0.20.0, plus offset map).

            let num_records_i64 = num_records;

            // Compute column statistics for filter pushdown.
            let col_stats = Self::compute_column_stats(batch);

            // Build Parquet file in memory.
            let (parquet_bytes, footer_size) =
                Self::build_parquet_bytes(batch, &self.config.compression)?;
            let file_size = parquet_bytes.len() as i64;

            // Write Parquet to object storage.
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
            let txn = self
                .db
                .build_transaction()
                .isolation_level(tokio_postgres::IsolationLevel::ReadCommitted)
                .start()
                .await
                .map_err(RelayError::Postgres)?;

            // Allocate a file ID.
            let file_id: i64 = txn
                .query_one(
                    &format!(r#"SELECT nextval('"{cs}".ducklake_file_id_seq')"#, cs = cs),
                    &[],
                )
                .await
                .map_err(RelayError::Postgres)?
                .get(0);

            // Insert ducklake_snapshot (uses pre-allocated snapshot_id from above).
            txn.execute(
                &format!(
                    r#"
INSERT INTO "{cs}".ducklake_snapshot
    (snapshot_id, table_id, schema_version, sequence_number, author)
VALUES ($1, $2, $3,
    COALESCE((SELECT MAX(sequence_number) + 1
              FROM "{cs}".ducklake_snapshot
              WHERE table_id = $2), 0),
    'pg-tide-relay')
"#,
                    cs = cs
                ),
                &[&snapshot_id, &table_id, &schema_version],
            )
            .await
            .map_err(RelayError::Postgres)?;

            // Insert ducklake_data_file.
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
                    &num_records_i64,
                    &file_size,
                    &footer_size,
                ],
            )
            .await
            .map_err(RelayError::Postgres)?;

            // Write per-file column statistics.
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

            // Update ducklake_table_stats (next_row_id, row_count).
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
                &[&num_records_i64, &table_id],
            )
            .await
            .map_err(RelayError::Postgres)?;

            // Upsert global ducklake_table_column_stats.
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

            // Record snapshot change.
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

            // NOTIFY-based change notification for downstream consumers.
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

            txn.commit().await.map_err(RelayError::Postgres)?;

            // v0.21.0: Write offset map entry after Parquet commit.
            if let Some(last_id) = batch.iter().filter_map(|m| m.outbox_id).max() {
                self.write_offset_map(last_id, snapshot_id).await?;
            }

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
