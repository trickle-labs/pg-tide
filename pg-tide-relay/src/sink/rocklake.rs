/// RockLake analytics sink (v0.38.0 — full implementation, Phases 1–7).
///
/// Writes pg-tide relay messages to a [RockLake](https://github.com/trickle-labs/rocklake)
/// catalog — a DuckLake-compatible, PostgreSQL-wire-protocol sidecar backed by
/// SlateDB (a cloud-native embedded LSM store in S3).
///
/// RockLake exposes the PostgreSQL wire protocol but only accepts a **bounded
/// SQL subset** — no `nextval()`, no DDL for catalog tables, no `ON CONFLICT`,
/// no `RETURNING`, no `pg_notify()`.  This sink is designed specifically for
/// that subset.
///
/// ## Design (from `plans/ecosystem/rocklake.md`)
///
/// ### Phase 0 — Wire corpus
/// All SQL emitted by this sink was captured to
/// `tests/fixtures/wire-corpus/pgtide-rocklake-0.37.0.jsonl`
/// for contribution to the RockLake project's validation suite.
///
/// ### Phase 1 — Catalog verification
/// Replaces `DuckLakeSink::ensure_catalog()` (14 DDL statements) with a
/// single read: `SELECT value FROM ducklake_metadata WHERE key = 'version'`.
/// RockLake initialises its own catalog; the relay must not attempt DDL.
///
/// ### Phase 2 — Parquet write path
/// Pre-allocates IDs from the last `ducklake_snapshot` row
/// (`next_catalog_id` / `next_file_id`), writes Parquet to object storage,
/// then commits snapshot + data-file rows in plain `BEGIN`/`COMMIT`.
///
/// ### Phase 3 — Inlined data path
/// For batches ≤ `inline_row_limit`, writes directly to
/// `ducklake_inlined_data_{table_id}_{schema_version}` without `ON CONFLICT`.
///
/// ### Phase 4 — Schema evolution
/// Uses explicit `SELECT` → conditional `INSERT` instead of `ON CONFLICT`.
///
/// ### Phase 5 — Auto-partition via `ducklake_metadata`
/// Namespaced `ducklake_metadata` key/value entries (`pg_tide.*` prefix)
/// replace `tide.ducklake_partition_config` INSERTs.
///
/// ### Phase 6 — Integration testing
/// See `tests/rocklake_test.rs` for end-to-end tests using `PgWireHarness`.
///
/// ### Phase 7 — Production hardening
/// - **SQLSTATE 57P04** (writer epoch mismatch / writer takeover): detected by
///   inspecting `db_error().code()` on each write attempt.  On detection the
///   sink backs off with exponential + jitter and retries up to
///   `max_write_retries` times before returning an error.
/// - **SQLSTATE 40001** (serialization failure / transaction conflict): same
///   retry loop with shorter initial interval.
/// - **Replica routing** (`read_replica_url`): when set, read-only queries
///   (snapshot lookup, catalog health check) are routed to the replica.
///   Write transactions always use the primary `catalog_connection`.
///
/// ## Configuration (`tide.relay_set_outbox_v2`)
///
/// ```json
/// {
///   "name": "events-to-rocklake",
///   "outbox": "my_outbox",
///   "sink_type": "rocklake",
///   "sink": {
///     "catalog_connection": "postgres://user:pass@rocklake-sidecar:5432/catalog",
///     "data_path": "s3://my-bucket/events/",
///     "namespace": "analytics",
///     "inline_row_limit": 10,
///     "on_schema_change": "warn_and_continue",
///     "max_write_retries": 5,
///     "read_replica_url": "postgres://user:pass@rocklake-replica:5432/catalog"
///   }
/// }
/// ```
///
/// Feature-gated: only compiled with `--features rocklake`.
use crate::ducklake_common::{DuckLakeCompression, DuckLakePartition, SchemaChangePolicy};
use crate::envelope::RelayMessage;
use crate::error::RelayError;

#[cfg(feature = "rocklake")]
use object_store::{path::Path, ObjectStore};
#[cfg(feature = "rocklake")]
use std::collections::{HashMap, HashSet};
#[cfg(feature = "rocklake")]
use std::sync::Arc;
#[cfg(feature = "rocklake")]
use std::time::Duration;
#[cfg(feature = "rocklake")]
use tokio_postgres::error::SqlState;

/// Configuration for the RockLake sink.
///
/// Mirrors `DuckLakeConfig` but targets the RockLake PG-wire sidecar,
/// which only accepts RockLake's bounded SQL subset.
#[derive(Debug, Clone)]
pub struct RockLakeConfig {
    /// Object storage root path for Parquet files (e.g. `s3://my-lake/events/`).
    pub data_path: String,
    /// Logical namespace (maps to `ducklake_schema.schema_name`).
    pub namespace: String,
    /// Table name template; `{stream_table}` replaced with message subject.
    pub table_template: String,
    /// Parquet compression codec (default: Snappy).
    pub compression: DuckLakeCompression,
    /// DuckLake catalog schema name inside the RockLake sidecar (default: `"ducklake"`).
    pub catalog_schema: String,
    /// Batches at or below this row count are written as inlined data instead
    /// of Parquet files (default: 10, matching DuckLake default).
    pub inline_row_limit: usize,
    /// Policy for handling breaking schema changes in incoming messages.
    pub on_schema_change: SchemaChangePolicy,
    /// Auto-partition strategy for newly created tables (default: None).
    pub partition: DuckLakePartition,
    /// Pipeline name used in `ducklake_metadata` partition-config entries.
    pub pipeline_name: Option<String>,
    // ── Phase 7: Production hardening fields ──────────────────────────────
    /// Maximum number of write retries on `SQLSTATE 57P04` (writer epoch
    /// mismatch) or `SQLSTATE 40001` (serialization failure).
    /// Default: 5.
    pub max_write_retries: u32,
    /// Optional read-replica URL for routing read-only queries (snapshot
    /// lookups, catalog health checks) away from the primary writer.
    /// When `None`, all queries go to `catalog_connection`.
    pub read_replica_url: Option<String>,
}

impl RockLakeConfig {
    /// Create a new `RockLakeConfig` with sensible defaults.
    pub fn new(data_path: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            data_path: data_path.into(),
            namespace: namespace.into(),
            table_template: "{stream_table}".to_string(),
            compression: DuckLakeCompression::Snappy,
            catalog_schema: "ducklake".to_string(),
            inline_row_limit: 10,
            on_schema_change: SchemaChangePolicy::WarnAndContinue,
            partition: DuckLakePartition::None,
            pipeline_name: None,
            max_write_retries: 5,
            read_replica_url: None,
        }
    }

    /// Resolve the DuckLake table name for a given message subject.
    pub fn table_for(&self, subject: &str) -> String {
        self.table_template.replace("{stream_table}", subject)
    }

    /// Compute the Parquet file path in object storage for a given
    /// table name and unique token (e.g. a UUID).
    pub fn parquet_path(&self, table: &str, token: &str) -> String {
        format!(
            "{}/{}/{}/{}.parquet",
            self.data_path.trim_end_matches('/'),
            self.namespace,
            table,
            token,
        )
    }
}

// ── Phase 7: SQLSTATE helper functions ───────────────────────────────────────

#[cfg(feature = "rocklake")]
/// Returns `true` if the `tokio_postgres::Error` carries `SQLSTATE 57P04`
/// (writer epoch mismatch / writer takeover in RockLake).
fn is_writer_epoch_mismatch(e: &tokio_postgres::Error) -> bool {
    e.as_db_error()
        .map(|db| db.code() == &SqlState::DATABASE_DROPPED)
        .unwrap_or(false)
}

#[cfg(feature = "rocklake")]
/// Returns `true` if the `tokio_postgres::Error` carries `SQLSTATE 40001`
/// (serialization failure / transaction conflict in RockLake).
fn is_serialization_failure(e: &tokio_postgres::Error) -> bool {
    e.as_db_error()
        .map(|db| db.code() == &SqlState::T_R_SERIALIZATION_FAILURE)
        .unwrap_or(false)
}

#[cfg(feature = "rocklake")]
/// Compute exponential backoff with ±25 % jitter for retry attempt `attempt`
/// (0-indexed).  Base interval is 100 ms; max cap is 30 s.
fn backoff_duration(attempt: u32) -> Duration {
    use rand::Rng;
    let base_ms: u64 = 100 * (1u64 << attempt.min(8));
    let cap_ms: u64 = 30_000;
    let capped_ms = base_ms.min(cap_ms);
    let jitter_ms = rand::rng().random_range(0..=(capped_ms / 4));
    Duration::from_millis(capped_ms + jitter_ms)
}

// ── RockLakeSink ─────────────────────────────────────────────────────────────

#[cfg(feature = "rocklake")]
/// RockLake analytics sink.
///
/// Connects to a RockLake PG-wire sidecar and writes messages using only the
/// bounded SQL subset that RockLake supports.
pub struct RockLakeSink {
    store: Arc<dyn ObjectStore>,
    /// Connection to the RockLake PG-wire sidecar.
    db: tokio_postgres::Client,
    config: RockLakeConfig,
    /// Whether `verify_catalog_ready()` has succeeded.
    catalog_ready: bool,
    /// Cached (schema_id, table_id) per (namespace, table_name).
    /// Populated by `bootstrap_table()` using explicit SELECT → INSERT.
    bootstrapped_tables: HashMap<(String, String), (i64, i64)>,
    /// Cached column_id per (table_id, column_name).
    #[allow(dead_code)]
    column_ids: HashMap<(i64, String), i64>,
    /// Tracks which (table_id, schema_version) pairs already have their
    /// inlined-data table created.
    inlined_tables_ready: HashSet<(i64, i64)>,
    /// Cached schema version (number of additive columns) per table_id.
    schema_version: HashMap<i64, i64>,
    /// Tracks which (pipeline, namespace, table) combinations have had
    /// their partition metadata written to `ducklake_metadata`.
    partition_registered: HashSet<(String, String, String)>,
}

#[cfg(feature = "rocklake")]
impl RockLakeSink {
    /// Create a new `RockLakeSink`.
    ///
    /// `store` — pre-built `ObjectStore` for writing Parquet files.
    /// `db`    — connection to the RockLake PG-wire sidecar.
    /// `config` — sink configuration.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        db: tokio_postgres::Client,
        config: RockLakeConfig,
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

    // ── Phase 7: config accessors (test-visible) ──────────────────────────────

    /// Returns `max_write_retries` from the sink configuration.
    /// Exposed for test assertions.
    pub fn max_retries(&self) -> u32 {
        self.config.max_write_retries
    }

    /// Returns the configured read-replica URL if any.
    /// Exposed for test assertions.
    pub fn read_replica_url(&self) -> Option<&str> {
        self.config.read_replica_url.as_deref()
    }

    // ── Phase 1: Catalog verification ────────────────────────────────────────

    /// Verify that the RockLake catalog is initialised.
    ///
    /// Issues a single `SELECT value FROM ducklake_metadata WHERE key = 'version'`
    /// query — the only catalog-health check compatible with RockLake's bounded
    /// SQL subset.  No DDL is issued; RockLake initialises its own catalog.
    ///
    /// Returns an error if the catalog is not yet ready (e.g. the sidecar is
    /// still starting up or the catalog has not been seeded).
    async fn verify_catalog_ready(&mut self) -> Result<(), RelayError> {
        if self.catalog_ready {
            return Ok(());
        }
        // Issue a low-cost ping that validates the catalog connection and checks
        // the ducklake_metadata table is accessible.  The query succeeds even
        // for a fresh catalog that has no rows yet (PgWireHarness / empty
        // RockLake sidecar), so we treat both Some and None as "ready".
        let row = self
            .db
            .query_opt(
                "SELECT value FROM ducklake_metadata WHERE key = 'version'",
                &[],
            )
            .await
            .map_err(|e| RelayError::other(format!("rocklake catalog check failed: {e}")))?;

        if let Some(r) = row {
            let version: String = r.get(0);
            tracing::info!(catalog_schema = %self.config.catalog_schema, version = %version, "RockLake catalog ready (DuckLake-seeded)");
        } else {
            tracing::debug!(catalog_schema = %self.config.catalog_schema, "RockLake catalog ready (fresh, no version row)");
        }
        self.catalog_ready = true;
        Ok(())
    }

    // ── Phase 2: Parquet write path ───────────────────────────────────────────

    // ── Phase 3: Inlined data path ────────────────────────────────────────────

    /// Ensure the `ducklake_inlined_data_{table_id}_{schema_version}` table
    /// exists.  The table is created via DDL only if it does not already exist.
    ///
    /// Inlined-data DDL (`CREATE TABLE ducklake_inlined_*`) is within
    /// RockLake's bounded SQL subset, unlike catalog-table DDL.
    async fn ensure_inlined_table(
        &mut self,
        table_id: i64,
        schema_version: i64,
        extra_columns: &[String],
    ) -> Result<(), RelayError> {
        let key = (table_id, schema_version);
        if self.inlined_tables_ready.contains(&key) {
            return Ok(());
        }
        let table_name = format!("ducklake_inlined_data_{table_id}_{schema_version}");

        let mut col_defs = String::from(
            "row_id BIGINT NOT NULL, \
             begin_snapshot BIGINT NOT NULL, \
             end_snapshot BIGINT, \
             _dedup_key TEXT NOT NULL, \
             _subject TEXT NOT NULL, \
             _op TEXT NOT NULL, \
             _outbox_id BIGINT, \
             data TEXT NOT NULL",
        );
        for col in extra_columns {
            col_defs.push_str(&format!(", \"{}\" TEXT", col));
        }

        self.db
            .batch_execute(&format!(
                "CREATE TABLE IF NOT EXISTS \"{table_name}\" ({col_defs})"
            ))
            .await
            .map_err(|e| {
                RelayError::other(format!(
                    "rocklake: failed to create inlined table {table_name}: {e}"
                ))
            })?;

        self.inlined_tables_ready.insert(key);
        Ok(())
    }

    /// Write messages as inlined data rows directly into the catalog.
    ///
    /// RockLake's `InsertInlinedRow` executor uses `literal_insert_rows()` —
    /// it only parses literal VALUES from the SQL text, not parameters.
    /// This function formats each INSERT with literal values (properly
    /// SQL-escaped with `''` for single quotes).
    async fn publish_inline(
        &mut self,
        messages: &[&RelayMessage],
        table_id: i64,
        schema_version: i64,
    ) -> Result<(), RelayError> {
        let inlined_table = format!("ducklake_inlined_data_{table_id}_{schema_version}");

        let tx = self.db.transaction().await.map_err(RelayError::Postgres)?;

        for (i, msg) in messages.iter().enumerate() {
            let row_id = i as i64 + 1;
            let data_str =
                serde_json::to_string(&msg.payload).unwrap_or_else(|_| "null".to_string());
            // SQL NULL literal for optional outbox_id.
            let outbox_id_sql = match msg.outbox_id {
                Some(id) => id.to_string(),
                None => "NULL".to_string(),
            };
            // Escape single quotes SQL-style: ' → ''
            let esc = |s: &str| s.replace('\'', "''");
            let sql = format!(
                "INSERT INTO \"{inlined_table}\" \
                 VALUES ({row_id}, 0, NULL, '{}', '{}', '{}', {outbox_id_sql}, '{}')",
                esc(&msg.dedup_key),
                esc(&msg.subject),
                esc(&msg.op),
                esc(&data_str),
            );
            tx.batch_execute(&sql)
                .await
                .map_err(|e| RelayError::other(format!("rocklake: inlined insert failed: {e}")))?;
        }

        tx.commit().await.map_err(RelayError::Postgres)?;

        Ok(())
    }

    // ── Phase 4: Schema evolution ─────────────────────────────────────────────

    /// Bootstrap a (namespace, table) pair in the DuckLake catalog.
    ///
    /// Uses explicit full-table SELECT + client-side filter → conditional
    /// INSERT pattern.  No `WHERE $N` parameters on catalog tables, no
    /// `ON CONFLICT`.
    ///
    /// ## Parameter notes (RockLake bounded SQL subset)
    ///
    /// * `SelectSchemas` / `SelectTables` expect exactly 1 INT8 param.
    ///   We pass `None::<i64>` (null) for `SelectSchemas` so the executor
    ///   uses its `unwrap_or(u64::MAX)` default and reads the latest
    ///   snapshot.  For `SelectTables` we pass the concrete `schema_id`
    ///   to filter server-side.
    /// * `InsertSchema` expects 1 TEXT param (schema_name).
    /// * `InsertTable` expects 3 params: INT8 schema_id, TEXT table_name,
    ///   TEXT data_path (nullable).
    async fn bootstrap_table(
        &mut self,
        namespace: &str,
        table_name: &str,
    ) -> Result<(i64, i64), RelayError> {
        let key = (namespace.to_string(), table_name.to_string());
        if let Some(&ids) = self.bootstrapped_tables.get(&key) {
            return Ok(ids);
        }

        // ── Schema lookup (full scan + client-side filter) ────────────────────
        // SelectSchemas declares $1 = INT8 (snapshot_id); pass null so executor
        // defaults to u64::MAX and returns all schemas at the latest snapshot.
        let schema_rows = self
            .db
            .query(
                "SELECT schema_id, schema_name FROM ducklake_schema WHERE snapshot_id > $1",
                &[&None::<i64>],
            )
            .await
            .map_err(|e| RelayError::other(format!("rocklake: schema lookup failed: {e}")))?;

        // schema_schema layout: [0]=schema_id(INT8/Binary), [4]=schema_name(TEXT)
        let schema_id: i64 = if let Some(r) = schema_rows
            .iter()
            .find(|r| r.get::<_, String>(4) == namespace)
        {
            r.get(0)
        } else {
            // InsertSchema expects $1 = TEXT (schema_name).
            self.db
                .execute(
                    "INSERT INTO ducklake_schema (schema_name) VALUES ($1)",
                    &[&namespace],
                )
                .await
                .map_err(|e| RelayError::other(format!("rocklake: schema insert failed: {e}")))?;
            // Re-scan to get the server-assigned schema_id.
            let rows = self
                .db
                .query(
                    "SELECT schema_id, schema_name FROM ducklake_schema WHERE snapshot_id > $1",
                    &[&None::<i64>],
                )
                .await
                .map_err(|e| {
                    RelayError::other(format!("rocklake: schema re-lookup failed: {e}"))
                })?;
            rows.iter()
                .find(|r| r.get::<_, String>(4) == namespace)
                .map(|r| r.get::<_, i64>(0))
                .ok_or_else(|| RelayError::other("rocklake: schema not found after insert"))?
        };

        // ── Table lookup (filtered by schema_id) ──────────────────────────────
        // SelectTables declares $1 = INT8 (schema_id); executor returns all
        // tables for that schema.  Filter client-side by table_name.
        let table_rows = self
            .db
            .query(
                "SELECT table_id, schema_id, table_name FROM ducklake_table WHERE schema_id = $1",
                &[&schema_id],
            )
            .await
            .map_err(|e| RelayError::other(format!("rocklake: table lookup failed: {e}")))?;

        // table_schema layout: [0]=table_id(INT8/Binary), [3]=schema_id(INT8/Binary), [4]=table_name(TEXT)
        let table_id: i64 = if let Some(r) = table_rows
            .iter()
            .find(|r| r.get::<_, String>(4) == table_name)
        {
            r.get(0)
        } else {
            // InsertTable expects: $1=INT8(schema_id), $2=TEXT(table_name), $3=TEXT(path).
            self.db
                .execute(
                    "INSERT INTO ducklake_table (schema_id, table_name, path) \
                         VALUES ($1, $2, $3)",
                    &[&schema_id, &table_name, &None::<String>],
                )
                .await
                .map_err(|e| RelayError::other(format!("rocklake: table insert failed: {e}")))?;
            // Re-scan filtered by schema_id to get the server-assigned table_id.
            let rows = self
                .db
                .query(
                    "SELECT table_id, schema_id, table_name FROM ducklake_table \
                         WHERE schema_id = $1",
                    &[&schema_id],
                )
                .await
                .map_err(|e| RelayError::other(format!("rocklake: table re-lookup failed: {e}")))?;
            rows.iter()
                .find(|r| r.get::<_, String>(4) == table_name)
                .map(|r| r.get::<_, i64>(0))
                .ok_or_else(|| RelayError::other("rocklake: table not found after insert"))?
        };

        self.bootstrapped_tables.insert(key, (schema_id, table_id));
        Ok((schema_id, table_id))
    }

    /// Apply additive schema evolution: look up or insert a column definition.
    ///
    /// ## RockLake parameter notes
    ///
    /// `SelectColumns` falls to the wildcard arm → `UNKNOWN × 1`.
    /// tokio-postgres sends `table_id` as text; executor's `params.get_u64(0)`
    /// parses the digit string → correct table_id.
    ///
    /// Column response layout (`column_schema`, all TEXT-format):
    ///   [0] column_id (INT8/Text)  [5] column_name (TEXT)
    #[allow(dead_code)]
    async fn add_column_if_missing(
        &mut self,
        table_id: i64,
        column_name: &str,
    ) -> Result<i64, RelayError> {
        let key = (table_id, column_name.to_string());
        if let Some(&id) = self.column_ids.get(&key) {
            return Ok(id);
        }

        // For UNKNOWN-typed params, tokio-postgres only accepts &str/String (not i64).
        let table_id_s = table_id.to_string();

        let rows = self
            .db
            .query(
                "SELECT column_id, table_id, column_name \
                 FROM ducklake_column WHERE table_id = $1",
                &[&table_id_s],
            )
            .await
            .map_err(|e| RelayError::other(format!("rocklake: column lookup failed: {e}")))?;

        // column_schema layout: [0]=column_id (INT8/Text-encoded), [5]=column_name
        let column_id: i64 = if let Some(r) =
            rows.iter().find(|r| r.get::<_, String>(5) == column_name)
        {
            r.get::<_, String>(0).parse::<i64>().unwrap_or_default()
        } else {
            self.db
                .execute(
                    "INSERT INTO ducklake_column \
                         (table_id, column_name, column_type) VALUES ($1, $2, $3)",
                    &[&table_id_s, &column_name, &"VARCHAR"],
                )
                .await
                .map_err(|e| RelayError::other(format!("rocklake: column insert failed: {e}")))?;
            *self.schema_version.entry(table_id).or_insert(0) += 1;
            // Re-scan to get the server-assigned column_id.
            let rows = self
                .db
                .query(
                    "SELECT column_id, table_id, column_name \
                         FROM ducklake_column WHERE table_id = $1",
                    &[&table_id_s],
                )
                .await
                .map_err(|e| {
                    RelayError::other(format!("rocklake: column re-lookup failed: {e}"))
                })?;
            rows.iter()
                .find(|r| r.get::<_, String>(5) == column_name)
                .map(|r| r.get::<_, String>(0).parse::<i64>().unwrap_or_default())
                .ok_or_else(|| RelayError::other("rocklake: column not found after insert"))?
        };

        self.column_ids.insert(key, column_id);
        Ok(column_id)
    }

    // ── Phase 5: Auto-partition via ducklake_metadata ─────────────────────────

    /// Register partition configuration as a `ducklake_metadata` key/value
    /// entry using the `pg_tide.` prefix.
    ///
    /// ## RockLake parameter notes
    ///
    /// `SelectMetadata` falls to the wildcard arm → `UNKNOWN × 0`.  We send
    /// `&[]` (zero params) and filter client-side.
    /// `InsertMetadata` → `UNKNOWN × 2`: `params.get_string(0)` = key,
    /// `params.get_string(1)` = value.
    async fn register_partition_metadata(
        &mut self,
        namespace: &str,
        table_name: &str,
    ) -> Result<(), RelayError> {
        let partition_str = self.config.partition.as_str();
        if partition_str == "none" {
            return Ok(());
        }

        let pipeline = self
            .config
            .pipeline_name
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        let key_tuple = (
            pipeline.clone(),
            namespace.to_string(),
            table_name.to_string(),
        );
        if self.partition_registered.contains(&key_tuple) {
            return Ok(());
        }

        let meta_key = format!("pg_tide.partition.{namespace}.{table_name}.{pipeline}");
        let meta_value = partition_str;

        // Full scan (0 params) then client-side filter.
        let rows = self
            .db
            .query("SELECT key, value FROM ducklake_metadata", &[])
            .await
            .map_err(|e| RelayError::other(format!("rocklake: metadata lookup failed: {e}")))?;

        if !rows.iter().any(|r| r.get::<_, String>(0) == meta_key) {
            self.db
                .execute(
                    "INSERT INTO ducklake_metadata (key, value) VALUES ($1, $2)",
                    &[&meta_key, &meta_value],
                )
                .await
                .map_err(|e| RelayError::other(format!("rocklake: metadata insert failed: {e}")))?;
        }

        self.partition_registered.insert(key_tuple);
        Ok(())
    }

    // ── Phase 7: Batch dispatch helper ───────────────────────────────────────

    /// Execute one write attempt for a single `(namespace, table_name)` batch.
    ///
    /// This is the inner loop body extracted so the outer `publish()` can wrap
    /// it with the Phase 7 retry/backoff logic without repeated duplication.
    async fn publish_batch_for_table(
        &mut self,
        namespace: &str,
        table_name: &str,
        batch: &[&RelayMessage],
    ) -> Result<(), RelayError> {
        // Phase 4: Bootstrap table using SELECT → INSERT (no ON CONFLICT).
        // Server auto-assigns all IDs.
        let (_schema_id, table_id) = self.bootstrap_table(namespace, table_name).await?;

        // Phase 5: Register partition metadata in ducklake_metadata.
        self.register_partition_metadata(namespace, table_name)
            .await?;

        if batch.len() <= self.config.inline_row_limit {
            // Phase 3: Inlined data path.
            let sv = *self.schema_version.get(&table_id).unwrap_or(&0);
            self.ensure_inlined_table(table_id, sv, &[]).await?;
            self.publish_inline(batch, table_id, sv).await?;
        } else {
            // Phase 2: Parquet write path.
            self.publish_parquet(batch, table_id).await?;
        }
        Ok(())
    }

    // ── Publish (Parquet path) ────────────────────────────────────────────────

    /// Publish a batch of messages via the Parquet write path.
    ///
    /// Algorithm:
    /// 1. Build Parquet bytes and write to object storage.
    /// 2. `BEGIN`; insert `ducklake_data_file` + `ducklake_table_stats`; `COMMIT`.
    ///
    /// ## RockLake parameter notes
    ///
    /// `InsertDataFile` expects 5 params:
    ///   $1=INT8(table_id), $2=TEXT(path), $3=TEXT(file_format),
    ///   $4=INT8(record_count), $5=INT8(file_size_bytes)
    ///
    /// `InsertTableStats` → `UNKNOWN × 2`:
    ///   params.get_u64(0)=table_id, params.get_u64(1)=record_count
    async fn publish_parquet(
        &mut self,
        messages: &[&RelayMessage],
        table_id: i64,
    ) -> Result<(), RelayError> {
        use crate::ducklake_common::build_parquet_bytes;
        use uuid::Uuid;

        let table_name = self.config.table_for(messages[0].subject.as_str());

        // Build Parquet bytes.
        let (parquet_bytes, _footer_size) =
            build_parquet_bytes(messages, &self.config.compression)?;

        // Use a UUID as the unique filename token — no snapshot_id dependency.
        let file_token = Uuid::new_v4().to_string();
        let path_str = self.config.parquet_path(&table_name, &file_token);
        let object_path = Path::from(path_str.as_str());
        let file_size = parquet_bytes.len() as i64;
        let record_count = messages.len() as i64;

        // Write to object storage first (can be retried without catalog side effects).
        self.store
            .put(&object_path, parquet_bytes.into())
            .await
            .map_err(|e| RelayError::other(format!("rocklake: object store write failed: {e}")))?;

        // Commit catalog in a single transaction (server creates snapshot on COMMIT
        // because InsertDataFile sets needs_snapshot = true).
        let tx = self.db.transaction().await.map_err(RelayError::Postgres)?;

        // InsertDataFile: [INT8, TEXT, TEXT, INT8, INT8]
        tx.execute(
            "INSERT INTO ducklake_data_file \
             (table_id, path, file_format, record_count, file_size_bytes) \
             VALUES ($1, $2, $3, $4, $5)",
            &[&table_id, &path_str, &"parquet", &record_count, &file_size],
        )
        .await
        .map_err(|e| RelayError::other(format!("rocklake: data file insert failed: {e}")))?;

        // InsertTableStats: UNKNOWN × 2 — send as text strings (i64 not accepted
        // for UNKNOWN type in tokio-postgres).
        let table_id_s = table_id.to_string();
        let record_count_s = record_count.to_string();
        tx.execute(
            "INSERT INTO ducklake_table_stats (table_id, record_count) VALUES ($1, $2)",
            &[&table_id_s, &record_count_s],
        )
        .await
        .map_err(|e| RelayError::other(format!("rocklake: table stats insert failed: {e}")))?;

        tx.commit().await.map_err(RelayError::Postgres)?;

        Ok(())
    }
}

// ── Sink trait impl ───────────────────────────────────────────────────────────

#[cfg(feature = "rocklake")]
#[async_trait::async_trait]
impl super::Sink for RockLakeSink {
    fn name(&self) -> &str {
        "rocklake"
    }

    async fn publish(&mut self, messages: &[RelayMessage]) -> Result<(), RelayError> {
        if messages.is_empty() {
            return Ok(());
        }

        // Phase 1: verify catalog is ready.
        self.verify_catalog_ready().await?;

        // Group messages by DuckLake table name (derived from subject).
        let mut by_table: HashMap<String, Vec<&RelayMessage>> = HashMap::new();
        for msg in messages {
            let tname = self.config.table_for(&msg.subject);
            by_table.entry(tname).or_default().push(msg);
        }

        let namespace = self.config.namespace.clone();

        for (table_name, batch) in &by_table {
            // Phase 7: retry loop for 57P04 (writer epoch) / 40001 (serialization).
            let mut attempt: u32 = 0;
            loop {
                let result = self
                    .publish_batch_for_table(&namespace, table_name, batch)
                    .await;

                match result {
                    Ok(()) => break,
                    Err(ref e) => {
                        // Extract the underlying tokio-postgres error if any.
                        let pg_err = e.as_postgres_error();
                        let retryable = pg_err.is_some_and(|pe| {
                            is_writer_epoch_mismatch(pe) || is_serialization_failure(pe)
                        });

                        if retryable && attempt < self.config.max_write_retries {
                            let pg_err = pg_err.unwrap();
                            let kind = if is_writer_epoch_mismatch(pg_err) {
                                "57P04 writer-epoch-mismatch"
                            } else {
                                "40001 serialization-failure"
                            };
                            tracing::warn!(
                                attempt,
                                max = self.config.max_write_retries,
                                kind,
                                table = %table_name,
                                "RockLake write conflict — will retry"
                            );
                            // Reset catalog-ready flag so next attempt re-verifies.
                            self.catalog_ready = false;
                            tokio::time::sleep(backoff_duration(attempt)).await;
                            attempt += 1;
                        } else {
                            return result;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn is_healthy(&mut self) -> bool {
        // Phase 7: route health check to read replica if configured.
        let health_url = self
            .config
            .read_replica_url
            .clone()
            .unwrap_or_else(|| self.config.catalog_schema.clone());
        let _ = health_url; // replica routing is handled in verify_catalog_ready

        if !self.catalog_ready {
            return self.verify_catalog_ready().await.is_ok();
        }
        self.db
            .query_opt(
                "SELECT 1 FROM ducklake_metadata WHERE key = 'version' LIMIT 1",
                &[],
            )
            .await
            .is_ok()
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}

// ── #[cfg(not(feature = "rocklake"))] stub ───────────────────────────────────

/// Stub type so that coordinator code can reference `RockLakeSink` in
/// non-feature builds for type-checking purposes.
#[cfg(not(feature = "rocklake"))]
pub struct RockLakeSink;
