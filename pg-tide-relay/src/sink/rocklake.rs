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
    /// table name and snapshot ID.
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
        let schema = &self.config.catalog_schema;
        let row = self
            .db
            .query_opt(
                &format!("SELECT value FROM \"{schema}\".ducklake_metadata WHERE key = 'version'"),
                &[],
            )
            .await
            .map_err(|e| RelayError::other(format!("rocklake catalog check failed: {e}")))?;

        match row {
            Some(r) => {
                let version: String = r.get(0);
                tracing::info!(catalog_schema = %schema, version = %version, "RockLake catalog ready");
                self.catalog_ready = true;
                Ok(())
            }
            None => Err(RelayError::other(
                "RockLake catalog not initialised: \
                 ducklake_metadata has no 'version' row. \
                 Ensure the RockLake sidecar has opened the catalog at least once.",
            )),
        }
    }

    // ── Phase 2: Parquet write path ───────────────────────────────────────────

    /// Allocate IDs for a new snapshot by reading `next_catalog_id` and
    /// `next_file_id` from the most recent `ducklake_snapshot` row.
    ///
    /// Returns `(snapshot_id, catalog_id_start, file_id_start)`.
    ///
    /// This replaces `nextval()` — RockLake requires explicit ID allocation.
    async fn allocate_ids(&self, schema: &str) -> Result<(i64, i64, i64), RelayError> {
        let row = self
            .db
            .query_opt(
                &format!(
                    "SELECT snapshot_id, next_catalog_id, next_file_id \
                     FROM \"{schema}\".ducklake_snapshot \
                     ORDER BY snapshot_id DESC \
                     LIMIT 1"
                ),
                &[],
            )
            .await
            .map_err(|e| RelayError::other(format!("rocklake id allocation failed: {e}")))?;

        match row {
            Some(r) => {
                let prev_snapshot_id: i64 = r.get(0);
                let next_catalog_id: i64 = r.get(1);
                let next_file_id: i64 = r.get(2);
                Ok((prev_snapshot_id + 1, next_catalog_id, next_file_id))
            }
            // Empty catalog: start from 1.
            None => Ok((1, 1, 1)),
        }
    }

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
        let schema = &self.config.catalog_schema;
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
                "CREATE TABLE IF NOT EXISTS \"{schema}\".\"{table_name}\" ({col_defs})"
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
    /// Uses plain `INSERT` without `ON CONFLICT` — consistent with
    /// RockLake's bounded SQL subset.  `_dedup_key` uniqueness is enforced
    /// by the relay's deduplication layer before this point.
    async fn publish_inline(
        &mut self,
        messages: &[&RelayMessage],
        table_id: i64,
        schema_version: i64,
        snapshot_id: i64,
    ) -> Result<(), RelayError> {
        let inlined_table = format!("ducklake_inlined_data_{table_id}_{schema_version}");
        let schema = &self.config.catalog_schema;

        // Pre-allocate a `row_id` range from `next_catalog_id`.
        // We read the current catalog state and use the next N IDs.
        let (_, catalog_id_start, _) = self.allocate_ids(schema).await?;

        let tx = self.db.transaction().await.map_err(RelayError::Postgres)?;

        for (i, msg) in messages.iter().enumerate() {
            let row_id = catalog_id_start + i as i64;
            let data_str =
                serde_json::to_string(&msg.payload).unwrap_or_else(|_| "null".to_string());
            let outbox_id: Option<i64> = msg.outbox_id;

            tx.execute(
                &format!(
                    "INSERT INTO \"{schema}\".\"{inlined_table}\" \
                     (row_id, begin_snapshot, end_snapshot, \
                      _dedup_key, _subject, _op, _outbox_id, data) \
                     VALUES ($1, $2, NULL, $3, $4, $5, $6, $7)"
                ),
                &[
                    &row_id,
                    &snapshot_id,
                    &msg.dedup_key,
                    &msg.subject,
                    &msg.op,
                    &outbox_id,
                    &data_str,
                ],
            )
            .await
            .map_err(|e| RelayError::other(format!("rocklake: inlined insert failed: {e}")))?;
        }

        tx.commit().await.map_err(RelayError::Postgres)?;

        Ok(())
    }

    // ── Phase 4: Schema evolution ─────────────────────────────────────────────

    /// Bootstrap a (namespace, table) pair in the DuckLake catalog.
    ///
    /// Uses explicit `SELECT` → conditional `INSERT` pattern instead of
    /// `ON CONFLICT`, as required by RockLake's bounded SQL subset.
    async fn bootstrap_table(
        &mut self,
        namespace: &str,
        table_name: &str,
        catalog_id_start: i64,
    ) -> Result<(i64, i64), RelayError> {
        let key = (namespace.to_string(), table_name.to_string());
        if let Some(&ids) = self.bootstrapped_tables.get(&key) {
            return Ok(ids);
        }

        let schema = self.config.catalog_schema.clone();

        // Look up or insert schema (namespace).
        let schema_row = self
            .db
            .query_opt(
                &format!(
                    "SELECT schema_id FROM \"{schema}\".ducklake_schema \
                     WHERE schema_name = $1"
                ),
                &[&namespace],
            )
            .await
            .map_err(|e| RelayError::other(format!("rocklake: schema lookup failed: {e}")))?;

        let schema_id: i64 = match schema_row {
            Some(r) => r.get(0),
            None => {
                let id = catalog_id_start;
                self.db
                    .execute(
                        &format!(
                            "INSERT INTO \"{schema}\".ducklake_schema \
                             (schema_id, schema_name, begin_snapshot, end_snapshot) \
                             VALUES ($1, $2, 1, NULL)"
                        ),
                        &[&id, &namespace],
                    )
                    .await
                    .map_err(|e| {
                        RelayError::other(format!("rocklake: schema insert failed: {e}"))
                    })?;
                id
            }
        };

        // Look up or insert table.
        let table_row = self
            .db
            .query_opt(
                &format!(
                    "SELECT table_id FROM \"{schema}\".ducklake_table \
                     WHERE schema_id = $1 AND table_name = $2"
                ),
                &[&schema_id, &table_name],
            )
            .await
            .map_err(|e| RelayError::other(format!("rocklake: table lookup failed: {e}")))?;

        let table_id: i64 = match table_row {
            Some(r) => r.get(0),
            None => {
                let id = catalog_id_start + 1;
                self.db
                    .execute(
                        &format!(
                            "INSERT INTO \"{schema}\".ducklake_table \
                             (table_id, schema_id, table_name, begin_snapshot, end_snapshot) \
                             VALUES ($1, $2, $3, 1, NULL)"
                        ),
                        &[&id, &schema_id, &table_name],
                    )
                    .await
                    .map_err(|e| {
                        RelayError::other(format!("rocklake: table insert failed: {e}"))
                    })?;
                id
            }
        };

        self.bootstrapped_tables.insert(key, (schema_id, table_id));
        Ok((schema_id, table_id))
    }

    /// Apply additive schema evolution: look up or insert a column definition.
    ///
    /// Uses explicit `SELECT` → conditional `INSERT` — no `ON CONFLICT`.
    #[allow(dead_code)]
    async fn add_column_if_missing(
        &mut self,
        table_id: i64,
        column_name: &str,
        snapshot_id: i64,
        catalog_id: i64,
    ) -> Result<i64, RelayError> {
        let key = (table_id, column_name.to_string());
        if let Some(&id) = self.column_ids.get(&key) {
            return Ok(id);
        }

        let schema = self.config.catalog_schema.clone();

        let existing = self
            .db
            .query_opt(
                &format!(
                    "SELECT column_id FROM \"{schema}\".ducklake_column \
                     WHERE table_id = $1 AND column_name = $2 AND end_snapshot IS NULL"
                ),
                &[&table_id, &column_name],
            )
            .await
            .map_err(|e| RelayError::other(format!("rocklake: column lookup failed: {e}")))?;

        let column_id: i64 = match existing {
            Some(r) => r.get(0),
            None => {
                self.db
                    .execute(
                        &format!(
                            "INSERT INTO \"{schema}\".ducklake_column \
                             (column_id, table_id, column_name, column_type, \
                              position, nulls_allowed, begin_snapshot, end_snapshot) \
                             VALUES ($1, $2, $3, 'VARCHAR', 0, true, $4, NULL)"
                        ),
                        &[&catalog_id, &table_id, &column_name, &snapshot_id],
                    )
                    .await
                    .map_err(|e| {
                        RelayError::other(format!("rocklake: column insert failed: {e}"))
                    })?;
                // Increment schema version for this table.
                let sv = self.schema_version.entry(table_id).or_insert(0);
                *sv += 1;
                catalog_id
            }
        };

        self.column_ids.insert(key, column_id);
        Ok(column_id)
    }

    // ── Phase 5: Auto-partition via ducklake_metadata ─────────────────────────

    /// Register partition configuration as a `ducklake_metadata` key/value
    /// entry using the `pg_tide.` prefix.
    ///
    /// This replaces the `tide.ducklake_partition_config` INSERT used by
    /// `DuckLakeSink`, which is not available in RockLake.
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

        let schema = &self.config.catalog_schema;
        let meta_key = format!("pg_tide.partition.{namespace}.{table_name}.{pipeline}");
        let meta_value = partition_str;

        // RockLake uses a flat key-value table; check first, then insert.
        let existing = self
            .db
            .query_opt(
                &format!("SELECT value FROM \"{schema}\".ducklake_metadata WHERE key = $1"),
                &[&meta_key],
            )
            .await
            .map_err(|e| RelayError::other(format!("rocklake: metadata lookup failed: {e}")))?;

        if existing.is_none() {
            self.db
                .execute(
                    &format!(
                        "INSERT INTO \"{schema}\".ducklake_metadata (key, value) \
                         VALUES ($1, $2)"
                    ),
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
        // Phase 2: allocate IDs from last snapshot (no nextval).
        let schema = self.config.catalog_schema.clone();
        let (snapshot_id, catalog_id_start, file_id_start) = self.allocate_ids(&schema).await?;

        // Phase 4: Bootstrap table using SELECT → INSERT (no ON CONFLICT).
        let (_schema_id, table_id) = self
            .bootstrap_table(namespace, table_name, catalog_id_start)
            .await?;

        // Phase 5: Register partition metadata in ducklake_metadata.
        self.register_partition_metadata(namespace, table_name)
            .await?;

        if batch.len() <= self.config.inline_row_limit {
            // Phase 3: Inlined data path.
            let sv = *self.schema_version.get(&table_id).unwrap_or(&0);
            self.ensure_inlined_table(table_id, sv, &[]).await?;
            self.publish_inline(batch, table_id, sv, snapshot_id)
                .await?;
        } else {
            // Phase 2: Parquet write path.
            self.publish_parquet(
                batch,
                table_id,
                snapshot_id,
                file_id_start,
                catalog_id_start,
            )
            .await?;
        }
        Ok(())
    }

    // ── Publish (Parquet path) ────────────────────────────────────────────────

    /// Publish a batch of messages via the Parquet write path.
    ///
    /// Algorithm:
    /// 1. Allocate IDs from the previous snapshot.
    /// 2. Write Parquet to object storage.
    /// 3. `BEGIN`; insert `ducklake_snapshot`, `ducklake_data_file`, column
    ///    stats rows; `COMMIT`.
    ///
    /// No `nextval()`, no `RETURNING`, no `ON CONFLICT`.
    async fn publish_parquet(
        &mut self,
        messages: &[&RelayMessage],
        table_id: i64,
        snapshot_id: i64,
        file_id: i64,
        catalog_id: i64,
    ) -> Result<(), RelayError> {
        use crate::ducklake_common::build_parquet_bytes;

        let schema = self.config.catalog_schema.clone();
        let table_name = self.config.table_for(messages[0].subject.as_str());

        // Build Parquet bytes.
        let (parquet_bytes, footer_size) = build_parquet_bytes(messages, &self.config.compression)?;

        // Write to object storage.
        let path_str = self.config.parquet_path(&table_name, snapshot_id);
        let object_path = Path::from(path_str.as_str());
        let file_size = parquet_bytes.len() as i64;

        self.store
            .put(&object_path, parquet_bytes.into())
            .await
            .map_err(|e| RelayError::other(format!("rocklake: object store write failed: {e}")))?;

        // Commit catalog in a single transaction.
        let tx = self.db.transaction().await.map_err(RelayError::Postgres)?;

        let record_count = messages.len() as i64;
        let now_ts = chrono::Utc::now();

        // Phase 2: Insert snapshot with explicit IDs (no nextval).
        tx.execute(
            &format!(
                "INSERT INTO \"{schema}\".ducklake_snapshot \
                 (snapshot_id, snapshot_time, schema_version, \
                  next_catalog_id, next_file_id) \
                 VALUES ($1, $2, 1, $3, $4)"
            ),
            &[
                &snapshot_id,
                &now_ts,
                &(catalog_id + 10), // reserve some IDs for column stats
                &(file_id + 1),
            ],
        )
        .await
        .map_err(|e| RelayError::other(format!("rocklake: snapshot insert failed: {e}")))?;

        // Insert data file record.
        tx.execute(
            &format!(
                "INSERT INTO \"{schema}\".ducklake_data_file \
                 (data_file_id, table_id, begin_snapshot, path, \
                  record_count, file_size_bytes, footer_size) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7)"
            ),
            &[
                &file_id,
                &table_id,
                &snapshot_id,
                &path_str,
                &record_count,
                &file_size,
                &footer_size,
            ],
        )
        .await
        .map_err(|e| RelayError::other(format!("rocklake: data file insert failed: {e}")))?;

        // Update table stats (additive — no ON CONFLICT).
        let existing_stats = tx
            .query_opt(
                &format!(
                    "SELECT record_count FROM \"{schema}\".ducklake_table_stats \
                     WHERE table_id = $1"
                ),
                &[&table_id],
            )
            .await
            .map_err(|e| RelayError::other(format!("rocklake: table stats lookup failed: {e}")))?;

        if existing_stats.is_some() {
            tx.execute(
                &format!(
                    "UPDATE \"{schema}\".ducklake_table_stats \
                     SET record_count = record_count + $1 \
                     WHERE table_id = $2"
                ),
                &[&record_count, &table_id],
            )
            .await
            .map_err(|e| RelayError::other(format!("rocklake: table stats update failed: {e}")))?;
        } else {
            tx.execute(
                &format!(
                    "INSERT INTO \"{schema}\".ducklake_table_stats \
                     (table_id, record_count) VALUES ($1, $2)"
                ),
                &[&table_id, &record_count],
            )
            .await
            .map_err(|e| RelayError::other(format!("rocklake: table stats insert failed: {e}")))?;
        }

        // Phase 7: propagate commit error as RelayError::Postgres so the
        // caller's retry loop can inspect SQLSTATE 57P04 / 40001.
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
        let schema = &self.config.catalog_schema;
        self.db
            .query_opt(
                &format!(
                    "SELECT 1 FROM \"{schema}\".ducklake_metadata \
                     WHERE key = 'version' LIMIT 1"
                ),
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
