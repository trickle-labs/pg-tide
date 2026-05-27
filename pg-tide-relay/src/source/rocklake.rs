/// RockLake reverse relay source (v0.37.0 — Phases 0–1 scaffold).
///
/// Polls a RockLake catalog for new snapshots by querying:
///
/// ```sql
/// SELECT max(snapshot_id) FROM <catalog_schema>.ducklake_snapshot
/// WHERE snapshot_id > $last_seen
/// ```
///
/// This is the only snapshot-detection query in RockLake's bounded SQL subset
/// (single-table SELECT, no JOINs, no subqueries).  When new snapshots appear,
/// the source fetches incremental data-file rows using the DuckLake
/// `begin_snapshot`/`end_snapshot` lifecycle semantics and delivers them as
/// `RelayMessage` objects for writing into a pg-tide inbox.
///
/// Configure via `tide.relay_set_inbox_v2(...)` with:
/// ```json
/// {
///   "source_type": "rocklake",
///   "catalog_connection": "postgres://user:pass@rocklake-sidecar:5432/catalog",
///   "catalog_schema": "ducklake",
///   "schema": "analytics",
///   "table": "events",
///   "snapshot_poll_interval_ms": 1000
/// }
/// ```
///
/// Feature-gated: only compiled with `--features rocklake`.
#[allow(unused_imports)]
use crate::envelope::{AckToken, RelayMessage};
use crate::error::RelayError;

/// Configuration for the RockLake source.
#[derive(Debug, Clone)]
pub struct RockLakeSourceConfig {
    /// PostgreSQL wire-protocol connection URL for the RockLake sidecar.
    pub catalog_connection: String,
    /// DuckLake catalog schema name (default: `"ducklake"`).
    pub catalog_schema: String,
    /// DuckLake namespace (maps to `ducklake_schema.schema_name`).
    pub schema: String,
    /// DuckLake table name to poll (maps to `ducklake_table.table_name`).
    pub table: String,
    /// Poll interval for new snapshots in milliseconds (default: 1 000).
    pub snapshot_poll_interval_ms: u64,
    /// Consumer group name for deduplication tracking.
    pub consumer_group: String,
}

impl RockLakeSourceConfig {
    /// Create a new `RockLakeSourceConfig` with sensible defaults.
    pub fn new(catalog_connection: &str, schema: &str, table: &str) -> Self {
        Self {
            catalog_connection: catalog_connection.to_string(),
            catalog_schema: "ducklake".to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            snapshot_poll_interval_ms: 1_000,
            consumer_group: "default".to_string(),
        }
    }
}

/// RockLake reverse relay source.
///
/// On each `poll()` call, checks for new RockLake snapshots beyond the last
/// acknowledged snapshot ID, then fetches the incremental rows from the
/// associated Parquet data files and delivers them as `RelayMessage` objects.
///
/// The snapshot-detection query is the only JOIN-free SELECT RockLake requires:
/// ```sql
/// SELECT max(snapshot_id) FROM ducklake_snapshot WHERE snapshot_id > $1
/// ```
pub struct RockLakeSource {
    config: RockLakeSourceConfig,
    /// The last snapshot ID that has been fully processed and acknowledged.
    #[allow(dead_code)]
    last_snapshot_id: i64,
}

impl RockLakeSource {
    /// Create a new `RockLakeSource`.
    ///
    /// `last_snapshot_id` should be loaded from the consumer offset store at
    /// startup; pass `0` to start from the beginning.
    pub fn new(config: RockLakeSourceConfig, last_snapshot_id: i64) -> Self {
        Self {
            config,
            last_snapshot_id,
        }
    }

    /// Returns the stream subject for messages sourced from this RockLake table.
    pub fn subject(&self) -> String {
        format!("{}.{}", self.config.schema, self.config.table)
    }
}

#[cfg(feature = "rocklake")]
#[async_trait::async_trait]
impl super::Source for RockLakeSource {
    fn name(&self) -> &str {
        "rocklake"
    }

    async fn poll(&mut self, batch_size: i64) -> Result<Vec<RelayMessage>, RelayError> {
        use crate::pg_tls;

        let (client, conn) = pg_tls::connect(&self.config.catalog_connection)
            .await
            .map_err(|e| RelayError::source_poll("rocklake", e))?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::error!("rocklake source connection closed: {e}");
            }
        });

        let catalog_schema = &self.config.catalog_schema;
        let last = self.last_snapshot_id;

        // Phase 0 / 1: single non-JOIN query — RockLake bounded SQL subset.
        let row = client
            .query_opt(
                &format!(
                    "SELECT max(snapshot_id) FROM \"{catalog_schema}\".ducklake_snapshot \
                     WHERE snapshot_id > $1"
                ),
                &[&last],
            )
            .await
            .map_err(|e| RelayError::source_poll("rocklake", e))?;

        let latest_snapshot: Option<i64> = row.and_then(|r| r.get(0));
        let latest = match latest_snapshot {
            Some(id) => id,
            None => return Ok(vec![]), // no new snapshots
        };

        // Fetch data-file rows for snapshots in (last, latest].
        let rows = client
            .query(
                &format!(
                    "SELECT df.path, df.record_count, df.begin_snapshot \
                     FROM \"{catalog_schema}\".ducklake_data_file df \
                     JOIN \"{catalog_schema}\".ducklake_table dt \
                         ON dt.table_id = df.table_id \
                     JOIN \"{catalog_schema}\".ducklake_schema ds \
                         ON ds.schema_id = dt.schema_id \
                     WHERE df.begin_snapshot > $1 \
                       AND df.begin_snapshot <= $2 \
                       AND ds.schema_name = $3 \
                       AND dt.table_name = $4 \
                       AND df.end_snapshot IS NULL \
                     ORDER BY df.begin_snapshot, df.data_file_id \
                     LIMIT $5"
                ),
                &[
                    &last,
                    &latest,
                    &self.config.schema,
                    &self.config.table,
                    &batch_size,
                ],
            )
            .await
            .map_err(|e| RelayError::source_poll("rocklake", e))?;

        if rows.is_empty() {
            return Ok(vec![]);
        }

        let subject = self.subject();
        let mut messages: Vec<RelayMessage> = Vec::with_capacity(rows.len());

        for row in &rows {
            let path: String = row.get(0);
            let record_count: i64 = row.get(1);
            let snapshot_id: i64 = row.get(2);

            // Emit a synthetic RelayMessage referencing the Parquet file path.
            // The actual row data lives in the Parquet file; downstream inboxes
            // receive the file reference and record count as a claim-check envelope.
            let payload = serde_json::json!({
                "path": path,
                "record_count": record_count,
                "snapshot_id": snapshot_id,
                "schema": self.config.schema,
                "table": self.config.table,
            });

            messages.push(RelayMessage {
                outbox_id: Some(snapshot_id),
                dedup_key: format!("rocklake:{snapshot_id}:{path}"),
                subject: subject.clone(),
                op: "snapshot".to_string(),
                payload,
                ack_token: AckToken::OutboxOffset(snapshot_id),
                is_full_refresh: false,
                refresh_id: None,
            });
        }

        Ok(messages)
    }

    async fn acknowledge(&mut self, last_message: &RelayMessage) -> Result<(), RelayError> {
        if let AckToken::OutboxOffset(snapshot_id) = last_message.ack_token {
            self.last_snapshot_id = snapshot_id;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}

// ── #[cfg(not(feature = "rocklake"))] stub ───────────────────────────────────

/// Stub type so coordinator code can reference `RockLakeSource` in
/// non-feature builds for type-checking purposes.
#[cfg(not(feature = "rocklake"))]
pub struct RockLakeSource;
