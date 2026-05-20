/// DuckLake reverse relay source (v0.22.0 — bidirectional flow & ecosystem surface).
///
/// Polls a DuckLake table for new snapshots by querying:
///
/// ```sql
/// SELECT max(snapshot_id) FROM <catalog_schema>.ducklake_snapshot
/// WHERE snapshot_id > $last_seen
/// ```
///
/// When new snapshots appear, fetches the incremental rows using DuckLake's
/// `begin_snapshot`/`end_snapshot` lifecycle semantics and delivers them as
/// `RelayMessage` objects for writing into a pg-tide inbox.
///
/// Configure via `tide.relay_set_inbox_v2(...)` with:
/// ```json
/// {
///   "source_type": "ducklake",
///   "catalog_connection": "postgres://...",
///   "catalog_schema": "ducklake",
///   "schema": "pgtide",
///   "table": "orders",
///   "snapshot_poll_interval_ms": 1000
/// }
/// ```
///
/// Feature-gated: only compiled with `--features ducklake`.
#[allow(unused_imports)]
use crate::envelope::{AckToken, RelayMessage};
use crate::error::RelayError;

/// Configuration for the DuckLake source.
#[derive(Debug, Clone)]
pub struct DuckLakeSourceConfig {
    /// PostgreSQL connection URL for the DuckLake catalog database.
    pub catalog_connection: String,
    /// PostgreSQL schema where DuckLake v1.0 catalog tables live (default: `"ducklake"`).
    pub catalog_schema: String,
    /// DuckLake namespace (maps to `ducklake_schema.schema_name`).
    pub schema: String,
    /// DuckLake table name to poll (maps to `ducklake_table.table_name`).
    pub table: String,
    /// How often to poll for new snapshots in milliseconds (default: 1000).
    pub snapshot_poll_interval_ms: u64,
    /// Consumer group name for deduplication tracking.
    pub consumer_group: String,
}

impl DuckLakeSourceConfig {
    /// Create a new DuckLake source config with sensible defaults.
    pub fn new(catalog_connection: &str, schema: &str, table: &str) -> Self {
        Self {
            catalog_connection: catalog_connection.to_string(),
            catalog_schema: "ducklake".to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            snapshot_poll_interval_ms: 1000,
            consumer_group: "default".to_string(),
        }
    }
}

/// DuckLake reverse relay source.
///
/// On each `poll()` call, checks for new DuckLake snapshots beyond the last
/// acknowledged snapshot ID, fetches incremental rows from live Parquet data,
/// and delivers them as `RelayMessage` objects.
pub struct DuckLakeSource {
    config: DuckLakeSourceConfig,
    /// The last snapshot ID that has been fully processed and acknowledged.
    #[allow(dead_code)]
    last_snapshot_id: i64,
}

impl DuckLakeSource {
    /// Create a new DuckLake source.
    ///
    /// `last_snapshot_id` should be loaded from the consumer offset store at
    /// startup; pass `0` to start from the beginning.
    pub fn new(config: DuckLakeSourceConfig, last_snapshot_id: i64) -> Self {
        Self {
            config,
            last_snapshot_id,
        }
    }

    /// Returns the stream subject for messages sourced from this DuckLake table.
    pub fn subject(&self) -> String {
        format!("{}.{}", self.config.schema, self.config.table)
    }
}

#[cfg(feature = "ducklake")]
#[async_trait::async_trait]
impl super::Source for DuckLakeSource {
    fn name(&self) -> &str {
        "ducklake"
    }

    async fn poll(&mut self, batch_size: i64) -> Result<Vec<RelayMessage>, RelayError> {
        use crate::pg_tls;

        let (client, conn) = pg_tls::connect(&self.config.catalog_connection)
            .await
            .map_err(|e| RelayError::Other(format!("ducklake catalog connect: {e}")))?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::error!("DuckLake catalog connection error: {e}");
            }
        });

        let last = self.last_snapshot_id;
        let schema = &self.config.catalog_schema;
        let dl_schema = &self.config.schema;
        let dl_table = &self.config.table;

        // Check for new snapshots beyond the last seen.
        let max_snap: Option<i64> = client
            .query_opt(
                &format!(
                    "SELECT max(s.snapshot_id) \
                     FROM {schema}.ducklake_snapshot s \
                     JOIN {schema}.ducklake_table t ON t.table_id = s.table_id \
                     JOIN {schema}.ducklake_schema sc ON sc.schema_id = t.schema_id \
                     WHERE sc.schema_name = $1 AND t.table_name = $2 \
                       AND s.snapshot_id > $3"
                ),
                &[dl_schema, dl_table, &last],
            )
            .await
            .map_err(|e| RelayError::source_poll("ducklake", e))?
            .and_then(|row| row.get(0));

        let new_max = match max_snap {
            None | Some(0) => return Ok(vec![]),
            Some(n) => n,
        };

        // Fetch table_id for the target table.
        let table_id_row = client
            .query_opt(
                &format!(
                    "SELECT t.table_id FROM {schema}.ducklake_table t \
                     JOIN {schema}.ducklake_schema sc ON sc.schema_id = t.schema_id \
                     WHERE sc.schema_name = $1 AND t.table_name = $2"
                ),
                &[dl_schema, dl_table],
            )
            .await
            .map_err(|e| RelayError::source_poll("ducklake", e))?;

        let table_id: i64 = match table_id_row {
            None => return Ok(vec![]),
            Some(r) => r.get(0),
        };

        // Fetch data files added between last_snapshot_id + 1 and new_max.
        let rows = client
            .query(
                &format!(
                    "SELECT f.file_id, f.file_path, f.record_count, f.begin_snapshot, \
                            f.file_size_bytes \
                     FROM {schema}.ducklake_data_file f \
                     WHERE f.table_id = $1 \
                       AND f.begin_snapshot > $2 \
                       AND f.begin_snapshot <= $3 \
                       AND (f.end_snapshot IS NULL OR f.end_snapshot > $3) \
                     ORDER BY f.begin_snapshot ASC \
                     LIMIT $4"
                ),
                &[&table_id, &last, &new_max, &batch_size],
            )
            .await
            .map_err(|e| RelayError::source_poll("ducklake", e))?;

        let subject = self.subject();
        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            let file_id: i64 = row.get("file_id");
            let file_path: String = row.get("file_path");
            let record_count: i64 = row.get("record_count");
            let begin_snapshot: i64 = row.get("begin_snapshot");

            let payload = serde_json::json!({
                "file_id": file_id,
                "file_path": file_path,
                "record_count": record_count,
                "snapshot_id": begin_snapshot,
                "table": dl_table,
                "schema": dl_schema,
                "source": "ducklake",
            });

            let msg = RelayMessage::new_forward(
                &subject, file_id, 0usize, "snapshot", payload, false, None, &subject,
            );
            // Store the snapshot_id in the ack_token so acknowledge() can
            // advance last_snapshot_id.
            let mut msg = msg;
            msg.ack_token = AckToken::OutboxOffset(begin_snapshot);
            messages.push(msg);
        }

        Ok(messages)
    }

    async fn acknowledge(&mut self, last_message: &RelayMessage) -> Result<(), RelayError> {
        // Extract the snapshot_id from the ack_token.
        if let AckToken::OutboxOffset(snapshot_id) = last_message.ack_token {
            self.last_snapshot_id = snapshot_id;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}

// Implement the Source trait for non-ducklake builds as a stub that always
// errors, so that the type is still available for the coordinator factory.
#[cfg(not(feature = "ducklake"))]
#[async_trait::async_trait]
impl super::Source for DuckLakeSource {
    fn name(&self) -> &str {
        "ducklake"
    }

    async fn poll(&mut self, _batch_size: i64) -> Result<Vec<RelayMessage>, RelayError> {
        Err(RelayError::Config(
            "DuckLake source requires --features ducklake".to_string(),
        ))
    }

    async fn acknowledge(&mut self, _last_message: &RelayMessage) -> Result<(), RelayError> {
        Err(RelayError::Config(
            "DuckLake source requires --features ducklake".to_string(),
        ))
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}
