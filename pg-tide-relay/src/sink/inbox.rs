/// pg-trickle inbox sink (RELAY-22).
/// Writes RelayMessages to a pg-trickle inbox table with ON CONFLICT dedup.
///
/// v0.13.0: Batch inserts via UNNEST — replaces per-row INSERT loop with a
/// single multi-row INSERT for significantly reduced round-trip overhead.
use std::sync::Arc;
use tokio_postgres::Client;

use crate::envelope::RelayMessage;
use crate::error::RelayError;

pub struct InboxSink {
    db: Arc<Client>,
    inbox_table: String,
    dedup_count: u64,
}

impl InboxSink {
    pub fn new(db: Arc<Client>, inbox_table: impl Into<String>) -> Self {
        Self {
            db,
            inbox_table: inbox_table.into(),
            dedup_count: 0,
        }
    }
}

#[async_trait::async_trait]
impl super::Sink for InboxSink {
    fn name(&self) -> &str {
        "pg-inbox"
    }

    async fn publish(&mut self, messages: &[RelayMessage]) -> Result<(), RelayError> {
        if messages.is_empty() {
            return Ok(());
        }

        // v0.13.0: Batch insert via UNNEST for significantly lower round-trip overhead.
        // Extension-created inbox tables have columns: event_id, source, payload, headers.
        // msg.subject maps to source; headers are stored as a jsonb object.
        let schema_table = format!("tide.{}", self.inbox_table);

        // Build parallel arrays for UNNEST.
        let event_ids: Vec<&str> = messages.iter().map(|m| m.dedup_key.as_str()).collect();
        let sources: Vec<&str> = messages.iter().map(|m| m.subject.as_str()).collect();
        let payloads: Vec<serde_json::Value> = messages.iter().map(|m| m.payload.clone()).collect();
        let headers: Vec<serde_json::Value> = messages
            .iter()
            .map(|m| serde_json::json!({ "event_type": m.subject }))
            .collect();

        let inserted = self
            .db
            .execute(
                &format!(
                    "INSERT INTO {table} (event_id, source, payload, headers)
                     SELECT * FROM UNNEST($1::text[], $2::text[], $3::jsonb[], $4::jsonb[])
                       AS t(event_id, source, payload, headers)
                     ON CONFLICT (event_id) DO NOTHING",
                    table = schema_table
                ),
                &[&event_ids, &sources, &payloads, &headers],
            )
            .await
            .map_err(RelayError::from)?;

        let duplicates = messages.len() as u64 - inserted;
        if duplicates > 0 {
            self.dedup_count += duplicates;
            tracing::debug!(
                inbox = %self.inbox_table,
                duplicates,
                "duplicate messages skipped (dedup)"
            );
        }

        Ok(())
    }

    async fn is_healthy(&mut self) -> bool {
        self.db.query_opt("SELECT 1", &[]).await.is_ok()
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}

impl InboxSink {
    pub fn dedup_count(&self) -> u64 {
        self.dedup_count
    }
}
