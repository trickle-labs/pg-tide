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
    inbox_name: String,
    inbox_relation: String,
    dedup_count: u64,
}

impl InboxSink {
    pub fn new(db: Arc<Client>, inbox_table: impl Into<String>) -> Result<Self, RelayError> {
        let table: String = inbox_table.into();
        // v0.18.0: Defence-in-depth identifier validation (overall_assessment_3 §2.2).
        crate::config::validate_relay_identifier(&table)?;
        Ok(Self {
            db,
            inbox_name: table.clone(),
            inbox_relation: format!("tide.\"{table}\""),
            dedup_count: 0,
        })
    }

    pub async fn new_for_logical_name(
        db: Arc<Client>,
        inbox_name: impl Into<String>,
    ) -> Result<Self, RelayError> {
        let inbox_name = inbox_name.into();
        let inbox_relation = resolve_inbox_relation(&db, &inbox_name).await?;
        Ok(Self {
            db,
            inbox_name,
            inbox_relation,
            dedup_count: 0,
        })
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
                    "INSERT INTO {} (event_id, source, payload, headers)
                     SELECT * FROM UNNEST($1::text[], $2::text[], $3::jsonb[], $4::jsonb[])
                       AS t(event_id, source, payload, headers)
                     ON CONFLICT (event_id) DO NOTHING",
                    self.inbox_relation
                ),
                &[&event_ids, &sources, &payloads, &headers],
            )
            .await
            .map_err(|error| RelayError::postgres_connector_failure("postgresql-inbox", &error))?;

        let duplicates = messages.len() as u64 - inserted;
        if duplicates > 0 {
            self.dedup_count += duplicates;
            tracing::debug!(
                inbox = %self.inbox_name,
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

pub(crate) async fn resolve_inbox_relation(
    db: &Client,
    inbox_name: &str,
) -> Result<String, RelayError> {
    crate::config::validate_relay_identifier(inbox_name)?;
    if inbox_name.len() + "_inbox".len() > 63 {
        return Err(RelayError::InvalidConfig {
            name: inbox_name.to_string(),
            reason: "inbox relation name exceeds PostgreSQL's 63-byte limit".to_string(),
        });
    }
    let row = db
        .query_opt(
            "SELECT inbox_schema::text FROM tide.tide_inbox_config WHERE inbox_name = $1",
            &[&inbox_name],
        )
        .await?;
    let Some(row) = row else {
        return Err(RelayError::InvalidConfig {
            name: inbox_name.to_string(),
            reason: "logical inbox is not registered in tide.tide_inbox_config".to_string(),
        });
    };
    let schema: String = row.get(0);
    crate::config::validate_relay_identifier(&schema)?;
    Ok(format!("\"{schema}\".\"{inbox_name}_inbox\""))
}

impl InboxSink {
    pub fn dedup_count(&self) -> u64 {
        self.dedup_count
    }
}
