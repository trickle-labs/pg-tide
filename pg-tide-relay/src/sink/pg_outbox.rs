/// PostgreSQL inbox sink for remote PG (RELAY-12).
/// Writes messages to a pg_tide inbox table on a different PostgreSQL instance.
///
/// v0.23.0: Fixed column names to match `tide.inbox_create()` schema:
/// `(event_id, source, payload, headers)` instead of the incorrect
/// `(event_id, event_type, payload, received_at)`.  Also switched from a
/// per-row INSERT loop to a single UNNEST batch insert (mirrors the local
/// InboxSink introduced in v0.13.0).
use tokio_postgres::types::Json;

use crate::envelope::RelayMessage;
use crate::error::RelayError;
/// Remote PostgreSQL inbox sink.
/// Uses tokio-postgres directly for PostgreSQL connections.
pub struct PgInboxSink {
    client: tokio_postgres::Client,
    inbox_relation: String,
    dedup_count: u64,
}

impl PgInboxSink {
    pub async fn new(
        postgres_url: &str,
        inbox_table: impl Into<String>,
    ) -> Result<Self, RelayError> {
        let table: String = inbox_table.into();
        // v0.18.0: Defence-in-depth identifier validation (overall_assessment_3 §2.2).
        crate::config::validate_relay_identifier(&table)?;
        // v0.15.0: Use pg_tls::connect to honour sslmode from the URL.
        let (client, conn) = crate::pg_tls::connect(postgres_url)
            .await
            .map_err(|error| error.into_connector_failure("postgresql-inbox"))?;

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::error!("pg-inbox remote connection error: {e}");
            }
        });
        let inbox_relation = match crate::sink::inbox::resolve_inbox_relation(&client, &table).await
        {
            Ok(relation) => relation,
            Err(error)
                if table.ends_with("_inbox")
                    && matches!(
                        &error,
                        RelayError::InvalidConfig { reason, .. }
                            if reason.contains("not registered")
                    ) =>
            {
                format!("tide.\"{table}\"")
            }
            Err(error) => return Err(error),
        };

        Ok(Self {
            client,
            inbox_relation,
            dedup_count: 0,
        })
    }
}

#[async_trait::async_trait]
impl super::Sink for PgInboxSink {
    fn name(&self) -> &str {
        "pg-inbox-remote"
    }

    async fn publish(&mut self, messages: &[RelayMessage]) -> Result<(), RelayError> {
        if messages.is_empty() {
            return Ok(());
        }

        // v0.23.0: UNNEST batch insert — one round-trip per batch, matching the
        // local InboxSink pattern from v0.13.0.
        // Column mapping:
        //   event_id → msg.dedup_key   (TEXT idempotency key)
        //   source   → msg.subject     (event subject / routing key)
        //   payload  → msg.payload     (JSONB message body)
        //   headers  → {"event_type": subject}  (JSONB metadata envelope)
        let mut event_ids: Vec<String> = Vec::with_capacity(messages.len());
        let mut sources: Vec<String> = Vec::with_capacity(messages.len());
        let mut payloads: Vec<serde_json::Value> = Vec::with_capacity(messages.len());
        let mut headers: Vec<serde_json::Value> = Vec::with_capacity(messages.len());

        for msg in messages {
            event_ids.push(msg.dedup_key.clone());
            sources.push(msg.subject.clone());
            payloads.push(msg.payload.clone());
            headers.push(serde_json::json!({"event_type": msg.subject}));
        }

        let payload_params: Vec<Json<&serde_json::Value>> = payloads.iter().map(Json).collect();
        let header_params: Vec<Json<&serde_json::Value>> = headers.iter().map(Json).collect();

        // v0.31.0: Double-quote the table identifier to handle inbox names
        // containing hyphens (e.g. "order-events").
        let sql = format!(
            "INSERT INTO {} (event_id, source, payload, headers) \
             SELECT * FROM UNNEST($1::text[], $2::text[], $3::jsonb[], $4::jsonb[]) \
             ON CONFLICT (event_id) DO NOTHING",
            self.inbox_relation
        );

        let inserted = self
            .client
            .execute(
                &sql,
                &[&event_ids, &sources, &payload_params, &header_params],
            )
            .await
            .map_err(|error| RelayError::postgres_connector_failure("postgresql-inbox", &error))?;

        self.dedup_count += (messages.len() as u64).saturating_sub(inserted);
        Ok(())
    }

    async fn is_healthy(&mut self) -> bool {
        self.client.query_opt("SELECT 1", &[]).await.is_ok()
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}
