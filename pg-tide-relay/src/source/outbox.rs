//! Native PostgreSQL outbox source.

use std::sync::Arc;

use tokio_postgres::Client;

use crate::envelope::{AckToken, OutboxBatch, RelayMessage};
use crate::error::RelayError;

/// Return the advisory-lock namespace shared by publishers, pollers, and
/// maintenance operations for one logical outbox.
pub fn outbox_fence_lock_key(outbox_name: &str) -> String {
    format!("pg_tide:outbox:{outbox_name}")
}

/// Decode one row from the canonical outbox table.
pub async fn decode_payload(
    payload: &serde_json::Value,
    db: &Client,
    _stream_table_name: &str,
    outbox_id: i64,
    _raw_mode: bool,
) -> Result<OutboxBatch, RelayError> {
    if payload
        .get("_cc")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        let oid = payload
            .get("oid")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| RelayError::other("native claim-check missing 'oid'"))?
            .parse::<i64>()
            .map_err(|_| RelayError::other("native claim-check 'oid' is invalid"))?;
        let row = db
            .query_one("SELECT lo_get($1)", &[&oid])
            .await
            .map_err(|error| RelayError::other(format!("lo_get({oid}) failed: {error}")))?;
        let bytes: Vec<u8> = row.get(0);
        let full_payload = serde_json::from_slice(&bytes).map_err(|error| {
            RelayError::other(format!("claim-check payload is invalid JSON: {error}"))
        })?;
        return Ok(OutboxBatch {
            outbox_id,
            refresh_id: None,
            is_full_refresh: false,
            inserted: vec![full_payload],
            deleted: vec![],
        });
    }

    Ok(OutboxBatch {
        outbox_id,
        refresh_id: None,
        is_full_refresh: false,
        inserted: vec![payload.clone()],
        deleted: vec![],
    })
}

pub struct OutboxPollerSource {
    db: Arc<Client>,
    outbox_name: String,
    subject_template: String,
    relay_group_id: String,
    pipeline_id: String,
    worker_id: String,
    consumer_group: Option<ConsumerGroupConfig>,
    last_offset: i64,
    pending_cc_oids: Vec<i64>,
    replay_mode: bool,
}

pub struct ConsumerGroupConfig {
    pub group_name: String,
    pub consumer_id: String,
    pub visibility_seconds: i32,
}

impl OutboxPollerSource {
    pub async fn new_simple_native(
        db: Arc<Client>,
        outbox_name: impl Into<String>,
        subject_template: impl Into<String>,
        relay_group_id: impl Into<String>,
        pipeline_id: impl Into<String>,
    ) -> Result<Self, RelayError> {
        let relay_group_id = relay_group_id.into();
        let pipeline_id = pipeline_id.into();
        let outbox_name = outbox_name.into();
        let last_offset = load_offset(&db, &relay_group_id, &pipeline_id, &outbox_name).await?;
        Ok(Self {
            db,
            outbox_name,
            subject_template: subject_template.into(),
            relay_group_id,
            pipeline_id,
            worker_id: worker_id(),
            consumer_group: None,
            last_offset,
            pending_cc_oids: Vec::new(),
            replay_mode: false,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn new_consumer_group(
        db: Arc<Client>,
        outbox_name: impl Into<String>,
        subject_template: impl Into<String>,
        relay_group_id: impl Into<String>,
        pipeline_id: impl Into<String>,
        group_name: impl Into<String>,
        consumer_id: impl Into<String>,
        visibility_seconds: i32,
    ) -> Result<Self, RelayError> {
        let outbox_name = outbox_name.into();
        crate::config::validate_relay_identifier(&outbox_name)?;
        Ok(Self {
            db,
            outbox_name,
            subject_template: subject_template.into(),
            relay_group_id: relay_group_id.into(),
            pipeline_id: pipeline_id.into(),
            worker_id: worker_id(),
            consumer_group: Some(ConsumerGroupConfig {
                group_name: group_name.into(),
                consumer_id: consumer_id.into(),
                visibility_seconds,
            }),
            last_offset: 0,
            pending_cc_oids: Vec::new(),
            replay_mode: false,
        })
    }
}

fn worker_id() -> String {
    format!(
        "{}:{}",
        std::env::var("HOSTNAME").unwrap_or_else(|_| "relay".to_string()),
        std::process::id()
    )
}

#[async_trait::async_trait]
impl super::Source for OutboxPollerSource {
    fn name(&self) -> &str {
        "outbox"
    }

    fn configure_replay(&mut self, from_offset: i64) -> Result<(), RelayError> {
        if self.consumer_group.is_some() {
            return Err(RelayError::InvalidConfig {
                name: self.pipeline_id.clone(),
                reason: "replay does not support consumer-group sources".to_string(),
            });
        }
        if from_offset < 0 {
            return Err(RelayError::InvalidConfig {
                name: self.pipeline_id.clone(),
                reason: "replay from_offset must be non-negative".to_string(),
            });
        }
        self.replay_mode = true;
        self.last_offset = from_offset.saturating_sub(1);
        Ok(())
    }

    async fn poll(&mut self, batch_size: i64) -> Result<Vec<RelayMessage>, RelayError> {
        self.pending_cc_oids.clear();
        if let Some(group) = &self.consumer_group {
            poll_consumer_group(
                &self.db,
                group,
                &self.outbox_name,
                &self.subject_template,
                batch_size as i32,
            )
            .await
        } else {
            poll_simple_native(
                &self.db,
                &self.outbox_name,
                &self.subject_template,
                self.last_offset,
                batch_size,
                &mut self.pending_cc_oids,
            )
            .await
        }
    }

    async fn acknowledge(&mut self, last_message: &RelayMessage) -> Result<(), RelayError> {
        if let AckToken::OutboxOffset(offset) = &last_message.ack_token {
            if let Some(group) = &self.consumer_group {
                self.db
                    .execute(
                        "SELECT tide.commit_offset($1, $2, $3)",
                        &[&group.group_name, &group.consumer_id, offset],
                    )
                    .await?;
            } else {
                if !self.replay_mode {
                    update_simple_offset(
                        &self.db,
                        &self.relay_group_id,
                        &self.pipeline_id,
                        &self.outbox_name,
                        *offset,
                        &self.worker_id,
                    )
                    .await?;
                }
                self.last_offset = self.last_offset.max(*offset);
            }
        }

        for oid in std::mem::take(&mut self.pending_cc_oids) {
            if let Err(error) = self.db.execute("SELECT lo_unlink($1)", &[&oid]).await {
                tracing::warn!(oid, error = %error, "lo_unlink failed after ack");
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}

async fn poll_simple_native(
    db: &Client,
    outbox_name: &str,
    subject_template: &str,
    last_offset: i64,
    batch_size: i64,
    pending_cc_oids: &mut Vec<i64>,
) -> Result<Vec<RelayMessage>, RelayError> {
    let rows = fenced_native_rows(db, outbox_name, last_offset, batch_size).await?;
    let logical_name = format!("outbox_{outbox_name}");
    let mut messages = Vec::new();
    for row in &rows {
        let id: i64 = row.get("id");
        let payload: serde_json::Value = row.get("payload");
        let headers: Option<serde_json::Value> = row.try_get("headers").ok();
        let created_at: Option<chrono::DateTime<chrono::Utc>> = row.try_get("created_at").ok();
        if let Some(oid) = payload
            .get("oid")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| value.parse::<i64>().ok())
        {
            if payload.get("_cc").and_then(serde_json::Value::as_bool) == Some(true) {
                pending_cc_oids.push(oid);
            }
        }
        let batch = decode_payload(&payload, db, outbox_name, id, true).await?;
        let mut batch_messages = batch.into_messages(&logical_name, subject_template);
        for message in &mut batch_messages {
            message.outbox_name = Some(outbox_name.to_string());
            message.headers = headers.clone();
            message.created_at = created_at;
        }
        if let Some(last) = batch_messages.last_mut() {
            last.ack_token = AckToken::OutboxOffset(id);
        }
        messages.extend(batch_messages);
    }
    Ok(messages)
}

async fn poll_consumer_group(
    db: &Client,
    group: &ConsumerGroupConfig,
    outbox_name: &str,
    subject_template: &str,
    batch_size: i32,
) -> Result<Vec<RelayMessage>, RelayError> {
    let key = outbox_fence_lock_key(outbox_name);
    db.batch_execute("BEGIN").await?;
    if let Err(error) = db
        .query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&key],
        )
        .await
    {
        let _ = db.batch_execute("ROLLBACK").await;
        return Err(RelayError::other(format!(
            "acquire outbox fence failed: {error}"
        )));
    }
    let rows = match db
        .query(
            "SELECT * FROM tide.poll_outbox($1, $2, $3, $4)",
            &[
                &group.group_name,
                &group.consumer_id,
                &batch_size,
                &group.visibility_seconds,
            ],
        )
        .await
    {
        Ok(rows) => rows,
        Err(error) => {
            let _ = db.batch_execute("ROLLBACK").await;
            return Err(error.into());
        }
    };
    db.batch_execute("COMMIT").await?;
    if rows.is_empty() {
        let _ = db
            .execute(
                "SELECT tide.consumer_heartbeat($1, $2)",
                &[&group.group_name, &group.consumer_id],
            )
            .await;
        return Ok(vec![]);
    }

    let logical_name = format!("outbox_{outbox_name}");
    let mut messages = Vec::new();
    for row in &rows {
        let id: i64 = row.get("outbox_id");
        let payload: serde_json::Value = row.get("payload");
        let batch = decode_payload(&payload, db, outbox_name, id, true).await?;
        let mut batch_messages = batch.into_messages(&logical_name, subject_template);
        for message in &mut batch_messages {
            message.outbox_name = Some(outbox_name.to_string());
        }
        if let Some(last) = batch_messages.last_mut() {
            last.ack_token = AckToken::OutboxOffset(id);
        }
        messages.extend(batch_messages);
    }
    Ok(messages)
}

async fn fenced_native_rows(
    db: &Client,
    outbox_name: &str,
    last_offset: i64,
    batch_size: i64,
) -> Result<Vec<tokio_postgres::Row>, RelayError> {
    db.batch_execute("BEGIN").await?;
    let key = outbox_fence_lock_key(outbox_name);
    if let Err(error) = db
        .query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&key],
        )
        .await
    {
        let _ = db.batch_execute("ROLLBACK").await;
        return Err(RelayError::other(format!(
            "acquire outbox fence failed: {error}"
        )));
    }
    let rows = match db
        .query(
            "SELECT id, payload, headers, created_at \
             FROM tide.tide_outbox_messages \
             WHERE outbox_name = $1 AND id > $2 \
             ORDER BY id LIMIT $3",
            &[&outbox_name, &last_offset, &batch_size],
        )
        .await
    {
        Ok(rows) => rows,
        Err(error) => {
            let _ = db.batch_execute("ROLLBACK").await;
            return Err(error.into());
        }
    };
    db.batch_execute("COMMIT").await?;
    Ok(rows)
}

async fn load_offset(
    db: &Client,
    relay_group_id: &str,
    pipeline_id: &str,
    outbox_name: &str,
) -> Result<i64, RelayError> {
    let row = db
        .query_opt(
            "SELECT last_change_id FROM tide.relay_consumer_offsets \
             WHERE relay_group_id = $1 AND pipeline_id = $2 AND outbox_name = $3",
            &[&relay_group_id, &pipeline_id, &outbox_name],
        )
        .await?;
    Ok(row.map(|row| row.get(0)).unwrap_or(0))
}

async fn update_simple_offset(
    db: &Client,
    relay_group_id: &str,
    pipeline_id: &str,
    outbox_name: &str,
    last_change_id: i64,
    worker_id: &str,
) -> Result<(), RelayError> {
    if last_change_id < 0 {
        return Err(RelayError::other(format!(
            "relay offset for pipeline '{pipeline_id}' must be non-negative"
        )));
    }
    let row = db
        .query_opt(
            "INSERT INTO tide.relay_consumer_offsets \
                 (relay_group_id, pipeline_id, outbox_name, last_change_id, worker_id, updated_at) \
             VALUES ($1, $2, $3, $4, $5, now()) \
             ON CONFLICT (relay_group_id, pipeline_id, outbox_name) DO UPDATE SET \
                 last_change_id = GREATEST(tide.relay_consumer_offsets.last_change_id, EXCLUDED.last_change_id), \
                 worker_id = EXCLUDED.worker_id, updated_at = now() \
             RETURNING last_change_id",
            &[
                &relay_group_id,
                &pipeline_id,
                &outbox_name,
                &last_change_id,
                &worker_id,
            ],
        )
        .await?;
    match row {
        Some(row) if row.get::<_, i64>(0) >= last_change_id => {
            crate::failpoints::hit("after_offset_db_commit", pipeline_id).await
        }
        Some(_) => Err(RelayError::other("offset commit moved backwards")),
        None => Err(RelayError::other("offset commit wrote zero rows")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outbox_fence_key_is_stable_and_namespaced() {
        assert_eq!(outbox_fence_lock_key("orders"), "pg_tide:outbox:orders");
        assert_ne!(
            outbox_fence_lock_key("orders"),
            outbox_fence_lock_key("payments")
        );
    }
}
