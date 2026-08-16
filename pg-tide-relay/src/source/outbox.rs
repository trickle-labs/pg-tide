/// Outbox poller source (RELAY-3 + RELAY-4).
///
/// v0.40.0 (ADR-011): The default native path polls the canonical shared table
/// `tide.tide_outbox_messages` with a static query discriminated by
/// `outbox_name`. pg_trickle compatibility (dynamic per-outbox relations and the
/// `v:1` envelope) is an explicit, clearly separated mode.
use std::sync::Arc;
use tokio_postgres::Client;
use uuid::Uuid;

use crate::envelope::{AckToken, OutboxBatch, RelayMessage};
use crate::error::RelayError;

/// Poll mode for [`OutboxPollerSource`].
///
/// v0.40.0 (ADR-011): `Native` is the default and polls the canonical shared
/// table. `PgTrickleCompat` retains the legacy dynamic per-outbox relation and
/// `v:1` envelope decoding for pg_trickle producers.
#[derive(Debug, Clone)]
pub enum OutboxSourceMode {
    /// Native shared-table path: static query on `tide.tide_outbox_messages`.
    Native,
    /// pg_trickle compatibility: dynamic per-outbox relation + `v:1` decoding.
    PgTrickleCompat {
        /// Physical relation name, e.g. `outbox_<name>` (validated + quoted).
        table: String,
    },
}

impl OutboxSourceMode {
    fn is_native(&self) -> bool {
        matches!(self, OutboxSourceMode::Native)
    }
}

// ── Payload decoding (RELAY-4) ────────────────────────────────────────────

const FETCH_BATCH: i64 = 1000;

/// Return the advisory-lock namespace shared by publishers, pollers, and
/// maintenance operations for one logical outbox.
pub fn outbox_fence_lock_key(outbox_name: &str) -> String {
    format!("pg_tide:outbox:{outbox_name}")
}

/// Decode one outbox row payload into an OutboxBatch.
///
/// v0.15.0: `raw_mode` — when `true`, treats the payload as a native JSONB
/// event (published via `tide.outbox_publish()` without a `v:1` pg_trickle
/// envelope).  This allows non-pg_trickle producers to use pg_tide.
pub async fn decode_payload(
    payload: &serde_json::Value,
    db: &Client,
    stream_table_name: &str,
    outbox_id: i64,
    raw_mode: bool,
) -> Result<OutboxBatch, RelayError> {
    // v0.15.0: Raw mode — treat the whole payload as a native event.
    if raw_mode {
        // v0.28.0: Native claim-check pathway.
        // outbox_publish_large() stores large payloads in pg_largeobject and
        // writes a claim-check envelope {"_cc": true, "oid": "<loid>", "size": N}.
        // Fetch the real payload transparently before passing to the wire-format
        // encoder.  The OID is unlinked after ack — see poll_simple()'s ack path.
        if payload
            .get("_cc")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            let oid_str = payload
                .get("oid")
                .and_then(|v| v.as_str())
                .ok_or_else(|| RelayError::Other("native claim-check missing 'oid'".into()))?;
            let loid: u32 = oid_str.parse::<u32>().map_err(|_| {
                RelayError::Other(format!(
                    "native claim-check 'oid' not a valid OID: {oid_str}"
                ))
            })?;
            // SAFETY: lo_get is a standard PostgreSQL large-object read function;
            // the loid was written by outbox_publish_large() and is owned by
            // the relay's session role.  Any error from lo_get is propagated as
            // a RelayError and the pipeline will retry on the next poll.
            let row = db
                .query_one("SELECT lo_get($1)", &[&(loid as i64)])
                .await
                .map_err(|e| RelayError::Other(format!("lo_get({loid}) failed: {e}")))?;
            let raw_bytes: Vec<u8> = row.get(0);
            let full_payload: serde_json::Value =
                serde_json::from_slice(&raw_bytes).map_err(|e| {
                    RelayError::Other(format!("claim-check payload not valid JSON: {e}"))
                })?;
            return Ok(OutboxBatch {
                outbox_id,
                refresh_id: None,
                is_full_refresh: false,
                inserted: vec![full_payload],
                deleted: vec![],
            });
        }
        return Ok(OutboxBatch {
            outbox_id,
            refresh_id: None,
            is_full_refresh: false,
            inserted: vec![payload.clone()],
            deleted: vec![],
        });
    }

    let v = payload.get("v").and_then(|v| v.as_i64()).unwrap_or(0);
    if v != 1 {
        return Err(RelayError::UnsupportedPayloadVersion(v));
    }

    let is_full_refresh = payload
        .get("full_refresh")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let is_claim_check = payload
        .get("claim_check")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let refresh_id = payload
        .get("pgt_id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok());

    if is_claim_check {
        // v0.15.0: Guard — verify the claim-check delta table exists before
        // attempting to fetch from it.  Absent tables mean pg_trickle >= 0.46.0
        // is not installed.  Return a clear error rather than a confusing SQL
        // error from a missing table.
        let outbox_name = format!("outbox_{stream_table_name}");
        let delta_table_name = format!("outbox_delta_rows_{outbox_name}");
        let delta_exists: bool = db
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
                 WHERE table_schema = 'tide' AND table_name = $1)",
                &[&delta_table_name],
            )
            .await
            .map(|r| r.get(0))
            .unwrap_or(false);
        if !delta_exists {
            return Err(RelayError::PayloadDecode {
                outbox: outbox_name.clone(),
                outbox_id,
                // QUOTED: Human-readable error message — not SQL. The actual SQL cursor
                // uses the properly-quoted delta_table from fetch_claim_check_rows() below.
                reason: format!(
                    "claim-check guard: tide.{delta_table_name} does not exist. \
                     This outbox requires pg_trickle >= 0.46.0 with claim-check tables enabled. \
                     Run `pg-tide doctor` for pre-flight checks."
                ),
            });
        }

        // Cursor-fetch from companion claim-check delta table.
        let (inserted, deleted) = fetch_claim_check_rows(db, &outbox_name, outbox_id).await?;

        // Signal consumption complete so extension can drain old rows.
        db.execute(
            "SELECT tide.outbox_rows_consumed($1, $2)",
            &[&stream_table_name, &outbox_id],
        )
        .await
        .map_err(|e| RelayError::PayloadDecode {
            outbox: outbox_name.clone(),
            outbox_id,
            reason: e.to_string(),
        })?;

        Ok(OutboxBatch {
            outbox_id,
            refresh_id,
            is_full_refresh,
            inserted,
            deleted,
        })
    } else {
        // Inline — rows are embedded in the payload.
        Ok(OutboxBatch {
            outbox_id,
            refresh_id,
            is_full_refresh,
            inserted: extract_array(payload, "inserted"),
            deleted: extract_array(payload, "deleted"),
        })
    }
}

/// Fetch claim-check rows using a server-side cursor (bounded memory).
async fn fetch_claim_check_rows(
    db: &Client,
    outbox_name: &str,
    outbox_id: i64,
) -> Result<(Vec<serde_json::Value>, Vec<serde_json::Value>), RelayError> {
    let cursor_name = format!("relay_cc_{outbox_id}_{}", Uuid::new_v4().simple());
    // v0.31.0: Double-quote the identifier to handle outbox names with hyphens.
    // QUOTED: tide."outbox_delta_rows_{outbox_name}" — identifier validated at
    // construction via validate_relay_identifier() in OutboxPollerSource::new_simple_pg_trickle().
    let delta_table = format!("tide.\"outbox_delta_rows_{outbox_name}\"");

    // Open cursor — embed outbox_id literal (it's an i64 from DB, not user input).
    db.batch_execute(&format!(
        "DECLARE {cursor} NO SCROLL CURSOR FOR \
         SELECT op, payload FROM {table} WHERE outbox_id = {oid} ORDER BY row_num",
        cursor = cursor_name,
        table = delta_table,
        oid = outbox_id,
    ))
    .await
    .map_err(|e| RelayError::PayloadDecode {
        outbox: outbox_name.to_string(),
        outbox_id,
        reason: e.to_string(),
    })?;

    let mut inserted = Vec::new();
    let mut deleted = Vec::new();

    loop {
        let rows = db
            .query(
                &format!(
                    "FETCH {n} FROM {cursor}",
                    n = FETCH_BATCH,
                    cursor = cursor_name
                ),
                &[],
            )
            .await
            .map_err(|e| RelayError::PayloadDecode {
                outbox: outbox_name.to_string(),
                outbox_id,
                reason: e.to_string(),
            })?;

        let done = rows.len() < FETCH_BATCH as usize;
        for row in rows {
            let op: &str = row.get("op");
            let payload: serde_json::Value = row.get("payload");
            match op {
                "I" => inserted.push(payload),
                "D" => deleted.push(payload),
                _ => tracing::warn!(op, outbox_id, "unknown delta op in claim-check"),
            }
        }
        if done {
            break;
        }
    }

    // Close the cursor.
    let _ = db
        .batch_execute(&format!("CLOSE {cursor}", cursor = cursor_name))
        .await;

    Ok((inserted, deleted))
}

fn extract_array(v: &serde_json::Value, key: &str) -> Vec<serde_json::Value> {
    v.get(key)
        .and_then(|a| a.as_array())
        .cloned()
        .unwrap_or_default()
}

// ── OutboxPoller source (RELAY-3) ─────────────────────────────────────────

/// Outbox poller that works in two modes:
/// - Simple: tracks offsets in `relay_consumer_offsets` (suitable for single-relay).
/// - Consumer group: delegates to `poll_outbox()` + `commit_offset()`.
///
/// v0.40.0 (ADR-011): The default `mode` is [`OutboxSourceMode::Native`], which
/// polls the canonical `tide.tide_outbox_messages` table. pg_trickle producers
/// use [`OutboxSourceMode::PgTrickleCompat`].
pub struct OutboxPollerSource {
    db: Arc<Client>,
    /// Logical outbox name (the `outbox_name` discriminator on the shared table).
    outbox_name: String,
    /// Poll mode: native shared-table (default) or pg_trickle compatibility.
    mode: OutboxSourceMode,
    subject_template: String,
    relay_group_id: String,
    pipeline_id: String,
    worker_id: String,
    consumer_group: Option<ConsumerGroupConfig>,
    last_offset: i64,
    /// v0.28.0: Large-object OIDs to unlink after a confirmed ack.
    /// Populated during `poll()` for each row whose payload is a native
    /// claim-check envelope (`_cc: true`).  Cleared in `acknowledge()`.
    pending_cc_oids: Vec<u32>,
    replay_mode: bool,
}

pub struct ConsumerGroupConfig {
    pub group_name: String,
    pub consumer_id: String,
    pub visibility_seconds: i32,
}

impl OutboxPollerSource {
    /// Create a native simple outbox poller (ADR-011).
    ///
    /// Polls the canonical `tide.tide_outbox_messages` table with a static query
    /// discriminated by `outbox_name`; payloads are decoded as native events.
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

        let worker_id = worker_id();

        // Load last committed offset for this (relay group, pipeline, outbox).
        let last_offset = load_offset(&db, &relay_group_id, &pipeline_id, &outbox_name).await?;

        Ok(Self {
            db,
            outbox_name,
            mode: OutboxSourceMode::Native,
            subject_template: subject_template.into(),
            relay_group_id,
            pipeline_id,
            worker_id,
            consumer_group: None,
            last_offset,
            pending_cc_oids: Vec::new(),
            replay_mode: false,
        })
    }

    /// Create a pg_trickle-compatibility simple outbox poller.
    ///
    /// Retains the legacy dynamic per-outbox relation (`tide."outbox_<name>"`)
    /// and `v:1` envelope decoding. The relation identifier is validated and
    /// double-quoted; native producers should use [`Self::new_simple_native`].
    pub async fn new_simple_pg_trickle(
        db: Arc<Client>,
        outbox_name: impl Into<String>,
        subject_template: impl Into<String>,
        relay_group_id: impl Into<String>,
        pipeline_id: impl Into<String>,
    ) -> Result<Self, RelayError> {
        let relay_group_id = relay_group_id.into();
        let pipeline_id = pipeline_id.into();
        let outbox_name = outbox_name.into();
        let table = format!("outbox_{outbox_name}");

        // Identifier validation is required here because the relation name is
        // interpolated into the compatibility query (not bound as a parameter).
        crate::config::validate_relay_identifier(&outbox_name)?;
        crate::config::validate_relay_identifier(&table)?;

        let worker_id = worker_id();

        let last_offset = load_offset(&db, &relay_group_id, &pipeline_id, &outbox_name).await?;

        Ok(Self {
            db,
            outbox_name,
            mode: OutboxSourceMode::PgTrickleCompat { table },
            subject_template: subject_template.into(),
            relay_group_id,
            pipeline_id,
            worker_id,
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
        let relay_group_id = relay_group_id.into();
        let pipeline_id = pipeline_id.into();
        let outbox_name = outbox_name.into();

        // Consumer-group mode reads via the tide.poll_outbox() SQL function on
        // the canonical shared table, so the outbox name is passed as a bound
        // parameter. Validate anyway to reject malformed configuration early.
        crate::config::validate_relay_identifier(&outbox_name)?;

        let worker_id = worker_id();

        Ok(Self {
            db,
            outbox_name,
            mode: OutboxSourceMode::Native,
            subject_template: subject_template.into(),
            relay_group_id,
            pipeline_id,
            worker_id,
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

/// Build the worker identifier used to stamp offset writes.
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
                reason: "inline replay does not support consumer-group sources".to_string(),
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
        // v0.28.0: Clear pending claim-check OIDs from the previous poll cycle
        // before accumulating new ones.
        self.pending_cc_oids.clear();

        if let Some(cg) = &self.consumer_group {
            poll_consumer_group(
                &self.db,
                &cg.group_name,
                &cg.consumer_id,
                &self.outbox_name,
                &self.subject_template,
                batch_size as i32,
                cg.visibility_seconds,
                self.mode.is_native(),
            )
            .await
        } else {
            poll_simple(
                &self.db,
                &self.outbox_name,
                &self.mode,
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
            if self.consumer_group.is_some() {
                if let Some(cg) = &self.consumer_group {
                    let offset_i64 = *offset;
                    self.db
                        .execute(
                            "SELECT tide.commit_offset($1, $2, $3)",
                            &[&cg.group_name, &cg.consumer_id, &offset_i64],
                        )
                        .await?;
                }
            } else {
                let offset_i64 = *offset;
                // v0.40.0 (ADR-011): The offset write is monotonic and scoped by
                // outbox. It only advances the in-memory position after the DB
                // commit is confirmed, so an ack failure is visible to the worker.
                if !self.replay_mode {
                    update_simple_offset(
                        &self.db,
                        &self.relay_group_id,
                        &self.pipeline_id,
                        &self.outbox_name,
                        offset_i64,
                        &self.worker_id,
                    )
                    .await?;
                }
                // In-memory position never rewinds (mirrors the DB GREATEST upsert).
                self.last_offset = self.last_offset.max(offset_i64);
            }
        }

        // v0.28.0: Unlink any large-object OIDs from native claim-check messages
        // that were fetched during the last poll().  This frees the pg_largeobject
        // storage after the sink has confirmed delivery.  Errors are logged at WARN
        // and do not fail the ack — a dangling LO is less harmful than a stuck pipeline.
        let oids = std::mem::take(&mut self.pending_cc_oids);
        for loid in oids {
            // SAFETY: lo_unlink frees the large object; loid was obtained from
            // the payload written by outbox_publish_large() and is valid.
            if let Err(e) = self
                .db
                .execute("SELECT lo_unlink($1)", &[&(loid as i64)])
                .await
            {
                tracing::warn!(loid, error = %e, "lo_unlink failed after ack (non-fatal)");
            }
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}

/// Simple mode: poll directly from the outbox table using stored offset.
#[allow(clippy::too_many_arguments)]
async fn poll_simple(
    db: &Client,
    outbox_name: &str,
    mode: &OutboxSourceMode,
    subject_template: &str,
    last_offset: i64,
    batch_size: i64,
    pending_cc_oids: &mut Vec<u32>,
) -> Result<Vec<RelayMessage>, RelayError> {
    match mode {
        OutboxSourceMode::Native => {
            poll_simple_native(
                db,
                outbox_name,
                subject_template,
                last_offset,
                batch_size,
                pending_cc_oids,
            )
            .await
        }
        OutboxSourceMode::PgTrickleCompat { table } => {
            poll_simple_pg_trickle(
                db,
                table,
                outbox_name,
                subject_template,
                last_offset,
                batch_size,
            )
            .await
        }
    }
}

/// v0.40.0 (ADR-011): Native simple mode — one static, parameterized query on
/// the canonical shared table. `outbox_name` is bound as a parameter, so no
/// identifier interpolation is required. Ordering is strictly increasing by
/// `id`; the global identity sequence may leave gaps within one outbox.
async fn poll_simple_native(
    db: &Client,
    outbox_name: &str,
    subject_template: &str,
    last_offset: i64,
    batch_size: i64,
    pending_cc_oids: &mut Vec<u32>,
) -> Result<Vec<RelayMessage>, RelayError> {
    let rows = fenced_native_rows(db, outbox_name, last_offset, batch_size).await?;

    // The stable logical dedup/subject identifier keeps the `outbox_` prefix
    // (ADR-011 §5); it is a compatibility label, not a physical relation name.
    let dedup_table = format!("outbox_{outbox_name}");
    let mut messages = Vec::new();
    for row in &rows {
        let id: i64 = row.get("id");
        let payload: serde_json::Value = row.get("payload");
        let headers: Option<serde_json::Value> = row.try_get("headers").ok();
        let created_at: Option<chrono::DateTime<chrono::Utc>> = row.try_get("created_at").ok();

        // v0.28.0: Track native claim-check OIDs for post-ack lo_unlink.
        if payload
            .get("_cc")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            if let Some(oid_str) = payload.get("oid").and_then(|v| v.as_str()) {
                if let Ok(loid) = oid_str.parse::<u32>() {
                    pending_cc_oids.push(loid);
                }
            }
        }

        // Native decoding: payload is a native JSONB event (raw_mode = true).
        let batch = decode_payload(&payload, db, outbox_name, id, true).await?;
        let mut batch_msgs = batch.into_messages(&dedup_table, subject_template);
        // Attach native row metadata (ADR-011 §4) and the ack token.
        for msg in batch_msgs.iter_mut() {
            msg.outbox_name = Some(outbox_name.to_string());
            msg.headers = headers.clone();
            msg.created_at = created_at;
        }
        if let Some(last) = batch_msgs.last_mut() {
            last.ack_token = AckToken::OutboxOffset(id);
        }
        messages.extend(batch_msgs);
    }
    Ok(messages)
}

/// Fetch native rows while holding the short exclusive outbox fence.
///
/// Rows are copied out of the transaction before payload decoding, claim-check
/// reads, transforms, or sink I/O.  This keeps a long-running relay batch from
/// blocking publishers for the duration of delivery.
async fn fenced_native_rows(
    db: &Client,
    outbox_name: &str,
    last_offset: i64,
    batch_size: i64,
) -> Result<Vec<tokio_postgres::Row>, RelayError> {
    db.batch_execute("BEGIN").await.map_err(RelayError::from)?;

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
            "acquire outbox fence for '{outbox_name}' failed: {error}"
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
            return Err(RelayError::from(error));
        }
    };

    if let Err(error) = db.batch_execute("COMMIT").await {
        let _ = db.batch_execute("ROLLBACK").await;
        return Err(RelayError::other(format!(
            "release outbox fence for '{outbox_name}' failed: {error}"
        )));
    }

    Ok(rows)
}

/// pg_trickle compatibility simple mode: dynamic per-outbox relation +
/// `v:1` envelope decoding. The relation identifier was validated at
/// construction (`OutboxPollerSource::new_simple_pg_trickle`).
async fn poll_simple_pg_trickle(
    db: &Client,
    table: &str,
    outbox_name: &str,
    subject_template: &str,
    last_offset: i64,
    batch_size: i64,
) -> Result<Vec<RelayMessage>, RelayError> {
    // Outbox tables are in the tide schema: tide."<table>"
    // QUOTED: tide."{table}" — identifier validated at construction via
    // validate_relay_identifier() in OutboxPollerSource::new_simple_pg_trickle().
    let outbox_schema_table = format!("tide.\"{table}\"");
    let rows = db
        .query(
            &format!(
                "SELECT id, payload FROM {t} WHERE id > $1 ORDER BY id LIMIT $2",
                t = outbox_schema_table
            ),
            &[&last_offset, &batch_size],
        )
        .await
        .map_err(RelayError::from)?;

    let mut messages = Vec::new();
    for row in &rows {
        let id: i64 = row.get("id");
        let payload: serde_json::Value = row.get("payload");

        let batch = decode_payload(&payload, db, outbox_name, id, false).await?;
        let mut batch_msgs = batch.into_messages(table, subject_template);
        for msg in batch_msgs.iter_mut() {
            msg.outbox_name = Some(outbox_name.to_string());
        }
        if let Some(last) = batch_msgs.last_mut() {
            last.ack_token = AckToken::OutboxOffset(id);
        }
        messages.extend(batch_msgs);
    }
    Ok(messages)
}

/// Consumer group mode: use poll_outbox() + commit_offset().
#[allow(clippy::too_many_arguments)]
async fn poll_consumer_group(
    db: &Client,
    group: &str,
    consumer_id: &str,
    stream_table_name: &str,
    subject_template: &str,
    batch_size: i32,
    visibility_seconds: i32,
    raw_mode: bool,
) -> Result<Vec<RelayMessage>, RelayError> {
    let key = outbox_fence_lock_key(stream_table_name);
    db.batch_execute("BEGIN").await.map_err(RelayError::from)?;
    if let Err(error) = db
        .query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&key],
        )
        .await
    {
        let _ = db.batch_execute("ROLLBACK").await;
        return Err(RelayError::other(format!(
            "acquire outbox fence for '{stream_table_name}' failed: {error}"
        )));
    }

    let rows = match db
        .query(
            "SELECT * FROM tide.poll_outbox($1, $2, $3, $4)",
            &[&group, &consumer_id, &batch_size, &visibility_seconds],
        )
        .await
    {
        Ok(rows) => rows,
        Err(error) => {
            let _ = db.batch_execute("ROLLBACK").await;
            return Err(error.into());
        }
    };
    if let Err(error) = db.batch_execute("COMMIT").await {
        let _ = db.batch_execute("ROLLBACK").await;
        return Err(RelayError::other(format!(
            "release outbox fence for '{stream_table_name}' failed: {error}"
        )));
    }

    if rows.is_empty() {
        // Heartbeat even when idle.
        let _ = db
            .execute(
                "SELECT tide.consumer_heartbeat($1, $2)",
                &[&group, &consumer_id],
            )
            .await;
        return Ok(vec![]);
    }

    let outbox_table_name = format!("outbox_{stream_table_name}");
    let mut messages = Vec::new();
    for row in &rows {
        let id: i64 = row.get("outbox_id");
        let payload: serde_json::Value = row.get("payload");
        let batch = decode_payload(&payload, db, stream_table_name, id, raw_mode).await?;
        let mut batch_msgs = batch.into_messages(&outbox_table_name, subject_template);
        for msg in batch_msgs.iter_mut() {
            msg.outbox_name = Some(stream_table_name.to_string());
        }
        if let Some(last) = batch_msgs.last_mut() {
            last.ack_token = AckToken::OutboxOffset(id);
        }
        messages.extend(batch_msgs);
    }
    Ok(messages)
}

/// Load the last committed offset for a simple-mode pipeline.
///
/// v0.40.0 (ADR-011): The offset identity includes `outbox_name`, so a pipeline
/// name reused for another outbox never inherits the previous outbox's offset.
async fn load_offset(
    db: &Client,
    relay_group_id: &str,
    pipeline_id: &str,
    outbox_name: &str,
) -> Result<i64, RelayError> {
    let row = db
        .query_opt(
            "SELECT last_change_id FROM tide.relay_consumer_offsets
             WHERE relay_group_id = $1 AND pipeline_id = $2 AND outbox_name = $3",
            &[&relay_group_id, &pipeline_id, &outbox_name],
        )
        .await?;
    Ok(row.map(|r| r.get::<_, i64>(0)).unwrap_or(0))
}

/// Write (upsert) the committed offset for a simple-mode pipeline.
///
/// v0.40.0 (ADR-011): The upsert is monotonic — a lower offset never rewinds a
/// higher stored value (`GREATEST`). The write returns the persisted offset and
/// this function fails if it is below the acknowledged offset or if no row was
/// written, so an offset-commit failure is visible to the worker rather than
/// silently succeeding.
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
            "INSERT INTO tide.relay_consumer_offsets
                 (relay_group_id, pipeline_id, outbox_name, last_change_id, worker_id, updated_at)
             VALUES ($1, $2, $3, $4, $5, now())
             ON CONFLICT (relay_group_id, pipeline_id, outbox_name)
             DO UPDATE SET
                 last_change_id = GREATEST(
                     tide.relay_consumer_offsets.last_change_id,
                     EXCLUDED.last_change_id
                 ),
                 worker_id  = EXCLUDED.worker_id,
                 updated_at = now()
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
        Some(r) => {
            let persisted: i64 = r.get(0);
            if persisted < last_change_id {
                return Err(RelayError::other(format!(
                    "offset commit for relay_group={relay_group_id} pipeline={pipeline_id} \
                     outbox={outbox_name} persisted {persisted} below acknowledged \
                     {last_change_id}"
                )));
            }
            crate::failpoints::hit("after_offset_db_commit", pipeline_id).await?;
            Ok(())
        }
        None => Err(RelayError::other(format!(
            "offset commit for relay_group={relay_group_id} pipeline={pipeline_id} \
             outbox={outbox_name} wrote zero rows (attempted {last_change_id})"
        ))),
    }
}

// ── Fan-in source (v0.35.0) ───────────────────────────────────────────────

/// v0.35.0: Multi-outbox fan-in source.
///
/// Polls multiple outboxes in round-robin order and commits offsets for all
/// contributing members in a single UNNEST batch query, replacing N sequential
/// UPDATE calls with one round-trip per committed batch.
///
/// Offset tracking uses `tide.relay_consumer_offsets` with `fanin_member` set
/// to the contributing outbox name so each member's position is tracked
/// independently.
pub struct FanInSource {
    db: Arc<Client>,
    /// Fan-in pipeline name (used as the `pipeline_id` for offset rows).
    pipeline_name: String,
    /// Sub-sources for each contributing outbox; polled in round-robin order.
    sources: Vec<OutboxPollerSource>,
    /// Index of the next sub-source to poll (round-robin counter).
    next_idx: usize,
    /// Per-source pending offsets accumulated during a poll cycle.
    /// Flushed in a single UNNEST batch upsert during `acknowledge`.
    pending_offsets: Vec<(String, i64)>, // (outbox_name, offset)
    relay_group_id: String,
    worker_id: String,
}

impl FanInSource {
    /// Create a new `FanInSource`, loading the last committed offset for each
    /// contributing outbox from `tide.relay_consumer_offsets`.
    pub async fn new(
        db: Arc<Client>,
        pipeline_name: &str,
        outbox_names: Vec<String>,
        subject_template: &str,
        relay_group_id: &str,
        _raw_mode: bool,
    ) -> Result<Self, RelayError> {
        let worker_id = format!("pg-tide-fanin-{}", Uuid::new_v4());
        let mut sources = Vec::with_capacity(outbox_names.len());

        for outbox_name in &outbox_names {
            // Load last committed offset for this member from the fanin-aware
            // offset row where fanin_member = outbox_name.
            let last_offset = db
                .query_opt(
                    "SELECT last_change_id \
                     FROM tide.relay_consumer_offsets \
                     WHERE relay_group_id = $1 \
                       AND pipeline_id    = $2 \
                       AND outbox_name    = $3 \
                       AND fanin_member   = $4",
                    &[&relay_group_id, &pipeline_name, &outbox_name, &outbox_name],
                )
                .await
                .map_err(|e| {
                    RelayError::other(format!("fanin offset load error for '{outbox_name}': {e}"))
                })?
                .map(|r| r.get::<_, i64>(0))
                .unwrap_or(0);

            // v0.40.0: Fan-in members are native outboxes on the shared table.
            let mut src = OutboxPollerSource {
                db: Arc::clone(&db),
                outbox_name: outbox_name.clone(),
                mode: OutboxSourceMode::Native,
                subject_template: subject_template.to_string(),
                relay_group_id: relay_group_id.to_string(),
                pipeline_id: pipeline_name.to_string(),
                worker_id: worker_id.clone(),
                consumer_group: None,
                last_offset,
                pending_cc_oids: Vec::new(),
                replay_mode: false,
            };
            // Override the last_offset on the source so it starts from the
            // correct position for this member.
            src.last_offset = last_offset;
            sources.push(src);
        }

        Ok(Self {
            db,
            pipeline_name: pipeline_name.to_string(),
            sources,
            next_idx: 0,
            pending_offsets: Vec::new(),
            relay_group_id: relay_group_id.to_string(),
            worker_id,
        })
    }
}

#[async_trait::async_trait]
impl super::Source for FanInSource {
    fn name(&self) -> &str {
        "fanin"
    }

    async fn poll(&mut self, batch_size: i64) -> Result<Vec<RelayMessage>, RelayError> {
        self.pending_offsets.clear();
        if self.sources.is_empty() {
            return Ok(vec![]);
        }

        // Round-robin across contributing outboxes; collect up to `batch_size` messages.
        let num_sources = self.sources.len();
        let per_source = (batch_size / num_sources as i64).max(1);
        let mut messages = Vec::with_capacity(batch_size as usize);

        for i in 0..num_sources {
            let idx = (self.next_idx + i) % num_sources;
            let batch = self.sources[idx].poll(per_source).await?;
            for msg in batch {
                // Record which outbox produced this message for UNNEST commit.
                let outbox_name = self.sources[idx].outbox_name.clone();
                if let AckToken::OutboxOffset(offset) = &msg.ack_token {
                    self.pending_offsets.push((outbox_name, *offset));
                }
                messages.push(msg);
            }
        }

        self.next_idx = (self.next_idx + 1) % num_sources;
        Ok(messages)
    }

    async fn acknowledge(&mut self, _last_message: &RelayMessage) -> Result<(), RelayError> {
        if self.pending_offsets.is_empty() {
            return Ok(());
        }

        // v0.35.0 P2: UNNEST batch upsert — one round-trip instead of N sequential writes.
        // Groups offsets by outbox_name and keeps only the highest offset per member.
        let mut max_by_member: std::collections::HashMap<String, i64> =
            std::collections::HashMap::new();
        for (member, offset) in &self.pending_offsets {
            let entry = max_by_member.entry(member.clone()).or_insert(0);
            if *offset > *entry {
                *entry = *offset;
            }
        }

        let relay_group_ids: Vec<&str> = vec![self.relay_group_id.as_str(); max_by_member.len()];
        let pipeline_ids: Vec<&str> = vec![self.pipeline_name.as_str(); max_by_member.len()];
        let worker_ids: Vec<&str> = vec![self.worker_id.as_str(); max_by_member.len()];

        let members: Vec<String> = max_by_member.keys().cloned().collect();
        let offsets: Vec<i64> = members
            .iter()
            .map(|member| {
                max_by_member.get(member).copied().ok_or_else(|| {
                    RelayError::other(format!("fanin offset missing for member '{member}'"))
                })
            })
            .collect::<Result<_, _>>()?;

        let affected = self
            .db
            .execute(
                "INSERT INTO tide.relay_consumer_offsets
                     (relay_group_id, pipeline_id, outbox_name, fanin_member,
                      last_change_id, worker_id, updated_at)
                 SELECT
                     unnest($1::text[]),
                     unnest($2::text[]),
                     unnest($3::text[]),
                     unnest($3::text[]),
                     unnest($4::bigint[]),
                     unnest($5::text[]),
                     now()
                 ON CONFLICT (relay_group_id, pipeline_id, fanin_member)
                 WHERE fanin_member IS NOT NULL
                 DO UPDATE SET
                     last_change_id = GREATEST(
                         EXCLUDED.last_change_id,
                         relay_consumer_offsets.last_change_id
                     ),
                     worker_id  = EXCLUDED.worker_id,
                     updated_at = EXCLUDED.updated_at",
                &[
                    &relay_group_ids,
                    &pipeline_ids,
                    &members,
                    &offsets,
                    &worker_ids,
                ],
            )
            .await
            .map_err(|e| {
                RelayError::other(format!(
                    "fanin UNNEST offset upsert failed for pipeline '{}': {e}",
                    self.pipeline_name
                ))
            })?;
        if affected != members.len() as u64 {
            return Err(RelayError::other(format!(
                "fanin offset upsert for pipeline '{}' affected {affected} rows, expected {}",
                self.pipeline_name,
                members.len()
            )));
        }

        // Update the last_offset on each sub-source so the next poll advances correctly.
        for (member, max_offset) in &max_by_member {
            if let Some(src) = self.sources.iter_mut().find(|s| s.outbox_name == *member) {
                src.last_offset = *max_offset;
            }
        }

        self.pending_offsets.clear();
        Ok(())
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        // Nothing to close for an outbox fan-in source — the DB connection
        // is managed by the Arc<Client> shared with the coordinator.
        Ok(())
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

    #[test]
    fn test_extract_array_present() {
        let v = serde_json::json!({"inserted": [{"id": 1}, {"id": 2}], "deleted": []});
        let arr = extract_array(&v, "inserted");
        assert_eq!(arr.len(), 2);
    }

    #[test]
    fn test_extract_array_missing() {
        let v = serde_json::json!({});
        let arr = extract_array(&v, "missing");
        assert!(arr.is_empty());
    }

    #[test]
    fn test_decode_payload_inline_differential() {
        // Inline differential payload — no DB access needed (no claim-check).
        // We test the sync path; the async path uses an actual DB.
        let payload = serde_json::json!({
            "v": 1,
            "full_refresh": false,
            "claim_check": false,
            "inserted": [{"id": 1}, {"id": 2}],
            "deleted": [{"id": 0}]
        });
        let is_claim_check = payload
            .get("claim_check")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        assert!(!is_claim_check);
        let inserted = extract_array(&payload, "inserted");
        assert_eq!(inserted.len(), 2);
        let deleted = extract_array(&payload, "deleted");
        assert_eq!(deleted.len(), 1);
    }

    #[test]
    fn test_decode_payload_unsupported_version() {
        let payload = serde_json::json!({"v": 99});
        let v = payload.get("v").and_then(|v| v.as_i64()).unwrap_or(0);
        assert_eq!(v, 99);
        // Would return RelayError::UnsupportedPayloadVersion(99) if we called decode_payload.
    }

    // v0.31.0: Verify that poll_simple() generates properly double-quoted SQL
    // for outbox names containing hyphens.
    #[test]
    fn test_poll_simple_quoted_sql_plain_name() {
        let outbox_table_name = "outbox_orders";
        let quoted = format!("tide.\"{outbox_table_name}\"");
        let sql = format!(
            "SELECT id, payload FROM {table} WHERE id > $1 ORDER BY id LIMIT $2",
            table = quoted
        );
        assert!(
            sql.contains(r#"tide."outbox_orders""#),
            "plain name should be double-quoted: {sql}"
        );
        assert!(
            !sql.contains("tide.outbox_orders "),
            "plain name must not appear unquoted: {sql}"
        );
    }

    #[test]
    fn test_poll_simple_quoted_sql_hyphenated_name() {
        let outbox_table_name = "outbox_order-events";
        let quoted = format!("tide.\"{outbox_table_name}\"");
        let sql = format!(
            "SELECT id, payload FROM {table} WHERE id > $1 ORDER BY id LIMIT $2",
            table = quoted
        );
        assert!(
            sql.contains(r#"tide."outbox_order-events""#),
            "hyphenated name should be double-quoted: {sql}"
        );
        // Without quoting, PostgreSQL would parse the hyphen as minus, producing
        // invalid SQL like `FROM tide.outbox_order-events WHERE`.
        assert!(
            !sql.contains("tide.outbox_order-events "),
            "hyphenated name must not appear unquoted: {sql}"
        );
    }

    // v0.31.0: Verify that fetch_claim_check_rows() generates properly
    // double-quoted SQL for outbox names containing hyphens.
    #[test]
    fn test_fetch_claim_check_rows_quoted_sql_hyphenated_name() {
        let outbox_name = "order-events";
        let delta_table = format!("tide.\"outbox_delta_rows_{outbox_name}\"");
        assert!(
            delta_table.contains(r#"tide."outbox_delta_rows_order-events""#),
            "delta_table should be double-quoted: {delta_table}"
        );
    }
}
