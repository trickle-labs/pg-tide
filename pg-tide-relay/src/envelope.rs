/// RelayMessage — the message envelope used by the forward relay.
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Message envelope shared by the forward relay and diagnostic sinks.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RelayMessage {
    /// Dedup key for idempotent delivery.
    /// Forward: "{outbox_table}:{outbox_id}:{row_index}"
    pub dedup_key: String,

    /// Resolved subject or topic.
    pub subject: String,

    /// The row/event payload as JSON.
    pub payload: serde_json::Value,

    /// Operation: "insert", "update", or "delete".
    pub op: String,

    /// Whether this batch is a full-refresh snapshot (forward only).
    pub is_full_refresh: bool,

    /// Native outbox metadata.
    pub outbox_id: Option<i64>,
    pub refresh_id: Option<Uuid>,

    /// v0.40.0 (ADR-011): Logical outbox name for native forward messages.
    /// Additive; serde default keeps older recorded messages readable.
    #[serde(default)]
    pub outbox_name: Option<String>,

    /// v0.40.0 (ADR-011): Outbox row `headers` JSONB (native forward messages).
    #[serde(default)]
    pub headers: Option<serde_json::Value>,

    /// v0.40.0 (ADR-011): Outbox row `created_at` timestamp (native forward).
    #[serde(default)]
    pub created_at: Option<chrono::DateTime<chrono::Utc>>,

    /// Source-specific metadata for acknowledgement (not serialized).
    #[serde(skip)]
    pub ack_token: AckToken,
}

/// Opaque token that the Source uses to acknowledge a message.
/// Each source backend stores whatever it needs here.
#[derive(Debug, Clone, Default)]
pub enum AckToken {
    #[default]
    None,
    OutboxOffset(i64),
}

impl RelayMessage {
    #[allow(clippy::too_many_arguments)]
    pub fn new_forward(
        outbox_table: &str,
        outbox_id: i64,
        row_index: usize,
        op: impl Into<String>,
        payload: serde_json::Value,
        is_full_refresh: bool,
        refresh_id: Option<Uuid>,
        subject: impl Into<String>,
    ) -> Self {
        Self {
            dedup_key: format!("{outbox_table}:{outbox_id}:{row_index}"),
            subject: subject.into(),
            payload,
            op: op.into(),
            is_full_refresh,
            outbox_id: Some(outbox_id),
            refresh_id,
            outbox_name: None,
            headers: None,
            created_at: None,
            ack_token: AckToken::None,
        }
    }

    pub fn new_reverse(
        dedup_key: impl Into<String>,
        event_type: impl Into<String>,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            dedup_key: dedup_key.into(),
            subject: event_type.into(),
            payload,
            op: "event".to_string(),
            is_full_refresh: false,
            outbox_id: None,
            refresh_id: None,
            outbox_name: None,
            headers: None,
            created_at: None,
            ack_token: AckToken::None,
        }
    }
}

/// Batch of outbox rows decoded from one outbox poll.
#[derive(Debug)]
pub struct OutboxBatch {
    pub outbox_id: i64,
    pub refresh_id: Option<Uuid>,
    pub is_full_refresh: bool,
    pub inserted: Vec<serde_json::Value>,
    pub deleted: Vec<serde_json::Value>,
}

impl OutboxBatch {
    /// Convert this batch into a vector of RelayMessages.
    ///
    /// v0.24.0: Consumes `self` via `into_iter()` to avoid cloning the full
    /// payload Vec on every message decode (was `.iter().clone()` previously).
    pub fn into_messages(self, outbox_table: &str, subject_template: &str) -> Vec<RelayMessage> {
        let inserted_len = self.inserted.len();
        let mut msgs = Vec::with_capacity(self.inserted.len() + self.deleted.len());

        for (i, row) in self.inserted.into_iter().enumerate() {
            let subject = render_subject(
                subject_template,
                outbox_table,
                "insert",
                self.outbox_id,
                self.refresh_id,
            );
            msgs.push(RelayMessage::new_forward(
                outbox_table,
                self.outbox_id,
                i,
                "insert",
                row,
                self.is_full_refresh,
                self.refresh_id,
                subject,
            ));
        }
        for (i, row) in self.deleted.into_iter().enumerate() {
            let subject = render_subject(
                subject_template,
                outbox_table,
                "delete",
                self.outbox_id,
                self.refresh_id,
            );
            msgs.push(RelayMessage::new_forward(
                outbox_table,
                self.outbox_id,
                inserted_len + i,
                "delete",
                row,
                false,
                self.refresh_id,
                subject,
            ));
        }
        msgs
    }
}

/// Render a subject/topic template.
/// Variables: `{stream_table}`, `{op}`, `{outbox_id}`, `{refresh_id}`.
pub fn render_subject(
    template: &str,
    stream_table: &str,
    op: &str,
    outbox_id: i64,
    refresh_id: Option<Uuid>,
) -> String {
    template
        .replace("{stream_table}", stream_table)
        .replace("{op}", op)
        .replace("{outbox_id}", &outbox_id.to_string())
        .replace(
            "{refresh_id}",
            &refresh_id.map(|u| u.to_string()).unwrap_or_default(),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_subject_all_vars() {
        let rid = Uuid::new_v4();
        let result = render_subject(
            "pgtrickle.{stream_table}.{op}",
            "orders",
            "insert",
            42,
            Some(rid),
        );
        assert_eq!(result, "pgtrickle.orders.insert");
    }

    #[test]
    fn test_render_subject_no_vars() {
        let result = render_subject("my-topic", "orders", "delete", 1, None);
        assert_eq!(result, "my-topic");
    }

    #[test]
    fn test_relay_message_forward_dedup_key() {
        let msg = RelayMessage::new_forward(
            "outbox_orders",
            10,
            3,
            "insert",
            serde_json::json!({"id": 1}),
            false,
            None,
            "test-subject",
        );
        assert_eq!(msg.dedup_key, "outbox_orders:10:3");
        assert_eq!(msg.op, "insert");
        assert!(!msg.is_full_refresh);
    }

    #[test]
    fn test_relay_message_reverse() {
        let msg = RelayMessage::new_reverse("kafka:0:100", "order.created", serde_json::json!({}));
        assert_eq!(msg.op, "event");
        assert_eq!(msg.dedup_key, "kafka:0:100");
    }

    #[test]
    fn test_outbox_batch_into_messages() {
        let batch = OutboxBatch {
            outbox_id: 5,
            refresh_id: None,
            is_full_refresh: false,
            inserted: vec![serde_json::json!({"id": 1}), serde_json::json!({"id": 2})],
            deleted: vec![serde_json::json!({"id": 0})],
        };
        let msgs = batch.into_messages("outbox_orders", "pgtrickle.{stream_table}.{op}");
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0].op, "insert");
        assert_eq!(msgs[2].op, "delete");
        assert_eq!(msgs[0].dedup_key, "outbox_orders:5:0");
        assert_eq!(msgs[2].dedup_key, "outbox_orders:5:2");
    }

    #[test]
    fn test_relay_message_serialization_round_trip() {
        let msg = RelayMessage::new_forward(
            "outbox_orders",
            7,
            0,
            "insert",
            serde_json::json!({"id": 42, "name": "test"}),
            true,
            Some(Uuid::new_v4()),
            "orders.insert",
        );
        let json = serde_json::to_string(&msg).unwrap();
        let decoded: RelayMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.dedup_key, msg.dedup_key);
        assert!(decoded.is_full_refresh);
        assert_eq!(decoded.outbox_id, Some(7));
        // ack_token is skipped in serialization
        assert!(matches!(decoded.ack_token, AckToken::None));
    }
}
