/// Native pg_tide envelope wire format.
///
/// This is the default wire format — it is a transparent pass-through of the
/// existing `RelayMessage` envelope. The decode and encode paths mirror the
/// current relay behaviour so that switching to the `WireFormat` abstraction
/// introduces no behaviour change for pipelines that do not set `wire_format`.
use serde_json::json;

#[allow(unused_imports)]
use super::{
    debezium_op_to_pg, EncodeContext, EncodedBatch, EncodedMessage, InboxRow, OutboxRow,
    RawMessage, WireError, WireFormat,
};

/// Wire format that speaks the native pg_tide envelope.
///
/// On the decode side it expects messages whose value is a JSON object that
/// can be treated directly as an inbox payload; on the encode side it emits
/// the outbox row's payload verbatim.
#[derive(Debug, Default)]
pub struct NativePgTideFormat;

impl NativePgTideFormat {
    pub fn new() -> Self {
        Self
    }
}

impl WireFormat for NativePgTideFormat {
    fn name(&self) -> &'static str {
        "native"
    }

    fn decode(&self, raw: &RawMessage) -> Result<Option<InboxRow>, WireError> {
        let value_bytes = match &raw.value {
            Some(b) => b,
            // Tombstone: skip it in the native format.
            None => return Ok(None),
        };

        let payload: serde_json::Value = serde_json::from_slice(value_bytes).map_err(|e| {
            WireError::decode(&raw.topic, format!("native: JSON decode failed: {e}"))
        })?;

        // Try to extract op, event_id, and event_type from the payload if
        // they are present; otherwise fall back to sensible defaults.
        let op = payload
            .get("op")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| {
                payload
                    .get("operation")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .unwrap_or_else(|| "event".to_string());

        let event_id = payload
            .get("dedup_key")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        Ok(Some(InboxRow {
            event_id,
            event_type: raw.topic.clone(),
            payload,
            old_payload: None,
            op,
            commit_ts: None,
            source_position: None,
        }))
    }

    fn encode(&self, row: &OutboxRow, ctx: &EncodeContext) -> Result<EncodedBatch, WireError> {
        let topic = ctx.resolve_topic(row);
        let key = Some(format!("{}:{}", row.stream_table, row.outbox_id).into_bytes());

        let value = json!({
            "outbox_id": row.outbox_id,
            "op": row.op,
            "stream_table": row.stream_table,
            "payload": row.new_row.as_ref().or(row.old_row.as_ref()),
        });

        let encoded = EncodedMessage {
            topic,
            key,
            value: Some(
                serde_json::to_vec(&value)
                    .map_err(|e| WireError::encode(row.outbox_id, e.to_string()))?,
            ),
        };

        Ok(EncodedBatch::single(encoded))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_raw(topic: &str, value: serde_json::Value) -> RawMessage {
        RawMessage::from_json(topic, &value)
    }

    #[test]
    fn test_decode_basic() {
        let fmt = NativePgTideFormat::new();
        let raw = make_raw("orders", json!({"order_id": 1, "op": "insert"}));
        let row = fmt.decode(&raw).unwrap().unwrap();
        assert_eq!(row.op, "insert");
        assert_eq!(row.event_type, "orders");
    }

    #[test]
    fn test_decode_tombstone_returns_none() {
        let fmt = NativePgTideFormat::new();
        let raw = RawMessage::tombstone("orders", b"key".to_vec());
        let result = fmt.decode(&raw).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_encode_insert() {
        let fmt = NativePgTideFormat::new();
        let ctx = EncodeContext::default();
        let row = OutboxRow {
            outbox_id: 42,
            stream_table: "orders".to_string(),
            database: "app".to_string(),
            schema_name: "public".to_string(),
            op: "insert".to_string(),
            new_row: Some(json!({"id": 1})),
            old_row: None,
            commit_ts: None,
            source_lsn: None,
        };

        let batch = fmt.encode(&row, &ctx).unwrap();
        assert_eq!(batch.messages.len(), 1);
        let msg = &batch.messages[0];
        assert!(msg.value.is_some());
        let v: serde_json::Value = serde_json::from_slice(msg.value.as_ref().unwrap()).unwrap();
        assert_eq!(v["outbox_id"], 42);
        assert_eq!(v["op"], "insert");
    }

    #[test]
    fn test_name() {
        let fmt = NativePgTideFormat::new();
        assert_eq!(fmt.name(), "native");
    }
}
