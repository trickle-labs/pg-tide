/// Amazon Kinesis Data Streams source (RELAY-P2-2).
///
/// Reads records from all shards of a Kinesis stream using `GetShardIterator`
/// + `GetRecords`. Checkpoints per-shard sequence numbers in memory between polls.
///
/// On restart, starts from `TRIM_HORIZON` (oldest available).
///
/// Feature-gated: only compiled with `--features kinesis`.
use std::collections::HashMap;

use crate::envelope::{AckToken, RelayMessage};
use crate::error::RelayError;

#[cfg(feature = "kinesis")]
use aws_sdk_kinesis::{
    types::{Shard, ShardIteratorType},
    Client as KinesisClient,
};

/// Per-shard state: current iterator and last sequence number.
#[cfg(feature = "kinesis")]
struct ShardState {
    iterator: Option<String>,
    last_sequence: Option<String>,
}

#[cfg(feature = "kinesis")]
pub struct KinesisSource {
    client: KinesisClient,
    stream_name: String,
    event_type: String,
    /// shard_id → state
    shards: HashMap<String, ShardState>,
    /// Iterator type for freshly-joined shards.
    iterator_type: ShardIteratorType,
}

#[cfg(feature = "kinesis")]
impl KinesisSource {
    pub async fn new(
        stream_name: impl Into<String>,
        event_type: impl Into<String>,
        iterator_type: impl Into<String>,
    ) -> Result<Self, RelayError> {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = KinesisClient::new(&config);
        let it_type = match iterator_type.into().as_str() {
            "LATEST" => ShardIteratorType::Latest,
            "AT_SEQUENCE_NUMBER" => ShardIteratorType::AtSequenceNumber,
            "AFTER_SEQUENCE_NUMBER" => ShardIteratorType::AfterSequenceNumber,
            _ => ShardIteratorType::TrimHorizon,
        };
        Ok(Self {
            client,
            stream_name: stream_name.into(),
            event_type: event_type.into(),
            shards: HashMap::new(),
            iterator_type: it_type,
        })
    }

    /// Discover shards and initialise iterators for any new shards.
    async fn refresh_shards(&mut self) -> Result<(), RelayError> {
        let resp = self
            .client
            .list_shards()
            .stream_name(&self.stream_name)
            .send()
            .await
            .map_err(|e| RelayError::source_poll("kinesis", e))?;

        let shards: Vec<Shard> = resp.shards.unwrap_or_default();

        for shard in shards {
            let shard_id = shard.shard_id;
            if self.shards.contains_key(&shard_id) {
                continue;
            }

            let it_resp = self
                .client
                .get_shard_iterator()
                .stream_name(&self.stream_name)
                .shard_id(&shard_id)
                .shard_iterator_type(self.iterator_type.clone())
                .send()
                .await
                .map_err(|e| RelayError::source_poll("kinesis", e))?;

            self.shards.insert(
                shard_id,
                ShardState {
                    iterator: it_resp.shard_iterator,
                    last_sequence: None,
                },
            );
        }

        Ok(())
    }
}

#[cfg(feature = "kinesis")]
#[async_trait::async_trait]
impl super::Source for KinesisSource {
    fn name(&self) -> &str {
        "kinesis"
    }

    async fn poll(&mut self, batch_size: i64) -> Result<Vec<RelayMessage>, RelayError> {
        self.refresh_shards().await?;

        let limit = batch_size.min(10_000) as i32;
        let mut messages = Vec::new();

        let shard_ids: Vec<String> = self.shards.keys().cloned().collect();

        for shard_id in &shard_ids {
            let state = match self.shards.get_mut(shard_id) {
                Some(s) => s,
                None => continue,
            };

            let iterator = match state.iterator.take() {
                Some(it) => it,
                None => continue, // Shard exhausted.
            };

            let resp = self
                .client
                .get_records()
                .shard_iterator(iterator)
                .limit(limit)
                .send()
                .await
                .map_err(|e| RelayError::source_poll("kinesis", e))?;

            // Store next iterator for subsequent polls.
            state.iterator = resp.next_shard_iterator;

            for record in resp.records {
                let seq = record.sequence_number.clone();
                state.last_sequence = Some(seq.clone());

                let payload: serde_json::Value =
                    serde_json::from_slice(record.data.as_ref()).unwrap_or(serde_json::Value::Null);

                let dedup_key = format!("kinesis:{shard_id}:{seq}");

                let event_type = payload
                    .get("event_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&self.event_type)
                    .to_string();

                let mut relay_msg = RelayMessage::new_reverse(dedup_key, event_type, payload);
                relay_msg.ack_token = AckToken::None;
                messages.push(relay_msg);
            }
        }

        Ok(messages)
    }

    async fn acknowledge(&mut self, _last_message: &RelayMessage) -> Result<(), RelayError> {
        // Kinesis does not have a native message acknowledgement mechanism.
        // Records are retained for the stream's retention period regardless.
        // Sequence numbers are checkpointed in memory and used by refresh_shards
        // to resume from the correct position on restart (future enhancement).
        Ok(())
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}
