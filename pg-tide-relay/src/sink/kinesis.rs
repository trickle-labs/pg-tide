/// Amazon Kinesis Data Streams sink (RELAY-P2-2).
///
/// Publishes outbox messages to a Kinesis stream using `PutRecords`.
/// Each record uses `partition_key_template` (rendered from the message subject)
/// to determine shard placement.
///
/// Feature-gated: only compiled with `--features kinesis`.
use crate::envelope::RelayMessage;
use crate::error::RelayError;

#[cfg(feature = "kinesis")]
use aws_sdk_kinesis::Client as KinesisClient;

#[cfg(feature = "kinesis")]
pub struct KinesisSink {
    client: KinesisClient,
    stream_name: String,
    partition_key_template: String,
}

#[cfg(feature = "kinesis")]
impl KinesisSink {
    pub async fn new(
        stream_name: impl Into<String>,
        partition_key_template: impl Into<String>,
    ) -> Result<Self, RelayError> {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = KinesisClient::new(&config);
        Ok(Self {
            client,
            stream_name: stream_name.into(),
            partition_key_template: partition_key_template.into(),
        })
    }
}

#[cfg(feature = "kinesis")]
#[async_trait::async_trait]
impl super::Sink for KinesisSink {
    fn name(&self) -> &str {
        "kinesis"
    }

    /// Publish a batch using `PutRecords` (up to 500 records per call).
    async fn publish(&mut self, messages: &[RelayMessage]) -> Result<(), RelayError> {
        use aws_sdk_kinesis::primitives::Blob;
        use aws_sdk_kinesis::types::PutRecordsRequestEntry;

        // Kinesis limit: 500 records per PutRecords call.
        for chunk in messages.chunks(500) {
            let mut records = Vec::with_capacity(chunk.len());

            for msg in chunk {
                let partition_key = crate::envelope::render_subject(
                    &self.partition_key_template,
                    &msg.subject,
                    &msg.op,
                    msg.outbox_id.unwrap_or(0),
                    msg.refresh_id,
                );

                let data_bytes = serde_json::to_vec(msg).map_err(RelayError::Json)?;

                let entry = PutRecordsRequestEntry::builder()
                    .data(Blob::new(data_bytes))
                    .partition_key(partition_key)
                    .build()
                    .map_err(|e| RelayError::sink("kinesis", e))?;

                records.push(entry);
            }

            let result = self
                .client
                .put_records()
                .stream_name(&self.stream_name)
                .set_records(Some(records))
                .send()
                .await
                .map_err(|e| RelayError::sink("kinesis", e))?;

            // Surface any per-record failures.
            if result.failed_record_count.unwrap_or(0) > 0 {
                for rec in &result.records {
                    if let Some(code) = &rec.error_code {
                        let msg_text = rec.error_message.as_deref().unwrap_or("");
                        return Err(RelayError::SinkPublish {
                            sink: "kinesis".to_string(),
                            source: format!("record error {code}: {msg_text}").into(),
                        });
                    }
                }
            }
        }

        Ok(())
    }

    async fn is_healthy(&mut self) -> bool {
        self.client
            .describe_stream_summary()
            .stream_name(&self.stream_name)
            .send()
            .await
            .is_ok()
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        Ok(())
    }
}
