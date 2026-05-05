/// Apache Arrow Flight gRPC sink (RELAY-P3-2).
///
/// Pushes relay messages to an Arrow Flight server using the `DoPut` RPC.
/// Messages are encoded as Arrow RecordBatches with the following fixed schema:
///
/// | Column      | Arrow type | Nullable |
/// |-------------|-----------|----------|
/// | dedup_key   | Utf8      | No       |
/// | subject     | Utf8      | No       |
/// | op          | Utf8      | No       |
/// | payload     | Utf8      | No       |
/// | outbox_id   | Int64     | Yes      |
///
/// The payload column contains the JSON-serialized relay message payload.
///
/// Feature-gated: only compiled with `--features arrow-flight`.
use crate::envelope::RelayMessage;
use crate::error::RelayError;

#[cfg(feature = "arrow-flight")]
use {
    arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray},
    arrow_flight::{
        encode::FlightDataEncoderBuilder, flight_service_client::FlightServiceClient,
        FlightDescriptor,
    },
    arrow_schema::{DataType, Field, Schema},
    futures_util::TryStreamExt,
    std::sync::Arc,
    tonic::transport::Channel,
};

#[cfg(feature = "arrow-flight")]
pub struct ArrowFlightSink {
    /// gRPC endpoint URL (e.g. `"http://localhost:50051"`).
    url: String,
    /// Optional Bearer token for authentication.
    auth_token: Option<String>,
    /// Flight descriptor path (identifies the stream on the server).
    descriptor_path: Vec<String>,
    /// Pre-built Arrow schema.
    schema: Arc<Schema>,
    /// gRPC channel (lazily established on first publish).
    channel: Option<FlightServiceClient<Channel>>,
}

#[cfg(feature = "arrow-flight")]
impl ArrowFlightSink {
    pub fn new(
        url: impl Into<String>,
        auth_token: Option<String>,
        descriptor_path: Vec<String>,
    ) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("dedup_key", DataType::Utf8, false),
            Field::new("subject", DataType::Utf8, false),
            Field::new("op", DataType::Utf8, false),
            Field::new("payload", DataType::Utf8, false),
            Field::new("outbox_id", DataType::Int64, true),
        ]));

        Self {
            url: url.into(),
            auth_token,
            descriptor_path,
            schema,
            channel: None,
        }
    }

    /// Establish the gRPC channel if not already connected.
    async fn ensure_connected(&mut self) -> Result<(), RelayError> {
        if self.channel.is_none() {
            let endpoint = tonic::transport::Endpoint::from_shared(self.url.clone())
                .map_err(|e| RelayError::sink("arrow-flight", e))?
                .connect_timeout(std::time::Duration::from_secs(10))
                .timeout(std::time::Duration::from_secs(30));

            let channel: Channel = endpoint
                .connect()
                .await
                .map_err(|e| RelayError::sink("arrow-flight", e))?;

            self.channel = Some(FlightServiceClient::new(channel));
        }
        Ok(())
    }

    /// Convert a slice of relay messages into an Arrow RecordBatch.
    fn messages_to_batch(&self, messages: &[RelayMessage]) -> Result<RecordBatch, RelayError> {
        let dedup_keys: Vec<&str> = messages.iter().map(|m| m.dedup_key.as_str()).collect();
        let subjects: Vec<&str> = messages.iter().map(|m| m.subject.as_str()).collect();
        let ops: Vec<&str> = messages.iter().map(|m| m.op.as_str()).collect();
        let payloads: Vec<String> = messages
            .iter()
            .map(|m| serde_json::to_string(&m.payload).unwrap_or_default())
            .collect();
        let payload_refs: Vec<&str> = payloads.iter().map(|s| s.as_str()).collect();
        let outbox_ids: Vec<Option<i64>> = messages.iter().map(|m| m.outbox_id).collect();

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(dedup_keys)) as ArrayRef,
            Arc::new(StringArray::from(subjects)) as ArrayRef,
            Arc::new(StringArray::from(ops)) as ArrayRef,
            Arc::new(StringArray::from(payload_refs)) as ArrayRef,
            Arc::new(Int64Array::from(outbox_ids)) as ArrayRef,
        ];

        RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(|e| {
            RelayError::Other(format!("arrow-flight: failed to build RecordBatch: {e}"))
        })
    }
}

#[cfg(feature = "arrow-flight")]
#[async_trait::async_trait]
impl super::Sink for ArrowFlightSink {
    fn name(&self) -> &str {
        "arrow-flight"
    }

    async fn publish(&mut self, messages: &[RelayMessage]) -> Result<(), RelayError> {
        if messages.is_empty() {
            return Ok(());
        }

        let batch = self.messages_to_batch(messages)?;
        let schema = Arc::clone(&self.schema);

        // Encode the RecordBatch as a stream of FlightData (Arrow IPC format).
        // Eagerly collect all FlightData frames so encoding errors surface before
        // the gRPC call starts — this also simplifies the streaming type.
        let descriptor = FlightDescriptor::new_path(self.descriptor_path.clone());
        let flight_data: Vec<arrow_flight::FlightData> = FlightDataEncoderBuilder::new()
            .with_schema(Arc::clone(&schema))
            .with_flight_descriptor(Some(descriptor))
            .build(futures_util::stream::iter(vec![Ok(batch)]))
            .try_collect()
            .await
            .map_err(|e| RelayError::Other(format!("arrow-flight: encode error: {e}")))?;

        let mut request = tonic::Request::new(futures_util::stream::iter(flight_data));

        // Attach auth header if provided.
        if let Some(ref token) = self.auth_token {
            let header_value = format!("Bearer {token}")
                .parse::<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>()
                .map_err(|e| RelayError::Other(format!("arrow-flight: invalid auth token: {e}")))?;
            request.metadata_mut().insert("authorization", header_value);
        }

        self.ensure_connected().await?;
        let client = self.channel.as_mut().expect("channel established");

        let mut response = client
            .do_put(request)
            .await
            .map_err(|e| RelayError::sink("arrow-flight", e))?
            .into_inner();

        // Drain the response stream (server sends PutResult messages back).
        while let Some(_put_result) = response
            .try_next()
            .await
            .map_err(|e| RelayError::sink("arrow-flight", e))?
        {
            // PutResult is an application-defined ack; we ignore the content
            // but consuming the stream ensures the server processed the batch.
        }

        Ok(())
    }

    async fn is_healthy(&mut self) -> bool {
        // A full health check would ping the server, but establishing a
        // connection is expensive. We rely on the publish error path for now.
        true
    }

    async fn close(&mut self) -> Result<(), RelayError> {
        // Tonic channels are reference-counted — dropping the client is enough.
        self.channel = None;
        Ok(())
    }
}

#[cfg(all(test, feature = "arrow-flight"))]
mod tests {
    use super::*;
    use crate::envelope::RelayMessage;

    fn make_sink() -> ArrowFlightSink {
        ArrowFlightSink::new("http://localhost:50051", None, vec!["pg-tide".to_string()])
    }

    fn make_msg(op: &str, order_id: i64) -> RelayMessage {
        RelayMessage::new_forward(
            "orders",
            order_id,
            0,
            op,
            serde_json::json!({"order_id": order_id}),
            false,
            None,
            format!("orders.{op}"),
        )
    }

    #[test]
    fn test_schema_has_five_columns() {
        let sink = make_sink();
        assert_eq!(sink.schema.fields().len(), 5);
        assert_eq!(sink.schema.field(0).name(), "dedup_key");
        assert_eq!(sink.schema.field(1).name(), "subject");
        assert_eq!(sink.schema.field(2).name(), "op");
        assert_eq!(sink.schema.field(3).name(), "payload");
        assert_eq!(sink.schema.field(4).name(), "outbox_id");
    }

    #[test]
    fn test_messages_to_batch_row_count() {
        let sink = make_sink();
        let msgs = vec![
            make_msg("insert", 1),
            make_msg("delete", 2),
            make_msg("insert", 3),
        ];
        let batch = sink.messages_to_batch(&msgs).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 5);
    }

    #[test]
    fn test_messages_to_batch_data_values() {
        let sink = make_sink();
        let msgs = vec![make_msg("delete", 99)];
        let batch = sink.messages_to_batch(&msgs).unwrap();

        let dedup_arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(dedup_arr.value(0), "orders:99:0");

        let op_arr = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(op_arr.value(0), "delete");

        let id_arr = batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(id_arr.value(0), 99);
    }

    #[test]
    fn test_messages_to_batch_null_outbox_id() {
        use crate::envelope::AckToken;
        use arrow_array::Array;
        let sink = make_sink();
        // A reverse-mode message has outbox_id = None.
        let mut msg = RelayMessage::new_reverse("rev:key", "event", serde_json::json!({}));
        msg.ack_token = AckToken::None;
        let batch = sink.messages_to_batch(&[msg]).unwrap();
        let id_arr = batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert!(
            id_arr.is_null(0),
            "reverse-mode message outbox_id must be null"
        );
    }
}
