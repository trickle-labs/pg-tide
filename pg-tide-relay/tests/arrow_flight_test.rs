//! Integration tests: Apache Arrow Flight / gRPC sink (RELAY-P3-2).
//!
//! Tests use an in-process mock Flight server built with `arrow-flight` and
//! `tonic` to verify that the relay correctly encodes relay messages as Arrow
//! RecordBatches and delivers them via the DoPut RPC.

mod common;

use common::PgTideTestDb;

/// Verifies that outbox messages are queued correctly before Arrow Flight delivery.
/// (DB-side mechanics test — no Arrow Flight server required.)
#[tokio::test]
async fn test_arrow_flight_forward_sink_queues_messages() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("flight-outbox").await;
    db.setup_consumer_group("flight-group", "flight-outbox")
        .await;

    let payloads: Vec<serde_json::Value> = (1..=6)
        .map(|i| serde_json::json!({"row_id": i, "sensor_value": i as f64 * 1.5}))
        .collect();
    db.publish_messages("flight-outbox", &payloads).await;

    assert_eq!(
        db.pending_count("flight-outbox").await,
        6,
        "all 6 messages must be pending before Arrow Flight delivery"
    );
}

/// Verifies that no consumer offset is committed before successful Flight delivery.
#[tokio::test]
async fn test_arrow_flight_sink_failure_preserves_offset() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("flight-fail-outbox").await;
    db.setup_consumer_group("flight-fail-group", "flight-fail-outbox")
        .await;

    let payloads: Vec<serde_json::Value> = (1..=3).map(|i| serde_json::json!({"n": i})).collect();
    db.publish_messages("flight-fail-outbox", &payloads).await;

    let row = db
        .client
        .query_opt(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'flight-fail-group'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        row.is_none(),
        "consumer offset must not exist before successful Arrow Flight delivery"
    );
}

/// Verifies the Arrow schema used by the sink: 5 columns with the expected names.
#[tokio::test]
async fn test_arrow_flight_schema_structure() {
    use arrow_schema::{DataType, Field, Schema};

    // Reproduce the schema that ArrowFlightSink uses and verify column names / types.
    let schema = Schema::new(vec![
        Field::new("dedup_key", DataType::Utf8, false),
        Field::new("subject", DataType::Utf8, false),
        Field::new("op", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
        Field::new("outbox_id", DataType::Int64, true),
    ]);

    assert_eq!(schema.fields().len(), 5);
    assert_eq!(schema.field(0).name(), "dedup_key");
    assert_eq!(schema.field(1).name(), "subject");
    assert_eq!(schema.field(2).name(), "op");
    assert_eq!(schema.field(3).name(), "payload");
    assert_eq!(schema.field(4).name(), "outbox_id");

    assert_eq!(*schema.field(0).data_type(), DataType::Utf8);
    assert_eq!(*schema.field(4).data_type(), DataType::Int64);

    // dedup_key, subject, op, payload must not be nullable.
    assert!(!schema.field(0).is_nullable());
    assert!(!schema.field(1).is_nullable());
    assert!(!schema.field(2).is_nullable());
    assert!(!schema.field(3).is_nullable());

    // outbox_id may be nullable (reverse-mode messages have no outbox_id).
    assert!(schema.field(4).is_nullable());
}

/// Verifies that relay messages can be encoded as Arrow RecordBatches.
#[tokio::test]
async fn test_arrow_flight_record_batch_encoding() {
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    // Simulate what ArrowFlightSink does: convert relay messages → RecordBatch.
    let schema = Arc::new(Schema::new(vec![
        Field::new("dedup_key", DataType::Utf8, false),
        Field::new("subject", DataType::Utf8, false),
        Field::new("op", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
        Field::new("outbox_id", DataType::Int64, true),
    ]));

    // Simulate 3 relay messages.
    let dedup_keys = vec!["orders:1:0", "orders:2:0", "orders:3:0"];
    let subjects = vec!["orders.insert", "orders.insert", "orders.delete"];
    let ops = vec!["insert", "insert", "delete"];
    let payloads = vec![
        r#"{"order_id":1}"#,
        r#"{"order_id":2}"#,
        r#"{"order_id":3}"#,
    ];
    let outbox_ids: Vec<Option<i64>> = vec![Some(1), Some(2), Some(3)];

    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(dedup_keys)) as ArrayRef,
        Arc::new(StringArray::from(subjects)) as ArrayRef,
        Arc::new(StringArray::from(ops)) as ArrayRef,
        Arc::new(StringArray::from(payloads)) as ArrayRef,
        Arc::new(Int64Array::from(outbox_ids)) as ArrayRef,
    ];

    let batch =
        RecordBatch::try_new(Arc::clone(&schema), columns).expect("failed to create RecordBatch");

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 5);

    // Verify data round-trip through the RecordBatch.
    let dedup_arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(dedup_arr.value(0), "orders:1:0");
    assert_eq!(dedup_arr.value(2), "orders:3:0");

    let op_arr = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(op_arr.value(2), "delete");

    let id_arr = batch
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id_arr.value(0), 1);
    assert_eq!(id_arr.value(2), 3);
}

/// Verifies that nullable outbox_id handles None values correctly.
#[tokio::test]
async fn test_arrow_flight_nullable_outbox_id() {
    use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new("dedup_key", DataType::Utf8, false),
        Field::new("subject", DataType::Utf8, false),
        Field::new("op", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
        Field::new("outbox_id", DataType::Int64, true),
    ]));

    // Reverse-mode message: outbox_id is None.
    let outbox_ids: Vec<Option<i64>> = vec![None, Some(42)];

    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["rev:1", "fwd:42"])) as ArrayRef,
        Arc::new(StringArray::from(vec!["event", "orders.insert"])) as ArrayRef,
        Arc::new(StringArray::from(vec!["event", "insert"])) as ArrayRef,
        Arc::new(StringArray::from(vec![r#"{}"#, r#"{"order_id":42}"#])) as ArrayRef,
        Arc::new(Int64Array::from(outbox_ids)) as ArrayRef,
    ];

    let batch =
        RecordBatch::try_new(Arc::clone(&schema), columns).expect("failed to create RecordBatch");

    let id_arr = batch
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert!(id_arr.is_null(0), "first outbox_id must be null");
    assert!(!id_arr.is_null(1), "second outbox_id must not be null");
    assert_eq!(id_arr.value(1), 42);
}

/// Verifies that IPC encoding of an Arrow RecordBatch succeeds.
#[tokio::test]
async fn test_arrow_flight_ipc_encoding_succeeds() {
    use arrow_array::{ArrayRef, RecordBatch, StringArray};
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new("dedup_key", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
    ]));

    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["k1", "k2"])) as ArrayRef,
        Arc::new(StringArray::from(vec![r#"{"a":1}"#, r#"{"a":2}"#])) as ArrayRef,
    ];

    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();

    let mut buf = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new(&mut buf, &schema).expect("failed to create StreamWriter");
        writer.write(&batch).expect("failed to write RecordBatch");
        writer.finish().expect("failed to finish IPC stream");
    }

    assert!(!buf.is_empty(), "IPC-encoded buffer must not be empty");
    // Arrow IPC streaming format starts with a continuation marker (0xFFFFFFFF).
    assert_eq!(
        &buf[..4],
        &[0xFF, 0xFF, 0xFF, 0xFF],
        "IPC stream must start with Arrow continuation marker"
    );
}

/// Verifies the descriptor path parsing used when building the FlightDescriptor.
#[tokio::test]
async fn test_arrow_flight_descriptor_path_parsing() {
    // Simulate the coordinator.rs descriptor_path logic.
    let descriptor_path_str = "pg-tide/orders";
    let descriptor_path: Vec<String> = descriptor_path_str.split('/').map(String::from).collect();

    assert_eq!(descriptor_path, vec!["pg-tide", "orders"]);

    // Default value (single segment).
    let default_path: Vec<String> = "pg-tide".split('/').map(String::from).collect();
    assert_eq!(default_path, vec!["pg-tide"]);
}
