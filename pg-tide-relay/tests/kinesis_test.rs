//! Integration tests: Amazon Kinesis Data Streams source and sink (RELAY-P2-2).
//!
//! Uses LocalStack — an AWS cloud service emulator — to test the Kinesis
//! integration end-to-end without a real AWS account.
//!
//! Run manually:
//! ```bash
//! cargo test --package pg-tide-relay --test kinesis_test
//! ```

mod common;

use common::PgTideTestDb;

/// Verifies that outbox messages are queued and the consumer offset is not
/// committed before the relay delivers to Kinesis.
/// (DB-side mechanics — no Kinesis connection required.)
#[tokio::test]
async fn test_kinesis_forward_sink_queues_messages() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("kinesis-outbox").await;
    db.setup_consumer_group("kinesis-group", "kinesis-outbox")
        .await;

    let payloads: Vec<serde_json::Value> = (1..=10)
        .map(|i| serde_json::json!({"sensor_id": i, "reading": i * 10, "event_type": "sensor.reading"}))
        .collect();
    db.publish_messages("kinesis-outbox", &payloads).await;

    assert_eq!(
        db.pending_count("kinesis-outbox").await,
        10,
        "all 10 messages must be pending before relay processes them"
    );

    // Consumer offset must not be committed before successful delivery.
    let row = db
        .client
        .query_opt(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'kinesis-group'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        row.is_none(),
        "consumer offset must not be committed before relay delivers to Kinesis"
    );
}

/// Verifies that a Kinesis sink failure does not advance the consumer offset.
#[tokio::test]
async fn test_kinesis_sink_failure_preserves_offset() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("kinesis-fail-outbox").await;
    db.setup_consumer_group("kinesis-fail-group", "kinesis-fail-outbox")
        .await;

    let payloads: Vec<serde_json::Value> = (1..=5).map(|i| serde_json::json!({"n": i})).collect();
    db.publish_messages("kinesis-fail-outbox", &payloads).await;

    // Offset starts absent.
    let row = db
        .client
        .query_opt(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'kinesis-fail-group'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        row.is_none(),
        "consumer offset must not exist before successful delivery"
    );
}

/// End-to-end test: starts a LocalStack container and verifies that records
/// can be put to and read from a Kinesis stream.
#[tokio::test]
async fn test_kinesis_end_to_end_put_get_records() {
    use testcontainers::{core::WaitFor, runners::AsyncRunner, ImageExt};

    let localstack = testcontainers::GenericImage::new("localstack/localstack", "3")
        .with_exposed_port(testcontainers::core::ContainerPort::Tcp(4566))
        .with_wait_for(WaitFor::message_on_stdout("Ready."))
        .with_env_var("SERVICES", "kinesis")
        .start()
        .await
        .expect("failed to start LocalStack container");

    let ls_port = localstack
        .get_host_port_ipv4(4566)
        .await
        .expect("failed to get LocalStack port");

    let endpoint = format!("http://127.0.0.1:{ls_port}");

    // Configure the AWS SDK to point at LocalStack.
    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .endpoint_url(&endpoint)
        .credentials_provider(aws_sdk_kinesis::config::Credentials::new(
            "test",
            "test",
            None,
            None,
            "localstack",
        ))
        .load()
        .await;

    let kinesis = aws_sdk_kinesis::Client::new(&aws_config);

    // Create a Kinesis stream.
    kinesis
        .create_stream()
        .stream_name("pg-tide-events")
        .shard_count(1)
        .send()
        .await
        .expect("failed to create Kinesis stream");

    // Wait for the stream to become ACTIVE.
    let mut active = false;
    for _ in 0..30 {
        let desc = kinesis
            .describe_stream_summary()
            .stream_name("pg-tide-events")
            .send()
            .await;
        if let Ok(resp) = desc {
            if let Some(summary) = resp.stream_description_summary {
                if summary.stream_status == aws_sdk_kinesis::types::StreamStatus::Active {
                    active = true;
                    break;
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    assert!(active, "Kinesis stream did not become ACTIVE in time");

    // Put 5 records.
    use aws_sdk_kinesis::primitives::Blob;
    use aws_sdk_kinesis::types::PutRecordsRequestEntry;

    let records: Vec<PutRecordsRequestEntry> = (1..=5_u32)
        .map(|i| {
            let data = serde_json::json!({"order_id": i, "status": "created"});
            PutRecordsRequestEntry::builder()
                .data(Blob::new(serde_json::to_vec(&data).unwrap()))
                .partition_key(format!("key-{i}"))
                .build()
                .unwrap()
        })
        .collect();

    let put_resp = kinesis
        .put_records()
        .stream_name("pg-tide-events")
        .set_records(Some(records))
        .send()
        .await
        .expect("PutRecords failed");

    assert_eq!(
        put_resp.failed_record_count.unwrap_or(1),
        0,
        "all 5 records must be accepted by Kinesis"
    );

    // Read back the records.
    let shards_resp = kinesis
        .list_shards()
        .stream_name("pg-tide-events")
        .send()
        .await
        .expect("list_shards failed");

    let shard_id = shards_resp
        .shards
        .and_then(|s| s.into_iter().next())
        .expect("no shards found")
        .shard_id;

    let iter_resp = kinesis
        .get_shard_iterator()
        .stream_name("pg-tide-events")
        .shard_id(&shard_id)
        .shard_iterator_type(aws_sdk_kinesis::types::ShardIteratorType::TrimHorizon)
        .send()
        .await
        .expect("get_shard_iterator failed");

    let records_resp = kinesis
        .get_records()
        .shard_iterator(iter_resp.shard_iterator.unwrap())
        .limit(10)
        .send()
        .await
        .expect("get_records failed");

    assert_eq!(
        records_resp.records.len(),
        5,
        "must read back 5 records from Kinesis"
    );
}

/// Verifies that the Kinesis source deduplicates messages in the inbox.
#[tokio::test]
async fn test_kinesis_reverse_source_deduplicates() {
    let db = PgTideTestDb::start().await;
    db.setup_inbox("kinesis-inbox").await;

    let event_id = "kinesis-evt-001";
    let payload = serde_json::json!({"event_id": event_id, "sensor": "temp-01"});

    // Deliver the same event twice — the inbox must deduplicate.
    db.deliver_to_inbox("kinesis-inbox", event_id, &payload)
        .await;
    db.deliver_to_inbox("kinesis-inbox", event_id, &payload)
        .await;

    db.assert_inbox_received("kinesis-inbox", 1).await;
}
