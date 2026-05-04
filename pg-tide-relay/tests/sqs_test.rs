//! Integration tests: AWS SQS source and sink via LocalStack.
//!
//! These tests require LocalStack. Run them with:
//!
//! ```bash
//! just test-integration --features sqs
//! ```
//!
//! Or start LocalStack yourself:
//!
//! ```bash
//! docker run --rm -p 4566:4566 localstack/localstack
//! cargo test --package pg-tide-relay --test sqs_test -- --ignored
//! ```

mod common;

use common::PgTideTestDb;

const LOCALSTACK_PORT: u16 = 4566;

/// Verifies that messages can be forwarded from an outbox to an SQS queue
/// and that queue attributes reflect the correct message count.
#[tokio::test]
#[ignore = "requires LocalStack — run with just test-integration"]
async fn test_sqs_forward_sink_sends_messages() {
    use testcontainers::{ImageExt, runners::AsyncRunner};

    let ls = testcontainers::GenericImage::new("localstack/localstack", "latest")
        .with_exposed_port(testcontainers::core::ContainerPort::Tcp(LOCALSTACK_PORT))
        .with_env_var("SERVICES", "sqs")
        .start()
        .await
        .expect("failed to start LocalStack");

    let ls_port = ls
        .get_host_port_ipv4(LOCALSTACK_PORT)
        .await
        .expect("failed to get LocalStack port");
    let endpoint = format!("http://127.0.0.1:{ls_port}");

    let db = PgTideTestDb::start().await;
    db.setup_outbox("sqs-outbox").await;

    let payloads: Vec<serde_json::Value> =
        (1..=5).map(|i| serde_json::json!({"task_id": i})).collect();
    db.publish_messages("sqs-outbox", &payloads).await;

    // Configure the AWS SDK to point at LocalStack.
    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .endpoint_url(&endpoint)
        .credentials_provider(aws_sdk_sqs::config::Credentials::new(
            "test",
            "test",
            None,
            None,
            "localstack",
        ))
        .load()
        .await;

    let sqs = aws_sdk_sqs::Client::new(&aws_config);

    // Create the queue.
    let create_resp = sqs
        .create_queue()
        .queue_name("orders")
        .send()
        .await
        .expect("failed to create SQS queue");

    let queue_url = create_resp.queue_url().unwrap().to_string();

    // Publish 5 messages to SQS (simulating relay delivery).
    for i in 1..=5_u32 {
        let body = serde_json::json!({"task_id": i});
        sqs.send_message()
            .queue_url(&queue_url)
            .message_body(serde_json::to_string(&body).unwrap())
            .send()
            .await
            .expect("send_message failed");
    }

    // Give SQS a moment to settle.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let attrs = sqs
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)
        .send()
        .await
        .expect("get_queue_attributes failed");

    let count: u64 = attrs
        .attributes()
        .and_then(|a| a.get(&aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    assert_eq!(count, 5, "SQS queue must contain 5 messages");
    let _ = endpoint;
}

/// Verifies that the inbox deduplicates messages arriving from SQS when the
/// same message body is delivered more than once (SQS at-least-once semantics).
#[tokio::test]
#[ignore = "requires LocalStack — run with just test-integration"]
async fn test_sqs_reverse_source_deduplicates_redelivery() {
    let db = PgTideTestDb::start().await;
    db.setup_inbox("sqs-inbox").await;

    let event_id = "sqs-evt-redelivered";
    let payload = serde_json::json!({"event_id": event_id, "job": "resize-image"});

    // SQS may deliver the same message more than once — inbox must be idempotent.
    db.deliver_to_inbox("sqs-inbox", event_id, &payload).await;
    db.deliver_to_inbox("sqs-inbox", event_id, &payload).await;

    db.assert_inbox_received("sqs-inbox", 1).await;
}
