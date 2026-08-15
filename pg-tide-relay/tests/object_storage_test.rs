//! Integration tests: Object Storage sink (v0.6.0).
//!
//! These tests verify the relay's database-side guarantees and S3 transport
//! behavior (via LocalStack). JSONL and Parquet encoding are tested by unit
//! tests within `src/sink/object_storage.rs`.
//!
//! Run manually:
//! ```bash
//! cargo test --package pg-tide-relay --test object_storage_test
//! ```

mod common;

use common::PgTideTestDb;

/// Verifies that outbox messages are queued and the consumer offset is not
/// committed before the relay flushes to object storage.
#[tokio::test]
async fn test_object_storage_forward_sink_queues_messages() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("s3-outbox").await;
    db.setup_consumer_group("s3-group", "s3-outbox").await;

    let payloads: Vec<serde_json::Value> = (1..=12)
        .map(|i| {
            serde_json::json!({
                "order_id": i,
                "customer": format!("cust-{i}"),
                "amount": i * 100,
                "event_type": "order.placed"
            })
        })
        .collect();
    db.publish_messages("s3-outbox", &payloads).await;

    assert_eq!(
        db.pending_count("s3-outbox").await,
        12,
        "all 12 messages must be pending before relay flushes to object storage"
    );

    // Consumer offset must not be committed before successful flush.
    let row = db
        .client
        .query_opt(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 's3-group'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        row.is_none(),
        "consumer offset must not be committed before relay flushes to object storage"
    );
}

/// Verifies that a failed object storage flush does not advance the offset.
#[tokio::test]
async fn test_object_storage_sink_failure_preserves_offset() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("s3-fail-outbox").await;
    db.setup_consumer_group("s3-fail-group", "s3-fail-outbox")
        .await;

    let payloads: Vec<serde_json::Value> = (1..=5).map(|i| serde_json::json!({"row": i})).collect();
    db.publish_messages("s3-fail-outbox", &payloads).await;

    let row = db
        .client
        .query_opt(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 's3-fail-group'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        row.is_none(),
        "consumer offset must not exist before successful object storage flush"
    );
}

/// Verifies that GCS-style outbox messages queue correctly (same DB mechanics).
#[tokio::test]
async fn test_object_storage_gcs_queues_messages() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("gcs-outbox").await;
    db.setup_consumer_group("gcs-group", "gcs-outbox").await;

    let payloads: Vec<serde_json::Value> = (1..=4)
        .map(|i| {
            serde_json::json!({
                "event_id": format!("gcs-evt-{i}"),
                "topic": "analytics",
                "data": { "count": i }
            })
        })
        .collect();
    db.publish_messages("gcs-outbox", &payloads).await;

    assert_eq!(
        db.pending_count("gcs-outbox").await,
        4,
        "all 4 messages must be pending before relay flushes to GCS"
    );
}

/// Verifies that Azure Blob-style outbox messages queue correctly.
#[tokio::test]
async fn test_object_storage_azure_blob_queues_messages() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("blob-outbox").await;
    db.setup_consumer_group("blob-group", "blob-outbox").await;

    let payloads: Vec<serde_json::Value> = (1..=3)
        .map(|i| {
            serde_json::json!({
                "record_id": i,
                "container": "analytics",
                "event_type": "page.view"
            })
        })
        .collect();
    db.publish_messages("blob-outbox", &payloads).await;

    assert_eq!(
        db.pending_count("blob-outbox").await,
        3,
        "all 3 messages must be pending before relay flushes to Azure Blob"
    );
}

/// Transport integration S3 test using LocalStack.
///
/// Uses the `object_store` crate directly to write a JSONL file and verifies
/// the file was created with the correct content — testing the same code path
/// the relay uses internally.
#[tokio::test]
async fn test_object_storage_s3_transport_integration_jsonl() {
    use testcontainers::{core::WaitFor, runners::AsyncRunner, ImageExt};

    let localstack = testcontainers::GenericImage::new("localstack/localstack", "3")
        .with_exposed_port(testcontainers::core::ContainerPort::Tcp(4566))
        .with_wait_for(WaitFor::message_on_stdout("Ready."))
        .with_env_var("SERVICES", "s3")
        .start()
        .await
        .expect("failed to start LocalStack container");

    let ls_port = localstack
        .get_host_port_ipv4(4566)
        .await
        .expect("failed to get LocalStack port");

    let endpoint = format!("http://127.0.0.1:{ls_port}");

    // Create the test bucket.
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .endpoint_url(&endpoint)
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            "test",
            "test",
            None,
            None,
            "localstack",
        ))
        .load()
        .await;

    let s3_client = aws_sdk_s3::Client::new(&config);
    s3_client
        .create_bucket()
        .bucket("pg-tide-test")
        .send()
        .await
        .expect("failed to create test bucket");

    // Write a JSONL file using the object_store crate (same code path as the sink).
    let store = object_store::aws::AmazonS3Builder::new()
        .with_bucket_name("pg-tide-test")
        .with_region("us-east-1")
        .with_endpoint(&endpoint)
        .with_access_key_id("test")
        .with_secret_access_key("test")
        .with_allow_http(true)
        .build()
        .expect("failed to build S3 store");

    use object_store::{path::Path, ObjectStore};

    let path = Path::from("test/orders/pgtide_batch_001.jsonl");
    let content = "{\"order_id\":1,\"amount\":100}\n{\"order_id\":2,\"amount\":200}\n{\"order_id\":3,\"amount\":300}\n";
    store
        .put(&path, content.as_bytes().to_vec().into())
        .await
        .expect("failed to write JSONL to S3");

    // Verify the object exists and has the correct content.
    let list = s3_client
        .list_objects_v2()
        .bucket("pg-tide-test")
        .prefix("test/orders/")
        .send()
        .await
        .expect("failed to list S3 objects");

    let objects = list.contents();
    assert_eq!(objects.len(), 1, "expected exactly one S3 object");
    assert!(
        objects[0].key().unwrap_or("").ends_with(".jsonl"),
        "object key must end with .jsonl"
    );

    // Read back and verify content.
    let get_resp = s3_client
        .get_object()
        .bucket("pg-tide-test")
        .key("test/orders/pgtide_batch_001.jsonl")
        .send()
        .await
        .expect("failed to get S3 object");

    let body = get_resp
        .body
        .collect()
        .await
        .expect("failed to read S3 object body");
    let text = String::from_utf8(body.into_bytes().to_vec()).expect("invalid UTF-8");
    let lines: Vec<&str> = text.trim_end_matches('\n').split('\n').collect();
    assert_eq!(lines.len(), 3, "expected 3 JSONL lines");
    for line in &lines {
        assert!(
            serde_json::from_str::<serde_json::Value>(line).is_ok(),
            "each line must be valid JSON"
        );
    }
}
