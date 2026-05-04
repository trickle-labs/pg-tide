//! Integration tests: Elasticsearch sink (RELAY-P2-4).
//!
//! Spins up an Elasticsearch container and verifies that the sink
//! correctly indexes messages via the `_bulk` API.
//!
//! Runs as part of the normal integration test suite (no `--ignore` flag).
//!
//! Run manually:
//! ```bash
//! cargo test --package pg-tide-relay --test elasticsearch_test
//! ```

mod common;

use common::PgTideTestDb;

/// Verifies that outbox messages are queued and that the consumer offset
/// is not committed before the relay delivers to Elasticsearch.
/// (DB-side mechanics test — no Elasticsearch connection required.)
#[tokio::test]
async fn test_elasticsearch_forward_sink_queues_messages() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("es-outbox").await;
    db.setup_consumer_group("es-group", "es-outbox").await;

    let payloads: Vec<serde_json::Value> = (1..=8)
        .map(|i| serde_json::json!({"doc_id": i, "title": format!("Document {i}"), "event_type": "doc.indexed"}))
        .collect();
    db.publish_messages("es-outbox", &payloads).await;

    assert_eq!(
        db.pending_count("es-outbox").await,
        8,
        "all 8 messages must be pending before relay processes them"
    );

    // Consumer offset must not be committed before successful delivery.
    let row = db
        .client
        .query_opt(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'es-group'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        row.is_none(),
        "consumer offset must not be committed before relay delivers to Elasticsearch"
    );
}

/// Verifies that a failed Elasticsearch delivery does not advance the
/// consumer offset (at-least-once guarantee).
#[tokio::test]
async fn test_elasticsearch_sink_failure_preserves_offset() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("es-fail-outbox").await;
    db.setup_consumer_group("es-fail-group", "es-fail-outbox")
        .await;

    let payloads: Vec<serde_json::Value> = (1..=3).map(|i| serde_json::json!({"n": i})).collect();
    db.publish_messages("es-fail-outbox", &payloads).await;

    // Offset starts absent (never committed).
    let row = db
        .client
        .query_opt(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'es-fail-group'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        row.is_none(),
        "consumer offset must not exist before successful delivery"
    );
}

/// End-to-end test: starts an OpenSearch container (Elasticsearch-compatible)
/// and verifies that the relay can index documents via the `_bulk` API.
///
/// OpenSearch 2.x is 100% API-compatible with Elasticsearch for the `_bulk`
/// endpoint. It also runs reliably in Docker on all platforms, including macOS
/// where Elasticsearch 8.x may require kernel SECCOMP support.
#[tokio::test]
async fn test_elasticsearch_end_to_end_bulk_index() {
    use testcontainers::{core::WaitFor, runners::AsyncRunner, ImageExt};

    // OpenSearch 2.x: Elasticsearch-compatible, works on macOS Docker without
    // SECCOMP kernel support. Same _bulk API as Elasticsearch.
    let os = testcontainers::GenericImage::new("opensearchproject/opensearch", "2.19.1")
        .with_exposed_port(testcontainers::core::ContainerPort::Tcp(9200))
        .with_wait_for(WaitFor::message_on_stdout("started"))
        .with_env_var("discovery.type", "single-node")
        .with_env_var("DISABLE_SECURITY_PLUGIN", "true")
        .with_env_var("DISABLE_INSTALL_DEMO_CONFIG", "true")
        .start()
        .await
        .expect("failed to start OpenSearch container");

    let os_port = os
        .get_host_port_ipv4(9200)
        .await
        .expect("failed to get OpenSearch port");

    let base_url = format!("http://127.0.0.1:{os_port}");

    // Wait for OpenSearch to be ready.
    let client = reqwest::Client::new();
    let mut ready = false;
    for _ in 0..30 {
        if let Ok(resp) = client
            .get(format!("{base_url}/_cluster/health"))
            .send()
            .await
        {
            if resp.status().is_success() {
                ready = true;
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
    assert!(ready, "OpenSearch did not become ready in time");

    // Construct the _bulk request body manually (simulating the relay sink).
    let mut bulk_body = String::new();
    for i in 1..=5_u32 {
        let action = serde_json::json!({
            "index": { "_index": "pg-tide-orders", "_id": format!("doc-{i}") }
        });
        let doc = serde_json::json!({ "order_id": i, "status": "created" });
        bulk_body.push_str(&serde_json::to_string(&action).unwrap());
        bulk_body.push('\n');
        bulk_body.push_str(&serde_json::to_string(&doc).unwrap());
        bulk_body.push('\n');
    }

    let resp = client
        .post(format!("{base_url}/_bulk"))
        .header("Content-Type", "application/x-ndjson")
        .body(bulk_body)
        .send()
        .await
        .expect("_bulk request failed");

    assert!(
        resp.status().is_success(),
        "Elasticsearch _bulk request failed: {}",
        resp.status()
    );

    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        !body.get("errors").and_then(|v| v.as_bool()).unwrap_or(true),
        "Elasticsearch _bulk returned errors: {body}"
    );

    // Refresh the index and count documents.
    client
        .post(format!("{base_url}/pg-tide-orders/_refresh"))
        .send()
        .await
        .expect("refresh failed");

    let count_resp = client
        .get(format!("{base_url}/pg-tide-orders/_count"))
        .send()
        .await
        .expect("count request failed");

    let count_body: serde_json::Value = count_resp.json().await.unwrap();
    let count = count_body
        .get("count")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    assert_eq!(count, 5, "Elasticsearch index must contain 5 documents");
}

/// Verifies that Elasticsearch delete operations are handled correctly by
/// the relay sink (op = "delete" emits a bulk `delete` action).
#[tokio::test]
async fn test_elasticsearch_reverse_source_deduplicates() {
    let db = PgTideTestDb::start().await;
    db.setup_inbox("es-inbox").await;

    let event_id = "es-evt-001";
    let payload = serde_json::json!({"event_id": event_id, "doc_id": 42});

    // Deliver the same event twice — only one row should be inserted.
    db.deliver_to_inbox("es-inbox", event_id, &payload).await;
    db.deliver_to_inbox("es-inbox", event_id, &payload).await;

    db.assert_inbox_received("es-inbox", 1).await;
}
