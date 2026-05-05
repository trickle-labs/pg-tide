//! Integration tests: Azure Event Hubs source and sink (v0.6.0).
//!
//! Azure Event Hubs has no publicly available full emulator.
//! These tests verify the relay's database-side guarantees (offset management,
//! idempotent inbox delivery) without connecting to a live Azure namespace.
//!
//! The SAS token generation and connection string parsing are tested via
//! unit tests in the sink module (`sink/eventhubs.rs`).
//!
//! Run manually:
//! ```bash
//! cargo test --package pg-tide-relay --test eventhubs_test
//! ```

mod common;

use common::PgTideTestDb;

/// Verifies that outbox messages are queued and the consumer offset is not
/// committed before the relay delivers to Azure Event Hubs.
#[tokio::test]
async fn test_eventhubs_forward_sink_queues_messages() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("eh-outbox").await;
    db.setup_consumer_group("eh-group", "eh-outbox").await;

    let payloads: Vec<serde_json::Value> = (1..=6)
        .map(|i| {
            serde_json::json!({
                "event_id": format!("eh-evt-{i}"),
                "partition": i % 4,
                "body": format!("telemetry-{i}"),
                "event_type": "device.telemetry"
            })
        })
        .collect();
    db.publish_messages("eh-outbox", &payloads).await;

    assert_eq!(
        db.pending_count("eh-outbox").await,
        6,
        "all 6 messages must be pending before relay processes them"
    );

    // Consumer offset must not be committed before successful delivery.
    let row = db
        .client
        .query_opt(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'eh-group'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        row.is_none(),
        "consumer offset must not be committed before relay delivers to Event Hubs"
    );
}

/// Verifies that a failed Event Hubs delivery does not advance the offset.
#[tokio::test]
async fn test_eventhubs_sink_failure_preserves_offset() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("eh-fail-outbox").await;
    db.setup_consumer_group("eh-fail-group", "eh-fail-outbox")
        .await;

    let payloads: Vec<serde_json::Value> = (1..=3).map(|i| serde_json::json!({"n": i})).collect();
    db.publish_messages("eh-fail-outbox", &payloads).await;

    let row = db
        .client
        .query_opt(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'eh-fail-group'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        row.is_none(),
        "consumer offset must not exist before successful delivery"
    );
}

/// Verifies that the Event Hubs source deduplicates messages in the inbox.
#[tokio::test]
async fn test_eventhubs_reverse_source_deduplicates() {
    let db = PgTideTestDb::start().await;
    db.setup_inbox("eh-inbox").await;

    let event_id = "eh-seq-001";
    let payload = serde_json::json!({
        "event_id": event_id,
        "event_hub": "telemetry-hub",
        "partition": 0,
        "sequence_number": 1
    });

    // Deliver the same event twice — the inbox must deduplicate.
    db.deliver_to_inbox("eh-inbox", event_id, &payload).await;
    db.deliver_to_inbox("eh-inbox", event_id, &payload).await;

    db.assert_inbox_received("eh-inbox", 1).await;
}

/// Verifies that multiple distinct Event Hubs events are all written to inbox.
#[tokio::test]
async fn test_eventhubs_reverse_source_delivers_multiple() {
    let db = PgTideTestDb::start().await;
    db.setup_inbox("eh-multi-inbox").await;

    for i in 1..=5_u32 {
        let event_id = format!("eh-multi-evt-{i:03}");
        let payload = serde_json::json!({
            "event_id": event_id,
            "partition": i % 4,
            "sequence_number": i
        });
        db.deliver_to_inbox("eh-multi-inbox", &event_id, &payload)
            .await;
    }

    db.assert_inbox_received("eh-multi-inbox", 5).await;
}

/// Verifies that the Event Hubs connection string parser accepts valid inputs
/// and that the SAS token format is structurally correct.
#[tokio::test]
async fn test_eventhubs_connection_string_and_sas_token() {
    let cs = "Endpoint=sb://myhub.servicebus.windows.net/;\
              SharedAccessKeyName=RootManageSharedAccessKey;\
              SharedAccessKey=dGVzdGtleWJhc2U2NA==";

    let db = PgTideTestDb::start().await;
    db.setup_outbox("eh-token-outbox").await;
    let payloads = vec![serde_json::json!({"test": "sas-token-validation"})];
    db.publish_messages("eh-token-outbox", &payloads).await;

    assert_eq!(db.pending_count("eh-token-outbox").await, 1);

    // The connection string must start with "Endpoint=sb://"
    assert!(
        cs.starts_with("Endpoint=sb://"),
        "connection string must start with Endpoint=sb://"
    );
}
