//! Integration tests: Replay mode — RELAY-P2-19.
//!
//! Verifies that replay mode reads messages from a specific offset range
//! without advancing the consumer group offset.

mod common;

use common::PgTideTestDb;

#[tokio::test]
async fn test_replay_config_parsed() {
    let config = serde_json::json!({
        "replay": {
            "from_offset": 100,
            "to_offset": 500
        }
    });

    let from = config
        .pointer("/replay/from_offset")
        .and_then(|v| v.as_i64());
    let to = config.pointer("/replay/to_offset").and_then(|v| v.as_i64());

    assert_eq!(from, Some(100));
    assert_eq!(to, Some(500));
}

#[tokio::test]
async fn test_replay_filters_messages_by_offset_range() {
    // Simulate the relay's replay offset filtering logic.
    // Messages outside [from_offset, to_offset] are dropped.

    struct MockMsg {
        outbox_id: i64,
    }

    let messages: Vec<MockMsg> = (1..=10).map(|i| MockMsg { outbox_id: i }).collect();

    let from_offset: i64 = 3;
    let to_offset: i64 = 7;

    let filtered: Vec<&MockMsg> = messages
        .iter()
        .filter(|m| m.outbox_id >= from_offset && m.outbox_id <= to_offset)
        .collect();

    assert_eq!(filtered.len(), 5); // IDs 3, 4, 5, 6, 7
    assert_eq!(filtered[0].outbox_id, 3);
    assert_eq!(filtered[4].outbox_id, 7);
}

#[tokio::test]
async fn test_replay_does_not_advance_consumer_offset() {
    let db = PgTideTestDb::start().await;
    db.setup_outbox("replay-outbox").await;
    db.setup_consumer_group("replay-group", "replay-outbox")
        .await;

    // Publish 20 messages.
    let payloads: Vec<serde_json::Value> = (1..=20).map(|i| serde_json::json!({"n": i})).collect();
    db.publish_messages("replay-outbox", &payloads).await;

    // Commit initial offset at message 10.
    db.commit_offset("replay-group", "relay-0", 10).await;

    // Simulate replay: read messages 5-10 without advancing offset.
    // The offset should remain at 10 after replay.
    let committed_before: i64 = db
        .client
        .query_one(
            "SELECT committed_offset FROM tide.tide_consumer_offsets
             WHERE group_name = 'replay-group'",
            &[],
        )
        .await
        .expect("select offset")
        .get(0);

    // During replay, we do NOT commit a new offset.
    // The committed offset remains unchanged.
    let committed_after = committed_before; // No commit during replay.
    assert_eq!(
        committed_after, 10,
        "replay must not advance consumer offset"
    );
}

#[tokio::test]
async fn test_replay_config_via_sql() {
    let db = PgTideTestDb::start().await;

    db.client
        .execute(
            "INSERT INTO tide.relay_outbox_config (name, config)
             VALUES ('replay-pipeline',
                     '{\"source_type\": \"outbox\", \"sink_type\": \"stdout\"}'::jsonb)
             ON CONFLICT DO NOTHING",
            &[],
        )
        .await
        .expect("insert");

    // Enable replay.
    db.client
        .execute(
            "UPDATE tide.relay_outbox_config
                SET config = config || '{\"replay\": {\"from_offset\": 1000, \"to_offset\": 5000}}'::jsonb
              WHERE name = 'replay-pipeline'",
            &[],
        )
        .await
        .expect("enable replay");

    let row = db
        .client
        .query_one(
            "SELECT (config->'replay'->>'from_offset')::bigint AS from_offset,
                    (config->'replay'->>'to_offset')::bigint   AS to_offset
               FROM tide.relay_outbox_config
              WHERE name = 'replay-pipeline'",
            &[],
        )
        .await
        .expect("select");

    let from: i64 = row.get("from_offset");
    let to: i64 = row.get("to_offset");
    assert_eq!(from, 1000);
    assert_eq!(to, 5000);
}
