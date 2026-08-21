//! Authoritative public-API end-to-end test: native outbox → real relay
//! coordinator → NATS JetStream (v0.40.0, ADR-011, Workstream G).
//!
//! This is the release proof for "One Real Pipeline". It uses:
//!   * a real packaged `pg_tide` extension (`CREATE EXTENSION pg_tide`),
//!   * only public SQL functions (`outbox_create_if_not_exists`,
//!     `relay_set_outbox_v2`, `outbox_publish`),
//!   * the compiled `pg-tide` process,
//!   * NATS with JetStream enabled.
//!
//! No test-only table, direct relay-catalog insert, direct outbox-message
//! insert, or simulated sink publish is used to prove delivery.
//!
//! # Running
//!
//! The extension cannot be installed from an integration test. Point the test
//! at a PostgreSQL 18 server that already has `pg_tide` available (built with
//! `cargo pgrx install`) via the `PG_TIDE_E2E_DATABASE_URL` environment
//! variable, e.g.:
//!
//! ```bash
//! PG_TIDE_E2E_DATABASE_URL='postgres://postgres:postgres@127.0.0.1:5432/postgres' \
//!   cargo test --package pg-tide-relay --test public_api_outbox_to_nats_e2e -- --nocapture
//! ```
//!
//! The tests are ignored by the broad integration suite and must be run with
//! `PG_TIDE_E2E_DATABASE_URL` by the dedicated CI job. NATS is provided by
//! testcontainers with JetStream (`-js`) enabled.

#![allow(clippy::needless_range_loop)]

mod common;

use common::process::RelayProcess;
use std::time::Duration;

use futures_util::StreamExt;
use tokio_postgres::NoTls;

const E2E_ENV: &str = "PG_TIDE_E2E_DATABASE_URL";
const STREAM: &str = "orders";
const SUBJECT: &str = "orders.created";

async fn connect(url: &str) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(url, NoTls)
        .await
        .expect("connect postgres");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

async fn connect_nats_with_retry(url: &str) -> async_nats::Client {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        match async_nats::connect(url).await {
            Ok(client) => return client,
            Err(error) if tokio::time::Instant::now() >= deadline => {
                panic!("connect nats after readiness retries: {error}")
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
}

async fn wait_for_offset(client: &tokio_postgres::Client, minimum: i64) -> i64 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(row) = client
            .query_opt(
                "SELECT last_change_id FROM tide.relay_consumer_offsets \
                 WHERE relay_group_id = 'e2e-a' AND pipeline_id = 'orders-nats' \
                   AND outbox_name = 'orders'",
                &[],
            )
            .await
            .expect("offset query")
        {
            let offset: i64 = row.get(0);
            if offset >= minimum {
                return offset;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("offset did not reach {minimum}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Publish one business event in a single transaction: an application write
/// plus `tide.outbox_publish()`, both committed atomically.
async fn publish_business_event(url: &str, order_id: &str, event_type: &str) {
    let mut client = connect(url).await;
    let tx = client.transaction().await.expect("begin tx");
    tx.execute(
        "SELECT tide.outbox_publish($1, $2::jsonb, $3::jsonb)",
        &[
            &"orders",
            &serde_json::json!({"order_id": order_id, "amount": 100}),
            &serde_json::json!({"event_type": event_type}),
        ],
    )
    .await
    .expect("outbox_publish");
    tx.commit().await.expect("commit");
}

/// Receive up to `n` messages from the JetStream stream within `timeout`.
async fn receive(
    js: &async_nats::jetstream::Context,
    consumer_name: &str,
    n: usize,
    timeout: Duration,
) -> Vec<async_nats::jetstream::Message> {
    let stream = js.get_stream(STREAM).await.expect("get stream");
    let consumer = stream
        .get_or_create_consumer(
            consumer_name,
            async_nats::jetstream::consumer::pull::Config {
                durable_name: Some(consumer_name.to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("create consumer");

    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + timeout;
    while out.len() < n && tokio::time::Instant::now() < deadline {
        let mut batch = match consumer
            .fetch()
            .max_messages(n - out.len())
            .expires(Duration::from_millis(500))
            .messages()
            .await
        {
            Ok(b) => b,
            Err(_) => continue,
        };
        while let Some(Ok(msg)) = batch.next().await {
            let _ = msg.ack().await;
            out.push(msg);
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires an installed pg_tide extension and PG_TIDE_E2E_DATABASE_URL"]
async fn public_api_outbox_to_nats_e2e() {
    let admin_url = std::env::var(E2E_ENV).expect("PG_TIDE_E2E_DATABASE_URL must be set");

    // ── NATS JetStream via testcontainers ────────────────────────────────
    use testcontainers::runners::AsyncRunner;
    use testcontainers::ImageExt;
    let nats = testcontainers::GenericImage::new("nats", "2.11.0")
        .with_exposed_port(testcontainers::core::ContainerPort::Tcp(4222))
        .with_cmd(["-js"])
        .start()
        .await
        .expect("start NATS with JetStream");
    let nats_port = nats.get_host_port_ipv4(4222).await.expect("nats port");
    let nats_url = format!("nats://127.0.0.1:{nats_port}");

    let nats_client = connect_nats_with_retry(&nats_url).await;
    let js = async_nats::jetstream::new(nats_client);
    // Deterministic, generous dedup window so the coordinator-B replay below is
    // deduplicated within the test lifetime.
    js.create_stream(async_nats::jetstream::stream::Config {
        name: STREAM.to_string(),
        subjects: vec!["orders.>".to_string()],
        duplicate_window: Duration::from_secs(120),
        ..Default::default()
    })
    .await
    .expect("create JetStream stream");

    // ── Clean extension state + public setup ─────────────────────────────
    let client = connect(&admin_url).await;
    client
        .batch_execute("DROP EXTENSION IF EXISTS pg_tide CASCADE; CREATE EXTENSION pg_tide;")
        .await
        .expect("CREATE EXTENSION pg_tide");

    client
        .execute(
            "SELECT tide.outbox_create_if_not_exists('orders', 24, 10000, 'none')",
            &[],
        )
        .await
        .expect("outbox_create_if_not_exists");

    let pipeline_cfg = serde_json::json!({
        "name": "orders-nats",
        "outbox": "orders",
        "sink_type": "nats",
        "config": { "url": nats_url, "subject": SUBJECT },
        "batch_size": 50
    });
    client
        .execute(
            "SELECT tide.relay_set_outbox_v2($1::jsonb)",
            &[&pipeline_cfg],
        )
        .await
        .expect("relay_set_outbox_v2");

    // ── Coordinator A ─────────────────────────────────────────────────────
    let coord_a = RelayProcess::start(&admin_url, "e2e-a");
    tokio::time::sleep(Duration::from_millis(1_000)).await;

    // ── Transaction visibility: uncommitted publish is invisible ─────────
    {
        let mut writer = connect(&admin_url).await;
        let tx = writer.transaction().await.expect("begin T1");
        tx.execute(
            "SELECT tide.outbox_publish('orders', $1::jsonb, $2::jsonb)",
            &[
                &serde_json::json!({"order_id": "A-1", "amount": 1}),
                &serde_json::json!({"event_type": "order.created"}),
            ],
        )
        .await
        .expect("publish in T1");

        // Second connection must not see the uncommitted row.
        let count: i64 = client
            .query_one(
                "SELECT COUNT(*)::bigint FROM tide.tide_outbox_messages WHERE outbox_name = 'orders'",
                &[],
            )
            .await
            .expect("visibility count")
            .get(0);
        assert_eq!(
            count, 0,
            "uncommitted outbox row must be invisible to the relay"
        );

        tx.commit().await.expect("commit T1");
    }

    // ── Receive first event from NATS ─────────────────────────────────────
    let msgs = receive(&js, "e2e-consumer", 1, Duration::from_secs(15)).await;
    assert_eq!(
        msgs.len(),
        1,
        "expected the first order event via JetStream"
    );
    let first = &msgs[0];
    let body: serde_json::Value =
        serde_json::from_slice(&first.payload).expect("decode relay message");
    assert_eq!(body["payload"]["order_id"].as_str(), Some("A-1"));
    assert_eq!(body["outbox_name"].as_str(), Some("orders"));
    assert_eq!(
        body["headers"]["event_type"].as_str(),
        Some("order.created"),
        "headers must survive native delivery"
    );
    let source_id = body["outbox_id"].as_i64().expect("outbox_id present");
    let msg_id = first
        .headers
        .as_ref()
        .and_then(|h| h.get("Nats-Msg-Id"))
        .map(|v| v.to_string())
        .expect("Nats-Msg-Id header present");
    assert_eq!(msg_id, format!("outbox_orders:{source_id}:0"));

    // ── Offset keyed by (relay group, pipeline, outbox) ──────────────────
    let offset = wait_for_offset(&client, source_id).await;
    assert!(offset >= source_id, "offset must cover the delivered event");

    // ── Stop A, publish while stopped, restart A ─────────────────────────
    coord_a.stop().await;
    publish_business_event(&admin_url, "A-2", "order.created").await;

    let coord_a2 = RelayProcess::start(&admin_url, "e2e-a");
    tokio::time::sleep(Duration::from_millis(800)).await;

    let msgs2 = receive(&js, "e2e-consumer", 1, Duration::from_secs(15)).await;
    assert!(
        !msgs2.is_empty(),
        "second event must arrive after coordinator restart (no committed event lost)"
    );
    let body2: serde_json::Value =
        serde_json::from_slice(&msgs2[0].payload).expect("decode second message");
    assert_eq!(body2["payload"]["order_id"].as_str(), Some("A-2"));
    let offset2 = wait_for_offset(&client, offset + 1).await;
    assert!(offset2 > offset, "offset must advance for the second event");

    // ── Coordinator B replays from offset 0; JetStream dedup by Nats-Msg-Id ─
    coord_a2.stop().await;
    let coord_b = RelayProcess::start(&admin_url, "e2e-b");
    tokio::time::sleep(Duration::from_millis(1_000)).await;

    // Give B time to re-publish the same rows with identical Nats-Msg-Ids.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let info = js
        .get_stream(STREAM)
        .await
        .expect("get stream")
        .info()
        .await
        .expect("stream info")
        .state
        .messages;
    // The stream deduplicates by the stable Nats-Msg-Id, so replaying the same
    // two rows must not grow the stored message count beyond the two originals.
    assert!(
        info <= 2,
        "JetStream must deduplicate the stable Nats-Msg-Id on replay (stored messages: {info})"
    );

    coord_b.stop().await;
}

/// G.4 negative companion: a pipeline bound to `orders` must ignore rows from
/// another outbox (`payments`), and its offset may cross global-ID gaps left by
/// the interleaved `payments` rows without treating them as missing events.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires an installed pg_tide extension and PG_TIDE_E2E_DATABASE_URL"]
async fn public_api_orders_only_ignores_other_outbox() {
    let admin_url = std::env::var(E2E_ENV).expect("PG_TIDE_E2E_DATABASE_URL must be set");

    use testcontainers::runners::AsyncRunner;
    use testcontainers::ImageExt;
    let nats = testcontainers::GenericImage::new("nats", "2.11.0")
        .with_exposed_port(testcontainers::core::ContainerPort::Tcp(4222))
        .with_cmd(["-js"])
        .start()
        .await
        .expect("start NATS with JetStream");
    let nats_port = nats.get_host_port_ipv4(4222).await.expect("nats port");
    let nats_url = format!("nats://127.0.0.1:{nats_port}");
    let js = async_nats::jetstream::new(connect_nats_with_retry(&nats_url).await);
    js.create_stream(async_nats::jetstream::stream::Config {
        name: STREAM.to_string(),
        subjects: vec!["orders.>".to_string()],
        duplicate_window: Duration::from_secs(120),
        ..Default::default()
    })
    .await
    .expect("create stream");

    let client = connect(&admin_url).await;
    client
        .batch_execute("DROP EXTENSION IF EXISTS pg_tide CASCADE; CREATE EXTENSION pg_tide;")
        .await
        .expect("CREATE EXTENSION");
    client
        .execute(
            "SELECT tide.outbox_create_if_not_exists('orders', 24, 10000, 'none')",
            &[],
        )
        .await
        .expect("create orders");
    client
        .execute(
            "SELECT tide.outbox_create_if_not_exists('payments', 24, 10000, 'none')",
            &[],
        )
        .await
        .expect("create payments");
    let cfg = serde_json::json!({
        "name": "orders-nats",
        "outbox": "orders",
        "sink_type": "nats",
        "config": { "url": nats_url, "subject": SUBJECT },
        "batch_size": 50
    });
    client
        .execute("SELECT tide.relay_set_outbox_v2($1::jsonb)", &[&cfg])
        .await
        .expect("relay_set_outbox_v2");

    let coord = RelayProcess::start(&admin_url, "e2e-gap");
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Interleave publishes to both outboxes; only `orders` has a pipeline.
    for i in 0..3 {
        publish_business_event(&admin_url, &format!("O-{i}"), "order.created").await;
        // payments rows consume global IDs but have no relay pipeline.
        let payments = connect(&admin_url).await;
        payments
            .execute(
                "SELECT tide.outbox_publish('payments', $1::jsonb, '{}'::jsonb)",
                &[&serde_json::json!({"pay": i})],
            )
            .await
            .expect("publish payments");
    }

    let msgs = receive(&js, "gap-consumer", 3, Duration::from_secs(15)).await;
    assert_eq!(msgs.len(), 3, "exactly the three orders events must arrive");
    for m in &msgs {
        let body: serde_json::Value = serde_json::from_slice(&m.payload).expect("decode");
        assert_eq!(
            body["outbox_name"].as_str(),
            Some("orders"),
            "only orders rows may reach the stream"
        );
    }

    // The stream must hold exactly the three orders messages — no payments.
    let stored = js
        .get_stream(STREAM)
        .await
        .expect("stream")
        .info()
        .await
        .expect("info")
        .state
        .messages;
    assert_eq!(stored, 3, "stream must contain only the 3 orders events");

    coord.stop().await;
}
