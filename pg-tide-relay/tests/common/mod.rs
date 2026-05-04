//! Shared test harness for pg-tide-relay integration tests.
//!
//! Provides `PgTideTestDb` — a reusable struct that spins up a PostgreSQL
//! container with the pg_tide schema installed, ready for test scenarios.
#![allow(dead_code)]

use std::time::Duration;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::{Client, NoTls};

/// SQL schema for pg_tide — loaded from the extension's migration file.
const SCHEMA_SQL: &str = include_str!("../../../sql/pg_tide--0.1.0.sql");

/// A test database with the pg_tide schema pre-installed.
pub struct PgTideTestDb {
    pub client: Client,
    /// Mapped host port for the PostgreSQL container (use this for second connections).
    pub host_port: u16,
    _container: ContainerAsync<Postgres>,
}

impl PgTideTestDb {
    /// Spin up a fresh PostgreSQL container and install the pg_tide schema.
    pub async fn start() -> Self {
        let container = Postgres::default()
            .start()
            .await
            .expect("failed to start postgres container");

        let host_port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("failed to get postgres port");

        let connection_string = format!(
            "host=127.0.0.1 port={host_port} user=postgres password=postgres dbname=postgres"
        );

        // Retry connection with short backoff (container may not be ready instantly).
        let client = Self::connect_with_retry(&connection_string, 10).await;

        // Create the tide schema and install all catalog tables.
        client
            .batch_execute("CREATE SCHEMA IF NOT EXISTS tide;")
            .await
            .expect("failed to create tide schema");

        client
            .batch_execute(SCHEMA_SQL)
            .await
            .expect("failed to install pg_tide schema");

        Self {
            client,
            host_port,
            _container: container,
        }
    }

    async fn connect_with_retry(url: &str, max_attempts: u32) -> Client {
        let mut attempt = 0;
        loop {
            match tokio_postgres::connect(url, NoTls).await {
                Ok((client, connection)) => {
                    tokio::spawn(async move {
                        if let Err(e) = connection.await {
                            eprintln!("connection error: {e}");
                        }
                    });
                    return client;
                }
                Err(e) => {
                    attempt += 1;
                    if attempt >= max_attempts {
                        panic!("failed to connect to postgres after {max_attempts} attempts: {e}");
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    // ── Helper functions ──────────────────────────────────────────────────

    /// Create an outbox with default settings.
    pub async fn setup_outbox(&self, name: &str) {
        self.client
            .execute(
                "INSERT INTO tide.tide_outbox_config (outbox_name, retention_hours, inline_threshold)
                 VALUES ($1, 24, 10000)
                 ON CONFLICT (outbox_name) DO NOTHING",
                &[&name],
            )
            .await
            .expect("failed to create outbox");
    }

    /// Publish messages to an outbox.
    pub async fn publish_messages(&self, outbox: &str, payloads: &[serde_json::Value]) {
        for payload in payloads {
            self.client
                .execute(
                    "INSERT INTO tide.tide_outbox_messages (outbox_name, payload, headers)
                     VALUES ($1, $2, '{}'::jsonb)",
                    &[&outbox, payload],
                )
                .await
                .expect("failed to publish message");
        }
    }

    /// Create a consumer group.
    pub async fn setup_consumer_group(&self, group: &str, outbox: &str) {
        self.client
            .execute(
                "INSERT INTO tide.tide_consumer_groups (group_name, outbox_name)
                 VALUES ($1, $2)
                 ON CONFLICT (group_name) DO NOTHING",
                &[&group, &outbox],
            )
            .await
            .expect("failed to create consumer group");
    }

    /// Create an inbox with default settings.
    pub async fn setup_inbox(&self, name: &str) {
        self.client
            .execute(
                "INSERT INTO tide.tide_inbox_config (inbox_name, inbox_schema, max_retries)
                 VALUES ($1, 'tide', 3)
                 ON CONFLICT (inbox_name) DO NOTHING",
                &[&name],
            )
            .await
            .expect("failed to create inbox config");

        let create_table = format!(
            r#"CREATE TABLE IF NOT EXISTS tide."{name}_inbox" (
                id             BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                event_id       TEXT        NOT NULL,
                source         TEXT,
                payload        JSONB,
                headers        JSONB,
                received_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
                processed_at   TIMESTAMPTZ,
                retry_count    INT         NOT NULL DEFAULT 0,
                last_error     TEXT,
                CONSTRAINT "uq_{name}_event_id" UNIQUE (event_id)
            )"#
        );
        self.client
            .batch_execute(&create_table)
            .await
            .expect("failed to create inbox table");
    }

    /// Assert that the inbox has received the expected number of messages.
    pub async fn assert_inbox_received(&self, inbox: &str, expected_count: i64) {
        let row = self
            .client
            .query_one(
                &format!(r#"SELECT COUNT(*)::bigint FROM tide."{inbox}_inbox""#),
                &[],
            )
            .await
            .expect("failed to count inbox messages");

        let count: i64 = row.get(0);
        assert_eq!(
            count, expected_count,
            "expected {expected_count} messages in inbox '{inbox}', got {count}"
        );
    }

    /// Get pending message count for an outbox.
    pub async fn pending_count(&self, outbox: &str) -> i64 {
        let row = self
            .client
            .query_one(
                "SELECT COUNT(*)::bigint FROM tide.tide_outbox_messages
                 WHERE outbox_name = $1 AND consumed_at IS NULL",
                &[&outbox],
            )
            .await
            .expect("failed to count pending messages");
        row.get(0)
    }

    /// Commit a consumer offset.
    pub async fn commit_offset(&self, group: &str, consumer: &str, offset: i64) {
        self.client
            .execute(
                "INSERT INTO tide.tide_consumer_offsets (group_name, consumer_id, committed_offset)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (group_name, consumer_id) DO UPDATE
                 SET committed_offset = EXCLUDED.committed_offset,
                     last_heartbeat = now()",
                &[&group, &consumer, &offset],
            )
            .await
            .expect("failed to commit offset");
    }

    /// Deliver a message to an inbox (simulates relay delivery).
    pub async fn deliver_to_inbox(&self, inbox: &str, event_id: &str, payload: &serde_json::Value) {
        self.client
            .execute(
                &format!(
                    r#"INSERT INTO tide."{inbox}_inbox" (event_id, source, payload, headers)
                       VALUES ($1, 'test', $2, '{{}}'::jsonb)
                       ON CONFLICT (event_id) DO NOTHING"#
                ),
                &[&event_id, payload],
            )
            .await
            .expect("failed to deliver to inbox");
    }
}
