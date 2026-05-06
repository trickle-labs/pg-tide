# Version Compatibility

This page documents compatibility between pg_tide versions, PostgreSQL versions, and the relay binary.

## PostgreSQL Compatibility

| pg_tide Extension | PostgreSQL Versions | Notes |
|-------------------|--------------------:|-------|
| 0.12.0 | 18 | Current release — PostgreSQL 18+ only |
| 0.11.0 | 18 | |
| 0.10.0 | 18 | |
| 0.9.0 | 18 | |
| 0.1.0–0.8.0 | 18 | Initial releases |

> **Note:** pg_tide uses the `pgrx` framework with only the `pg18` feature enabled.
> PostgreSQL 14–17 are not currently supported.

## Extension / Relay Compatibility

The pg_tide extension and relay binary are versioned together. Use matching major.minor versions:

| Extension | Relay Binary | Compatible? |
|-----------|-------------|:-----------:|
| 0.12.x | 0.12.x | ✓ |
| 0.12.x | 0.11.x | ✓ (backward compatible) |
| 0.11.x | 0.12.x | ⚠️ May work, not tested |
| 0.11.x | 0.11.x | ✓ |

**Rule of thumb:** The relay binary should be ≥ the extension version. Newer relays are backward compatible with older extensions (they ignore unknown catalog features). Older relays may not understand newer catalog schema additions.

## Upgrade Path

pg_tide supports sequential upgrades. Each version provides a migration script:

```sql
-- Upgrade from 0.11.0 to 0.12.0
ALTER EXTENSION pg_tide UPDATE TO '0.12.0';
```

Available upgrade scripts:
- `pg_tide--0.1.0--0.2.0.sql`
- `pg_tide--0.2.0--0.3.0.sql`
- `pg_tide--0.3.0--0.4.0.sql`
- `pg_tide--0.4.0--0.5.0.sql`
- `pg_tide--0.5.0--0.6.0.sql`
- `pg_tide--0.6.0--0.7.0.sql`
- `pg_tide--0.7.0--0.8.0.sql`
- `pg_tide--0.8.0--0.9.0.sql`
- `pg_tide--0.9.0--0.10.0.sql`
- `pg_tide--0.10.0--0.11.0.sql`
- `pg_tide--0.11.0--0.12.0.sql`

**Important:** Upgrades must be sequential. You cannot skip versions (e.g., jump from 0.8.0 to 0.12.0 directly). Apply each migration in order.

## Feature Availability by Version

| Feature | Since Version |
|---------|:-------------:|
| Outbox/Inbox core | 0.1.0 |
| Relay catalog | 0.1.0 |
| Consumer groups | 0.3.0 |
| Dead letter queue | 0.7.0 |
| Circuit breaker | 0.7.0 |
| Rate limiting | 0.7.0 |
| JMESPath transforms | 0.7.0 |
| Content-based routing | 0.7.0 |
| Webhook signatures | 0.7.0 |
| OpenTelemetry | 0.7.0 |
| Schema Registry (Avro/Protobuf) | 0.7.0 |
| Notification sinks (Slack, Discord) | 0.8.0 |
| Arrow Flight sink | 0.8.0 |
| Singer protocol adapter | 0.9.0 |
| Airbyte protocol adapter | 0.9.0 |
| Analytics sinks (ClickHouse, Snowflake, BigQuery, Iceberg, Delta, DuckLake, MongoDB) | 0.10.0 |
| Wire formats (Debezium, Maxwell, Canal, CDC JSON) | 0.11.0 |
| Contract correctness, pg-tide doctor, validate-config CLI | 0.12.0 |
| Identifier validation (SQL injection guards) | 0.12.0 |

## Sink/Source Availability

Sinks and sources are feature-gated at compile time. The default build includes
NATS, Kafka, HTTP webhook, stdout/file, and PostgreSQL sinks/sources.
All other sinks require the corresponding feature flag at build time:

| Feature Gate | Includes |
|-------------|----------|
| (default) | NATS, Kafka, HTTP webhook, stdout/file, PostgreSQL (outbox/inbox) |
| `redis` | Redis Streams |
| `sqs` | Amazon SQS |
| `rabbitmq` | RabbitMQ |
| `pubsub` | Google Cloud Pub/Sub |
| `kinesis` | Amazon Kinesis |
| `eventhubs` | Azure Event Hubs |
| `servicebus` | Azure Service Bus |
| `elasticsearch` | Elasticsearch / OpenSearch |
| `mqtt` | MQTT v5 |
| `object-storage` | S3 / GCS / Azure Blob (JSONL + Parquet) |
| `clickhouse` | ClickHouse |
| `mongodb` | MongoDB |
| `snowflake` | Snowflake |
| `bigquery` | Google BigQuery |
| `iceberg` | Apache Iceberg |
| `delta` | Delta Lake |
| `ducklake` | DuckLake |
| `arrow-flight` | Apache Arrow Flight / gRPC |
| `singer` | Singer protocol (Meltano Hub taps/targets) |
| `airbyte` | Airbyte protocol connectors |
| `schema-registry` | Confluent / Apicurio Schema Registry (Avro + Protobuf) |
| `otel` | OpenTelemetry tracing |
| `slack` | Slack notifications |
| `discord` | Discord notifications |
| `pagerduty` | PagerDuty notifications |
| `fivetran` | Fivetran HVR endpoint |

| `singer` | Singer/Meltano tap and target support |
| `airbyte` | Airbyte connector support |

Pre-built release binaries include all feature gates enabled.

## Breaking Changes

| Version | Breaking Change |
|---------|----------------|
| 0.8.0 | Wire format config moved from top-level to `wire_config` sub-object |
| 0.5.0 | DLQ table schema changed (added `error_kind` column) |
| 0.3.0 | Consumer group API changed (renamed functions) |

## Further Reading

- [Architecture Decisions](architecture-decisions.md) — Design rationale
- [CHANGELOG](https://github.com/your-org/pg-tide/blob/main/CHANGELOG.md) — Detailed release notes
