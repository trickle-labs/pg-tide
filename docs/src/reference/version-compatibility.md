# Version Compatibility

This page documents compatibility between pg_tide versions, PostgreSQL versions, and the relay binary.

## PostgreSQL Compatibility

| pg_tide Extension | PostgreSQL Versions | Notes |
|-------------------|--------------------:|-------|
| 0.11.0 | 14, 15, 16, 17, 18 | Current release |
| 0.10.0 | 14, 15, 16, 17, 18 | |
| 0.9.0 | 14, 15, 16, 17 | |
| 0.1.0–0.8.0 | 14, 15, 16 | Initial releases |

## Extension / Relay Compatibility

The pg_tide extension and relay binary are versioned together. Use matching major.minor versions:

| Extension | Relay Binary | Compatible? |
|-----------|-------------|:-----------:|
| 0.11.x | 0.11.x | ✓ |
| 0.11.x | 0.10.x | ✓ (backward compatible) |
| 0.10.x | 0.11.x | ⚠️ May work, not tested |
| 0.10.x | 0.10.x | ✓ |

**Rule of thumb:** The relay binary should be ≥ the extension version. Newer relays are backward compatible with older extensions (they ignore unknown catalog features). Older relays may not understand newer catalog schema additions.

## Upgrade Path

pg_tide supports sequential upgrades. Each version provides a migration script:

```sql
-- Upgrade from 0.10.0 to 0.11.0
ALTER EXTENSION pg_tide UPDATE TO '0.11.0';
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

**Important:** Upgrades must be sequential. You cannot skip versions (e.g., jump from 0.8.0 to 0.11.0 directly). Apply each migration in order.

## Feature Availability by Version

| Feature | Since Version |
|---------|:-------------:|
| Outbox/Inbox core | 0.1.0 |
| Relay catalog | 0.1.0 |
| Consumer groups | 0.3.0 |
| Dead letter queue | 0.5.0 |
| Circuit breaker | 0.5.0 |
| Rate limiting | 0.6.0 |
| JMESPath transforms | 0.7.0 |
| Content-based routing | 0.7.0 |
| Wire formats (Debezium, Maxwell, Canal) | 0.8.0 |
| Schema Registry | 0.8.0 |
| OpenTelemetry | 0.9.0 |
| CDC JSON wire format | 0.10.0 |
| Webhook signatures | 0.10.0 |
| DuckLake sink | 0.11.0 |
| Arrow Flight sink | 0.11.0 |

## Sink/Source Availability

All sinks and sources listed in the documentation are available in the current release (0.11.0). Some are feature-gated at compile time:

| Feature Gate | Includes |
|-------------|----------|
| Default (no flag) | Kafka, NATS, HTTP, stdout, PostgreSQL sinks/sources |
| `cloud` | S3, GCS, Azure Blob, BigQuery, Pub/Sub, SQS, Kinesis, Event Hubs, Service Bus |
| `analytics` | ClickHouse, Snowflake, Iceberg, Delta, DuckLake, Arrow Flight |
| `schema-registry` | Confluent Schema Registry + Avro |
| `otel` | OpenTelemetry tracing |
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
