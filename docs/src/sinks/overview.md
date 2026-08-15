# Sinks Overview

When you publish a message to a pg_tide outbox, that message sits safely in your PostgreSQL database, waiting to be delivered somewhere useful. A **sink** is the destination where the pg_tide relay delivers those messages. Think of it like a postal service: your application drops a letter (message) into a mailbox (outbox), and the postal carrier (relay) delivers it to the recipient's address (sink).

The current sink surface is intentionally narrow and evidence-based. See the [generated connector compatibility matrix](../support/connector-compatibility.md) for maturity, build profiles, tested versions, and evidence before choosing a sink.

## Choosing the Right Sink

Selecting a sink depends on what you are trying to accomplish. The table below groups sinks by category and highlights the primary use case for each. If you are building a new system and have flexibility in choosing your messaging infrastructure, start with the category that matches your architectural goals, then read the detailed page for each sink to understand the trade-offs.

### Message Queues & Streaming

These sinks deliver messages to traditional message brokers and streaming platforms. They are ideal when you need durable, ordered message delivery to multiple consumers, or when you are integrating with an existing event-driven architecture.

| Sink | Best For | Ordering | Delivery Guarantee |
|------|----------|----------|-------------------|
| [Apache Kafka](kafka.md) | High-throughput streaming, event sourcing, CDC pipelines | Per-partition | At-least-once (exactly-once with idempotent producer) |
| [NATS JetStream](nats.md) | Low-latency pub/sub, microservice communication | Per-subject | At-least-once (exactly-once with dedup) |
| [RabbitMQ](rabbitmq.md) | Complex routing, work queues, legacy integration | Per-queue | At-least-once (with publisher confirms) |
| [Redis Streams](redis.md) | Lightweight streaming, real-time dashboards | Per-stream | At-least-once |
| [Amazon SQS](sqs.md) | AWS-native queuing, serverless triggers | FIFO optional | At-least-once (exactly-once with FIFO) |
| [Amazon Kinesis](kinesis.md) | Real-time analytics on AWS, high-volume ingestion | Per-shard | At-least-once |
| [Google Cloud Pub/Sub](pubsub.md) | GCP-native messaging, global distribution | Per-ordering-key | At-least-once |
| [Azure Service Bus](servicebus.md) | Enterprise messaging on Azure, sessions, transactions | Per-session | At-least-once |
| [Azure Event Hubs](eventhubs.md) | High-throughput event ingestion on Azure | Per-partition | At-least-once |
| [MQTT v5](mqtt.md) | IoT device communication, edge computing | Per-topic (QoS dependent) | Configurable (QoS 0/1/2) |

### Analytics & Data Lakes

These sinks write messages directly into analytical databases and data lake storage. They are ideal when your PostgreSQL events need to feed dashboards, machine learning pipelines, or long-term analytical storage without going through an intermediate message broker.

| Sink | Best For | Format | Batch Support |
|------|----------|--------|---------------|
| [ClickHouse](clickhouse.md) | Real-time analytics, time-series, log storage | Native protocol | Yes (batch inserts) |
| [Snowflake](snowflake.md) | Cloud data warehouse, BI reporting | Stage + COPY | Yes (micro-batches) |
| [BigQuery](bigquery.md) | Google Cloud analytics, large-scale queries | Streaming/Load | Yes (streaming inserts) |
| [Apache Iceberg](iceberg.md) | Open table format, lakehouse architecture | Parquet | Yes (append commits) |
| [Delta Lake](delta.md) | Databricks ecosystem, ACID on object storage | Parquet | Yes (append commits) |
| [DuckLake](ducklake.md) | Lightweight lakehouse, PostgreSQL-cataloged Parquet | Parquet | Yes (batch writes) |
| [MongoDB](mongodb.md) | Document storage, flexible schemas | BSON documents | Yes (bulk writes) |
| [Elasticsearch](elasticsearch.md) | Full-text search, log analytics, APM | JSON documents | Yes (bulk API) |
| [Object Storage](object-storage.md) | S3/GCS/Azure Blob archival, data lake landing | JSONL or Parquet | Yes (file-per-batch) |
| [Apache Arrow Flight](arrow-flight.md) | High-performance columnar transfer, ML pipelines | Arrow IPC | Yes (record batches) |

### Notifications & Webhooks

These sinks deliver messages to notification services and HTTP endpoints. They are ideal for alerting, triggering external workflows, and integrating with third-party APIs that expect HTTP callbacks.

| Sink | Best For | Format |
|------|----------|--------|
| [HTTP Webhook](webhook.md) | Third-party API integration, custom endpoints | JSON POST |
| [Slack](slack.md) | Team notifications, operational alerts | Block Kit messages |
| [Discord](discord.md) | Community notifications, bot integrations | Embed messages |
| [PagerDuty](pagerduty.md) | Incident management, on-call alerting | Events API v2 |

### Connector Ecosystems

These sinks integrate with established connector frameworks, giving you access to hundreds of additional destinations through a single configuration. Instead of pg_tide implementing a direct connection to every possible system, these adapters let you leverage existing connector ecosystems.

| Sink | Best For | Ecosystem Size |
|------|----------|---------------|
| [Singer / Meltano](singer.md) | Open-source ETL, Meltano Hub targets | ~500 targets |
| [Airbyte](airbyte.md) | Managed data integration, destination connectors | ~400 connectors |

### Infrastructure

These sinks deliver messages to other PostgreSQL instances or output streams. They are useful for database-to-database messaging, testing, and debugging.

| Sink | Best For |
|------|----------|
| [PostgreSQL Inbox](pg-inbox.md) | Cross-service messaging within PostgreSQL |
| [Remote PostgreSQL Outbox](pg-outbox.md) | Multi-cluster federation |
| [stdout / File](stdout.md) | Debugging, log capture, piping to external tools |

## Common Configuration Patterns

Every sink is configured through the relay pipeline's JSONB configuration stored in PostgreSQL. The basic pattern looks like this:

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'my-pipeline',
    'outbox', 'orders',
    'sink_type', 'kafka',
    'config', '{
        "brokers": "localhost:9092",
        "topic": "order-events"
    }'::jsonb
  )
);
```

The `sink_type` field determines which sink implementation the relay uses. All other fields in the JSON object are sink-specific configuration. Every sink page documents its complete configuration reference.

### Secret Management

Sensitive values like passwords, API keys, and connection strings should never be stored directly in the pipeline configuration. Instead, use secret interpolation:

```json
{
    "sink_type": "kafka",
    "brokers": "${env:KAFKA_BROKERS}",
    "sasl_username": "${env:KAFKA_USERNAME}",
    "sasl_password": "${env:KAFKA_PASSWORD}"
}
```

The relay resolves `${env:VAR_NAME}` tokens from environment variables and `${file:/path/to/secret}` from files at startup. Resolved values are never written to logs or metric labels.

## Delivery Guarantees

All sinks provide **at-least-once delivery** by default. The relay acknowledges messages in the outbox only after the sink confirms receipt. If the relay crashes between delivering a message and acknowledging it, the message will be delivered again on restart.

For sinks that support it, you can achieve **exactly-once semantics** by combining pg_tide's outbox deduplication key with the sink's native deduplication mechanism. Each sink page documents whether exactly-once is possible and how to configure it.

## Error Handling

When a sink cannot accept a message (network failure, authentication error, malformed payload), the relay retries with exponential backoff. If retries are exhausted, the message is routed to the dead-letter queue (DLQ) for manual inspection and replay. The [circuit breaker](../features/circuit-breaker.md) protects against cascading failures by temporarily halting delivery when a sink is consistently unavailable.

## Next Steps

Browse the sink pages that match your use case, or start with the most common choices:

- **New to event streaming?** Start with [NATS JetStream](nats.md) for simplicity
- **Enterprise Kafka shop?** Go to [Apache Kafka](kafka.md)
- **Building a data lake?** See [Apache Iceberg](iceberg.md) or [Object Storage](object-storage.md)
- **Need webhooks?** See [HTTP Webhook](webhook.md)
- **Want maximum connector coverage?** See [Singer / Meltano](singer.md)
