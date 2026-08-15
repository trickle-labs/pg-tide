# Source: Apache Kafka

The Kafka source consumes messages from Kafka topics and delivers them into a pg_tide inbox. This enables reverse pipelines where events produced by other systems (via Kafka) flow reliably into your PostgreSQL database for processing. The relay acts as a Kafka consumer, managing offsets, partition assignment, and rebalancing automatically.

## When to Use This Source

Use the Kafka source when other services produce events to Kafka that your PostgreSQL-based application needs to process, when you want to consume CDC events from Debezium (which publishes to Kafka), or when you want to build a reliable event consumer that processes Kafka messages within database transactions.

## Configuration

```sql
SELECT tide.relay_set_inbox_v2(
  jsonb_build_object(
    'name', 'payments-from-kafka',
    'inbox', 'payment_events',
    'source', 'kafka',
    'config', '{
        "brokers": "${env:KAFKA_BROKERS}",
        "topic": "payment-events",
        "group_id": "pg-tide-payments",
        "auto_offset_reset": "earliest",
        "sasl_mechanism": "SCRAM-SHA-256",
        "sasl_username": "${env:KAFKA_USER}",
        "sasl_password": "${env:KAFKA_PASS}",
        "tls_enabled": true
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source_type` | string | — | Must be `"kafka"` |
| `brokers` | string | — | Kafka broker addresses |
| `topic` | string | — | Topic to consume from |
| `group_id` | string | — | Kafka consumer group ID |
| `auto_offset_reset` | string | `"earliest"` | Where to start: `"earliest"` or `"latest"` |
| `sasl_mechanism` | string | `null` | Auth mechanism |
| `sasl_username` | string | `null` | SASL username |
| `sasl_password` | string | `null` | SASL password |
| `tls_enabled` | bool | `false` | Enable TLS |
| `batch_size` | int | `100` | Messages per inbox insert batch |

## Offset Management

The relay commits Kafka consumer offsets only after messages are successfully written to the inbox. This ensures no messages are lost even if the relay crashes. On restart, Kafka redelivers any uncommitted messages, and the inbox's deduplication prevents duplicates.

## Wire Format Integration

When consuming Debezium-formatted messages from Kafka, specify the wire format:

```json
{
    "source_type": "kafka",
    "brokers": "localhost:9092",
    "topic": "dbserver1.public.orders",
    "group_id": "pg-tide-cdc",
    "wire_format": "debezium"
}
```

This decodes Debezium envelope messages and maps them to inbox rows with proper operation type (insert/update/delete), old and new payload, and commit timestamp.

## Troubleshooting

- **"Group coordinator not available"** — Brokers are unreachable or the cluster is starting up
- **"Topic not found"** — Create the topic or check the name spelling
- **Consumer lag growing** — Increase `batch_size` or check if inbox inserts are slow

## Further Reading

- [Sinks: Kafka](../sinks/kafka.md) — Publishing to Kafka (forward direction)
- [Debezium Wire Format](../wire-formats/debezium.md) — Consuming CDC events from Debezium
