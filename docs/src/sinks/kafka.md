# Apache Kafka

The Kafka sink publishes native pg_tide JSON or CloudEvents from the
PostgreSQL outbox. It is outbound only and uses Kafka producer acknowledgments.

```json
{
  "source_type": "outbox",
  "source": {"outbox": "orders"},
  "sink_type": "kafka",
  "sink": {
    "brokers": "kafka:9092",
    "topic": "orders"
  },
  "wire_format": "native"
}
```

Use `topic_template` instead of `topic` when the topic is derived from message
metadata. Configure TLS and SASL through the Kafka client connection settings.
The sink preserves at-least-once delivery; consumers should use the stable
outbox identity for deduplication. Producer idempotence is session-bounded, so
a restart after broker acknowledgment can produce a consumer-visible duplicate.
