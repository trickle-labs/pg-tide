# Feature: Schema Registry

The schema registry integration enables Avro serialization with Confluent Schema Registry compatibility. Instead of sending verbose JSON over the wire, messages are serialized as compact Avro binary with schema IDs — reducing message size by 50-80% while providing schema evolution guarantees.

## Why Use a Schema Registry?

- **Compact messages:** Avro binary is significantly smaller than JSON, reducing network bandwidth and storage costs
- **Schema evolution:** Add fields, remove optional fields, or change defaults without breaking consumers
- **Contract enforcement:** Producers can't send data that violates the registered schema
- **Discovery:** Consumers can look up the schema by ID rather than out-of-band documentation

## Configuration

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-pipeline',
    'outbox', 'order_events',
    'sink_type', 'kafka',
    'config', '{
        "brokers": "kafka:9092",
        "topic": "orders",
        "wire_format": "debezium",
        "wire_config": {
            "envelope": "avro",
            "schema_registry_url": "http://schema-registry:8081"
        },
        "schema_registry": {
            "url": "http://schema-registry:8081",
            "username": "${env:SR_USER}",
            "password": "${env:SR_PASS}",
            "auto_register": true
        },
        "serialization": {
            "format": "avro",
            "subject_name_strategy": "TopicName"
        }
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `schema_registry.url` | string | `null` | Schema Registry URL |
| `schema_registry.username` | string | `null` | HTTP Basic Auth username |
| `schema_registry.password` | string | `null` | HTTP Basic Auth password |
| `schema_registry.auto_register` | bool | `true` | Auto-register new schemas |
| `serialization.format` | string | `"json"` | Wire format: `"json"` or `"avro"` |
| `serialization.subject_name_strategy` | string | `"TopicName"` | Subject naming strategy |

## Subject Name Strategies

The subject name strategy determines how schemas are organized in the registry:

| Strategy | Subject Format | Use Case |
|----------|---------------|----------|
| `TopicName` | `{topic}-value` | One schema per topic (most common) |
| `RecordName` | `{record_name}-value` | Schema shared across topics |
| `TopicRecordName` | `{topic}-{record_name}-value` | Multiple record types per topic |

## Confluent Wire Format

Messages serialized with the schema registry follow the Confluent wire format:

```
┌───────┬──────────────┬─────────────────┐
│ Magic │  Schema ID   │   Avro Payload  │
│ 0x00  │  (4 bytes)   │   (N bytes)     │
└───────┴──────────────┴─────────────────┘
```

- **Magic byte (0x00):** Identifies Confluent serialization
- **Schema ID (4 bytes, big-endian):** Registry identifier for this schema
- **Payload:** Avro-encoded binary data

## Schema Evolution

When your outbox table gains new columns, the schema evolves:

1. pg_tide detects the new field in the outbox row
2. A new Avro schema is generated with the additional field
3. If `auto_register` is true, the schema is registered (compatibility checked)
4. Messages are serialized with the new schema ID
5. Consumers using the registry can decode both old and new messages

The registry enforces backward compatibility by default — new schemas must be readable by consumers using the previous schema version.

## Further Reading

- [Wire Format: Debezium](../wire-formats/debezium.md) — Avro support in Debezium format
- [Sinks: Kafka](../sinks/kafka.md) — Common pairing with Schema Registry
