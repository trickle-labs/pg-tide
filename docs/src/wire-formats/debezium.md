# Wire Format: Debezium

The Debezium wire format produces and consumes messages in the same shape as [Debezium](https://debezium.io), the popular open-source CDC platform. This means pg_tide can be a drop-in replacement for Debezium in existing architectures — your Kafka consumers, ksqlDB queries, Flink jobs, and stream processors continue working without modification.

## Encoded Format (Outbox → Sink)

For an INSERT operation:

```json
{
  "schema": { ... },
  "payload": {
    "before": null,
    "after": {
      "order_id": "ORD-001",
      "status": "confirmed",
      "total": 99.95
    },
    "op": "c",
    "ts_ms": 1714029482000,
    "source": {
      "version": "pg-tide",
      "connector": "postgresql",
      "name": "production",
      "ts_ms": 1714029482000,
      "db": "mydb",
      "schema": "public",
      "table": "orders",
      "lsn": 12345678
    }
  }
}
```

For a DELETE operation with tombstones enabled, two messages are produced:
1. The delete event (with `before` populated and `after` as null)
2. A tombstone message (null value with the same key) for log compaction

### Operation Mapping

| pg_tide op | Debezium op | Notes |
|-----------|-------------|-------|
| `insert` | `c` (create) | `before` = null, `after` = new row |
| `update` | `u` (update) | `before` = old row, `after` = new row |
| `delete` | `d` (delete) | `before` = old row, `after` = null |

## Decoded Format (Source → Inbox)

When consuming Debezium messages (e.g., from an actual Debezium deployment feeding Kafka), pg_tide extracts:

| Field | Source |
|-------|--------|
| `event_id` | From message key or generated UUID |
| `event_type` | `{source.db}.{source.table}` |
| `op` | Mapped from `payload.op`: c→insert, u→update, d→delete, r→insert/upsert |
| `payload` | From `payload.after` (or `payload.before` for deletes) |
| `old_payload` | From `payload.before` (for updates) |
| `commit_ts` | From `payload.source.ts_ms` |
| `source_position` | From `payload.source.lsn` |

Snapshot reads (`op: "r"`) are treated as inserts by default, configurable via `snapshot_op_treatment`.

## Configuration

```toml
[[pipelines]]
name = "orders-to-kafka"
wire_format = "debezium"

[pipelines.wire_config]
server_name = "production"
emit_tombstones = true
envelope = "json"
tombstone_handling = "delete"
key_strategy = "primary_key"
snapshot_op_treatment = "insert"
heartbeat_interval_ms = 10000
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `server_name` | string | `"pg-tide"` | Logical name in `source.name` field |
| `envelope` | string | `"json"` | Envelope type: `"json"` or `"avro"` |
| `emit_tombstones` | bool | `true` | Emit null-value tombstone after DELETE |
| `tombstone_handling` | string | `"delete"` | How to handle incoming tombstones: `"delete"` or `"drop"` |
| `key_strategy` | string | `"primary_key"` | Message key: `"primary_key"` or `"message_key"` |
| `snapshot_op_treatment` | string | `"insert"` | Treat `op=r` as: `"insert"` or `"upsert"` |
| `heartbeat_interval_ms` | int | `10000` | Heartbeat emission interval (0 = disabled) |
| `schema_registry_url` | string | `null` | Confluent Schema Registry URL (for Avro) |

## Avro Support

When `envelope` is set to `"avro"`, messages are serialized using Apache Avro with schemas registered in a Confluent-compatible Schema Registry. This provides:

- Compact binary encoding (smaller messages)
- Schema evolution with compatibility checks
- Integration with the Confluent ecosystem

```toml
[pipelines.wire_config]
envelope = "avro"
schema_registry_url = "http://schema-registry:8081"
```

## Schema Evolution

The Debezium format tracks schema changes. When a new column appears in the outbox, it's added to the Avro schema or JSON structure automatically. Incompatible changes (column removal) are detected and reported.

## Tombstones and Log Compaction

Kafka topic compaction uses null-value messages (tombstones) to signal that a key should be removed. When `emit_tombstones` is true, a DELETE operation produces two messages:

1. **Delete event** — Contains the old row state in `before`
2. **Tombstone** — Null value with the same key, enabling compaction to remove the key

This is essential for Kafka topics configured with `cleanup.policy=compact`.

## Migrating from Debezium to pg_tide

If you're currently using Debezium and want to switch to pg_tide:

1. Configure pg_tide with `wire_format = "debezium"` and matching `server_name`
2. Consumers see identical message shapes — no changes needed
3. The `source.version` field changes to `"pg-tide"` but this rarely matters
4. You lose Debezium-specific features (schema history topic) but gain transactional outbox guarantees

## Further Reading

- [Wire Formats Overview](overview.md) — Comparison of all formats
- [Schema Registry Feature](../features/schema-registry.md) — Avro schema management
- [Sinks: Kafka](../sinks/kafka.md) — Common pairing with Debezium format
