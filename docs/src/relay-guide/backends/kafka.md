# Kafka Backend

[Apache Kafka](https://kafka.apache.org) integration for high-throughput event streaming.

---

## Forward (Outbox → Kafka)

```sql
SELECT tide.relay_set_outbox('events-kafka', 'events', 'kafka',
  jsonb_build_object(
    'brokers', 'localhost:9092',
    'topic', 'app-events'
  )
);
```

### Configuration

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `brokers` | Yes | — | Comma-separated broker list |
| `topic` | Yes | — | Target topic |
| `key` | No | — | Message key template (for partitioning) |
| `acks` | No | `all` | Acknowledgment level: `0`, `1`, `all` |
| `compression` | No | `none` | `none`, `gzip`, `snappy`, `lz4`, `zstd` |

---

## Reverse (Kafka → Inbox)

```sql
SELECT tide.relay_set_inbox('kafka-to-inbox', 'kafka-events',
  jsonb_build_object(
    'brokers', 'localhost:9092',
    'topic', 'external-events',
    'group_id', 'pg-tide-consumer'
  ),
  p_source := 'kafka'
);
```

### Configuration

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `brokers` | Yes | — | Comma-separated broker list |
| `topic` | Yes | — | Topic to consume from |
| `group_id` | Yes | — | Kafka consumer group ID |
| `auto_offset_reset` | No | `earliest` | `earliest` or `latest` |

---

## Cargo Feature

```bash
cargo build --package pg-tide-relay --features "kafka"
```

Requires `librdkafka` or the bundled `cmake` build (via `rdkafka/cmake-build`).
