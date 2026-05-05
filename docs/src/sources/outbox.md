# Source: PostgreSQL Outbox

The PostgreSQL Outbox source is the heart of pg_tide's forward pipeline. It polls the outbox table for new messages and feeds them to whichever sink is configured for the pipeline. Unlike the reverse sources (which consume from external systems), the outbox source reads from your own PostgreSQL database — it is the starting point for every forward pipeline.

## How It Works

The outbox source uses a combination of polling and PostgreSQL NOTIFY to detect new messages efficiently:

1. **Notification-driven wake-up** — When your application publishes a message with `outbox_publish()`, PostgreSQL sends a NOTIFY on the `tide_outbox_notify` channel. The relay, which is LISTENing on this channel, wakes up immediately.
2. **Batch polling** — The relay queries the outbox table for unacknowledged messages belonging to this pipeline's consumer group, fetching up to `batch_size` messages at a time.
3. **Offset tracking** — After the sink confirms delivery, the relay commits the consumer group offset, marking those messages as delivered.
4. **Retention cleanup** — Messages older than the configured `retention_hours` are periodically deleted by background cleanup.

This hybrid approach means messages are typically delivered within milliseconds of being published (notification-driven), while the polling fallback ensures no messages are missed even if a notification is lost.

## Configuration

The outbox source is implicitly configured when you create a forward pipeline with `relay_set_outbox()`:

```sql
SELECT tide.relay_set_outbox(
    'orders-pipeline',      -- pipeline name
    'orders',               -- outbox name (this IS the source)
    'relay-consumer',       -- consumer group name
    '{
        "sink_type": "kafka",
        "brokers": "localhost:9092",
        "topic": "order-events",
        "batch_size": 100,
        "poll_interval_ms": 1000
    }'::jsonb
);
```

### Source-Relevant Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `batch_size` | int | `100` | Maximum messages fetched per poll cycle |
| `poll_interval_ms` | int | `1000` | Fallback polling interval when no NOTIFY is received |
| `visibility_timeout_ms` | int | `30000` | How long a batch is "leased" before it can be re-claimed by another relay instance |

## Consumer Groups

Each forward pipeline uses a consumer group to track its position in the outbox. Multiple pipelines can read from the same outbox independently:

```sql
-- Two pipelines reading the same outbox independently
SELECT tide.relay_set_outbox('to-kafka', 'orders', 'kafka-group', '{"sink_type": "kafka", ...}'::jsonb);
SELECT tide.relay_set_outbox('to-s3', 'orders', 's3-group', '{"sink_type": "object_storage", ...}'::jsonb);
```

Each consumer group maintains its own offset, so Kafka delivery and S3 archival proceed independently without affecting each other.

## Troubleshooting

- **Messages not flowing** — Check that the pipeline is enabled: `SELECT tide.relay_get_config('orders-pipeline')`
- **High consumer lag** — Increase `batch_size` or add more relay instances (with different `relay_group_id`)
- **Messages redelivered after relay restart** — Normal behavior for uncommitted batches; downstream sinks should be idempotent

## Further Reading

- [Consumer Groups Concept](../concepts/consumer-groups.md) — How consumer groups and offsets work
- [HA Coordination](../features/ha-coordination.md) — Multiple relay instances sharing pipelines
- [Scaling](../operations/scaling.md) — Increasing outbox throughput
