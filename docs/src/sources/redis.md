# Source: Redis Streams

The Redis source consumes messages from Redis Streams using consumer groups (XREADGROUP) and delivers them into a pg_tide inbox.

## Configuration

```sql
SELECT tide.relay_set_inbox_v2(
  jsonb_build_object(
    'name', 'events-from-redis',
    'inbox', 'incoming_events',
    'source', 'redis',
    'config', '{
        "url": "redis://localhost:6379",
        "stream_key": "events:orders",
        "group_name": "pg-tide",
        "consumer_name": "relay-01",
        "batch_size": 100
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source_type` | string | — | Must be `"redis"` |
| `url` | string | — | Redis connection URL |
| `stream_key` | string | — | Redis stream key to consume from |
| `group_name` | string | — | Consumer group name |
| `consumer_name` | string | — | Consumer name within the group |
| `batch_size` | int | `100` | Messages per XREADGROUP call |
| `password` | string | `null` | Redis password |

## Delivery Guarantees

Messages are acknowledged (XACK) only after inbox insertion. Unacknowledged messages remain in the pending entries list (PEL) and are reclaimed on relay restart, ensuring no message loss.

## Further Reading

- [Sinks: Redis](../sinks/redis.md) — Publishing to Redis Streams
