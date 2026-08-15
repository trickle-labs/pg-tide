# Redis Streams

Redis Streams is a log-like data structure built into Redis that combines the simplicity of Redis with the durability of an append-only log. Unlike Redis Pub/Sub (which is fire-and-forget), Streams persist messages and support consumer groups with acknowledgment semantics, making them suitable for reliable event delivery. When you connect pg_tide to Redis Streams, your outbox messages are appended to a Redis stream where multiple consumer groups can independently read and process them at their own pace.

Redis Streams is an excellent choice when you already run Redis in your infrastructure and want lightweight event streaming without deploying a separate message broker. It provides ordering guarantees within a single stream, consumer group support for load balancing, and the sub-millisecond latency that Redis is known for.

## When to Use This Sink

Choose Redis Streams when you need lightweight, fast message delivery and already have Redis in your stack. It is particularly effective for real-time dashboards, caching invalidation, rate-limited work queues, and microservice communication where message volumes are moderate (thousands per second rather than millions). Consider Kafka or NATS for higher throughput requirements or when you need longer retention periods.

## Configuration

### Minimal Configuration

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'events-to-redis',
    'outbox', 'events',
    'sink_type', 'redis',
    'config', '{
        "url": "redis://localhost:6379",
        "stream_key": "events:outbox"
    }'::jsonb
  )
);
```

### Production Configuration

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'events-to-redis',
    'outbox', 'events',
    'sink_type', 'redis',
    'config', '{
        "url": "rediss://${env:REDIS_HOST}:6380",
        "password": "${env:REDIS_PASSWORD}",
        "stream_key": "events:{stream_table}",
        "maxlen": 100000,
        "batch_size": 200,
        "tls_enabled": true
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"redis"` |
| `url` | string | — | Redis connection URL |
| `password` | string | `null` | Redis password (or use URL auth) |
| `stream_key` | string | — | Redis stream key. Supports `{stream_table}`, `{op}` |
| `maxlen` | int | `null` | Maximum stream length (MAXLEN trimming). Older entries are evicted |
| `batch_size` | int | `100` | Messages per pipeline batch |
| `tls_enabled` | bool | `false` | Enable TLS |
| `database` | int | `0` | Redis database number |

## Delivery Guarantees

Redis Streams provides at-least-once delivery when combined with pg_tide's offset tracking. Messages are appended atomically to the stream using XADD, and the relay commits its offset only after Redis confirms the write. Redis Streams' consumer groups provide independent progress tracking for multiple downstream consumers.

## Stream Management

Redis Streams grow indefinitely unless you configure trimming. The `maxlen` parameter caps the stream size:

```json
{"maxlen": 100000}
```

This keeps at most 100,000 entries. Redis uses approximate trimming by default for performance, so the actual size may briefly exceed this limit. For time-based retention, use Redis's built-in XTRIM with MINID in a periodic cleanup job.

## Complete Example

```sql
SELECT tide.outbox_publish(
    'cache_invalidation',
    '{"entity": "product", "id": "prod-42", "action": "updated"}'::jsonb,
    'prod-42-v7'
);
```

Verify with redis-cli:
```bash
redis-cli XRANGE events:cache_invalidation - + COUNT 5
```

## Troubleshooting

- **"Connection refused"** — Verify Redis is running and accessible on the specified port
- **"NOAUTH Authentication required"** — Set the `password` parameter
- **"OOM command not allowed"** — Redis is out of memory; configure `maxlen` or increase Redis memory
- **High memory usage** — Streams without `maxlen` grow indefinitely; configure trimming

## Further Reading

- [Sources: Redis](../sources/redis.md) — Consuming from Redis Streams into pg_tide inbox
- [Rate Limiting](../features/rate-limiting.md) — Controlling message flow to Redis
