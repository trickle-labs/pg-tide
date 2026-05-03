# Redis Backend

[Redis Streams](https://redis.io/docs/data-types/streams/) integration for lightweight event streaming with consumer groups.

---

## Forward (Outbox → Redis Stream)

```sql
SELECT tide.relay_set_outbox('events-redis', 'events', 'redis',
  jsonb_build_object(
    'url', 'redis://localhost:6379',
    'stream', 'app:events'
  )
);
```

### Configuration

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `url` | Yes | — | Redis connection URL |
| `stream` | Yes | — | Stream key name |
| `maxlen` | No | — | Max stream length (MAXLEN for trimming) |

---

## Reverse (Redis Stream → Inbox)

```sql
SELECT tide.relay_set_inbox('redis-to-inbox', 'redis-events',
  jsonb_build_object(
    'url', 'redis://localhost:6379',
    'stream', 'external:events',
    'group', 'pg-tide',
    'consumer', 'relay-0'
  ),
  p_source := 'redis'
);
```

### Configuration

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `url` | Yes | — | Redis connection URL |
| `stream` | Yes | — | Stream key to read from |
| `group` | Yes | — | Consumer group name |
| `consumer` | Yes | — | Consumer name within the group |

---

## Cargo Feature

```bash
cargo build --package pg-tide-relay --features "redis"
```
