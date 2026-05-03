# RabbitMQ Backend

[RabbitMQ](https://www.rabbitmq.com) integration via AMQP 0-9-1.

---

## Forward (Outbox → RabbitMQ)

```sql
SELECT tide.relay_set_outbox('events-rabbit', 'events', 'rabbitmq',
  jsonb_build_object(
    'url', 'amqp://guest:guest@localhost:5672',
    'exchange', 'app.events',
    'routing_key', 'orders.created'
  )
);
```

### Configuration

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `url` | Yes | — | AMQP connection URL |
| `exchange` | Yes | — | Exchange to publish to |
| `routing_key` | No | `""` | Routing key (supports templates) |
| `exchange_type` | No | `topic` | `direct`, `topic`, `fanout`, `headers` |
| `durable` | No | `true` | Whether messages are persistent |

---

## Reverse (RabbitMQ → Inbox)

```sql
SELECT tide.relay_set_inbox('rabbit-to-inbox', 'amqp-events',
  jsonb_build_object(
    'url', 'amqp://guest:guest@localhost:5672',
    'queue', 'incoming-events'
  ),
  p_source := 'rabbitmq'
);
```

### Configuration

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `url` | Yes | — | AMQP connection URL |
| `queue` | Yes | — | Queue to consume from |
| `prefetch` | No | `10` | Prefetch count |

---

## Cargo Feature

```bash
cargo build --package pg-tide-relay --features "rabbitmq"
```
