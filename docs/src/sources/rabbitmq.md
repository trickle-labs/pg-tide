# Source: RabbitMQ

The RabbitMQ source consumes messages from RabbitMQ queues and delivers them into a pg_tide inbox.

## Configuration

```sql
SELECT tide.relay_set_inbox(
    'events-from-rabbit',
    'incoming_events',
    '{
        "source_type": "rabbitmq",
        "url": "amqp://user:pass@rabbitmq:5672/%2f",
        "queue": "pg-tide-events",
        "prefetch_count": 100,
        "auto_ack": false
    }'::jsonb
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source_type` | string | — | Must be `"rabbitmq"` |
| `url` | string | — | AMQP connection URL |
| `queue` | string | — | Queue to consume from |
| `prefetch_count` | int | `100` | Messages pre-fetched from broker |
| `auto_ack` | bool | `false` | Auto-acknowledge (set false for at-least-once) |
| `tls_enabled` | bool | `false` | Enable TLS |

## Delivery Guarantees

Messages are acknowledged only after successful inbox insertion. The inbox's deduplication uses the RabbitMQ message ID as the event identifier, preventing duplicate processing even on redelivery.

## Further Reading

- [Sinks: RabbitMQ](../sinks/rabbitmq.md) — Publishing to RabbitMQ
