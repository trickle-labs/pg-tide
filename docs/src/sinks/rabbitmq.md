# RabbitMQ

RabbitMQ is one of the most widely deployed open-source message brokers, trusted by tens of thousands of organizations for reliable message delivery. Built on the AMQP 0-9-1 protocol, RabbitMQ provides sophisticated routing capabilities through its exchange-and-queue model, making it particularly well-suited for scenarios where messages need to be routed to different consumers based on content, headers, or routing patterns. When you connect pg_tide to RabbitMQ, your outbox messages are published to exchanges where RabbitMQ's routing rules determine which queues (and ultimately which consumers) receive each message.

## When to Use This Sink

Choose RabbitMQ when you need complex message routing patterns (topic exchanges, header-based routing, priority queues), when you are integrating with existing RabbitMQ infrastructure, or when you need per-message acknowledgment with sophisticated dead-letter handling built into the broker itself. RabbitMQ's management UI and mature tooling ecosystem also make it an excellent choice for teams that value operational visibility.

## Configuration

### Minimal Configuration

```sql
SELECT tide.relay_set_outbox(
    'orders-to-rabbit',
    'orders',
    'rabbit-relay',
    '{
        "sink_type": "rabbitmq",
        "url": "amqp://localhost:5672",
        "exchange": "events",
        "routing_key": "orders.created"
    }'::jsonb
);
```

### Production Configuration

```sql
SELECT tide.relay_set_outbox(
    'orders-to-rabbit',
    'orders',
    'rabbit-relay',
    '{
        "sink_type": "rabbitmq",
        "url": "amqps://${env:RABBITMQ_USER}:${env:RABBITMQ_PASS}@${env:RABBITMQ_HOST}:5671/%2f",
        "exchange": "events",
        "exchange_type": "topic",
        "routing_key": "{stream_table}.{op}",
        "publisher_confirms": true,
        "mandatory": true,
        "tls_enabled": true,
        "batch_size": 100
    }'::jsonb
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"rabbitmq"` |
| `url` | string | — | AMQP connection URL |
| `exchange` | string | — | Target exchange name |
| `exchange_type` | string | `"topic"` | Exchange type: `"direct"`, `"topic"`, `"fanout"`, `"headers"` |
| `routing_key` | string | `""` | Routing key template. Supports `{stream_table}`, `{op}` |
| `publisher_confirms` | bool | `true` | Wait for broker acknowledgment |
| `mandatory` | bool | `false` | Return unroutable messages as errors |
| `tls_enabled` | bool | `false` | Enable TLS |
| `batch_size` | int | `100` | Messages per batch |
| `headers` | object | `null` | Additional AMQP message headers |

## Delivery Guarantees

With `publisher_confirms: true` (the default), RabbitMQ acknowledges each message after it has been written to disk and (if mirrored) replicated to mirror nodes. This provides at-least-once delivery. Combined with RabbitMQ's built-in deduplication plugin or consumer-side idempotency, you can achieve effectively exactly-once processing.

## Routing Patterns

RabbitMQ's routing model is more flexible than simple topic-based systems. The `routing_key` template combined with exchange types enables powerful patterns:

- **Direct exchange:** Messages with routing key `orders.insert` go only to queues bound with that exact key.
- **Topic exchange:** Messages with routing key `orders.insert` match queues bound to `orders.*`, `orders.#`, or `*.insert`.
- **Fanout exchange:** All messages go to all bound queues regardless of routing key (broadcast).
- **Headers exchange:** Route based on message headers rather than routing key.

## Complete Example

```sql
-- Publish an order event
SELECT tide.outbox_publish(
    'orders',
    '{"order_id": "ord-99", "status": "paid", "amount": 149.99}'::jsonb,
    'ord-99-paid'
);
```

Verify with `rabbitmqadmin`:
```bash
rabbitmqadmin get queue=order-processing count=1
```

## Troubleshooting

- **"Connection refused"** — Check RabbitMQ is running and the port is correct (5672 for AMQP, 5671 for AMQPS)
- **"Access refused"** — Verify username/password and that the user has publish permission on the exchange
- **"Exchange not found"** — Create the exchange first or set `exchange_declare: true` if supported
- **Messages not arriving in queue** — Check queue bindings match the routing key pattern

## Further Reading

- [Sources: RabbitMQ](../sources/rabbitmq.md) — Consuming from RabbitMQ into pg_tide inbox
- [Content-Based Routing](../features/routing.md) — Dynamic routing key templates
