# Backends

The pg-tide relay supports multiple messaging backends as both sources (reverse mode) and sinks (forward mode). Each backend is feature-gated — only enabled backends are compiled into the binary.

---

## Available Backends

| Backend | Cargo Feature | Forward (Sink) | Reverse (Source) |
|---------|--------------|:--------------:|:----------------:|
| [NATS](nats.md) | `nats` | ✓ | ✓ |
| [Kafka](kafka.md) | `kafka` | ✓ | ✓ |
| [Redis Streams](redis.md) | `redis` | ✓ | ✓ |
| [RabbitMQ](rabbitmq.md) | `rabbitmq` | ✓ | ✓ |
| [SQS](sqs.md) | `sqs` | ✓ | ✓ |
| [HTTP Webhook](webhook.md) | `webhook` | ✓ | ✓ |
| pg_tide Inbox | `pg-inbox` | ✓ | — |
| stdout | `stdout` | ✓ | — |
| stdin | (always) | — | ✓ |

---

## Default Features

The default build includes: `nats`, `webhook`, `stdout`. To enable additional backends:

```bash
cargo build --package pg-tide-relay --features "kafka,redis,rabbitmq,sqs"
```

Or build with all backends:

```bash
cargo build --package pg-tide-relay --all-features
```

---

## Configuration Pattern

Each backend is configured via the JSONB `config` parameter in `tide.relay_set_outbox()` or `tide.relay_set_inbox()`. The config keys are backend-specific and documented on each backend's page.

```sql
SELECT tide.relay_set_outbox('my-pipeline', 'my-outbox', 'nats',
  jsonb_build_object(
    'url', 'nats://localhost:4222',
    'subject', 'events.{event_type}'
  )
);
```
