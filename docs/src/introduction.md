# pg_tide

**Transactional outbox, idempotent inbox, and relay pipelines for PostgreSQL 18.**

pg_tide gives your PostgreSQL database a built-in messaging backbone. Publish events within your existing transactions — no dual-writes, no distributed transactions, no message brokers required at the database layer.

When you're ready to fan out to Kafka, NATS, Redis Streams, or any other system, the `pg-tide` relay binary bridges the gap — reading from outboxes, delivering to external sinks, and writing back to inboxes with exactly-once semantics.

---

## The Problem pg_tide Solves

Most applications eventually need to publish events to other systems. A customer places an order — and the warehouse needs to know, the analytics pipeline needs to know, the email service needs to send a confirmation. The naive approach is deceptively simple: save the order to your database, then publish an event to your message broker.

But what happens when the broker publish fails after the database commit? Or when your application crashes between the two operations? You get **silent data loss** — the order exists but nobody downstream knows about it. This is the dual-write problem, and it's one of the most common sources of data inconsistency in distributed systems.

pg_tide eliminates this entire class of bugs by implementing the **transactional outbox pattern** as a PostgreSQL extension. Your application writes both the business data and the event in a single database transaction. They succeed or fail together — atomically, consistently, and durably. A separate relay process then delivers committed events to downstream systems, retrying indefinitely until delivery succeeds.

---

## How It Works

```
┌─────────────────────────────────────────────────────────────┐
│                    PostgreSQL 18                              │
│                                                              │
│  Your Application                                            │
│       │                                                      │
│       ├──▶ INSERT INTO orders (...)  ─┐                      │
│       │                               ├── Same transaction   │
│       └──▶ SELECT tide.outbox_publish ─┘                     │
│                        │                                     │
│            tide.tide_outbox_messages                          │
│                        │                                     │
└────────────────────────┼─────────────────────────────────────┘
                         │  pg_notify wakes relay
                         ▼
┌──────────────────────────────────────────────────────────────┐
│                   pg-tide relay binary                         │
│                                                               │
│  Polls outbox ──▶ Delivers to sink ──▶ Commits offset         │
└──────────────────────────────────────────────────────────────┘
                         │
                         ▼
          NATS · Kafka · Redis · RabbitMQ · SQS · Webhooks
```

---

## Key Features

| Feature | What it does |
|---------|-------------|
| **Transactional Outbox** | Publish messages within a database transaction. No 2PC, no dual-writes, no data loss. |
| **Idempotent Inbox** | Exactly-once delivery with automatic deduplication via unique constraints. |
| **Consumer Groups** | Kafka-style offset tracking with heartbeats, visibility leases, and independent progress per consumer. |
| **Relay Binary** | Standalone `pg-tide` process that bridges outboxes/inboxes with external systems. |
| **Multi-Backend** | NATS, Kafka, Redis Streams, RabbitMQ, SQS, HTTP Webhooks — all supported. |
| **Hot Reload** | Pipeline config lives in PostgreSQL. Changes apply without relay restart. |
| **HA Ready** | Advisory lock coordination provides automatic failover across relay instances. |

---

## At a Glance

```sql
-- Create an outbox (one-time setup)
SELECT tide.outbox_create('orders', p_retention_hours := 48);

-- Publish within your business transaction
BEGIN;
  INSERT INTO orders (id, total) VALUES (42, 99.99);
  SELECT tide.outbox_publish('orders',
    '{"order_id": 42, "total": 99.99}'::jsonb,
    '{"event_type": "order.created"}'::jsonb
  );
COMMIT;

-- Configure a relay pipeline
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-nats',
    'outbox', 'orders',
    'sink_type', 'nats',
    'config', '{"url": "nats://localhost:4222", "subject": "orders.events"}'::jsonb
  )
);

-- Start the relay — messages flow automatically
-- pg-tide --postgres-url "postgres://user:pass@localhost:5432/mydb"
```

---

## Who Is This For?

pg_tide is built for:

- **Backend engineers** building applications on PostgreSQL who need reliable event publishing
- **Platform teams** providing messaging infrastructure without the operational burden of a full streaming platform
- **DBAs** who want messaging capabilities that integrate naturally with their existing PostgreSQL operational practices

If PostgreSQL is your source of truth and you need events to flow reliably to other systems, pg_tide is designed for you.

---

## Glossary

Key terms used throughout this documentation:

| Term | Meaning |
|------|---------|
| **Outbox** | A named message stream stored in PostgreSQL. Messages are published to an outbox within a transaction. |
| **Inbox** | A named receiving table with deduplication. External messages are written here with exactly-once semantics. |
| **Relay** | The `pg-tide` binary that bridges outboxes/inboxes with external systems. |
| **Pipeline** | A configured connection between an outbox and a sink (forward) or a source and an inbox (reverse). |
| **Consumer Group** | A named entity that tracks reading progress through an outbox independently. |
| **Offset** | The ID of the last message successfully processed by a consumer group. |
| **Sink** | The destination system in a forward pipeline (e.g., NATS, Kafka). |
| **Source** | The origin system in a reverse pipeline (e.g., NATS subscription, webhook endpoint). |
| **DLQ (Dead-Letter Queue)** | Messages that have exhausted retry attempts. Stored in the inbox for investigation and replay. |
| **Advisory Lock** | A PostgreSQL lock mechanism used to coordinate pipeline ownership across relay instances. |
| **Relay Group ID** | An identifier that namespaces advisory locks, allowing multiple relay deployments to coexist. |
| **Visibility Lease** | A time-limited reservation on a batch of messages, preventing double-processing. |
| **Dedup Key** | A unique identifier (event_id) used by the inbox to detect and discard duplicate deliveries. |
| **Hot Reload** | The relay's ability to pick up pipeline config changes from the database without restart. |

---

## Documentation Guide

This documentation is organized to match your learning journey:

1. **[Evaluate](evaluate/choosing-pg-tide.md)** — decide if pg_tide is right for your use case
2. **[Getting Started](getting-started/installation.md)** — install and build your first pipeline
3. **[Concepts](concepts/message-guarantees.md)** — understand the mechanics in depth
4. **[SQL Reference](sql-reference/outbox-api.md)** — complete API documentation
5. **[Relay Guide](relay-guide/configuration.md)** — configure and operate the relay binary
6. **[Tutorials](tutorials/end-to-end-pipeline.md)** — guided walkthroughs of common patterns
7. **[Operations](operations/deployment-guide.md)** — production deployment and maintenance
8. **[Integrations](integration/cloudnativepg.md)** — platform-specific guidance

---

## License

pg_tide is released under the [Apache-2.0 license](https://github.com/trickle-labs/pg-tide/blob/main/LICENSE).
