# pg_tide

**Transactional outbox, idempotent inbox, and relay pipelines for PostgreSQL 18+.**

pg_tide gives your PostgreSQL database a built-in messaging backbone. Publish events within your existing transactions — no dual-writes, no distributed transactions, no message brokers required at the database layer.

When you're ready to fan out to Kafka, NATS, Redis Streams, or any other system, the `pg-tide` relay binary bridges the gap — reading from outboxes, delivering to external sinks, and writing back to inboxes with exactly-once semantics.

---

## Why pg_tide?

Most applications eventually need to publish events to other systems. The naive approach — writing to a database and a message broker in the same request — is inherently unreliable. Network failures, crashes, and timeouts create silent data loss or duplicates.

The **transactional outbox pattern** solves this by treating message publishing as a database write. Your application inserts business data and the outbox message in a single transaction. A separate relay process picks up committed messages and delivers them downstream.

pg_tide implements this pattern as a PostgreSQL extension with zero application-side dependencies:

- **One `SELECT` to publish** — no SDK, no client library, just SQL
- **Exactly-once delivery** via the idempotent inbox and dedup keys
- **Consumer groups** with offset tracking, heartbeats, and visibility leases
- **Hot-reloadable relay** that reads pipeline config from PostgreSQL itself

---

## At a Glance

```sql
-- Create an outbox (one-time setup)
SELECT tide.outbox_create('orders', retention_hours := 48);

-- Publish within your business transaction
BEGIN;
  INSERT INTO orders (id, total) VALUES (42, 99.99);
  SELECT tide.outbox_publish('orders',
    '{"order_id": 42, "total": 99.99}'::jsonb,
    '{"event_type": "order.created"}'::jsonb
  );
COMMIT;

-- Messages flow automatically to downstream systems via the relay.
```

---

## Components

| Component | What it does |
|-----------|-------------|
| **pg_tide extension** | SQL functions + catalog tables for outbox, inbox, consumer groups, and relay config |
| **pg-tide relay** | Standalone binary that bridges outboxes/inboxes with external systems |
| **Consumer groups** | Named groups with committed offsets, heartbeats, and partition-like semantics |

---

## Quick Links

- [Installation →](getting-started/installation.md)
- [5-minute Quickstart →](getting-started/quickstart.md)
- [SQL API Reference →](sql-reference/outbox-api.md)
- [Relay Configuration →](relay-guide/configuration.md)
- [Architecture →](evaluate/architecture.md)

---

## License

pg_tide is released under the [Apache-2.0 license](https://github.com/trickle-labs/pg-tide/blob/main/LICENSE).
