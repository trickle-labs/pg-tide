# pg_tide

[![CI](https://github.com/trickle-labs/pg-tide/actions/workflows/ci.yml/badge.svg)](https://github.com/trickle-labs/pg-tide/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**Transactional outbox, idempotent inbox, and relay pipelines for PostgreSQL 18+.**

pg_tide gives your PostgreSQL database a built-in messaging backbone. Publish events atomically within your existing transactions — no dual-writes, no distributed transactions, no message broker required at the database layer.

When you're ready to fan out to Kafka, NATS, Redis Streams, or any other system, the `pg-tide` relay binary bridges the gap with transactional publish and idempotent delivery primitives (at-least-once relay with deduplication via unique event IDs).

## Features

- **Transactional Outbox** — publish messages within a database transaction; no 2PC, no dual-writes
- **Idempotent Inbox** — exactly-once delivery with event deduplication via unique constraints
- **Consumer Groups** — Kafka-style offset tracking with heartbeats and visibility leases
- **Relay Binary** — standalone `pg-tide` process bridging outboxes/inboxes with external systems
- **Multi-Backend** — NATS, Kafka, Redis Streams, RabbitMQ, SQS, HTTP Webhooks
- **Hot Reload** — pipeline config lives in PostgreSQL; changes apply without relay restart
- **HA Ready** — advisory lock coordination for automatic failover across relay instances

## Quick Start

```sql
-- Install the extension
CREATE EXTENSION pg_tide;

-- Create an outbox
SELECT tide.outbox_create('orders', p_retention_hours := 24);

-- Publish a message (atomically with your business transaction)
BEGIN;
  INSERT INTO orders (id, total) VALUES (42, 99.99);
  SELECT tide.outbox_publish('orders',
    '{"order_id": 42, "total": 99.99}'::jsonb,
    '{"event_type": "order.created"}'::jsonb
  );
COMMIT;

-- Configure a relay pipeline
SELECT tide.relay_set_outbox('orders-nats', 'orders', 'nats',
  '{"url": "nats://localhost:4222", "subject": "orders.events"}'::jsonb
);
```

Then start the relay:

```bash
pg-tide --postgres-url "postgres://user:pass@localhost:5432/mydb"
```

Messages flow automatically from the outbox to NATS. See the [documentation](https://trickle-labs.github.io/pg-tide/) for full details.

## Installation

### Extension

```sql
CREATE EXTENSION pg_tide;
```

### Relay Binary

```bash
# From GitHub releases
curl -LO https://github.com/trickle-labs/pg-tide/releases/latest/download/pg-tide-x86_64-unknown-linux-gnu.tar.gz
tar xzf pg-tide-*.tar.gz && sudo mv pg-tide /usr/local/bin/

# Or via Docker
docker pull ghcr.io/trickle-labs/pg-tide:latest
```

## Documentation

Full documentation is available at **[trickle-labs.github.io/pg-tide](https://trickle-labs.github.io/pg-tide/)**.

- [Getting Started](https://trickle-labs.github.io/pg-tide/getting-started/first-pipeline.html)
- [SQL API Reference](https://trickle-labs.github.io/pg-tide/sql-reference/outbox-api.html)
- [Relay Configuration](https://trickle-labs.github.io/pg-tide/relay-guide/configuration.html)
- [Architecture](https://trickle-labs.github.io/pg-tide/evaluate/architecture.html)

## SQL API Overview

All functions live in the `tide` schema:

| Function | Description |
|----------|-------------|
| `tide.outbox_create()` | Create a named outbox |
| `tide.outbox_publish()` | Publish a message atomically |
| `tide.outbox_status()` | Status summary as JSONB |
| `tide.inbox_create()` | Create a named inbox |
| `tide.inbox_mark_processed()` | Mark message processed |
| `tide.inbox_mark_failed()` | Record failure with retry tracking |
| `tide.relay_set_outbox()` | Configure forward pipeline (outbox → sink) |
| `tide.relay_set_inbox()` | Configure reverse pipeline (source → inbox) |
| `tide.create_consumer_group()` | Create a consumer group |
| `tide.commit_offset()` | Commit consumer position |

Views: `tide.outbox_pending` · `tide.consumer_lag`

## Integration with pg_trickle

If you use [pg_trickle](https://github.com/trickle-labs/pg-trickle) ≥ v0.46.0,
install pg_tide first and then use `pgtrickle.attach_outbox()` to automatically
publish stream table changes to an outbox:

```sql
CREATE EXTENSION pg_tide;
SELECT pgtrickle.attach_outbox('my_stream_table', retention_hours := 48);
```

## License

Apache-2.0 — see [LICENSE](LICENSE).
