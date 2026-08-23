# Your First Pipeline

This guide walks you through setting up pg_tide from scratch and building a complete message pipeline. By the end, you'll have an outbox publishing order events and a relay delivering them to NATS JetStream — with monitoring, per-pipeline offset tracking, and at-least-once delivery (with stable deduplication identities) all working together.

We'll go step by step, explaining what's happening at each stage so you understand not just *what* to do, but *why* each piece matters.

---

## Prerequisites

Before starting, make sure you have:

- **PostgreSQL 18** running and accessible (local or remote)
- **NATS server** running locally (we'll use this as our message sink)
- **pg-tide relay** binary installed (see [Installation](installation.md))

If you just want to kick the tires quickly, here's a Docker Compose file that sets up everything:

```yaml
# docker-compose.yml — complete pg_tide development environment
services:
  postgres:
    image: postgres:18
    environment:
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: app
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

  nats:
    image: nats:latest
    ports:
      - "4222:4222"   # Client connections
      - "8222:8222"   # Monitoring

  pg-tide-relay:
    image: ghcr.io/trickle-labs/pg-tide:latest
    depends_on:
      - postgres
      - nats
    environment:
      PG_TIDE_POSTGRES_URL: "postgres://postgres:postgres@postgres:5432/app"
      PG_TIDE_LOG_FORMAT: "json"
      PG_TIDE_LOG_LEVEL: "info"
    ports:
      - "9090:9090"   # Metrics + health

volumes:
  pgdata:
```

Start it with `docker compose up -d`, then connect to PostgreSQL with:

```bash
psql "postgres://postgres:postgres@localhost:5432/app"
```

---

## Step 1: Install the Extension

The pg_tide extension creates the `tide` schema with all the catalog tables, views, and functions you'll need:

<!-- pg-tide-example: tested id=first-pipeline-install-sql test=quickstart-sql-pr -->
<!-- quickstart:run -->
```sql
CREATE EXTENSION IF NOT EXISTS pg_tide;
```

Let's verify it's installed correctly:

<!-- pg-tide-example: tested id=first-pipeline-version-sql test=quickstart-sql-pr -->
<!-- quickstart:run -->
```sql
SELECT extname, extversion FROM pg_extension WHERE extname = 'pg_tide';
```

The output shows the current installed version:
```
 extname | extversion
---------+-------------------
 pg_tide | <installed-version>
```

Behind the scenes, this created:
- The `tide` schema
- Configuration tables for outboxes, inboxes, consumer groups, and relay pipelines
- The shared `tide.tide_outbox_messages` table where all outbox messages live
- Offset-aware views like `tide.outbox_retention_status` and
  `tide.relay_pipeline_lag` for monitoring
- SQL functions like `tide.outbox_publish()` for the API

---

## Step 2: Create an Outbox

An outbox is a named message stream. You might have one outbox for order events, another for user events, another for inventory changes — each is logically separate but physically stored in the same table (discriminated by name).

Let's create an outbox for order events:

<!-- pg-tide-example: tested id=first-pipeline-outbox-sql test=quickstart-sql-pr -->
<!-- quickstart:run -->
```sql
SELECT tide.outbox_create_if_not_exists('orders',
  p_retention_hours := 48,
  p_inline_threshold := 10000
);
```

What do these parameters mean?

- **`'orders'`** — the name of our outbox. This is how you'll refer to it when publishing and when configuring relay pipelines.
- **`p_retention_hours := 48`** — consumed messages are kept for 48 hours before cleanup. This gives you time to investigate issues and replay if needed.
- **`p_inline_threshold := 10000`** — retained compatibility/configuration
  metadata. Native publishing does not reject transactions based on pending
  row count; reserve disk and alert on exact lag when a sink is unavailable.

Verify the outbox exists:

```sql
SELECT * FROM tide.tide_outbox_config;
```

```
 outbox_name | retention_hours | inline_threshold | enabled |         created_at
-------------+-----------------+------------------+---------+----------------------------
 orders      |              48 |            10000 | t       | 2025-01-15 10:00:00.000+00
```

---

## Step 3: Publish Your First Messages

Now let's simulate what your application would do — publishing events within business transactions. The key insight is that the event publish is *inside* the same transaction as the business logic:

```sql
-- Create a simple orders table for this tutorial
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    customer_id TEXT NOT NULL,
    total NUMERIC(10,2) NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Now publish an event atomically with the business operation
BEGIN;
  INSERT INTO orders (id, customer_id, total, status)
  VALUES (1, 'cust-alice', 149.99, 'confirmed');

  SELECT tide.outbox_publish('orders',
    '{"order_id": 1, "customer_id": "cust-alice", "total": 149.99, "status": "confirmed"}'::jsonb,
    '{"event_type": "order.confirmed", "source": "tutorial"}'::jsonb
  );
COMMIT;
```

Let's publish a few more events to make things interesting:

```sql
BEGIN;
  INSERT INTO orders (id, customer_id, total, status)
  VALUES (2, 'cust-bob', 42.00, 'confirmed');

  SELECT tide.outbox_publish('orders',
    '{"order_id": 2, "customer_id": "cust-bob", "total": 42.00, "status": "confirmed"}'::jsonb,
    '{"event_type": "order.confirmed", "source": "tutorial"}'::jsonb
  );
COMMIT;

BEGIN;
  INSERT INTO orders (id, customer_id, total, status)
  VALUES (3, 'cust-charlie', 299.95, 'confirmed');

  SELECT tide.outbox_publish('orders',
    '{"order_id": 3, "customer_id": "cust-charlie", "total": 299.95, "status": "confirmed"}'::jsonb,
    '{"event_type": "order.confirmed", "source": "tutorial"}'::jsonb
  );
COMMIT;
```

**What just happened:** Each transaction atomically wrote the order to the `orders` table AND the event to the outbox. If any transaction had failed (constraint violation, network error, application crash), both the order and the event would have been rolled back together. No orphaned events, no missing events.

---

## Step 4: Check the Outbox Status

Let's verify our messages are pending (waiting for delivery):

```sql
SELECT * FROM tide.outbox_retention_status;
```

```
 outbox_name | pending_count |       oldest_at        | max_id
-------------+---------------+------------------------+--------
 orders      |             3 | 2025-01-15 10:01:00+00 |      3
```

Three messages waiting for the relay to pick them up. You can also get detailed status:

```sql
SELECT tide.outbox_status('orders');
```

This returns a JSONB object with comprehensive information about the outbox's current state.

---

## Step 5: (Optional) Create a Consumer Group

The native relay tracks its own durable offset per relay group, pipeline, and outbox, so it does **not** require a consumer group. Consumer groups remain available as a legacy/global-consumer surface (for example, for SQL-side `tide.poll_outbox()` consumers). You can skip to Step 6 for the native pipeline.

If you do want a consumer group, create one like this:

```sql
SELECT tide.create_consumer_group('nats-relay', 'orders',
  p_auto_offset_reset := 'earliest'
);
```

`'earliest'` processes all existing messages; `'latest'` skips to only future messages.

---

## Step 6: Configure the Relay Pipeline

Now we tell pg_tide how to deliver messages from the `orders` outbox to NATS. The native relay polls the shared `tide.tide_outbox_messages` table directly and tracks a durable offset per relay group, pipeline, and outbox (ADR-011) — no consumer group is required:

<!-- pg-tide-example: tested id=first-pipeline-relay-sql test=quickstart-sql-pr -->
<!-- quickstart:run -->
```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-to-nats',       -- pipeline name (unique identifier)
    'outbox', 'orders',             -- source outbox
    'sink_type', 'nats',            -- sink type
    'config', jsonb_build_object(   -- sink-specific configuration
      'url', 'tls://nats.example:4222',
      'subject_template', 'orders.{event_type}'
    )
  )
);
```

Notice the **subject template**: `'orders.{event_type}'`. The NATS sink substitutes `{event_type}` with the value from the message's `event_type` header (a missing or non-string header falls back to the literal `event`). Our messages have `"event_type": "order.confirmed"`, so they'll be published to the NATS subject `orders.order.confirmed`.

This is powerful — different event types from the same outbox can be routed to different NATS subjects without any relay-side logic.

---

## Step 7: Start the Relay

If you're using the Docker Compose setup, the relay is already running. Otherwise, start it manually:

```bash
pg-tide --postgres-url "postgres://postgres:postgres@localhost:5432/app"
```

You'll see log output like:

```
INFO  pg_tide_relay: Starting pg-tide relay v<installed-version>
INFO  pg_tide_relay: Connected to PostgreSQL
INFO  pg_tide_relay: Discovered pipeline: orders-to-nats (forward, nats)
INFO  pg_tide_relay: Acquired advisory lock for pipeline: orders-to-nats
INFO  pg_tide_relay: Pipeline orders-to-nats: processing 3 pending messages
INFO  pg_tide_relay: Pipeline orders-to-nats: published batch [1..3] to nats
```

**What's happening under the hood:**

1. The relay connects to PostgreSQL and reads the pipeline catalog
2. It discovers `orders-to-nats` and attempts to acquire an advisory lock for it
3. It succeeds (it's the only relay instance), so it owns this pipeline
4. It polls `tide.tide_outbox_messages` for messages in the `orders` outbox where `id > last_committed_offset`
5. It delivers each message to the configured NATS subject
6. On success, it commits the offset and marks messages as consumed

---

## Step 8: Verify Delivery

Subscribe to NATS to see the delivered messages (in another terminal):

```bash
# Using the nats CLI tool
nats sub "orders.>"
```

If messages were already delivered (relay started before you subscribed), you can verify from the PostgreSQL side:

```sql
-- Check that messages are now consumed
SELECT * FROM tide.outbox_retention_status;
```

```
 outbox_name | pending_count | oldest_at | max_id
-------------+---------------+-----------+--------
(0 rows)
```

No pending messages — they've all been delivered! Check consumer lag:

```sql
SELECT * FROM tide.relay_pipeline_lag;
```

```
 group_name | outbox_name | consumer_id | committed_offset | lag | last_heartbeat
------------+-------------+-------------+------------------+-----+--------------------
 nats-relay | orders      | relay-0     |                3 |   0 | 2025-01-15 10:02:00
```

Zero lag — the relay is fully caught up. The `committed_offset` of 3 means all messages through ID 3 have been delivered.

---

## Step 9: Publish More Messages and Watch Them Flow

Now that the pipeline is running, new messages are delivered in near-real-time. In another terminal, subscribe to NATS:

```bash
nats sub "orders.>"
```

Then publish a new event in psql:

```sql
BEGIN;
  INSERT INTO orders (id, customer_id, total, status)
  VALUES (4, 'cust-diana', 75.50, 'confirmed');

  SELECT tide.outbox_publish('orders',
    '{"order_id": 4, "customer_id": "cust-diana", "total": 75.50, "status": "confirmed"}'::jsonb,
    '{"event_type": "order.confirmed"}'::jsonb
  );
COMMIT;
```

Within milliseconds, you'll see the message appear on the NATS subscription. The flow is:

1. `COMMIT` triggers `pg_notify('tide_outbox_new', 'orders')`
2. The relay receives the notification and immediately polls for new messages
3. Message ID 4 is fetched, delivered to NATS, and the offset is committed

---

## Step 10: Monitor with Prometheus

The relay exposes Prometheus metrics at its metrics endpoint:

```bash
curl http://localhost:9090/metrics
```

Key metrics to watch:

```
# Total messages delivered
pg_tide_relay_messages_published_total{pipeline="orders-to-nats",direction="forward"} 4

# Error count (should be 0)
pg_tide_relay_publish_errors_total{pipeline="orders-to-nats",direction="forward"} 0

# Pipeline health
pg_tide_relay_pipeline_healthy{pipeline="orders-to-nats"} 1
```

And the health endpoint:

```bash
curl http://localhost:9090/health
# Returns 200 OK when all pipelines are healthy
```

---

## What You've Built

In this tutorial, you've set up a complete transactional outbox pipeline:

1. **The extension** provides the schema, tables, and SQL API
2. **The outbox** stores events atomically with your business transactions
3. **The consumer group** tracks how far the relay has progressed
4. **The pipeline configuration** tells the relay where to deliver messages
5. **The relay binary** bridges the gap between PostgreSQL and NATS
6. **Monitoring** via Prometheus metrics and the consumer lag view

This is the foundational pattern. From here, you can:

- Add more pipelines to fan out events to multiple systems
- Create an inbox to receive events from external services
- Add more relay instances for high availability
- Configure different destinations (PostgreSQL inbox, Kafka, NATS, and webhooks)

---

## Next Steps

- [Concepts: Message Guarantees →](../concepts/message-guarantees.md) — understand at-least-once delivery and application-level deduplication in depth
- [Concepts: Consumption and Relay →](../concepts/consumption-and-relay.md) — deep dive into consumer groups and pipeline mechanics
- [Relay Guide: Configuration →](../relay-guide/configuration.md) — configure the supported relay destinations
- [Tutorial: End-to-End Pipeline →](../tutorials/end-to-end-pipeline.md) — build a forward pipeline with Kafka
- [Operations: Deployment →](../operations/deployment.md) — production deployment patterns
