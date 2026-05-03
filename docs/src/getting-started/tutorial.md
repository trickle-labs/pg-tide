# Tutorial: Your First Pipeline

This tutorial walks you through setting up a complete outbox → NATS pipeline. You'll publish order events from PostgreSQL and receive them on a NATS subject.

---

## Prerequisites

- PostgreSQL 18+ with pg_tide installed
- NATS server running locally (`nats-server` or Docker)
- The `pg-tide` relay binary installed

---

## Step 1: Set Up the Database

```sql
-- Create the extension
CREATE EXTENSION pg_tide;

-- Create an outbox for order events
SELECT tide.outbox_create('orders', p_retention_hours := 48);

-- Create a consumer group for the relay
SELECT tide.create_consumer_group('nats-relay', 'orders');
```

## Step 2: Configure the Relay Pipeline

Tell pg_tide how to relay messages from the `orders` outbox to NATS:

```sql
SELECT tide.relay_set_outbox(
  'orders-to-nats',        -- pipeline name
  'orders',                -- source outbox
  'nats',                  -- sink type
  jsonb_build_object(
    'url', 'nats://localhost:4222',
    'subject', 'orders.events'
  )
);
```

## Step 3: Start the Relay

```bash
pg-tide --postgres-url "postgres://user:pass@localhost:5432/mydb"
```

The relay discovers the `orders-to-nats` pipeline from the database, acquires an advisory lock, and begins polling.

## Step 4: Publish Events

In your application (or via psql):

```sql
BEGIN;
  INSERT INTO orders (id, customer_id, total)
  VALUES (101, 'cust-42', 149.99);

  SELECT tide.outbox_publish('orders',
    '{"order_id": 101, "customer_id": "cust-42", "total": 149.99}'::jsonb,
    '{"event_type": "order.created"}'::jsonb
  );
COMMIT;
```

## Step 5: Receive on NATS

Subscribe with the NATS CLI:

```bash
nats sub "orders.events"
```

You'll see:

```
[#1] Received on "orders.events"
{"order_id": 101, "customer_id": "cust-42", "total": 149.99}
```

## Step 6: Monitor

Check outbox status and consumer lag:

```sql
-- Pending messages
SELECT * FROM tide.outbox_pending;

-- Consumer lag
SELECT * FROM tide.consumer_lag;

-- Detailed status
SELECT tide.outbox_status('orders');
```

The relay also exposes Prometheus metrics at `http://localhost:9090/metrics`.

---

## What's Happening Under the Hood

1. Your `INSERT` and `outbox_publish` run in the same transaction — atomically committed
2. A `pg_notify('tide_outbox_new', 'orders')` fires on commit, waking the relay
3. The relay polls `tide_outbox_messages` for pending rows
4. Each message is wrapped in a `RelayMessage` envelope with a dedup key
5. The NATS sink publishes to the configured subject
6. On success, the relay commits the consumer offset and marks messages consumed
7. If NATS is unavailable, the relay retries with exponential backoff

---

## Next Steps

- Add an **inbox** to receive events from other services: [Inbox API →](../sql-reference/inbox-api.md)
- Configure **multiple pipelines** for different event types
- Set up **monitoring** with Prometheus and Grafana: [Monitoring →](../relay-guide/monitoring.md)
