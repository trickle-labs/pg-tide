# Tutorial: End-to-End Pipeline

This tutorial builds a complete bidirectional messaging system: order events flow *out* from PostgreSQL to Kafka (forward pipeline), and payment confirmations flow *in* from NATS back into PostgreSQL (reverse pipeline). By the end, you'll have both directions working with exactly-once delivery guarantees.

This demonstrates a realistic pattern: your order service publishes events when orders are created, and a payment service (running elsewhere) confirms payments by publishing to NATS, which pg_tide relays back into your database for processing.

---

## Prerequisites

- PostgreSQL 18+ with pg_tide installed
- Kafka cluster running (or [Redpanda](https://redpanda.com), which is Kafka-compatible)
- NATS server running
- pg-tide relay built with `kafka` and `nats` features
- `kafka-console-consumer` CLI (comes with Kafka) and `nats` CLI

---

## Part 1: Forward Pipeline (Orders → Kafka)

### Step 1: Set up the database schema

```sql
-- Install the extension
CREATE EXTENSION pg_tide;

-- Create a table for our orders
CREATE TABLE orders (
    id          SERIAL PRIMARY KEY,
    customer_id TEXT NOT NULL,
    total       NUMERIC(10,2) NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMPTZ DEFAULT now()
);

-- Create an outbox for order events with 72-hour retention
-- (longer retention gives us time to investigate if something goes wrong)
SELECT tide.outbox_create('orders', p_retention_hours := 72);

-- Create a consumer group for the Kafka relay
-- Using 'earliest' so it processes all existing messages on startup
SELECT tide.create_consumer_group('kafka-relay', 'orders',
  p_auto_offset_reset := 'earliest'
);
```

### Step 2: Configure the Kafka pipeline

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-to-kafka',
    'outbox', 'orders',
    'sink_type', 'kafka',
    'config', jsonb_build_object(
      'brokers', 'localhost:9092',
      'topic', 'order-events',
      'acks', 'all',            -- wait for all replicas to acknowledge
      'compression', 'snappy',  -- good balance of speed and compression ratio
      'key', '{event_type}'     -- partition by event type for ordering
    ),
    'batch_size', 200
  )
);
```

Why these choices?

- **`acks=all`** — ensures messages are replicated before the relay considers them delivered. This is the safest option for production.
- **`compression=snappy`** — reduces network bandwidth with minimal CPU overhead. Kafka consumers decompress transparently.
- **`key={event_type}`** — messages with the same event type go to the same Kafka partition, preserving ordering within a type.
- **`batch_size=200`** — Kafka's efficiency improves with larger batches (amortizes protocol overhead and compression).

### Step 3: Start the relay

```bash
pg-tide --postgres-url "postgres://user:pass@localhost:5432/mydb"
```

The relay discovers the `orders-to-kafka` pipeline, acquires an advisory lock, and begins polling.

### Step 4: Publish order events

Simulate a new order being placed:

```sql
BEGIN;
  INSERT INTO orders (id, customer_id, total, status)
  VALUES (1, 'alice', 149.99, 'confirmed');

  -- Publish the event atomically with the business data
  SELECT tide.outbox_publish('orders',
    jsonb_build_object(
      'order_id', 1,
      'customer_id', 'alice',
      'total', 149.99,
      'status', 'confirmed',
      'items', jsonb_build_array(
        jsonb_build_object('sku', 'WIDGET-01', 'qty', 2, 'price', 49.99),
        jsonb_build_object('sku', 'GADGET-03', 'qty', 1, 'price', 50.01)
      )
    ),
    jsonb_build_object(
      'event_type', 'order.confirmed',
      'schema_version', '1.0',
      'correlation_id', 'req-abc-123'
    )
  );
COMMIT;
```

Publish a few more to make the pipeline active:

```sql
BEGIN;
  INSERT INTO orders (id, customer_id, total, status) VALUES (2, 'bob', 42.00, 'confirmed');
  SELECT tide.outbox_publish('orders',
    '{"order_id": 2, "customer_id": "bob", "total": 42.00, "status": "confirmed"}'::jsonb,
    '{"event_type": "order.confirmed", "schema_version": "1.0"}'::jsonb);
COMMIT;

BEGIN;
  INSERT INTO orders (id, customer_id, total, status) VALUES (3, 'charlie', 299.95, 'confirmed');
  SELECT tide.outbox_publish('orders',
    '{"order_id": 3, "customer_id": "charlie", "total": 299.95, "status": "confirmed"}'::jsonb,
    '{"event_type": "order.confirmed", "schema_version": "1.0"}'::jsonb);
COMMIT;
```

### Step 5: Verify delivery to Kafka

```bash
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic order-events --from-beginning --max-messages 3
```

You should see all three order events. Verify from the PostgreSQL side:

```sql
-- All messages delivered
SELECT * FROM tide.outbox_pending;
-- Should show 0 pending

-- Consumer lag at zero
SELECT * FROM tide.consumer_lag;
-- Should show lag = 0 for kafka-relay group
```

---

## Part 2: Reverse Pipeline (Payment Confirmations from NATS → Inbox)

Now let's set up the reverse direction: payment confirmations arrive on a NATS subject and are written to a pg_tide inbox for processing.

### Step 6: Create the inbox

```sql
-- Create an inbox for payment events
-- max_retries: how many times we'll retry processing before DLQ
-- processed_retention: keep successfully processed messages for 3 days (auditing)
-- dlq_retention: keep failed messages forever (manual investigation)
SELECT tide.inbox_create('payments',
  p_max_retries := 5,
  p_processed_retention_hours := 72,
  p_dlq_retention_hours := 0
);
```

This creates a table `tide."payments_inbox"` with a UNIQUE constraint on `event_id` for deduplication.

### Step 7: Configure the reverse pipeline

```sql
SELECT tide.relay_set_inbox_v2(
  jsonb_build_object(
    'name', 'nats-to-payments',
    'inbox', 'payments',
    'source', 'nats',
    'config', jsonb_build_object(
      'url', 'nats://localhost:4222',
      'subject', 'payments.confirmed',
      'queue_group', 'pg-tide-payments'
    ),
    'batch_size', 50,
    'idempotent', true
  )
);
```

The relay subscribes to `payments.confirmed` on NATS and writes incoming messages to the `payments` inbox. The `queue_group` ensures that if you run multiple relay instances, each message is processed by only one instance.

### Step 8: Simulate incoming payment confirmations

Using the NATS CLI, publish some payment events (as if a payment service were sending them):

```bash
nats pub payments.confirmed '{"payment_id": "pay-001", "order_id": 1, "amount": 149.99, "status": "completed", "processor": "stripe"}'

nats pub payments.confirmed '{"payment_id": "pay-002", "order_id": 2, "amount": 42.00, "status": "completed", "processor": "stripe"}'

nats pub payments.confirmed '{"payment_id": "pay-003", "order_id": 3, "amount": 299.95, "status": "completed", "processor": "stripe"}'
```

### Step 9: Verify inbox delivery

```sql
-- Check that messages arrived in the inbox
SELECT event_id, payload->>'payment_id' as payment_id,
       payload->>'order_id' as order_id,
       received_at, processed_at
FROM tide."payments_inbox"
ORDER BY id;
```

```
 event_id              | payment_id | order_id |       received_at        | processed_at
-----------------------+------------+----------+--------------------------+--------------
 payments:nats:seq-1   | pay-001    | 1        | 2025-01-15 10:05:00+00  |
 payments:nats:seq-2   | pay-002    | 2        | 2025-01-15 10:05:01+00  |
 payments:nats:seq-3   | pay-003    | 3        | 2025-01-15 10:05:02+00  |
```

Messages are in the inbox, waiting to be processed. Notice the `event_id` — this is the dedup key that prevents duplicate processing if the same message is delivered twice.

### Step 10: Process inbox messages

In your application, you'd read and process these messages:

```sql
-- Read pending payments
SELECT id, event_id, payload
FROM tide."payments_inbox"
WHERE processed_at IS NULL
  AND retry_count < 5
ORDER BY id
LIMIT 10;

-- After successfully updating the order status
UPDATE orders SET status = 'paid' WHERE id = 1;
SELECT tide.inbox_mark_processed('payments', 'payments:nats:seq-1');

-- Process the rest
UPDATE orders SET status = 'paid' WHERE id = 2;
SELECT tide.inbox_mark_processed('payments', 'payments:nats:seq-2');

UPDATE orders SET status = 'paid' WHERE id = 3;
SELECT tide.inbox_mark_processed('payments', 'payments:nats:seq-3');
```

### Step 11: Test deduplication

What happens if the same payment confirmation is delivered twice (e.g., the payment service retried)?

```bash
# Publish the same payment again
nats pub payments.confirmed '{"payment_id": "pay-001", "order_id": 1, "amount": 149.99, "status": "completed", "processor": "stripe"}'
```

The inbox's UNIQUE constraint catches the duplicate:

```sql
-- Still only one row for pay-001
SELECT count(*) FROM tide."payments_inbox"
WHERE payload->>'payment_id' = 'pay-001';
-- Returns: 1
```

No double-processing, no duplicate orders marked as paid.

---

## Part 3: Monitoring the Complete System

### Check both directions

```sql
-- Forward pipeline status: outbox → Kafka
SELECT * FROM tide.outbox_pending;
SELECT * FROM tide.consumer_lag;

-- Reverse pipeline status: NATS → inbox
SELECT
  count(*) FILTER (WHERE processed_at IS NULL AND retry_count < 5) as pending,
  count(*) FILTER (WHERE processed_at IS NOT NULL) as processed,
  count(*) FILTER (WHERE processed_at IS NULL AND retry_count >= 5) as dead_letter
FROM tide."payments_inbox";
```

### Prometheus metrics

```bash
curl -s http://localhost:9090/metrics | grep pg_tide
```

Key metrics to watch:

```
pg_tide_relay_messages_published_total{pipeline="orders-to-kafka",direction="forward"} 3
pg_tide_relay_messages_consumed_total{pipeline="nats-to-payments",direction="reverse"} 3
pg_tide_relay_pipeline_healthy{pipeline="orders-to-kafka"} 1
pg_tide_relay_pipeline_healthy{pipeline="nats-to-payments"} 1
```

---

## Part 4: Failure Scenarios

Let's test what happens when things go wrong.

### Scenario: Kafka is temporarily down

Stop Kafka, then publish a new order:

```sql
BEGIN;
  INSERT INTO orders (id, customer_id, total, status) VALUES (4, 'diana', 75.50, 'confirmed');
  SELECT tide.outbox_publish('orders',
    '{"order_id": 4, "customer_id": "diana", "total": 75.50}'::jsonb,
    '{"event_type": "order.confirmed"}'::jsonb);
COMMIT;
```

The message is safely in the outbox. The relay will retry delivery with exponential backoff until Kafka recovers. Check the outbox:

```sql
SELECT * FROM tide.outbox_pending;
-- Shows 1 pending message
```

Start Kafka again — the relay delivers the message automatically:

```sql
SELECT * FROM tide.outbox_pending;
-- Back to 0 pending
```

### Scenario: Processing an inbox message fails

```sql
-- Simulate a processing failure
SELECT tide.inbox_mark_failed('payments', 'payments:nats:seq-4',
  'External API timeout: Stripe returned 504');
```

The message stays in the inbox with an incremented `retry_count`. Your application can retry it later. After 5 failures (our `max_retries`), it's in the dead-letter queue:

```sql
-- Check DLQ
SELECT event_id, retry_count, last_error
FROM tide."payments_inbox"
WHERE processed_at IS NULL AND retry_count >= 5;
```

After fixing the issue, replay the message:

```sql
SELECT tide.replay_inbox_messages('payments', ARRAY['payments:nats:seq-4']);
```

---

## Production Considerations

When adapting this tutorial for production:

- **Use TLS** for all connections (PostgreSQL, Kafka, NATS)
- **Use `acks=all`** for Kafka to ensure message durability
- **Set appropriate batch sizes** — 200-500 for Kafka, 50-100 for NATS
- **Monitor consumer lag** and alert when it exceeds thresholds
- **Run multiple relay instances** (same `relay_group_id`) for HA
- **Use structured JSON logging** (`--log-format json`) for log aggregation
- **Set retention appropriately** — longer retention means more disk usage but easier debugging

---

## Next Steps

- [Bidirectional Sync →](bidirectional-sync.md) — synchronize state between two services
- [Fan-out Pattern →](fan-out-pattern.md) — deliver one outbox to multiple sinks
- [Dead-Letter Queue →](dead-letter-queue.md) — manage failed messages systematically
- [Real-World Scenarios →](real-world-scenarios.md) — complete business use cases
