# Tutorial: Outbox to Kafka

This tutorial sets up a forward pipeline that relays order events from a pg_tide outbox to a Kafka topic.

---

## Prerequisites

- PostgreSQL 18+ with pg_tide installed
- Kafka cluster running (or Redpanda, which is Kafka-compatible)
- pg-tide relay built with the `kafka` feature

---

## Step 1: Create the Outbox

```sql
SELECT tide.outbox_create('orders', p_retention_hours := 72);
SELECT tide.create_consumer_group('kafka-relay', 'orders');
```

## Step 2: Configure the Pipeline

```sql
SELECT tide.relay_set_outbox('orders-to-kafka', 'orders', 'kafka',
  jsonb_build_object(
    'brokers', 'localhost:9092',
    'topic', 'order-events',
    'acks', 'all',
    'compression', 'snappy'
  ),
  p_batch_size := 200
);
```

## Step 3: Start the Relay

```bash
pg-tide --postgres-url "postgres://user:pass@localhost:5432/mydb"
```

## Step 4: Publish Events

```sql
BEGIN;
  INSERT INTO orders (id, customer, total) VALUES (1, 'alice', 42.00);
  SELECT tide.outbox_publish('orders',
    '{"order_id": 1, "customer": "alice", "total": 42.00}'::jsonb,
    '{"event_type": "order.created"}'::jsonb
  );
COMMIT;
```

## Step 5: Consume from Kafka

```bash
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic order-events --from-beginning
```

---

## Production Considerations

- Use `acks=all` for durability
- Enable compression (`snappy` or `lz4`) for throughput
- Set an appropriate `batch_size` (200-500 for Kafka)
- Monitor consumer lag: `SELECT * FROM tide.consumer_lag;`
