# Migrating to pg_tide

This guide covers migrating from common messaging and CDC solutions to pg_tide. Whether you're moving from Debezium, a custom outbox implementation, application-level message publishing, or another event streaming platform, this guide provides a path.

## From Debezium

### What Changes

| Aspect | Debezium | pg_tide |
|--------|----------|---------|
| CDC mechanism | WAL logical replication | Transactional outbox |
| Infrastructure | Kafka Connect cluster | Single relay binary |
| Configuration | Connector JSON via REST API | SQL catalog + relay process |
| Message format | Debezium JSON/Avro | Same (use `wire_format = "debezium"`) |
| Schema changes | Automatic (WAL captures all) | Application explicitly publishes |
| Filtering | SMTs (Single Message Transforms) | JMESPath transforms |

### Migration Steps

1. **Install pg_tide extension:**
   ```sql
   CREATE EXTENSION pg_tide;
   ```

2. **Create outbox for each captured table:**
   ```sql
   SELECT tide.outbox_create('orders_cdc');
   ```

3. **Add triggers or modify application to publish events:**
   ```sql
   -- Option A: Trigger (captures all changes automatically)
   CREATE TRIGGER orders_cdc AFTER INSERT OR UPDATE OR DELETE ON orders
   FOR EACH ROW EXECUTE FUNCTION your_cdc_trigger();
   
   -- Option B: Application publishes explicitly (recommended)
   SELECT tide.outbox_publish('orders_cdc', 'orders', your_payload);
   ```

4. **Configure pipeline with Debezium wire format:**
   ```sql
   SELECT tide.relay_set_outbox('orders-cdc', 'orders_cdc', '{
       "sink_type": "kafka",
       "brokers": "kafka:9092",
       "topic": "dbserver1.public.orders",
       "wire_format": "debezium",
       "wire_config": { "server_name": "dbserver1" }
   }'::jsonb);
   ```

5. **Run in parallel:** Deploy pg_tide alongside Debezium, publishing to a separate topic. Compare output.

6. **Switch consumers:** Once validated, point consumers to the pg_tide topic.

7. **Decommission Debezium:** Remove Kafka Connect connectors.

### Key Differences to Communicate to Your Team

- Consumers see identical Debezium-format messages (no consumer changes needed)
- Events are now guaranteed to be published if and only if the transaction commits
- You choose what to publish (vs. Debezium capturing everything)
- No more "snapshot" mode — data is published at application time

## From Custom Outbox Implementations

Many teams have hand-built outbox tables with cron jobs or application-level polling. Migrating to pg_tide replaces the custom infrastructure with a maintained, optimized solution.

### Typical Custom Outbox

```sql
-- Your existing custom outbox table
CREATE TABLE outbox (
    id BIGSERIAL PRIMARY KEY,
    event_type TEXT,
    payload JSONB,
    published BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT now()
);
```

### Migration Steps

1. **Create pg_tide outbox:**
   ```sql
   SELECT tide.outbox_create('events');
   ```

2. **Backfill unpublished messages (if needed):**
   ```sql
   INSERT INTO tide.outbox_events (stream_table, payload)
   SELECT event_type, payload FROM outbox WHERE published = FALSE;
   ```

3. **Update application code:**
   ```sql
   -- Before: INSERT INTO outbox (event_type, payload) VALUES (...)
   -- After:
   SELECT tide.outbox_publish('events', 'orders', '{"order_id": "..."}'::jsonb);
   ```

4. **Configure relay pipeline:**
   ```sql
   SELECT tide.relay_set_outbox('events-pipeline', 'events', '{
       "sink_type": "your-sink",
       ...
   }'::jsonb);
   ```

5. **Remove old polling infrastructure:** Delete cron jobs, background workers, custom retry logic.

## From Direct Message Publishing

If your application publishes directly to Kafka/NATS/RabbitMQ (without an outbox), you have a dual-write problem — the database write and message publish can become inconsistent.

### The Dual-Write Problem

```
BEGIN;
  INSERT INTO orders (...);  -- ✓ succeeds
COMMIT;
publish_to_kafka(...);       -- ✗ fails (network error)
-- Order exists but event was never published!
```

### Migration to Transactional Outbox

```sql
BEGIN;
  INSERT INTO orders (...);
  SELECT tide.outbox_publish('order_events', 'orders', ...);
COMMIT;
-- Both succeed or both fail. pg_tide relay handles delivery.
```

### Steps

1. Install pg_tide and create outbox
2. Replace direct publish calls with `tide.outbox_publish()` inside the transaction
3. Configure relay to deliver to the same broker
4. Remove direct publishing code and client libraries

## From RabbitMQ / ActiveMQ

If you're using a traditional message broker and want to move to pg_tide:

1. **Keep the broker** as the transport — pg_tide publishes to RabbitMQ/AMQP
2. **Replace the producer** — use transactional outbox instead of direct publish
3. **Consumers stay the same** — they still read from the same queues

```sql
SELECT tide.relay_set_outbox('orders-to-rabbit', 'order_events', '{
    "sink_type": "rabbitmq",
    "url": "amqp://guest:guest@rabbitmq:5672",
    "exchange": "orders",
    "routing_key": "order.created"
}'::jsonb);
```

## Rollback Plan

If you need to roll back the migration:

1. pg_tide outbox tables remain in your database (no data loss)
2. Re-enable your previous publishing mechanism
3. Disable the pg_tide relay pipeline:
   ```sql
   UPDATE tide.relay_outbox_config SET enabled = false WHERE name = 'your-pipeline';
   ```
4. The extension can be dropped if no longer needed:
   ```sql
   DROP EXTENSION pg_tide CASCADE;
   ```

## Further Reading

- [Tutorial: Debezium-Compatible Replication](../tutorials/debezium-replication.md) — Step-by-step Debezium replacement
- [Concepts: Transactional Outbox](../concepts/transactional-outbox.md) — Why this pattern works
- [Architecture](../evaluate/architecture.md) — How pg_tide works internally
