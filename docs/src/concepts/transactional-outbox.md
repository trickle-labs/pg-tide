# Transactional Outbox

The transactional outbox is the core pattern that pg_tide implements. It solves the fundamental problem of reliably publishing events when your data and your message broker are separate systems.

---

## The Problem: Dual Writes

Consider a typical flow: your application writes to PostgreSQL and then publishes an event to Kafka. What happens if the Kafka publish fails after the database commit? Or if the application crashes between the two operations?

```
Application ──INSERT──▶ PostgreSQL  ✓
            ──publish──▶ Kafka      ✗ (network timeout)
```

The database has the data. Kafka does not. Downstream consumers never learn about the change. This is the **dual-write problem**, and it creates silent data loss that's extremely difficult to detect and recover from.

---

## The Solution: Write Once, Relay Later

The transactional outbox flips the model. Instead of writing to two systems, you write to **one system** (PostgreSQL) in a single transaction:

```sql
BEGIN;
  -- Your business logic
  INSERT INTO orders (id, total) VALUES (42, 99.99);

  -- Event publishing — same transaction
  SELECT tide.outbox_publish('orders',
    '{"order_id": 42, "total": 99.99}'::jsonb,
    '{"event_type": "order.created"}'::jsonb
  );
COMMIT;
```

Both writes succeed or fail together. There is no window where one succeeds without the other.

A separate **relay process** then reads committed messages from the outbox table and delivers them to downstream systems. The relay runs independently and can retry indefinitely — the message is safely persisted in PostgreSQL until delivery succeeds.

---

## How pg_tide Implements It

### The Outbox Table

All outbox messages are stored in `tide.tide_outbox_messages`:

| Column | Purpose |
|--------|---------|
| `id` | Auto-incrementing sequence (used as offset) |
| `outbox_name` | Discriminator — routes messages to the right pipeline |
| `payload` | Your event data (JSONB) |
| `headers` | Metadata: event type, correlation ID, etc. |
| `created_at` | When the message was published |
| `consumed_at` | When the relay delivered it (NULL = pending) |

### The Publish Function

`tide.outbox_publish(name, payload, headers)` inserts a row and fires `pg_notify('tide_outbox_new', name)` to wake the relay immediately.

### The Relay Loop

The relay binary continuously:

1. Polls for pending messages (`consumed_at IS NULL`)
2. Delivers them to the configured sink
3. Marks them consumed and commits the consumer offset
4. Respects retention policy (old messages are cleaned up)

---

## Guarantees

- **At-least-once delivery** — the relay retries until the sink acknowledges
- **Ordering** — messages within an outbox are delivered in `id` order
- **Transactional atomicity** — the event and the business data are committed together
- **Crash recovery** — uncommitted messages are rolled back; committed messages are retried

Combined with the [idempotent inbox](idempotent-inbox.md), this provides effective **exactly-once** end-to-end semantics.

---

## Retention and Cleanup

Each outbox has a configurable `retention_hours`. The relay marks messages as consumed after delivery. A background cleanup process (or manual `DELETE`) removes messages older than the retention window.

```sql
-- Create with 48-hour retention
SELECT tide.outbox_create('orders', p_retention_hours := 48);
```
