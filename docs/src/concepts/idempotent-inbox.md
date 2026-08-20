# Concept: The Idempotent Inbox Pattern

The idempotent inbox is the receiving counterpart to the transactional
outbox. It durably deduplicates repeated deliveries and can support an
effectively exactly-once processing outcome when application work and
`inbox_mark_processed()` share one transaction.

## The Problem

In distributed systems, at-least-once delivery is the norm. Messages can be delivered more than once due to:
- Network timeouts (sender retries after no ack)
- Consumer crashes (message re-delivered after acknowledgment timeout)
- Relay restarts (last batch re-delivered)
- Partition rebalances (offset not committed)

If your service processes the same payment event twice, you might charge the customer twice. If it processes the same order event twice, you might ship duplicate items.

## The Solution

The inbox table stores every received message with a unique identifier (deduplication key). Before processing, it checks whether the message has already been seen:

```sql
-- The relay writes incoming messages to the inbox
-- Duplicate dedup_keys are silently ignored (idempotent)
INSERT INTO tide.inbox_events (dedup_key, event_type, payload)
VALUES ('evt-123', 'order.created', '{"order_id": "ORD-001"}')
ON CONFLICT (dedup_key) DO NOTHING;
```

If the same message arrives again (same dedup_key), the INSERT silently does nothing. The message is not processed a second time.

## Processing Workflow

```
1. Message arrives from source (Kafka, NATS, webhook, etc.)
2. Relay writes to inbox (ON CONFLICT DO NOTHING)
3. Application queries inbox for pending messages
4. Application processes the message within a transaction
5. Application marks the message as processed
```

```sql
-- Step 3: Query pending messages
SELECT id, event_type, payload 
FROM tide.inbox_pending('payment_events') 
LIMIT 10;

-- Step 4-5: Process within transaction
BEGIN;
  -- Your business logic here
  INSERT INTO payments (order_id, amount, status) 
  VALUES ('ORD-001', 149.99, 'captured');
  
  -- Mark as processed (atomically with business logic)
  SELECT tide.inbox_mark_processed('payment_events', 42);
COMMIT;
```

Because the mark-processed call is inside the same transaction as the business logic, either both succeed or both fail. If the transaction rolls back, the message remains pending and will be retried.

## Deduplication Keys

The dedup key uniquely identifies a message. Common strategies:

| Strategy | Example | Use Case |
|----------|---------|----------|
| Message ID | `"evt-abc-123"` | Source provides unique IDs |
| Outbox ID | `"outbox-42"` | Cross-service pg_tide communication |
| Composite | `"order-ORD-001-created"` | Derived from payload |
| Kafka offset | `"topic-0-12345"` | Kafka partition + offset |

pg_tide extracts the dedup key from the native or CloudEvents message key.

## Failure Handling

If processing a message fails (business logic error, constraint violation), you have two options:

**Retry later:**
```sql
-- Leave as pending, it will be retried on next poll
-- Optionally record the error for monitoring
SELECT tide.inbox_mark_failed('payment_events', 42, 'Insufficient funds');
```

**Skip permanently:**
```sql
-- Mark as processed to advance past it
SELECT tide.inbox_mark_processed('payment_events', 42);
```

## Effectively exactly-once processing

The inbox provides a transactional, effectively exactly-once *processing
outcome* (not transport delivery) through this mechanism:

1. **At-least-once delivery:** The source may deliver the same message multiple times
2. **Deduplication on write:** The inbox's unique constraint prevents duplicate storage
3. **Atomic processing:** Business logic + mark-processed in one transaction

The combination makes each unique message eligible for one transactional
processing outcome, regardless of how many times the relay delivers it.

## When to Use the Inbox

Use the inbox when:
- You receive events from external systems that may duplicate
- Processing has side effects (charging money, sending emails, updating state)
- You need a reliable buffer between message receipt and processing
- You want to decouple message consumption from message processing

## Further Reading

- [Transactional Outbox](transactional-outbox.md) — The publishing counterpart
- [SQL Reference: Inbox API](../sql-reference/inbox-api.md) — Complete function reference
- [Message Guarantees](message-guarantees.md) — Delivery semantics
