# Tutorial: Inbox from NATS

This tutorial sets up a reverse pipeline that receives events from a NATS subject and writes them to a pg_tide inbox with exactly-once semantics.

---

## Prerequisites

- PostgreSQL 18+ with pg_tide installed
- NATS server running
- pg-tide relay binary

---

## Step 1: Create the Inbox

```sql
SELECT tide.inbox_create('external-events',
  p_max_retries := 5,
  p_processed_retention_hours := 168
);
```

## Step 2: Configure the Reverse Pipeline

```sql
SELECT tide.relay_set_inbox('nats-to-inbox', 'external-events',
  jsonb_build_object(
    'url', 'nats://localhost:4222',
    'subject', 'partner.events.>'
  ),
  p_source := 'nats',
  p_idempotent := true
);
```

## Step 3: Start the Relay

```bash
pg-tide --postgres-url "postgres://user:pass@localhost:5432/mydb"
```

## Step 4: Publish to NATS

From another service or the NATS CLI:

```bash
nats pub partner.events.order '{"order_id": 99, "status": "shipped"}'
```

## Step 5: Read from the Inbox

```sql
-- Check what arrived
SELECT event_id, payload, received_at
FROM tide."external-events_inbox"
WHERE processed_at IS NULL;

-- Process the message
SELECT tide.inbox_mark_processed('external-events', 'nats:msg-id-123');
```

---

## Deduplication in Action

If the same NATS message is delivered twice (e.g., due to a reconnect), the inbox's `UNIQUE(event_id)` constraint silently rejects the duplicate:

```sql
-- Only one row, even after multiple deliveries
SELECT COUNT(*) FROM tide."external-events_inbox"
WHERE event_id = 'nats:msg-id-123';
-- Result: 1
```

---

## Error Handling

If your processing logic fails, mark the message as failed:

```sql
SELECT tide.inbox_mark_failed('external-events', 'nats:msg-id-123', 'payment gateway timeout');
```

After `max_retries` failures, the message enters the DLQ. Fix the issue and replay:

```sql
SELECT tide.replay_inbox_messages('external-events', ARRAY['nats:msg-id-123']);
```
