# Idempotent Inbox

The idempotent inbox complements the transactional outbox by solving the receiving side of the equation. It ensures that duplicate deliveries — caused by retries, network issues, or relay restarts — are detected and discarded.

---

## Why Idempotency Matters

In distributed systems, **at-least-once delivery** is the practical reality. A relay might deliver a message, crash before committing its offset, and re-deliver the same message on restart. Without inbox-side deduplication, your application processes the event twice.

The idempotent inbox uses a unique `event_id` per message. If a message with the same `event_id` arrives again, the `UNIQUE` constraint prevents a duplicate insert. Your application sees each event exactly once.

---

## How It Works

Each inbox has its own message table with a unique constraint on `event_id`:

```sql
CREATE TABLE tide."my-inbox_inbox" (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id       TEXT NOT NULL,
    source         TEXT,
    payload        JSONB,
    headers        JSONB,
    received_at    TIMESTAMPTZ DEFAULT now(),
    processed_at   TIMESTAMPTZ,
    retry_count    INT DEFAULT 0,
    last_error     TEXT,
    CONSTRAINT uq_my_inbox_event_id UNIQUE (event_id)
);
```

When the relay (or any producer) delivers a message:

1. It attempts an `INSERT` with the event's dedup key as `event_id`
2. If the key already exists, the `ON CONFLICT` clause skips the insert
3. The message is only processed once, regardless of delivery attempts

---

## Creating an Inbox

```sql
SELECT tide.inbox_create('payment-events',
  p_max_retries := 5,
  p_processed_retention_hours := 72,
  p_dlq_retention_hours := 168
);
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `p_schema` | `'tide'` | Schema for the inbox table |
| `p_max_retries` | `3` | Attempts before moving to DLQ |
| `p_processed_retention_hours` | `72` | How long to keep processed messages |
| `p_dlq_retention_hours` | `0` | How long to keep failed messages (0 = forever) |

---

## Processing Messages

Your application reads from the inbox table and marks messages as processed:

```sql
-- Read pending messages
SELECT * FROM tide."payment-events_inbox"
WHERE processed_at IS NULL
ORDER BY id
LIMIT 10;

-- Mark as processed after handling
SELECT tide.inbox_mark_processed('payment-events', 'evt-001');

-- Mark as failed if processing errors
SELECT tide.inbox_mark_failed('payment-events', 'evt-003', 'timeout connecting to Stripe');
```

---

## Dead Letter Queue (DLQ)

Messages that fail more than `max_retries` times are effectively in the DLQ — they remain in the inbox table with `processed_at IS NULL` and `retry_count >= max_retries`.

Query DLQ messages:

```sql
SELECT * FROM tide."payment-events_inbox"
WHERE processed_at IS NULL
  AND retry_count >= 5;
```

Replay failed messages after fixing the issue:

```sql
SELECT tide.replay_inbox_messages('payment-events', ARRAY['evt-003', 'evt-007']);
```

---

## Dedup Key Strategy

The `event_id` should be deterministic and unique per logical event:

| Source | Recommended dedup key |
|--------|----------------------|
| pg_tide outbox | `{outbox_name}:{message_id}` (automatic) |
| Kafka | `{topic}:{partition}:{offset}` |
| NATS | Message ID from JetStream |
| HTTP webhook | `X-Request-ID` header |
| Custom | Any stable unique identifier |

The relay automatically generates appropriate dedup keys based on the source type.
