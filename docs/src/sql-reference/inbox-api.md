# Inbox API

All inbox functions live in the `tide` schema.

---

## tide.inbox_create

Create a named inbox with its message table.

```sql
SELECT tide.inbox_create(
  p_name                      TEXT,
  p_schema                    TEXT DEFAULT 'tide',
  p_max_retries               INT  DEFAULT 3,
  p_processed_retention_hours INT  DEFAULT 72,
  p_dlq_retention_hours       INT  DEFAULT 0
);
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `p_name` | TEXT | (required) | Unique inbox name |
| `p_schema` | TEXT | `'tide'` | Schema where the inbox table is created |
| `p_max_retries` | INT | 3 | Max processing attempts before DLQ |
| `p_processed_retention_hours` | INT | 72 | Hours to keep processed messages |
| `p_dlq_retention_hours` | INT | 0 | Hours to keep DLQ messages (0 = forever) |

**Creates:** A table `{schema}."{name}_inbox"` with columns for dedup, retry tracking, and payload storage.

**Example:**

```sql
SELECT tide.inbox_create('payment-webhooks',
  p_max_retries := 5,
  p_processed_retention_hours := 168
);
```

---

## tide.inbox_drop

Drop a named inbox and its message table.

```sql
SELECT tide.inbox_drop(
  p_name       TEXT,
  p_if_exists  BOOLEAN DEFAULT false
);
```

**Cascades:** Drops the inbox table and removes the config entry.

---

## tide.inbox_mark_processed

Mark an inbox message as successfully processed.

```sql
SELECT tide.inbox_mark_processed(
  p_name      TEXT,
  p_event_id  TEXT
);
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `p_name` | TEXT | Inbox name |
| `p_event_id` | TEXT | The event_id to mark as processed |

Sets `processed_at = now()` on the matching row. Idempotent — calling it on an already-processed message is a no-op.

---

## tide.inbox_mark_failed

Mark an inbox message as failed. Increments `retry_count` and stores the error.

```sql
SELECT tide.inbox_mark_failed(
  p_name      TEXT,
  p_event_id  TEXT,
  p_error     TEXT
);
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `p_name` | TEXT | Inbox name |
| `p_event_id` | TEXT | The event_id that failed |
| `p_error` | TEXT | Error message to store |

---

## tide.inbox_status

Get status summary for an inbox (or all inboxes).

```sql
-- Single inbox
SELECT tide.inbox_status('payment-webhooks') → JSONB

-- All inboxes
SELECT tide.inbox_status() → JSONB
```

**Returns (single inbox):**

```json
{
  "inbox_name": "payment-webhooks",
  "pending": 3,
  "dlq_count": 1
}
```

---

## tide.replay_inbox_messages

Re-queue failed messages for reprocessing. Resets `retry_count` to 0 and clears `last_error`.

```sql
SELECT tide.replay_inbox_messages(
  p_name       TEXT,
  p_event_ids  TEXT[]
) → BIGINT
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `p_name` | TEXT | Inbox name |
| `p_event_ids` | TEXT[] | Array of event_ids to replay |

**Returns:** Number of messages successfully re-queued.

**Example:**

```sql
SELECT tide.replay_inbox_messages('payment-webhooks',
  ARRAY['evt-003', 'evt-007', 'evt-015']
);
```

---

## Inbox Table Schema

Each inbox gets a table `{schema}."{name}_inbox"` with this structure:

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGINT | Auto-generated primary key |
| `event_id` | TEXT | Dedup key (UNIQUE) |
| `source` | TEXT | Where the message came from |
| `payload` | JSONB | Message body |
| `headers` | JSONB | Metadata |
| `received_at` | TIMESTAMPTZ | When the message arrived |
| `processed_at` | TIMESTAMPTZ | When processing completed (NULL = pending) |
| `retry_count` | INT | Number of failed processing attempts |
| `last_error` | TEXT | Most recent error message |
