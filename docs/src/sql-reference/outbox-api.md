# Outbox API

All outbox functions live in the `tide` schema.

---

## tide.outbox_create

Create a new named outbox.

```sql
SELECT tide.outbox_create(
  p_name              TEXT,
  p_retention_hours   INT  DEFAULT 24,
  p_inline_threshold  INT  DEFAULT 10000
);
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `p_name` | TEXT | (required) | Unique outbox name |
| `p_retention_hours` | INT | 24 | Hours to retain consumed messages before cleanup |
| `p_inline_threshold` | INT | 10000 | Maximum pending messages before backpressure signals |

**Errors:**
- Raises an error if an outbox with the same name already exists.

**Example:**

```sql
SELECT tide.outbox_create('order-events', 48, 50000);
```

---

## tide.outbox_publish

Publish a message to a named outbox. Runs within the caller's transaction.

```sql
SELECT tide.outbox_publish(
  p_name     TEXT,
  p_payload  JSONB,
  p_headers  JSONB
);
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `p_name` | TEXT | Target outbox name |
| `p_payload` | JSONB | Message body |
| `p_headers` | JSONB | Metadata (event_type, correlation_id, etc.) |

**Behavior:**
- Inserts into `tide.tide_outbox_messages`
- Fires `pg_notify('tide_outbox_new', p_name)` to wake the relay
- Errors if the outbox does not exist or is disabled

**Example:**

```sql
BEGIN;
  INSERT INTO orders (id, total) VALUES (42, 99.99);
  SELECT tide.outbox_publish('order-events',
    '{"order_id": 42, "total": 99.99}'::jsonb,
    '{"event_type": "order.created"}'::jsonb
  );
COMMIT;
```

---

## tide.outbox_drop

Drop a named outbox and all its messages.

```sql
SELECT tide.outbox_drop(
  p_name       TEXT,
  p_if_exists  BOOLEAN DEFAULT false
);
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `p_name` | TEXT | (required) | Outbox to drop |
| `p_if_exists` | BOOLEAN | false | Suppress error if outbox doesn't exist |

**Cascades:** Removes all messages and consumer groups for this outbox.

---

## tide.outbox_status

Get a status summary for a named outbox.

```sql
SELECT tide.outbox_status(p_name TEXT) → JSONB
```

**Returns:**

```json
{
  "outbox_name": "orders",
  "pending_messages": 42,
  "total_messages": 1500,
  "oldest_pending_age_seconds": 3.7,
  "retention_hours": 24
}
```

---

## tide.outbox_disable

Pause an outbox. Calls to `outbox_publish` will error while the outbox is disabled.

```sql
SELECT tide.outbox_disable(p_name TEXT);
```

---

## tide.outbox_enable

Resume a previously disabled outbox.

```sql
SELECT tide.outbox_enable(p_name TEXT);
```

---

## Views

### tide.outbox_pending

Pending (unconsumed) messages per outbox:

```sql
SELECT * FROM tide.outbox_pending;
```

| Column | Type | Description |
|--------|------|-------------|
| `outbox_name` | TEXT | Outbox name |
| `pending_count` | BIGINT | Number of unconsumed messages |
| `oldest_at` | TIMESTAMPTZ | Timestamp of the oldest pending message |
| `max_id` | BIGINT | Highest message ID in this outbox |
