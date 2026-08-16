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
| `p_inline_threshold` | INT | 10000 | Deprecated compatibility value; native publishing does not enforce a pending-row cap |

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
  "retained_messages": 1500,
  "native_safe_offset": 1200,
  "oldest_pending_age_seconds": 3.7,
  "retention_hours": 24
}
```

Native status and lag are derived from per-pipeline offsets. `consumed_at` is
reported only as deprecated compatibility state.

---

## tide.outbox_sweep

Run one bounded retention batch per selected outbox.

```sql
SELECT tide.outbox_sweep(
  p_outbox_name TEXT DEFAULT NULL,
  p_batch_size INTEGER DEFAULT 1000,
  p_dry_run BOOLEAN DEFAULT FALSE
) → JSONB;
```

`p_batch_size` must be between 1 and 10,000. Candidates satisfy both the
retention cutoff and the minimum checkpoint across all configured native
pipelines, consumer groups, enabled fan-in members, and overlapping leases.
Disabled pipelines remain participants. Dry-run examines at most
`p_batch_size + 1` rows and returns `has_more` without deleting.

The result includes `outbox`, `retention_cutoff`, `safe_offset`,
`participants`, `blockers`, `eligible_in_batch`, `affected_rows`, `has_more`,
`highest_deleted_id`, `duration_ms`, and `partition_action`. A failure is a
PostgreSQL error, not a zero-row success.

`outbox_truncate_delivered(NULL)` is a deprecated compatibility wrapper for
one 1,000-row sweep.

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
| `max_id` | BIGINT | Highest retained message ID in this outbox |

### tide.outbox_retention_status

Offset-aware retention state per outbox: retained row count/bytes, oldest and
newest retained timestamps, cutoff, participant count, safe offset, bounded
eligible-row preview, blocker names/offsets, cleanup progress, storage layout,
and default-partition rows.

### tide.relay_pipeline_lag

Exact lag per relay group, pipeline, and outbox. Lag is computed as
`COUNT(*) WHERE outbox_name = ... AND id > last_change_id`; it does not subtract
a globally gapped ID from an offset.
