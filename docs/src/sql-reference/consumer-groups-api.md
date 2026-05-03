# Consumer Groups API

Functions for managing consumer groups. All live in the `tide` schema.

---

## tide.create_consumer_group

Create a named consumer group for an outbox.

```sql
SELECT tide.create_consumer_group(
  p_name               TEXT,
  p_outbox             TEXT,
  p_auto_offset_reset  TEXT     DEFAULT 'earliest',
  p_if_not_exists      BOOLEAN  DEFAULT false
);
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `p_name` | TEXT | (required) | Unique group name |
| `p_outbox` | TEXT | (required) | Outbox this group consumes from |
| `p_auto_offset_reset` | TEXT | `'earliest'` | `earliest`, `latest`, or `none` |
| `p_if_not_exists` | BOOLEAN | false | Suppress error if already exists |

**Errors:**
- Outbox must exist
- Group name must be unique (unless `p_if_not_exists = true`)
- `p_auto_offset_reset` must be one of: `earliest`, `latest`, `none`

---

## tide.drop_consumer_group

Drop a consumer group and all its offset/lease records.

```sql
SELECT tide.drop_consumer_group(
  p_name       TEXT,
  p_if_exists  BOOLEAN DEFAULT false
);
```

---

## tide.commit_offset

Commit a consumer's processing position.

```sql
SELECT tide.commit_offset(
  p_group       TEXT,
  p_consumer    TEXT,
  p_last_offset BIGINT
);
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `p_group` | TEXT | Consumer group name |
| `p_consumer` | TEXT | Consumer identifier (e.g., relay instance ID) |
| `p_last_offset` | BIGINT | Last successfully processed message ID |

**Behavior:**
- Upserts into `tide.tide_consumer_offsets`
- Updates `last_heartbeat` to `now()`
- Releases any visibility lease held by this consumer

---

## tide.consumer_heartbeat

Update the heartbeat timestamp for a consumer.

```sql
SELECT tide.consumer_heartbeat(
  p_group     TEXT,
  p_consumer  TEXT
);
```

Call this periodically (e.g., every 10 seconds) while processing to signal liveness.

---

## Views

### tide.consumer_lag

Per-consumer lag relative to the latest outbox message:

```sql
SELECT * FROM tide.consumer_lag;
```

| Column | Type | Description |
|--------|------|-------------|
| `group_name` | TEXT | Consumer group |
| `outbox_name` | TEXT | Source outbox |
| `consumer_id` | TEXT | Consumer identifier |
| `committed_offset` | BIGINT | Last committed position |
| `lag` | BIGINT | Messages behind (max_id - committed_offset) |
| `last_heartbeat` | TIMESTAMPTZ | Last heartbeat time |
