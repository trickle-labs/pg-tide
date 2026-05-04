# pg-trickle Integration

[pg-trickle](https://github.com/trickle-labs/pg-trickle) is the PostgreSQL
extension that pg_tide was originally extracted from. If you already use
pg-trickle, you can adopt pg_tide incrementally — or run both side-by-side
during a migration.

## When to use pg_tide instead of pg-trickle

| Situation | Recommendation |
|-----------|---------------|
| Starting a new project | Use pg_tide — it is the focused, standalone successor |
| Existing pg-trickle installation | Migrate when ready; pg_tide's schema is compatible |
| Need stream tables (`pg_trickle_streams`) | Stay on pg-trickle for now |
| Need transactional outbox + inbox only | pg_tide covers this fully |

## Schema compatibility

pg_tide uses a `tide.*` schema prefix. pg-trickle uses `pg_trickle_*` table
names. The two schemas can coexist in the same database without conflict.

## Migrating from pg-trickle

### 1. Install pg_tide alongside pg-trickle

```sql
CREATE EXTENSION pg_tide;
```

### 2. Create matching outboxes and inboxes

For each `pg_trickle_outbox` in your existing schema:

```sql
SELECT tide.outbox_create(outbox_name, retention_hours)
FROM pg_trickle_outbox_config;
```

### 3. Migrate pending messages

```sql
INSERT INTO tide.tide_outbox_messages (outbox_name, payload, headers, created_at)
SELECT stream_name, payload, headers, created_at
FROM pg_trickle_outbox_messages
WHERE consumed_at IS NULL;
```

### 4. Point the relay at pg_tide

Update your `pg-tide` relay configuration:

```sql
SELECT tide.relay_set_outbox(
    'my-pipeline',
    'orders',
    'nats',
    '{"url":"nats://broker:4222"}'::jsonb
);
```

### 5. Verify and cut over

Run both relays in parallel during the transition. Once the pg_tide relay is
processing all new messages, decommission the pg-trickle relay.

## Using both together

Both extensions can write to the same NATS subject or Kafka topic — consumers
should use the `x-source` header to distinguish messages originating from
pg_tide versus pg-trickle.

pg_tide stamps every envelope with:

```json
{
  "id": 42,
  "outbox_name": "orders",
  "payload": { ... },
  "headers": { "x-source": "pg-tide", "x-outbox": "orders" }
}
```
