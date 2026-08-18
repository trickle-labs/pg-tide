# PostgreSQL Inbox

The PostgreSQL Inbox sink delivers outbox messages to a pg_tide inbox on the relay database or on another PostgreSQL database. This enables cross-service messaging entirely within PostgreSQL with at-least-once delivery and durable `event_id` deduplication.

## When to Use This Sink

Choose the PostgreSQL Inbox sink when you need direct database-to-database
messaging. Messages flow from outbox in Database A to inbox in Database B with
at-least-once transport and durable `event_id` deduplication.

## Configuration

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-to-warehouse',
    'outbox', 'orders',
    'sink_type', 'inbox',
    'config', '{
        "postgres_url": "${env:WAREHOUSE_DB_URL}",
        "inbox": "incoming_orders",
        "batch_size": 100
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"inbox"` |
| `postgres_url` | string | relay database | Optional PostgreSQL URL for a remote target database |
| `inbox` | string | — | Target inbox name (must exist on the selected database) |
| `batch_size` | int | `100` | Messages per batch insert |

## Delivery Guarantees

The inbox sink provides durable destination deduplication. Each message's
`dedup_key` is used as the inbox identifier; if the same message is delivered
twice after a relay restart, the inbox's UNIQUE constraint rejects the
duplicate. The transport remains at-least-once; application processing can use
the unique `event_id` constraint to make repeated deliveries harmless.

## Troubleshooting

- **"Inbox not found"** — Create the inbox on the target database: `SELECT tide.inbox_create('incoming_orders')`
- **"Connection refused"** — Verify the target database URL is reachable from the relay
- **"Duplicate key violation"** — This should normally be absorbed by the inbox's `ON CONFLICT DO NOTHING` path; inspect schema and permissions if it surfaces as an error.

## Further Reading

- [PostgreSQL Inbox Compatibility Alias](pg-outbox.md) — For existing remote inbox configurations
- [Bidirectional Sync Tutorial](../tutorials/bidirectional-sync.md) — Using inbox sinks for two-way communication
