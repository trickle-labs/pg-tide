# PostgreSQL Inbox

The PostgreSQL Inbox sink delivers outbox messages from one pg_tide instance to an inbox on another PostgreSQL database (or the same database). This enables cross-service messaging entirely within PostgreSQL — no external message broker required. When your architecture consists of multiple services that each have their own PostgreSQL database with pg_tide installed, the inbox sink provides reliable, deduplicated message delivery between them.

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
        "target_url": "${env:WAREHOUSE_DB_URL}",
        "inbox_name": "incoming_orders",
        "batch_size": 100
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"inbox"` |
| `target_url` | string | — | PostgreSQL connection URL for the target database |
| `inbox_name` | string | — | Target inbox name (must exist on the target database) |
| `batch_size` | int | `100` | Messages per batch insert |

## Delivery Guarantees

The inbox sink provides durable destination deduplication. Each message's
`dedup_key` is used as the inbox identifier; if the same message is delivered
twice after a relay restart, the inbox's UNIQUE constraint rejects the
duplicate. Transactional application processing can therefore be effectively
exactly once.

## Troubleshooting

- **"Inbox not found"** — Create the inbox on the target database: `SELECT tide.inbox_create('incoming_orders')`
- **"Connection refused"** — Verify the target database URL is reachable from the relay
- **"Duplicate key violation"** — This is expected behavior (deduplication working correctly); the relay handles this gracefully

## Further Reading

- [Remote PostgreSQL Outbox](pg-outbox.md) — For outbox-to-outbox federation
- [Bidirectional Sync Tutorial](../tutorials/bidirectional-sync.md) — Using inbox sinks for two-way communication
