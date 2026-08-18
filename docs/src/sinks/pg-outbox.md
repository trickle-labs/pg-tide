# PostgreSQL Inbox Compatibility Alias

`pg_outbox` is a compatibility alias for the remote PostgreSQL inbox sink. It
does not replicate into a remote outbox and is not counted as a separate
supported destination. New configurations should use `sink_type = "inbox"`.

## When to Use This Sink

Use the alias only while migrating an existing remote inbox configuration.

## Configuration

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'replicate-to-eu',
    'outbox', 'orders',
    'sink_type', 'pg_outbox',
    'config', '{
        "postgres_url": "${env:EU_DB_URL}",
        "inbox": "orders_eu_events",
        "batch_size": 200
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"pg_outbox"` |
| `postgres_url` | string | — | PostgreSQL URL for the remote target instance |
| `inbox` | string | — | Target inbox name on the remote instance |
| `batch_size` | int | `100` | Messages per batch |

## Further Reading

- [PostgreSQL Inbox](pg-inbox.md) — For direct inbox delivery
- [Cross-Region Tutorial](../tutorials/cross-region.md) — Multi-region relay patterns
