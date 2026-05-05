# Remote PostgreSQL Outbox

The Remote PostgreSQL Outbox sink delivers messages from one outbox to another outbox on a different PostgreSQL instance. This enables multi-cluster federation — messages published in one data center or region can be forwarded to outboxes in other regions, where local relays deliver them to local consumers. This creates a hierarchical event distribution topology.

## When to Use This Sink

Choose this sink for multi-region deployments where events published in one region need to be available to consumers in other regions, or for organizational boundaries where different teams manage separate PostgreSQL clusters but need shared event streams.

## Configuration

```sql
SELECT tide.relay_set_outbox(
    'replicate-to-eu',
    'orders',
    'federation-relay',
    '{
        "sink_type": "pg_outbox",
        "target_url": "${env:EU_DB_URL}",
        "target_outbox": "orders_eu_replica",
        "batch_size": 200
    }'::jsonb
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"pg_outbox"` |
| `target_url` | string | — | PostgreSQL URL for the target instance |
| `target_outbox` | string | — | Target outbox name |
| `batch_size` | int | `100` | Messages per batch |

## Further Reading

- [PostgreSQL Inbox](pg-inbox.md) — For direct inbox delivery
- [Cross-Region Tutorial](../tutorials/cross-region.md) — Multi-region event relay patterns
