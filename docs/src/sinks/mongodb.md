# MongoDB

MongoDB is a document-oriented database that stores data as flexible JSON-like documents. It is widely used for applications that need schema flexibility, horizontal scaling, and the ability to store nested, hierarchical data naturally. When pg_tide delivers messages to MongoDB, your PostgreSQL events are written as documents to a collection, where they can be queried with MongoDB's rich query language, aggregated for analytics, or used to maintain a read-optimized view of your data.

## When to Use This Sink

Choose MongoDB when your downstream consumers use MongoDB as their primary data store, when you want to maintain a denormalized read model of your PostgreSQL data in a document database, or when you need flexible schema storage for event data that evolves rapidly. MongoDB's document model maps naturally to JSON event payloads without requiring a predefined schema.

## Configuration

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'events-to-mongo',
    'outbox', 'events',
    'sink_type', 'mongodb',
    'config', '{
        "connection_string": "${env:MONGODB_URI}",
        "database": "events",
        "collection": "outbox_events",
        "batch_size": 500,
        "write_concern": "majority"
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"mongodb"` |
| `connection_string` | string | — | MongoDB connection URI |
| `database` | string | — | Target database |
| `collection` | string | — | Target collection |
| `batch_size` | int | `500` | Documents per bulk write |
| `write_concern` | string | `"majority"` | Write concern level |
| `upsert` | bool | `false` | Use upsert mode (update if exists, insert if not) |
| `upsert_key` | string | `"dedup_key"` | Field used as document ID for upserts |

## Upsert Mode

When `upsert: true`, the relay uses the `dedup_key` as the document `_id`. This provides natural deduplication — re-delivered messages update the existing document rather than creating duplicates. This is particularly useful for maintaining a current-state view of entities:

```json
{
    "sink_type": "mongodb",
    "connection_string": "mongodb+srv://...",
    "database": "orders",
    "collection": "current_state",
    "upsert": true,
    "upsert_key": "dedup_key"
}
```

## Delivery Guarantees

With `write_concern: "majority"`, MongoDB acknowledges writes only after they are replicated to a majority of replica set members, providing durable at-least-once delivery. With upsert mode, re-delivery is idempotent.

## Troubleshooting

- **"Authentication failed"** — Check credentials in the connection string and verify the user has write access
- **"Connection timeout"** — Verify network connectivity; for Atlas, ensure your IP is in the access list
- **"Write concern timeout"** — Replica set members are unavailable; check cluster health

## Further Reading

- [Elasticsearch](elasticsearch.md) — For full-text search use cases
- [ClickHouse](clickhouse.md) — For columnar analytics
