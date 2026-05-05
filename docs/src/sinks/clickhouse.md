# ClickHouse

ClickHouse is an open-source columnar database management system designed for real-time analytical queries on large datasets. It can process billions of rows per second, making it one of the fastest analytical databases available. When pg_tide delivers messages to ClickHouse, your PostgreSQL events become immediately queryable for real-time dashboards, log analytics, time-series analysis, and business intelligence workloads.

Unlike traditional message queues where data is consumed and deleted, ClickHouse stores your events permanently (or until you define a TTL), letting you run ad-hoc analytical queries across your entire event history. This makes it an excellent complement to pg_tide — your outbox provides reliable event delivery, and ClickHouse provides the analytical query engine.

## When to Use This Sink

Choose ClickHouse when you need real-time analytics on your PostgreSQL events, when you want sub-second query performance on billions of rows, or when you are building observability platforms (log storage, metrics, traces). ClickHouse excels at time-series data, aggregation queries, and full-text search across structured event data. It is particularly cost-effective for high-volume workloads because its columnar compression achieves 10-50x data reduction.

Consider Snowflake or BigQuery if you prefer fully managed cloud services with zero operations, or Elasticsearch if your primary need is full-text search with fuzzy matching.

## Configuration

### Minimal Configuration

```sql
SELECT tide.relay_set_outbox(
    'events-to-clickhouse',
    'events',
    'clickhouse-relay',
    '{
        "sink_type": "clickhouse",
        "url": "http://localhost:8123",
        "database": "analytics",
        "table": "events"
    }'::jsonb
);
```

### Production Configuration

```sql
SELECT tide.relay_set_outbox(
    'events-to-clickhouse',
    'events',
    'clickhouse-relay',
    '{
        "sink_type": "clickhouse",
        "url": "https://${env:CLICKHOUSE_HOST}:8443",
        "database": "analytics",
        "table": "events",
        "username": "${env:CLICKHOUSE_USER}",
        "password": "${env:CLICKHOUSE_PASSWORD}",
        "batch_size": 1000,
        "tls_enabled": true
    }'::jsonb
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"clickhouse"` |
| `url` | string | — | ClickHouse HTTP interface URL |
| `database` | string | — | Target database |
| `table` | string | — | Target table |
| `username` | string | `"default"` | Authentication username |
| `password` | string | `""` | Authentication password |
| `batch_size` | int | `1000` | Rows per INSERT batch |
| `tls_enabled` | bool | `false` | Enable TLS |

## Table Schema

The relay inserts messages as structured rows. Create a ClickHouse table that matches your event schema:

```sql
CREATE TABLE analytics.events (
    event_id String,
    outbox_name String,
    event_type String,
    payload String,  -- JSON string
    dedup_key String,
    created_at DateTime64(3),
    op String
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (event_type, created_at)
TTL created_at + INTERVAL 90 DAY;
```

The `ReplacingMergeTree` engine automatically deduplicates rows with the same sort key during background merges, providing eventual deduplication even if the relay delivers a message twice.

## Delivery Guarantees

ClickHouse provides at-least-once delivery. The relay uses batch INSERT operations and commits offsets only after ClickHouse confirms the insert succeeded. Using `ReplacingMergeTree` or `dedup_key` checks in your queries provides idempotent behavior.

## Performance Tuning

ClickHouse performs best with large batch inserts (1,000+ rows). Small, frequent inserts create many small parts that require background merging. Configure:

- **`batch_size: 1000-5000`** — Larger batches are more efficient for ClickHouse
- Adjust the relay's polling interval to accumulate larger batches during high throughput

## Troubleshooting

- **"Table not found"** — Create the target table in ClickHouse before starting the pipeline
- **"Column count mismatch"** — Ensure the ClickHouse table schema matches the fields the relay produces
- **"Too many parts"** — Batch size is too small; increase `batch_size` to reduce insert frequency
- **"Authentication failed"** — Check username/password and that the user has INSERT permission

## Further Reading

- [Snowflake](snowflake.md) — Cloud data warehouse alternative
- [BigQuery](bigquery.md) — Google Cloud analytics alternative
- [Wire Formats](../wire-formats/overview.md) — Customize how events are structured for ClickHouse
