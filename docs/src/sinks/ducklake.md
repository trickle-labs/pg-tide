# DuckLake

DuckLake is a novel lakehouse architecture that combines Parquet data files with a PostgreSQL metadata catalog. Unlike Iceberg or Delta Lake (which store metadata as JSON files alongside data), DuckLake uses a relational database (PostgreSQL) as the source of truth for table metadata, schema, and transaction history. This design makes metadata operations (listing tables, schema evolution, time travel) dramatically faster while keeping data in efficient Parquet format on any object storage.

When pg_tide delivers messages to DuckLake, your events are written as Parquet files while the catalog metadata is maintained in PostgreSQL — potentially even in the same PostgreSQL instance that hosts your outbox. This creates an elegantly simple architecture where your database manages both the events and their analytical storage metadata.

## When to Use This Sink

Choose DuckLake when you want a lightweight lakehouse that integrates naturally with PostgreSQL, when you want to query event data with DuckDB (including from the command line or embedded in applications), or when you prefer the simplicity of a single PostgreSQL database managing both operational and analytical metadata. DuckLake is particularly compelling for smaller teams that want lakehouse capabilities without the operational complexity of running a separate catalog service.

## Configuration

```sql
SELECT tide.relay_set_outbox(
    'events-to-ducklake',
    'events',
    'ducklake-relay',
    '{
        "sink_type": "ducklake",
        "catalog_url": "postgresql://localhost:5432/analytics",
        "data_path": "s3://my-lake/ducklake/events",
        "table": "raw_events",
        "batch_size": 1000,
        "storage_options": {
            "AWS_REGION": "us-east-1"
        }
    }'::jsonb
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"ducklake"` |
| `catalog_url` | string | — | PostgreSQL connection URL for the DuckLake catalog |
| `data_path` | string | — | Storage path for Parquet data files (S3/GCS/local) |
| `table` | string | — | DuckLake table name |
| `batch_size` | int | `1000` | Records per Parquet file |
| `storage_options` | object | `{}` | Cloud storage credentials |

## Querying DuckLake Data

Once events are written, you can query them with DuckDB:

```sql
-- Attach the DuckLake catalog
ATTACH 'ducklake:postgresql://localhost:5432/analytics' AS lake;

-- Query your events
SELECT * FROM lake.raw_events
WHERE event_type = 'order.created'
  AND published_at > '2024-01-01';
```

## Troubleshooting

- **"Catalog connection failed"** — Verify the PostgreSQL URL is reachable from the relay
- **"Storage access denied"** — Check cloud storage credentials in `storage_options`
- **"Table not found"** — Create the DuckLake table first using DuckDB

## Further Reading

- [Apache Iceberg](iceberg.md) — For broader engine ecosystem support
- [Delta Lake](delta.md) — For Databricks/Spark integration
