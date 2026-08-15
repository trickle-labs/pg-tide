# Tutorial: Loading Data into a Data Lake

This tutorial demonstrates how to stream events from PostgreSQL into a data lake (S3/GCS with Apache Iceberg or Delta Lake format) for analytics. You'll set up a pipeline that continuously loads transactional data into a lakehouse architecture.

## What You'll Build

```
PostgreSQL (transactions)  →  pg_tide  →  Object Storage (S3/GCS)
                                          └── Iceberg/Delta tables
                                              └── Query with Spark/Trino/DuckDB
```

## Prerequisites

- PostgreSQL with pg_tide installed
- Object storage (AWS S3, GCS, or MinIO for local testing)
- A query engine (Trino, Spark, or DuckDB) for reading the lake

## Step 1: Create the Outbox

```sql
SELECT tide.outbox_create('analytics_events');
```

## Step 2: Configure Iceberg Sink

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'events-to-lake',
    'outbox', 'analytics_events',
    'sink_type', 'iceberg',
    'config', '{
        "catalog_type": "rest",
        "catalog_uri": "http://iceberg-rest:8181",
        "warehouse": "s3://my-lake/warehouse",
        "namespace": "raw",
        "table": "events",
        "s3_endpoint": "https://s3.amazonaws.com",
        "s3_region": "us-east-1",
        "aws_access_key_id": "${env:AWS_ACCESS_KEY_ID}",
        "aws_secret_access_key": "${env:AWS_SECRET_ACCESS_KEY}",
        "partition_by": ["year", "month", "event_type"],
        "commit_interval_seconds": 60
    }'::jsonb
  )
);
```

### Alternative: Delta Lake

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'events-to-delta',
    'outbox', 'analytics_events',
    'sink_type', 'delta',
    'config', '{
        "table_uri": "s3://my-lake/delta/events",
        "partition_columns": ["year", "month"],
        "aws_access_key_id": "${env:AWS_ACCESS_KEY_ID}",
        "aws_secret_access_key": "${env:AWS_SECRET_ACCESS_KEY}",
        "target_file_size_mb": 128
    }'::jsonb
  )
);
```

### Alternative: Raw Object Storage (Parquet)

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'events-to-parquet',
    'outbox', 'analytics_events',
    'sink_type', 'object_storage',
    'config', '{
        "provider": "s3",
        "bucket": "my-lake",
        "prefix": "raw/events/",
        "format": "parquet",
        "partition_template": "year={year}/month={month}/day={day}/",
        "file_rotation_seconds": 300,
        "file_rotation_rows": 100000,
        "aws_access_key_id": "${env:AWS_ACCESS_KEY_ID}",
        "aws_secret_access_key": "${env:AWS_SECRET_ACCESS_KEY}"
    }'::jsonb
  )
);
```

## Step 3: Publish Events

```sql
-- Your application publishes events as part of normal transactions
BEGIN;
INSERT INTO orders (id, customer_id, total) VALUES ('ORD-001', 'CUST-42', 299.99);

SELECT tide.outbox_publish('analytics_events', 'orders', jsonb_build_object(
    'event_type', 'order_created',
    'order_id', 'ORD-001',
    'customer_id', 'CUST-42',
    'total', 299.99,
    'year', extract(year from now())::int,
    'month', extract(month from now())::int
));
COMMIT;
```

## Step 4: Start the Relay

```bash
pg-tide --postgres-url "postgres://user:pass@localhost/mydb"
```

## Step 5: Query the Lake

### With Trino

```sql
SELECT event_type, count(*), sum(cast(json_extract_scalar(payload, '$.total') as decimal))
FROM iceberg.raw.events
WHERE year = 2024 AND month = 6
GROUP BY event_type;
```

### With DuckDB

```sql
SELECT event_type, count(*), sum(payload->>'total')
FROM read_parquet('s3://my-lake/raw/events/**/*.parquet')
GROUP BY event_type;
```

## Choosing a Lake Format

| Format | Best For | Ecosystem |
|--------|----------|-----------|
| **Iceberg** | Large-scale analytics, schema evolution | Spark, Trino, Flink, Snowflake |
| **Delta Lake** | Databricks ecosystem, ACID transactions | Spark, Databricks, DuckDB |
| **Parquet files** | Simple analytics, maximum compatibility | Everything |

## Key Considerations

- **Commit interval:** Controls how frequently data becomes queryable. 60s gives near-real-time; 300s reduces small file overhead.
- **Partitioning:** Partition by time (year/month/day) and optionally by event type for efficient pruning.
- **File size:** Target 128-256 MB files for optimal query performance.

## Further Reading

- [Sinks: Iceberg](../sinks/iceberg.md) — Apache Iceberg configuration
- [Sinks: Delta](../sinks/delta.md) — Delta Lake configuration
- [Sinks: Object Storage](../sinks/object-storage.md) — Raw file storage
