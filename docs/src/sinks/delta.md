# Delta Lake

Delta Lake is an open-source storage framework that brings ACID transactions and scalable metadata handling to data lakes. Originally created by Databricks, Delta Lake has become a foundational technology in the Databricks ecosystem and is widely adopted beyond it. When pg_tide delivers messages to Delta Lake, your PostgreSQL events are written as Parquet files with a transaction log that enables time travel, schema enforcement, and reliable upserts.

## When to Use This Sink

Choose Delta Lake when your analytics platform is built on Databricks or Spark, when you need ACID transactions on object storage, or when you want to support both streaming and batch queries on the same dataset. Delta Lake's integration with Databricks Unity Catalog provides governance, lineage tracking, and fine-grained access control.

## Configuration

```sql
SELECT tide.relay_set_outbox(
    'events-to-delta',
    'events',
    'delta-relay',
    '{
        "sink_type": "delta",
        "table_uri": "s3://my-lake/delta/events",
        "storage_options": {
            "AWS_ACCESS_KEY_ID": "${env:AWS_ACCESS_KEY_ID}",
            "AWS_SECRET_ACCESS_KEY": "${env:AWS_SECRET_ACCESS_KEY}",
            "AWS_REGION": "us-east-1"
        },
        "batch_size": 1000
    }'::jsonb
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"delta"` |
| `table_uri` | string | — | Delta table location (S3/GCS/ADLS/local path) |
| `storage_options` | object | `{}` | Cloud storage credentials and options |
| `batch_size` | int | `1000` | Records per commit |
| `mode` | string | `"append"` | Write mode: `"append"` or `"overwrite"` |

## How It Works

Each batch of messages is written as a Parquet data file to the Delta table location. The relay then atomically commits the file to the Delta transaction log (`_delta_log/`). This ensures that readers always see complete, consistent batches. Failed or partial writes do not affect the table state.

## Delivery Guarantees

At-least-once delivery. Delta Lake's transaction log is append-only, and each commit is atomic. If the relay crashes before committing, the orphaned Parquet file is ignored. Duplicates can be handled using Delta Lake's MERGE operations or by deduplication during downstream queries.

## Troubleshooting

- **"Access denied"** — Check storage credentials in `storage_options`
- **"Table does not exist"** — Create the table first using Spark or delta-rs, or enable auto-creation
- **"Conflict during commit"** — Concurrent writers detected; relay retries automatically

## Further Reading

- [Apache Iceberg](iceberg.md) — Alternative open table format (broader engine support)
- [DuckLake](ducklake.md) — Lightweight lakehouse alternative
- [Object Storage](object-storage.md) — Raw file storage without table format
