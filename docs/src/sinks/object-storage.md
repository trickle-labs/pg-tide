# Object Storage (S3 / GCS / Azure Blob)

Object storage services like Amazon S3, Google Cloud Storage (GCS), and Azure Blob Storage provide virtually unlimited, highly durable data storage at very low cost. When pg_tide delivers messages to object storage, your events are written as files (JSONL or Parquet format) organized in a path structure you define. This creates a data lake landing zone that can be queried by tools like Athena, BigQuery, Trino, DuckDB, or Spark without requiring a dedicated streaming infrastructure.

## When to Use This Sink

Choose object storage when you need cost-effective long-term archival of events, when you want to build a data lake without committing to a specific table format (Iceberg/Delta), when compliance requires immutable event retention, or when your analytical tools can query files directly from object storage. Object storage is the most cost-effective option for high-volume event archival.

## Configuration

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'events-to-s3',
    'outbox', 'events',
    'sink_type', 'object_storage',
    'config', '{
        "provider": "s3",
        "bucket": "my-data-lake",
        "prefix": "events/{stream_table}/year={year}/month={month}/day={day}/",
        "format": "parquet",
        "region": "us-east-1",
        "batch_size": 1000,
        "file_rotation_seconds": 300
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"object_storage"` |
| `provider` | string | — | Storage provider: `"s3"`, `"gcs"`, `"azure"` |
| `bucket` | string | — | Bucket/container name |
| `prefix` | string | `""` | Path prefix template. Supports `{stream_table}`, `{year}`, `{month}`, `{day}`, `{hour}` |
| `format` | string | `"jsonl"` | File format: `"jsonl"` or `"parquet"` |
| `region` | string | `null` | Cloud region |
| `access_key_id` | string | `null` | Access key (falls back to default credential chain) |
| `secret_access_key` | string | `null` | Secret key |
| `batch_size` | int | `1000` | Records per file |
| `file_rotation_seconds` | int | `300` | Maximum seconds before a file is finalized and uploaded |
| `compression` | string | `null` | Compression for JSONL: `"gzip"`, `"zstd"` |

## File Formats

### JSONL (JSON Lines)

One JSON object per line. Human-readable, easy to process with standard tools:

```
{"event_id":"abc","payload":{"order_id":"ord-1"},"op":"insert","ts":"2024-01-15T10:30:00Z"}
{"event_id":"def","payload":{"order_id":"ord-2"},"op":"insert","ts":"2024-01-15T10:30:01Z"}
```

### Parquet

Columnar binary format optimized for analytics. 10-50x compression vs. raw JSON, and dramatically faster query performance for analytical workloads. Use Parquet when the data will be queried by analytics engines.

## Path Partitioning

The `prefix` template creates a Hive-style partitioned layout that analytical engines recognize automatically:

```
s3://my-lake/events/orders/year=2024/month=01/day=15/batch-001.parquet
s3://my-lake/events/orders/year=2024/month=01/day=15/batch-002.parquet
```

This enables partition pruning — queries that filter by date only read the relevant files, dramatically reducing scan costs.

## Troubleshooting

- **"Access Denied"** — Check IAM permissions: `s3:PutObject` on the bucket/prefix
- **"Bucket not found"** — Verify bucket name and region
- **Large files / memory pressure** — Reduce `batch_size` or `file_rotation_seconds`
- **Query engine can't read files** — Ensure format matches what the engine expects; check compression codec

## Further Reading

- [Apache Iceberg](iceberg.md) — Add ACID transactions and time travel on top of object storage
- [Delta Lake](delta.md) — Alternative table format for object storage
- [Snowflake](snowflake.md) — Query object storage files from Snowflake external tables
