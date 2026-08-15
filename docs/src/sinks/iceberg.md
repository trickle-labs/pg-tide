# Apache Iceberg

Apache Iceberg is an open table format for large-scale analytical datasets, designed to bring reliability and simplicity to data lakes. Unlike raw files on object storage, Iceberg provides ACID transactions, schema evolution, time travel, and partition evolution — features traditionally associated with data warehouses, but available on open storage like S3, GCS, and ADLS. When pg_tide delivers messages to Iceberg, your PostgreSQL events become part of a queryable lakehouse that can be accessed by Spark, Trino, Flink, Snowflake, BigQuery, and dozens of other engines.

## When to Use This Sink

Choose Apache Iceberg when you want the cost efficiency of object storage with the reliability of a data warehouse, when you need multi-engine access to the same data (Spark for ETL, Trino for ad-hoc queries, Flink for streaming), or when vendor lock-in is a concern and you prefer open formats. Iceberg is the foundation of the modern lakehouse architecture and is supported by all major cloud providers and query engines.

## Configuration

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'events-to-iceberg',
    'outbox', 'events',
    'sink_type', 'iceberg',
    'config', '{
        "catalog_type": "rest",
        "catalog_uri": "${env:ICEBERG_CATALOG_URI}",
        "warehouse": "s3://my-lake/warehouse",
        "namespace": "analytics",
        "table": "events",
        "batch_size": 1000
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"iceberg"` |
| `catalog_type` | string | — | Catalog type: `"rest"`, `"glue"`, `"hive"` |
| `catalog_uri` | string | — | Catalog service URI |
| `warehouse` | string | — | Storage location (S3/GCS/ADLS path) |
| `namespace` | string | — | Iceberg namespace (database) |
| `table` | string | — | Iceberg table name |
| `batch_size` | int | `1000` | Records per data file |
| `s3_access_key_id` | string | `null` | S3 credentials (falls back to default chain) |
| `s3_secret_access_key` | string | `null` | S3 secret key |
| `s3_region` | string | `null` | S3 region |

## Catalog Types

- **REST Catalog** — The most portable option. Works with Tabular, Polaris, and any REST-compatible catalog.
- **AWS Glue** — Native integration with AWS analytics services (Athena, EMR, Redshift Spectrum).
- **Hive Metastore** — For Hadoop-based environments with existing Hive infrastructure.

## How It Works

The relay accumulates messages into batches and writes them as Parquet data files to object storage. Each batch becomes an Iceberg append commit, maintaining full ACID transactional semantics. This means:

- Partial writes never become visible (atomic commits)
- Concurrent readers always see a consistent snapshot
- Failed writes are automatically cleaned up
- Time travel lets you query the state at any point in history

## Delivery Guarantees

At-least-once delivery. If the relay restarts mid-batch, the uncommitted data files are orphaned and cleaned up by Iceberg's periodic orphan file removal. The re-delivered messages create a new commit. For exact deduplication, include the `dedup_key` as a column and deduplicate at query time.

## Debezium Compatibility

When combined with the [Debezium wire format](../wire-formats/debezium.md), the Iceberg sink produces CDC-compatible records that standard Iceberg CDC consumers (like the Iceberg Flink connector) can process for upsert/delete semantics:

```json
{
    "sink_type": "iceberg",
    "wire_format": "debezium",
    "catalog_type": "rest",
    "catalog_uri": "http://catalog:8181",
    "namespace": "cdc",
    "table": "orders"
}
```

## Troubleshooting

- **"Catalog not found"** — Verify `catalog_uri` is reachable and the catalog service is running
- **"Namespace/Table not found"** — Create the table first using Spark, Trino, or the catalog API
- **"Access denied to storage"** — Check S3/GCS/ADLS credentials and bucket policies
- **"Commit conflict"** — Another writer committed concurrently; the relay will retry automatically

## Further Reading

- [Delta Lake](delta.md) — Alternative open table format (Databricks ecosystem)
- [DuckLake](ducklake.md) — Lightweight lakehouse with PostgreSQL catalog
- [Object Storage](object-storage.md) — Raw file storage without table format
