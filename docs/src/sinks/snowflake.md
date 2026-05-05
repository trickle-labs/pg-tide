# Snowflake

Snowflake is a cloud-native data warehouse that separates compute from storage, allowing you to scale query processing independently of data volume. It runs on AWS, Azure, and GCP, providing a single platform for data warehousing, data lakes, and data sharing across clouds. When pg_tide delivers messages to Snowflake, your PostgreSQL events flow directly into your data warehouse for analytics, reporting, and machine learning without requiring intermediate ETL pipelines.

## When to Use This Sink

Choose Snowflake when your organization uses it as the primary data warehouse for analytics and BI, when you need to combine PostgreSQL event data with other data sources already in Snowflake, or when you want zero-maintenance analytical storage that scales automatically. Snowflake's semi-structured data support (VARIANT type) handles JSON event payloads natively, making it easy to query nested event data without predefined schemas.

## Configuration

### Production Configuration

```sql
SELECT tide.relay_set_outbox(
    'events-to-snowflake',
    'events',
    'snowflake-relay',
    '{
        "sink_type": "snowflake",
        "account": "${env:SNOWFLAKE_ACCOUNT}",
        "database": "ANALYTICS",
        "schema": "EVENTS",
        "table": "RAW_EVENTS",
        "warehouse": "INGEST_WH",
        "username": "${env:SNOWFLAKE_USER}",
        "private_key_path": "${env:SNOWFLAKE_KEY_PATH}",
        "batch_size": 500,
        "stage": "pg_tide_stage"
    }'::jsonb
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"snowflake"` |
| `account` | string | — | Snowflake account identifier |
| `database` | string | — | Target database |
| `schema` | string | — | Target schema |
| `table` | string | — | Target table |
| `warehouse` | string | — | Compute warehouse for COPY operations |
| `username` | string | — | Authentication username |
| `password` | string | `null` | Password (use key-pair auth instead for production) |
| `private_key_path` | string | `null` | Path to RSA private key for key-pair authentication |
| `stage` | string | `null` | Internal stage name for COPY operations |
| `batch_size` | int | `500` | Rows per micro-batch |
| `role` | string | `null` | Snowflake role to assume |

## Authentication

For production, use key-pair authentication rather than passwords:

1. Generate a key pair: `openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowflake_key.p8 -nocrypt`
2. Assign the public key to your Snowflake user: `ALTER USER relay_user SET RSA_PUBLIC_KEY='...'`
3. Reference the private key in your config: `"private_key_path": "/etc/snowflake/key.p8"`

## How It Works

The relay uses a stage-based approach for efficient loading:
1. Messages are accumulated into micro-batches
2. Each batch is written as a compressed file to an internal Snowflake stage
3. A COPY INTO command loads the staged file into the target table
4. The stage file is removed after successful loading

This approach is more cost-effective than streaming inserts because Snowflake charges per-compute-second, and batch loading uses minimal warehouse time.

## Table Schema

```sql
CREATE TABLE ANALYTICS.EVENTS.RAW_EVENTS (
    event_id VARCHAR,
    outbox_name VARCHAR,
    payload VARIANT,        -- Stores JSON natively
    dedup_key VARCHAR,
    operation VARCHAR,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

## Cost Optimization

- Use an X-Small warehouse for ingestion (sufficient for most event volumes)
- Set warehouse auto-suspend to 60 seconds to minimize idle costs
- Batch sizes of 500-1000 reduce the number of COPY operations
- Consider using Snowpipe for continuous micro-batch loading in very high volume scenarios

## Troubleshooting

- **"Warehouse not running"** — Ensure the warehouse is set to auto-resume, or start it manually
- **"Insufficient privileges"** — Grant USAGE on warehouse, INSERT on table, WRITE on stage
- **"Key-pair authentication failed"** — Verify the public key is assigned to the Snowflake user and the private key path is correct

## Further Reading

- [BigQuery](bigquery.md) — Google Cloud alternative
- [ClickHouse](clickhouse.md) — Self-hosted real-time analytics alternative
- [Object Storage](object-storage.md) — Raw file-based data lake landing
