# BigQuery

Google BigQuery is a serverless, highly scalable data warehouse that can analyze petabytes of data with standard SQL. Unlike traditional warehouses that require provisioning, BigQuery separates storage from compute and charges only for queries run and data stored. When pg_tide delivers messages to BigQuery, your PostgreSQL events become immediately available for analytics, machine learning (via BigQuery ML), and visualization in Looker or Data Studio.

## When to Use This Sink

Choose BigQuery when your analytics stack runs on Google Cloud, when you want truly serverless analytics with no capacity planning, or when you need to combine PostgreSQL event data with other GCP datasets. BigQuery's streaming insert API makes events queryable within seconds of delivery, and its columnar storage format provides excellent compression and query performance for analytical workloads.

## Configuration

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'events-to-bigquery',
    'outbox', 'events',
    'sink_type', 'bigquery',
    'config', '{
        "project_id": "${env:GCP_PROJECT_ID}",
        "dataset": "event_analytics",
        "table": "raw_events",
        "credentials_json": "${file:/etc/gcp/service-account.json}",
        "batch_size": 500
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"bigquery"` |
| `project_id` | string | — | GCP project ID |
| `dataset` | string | — | BigQuery dataset name |
| `table` | string | — | BigQuery table name |
| `credentials_json` | string | `null` | Service account JSON (falls back to ADC) |
| `batch_size` | int | `500` | Rows per streaming insert batch (max 10,000) |
| `insert_method` | string | `"streaming"` | Insert method: `"streaming"` or `"load"` |

## Insert Methods

- **Streaming inserts** (default): Events are queryable within seconds. Best for real-time analytics. Incurs streaming insert pricing.
- **Load jobs**: Events are batched into files and loaded periodically. Lower cost but higher latency (minutes). Best for cost-sensitive batch analytics.

## Table Schema

```sql
CREATE TABLE event_analytics.raw_events (
    event_id STRING,
    outbox_name STRING,
    payload JSON,
    dedup_key STRING,
    operation STRING,
    published_at TIMESTAMP
)
PARTITION BY DATE(published_at)
CLUSTER BY outbox_name, operation;
```

Partitioning by date and clustering by common filter columns optimizes both cost and query performance.

## Troubleshooting

- **"Access Denied"** — Service account needs `roles/bigquery.dataEditor` on the dataset
- **"Table not found"** — Create the table and dataset before starting the pipeline
- **"Streaming insert quota exceeded"** — BigQuery has per-table streaming limits; use load jobs for very high volumes
- **"Invalid rows"** — Schema mismatch between event structure and table schema

## Further Reading

- [Snowflake](snowflake.md) — Multi-cloud data warehouse alternative
- [Google Cloud Pub/Sub](pubsub.md) — GCP messaging for intermediate processing
