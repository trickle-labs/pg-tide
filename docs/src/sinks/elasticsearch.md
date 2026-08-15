# Elasticsearch / OpenSearch

Elasticsearch is a distributed search and analytics engine built on Apache Lucene. It excels at full-text search, log analytics, application performance monitoring (APM), and real-time data exploration. OpenSearch is an AWS-maintained fork with identical functionality. When pg_tide delivers messages to Elasticsearch, your PostgreSQL events become searchable — enabling full-text queries, aggregations, dashboards (via Kibana/OpenSearch Dashboards), and real-time alerting on your event data.

## When to Use This Sink

Choose Elasticsearch when you need full-text search across your events (e.g., searching order descriptions, user messages, log entries), when you are building observability dashboards with Kibana, or when you need real-time aggregations and alerting on high-volume event streams. Elasticsearch's inverted index makes text search blazing fast, while its aggregation framework supports complex analytics.

## Configuration

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'events-to-elastic',
    'outbox', 'events',
    'sink_type', 'elasticsearch',
    'config', '{
        "url": "https://${env:ELASTIC_HOST}:9200",
        "index": "events-{stream_table}",
        "username": "${env:ELASTIC_USER}",
        "password": "${env:ELASTIC_PASS}",
        "batch_size": 500,
        "document_id": "{dedup_key}"
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"elasticsearch"` |
| `url` | string | — | Elasticsearch/OpenSearch URL |
| `index` | string | — | Target index name. Supports templates |
| `username` | string | `null` | Basic auth username |
| `password` | string | `null` | Basic auth password |
| `api_key` | string | `null` | API key authentication (alternative to basic auth) |
| `batch_size` | int | `500` | Documents per bulk request |
| `document_id` | string | `null` | Document ID template. Enables idempotent upserts |
| `tls_enabled` | bool | `true` | Enable TLS |
| `tls_ca_cert` | string | `null` | Custom CA certificate |

## Document IDs and Idempotency

Setting `document_id` to `{dedup_key}` makes writes idempotent — if the same message is delivered twice, it overwrites the same document rather than creating a duplicate. This is strongly recommended for production.

## Index Lifecycle Management (ILM)

For high-volume event streams, use time-based indices with ILM policies:

```json
{"index": "events-{stream_table}-2024.01"}
```

Configure Elasticsearch ILM to roll over indices by size or age, and delete old indices after your retention period.

## Troubleshooting

- **"Connection refused"** — Verify Elasticsearch is running and the URL includes the correct port
- **"Authentication failed"** — Check username/password or API key
- **"Index not found"** — Elasticsearch auto-creates indices by default; if disabled, create the index first
- **"Bulk request failed"** — Check individual error messages; common causes are mapping conflicts or disk space

## Further Reading

- [ClickHouse](clickhouse.md) — For columnar analytics (faster aggregations, no full-text search)
- [MongoDB](mongodb.md) — For document storage without search focus
