# Fivetran HVR

Fivetran is an enterprise data integration platform that automates data pipelines from sources to destinations. The Fivetran HVR sink exposes a webhook-compatible endpoint that speaks Fivetran's HVR (High Volume Replication) format, allowing pg_tide to deliver events in a format that Fivetran's infrastructure can consume and route to any Fivetran-supported destination.

## When to Use This Sink

Choose the Fivetran sink when your organization uses Fivetran as its primary data integration platform and you want pg_tide events to flow through Fivetran's managed pipeline infrastructure for delivery to final destinations.

## Configuration

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'events-to-fivetran',
    'outbox', 'events',
    'sink_type', 'fivetran',
    'config', '{
        "endpoint_url": "${env:FIVETRAN_WEBHOOK_URL}",
        "api_key": "${env:FIVETRAN_API_KEY}",
        "api_secret": "${env:FIVETRAN_API_SECRET}",
        "batch_size": 100
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"fivetran"` |
| `endpoint_url` | string | — | Fivetran HVR endpoint URL |
| `api_key` | string | — | Fivetran API key |
| `api_secret` | string | — | Fivetran API secret |
| `batch_size` | int | `100` | Records per webhook batch |

## Further Reading

- [Singer / Meltano](singer.md) — Open-source alternative with ~500 connectors
- [Airbyte](airbyte.md) — Open-source alternative with ~400 connectors
