# Source: Airbyte

The Airbyte source runs Airbyte source connectors (as Docker containers) and delivers extracted records into a pg_tide inbox. This provides access to approximately 400 data sources from the Airbyte connector catalog.

## Configuration

```sql
SELECT tide.relay_set_inbox(
    'salesforce-data',
    'crm_inbox',
    '{
        "source_type": "airbyte",
        "source_image": "airbyte/source-salesforce:latest",
        "source_config": {
            "client_id": "${env:SF_CLIENT_ID}",
            "client_secret": "${env:SF_CLIENT_SECRET}",
            "refresh_token": "${env:SF_REFRESH_TOKEN}"
        },
        "streams": ["contacts", "opportunities"]
    }'::jsonb
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source_type` | string | — | Must be `"airbyte"` |
| `source_image` | string | — | Docker image for the Airbyte source connector |
| `source_config` | object | `{}` | Connector configuration |
| `streams` | array | `null` | Streams to sync (null = all discovered) |
| `sync_mode` | string | `"incremental"` | Sync mode: `"incremental"` or `"full_refresh"` |

## Further Reading

- [Sinks: Airbyte](../sinks/airbyte.md) — Airbyte destination connectors
- [Singer Source](singer.md) — Alternative connector ecosystem (no Docker needed)
