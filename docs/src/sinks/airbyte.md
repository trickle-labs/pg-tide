# Airbyte

Airbyte is an open-source data integration platform with approximately 400 connectors for moving data between systems. When pg_tide uses the Airbyte sink, it streams your outbox messages through Airbyte destination connectors, providing access to a vast ecosystem of data loading targets. Airbyte connectors run as Docker containers with a standardized protocol, making them portable and well-tested.

## When to Use This Sink

Choose the Airbyte sink when your target destination has an Airbyte connector but not a native pg_tide sink, when you are already invested in the Airbyte ecosystem, or when you need the protocol's built-in state management and catalog discovery features. Airbyte connectors are particularly strong for loading data into data warehouses and SaaS tools.

## Configuration

```sql
SELECT tide.relay_set_outbox(
    'events-to-destination',
    'events',
    'airbyte-relay',
    '{
        "sink_type": "airbyte",
        "destination_image": "airbyte/destination-bigquery:latest",
        "destination_config": {
            "project_id": "${env:GCP_PROJECT}",
            "dataset_id": "events",
            "credentials_json": "${env:GCP_CREDS_JSON}"
        },
        "batch_size": 500
    }'::jsonb
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"airbyte"` |
| `destination_image` | string | — | Docker image for the Airbyte destination connector |
| `destination_config` | object | `{}` | Connector-specific configuration |
| `batch_size` | int | `500` | Records per state checkpoint |
| `catalog` | object | `null` | Airbyte catalog (auto-generated if not provided) |

## How It Works

The relay launches the Airbyte destination connector as a Docker container and communicates via the Airbyte protocol over stdin/stdout. Messages are sent as Airbyte RECORD messages, with periodic STATE messages for checkpointing. The relay manages the container lifecycle, handles restarts, and persists state for resumable syncs.

## Troubleshooting

- **"Docker image not found"** — Pull the image first: `docker pull airbyte/destination-bigquery:latest`
- **"Container exited with error"** — Check connector logs for configuration issues
- **"Docker not available"** — The relay host needs Docker installed and the relay user needs Docker socket access

## Further Reading

- [Singer / Meltano](singer.md) — Alternative connector ecosystem (no Docker required)
- [Fivetran](fivetran.md) — Enterprise managed alternative
