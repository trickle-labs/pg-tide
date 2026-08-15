# Feature: Airbyte Protocol Support

pg_tide implements the [Airbyte protocol](https://docs.airbyte.com/understanding-airbyte/airbyte-protocol/) for running Airbyte source and destination connectors. This gives you access to approximately 400 data connectors from the Airbyte catalog, each packaged as a Docker container with a standardized interface.

## What is the Airbyte Protocol?

The Airbyte protocol defines how source connectors (extractors) and destination connectors (loaders) communicate. Connectors are Docker containers that read configuration from a JSON file and exchange messages via stdout/stdin:

- **AirbyteRecordMessage** — A single data row
- **AirbyteStateMessage** — Sync checkpoint for incremental mode
- **AirbyteCatalogMessage** — Available streams and their schemas
- **AirbyteLogMessage** — Connector log output

## pg_tide as an Airbyte Source Host

pg_tide can run Airbyte source connectors and ingest their records into an inbox:

```sql
SELECT tide.relay_set_inbox_v2(
  jsonb_build_object(
    'name', 'salesforce-data',
    'inbox', 'crm_inbox',
    'source', 'airbyte',
    'config', '{
        "source_image": "airbyte/source-salesforce:latest",
        "source_config": {
            "client_id": "${env:SF_CLIENT_ID}",
            "client_secret": "${env:SF_CLIENT_SECRET}",
            "refresh_token": "${env:SF_REFRESH_TOKEN}"
        },
        "streams": ["contacts", "opportunities"],
        "sync_mode": "incremental"
    }'::jsonb
  )
);
```

See [Sources: Airbyte](../sources/airbyte.md) for full configuration.

## pg_tide as an Airbyte Destination Host

pg_tide can run Airbyte destination connectors and feed them outbox messages:

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'warehouse-sync',
    'outbox', 'analytics_events',
    'sink_type', 'airbyte',
    'config', '{
        "destination_image": "airbyte/destination-bigquery:latest",
        "destination_config": {
            "project_id": "my-project",
            "dataset_id": "raw_events",
            "credentials_json": "${env:GCP_CREDENTIALS}"
        }
    }'::jsonb
  )
);
```

See [Sinks: Airbyte](../sinks/airbyte.md) for full configuration.

## Sync Modes

| Mode | Behavior |
|------|----------|
| `incremental` | Only new/changed records since last sync |
| `full_refresh` | Re-extract all records on every run |

Incremental mode persists state between runs (same as Singer STATE), so each sync only transfers the delta.

## Docker Requirement

Airbyte connectors run as Docker containers. The relay host must have Docker available:

```bash
# Verify Docker is accessible
docker ps
```

The relay pulls connector images automatically on first use. For air-gapped environments, pre-pull images into a local registry.

## Differences from Singer

| Aspect | Singer | Airbyte |
|--------|--------|---------|
| Packaging | Python packages (pip) | Docker containers |
| Discovery | `--discover` flag | Separate `discover` command |
| State | JSON file on stdin | State messages in protocol |
| Schema | SCHEMA messages | Catalog with supported sync modes |
| Ecosystem | ~500 connectors | ~400 connectors |
| Overhead | Low (native process) | Higher (Docker container per sync) |

Choose Singer when you want lightweight connectors without Docker. Choose Airbyte when you need connectors only available in the Airbyte catalog or prefer container isolation.

## Further Reading

- [Sources: Airbyte](../sources/airbyte.md) — Running Airbyte source connectors
- [Sinks: Airbyte](../sinks/airbyte.md) — Running Airbyte destination connectors
- [Singer Protocol](singer-protocol.md) — Alternative connector ecosystem
