# Feature: Fivetran Destination Support

pg_tide can function as a [Fivetran](https://fivetran.com) destination connector, receiving data from Fivetran's managed extraction pipelines and writing it into a pg_tide inbox. This lets you use Fivetran's 300+ managed connectors while routing the data through your transactional inbox for further processing.

## How It Works

Fivetran manages the extraction side (connecting to sources like Salesforce, Stripe, databases) and pushes data to a destination. pg_tide acts as that destination — receiving Fivetran's standardized output and writing records into your PostgreSQL inbox table.

```
Fivetran Cloud  →  HTTP Push  →  pg_tide (Fivetran destination)  →  Inbox table
```

## Configuration

```sql
SELECT tide.relay_set_inbox(
    'fivetran-crm',
    'crm_inbox',
    '{
        "source_type": "fivetran",
        "listen_addr": "0.0.0.0:8080",
        "api_key": "${env:FIVETRAN_API_KEY}",
        "api_secret": "${env:FIVETRAN_API_SECRET}"
    }'::jsonb
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source_type` | string | — | Must be `"fivetran"` |
| `listen_addr` | string | `"0.0.0.0:8080"` | HTTP server address |
| `api_key` | string | — | Fivetran API key for authentication |
| `api_secret` | string | — | Fivetran API secret |

## When to Use Fivetran vs Singer/Airbyte

| Aspect | Fivetran | Singer/Airbyte |
|--------|----------|----------------|
| Management | Fully managed (Fivetran Cloud) | Self-hosted |
| Scheduling | Fivetran handles sync schedule | You manage cron/orchestration |
| Monitoring | Fivetran dashboard | Your own metrics |
| Cost | Per-row pricing | Free (compute cost only) |
| Connector quality | Enterprise-grade, maintained by Fivetran | Community-maintained |

Choose Fivetran when you want zero-maintenance extraction with enterprise SLAs. Choose Singer/Airbyte when you want full control and cost predictability.

## Further Reading

- [Sinks: Fivetran](../sinks/fivetran.md) — Acting as a Fivetran destination
- [Singer Protocol](singer-protocol.md) — Self-hosted alternative
- [Airbyte Protocol](airbyte-protocol.md) — Self-hosted alternative with Docker
