# Singer / Meltano

The Singer protocol is an open standard for moving data between systems. It defines a simple JSON-based interface that allows "taps" (data extractors) and "targets" (data loaders) to communicate through stdin/stdout pipes. The Meltano Hub catalogs approximately 500 taps and targets maintained by the community, covering everything from SaaS APIs (Salesforce, HubSpot, Stripe) to databases (MySQL, Oracle) to file formats (CSV, Parquet). When pg_tide uses the Singer sink, it streams your outbox messages through any Singer target, giving you instant access to hundreds of destinations without pg_tide needing to implement each one individually.

This is one of pg_tide's most powerful integrations because it multiplies the number of available destinations dramatically. Instead of waiting for pg_tide to add native support for a niche system, you can connect today using an existing Singer target from Meltano Hub.

## When to Use This Sink

Choose the Singer sink when your destination is not directly supported by pg_tide's native sinks, when you want to leverage existing Singer targets maintained by the Meltano community, or when you need the Singer protocol's built-in STATE persistence for resumable syncs and SCHEMA handling for data type management. Common use cases include loading events into SaaS analytics tools (Amplitude, Mixpanel), sending to CRM systems (Salesforce, HubSpot), or writing to specialized databases.

## How It Works

The relay launches a Singer target process and streams messages to it via stdin in Singer's RECORD format. The relay also manages STATE messages (for resumable syncs) and SCHEMA messages (for data type declarations):

1. The relay sends a SCHEMA message declaring the event structure
2. For each outbox message, the relay sends a RECORD message via stdin
3. The target writes STATE messages back to stdout, which the relay persists in the `tide.singer_state` table
4. If the relay restarts, it resumes from the last persisted STATE

## Configuration

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'events-to-amplitude',
    'outbox', 'analytics_events',
    'sink_type', 'singer',
    'config', '{
        "target_command": "target-amplitude",
        "target_config": {
            "api_key": "${env:AMPLITUDE_API_KEY}",
            "project_id": "${env:AMPLITUDE_PROJECT_ID}"
        },
        "on_schema_change": "log",
        "batch_size": 100
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"singer"` |
| `target_command` | string | — | Singer target executable name or path |
| `target_config` | object | `{}` | Configuration object passed to the target |
| `on_schema_change` | string | `"log"` | Schema drift policy: `"ignore"`, `"log"`, `"fail"`, `"evolve"` |
| `batch_size` | int | `100` | Records per STATE checkpoint |
| `stream_name` | string | auto | Singer stream name (defaults to outbox name) |

## STATE Persistence

Singer targets emit STATE messages to communicate their progress. pg_tide persists these in the `tide.singer_state` catalog table, enabling resumable syncs:

```sql
-- Inspect Singer state
SELECT * FROM tide.singer_state_list();
```

If the relay restarts, it sends the last persisted STATE to the target on startup, so the target can resume from where it left off rather than reprocessing all data.

## Schema Handling

The `on_schema_change` policy controls what happens when the structure of outbox messages changes:

- **`"ignore"`** — Silently accept new fields
- **`"log"`** — Log a warning but continue processing
- **`"fail"`** — Stop the pipeline and alert (safest for production)
- **`"evolve"`** — Automatically send an updated SCHEMA message to the target

Monitor schema drift with:
```sql
SELECT * FROM tide.singer_schema_drift();
```

## Finding Targets on Meltano Hub

Browse available targets at [hub.meltano.com](https://hub.meltano.com/). Popular targets include:

- `target-bigquery` — Load into BigQuery
- `target-snowflake` — Load into Snowflake
- `target-postgres` — Load into another PostgreSQL
- `target-s3-csv` — Write CSV files to S3
- `target-hubspot` — Push to HubSpot CRM
- `target-salesforce` — Push to Salesforce

Install targets with pip: `pip install target-amplitude`

## Troubleshooting

- **"Target command not found"** — Ensure the target is installed and available in the relay's PATH
- **"Invalid STATE message"** — The target emitted malformed STATE; check target version compatibility
- **Schema drift detected** — Event structure changed; review with `singer_schema_drift()` and adjust `on_schema_change` policy
- **"Target process exited unexpectedly"** — Check target logs for configuration errors

## Further Reading

- [Airbyte](airbyte.md) — Alternative connector ecosystem
- [Fivetran](fivetran.md) — Enterprise data integration
- [Singer Protocol Feature Guide](../features/singer-protocol.md) — Detailed protocol documentation
