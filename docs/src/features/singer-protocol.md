# Feature: Singer Protocol Support

pg_tide implements the [Singer specification](https://hub.meltano.com/singer/spec/) for both extraction (taps) and loading (targets). This gives you access to approximately 500 data connectors from the Meltano Hub ecosystem without writing custom integration code.

## What is Singer?

Singer is a specification for moving data between systems. It defines three message types that flow between "taps" (data extractors) and "targets" (data loaders) over standard I/O:

- **RECORD** — A single data row with stream name and record data
- **SCHEMA** — JSON Schema for a stream (column names, types)
- **STATE** — Bookmark for incremental sync (last sync position)

## pg_tide as a Singer Target

When used as a sink, pg_tide acts as a Singer target — it receives RECORD, SCHEMA, and STATE messages from any Singer tap and writes them into the configured destination:

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'hubspot-to-warehouse',
    'outbox', 'etl_events',
    'sink_type', 'singer',
    'config', '{
        "target_command": "target-postgres",
        "target_config": {
            "host": "warehouse.example.com",
            "database": "analytics"
        }
    }'::jsonb
  )
);
```

See [Sinks: Singer](../sinks/singer.md) for full configuration.

## pg_tide as a Singer Tap Consumer

When used as a source, pg_tide runs a Singer tap subprocess and ingests its output into a pg_tide inbox:

```sql
SELECT tide.relay_set_inbox_v2(
  jsonb_build_object(
    'name', 'salesforce-sync',
    'inbox', 'crm_inbox',
    'source', 'singer',
    'config', '{
        "tap_command": "tap-salesforce",
        "tap_config": {
            "client_id": "${env:SF_CLIENT_ID}",
            "start_date": "2024-01-01"
        }
    }'::jsonb
  )
);
```

See [Sources: Singer](../sources/singer.md) for full configuration.

## STATE Persistence

Singer STATE messages contain bookmarks — the last sync position for each stream. pg_tide persists these in the catalog so incremental syncs resume where they left off:

1. Tap emits STATE message after processing a page of records
2. pg_tide writes STATE to `tide.singer_state` table
3. On next run, pg_tide passes the saved STATE back to the tap via `--state` argument
4. Tap resumes from the bookmark (only fetches new/changed records)

## Schema Handling

When a tap emits a SCHEMA message, pg_tide uses it for:
- **Validation:** Reject records that don't conform (optional)
- **Evolution:** Detect new fields and update downstream schemas
- **Documentation:** Store discovered schemas for inspection

### On Schema Change

```json
{
  "on_schema_change": "log"
}
```

| Policy | Behavior |
|--------|----------|
| `"log"` | Log the change, continue processing |
| `"stop"` | Stop the pipeline (manual intervention needed) |
| `"evolve"` | Automatically adapt (add new columns, etc.) |

## Stream Selection

By default, all streams discovered by the tap are synced. To select specific streams:

```json
{
  "stream_filter": ["contacts", "deals", "companies"]
}
```

## Compatible Taps and Targets

Any Singer-compatible tap or target works with pg_tide. Popular examples:

**Taps (data sources):** tap-salesforce, tap-hubspot, tap-stripe, tap-github, tap-postgres, tap-mysql, tap-google-analytics, tap-shopify, tap-zendesk

**Targets (data loaders):** target-postgres, target-snowflake, target-bigquery, target-redshift, target-s3-csv, target-jsonl

Browse the full catalog at [hub.meltano.com](https://hub.meltano.com).

## Further Reading

- [Sources: Singer](../sources/singer.md) — Running Singer taps
- [Sinks: Singer](../sinks/singer.md) — Running Singer targets
- [Airbyte Protocol](airbyte-protocol.md) — Alternative connector ecosystem
