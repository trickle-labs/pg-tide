# Source: Singer / Meltano

The Singer source runs a Singer "tap" (data extractor) and delivers its output records into a pg_tide inbox. This gives you access to approximately 500 data sources from the Meltano Hub ecosystem — SaaS APIs, databases, file formats, and more — all flowing reliably into your PostgreSQL inbox with incremental sync state management.

## Configuration

```sql
SELECT tide.relay_set_inbox(
    'hubspot-contacts',
    'crm_events',
    '{
        "source_type": "singer",
        "tap_command": "tap-hubspot",
        "tap_config": {
            "api_key": "${env:HUBSPOT_API_KEY}",
            "start_date": "2024-01-01T00:00:00Z"
        },
        "on_schema_change": "log"
    }'::jsonb
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source_type` | string | — | Must be `"singer"` |
| `tap_command` | string | — | Singer tap executable |
| `tap_config` | object | `{}` | Tap configuration |
| `on_schema_change` | string | `"log"` | Schema drift policy |
| `state_persistence` | bool | `true` | Persist STATE for incremental syncs |
| `stream_filter` | array | `null` | Specific streams to extract (null = all) |

## STATE Persistence for Incremental Syncs

Singer taps emit STATE messages containing bookmarks (last sync position). pg_tide persists these in `tide.singer_state`, enabling incremental syncs — on each run, only new or changed data is extracted.

## Further Reading

- [Sinks: Singer](../sinks/singer.md) — Running Singer targets (forward direction)
- [Singer Protocol Feature](../features/singer-protocol.md) — Detailed protocol documentation
