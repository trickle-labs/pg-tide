# Source: Azure Event Hubs

The Event Hubs source reads events from Azure Event Hubs and delivers them into a pg_tide inbox.

## Configuration

```sql
SELECT tide.relay_set_inbox_v2(
  jsonb_build_object(
    'name', 'events-from-eventhubs',
    'inbox', 'incoming_events',
    'source', 'eventhubs',
    'config', '{
        "connection_string": "${env:EVENTHUBS_CONNECTION_STRING}",
        "event_hub_name": "external-events",
        "consumer_group": "$Default",
        "batch_size": 100
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source_type` | string | — | Must be `"eventhubs"` |
| `connection_string` | string | — | Event Hubs connection string (Listen permission) |
| `event_hub_name` | string | — | Event Hub name |
| `consumer_group` | string | `"$Default"` | Consumer group |
| `batch_size` | int | `100` | Events per read batch |
| `starting_position` | string | `"latest"` | Start position: `"latest"`, `"earliest"` |

## Further Reading

- [Sinks: Event Hubs](../sinks/eventhubs.md) — Publishing to Event Hubs
