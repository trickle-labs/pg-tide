# Source: Azure Service Bus

The Service Bus source receives messages from Azure Service Bus queues or topic subscriptions and delivers them into a pg_tide inbox.

## Configuration

```sql
SELECT tide.relay_set_inbox_v2(
  jsonb_build_object(
    'name', 'events-from-servicebus',
    'inbox', 'incoming_events',
    'source', 'servicebus',
    'config', '{
        "connection_string": "${env:SERVICEBUS_CONNECTION_STRING}",
        "queue_or_subscription": "incoming-events",
        "batch_size": 50
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source_type` | string | — | Must be `"servicebus"` |
| `connection_string` | string | — | Service Bus connection string (Listen permission) |
| `queue_or_subscription` | string | — | Queue name or `topic/subscription` path |
| `batch_size` | int | `50` | Messages per receive batch |
| `max_lock_duration_secs` | int | `60` | Message lock duration |

## Delivery Guarantees

Messages are completed (acknowledged) only after inbox insertion. The peek-lock mechanism ensures unprocessed messages are redelivered after the lock expires.

## Further Reading

- [Sinks: Service Bus](../sinks/servicebus.md) — Publishing to Service Bus
