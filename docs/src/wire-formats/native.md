# Wire Format: Native

The native wire format is pg_tide's default. It passes outbox rows through with minimal transformation, producing clean JSON that contains all the information needed to reconstruct the event on the receiving side. If you control both the producer and consumer, native is the simplest and most transparent choice.

## Encoded Format (Outbox → Sink)

When encoding an outbox row for delivery, the native format produces:

```json
{
  "outbox_id": 42,
  "op": "insert",
  "stream_table": "order_events",
  "payload": {
    "order_id": "ORD-001",
    "status": "confirmed",
    "total": 99.95
  }
}
```

The message key is set to the outbox row's routing key (if configured) or the outbox ID.

## Decoded Format (Source → Inbox)

When decoding incoming messages, the native format expects JSON payloads. It extracts:

| Field | Source |
|-------|--------|
| `event_id` | From message key, or generated UUID |
| `event_type` | From `stream_table` field or message topic |
| `op` | From `op` field (`insert`, `update`, `delete`) |
| `payload` | From `payload` field (or entire message) |

## Configuration

No configuration is needed — native is the default when no `wire_format` is specified:

```toml
[[pipelines]]
name = "orders"
# wire_format defaults to "native"
```

Or explicitly:

```toml
[[pipelines]]
name = "orders"
wire_format = "native"
```

## When to Use Native

- Both producer and consumer are pg_tide (e.g., outbox → inbox replication)
- You control the consumer and can parse the simple JSON envelope
- You want maximum transparency — what goes in is what comes out
- You're debugging or developing and want to see raw messages clearly

## When to Use Something Else

- Your consumer expects Debezium format → use [debezium](debezium.md)
- You're ingesting from Maxwell or Canal → use [maxwell](maxwell.md) or [canal](canal.md)
- You have a custom format with non-standard field names → use [cdc_json](cdc-json.md)

## Further Reading

- [Wire Formats Overview](overview.md) — Comparison of all formats
- [Sinks: stdout](../sinks/stdout.md) — See native output directly
