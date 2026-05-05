# Wire Format: CDC JSON

The CDC JSON wire format is a universal decoder that maps any JSON-based change data capture format into pg_tide's internal model using JSONPath expressions. Instead of being tied to a specific CDC tool's output shape, you define paths that tell pg_tide where to find the operation type, payload, timestamps, and other fields within your custom message format.

This is the format to use when your source produces CDC-like messages but doesn't match Debezium, Maxwell, or Canal exactly.

> **Note:** CDC JSON is decode-only. pg_tide can consume custom CDC messages but does not produce them.

## How It Works

You provide JSONPath expressions that map fields from your message format to pg_tide's internal representation:

```
Your custom message            JSONPath config             pg_tide inbox row
─────────────────────          ─────────────────           ─────────────────
{                                                          event_id: "evt-42"
  "event_type": "created",     op_path: "$.event_type"    op: insert
  "occurred_at": "2024...",    commit_ts_path: ...        commit_ts: 2024-...
  "data": {                    payload_path: "$.data"     payload: {id: 7}
    "id": 7,
    "name": "alice"
  }
}
```

## Configuration

```toml
[[pipelines]]
name = "custom-cdc-ingest"
wire_format = "cdc_json"

[pipelines.wire_config]
op_path = "$.event_type"
payload_path = "$.data"
commit_ts_path = "$.occurred_at"
commit_ts_format = "rfc3339"

[pipelines.wire_config.op_map]
created = "insert"
modified = "update"
removed = "delete"
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `op_path` | string | `"$.op"` | JSONPath to the operation field |
| `op_map` | object | `{}` | Map source values to pg_tide ops |
| `payload_path` | string | `"$"` | JSONPath to new row / event data |
| `old_payload_path` | string | `null` | JSONPath to before-state (optional) |
| `event_id_path` | string | `null` | JSONPath to deduplication key |
| `event_type_path` | string | `null` | JSONPath to event type (defaults to topic) |
| `commit_ts_path` | string | `null` | JSONPath to commit timestamp |
| `commit_ts_format` | string | `"rfc3339"` | Timestamp format: `"rfc3339"`, `"unix_seconds"`, `"unix_millis"` |
| `source_position_path` | string | `null` | JSONPath to source position / offset |

## JSONPath Syntax

pg_tide supports simple dot-notation JSONPath expressions:

- `$` — The entire message (root)
- `$.field` — Top-level field
- `$.field.nested` — Nested field access

Array indexing is not supported. If your messages use arrays, consider preprocessing or using a [JMESPath transform](../features/transforms.md).

## Operation Mapping

The `op_map` configuration translates your source's operation names into pg_tide's standard operations (`insert`, `update`, `delete`):

```toml
[pipelines.wire_config.op_map]
# Map custom names to pg_tide standard ops
"CREATED" = "insert"
"UPDATED" = "update"
"DELETED" = "delete"
"SNAPSHOT" = "insert"
```

If `op_map` is empty and `op_path` points to a field that already contains `insert`/`update`/`delete`, no mapping is needed.

## Examples

### Stripe-style Events

```json
{
  "id": "evt_1234",
  "type": "invoice.paid",
  "created": 1714029482,
  "data": {
    "object": { "id": "in_5678", "amount": 5000 }
  }
}
```

```toml
[pipelines.wire_config]
event_id_path = "$.id"
event_type_path = "$.type"
payload_path = "$.data.object"
commit_ts_path = "$.created"
commit_ts_format = "unix_seconds"
# No op_path needed — all events are treated as inserts by default
```

### Custom Microservice Events

```json
{
  "action": "user.updated",
  "timestamp": "2024-04-25T14:30:00Z",
  "correlation_id": "corr-abc-123",
  "before": { "name": "Alice", "role": "user" },
  "after": { "name": "Alice", "role": "admin" }
}
```

```toml
[pipelines.wire_config]
op_path = "$.action"
op_map = { "user.created" = "insert", "user.updated" = "update", "user.deleted" = "delete" }
event_id_path = "$.correlation_id"
event_type_path = "$.action"
payload_path = "$.after"
old_payload_path = "$.before"
commit_ts_path = "$.timestamp"
commit_ts_format = "rfc3339"
```

## Further Reading

- [Wire Formats Overview](overview.md) — Comparison of all formats
- [Transforms](../features/transforms.md) — Post-decode JMESPath transformations
- [Debezium Format](debezium.md) — Standard CDC format (if your source matches)
