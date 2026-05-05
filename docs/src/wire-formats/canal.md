# Wire Format: Canal

The Canal wire format decodes messages produced by [Alibaba Canal](https://github.com/alibaba/canal), a MySQL/MariaDB CDC tool popular in the Chinese technology ecosystem. pg_tide can ingest Canal change streams into a PostgreSQL inbox for cross-database replication, data consolidation, or event-driven processing.

> **Note:** Canal is decode-only. pg_tide can consume Canal messages but does not produce them.

## Message Shape

Canal produces JSON messages with this structure:

```json
{
  "id": 1,
  "database": "myapp",
  "table": "orders",
  "pkNames": ["id"],
  "isDdl": false,
  "type": "INSERT",
  "es": 1714029482000,
  "ts": 1714029483000,
  "data": [
    {
      "id": "42",
      "status": "confirmed",
      "total": "99.95"
    }
  ],
  "old": null
}
```

For UPDATE operations:

```json
{
  "id": 2,
  "database": "myapp",
  "table": "orders",
  "pkNames": ["id"],
  "isDdl": false,
  "type": "UPDATE",
  "es": 1714029484000,
  "ts": 1714029485000,
  "data": [
    {
      "id": "42",
      "status": "shipped",
      "total": "99.95"
    }
  ],
  "old": [
    {
      "status": "confirmed"
    }
  ]
}
```

> **Important:** Canal serializes all column values as strings, regardless of their original MySQL type.

## Decoded Fields

| Inbox Field | Canal Source |
|-------------|-------------|
| `event_id` | Message key or generated UUID |
| `event_type` | `{database}.{table}` |
| `op` | From `type`: INSERT→insert, UPDATE→update, DELETE→delete |
| `payload` | First element of `data` array |
| `old_payload` | First element of `old` array (updates only) |
| `commit_ts` | From `es` (event timestamp, milliseconds) |
| `source_position` | From `id` field |

## Configuration

```toml
[[pipelines]]
name = "canal-ingest"
wire_format = "canal"

[pipelines.wire_config]
skip_ddl = true
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `skip_ddl` | bool | `true` | Skip DDL events (CREATE TABLE, ALTER TABLE, etc.) |

## DDL Events

Canal captures DDL (Data Definition Language) statements like `ALTER TABLE` and `CREATE INDEX`. These events have `isDdl: true`. By default, pg_tide skips them since they don't represent data changes. Set `skip_ddl: false` if you want to capture schema changes as events in your inbox.

## String-Typed Values

Canal represents all MySQL column values as JSON strings. A numeric column `total DECIMAL(10,2)` arrives as `"99.95"` rather than `99.95`. Your downstream processing should account for this — you may want to use a [JMESPath transform](../features/transforms.md) to cast values back to their intended types.

## Further Reading

- [Wire Formats Overview](overview.md) — Comparison of all formats
- [Maxwell Format](maxwell.md) — Another MySQL CDC format
- [CDC JSON Format](cdc-json.md) — Generic format for custom CDC shapes
