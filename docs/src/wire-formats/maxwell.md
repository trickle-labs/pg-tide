# Wire Format: Maxwell

The Maxwell wire format decodes messages produced by [Maxwell's Daemon](https://maxwells-daemon.io), a MySQL CDC tool. This allows pg_tide to ingest MySQL change streams into a PostgreSQL inbox — useful for migrating data from MySQL to PostgreSQL, building cross-database event pipelines, or consolidating changes from multiple MySQL instances.

> **Note:** Maxwell is decode-only. pg_tide can consume Maxwell messages but does not produce them.

## Message Shape

Maxwell produces JSON messages with this structure:

```json
{
  "database": "myapp",
  "table": "users",
  "type": "insert",
  "ts": 1714029482,
  "xid": 12345,
  "data": {
    "id": 7,
    "name": "alice",
    "email": "alice@example.com"
  }
}
```

For UPDATE operations, an `old` field contains the previous values of changed columns:

```json
{
  "database": "myapp",
  "table": "users",
  "type": "update",
  "ts": 1714029482,
  "xid": 12346,
  "data": {
    "id": 7,
    "name": "alice_new",
    "email": "alice@example.com"
  },
  "old": {
    "name": "alice"
  }
}
```

## Decoded Fields

| Inbox Field | Maxwell Source |
|-------------|---------------|
| `event_id` | Message key or generated UUID |
| `event_type` | `{database}.{table}` |
| `op` | From `type`: insert, update, delete |
| `payload` | From `data` field |
| `old_payload` | From `old` field (updates only) |
| `commit_ts` | From `ts` (Unix seconds) |
| `source_position` | From `xid` or `position` |

## Configuration

```toml
[[pipelines]]
name = "mysql-to-postgres"
wire_format = "maxwell"

[pipelines.wire_config]
treat_bootstrap_as_insert = true
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `treat_bootstrap_as_insert` | bool | `true` | Map `bootstrap-insert` events as INSERT operations |

## Bootstrap Events

Maxwell supports "bootstrapping" — bulk-loading existing table data. These events have `type: "bootstrap-insert"`. With `treat_bootstrap_as_insert: true` (the default), they're treated as normal inserts, allowing you to perform initial data loads through the same pipeline.

## Further Reading

- [Wire Formats Overview](overview.md) — Comparison of all formats
- [Canal Format](canal.md) — Another MySQL CDC format
- [CDC JSON Format](cdc-json.md) — Generic format for custom CDC shapes
