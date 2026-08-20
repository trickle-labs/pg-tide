# Diagnostic stdout and file output

`stdout` and `file` are small diagnostic sinks. They are useful for local
inspection and smoke tests, not production delivery guarantees.

```json
{
  "source_type": "outbox",
  "source": {"outbox": "orders"},
  "sink_type": "stdout",
  "sink": {"format": "jsonl"}
}
```

For file output, provide a path:

```json
{
  "sink_type": "file",
  "sink": {"path": "/var/log/pg-tide/orders.jsonl", "format": "jsonl"}
}
```

Supported formats are `jsonl` and `pretty`.
