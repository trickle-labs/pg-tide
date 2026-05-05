# Source: stdin / File

The stdin source reads line-delimited JSON from standard input or a file and delivers each line as a message into a pg_tide inbox. This is useful for testing, replay from log files, data migration, and piping output from external commands into your inbox.

## Configuration

```sql
SELECT tide.relay_set_inbox(
    'file-import',
    'imported_events',
    '{
        "source_type": "stdin",
        "input_file": "/data/events-export.jsonl",
        "batch_size": 1000
    }'::jsonb
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source_type` | string | — | Must be `"stdin"` |
| `input_file` | string | `null` | File path (null = read from stdin) |
| `batch_size` | int | `100` | Lines per inbox insert batch |
| `format` | string | `"jsonl"` | Input format: `"jsonl"` (one JSON per line) |

## Use Cases

### Replaying from a log file
```bash
pg-tide --postgres-url "..." < events-backup.jsonl
```

### Piping from another tool
```bash
kafka-console-consumer --topic events --from-beginning | pg-tide --postgres-url "..."
```

## Further Reading

- [Sinks: stdout](../sinks/stdout.md) — Writing to stdout (forward direction)
- [Dry-Run & Replay](../features/dry-run-replay.md) — Replay mode for reprocessing
