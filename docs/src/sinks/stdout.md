# stdout / File

The stdout sink writes messages to standard output or a file. This is primarily useful for debugging, testing, and piping pg_tide output to external tools. In development, it lets you verify your pipeline configuration and transforms without needing an external message broker running.

## When to Use This Sink

Choose stdout when debugging pipeline configurations, testing JMESPath transforms, validating wire format encoding, or piping events to external command-line tools. It is also useful for log-based delivery patterns where messages are written to a file that is then consumed by a log shipper (Fluentd, Vector, Filebeat).

## Configuration

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'debug-pipeline',
    'outbox', 'orders',
    'sink_type', 'stdout',
    'config', '{
        "format": "json",
        "output_file": null
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"stdout"` |
| `format` | string | `"json"` | Output format: `"json"` (one JSON per line) or `"pretty"` (indented JSON) |
| `output_file` | string | `null` | Write to file instead of stdout (optional) |

## Use Cases

### Debugging transforms

```bash
# See what your JMESPath transform produces
pg-tide --postgres-url "..." 2>/dev/null | jq .
```

### Piping to external tools

```bash
# Pipe to a custom processor
pg-tide --postgres-url "..." | my-custom-processor
```

## Further Reading

- [Dry-Run Mode](../features/dry-run-replay.md) — Test pipelines without delivering
- [JMESPath Transforms](../features/transforms.md) — Transform messages before delivery
