# Source: Amazon Kinesis

The Kinesis source reads records from Amazon Kinesis Data Streams and delivers them into a pg_tide inbox.

## Configuration

```sql
SELECT tide.relay_set_inbox(
    'events-from-kinesis',
    'incoming_events',
    '{
        "source_type": "kinesis",
        "stream_name": "external-events",
        "region": "us-east-1",
        "iterator_type": "LATEST",
        "batch_size": 100
    }'::jsonb
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source_type` | string | — | Must be `"kinesis"` |
| `stream_name` | string | — | Kinesis stream name |
| `region` | string | — | AWS region |
| `iterator_type` | string | `"LATEST"` | Start position: `"LATEST"`, `"TRIM_HORIZON"`, `"AT_TIMESTAMP"` |
| `batch_size` | int | `100` | Records per GetRecords call |
| `access_key_id` | string | `null` | AWS credentials |
| `secret_access_key` | string | `null` | AWS secret key |

## Further Reading

- [Sinks: Kinesis](../sinks/kinesis.md) — Publishing to Kinesis
