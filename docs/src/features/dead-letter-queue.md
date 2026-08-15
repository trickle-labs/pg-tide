# Feature: Dead Letter Queue

When a message fails to deliver after all retry attempts — because the sink rejected it, the payload couldn't be decoded, or a permanent error occurred — it needs somewhere to go. The dead letter queue (DLQ) captures these failed messages in a PostgreSQL table where you can inspect them, understand why they failed, fix the underlying issue, and replay them back through the pipeline.

Without a DLQ, a single poison message can block an entire pipeline. The DLQ isolates failures so healthy messages continue flowing while problematic ones wait for human attention.

## How It Works

```
Message fails        →  Retry (up to max_retries)
Still failing        →  Classify error kind
Route to DLQ         →  INSERT INTO tide.relay_dlq
Continue pipeline    →  Next messages flow normally
```

Failed messages are inserted into `tide.relay_dlq` with full context: the pipeline name, source and sink names, the original payload, the error message, and a classification of why it failed.

## Error Classifications

| Error Kind | Meaning | Example |
|-----------|---------|---------|
| `decode` | Payload couldn't be decoded from wire format | Malformed JSON, Avro schema mismatch |
| `sink_permanent` | Sink rejected permanently (no retry will help) | Invalid credentials, schema validation failure |
| `inbox_permanent` | Inbox insertion failed | Constraint violation, duplicate key |
| `max_retries_exceeded` | Transient error persisted beyond retry limit | Network timeout after 5 attempts |

## Configuration

DLQ is configured per-pipeline:

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-pipeline',
    'outbox', 'order_events',
    'sink_type', 'kafka',
    'config', '{
        "brokers": "kafka:9092",
        "topic": "orders",
        "dlq": {
            "enabled": true,
            "max_retries": 5,
            "retry_delay_seconds": 10,
            "retention_days": 30
        }
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dlq.enabled` | bool | `true` | Enable dead letter queue |
| `dlq.max_retries` | int | `5` | Delivery attempts before DLQ routing |
| `dlq.retry_delay_seconds` | int | `10` | Delay between retry attempts |
| `dlq.retention_days` | int | `30` | Days to keep resolved DLQ entries |

## Inspecting the DLQ

Query the DLQ table directly:

```sql
SELECT id, pipeline_name, error_kind, error_message, created_at
FROM tide.relay_dlq
WHERE resolved_at IS NULL
ORDER BY created_at DESC;
```

Or use the SQL API:

```sql
-- List unresolved DLQ entries for a pipeline
SELECT * FROM tide.relay_dlq_list('orders-pipeline');

-- View full payload of a specific entry
SELECT payload FROM tide.relay_dlq WHERE id = 42;
```

## Replaying Failed Messages

Once you've fixed the underlying issue (corrected credentials, updated schema, fixed payload format), replay messages back through the pipeline:

```sql
-- Retry a single message
SELECT tide.relay_dlq_retry(42);

-- Retry all messages for a pipeline
SELECT tide.relay_dlq_retry_all('orders-pipeline');
```

Replayed messages go through the normal pipeline path. If they fail again, they return to the DLQ with an updated retry count.

## Integration with Circuit Breaker

When the [circuit breaker](circuit-breaker.md) opens (sink is unhealthy), messages are routed directly to the DLQ rather than waiting indefinitely. This prevents message buildup in memory while the sink recovers. Once the circuit closes, new messages flow normally — and you can replay DLQ entries to recover the ones that were sidelined.

## Monitoring

Track DLQ activity via Prometheus metrics:

- `pg_tide_dlq_entries_total` — Total messages routed to DLQ (by pipeline, error_kind)
- Check the DLQ table row count as part of your alerting

## Further Reading

- [Circuit Breaker](circuit-breaker.md) — Automatic failure detection
- [Troubleshooting](../operations/troubleshooting.md) — Diagnosing delivery failures
