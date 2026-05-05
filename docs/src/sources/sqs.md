# Source: Amazon SQS

The SQS source receives messages from Amazon SQS queues and delivers them into a pg_tide inbox. It uses long polling for efficient message retrieval and deletes messages from SQS only after successful inbox insertion.

## Configuration

```sql
SELECT tide.relay_set_inbox(
    'events-from-sqs',
    'incoming_events',
    '{
        "source_type": "sqs",
        "queue_url": "${env:SQS_QUEUE_URL}",
        "region": "us-east-1",
        "wait_time_seconds": 20,
        "visibility_timeout": 60,
        "batch_size": 10
    }'::jsonb
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source_type` | string | — | Must be `"sqs"` |
| `queue_url` | string | — | SQS queue URL |
| `region` | string | — | AWS region |
| `wait_time_seconds` | int | `20` | Long poll wait time (max 20) |
| `visibility_timeout` | int | `60` | Seconds before unprocessed messages become visible again |
| `batch_size` | int | `10` | Messages per ReceiveMessage call (max 10) |
| `access_key_id` | string | `null` | AWS credentials (optional, falls back to default chain) |
| `secret_access_key` | string | `null` | AWS secret key |

## Delivery Guarantees

Messages are deleted from SQS only after inbox insertion succeeds. The inbox's deduplication uses the SQS message ID, preventing duplicates from Standard queues (which may deliver messages more than once). FIFO queues provide exactly-once semantics.

## Further Reading

- [Sinks: SQS](../sinks/sqs.md) — Publishing to SQS
