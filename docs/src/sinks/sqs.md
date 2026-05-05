# Amazon SQS

Amazon Simple Queue Service (SQS) is a fully managed message queuing service provided by AWS. It requires zero operational overhead — there are no brokers to provision, no clusters to manage, and no capacity to plan. SQS automatically scales from one message per second to thousands, and you pay only for what you use. When you connect pg_tide to SQS, your outbox messages are delivered to SQS queues where they can trigger Lambda functions, feed ECS services, or be consumed by any AWS service or application that polls SQS.

SQS offers two queue types: Standard queues provide nearly unlimited throughput with at-least-once delivery, while FIFO queues guarantee exactly-once processing with strict message ordering. Both work seamlessly with pg_tide.

## When to Use This Sink

Choose SQS when your infrastructure runs on AWS and you want a zero-maintenance message queue. SQS is particularly valuable for triggering Lambda functions (event-driven serverless), decoupling microservices within AWS, and building reliable work queues where messages must not be lost. The FIFO variant is excellent when you need both ordering and exactly-once delivery without managing broker infrastructure.

## Configuration

### Minimal Configuration

```sql
SELECT tide.relay_set_outbox(
    'orders-to-sqs',
    'orders',
    'sqs-relay',
    '{
        "sink_type": "sqs",
        "queue_url": "https://sqs.us-east-1.amazonaws.com/123456789/order-events",
        "region": "us-east-1"
    }'::jsonb
);
```

### Production Configuration (FIFO Queue)

```sql
SELECT tide.relay_set_outbox(
    'orders-to-sqs',
    'orders',
    'sqs-relay',
    '{
        "sink_type": "sqs",
        "queue_url": "${env:SQS_QUEUE_URL}",
        "region": "${env:AWS_REGION}",
        "message_group_id": "{stream_table}",
        "deduplication_id": "{dedup_key}",
        "batch_size": 10
    }'::jsonb
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"sqs"` |
| `queue_url` | string | — | Full SQS queue URL |
| `region` | string | — | AWS region |
| `access_key_id` | string | `null` | AWS access key (falls back to default credential chain) |
| `secret_access_key` | string | `null` | AWS secret key |
| `message_group_id` | string | `null` | Message group ID for FIFO queues. Supports templates |
| `deduplication_id` | string | `null` | Deduplication ID for FIFO queues. Supports `{dedup_key}` |
| `batch_size` | int | `10` | Messages per SendMessageBatch (max 10 for SQS) |
| `message_attributes` | object | `null` | Custom SQS message attributes |

## Authentication

The relay uses the standard AWS credential chain: environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), instance profile (EC2/ECS), or explicit credentials in the pipeline config. For production on AWS, use IAM roles attached to your ECS task or EC2 instance rather than explicit keys.

## Delivery Guarantees

- **Standard queues:** At-least-once delivery with best-effort ordering. Messages may occasionally be delivered more than once.
- **FIFO queues:** Exactly-once processing with strict ordering within a message group. Set `deduplication_id` to `{dedup_key}` for automatic deduplication.

## Complete Example

```sql
SELECT tide.outbox_publish(
    'orders',
    '{"event": "order.created", "order_id": "ord-100", "total": 299.00}'::jsonb,
    'ord-100-created'
);
```

The message appears in the SQS queue and can trigger a Lambda function:

```bash
aws sqs receive-message --queue-url $SQS_QUEUE_URL --max-number-of-messages 1
```

## Troubleshooting

- **"Access Denied"** — The IAM role/user needs `sqs:SendMessage` and `sqs:SendMessageBatch` permissions on the queue
- **"Queue does not exist"** — Verify the queue URL is correct and the queue exists in the specified region
- **"InvalidParameterValue" for FIFO** — FIFO queues require `message_group_id`; ensure it's configured
- **Duplicate messages in Standard queue** — Expected behavior; implement idempotent consumers

## Further Reading

- [Sources: SQS](../sources/sqs.md) — Consuming from SQS into pg_tide inbox
- [Amazon Kinesis](kinesis.md) — For higher throughput streaming on AWS
