# Amazon Kinesis Data Streams

Amazon Kinesis Data Streams is a real-time data streaming service designed for high-volume, continuous data ingestion on AWS. Unlike SQS (which is a queue), Kinesis is a stream — data is retained for a configurable period and multiple consumers can independently read from the same stream at their own pace. When pg_tide delivers messages to Kinesis, they become available to real-time analytics applications, machine learning pipelines, and data lake ingestion processes running on AWS.

Kinesis is designed for scenarios where you need to process hundreds of thousands of records per second in real time. Each stream is composed of shards, and each shard provides 1 MB/s of write capacity and 2 MB/s of read capacity, allowing you to scale by adding more shards.

## When to Use This Sink

Choose Kinesis when you need high-throughput real-time streaming on AWS, when you want multiple consumers reading the same data independently (Kinesis Analytics, Lambda, custom applications), or when you need data retention for replay purposes (up to 365 days). Kinesis integrates deeply with AWS services like Firehose, Analytics, and Lambda.

## Configuration

### Minimal Configuration

```sql
SELECT tide.relay_set_outbox(
    'events-to-kinesis',
    'events',
    'kinesis-relay',
    '{
        "sink_type": "kinesis",
        "stream_name": "pg-tide-events",
        "region": "us-east-1",
        "partition_key": "{dedup_key}"
    }'::jsonb
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"kinesis"` |
| `stream_name` | string | — | Kinesis stream name |
| `region` | string | — | AWS region |
| `partition_key` | string | — | Partition key template. Determines shard assignment. Supports `{dedup_key}`, `{stream_table}` |
| `access_key_id` | string | `null` | AWS access key (falls back to default credential chain) |
| `secret_access_key` | string | `null` | AWS secret key |
| `batch_size` | int | `100` | Records per PutRecords call (max 500) |

## Delivery Guarantees

Kinesis provides at-least-once delivery. The relay uses the PutRecords API for batch ingestion and confirms delivery before committing offsets. Kinesis guarantees ordering within a partition key, so messages with the same `partition_key` value are always delivered in order.

## Partition Strategy

The partition key determines which shard receives each record. Use `{dedup_key}` to keep all events for the same entity on the same shard (preserving per-entity ordering), or `{stream_table}` to group by outbox name. For maximum throughput distribution, use a high-cardinality key.

## Troubleshooting

- **"Stream not found"** — Verify stream name and region are correct
- **"ProvisionedThroughputExceededException"** — Shard capacity exceeded; add more shards or reduce batch rate with the [rate limiter](../features/rate-limiting.md)
- **"Access Denied"** — IAM role needs `kinesis:PutRecord` and `kinesis:PutRecords` permissions

## Further Reading

- [Sources: Kinesis](../sources/kinesis.md) — Consuming from Kinesis into pg_tide inbox
- [Amazon SQS](sqs.md) — For simpler queuing without stream semantics
