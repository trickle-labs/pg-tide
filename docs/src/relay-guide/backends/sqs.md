# SQS Backend

[Amazon SQS](https://aws.amazon.com/sqs/) integration for AWS-native message queuing.

---

## Forward (Outbox → SQS)

```sql
SELECT tide.relay_set_outbox('events-sqs', 'events', 'sqs',
  jsonb_build_object(
    'queue_url', 'https://sqs.us-east-1.amazonaws.com/123456789/my-queue',
    'region', 'us-east-1'
  )
);
```

### Configuration

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `queue_url` | Yes | — | Full SQS queue URL |
| `region` | Yes | — | AWS region |
| `message_group_id` | No | — | For FIFO queues |
| `delay_seconds` | No | `0` | Message delivery delay |

---

## Reverse (SQS → Inbox)

```sql
SELECT tide.relay_set_inbox('sqs-to-inbox', 'sqs-events',
  jsonb_build_object(
    'queue_url', 'https://sqs.us-east-1.amazonaws.com/123456789/incoming',
    'region', 'us-east-1',
    'wait_time_seconds', 20
  ),
  p_source := 'sqs'
);
```

### Configuration

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `queue_url` | Yes | — | Full SQS queue URL |
| `region` | Yes | — | AWS region |
| `wait_time_seconds` | No | `20` | Long-poll wait time |
| `max_messages` | No | `10` | Max messages per receive call |

---

## Authentication

Uses the standard AWS credential chain:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS config/credentials files
3. IAM instance role (EC2/ECS/Lambda)
4. EKS IRSA (via web identity token)

---

## Cargo Feature

```bash
cargo build --package pg-tide-relay --features "sqs"
```
