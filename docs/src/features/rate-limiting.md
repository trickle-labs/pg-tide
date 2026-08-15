# Feature: Rate Limiting

Rate limiting controls how fast messages flow from the outbox to the sink. It uses a token-bucket algorithm that allows short bursts above the steady-state rate while enforcing a long-term maximum throughput. When the bucket is empty, the relay pauses — this back-pressure propagates upstream, causing outbox rows to accumulate in PostgreSQL until the rate allows them through.

## Why Rate Limit?

Rate limiting protects downstream systems from being overwhelmed. Common scenarios:

- **API rate limits:** Webhook endpoints, Slack, PagerDuty, and cloud APIs enforce request-per-second limits. Exceeding them causes 429 errors and potential account throttling.
- **Cost control:** Cloud services (BigQuery, Kinesis, Pub/Sub) charge per operation. Rate limiting caps your bill predictably.
- **Graceful degradation:** During bulk backfill operations, rate limiting prevents your relay from saturating network links or database connections.
- **Fair sharing:** Multiple pipelines competing for the same sink — rate limiting ensures each gets predictable throughput.

## Configuration

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'notifications',
    'outbox', 'notification_events',
    'sink_type', 'slack',
    'config', '{
        "webhook_url": "${env:SLACK_WEBHOOK_URL}",
        "rate_limit": {
            "enabled": true,
            "max_messages_per_second": 1,
            "burst_size": 5
        }
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `rate_limit.enabled` | bool | `false` | Enable rate limiting |
| `rate_limit.max_messages_per_second` | int | `0` | Steady-state rate (0 = unlimited) |
| `rate_limit.burst_size` | int | same as rate | Burst capacity above steady rate |

## How the Token Bucket Works

The token bucket starts full with `burst_size` tokens. Each message consumes one token. Tokens refill at `max_messages_per_second` rate. When the bucket is empty, the relay blocks until tokens are available.

```
Bucket capacity: burst_size = 10
Refill rate: max_messages_per_second = 5

Time 0s:   [■■■■■■■■■■] 10 tokens (full)
           → Send 10 messages instantly (burst)
Time 0s:   [          ]  0 tokens (empty)
           → Block until tokens refill
Time 1s:   [■■■■■     ]  5 tokens (refilled at 5/s)
           → Send 5 messages
Time 2s:   [■■■■■     ]  5 tokens
           ...
```

This means:
- The first batch can send up to `burst_size` messages immediately
- After the burst, throughput stabilizes at `max_messages_per_second`
- Brief pauses allow tokens to accumulate for the next burst

## Back-Pressure Propagation

When the rate limiter blocks, it creates a chain of back-pressure:

1. **Rate limiter blocks** → Worker pauses before publishing
2. **Worker pauses** → Source poll interval stretches
3. **Source poll stretches** → Outbox rows stay in PostgreSQL longer
4. **Rows stay in DB** → No data loss, messages wait safely in the transactional outbox

This is safe because the outbox is durable — messages persist in PostgreSQL until acknowledged. The rate limiter doesn't drop messages; it just slows the relay down.

## Tuning Guidelines

**For webhook/API sinks:** Set `max_messages_per_second` at 80% of the API's documented rate limit. This leaves headroom for retries and other clients.

**For streaming sinks (Kafka, NATS):** Usually unnecessary — these systems handle high throughput natively. Only rate-limit if you're paying per-message or want to control network bandwidth.

**For database sinks (ClickHouse, BigQuery):** Set rate to keep bulk insert batches at a comfortable size. 100-1000 msg/s is typical for analytical databases.

**Burst size:** Set to your expected batch size. If `default_batch_size` is 100, a `burst_size` of 100-200 allows full batches to flow without blocking.

## Further Reading

- [Circuit Breaker](circuit-breaker.md) — Complementary failure protection
- [Scaling](../operations/scaling.md) — Throughput optimization strategies
