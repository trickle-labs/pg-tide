# Backends

The pg-tide relay supports multiple messaging backends as both sinks (forward mode: outbox → external system) and sources (reverse mode: external system → inbox). This page covers all available backends with their configuration, use cases, and operational guidance.

---

## Choosing a Backend

Different backends suit different architectural needs. Use this decision matrix to pick the right one:

| Backend | Best for | Latency | Durability | Ordering | Throughput |
|---------|----------|---------|------------|----------|-----------|
| **NATS** | Low-latency microservice communication, pub/sub | ~1ms | With JetStream | Per-subject | Very high |
| **Kafka** | High-throughput event streaming, analytics pipelines | ~5ms | Strong | Per-partition | Extremely high |
| **Redis Streams** | Lightweight streaming, existing Redis infrastructure | ~1ms | Configurable (AOF/RDB) | Per-stream | High |
| **RabbitMQ** | Complex routing, work queues, existing AMQP infrastructure | ~2ms | Per-message (persistent) | Per-queue | Moderate |
| **SQS** | AWS-native, serverless consumers, managed infrastructure | ~20ms | Extremely high | Best-effort (FIFO available) | Moderate |
| **Webhook** | Push notifications, third-party integrations, serverless endpoints | ~50-500ms | Depends on receiver | Per-delivery | Low-moderate |

### Quick recommendations

- **Starting out / prototyping:** NATS (default, zero configuration, fast)
- **Enterprise data pipelines:** Kafka (strongest durability and ordering guarantees)
- **AWS-native infrastructure:** SQS (fully managed, no servers to operate)
- **Existing Redis stack:** Redis Streams (reuse your Redis deployment)
- **Third-party integrations:** Webhook (push to any HTTP endpoint)
- **Legacy AMQP systems:** RabbitMQ (rich routing, mature ecosystem)

---

## Feature Flags

Backends are feature-gated at compile time. Only enabled backends are compiled into the relay binary:

| Backend | Cargo Feature | Enabled by default |
|---------|--------------|-------------------|
| NATS | `nats` | ✓ |
| Kafka | `kafka` | ✗ |
| Redis | `redis` | ✗ |
| RabbitMQ | `rabbitmq` | ✗ |
| SQS | `sqs` | ✗ |
| Webhook | `webhook` | ✓ |
| stdout | `stdout` | ✓ |

To build with specific backends:

```bash
# Only NATS and Kafka
cargo build --package pg-tide-relay --features "nats,kafka"

# All backends
cargo build --package pg-tide-relay --all-features
```

The official Docker image and GitHub release binaries include all backends.

---

## NATS

[NATS](https://nats.io) is the default and recommended backend for pg_tide. It provides extremely low-latency publish/subscribe messaging with optional JetStream durability. NATS is lightweight (single binary, no JVM), supports wildcards, and handles millions of messages per second.

**When to use NATS:** Real-time microservice communication, event fan-out, lightweight pub/sub where you want simplicity and speed. NATS is the "batteries included" choice for most pg_tide deployments.

### Forward (Outbox → NATS)

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-nats',
    'outbox', 'orders',
    'sink_type', 'nats',
    'config', jsonb_build_object(
      'url', 'nats://localhost:4222',
      'subject', 'orders.events'
    )
  )
);
```

#### Configuration

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `url` | Yes | — | NATS server URL (e.g., `nats://localhost:4222`) |
| `subject` | Yes | — | Subject to publish to. Supports template variables. |
| `credentials` | No | — | Path to NATS credentials file (`.creds`) for authentication |

#### Subject templates

The subject supports variable substitution from the message headers, allowing dynamic routing without relay-side logic:

- `{outbox_name}` — source outbox name
- `{event_type}` — value of the `event_type` key in the message headers
- `{outbox_id}` — the message ID (numeric)

**Example:** `"orders.{event_type}"` with a message that has `"event_type": "order.created"` in its headers will publish to subject `"orders.order.created"`.

This is powerful for fan-out patterns: a single outbox can route events to many different NATS subjects based on their type, and downstream services can subscribe to only the events they care about using NATS wildcards.

### Reverse (NATS → Inbox)

```sql
SELECT tide.relay_set_inbox_v2(
  jsonb_build_object(
    'name', 'nats-to-inbox',
    'inbox', 'incoming-events',
    'source', 'nats',
    'config', jsonb_build_object(
      'url', 'nats://localhost:4222',
      'subject', 'external.events.>'
    )
  )
);
```

#### Configuration

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `url` | Yes | — | NATS server URL |
| `subject` | Yes | — | Subject to subscribe to (NATS wildcards `*` and `>` supported) |
| `queue_group` | No | — | Queue group for load balancing across multiple relay instances |
| `credentials` | No | — | Path to NATS credentials file |

The `queue_group` option enables NATS queue subscriptions: if multiple relay instances subscribe to the same subject with the same queue group, NATS distributes messages among them (each message goes to one subscriber). This is useful for horizontal scaling of reverse pipelines.

### NATS operational notes

- NATS Core (without JetStream) provides at-most-once delivery — if no subscriber is active, messages are lost. For durable delivery, enable JetStream on your NATS server.
- The relay handles NATS reconnection automatically with exponential backoff.
- For multi-server NATS clusters, provide any single server URL — the NATS client discovers other servers automatically.

---

## Kafka

[Apache Kafka](https://kafka.apache.org) is the industry standard for high-throughput event streaming. If you're building data pipelines, event-driven architectures at scale, or need strong ordering guarantees with long-term retention, Kafka is the natural choice.

**When to use Kafka:** High-volume event streaming (>10K events/sec), data pipelines feeding analytics/ML systems, scenarios requiring strong ordering guarantees, or when Kafka is already part of your infrastructure.

### Forward (Outbox → Kafka)

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'events-kafka',
    'outbox', 'events',
    'sink_type', 'kafka',
    'config', jsonb_build_object(
      'brokers', 'broker1:9092,broker2:9092,broker3:9092',
      'topic', 'app-events',
      'acks', 'all',
      'compression', 'snappy',
      'key', '{event_type}'
    )
  )
);
```

#### Configuration

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `brokers` | Yes | — | Comma-separated list of Kafka broker addresses |
| `topic` | Yes | — | Target Kafka topic |
| `key` | No | — | Message key template (determines partition assignment). Supports `{outbox_name}`, `{event_type}`, etc. |
| `acks` | No | `all` | Acknowledgment level: `0` (fire-and-forget), `1` (leader only), `all` (all in-sync replicas) |
| `compression` | No | `none` | Compression: `none`, `gzip`, `snappy`, `lz4`, `zstd` |

**Key (partition) strategy:** The `key` field determines which Kafka partition each message goes to. Messages with the same key are guaranteed to land in the same partition, preserving ordering. Common patterns:

- `{event_type}` — all events of the same type go to the same partition
- `{outbox_name}` — partition by source outbox
- No key — round-robin distribution across partitions (best throughput, no ordering)

### Reverse (Kafka → Inbox)

```sql
SELECT tide.relay_set_inbox_v2(
  jsonb_build_object(
    'name', 'kafka-to-inbox',
    'inbox', 'kafka-events',
    'source', 'kafka',
    'config', jsonb_build_object(
      'brokers', 'broker1:9092,broker2:9092',
      'topic', 'external-events',
      'group_id', 'pg-tide-consumer'
    )
  )
);
```

#### Configuration

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `brokers` | Yes | — | Comma-separated broker list |
| `topic` | Yes | — | Kafka topic to consume from |
| `group_id` | Yes | — | Kafka consumer group ID (for offset tracking within Kafka) |
| `auto_offset_reset` | No | `earliest` | Where to start if no Kafka offset exists: `earliest` or `latest` |

### Kafka operational notes

- Building with Kafka support requires `librdkafka` (or the bundled cmake build via `rdkafka/cmake-build` feature).
- Use `acks=all` for durability in production — it ensures the message is replicated before acknowledgment.
- For high throughput, use `snappy` or `lz4` compression and increase `p_batch_size` to 200-500.
- The relay commits Kafka consumer offsets back to Kafka (for reverse pipelines) in addition to tracking them in pg_tide's own offset table.

---

## Redis Streams

[Redis Streams](https://redis.io/docs/data-types/streams/) provide lightweight event streaming with consumer groups built on top of Redis. If you already run Redis and need simple, fast event delivery without the operational overhead of Kafka, Redis Streams is an excellent choice.

**When to use Redis:** You already have Redis in your stack, you need low-latency delivery, your event volume is moderate (<50K/sec), or you want minimal additional infrastructure.

### Forward (Outbox → Redis Stream)

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'events-redis',
    'outbox', 'events',
    'sink_type', 'redis',
    'config', jsonb_build_object(
      'url', 'redis://localhost:6379',
      'stream', 'app:events',
      'maxlen', 100000
    )
  )
);
```

#### Configuration

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `url` | Yes | — | Redis connection URL (e.g., `redis://localhost:6379`) |
| `stream` | Yes | — | Redis stream key name |
| `maxlen` | No | — | Maximum stream length. Older entries are trimmed automatically (Redis `MAXLEN`). |

### Reverse (Redis Stream → Inbox)

```sql
SELECT tide.relay_set_inbox_v2(
  jsonb_build_object(
    'name', 'redis-to-inbox',
    'inbox', 'redis-events',
    'source', 'redis',
    'config', jsonb_build_object(
      'url', 'redis://localhost:6379',
      'stream', 'external:events',
      'group', 'pg-tide',
      'consumer', 'relay-0'
    )
  )
);
```

#### Configuration

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `url` | Yes | — | Redis connection URL |
| `stream` | Yes | — | Stream key to read from |
| `group` | Yes | — | Redis consumer group name |
| `consumer` | Yes | — | Consumer name within the group (should be unique per relay instance) |

### Redis operational notes

- Redis Streams durability depends on your Redis persistence configuration (AOF, RDB, or none). For production event delivery, enable AOF with `appendfsync everysec`.
- Use `maxlen` to prevent unbounded stream growth. Redis will trim the oldest entries when the limit is reached.
- For Redis Cluster, the relay connects to the cluster and routes to the correct shard based on the stream key.

---

## RabbitMQ

[RabbitMQ](https://www.rabbitmq.com) provides rich message routing through exchanges, bindings, and queues. It's ideal when you need complex routing patterns (topic-based, header-based, fanout) or when you're integrating with existing AMQP infrastructure.

**When to use RabbitMQ:** Complex routing requirements, existing AMQP infrastructure, work queue patterns where messages should be processed by exactly one consumer, or when you need message priority and TTL features.

### Forward (Outbox → RabbitMQ)

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'events-rabbit',
    'outbox', 'events',
    'sink_type', 'rabbitmq',
    'config', jsonb_build_object(
      'url', 'amqp://user:pass@localhost:5672/%2f',
      'exchange', 'app.events',
      'routing_key', 'orders.created',
      'exchange_type', 'topic',
      'durable', true
    )
  )
);
```

#### Configuration

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `url` | Yes | — | AMQP connection URL |
| `exchange` | Yes | — | Exchange to publish to |
| `routing_key` | No | `""` | Routing key for the exchange. Supports template variables like `{event_type}`. |
| `exchange_type` | No | `topic` | Exchange type: `direct`, `topic`, `fanout`, `headers` |
| `durable` | No | `true` | Whether messages should be persisted to disk |

### Reverse (RabbitMQ → Inbox)

```sql
SELECT tide.relay_set_inbox_v2(
  jsonb_build_object(
    'name', 'rabbit-to-inbox',
    'inbox', 'amqp-events',
    'source', 'rabbitmq',
    'config', jsonb_build_object(
      'url', 'amqp://user:pass@localhost:5672/%2f',
      'queue', 'incoming-events',
      'prefetch', 20
    )
  )
);
```

#### Configuration

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `url` | Yes | — | AMQP connection URL |
| `queue` | Yes | — | Queue to consume from |
| `prefetch` | No | `10` | How many messages to prefetch (controls parallelism and memory) |

### RabbitMQ operational notes

- For the URL, use `%2f` for the default vhost (`/`): `amqp://user:pass@host:5672/%2f`
- The relay declares the exchange if it doesn't exist (for forward mode). For reverse mode, the queue must already exist.
- Use `durable=true` in production to survive broker restarts.
- RabbitMQ's topic exchange routing keys support wildcards: `orders.*` matches `orders.created`, `orders.shipped`, etc.

---

## SQS

[Amazon SQS](https://aws.amazon.com/sqs/) is a fully managed message queue service. It requires zero infrastructure management and integrates seamlessly with the AWS ecosystem (Lambda, ECS, Step Functions).

**When to use SQS:** AWS-native infrastructure, serverless consumers (Lambda), when you want zero queue management overhead, or when your team already uses AWS services extensively.

### Forward (Outbox → SQS)

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'events-sqs',
    'outbox', 'events',
    'sink_type', 'sqs',
    'config', jsonb_build_object(
      'queue_url', 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
      'region', 'us-east-1'
    )
  )
);
```

#### Configuration

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `queue_url` | Yes | — | Full SQS queue URL |
| `region` | Yes | — | AWS region |
| `message_group_id` | No | — | Required for FIFO queues. Messages with the same group ID are delivered in order. |
| `delay_seconds` | No | `0` | Delivery delay (0-900 seconds) |

### Reverse (SQS → Inbox)

```sql
SELECT tide.relay_set_inbox_v2(
  jsonb_build_object(
    'name', 'sqs-to-inbox',
    'inbox', 'sqs-events',
    'source', 'sqs',
    'config', jsonb_build_object(
      'queue_url', 'https://sqs.us-east-1.amazonaws.com/123456789012/incoming',
      'region', 'us-east-1',
      'wait_time_seconds', 20,
      'max_messages', 10
    )
  )
);
```

#### Configuration

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `queue_url` | Yes | — | Full SQS queue URL |
| `region` | Yes | — | AWS region |
| `wait_time_seconds` | No | `20` | Long-poll wait time (reduces API calls, max 20s) |
| `max_messages` | No | `10` | Max messages per receive call (1-10) |

### SQS authentication

The relay uses the standard AWS credential chain (in priority order):

1. **Environment variables:** `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
2. **AWS config/credentials files:** `~/.aws/credentials`
3. **IAM instance role:** Automatic on EC2/ECS/Lambda
4. **EKS IRSA:** Via web identity token (recommended for Kubernetes)

For EKS deployments, use IAM Roles for Service Accounts (IRSA) to avoid managing credentials directly.

### SQS operational notes

- Use **FIFO queues** when ordering matters. Set `message_group_id` to group related messages (e.g., by customer ID or order ID).
- Standard queues provide higher throughput but best-effort ordering.
- Long polling (`wait_time_seconds: 20`) reduces costs by minimizing empty receives.
- SQS has a 256 KB message size limit. For larger payloads, consider the claim-check pattern: store the payload in S3 and put a reference in the SQS message.

---

## Webhook

HTTP webhooks are the universal integration mechanism — any system with an HTTP endpoint can receive events from pg_tide, and any system that can send HTTP requests can push events into a pg_tide inbox.

**When to use Webhooks:** Third-party integrations (Stripe, Twilio, GitHub), push notifications to serverless functions, integrating with systems that don't support native messaging protocols, or receiving events from external services.

### Forward (Outbox → HTTP Webhook)

Delivers outbox messages as HTTP POST requests to a configured URL:

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'events-webhook',
    'outbox', 'events',
    'sink_type', 'webhook',
    'config', jsonb_build_object(
      'url', 'https://api.example.com/webhooks/events',
      'timeout_ms', 5000,
      'headers', '{"Authorization": "Bearer token123", "X-Source": "pg-tide"}'
    )
  )
);
```

#### Configuration

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `url` | Yes | — | Webhook endpoint URL (HTTPS recommended) |
| `timeout_ms` | No | `30000` | Request timeout in milliseconds |
| `headers` | No | `{}` | Additional HTTP headers as a JSON object |
| `method` | No | `POST` | HTTP method |
| `retry_codes` | No | `[429, 500, 502, 503, 504]` | HTTP status codes that trigger retry |

#### Request format

The relay sends each message as a JSON POST request:

```http
POST /webhooks/events HTTP/1.1
Content-Type: application/json
X-PgTide-Dedup-Key: orders:42:0
X-PgTide-Event-Type: order.created
Authorization: Bearer token123

{"order_id": 42, "total": 99.99}
```

The `X-PgTide-Dedup-Key` header allows the receiver to implement idempotency. The `X-PgTide-Event-Type` header carries the event type from the outbox message headers.

### Reverse (HTTP Webhook → Inbox)

Exposes an HTTP endpoint that accepts incoming webhook deliveries and writes them to an inbox:

```sql
SELECT tide.relay_set_inbox_v2(
  jsonb_build_object(
    'name', 'webhook-receiver',
    'inbox', 'incoming-hooks',
    'source', 'webhook',
    'config', jsonb_build_object(
      'port', 8080,
      'path', '/webhooks/incoming',
      'auth_header', 'Bearer whsec_your_secret'
    )
  )
);
```

#### Configuration

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `port` | No | `8080` | Port for the HTTP listener |
| `path` | No | `/` | URL path to accept requests on |
| `auth_header` | No | — | Expected `Authorization` header value. Requests without this are rejected with 401. |

#### Dedup key extraction

For incoming webhooks, the relay extracts a dedup key from these headers (in priority order):

1. `X-Request-ID`
2. `X-Idempotency-Key`
3. `X-Webhook-ID`
4. Auto-generated UUID (fallback — use only if the sender doesn't provide idempotency keys)

The extracted key becomes the `event_id` in the inbox table, enabling deduplication of retried webhook deliveries.

### Webhook operational notes

- Always use HTTPS for outbound webhooks in production (sensitive data in payloads, authentication headers).
- Set `retry_codes` to match the receiver's error semantics. 429 (rate limited) should always trigger retry.
- For inbound webhooks, validate the `auth_header` to prevent unauthorized writes to your inbox.
- Consider using a short `timeout_ms` (5000ms) for forward webhooks to avoid holding relay resources on slow receivers.

---

## stdout and stdin (Development)

For development, testing, and debugging, the relay includes `stdout` (forward) and `stdin` (reverse) backends.

**stdout** prints delivered messages to the relay's standard output — useful for verifying that your pipeline configuration works without setting up an external system:

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'debug-pipeline',
    'outbox', 'events',
    'sink_type', 'stdout'
  )
);
```

**stdin** reads messages from standard input — useful for manual testing of inbox processing:

```bash
echo '{"event_id": "test-1", "payload": {"hello": "world"}}' | pg-tide --stdin-pipeline my-inbox
```

---

## Common Configuration Patterns

### TLS/SSL for all backends

Most backends support TLS via their URL scheme:

```sql
-- NATS with TLS
'url', 'nats://nats.example.com:4443'  -- with credentials file for mutual TLS

-- Kafka with SSL
'brokers', 'broker1:9093'  -- SSL port, configured via librdkafka

-- Redis with TLS
'url', 'rediss://redis.example.com:6380'  -- note: rediss:// (double-s)

-- RabbitMQ with TLS
'url', 'amqps://user:pass@rabbit.example.com:5671/%2f'  -- amqps:// scheme

-- Webhook with TLS
'url', 'https://api.example.com/webhooks'  -- always HTTPS in production
```

### Naming conventions for pipelines

Choose pipeline names that describe the data flow clearly:

```sql
-- Good: describes source and destination
'orders-to-kafka'
'payment-webhooks-to-inbox'
'inventory-nats-fanout'

-- Bad: vague or too generic
'pipeline-1'
'my-pipeline'
'test'
```

### Batching strategy

All forward pipelines support `p_batch_size`. The right batch size depends on your backend:

| Backend | Recommended batch size | Rationale |
|---------|----------------------|-----------|
| NATS | 50-100 | NATS is fast per-message; small batches keep latency low |
| Kafka | 200-500 | Kafka benefits from batching (compression, fewer round-trips) |
| Redis | 100-200 | XADD is fast but benefits from pipelining |
| RabbitMQ | 50-100 | Per-message confirms; moderate batches balance throughput and latency |
| SQS | 10 | SQS `SendMessageBatch` supports max 10 messages |
| Webhook | 1-10 | HTTP round-trips are expensive; batch if the receiver supports it |
