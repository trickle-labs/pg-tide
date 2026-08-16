# Apache Kafka

Apache Kafka is a distributed event streaming platform that serves as the backbone of real-time data architectures at thousands of organizations worldwide. Originally developed at LinkedIn and now maintained by the Apache Software Foundation, Kafka excels at handling high-throughput, fault-tolerant, ordered streams of events. When you connect pg_tide to Kafka, every message published to your PostgreSQL outbox is automatically delivered to Kafka topics, making your database changes available to any downstream system that speaks the Kafka protocol — from stream processors like Apache Flink to data warehouses like Snowflake.

If your organization already uses Kafka, connecting pg_tide is the simplest way to get your PostgreSQL events into the broader event-driven ecosystem without writing custom producer code or managing CDC infrastructure like Debezium. If you are evaluating message brokers, Kafka is an excellent choice when you need durable, ordered, replayable event streams at scale.

## When to Use This Sink

Choose the Kafka sink when you need one or more of the following:

- **High throughput** — Kafka handles millions of messages per second across partitioned topics. If your outbox produces thousands of events per second, Kafka will keep up without breaking a sweat.
- **Durable replay** — Kafka retains messages for a configurable period (days, weeks, or forever with log compaction). Downstream consumers can replay the entire history or start from any point in time.
- **Multiple consumers** — Many different services need to independently consume the same stream of events. Kafka's consumer group model makes this natural.
- **Existing Kafka ecosystem** — Your organization already runs Kafka and your downstream consumers (Flink, ksqlDB, Materialize, Connect) expect Kafka topics.
- **CDC compatibility** — You want to produce Debezium-formatted change events that existing CDC-aware tools can consume natively.

Consider a different sink if you need sub-millisecond latency (NATS is faster for point-to-point), if you want zero operational overhead (SQS/Pub/Sub are fully managed), or if your total message volume is very low (Kafka's operational cost may not be justified).

## How It Works

When the relay processes a batch of outbox messages destined for Kafka, it performs the following steps:

1. **Fetch** — The relay polls the outbox table for unacknowledged messages belonging to this pipeline's consumer group.
2. **Transform** — If JMESPath transforms are configured, the relay applies filter and projection expressions to each message payload.
3. **Encode** — Messages are serialized according to the configured wire format (native JSON, Debezium, or Avro via Schema Registry).
4. **Route** — The relay determines the target Kafka topic for each message using the configured topic template (static or dynamic based on message content).
5. **Produce** — Messages are sent to Kafka using the configured producer settings (compression, batching, acknowledgment level).
6. **Acknowledge** — Once Kafka confirms receipt (based on the `acks` setting),
   the relay commits the source checkpoint. This is at-least-once transport;
   Kafka producer idempotence does not make downstream consumer processing
   exactly once.

If any step fails, the relay retries with exponential backoff. If retries are exhausted, failed messages are routed to the dead-letter queue.

```mermaid
sequenceDiagram
    participant App as Application
    participant PG as PostgreSQL
    participant Relay as pg-tide relay
    participant Kafka as Kafka Cluster

    App->>PG: INSERT order + outbox_publish()
    Note over PG: Single transaction
    Relay->>PG: Poll outbox (batch)
    PG-->>Relay: Messages batch
    Relay->>Kafka: Produce (compressed, batched)
    Kafka-->>Relay: Ack (all replicas)
    Relay->>PG: Commit offset
```

## Configuration

### Minimal Configuration

The simplest possible Kafka sink configuration requires only the broker addresses and a topic name:

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-to-kafka',
    'outbox', 'orders',
    'sink_type', 'kafka',
    'config', '{
        "brokers": "localhost:9092",
        "topic": "order-events"
    }'::jsonb
  )
);
```

This connects to a local Kafka cluster without authentication, sends all messages to the `order-events` topic, and uses default producer settings. This is appropriate for development but not for production.

### Production Configuration

A production-ready configuration includes authentication, compression, and tuned producer settings:

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-to-kafka',
    'outbox', 'orders',
    'sink_type', 'kafka',
    'config', '{
        "brokers": "${env:KAFKA_BROKERS}",
        "topic": "orders.events.{op}",
        "sasl_mechanism": "SCRAM-SHA-256",
        "sasl_username": "${env:KAFKA_USERNAME}",
        "sasl_password": "${env:KAFKA_PASSWORD}",
        "tls_enabled": true,
        "compression": "zstd",
        "acks": "all",
        "batch_size": 500,
        "linger_ms": 50,
        "idempotent": true,
        "request_timeout_ms": 30000
    }'::jsonb
  )
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"kafka"` |
| `brokers` | string | — | Comma-separated list of broker addresses (`host:port`) |
| `topic` | string | — | Target topic name. Supports template variables: `{stream_table}`, `{op}`, `{outbox_id}` |
| `sasl_mechanism` | string | `null` | Authentication mechanism: `"PLAIN"`, `"SCRAM-SHA-256"`, `"SCRAM-SHA-512"` |
| `sasl_username` | string | `null` | SASL username |
| `sasl_password` | string | `null` | SASL password |
| `tls_enabled` | bool | `false` | Enable TLS for broker connections |
| `tls_ca_cert` | string | `null` | Path to CA certificate file for TLS verification |
| `tls_client_cert` | string | `null` | Path to client certificate for mTLS authentication |
| `tls_client_key` | string | `null` | Path to client private key for mTLS authentication |
| `compression` | string | `"none"` | Compression codec: `"none"`, `"gzip"`, `"snappy"`, `"lz4"`, `"zstd"` |
| `acks` | string | `"all"` | Acknowledgment level: `"0"` (fire-and-forget), `"1"` (leader only), `"all"` (all ISR replicas) |
| `batch_size` | int | `100` | Maximum messages per produce request |
| `linger_ms` | int | `10` | Time to wait for batch to fill before sending |
| `idempotent` | bool | `false` | Enable idempotent producer (prevents duplicates on retry) |
| `request_timeout_ms` | int | `30000` | Timeout for produce requests |
| `message_key` | string | `null` | Template for Kafka message key. Determines partition assignment. Supports `{dedup_key}`, `{stream_table}` |
| `headers` | object | `null` | Static headers to include on every message |

## Authentication

### No Authentication (Development Only)

For local development with an unsecured Kafka cluster, no authentication configuration is needed. Simply provide the broker addresses:

```json
{
    "sink_type": "kafka",
    "brokers": "localhost:9092",
    "topic": "dev-events"
}
```

This is **not recommended** for any environment accessible over a network.

### SASL/PLAIN (Confluent Cloud)

Confluent Cloud and many managed Kafka services use SASL/PLAIN over TLS. This requires a username (API key) and password (API secret):

```json
{
    "sink_type": "kafka",
    "brokers": "${env:CONFLUENT_BOOTSTRAP_SERVERS}",
    "topic": "my-topic",
    "sasl_mechanism": "PLAIN",
    "sasl_username": "${env:CONFLUENT_API_KEY}",
    "sasl_password": "${env:CONFLUENT_API_SECRET}",
    "tls_enabled": true
}
```

### SASL/SCRAM-SHA-256

Self-hosted Kafka clusters often use SCRAM-SHA-256 for username/password authentication with stronger security than PLAIN:

```json
{
    "sink_type": "kafka",
    "brokers": "kafka-1:9093,kafka-2:9093,kafka-3:9093",
    "topic": "events",
    "sasl_mechanism": "SCRAM-SHA-256",
    "sasl_username": "${env:KAFKA_USER}",
    "sasl_password": "${env:KAFKA_PASS}",
    "tls_enabled": true
}
```

### mTLS (Certificate-Based)

For environments requiring mutual TLS authentication (common in financial services and regulated industries), provide client certificates:

```json
{
    "sink_type": "kafka",
    "brokers": "kafka-1:9093,kafka-2:9093",
    "topic": "secure-events",
    "tls_enabled": true,
    "tls_ca_cert": "/etc/certs/ca.pem",
    "tls_client_cert": "/etc/certs/client.pem",
    "tls_client_key": "/etc/certs/client-key.pem"
}
```

## Message Format

Each outbox message becomes a Kafka record with the following mapping:

| Kafka Record Field | Source | Example |
|-------------------|--------|---------|
| **Key** | `message_key` template or `dedup_key` | `"order-12345"` |
| **Value** | Serialized message payload (JSON by default) | `{"order_id": 12345, ...}` |
| **Topic** | `topic` template | `"orders.events.insert"` |
| **Headers** | pg_tide metadata + configured static headers | `pg_tide_outbox: "orders"`, `pg_tide_op: "insert"` |

### Topic Routing

The `topic` field supports template variables that are resolved per-message:

- `{stream_table}` — The outbox name (e.g., `orders`)
- `{op}` — The operation type (`insert`, `update`, `delete`)
- `{outbox_id}` — The unique outbox message ID

For example, `"events.{stream_table}.{op}"` routes INSERT messages from the `orders` outbox to the topic `events.orders.insert`.

### Wire Format Integration

When using the Debezium wire format, messages are produced in Debezium envelope format, making them compatible with tools like Apache Iceberg's Debezium sink connector, Flink CDC, ksqlDB, and Materialize:

```json
{
    "sink_type": "kafka",
    "brokers": "localhost:9092",
    "topic": "dbserver1.public.orders",
    "wire_format": "debezium"
}
```

See the [Debezium Wire Format](../wire-formats/debezium.md) page for details on the message structure.

## Delivery Guarantees

The Kafka sink provides **at-least-once transport** by default. With the
`idempotent` producer enabled, Kafka can deduplicate retried produce requests
within the producer/broker contract; this does not make downstream processing
exactly once.

Combined with pg_tide's consumer group offset tracking, this means:

- A message is published to Kafka at least once (at-least-once from outbox to Kafka)
- With `idempotent: true`, Kafka deduplicates retried produces (effectively exactly-once on the produce side)
- If the downstream consumer also uses an idempotent inbox, the application can
  achieve an effectively exactly-once outcome

### Acknowledgment Levels

The `acks` setting controls when Kafka considers a produce successful:

- **`"0"`** — The relay does not wait for any acknowledgment. Fastest, but messages can be lost if the leader fails before replicating.
- **`"1"`** — The leader broker acknowledges after writing to its local log. Messages can be lost if the leader fails before followers replicate.
- **`"all"`** — All in-sync replicas (ISR) must acknowledge. No data loss as long as at least one replica survives. **Recommended for production.**

## Performance Tuning

### Batch Size and Linger

The relay collects messages into batches before sending to Kafka. Larger batches improve throughput but increase latency:

- **`batch_size: 100`** (default) — Good balance for most workloads
- **`batch_size: 500-1000`** — Better throughput for high-volume pipelines
- **`linger_ms: 50-100`** — Wait longer to fill batches; reduces request count at the cost of latency

### Compression

Compression reduces network bandwidth and Kafka storage at the cost of CPU:

- **`"zstd"`** — Best compression ratio, good speed. Recommended for most workloads.
- **`"lz4"`** — Fastest compression/decompression, moderate ratio. Best when CPU is constrained.
- **`"snappy"`** — Good balance, widely supported. Safe default.
- **`"gzip"`** — Highest ratio but slowest. Use only when bandwidth is extremely limited.

### Expected Throughput

Under typical conditions with `acks: "all"` and `compression: "zstd"`:

| Batch Size | Messages/sec | Latency (p99) |
|------------|-------------|---------------|
| 100 | ~5,000 | ~50ms |
| 500 | ~20,000 | ~100ms |
| 1000 | ~40,000 | ~200ms |

Actual numbers depend on message size, network latency, Kafka cluster capacity, and replication factor.

## Complete Example: Order Events to Kafka

This example demonstrates a complete pipeline from order creation in PostgreSQL to delivery on a Kafka topic.

### 1. Set Up the Outbox

```sql
-- Create the orders outbox with 48-hour retention
SELECT tide.outbox_create('orders', retention_hours => 48);
```

### 2. Publish Messages from Your Application

```sql
-- In your order processing transaction:
BEGIN;
INSERT INTO orders (id, customer_id, total, status)
VALUES (gen_random_uuid(), 'cust-789', 99.99, 'confirmed');

SELECT tide.outbox_publish(
    'orders',
    jsonb_build_object(
        'event_type', 'order.confirmed',
        'order_id', 'ord-12345',
        'customer_id', 'cust-789',
        'total', 99.99,
        'items', jsonb_build_array(
            jsonb_build_object('sku', 'WIDGET-A', 'qty', 2)
        )
    ),
    'ord-12345'  -- dedup_key
);
COMMIT;
```

### 3. Configure the Pipeline

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-to-kafka',
    'outbox', 'orders',
    'sink_type', 'kafka',
    'config', '{
        "brokers": "kafka:9092",
        "topic": "order-events",
        "compression": "zstd",
        "acks": "all",
        "idempotent": true,
        "batch_size": 100
    }'::jsonb
  )
);
SELECT tide.relay_enable('orders-to-kafka');
```

### 4. Start the Relay

```bash
pg-tide --postgres-url "postgresql://relay_user:password@localhost:5432/mydb"
```

### 5. Verify Messages Arrive

Using the Kafka console consumer:

```bash
kafka-console-consumer \
    --bootstrap-server kafka:9092 \
    --topic order-events \
    --from-beginning
```

You should see your order event messages arriving as JSON payloads.

## Compatibility

The pg_tide Kafka sink is compatible with:

- **Apache Kafka** 2.8+ (including KRaft mode)
- **Confluent Cloud** (fully managed)
- **Confluent Platform** (self-managed)
- **Amazon MSK** (with IAM or SASL auth)
- **Redpanda** (Kafka-compatible API)
- **Aiven for Kafka**
- **Upstash Kafka** (serverless)

## Troubleshooting

### "Connection refused" or "Broker not available"

The relay cannot reach the Kafka brokers. Check:
- Broker addresses are correct and include ports
- Network connectivity exists (firewall rules, security groups, VPC peering)
- DNS resolution works for broker hostnames
- TLS is enabled if the cluster requires it

### "SASL authentication failed"

Authentication credentials are incorrect or misconfigured:
- Verify `sasl_mechanism` matches what the cluster expects
- Check that environment variables containing credentials are set
- For Confluent Cloud, ensure you're using the API key (not the cluster ID) as the username

### "Topic does not exist"

The target topic has not been created and auto-creation is disabled on the cluster:
- Create the topic manually: `kafka-topics --create --topic order-events --partitions 6 --replication-factor 3`
- Or enable `auto.create.topics.enable=true` on the cluster (not recommended for production)

### "Message too large"

The message payload exceeds `max.message.bytes` on the broker:
- Check your message payload sizes
- Increase `max.message.bytes` on the broker/topic configuration
- Consider using JMESPath projections to reduce payload size before delivery

### Messages delivered but not in expected order

Kafka guarantees ordering only within a single partition. If you need message ordering:
- Set `message_key` to a field that should determine ordering (e.g., `{dedup_key}` for per-entity ordering)
- Messages with the same key always go to the same partition

## Further Reading

- [Wire Formats](../wire-formats/overview.md) — Produce Debezium, Avro, or custom formats to Kafka
- [Content-Based Routing](../features/routing.md) — Route different event types to different topics
- [Schema Registry](../features/schema-registry.md) — Enforce Avro/Protobuf schemas on produced messages
- [Dead-Letter Queue](../features/dead-letter-queue.md) — Handle delivery failures gracefully
- [Circuit Breaker](../features/circuit-breaker.md) — Protect against Kafka cluster outages
