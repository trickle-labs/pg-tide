# NATS JetStream

NATS is a lightweight, high-performance messaging system designed for cloud-native applications. JetStream is NATS's built-in persistence layer that adds durable message storage, replay capabilities, and exactly-once delivery semantics to the core NATS protocol. When you connect pg_tide to NATS JetStream, your PostgreSQL outbox messages are delivered with sub-millisecond latency to any service subscribed to the relevant subjects, while JetStream ensures messages are persisted and can be replayed if a consumer was offline.

NATS is particularly well-suited for microservice architectures where you need fast, reliable communication between services without the operational complexity of running a Kafka cluster. Its subject-based addressing model makes routing intuitive, and its lightweight footprint means you can run it anywhere — from a single container in development to a globally distributed supercluster in production.

## When to Use This Sink

Choose the NATS JetStream sink when your architecture values simplicity and speed:

- **Low-latency messaging** — NATS delivers messages in microseconds. If your downstream services need near-real-time notification of database changes, NATS is one of the fastest options available.
- **Simple operations** — NATS is a single binary with minimal configuration. Unlike Kafka, there is no ZooKeeper, no partition management, and no broker coordination to think about.
- **Subject-based routing** — NATS's hierarchical subject naming (e.g., `orders.created`, `orders.shipped`) provides natural topic routing without needing separate topic creation steps.
- **Microservice communication** — When your services communicate through events and you want a lightweight broker that scales horizontally with minimal fuss.
- **Cloud-native deployments** — NATS has first-class support for Kubernetes, runs efficiently in containers, and supports leaf nodes for edge computing scenarios.

Consider Kafka instead if you need very long retention periods (weeks/months), strict partition-level ordering guarantees, or compatibility with the Kafka ecosystem (Connect, Streams, ksqlDB).

## How It Works

The relay connects to a NATS server (or cluster) and publishes messages to JetStream subjects. JetStream provides durable storage, so messages are persisted even if no consumer is currently subscribed. The flow is:

1. The relay fetches a batch of undelivered messages from the outbox.
2. Each message is published to the configured NATS subject (which can be templated per-message).
3. JetStream acknowledges persistence of each message.
4. The relay commits the consumer group offset in PostgreSQL.

NATS JetStream supports message deduplication based on a `Nats-Msg-Id` header. pg_tide automatically sets this header to the outbox message's dedup_key, which means that even if the relay retries a publish (after a network interruption, for example), NATS will not create duplicate messages in the stream.

## Configuration

### Minimal Configuration

```sql
SELECT tide.relay_set_outbox(
    'orders-to-nats',
    'orders',
    'nats-relay',
    '{
        "sink_type": "nats",
        "url": "nats://localhost:4222",
        "subject": "orders.events"
    }'::jsonb
);
```

### Production Configuration

```sql
SELECT tide.relay_set_outbox(
    'orders-to-nats',
    'orders',
    'nats-relay',
    '{
        "sink_type": "nats",
        "url": "${env:NATS_URL}",
        "subject": "events.{stream_table}.{op}",
        "credentials_file": "${env:NATS_CREDS_FILE}",
        "tls_enabled": true,
        "tls_ca_cert": "/etc/certs/nats-ca.pem",
        "stream": "EVENTS",
        "batch_size": 200
    }'::jsonb
);
```

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sink_type` | string | — | Must be `"nats"` |
| `url` | string | — | NATS server URL(s). Comma-separated for clusters: `"nats://host1:4222,nats://host2:4222"` |
| `subject` | string | — | Target subject. Supports templates: `{stream_table}`, `{op}`, `{outbox_id}` |
| `stream` | string | `null` | JetStream stream name (auto-detected from subject if not specified) |
| `credentials_file` | string | `null` | Path to NATS credentials file (`.creds`) |
| `nkey_seed` | string | `null` | NKey seed for authentication |
| `token` | string | `null` | Authentication token |
| `username` | string | `null` | Username for user/password auth |
| `password` | string | `null` | Password for user/password auth |
| `tls_enabled` | bool | `false` | Enable TLS |
| `tls_ca_cert` | string | `null` | CA certificate path |
| `batch_size` | int | `100` | Messages per batch |

## Authentication

### No Authentication (Development)

For local development:

```json
{
    "sink_type": "nats",
    "url": "nats://localhost:4222",
    "subject": "dev.events"
}
```

### Credentials File (NATS.io Cloud / Production)

NATS credentials files contain both the JWT and the NKey seed. This is the recommended authentication method for NATS.io's managed service (Synadia Cloud):

```json
{
    "sink_type": "nats",
    "url": "tls://connect.ngs.global",
    "subject": "myapp.events",
    "credentials_file": "/etc/nats/user.creds"
}
```

### NKey Authentication

NKeys provide public-key authentication without passwords:

```json
{
    "sink_type": "nats",
    "url": "nats://nats-server:4222",
    "subject": "events",
    "nkey_seed": "${env:NATS_NKEY_SEED}",
    "tls_enabled": true
}
```

### Token Authentication

Simple token-based auth for smaller deployments:

```json
{
    "sink_type": "nats",
    "url": "nats://nats-server:4222",
    "subject": "events",
    "token": "${env:NATS_TOKEN}"
}
```

## Delivery Guarantees

The NATS JetStream sink provides **exactly-once delivery** when properly configured. This is achieved through the combination of:

1. **JetStream message deduplication** — pg_tide sets the `Nats-Msg-Id` header to the message's dedup_key. JetStream tracks published message IDs within its deduplication window and rejects duplicates silently.
2. **Outbox offset tracking** — The relay only commits offsets after JetStream acknowledges persistence.

This means that even if the relay crashes and restarts, re-published messages will be deduplicated by JetStream, preventing downstream consumers from seeing duplicates.

## Subject Routing

NATS subjects use a dot-separated hierarchical namespace that makes routing intuitive. pg_tide's template variables map naturally to this model:

```
events.orders.insert     → new orders
events.orders.update     → order status changes  
events.payments.insert   → new payments
events.*.delete          → all deletes (wildcard subscription)
```

Configure dynamic subject routing with:

```json
{
    "subject": "events.{stream_table}.{op}"
}
```

Downstream services can subscribe to exactly the events they care about using NATS wildcards (`*` for single token, `>` for multiple tokens).

## Complete Example

### 1. Create the Outbox

```sql
SELECT tide.outbox_create('notifications', retention_hours => 24);
```

### 2. Configure the Pipeline

```sql
SELECT tide.relay_set_outbox(
    'notify-pipeline',
    'notifications',
    'nats-group',
    '{
        "sink_type": "nats",
        "url": "nats://localhost:4222",
        "subject": "notifications.{op}",
        "stream": "NOTIFICATIONS"
    }'::jsonb
);
SELECT tide.relay_enable('notify-pipeline');
```

### 3. Publish an Event

```sql
SELECT tide.outbox_publish(
    'notifications',
    '{"type": "order.shipped", "order_id": "ord-555", "customer": "alice@example.com"}'::jsonb,
    'ord-555-shipped'
);
```

### 4. Verify with NATS CLI

```bash
nats sub "notifications.>"
# Output: [notifications.insert] {"type": "order.shipped", ...}
```

## Troubleshooting

### "Connection refused"

NATS server is not reachable:
- Check the URL includes the correct port (default 4222)
- Verify network connectivity and firewall rules
- For NATS clusters, ensure at least one seed server is accessible

### "Authorization violation"

Authentication or authorization failed:
- Verify credentials file path exists and is readable
- Check that the user/account has publish permission on the target subject
- For NKey auth, ensure the seed matches the configured user

### "No responders" or "Stream not found"

JetStream is not configured for the target subject:
- Create the JetStream stream: `nats stream add EVENTS --subjects "events.>"`
- Or set the `stream` parameter to match an existing stream
- Verify JetStream is enabled on the NATS server (`jetstream: enabled` in config)

## Further Reading

- [Sources: NATS](../sources/nats.md) — Consuming from NATS into a pg_tide inbox
- [Content-Based Routing](../features/routing.md) — Advanced subject routing patterns
- [Bidirectional Sync Tutorial](../tutorials/bidirectional-sync.md) — Using NATS for two-way communication
