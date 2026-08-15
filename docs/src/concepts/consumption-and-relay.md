# Consumption and Relay

Once messages are safely stored in the transactional outbox, they need to be delivered to downstream systems and tracked independently by each consumer. This page explains how pg_tide's consumer groups and relay pipelines work together to provide reliable, independent message consumption with automatic failover.

---

## Consumer Groups: Independent Bookmarks in a Shared Stream

Think of a consumer group as a bookmark in a shared book. Multiple services might be interested in the same stream of outbox messages — one service sends emails, another updates a search index, a third feeds an analytics pipeline. Each of these services needs to track its own progress independently. That's exactly what a consumer group provides: an independent offset that records how far a particular consumer has read through the outbox.

If the email service crashes and restarts, it picks up right where it left off — at its own bookmark — without replaying messages that the analytics service has already processed, and without skipping messages that it hasn't yet seen.

### Core concepts

A consumer group has these key properties:

- **Independent progress** — different groups read the same outbox at their own pace. The email sender might be at offset 500 while the analytics pipeline is at offset 2000. They don't interfere with each other.
- **Offset tracking** — each group records the ID of the last message it successfully processed. On restart, consumption resumes from that exact position.
- **Heartbeats** — consumers periodically signal that they're alive. Stale heartbeats indicate a dead consumer whose work might need to be redistributed.
- **Visibility leases** — when a consumer claims a batch of messages, it takes a time-limited lease. If it fails to commit the offset before the lease expires, the messages become available for another consumer to process.

### Creating a consumer group

```sql
SELECT tide.create_consumer_group('email-sender', 'orders',
  p_auto_offset_reset := 'earliest'
);
```

The `p_auto_offset_reset` parameter determines where consumption starts if no offset has been committed yet:

| Value | Behavior | When to use |
|-------|----------|-------------|
| `earliest` | Start from the very first available message in the outbox | When you want to process the complete history — common for new consumers that need to catch up |
| `latest` | Start from the current end of the outbox (skip historical messages) | When you only care about future events — useful for real-time notification services |
| `none` | Raise an error if no committed offset exists | When you want to be explicit and prevent accidentally processing from the wrong position |

### Committing offsets

After your application (or the relay) successfully processes a batch of messages, it commits the offset to record its progress:

```sql
SELECT tide.commit_offset('email-sender', 'worker-1', 42);
```

This records that `worker-1` in the `email-sender` group has processed all messages up to and including ID 42. On restart, this consumer will resume from message 43.

Offset commits are idempotent — committing the same offset twice is a no-op. They're also monotonic within a consumer — you should only ever commit a higher offset than the previous one.

### Heartbeats and liveness

Consumers periodically send heartbeats to signal that they're alive and actively processing:

```sql
SELECT tide.consumer_heartbeat('email-sender', 'worker-1');
```

Heartbeats serve two purposes:
1. **Monitoring** — you can detect stale consumers by checking `last_heartbeat` against a threshold
2. **Lease management** — future versions of pg_tide may automatically reassign work from consumers that haven't heartbeated recently

The `tide.consumer_lag` view shows the current state of each consumer:

```sql
SELECT * FROM tide.consumer_lag;
```

```
 group_name      | outbox_name | consumer_id | committed_offset | lag  | last_heartbeat
-----------------+-------------+-------------+------------------+------+--------------------
 email-sender    | orders      | worker-1    |              500 | 1500 | 2025-01-15 10:30:00
 analytics       | orders      | relay-0     |             1800 |  200 | 2025-01-15 10:31:00
 search-indexer  | orders      | relay-0     |             2000 |    0 | 2025-01-15 10:31:02
```

In this example, the email sender is 1,500 messages behind the latest — it might be slow or stuck. The search indexer is fully caught up.

### Visibility leases

Visibility leases prevent two consumers from processing the same batch of messages simultaneously. When a consumer claims a batch:

1. A lease is recorded in `tide.tide_consumer_leases` with a start ID, end ID, and expiry time
2. Other consumers in the same group won't see those messages until the lease expires
3. When the consumer commits the offset, the lease is released
4. If the consumer crashes without committing, the lease eventually expires and the messages become available again

This mechanism is similar to SQS's visibility timeout or Kafka's partition assignment — it provides at-most-once assignment of work within a consumer group.

### Multiple groups, one outbox

The most powerful aspect of consumer groups is that a single outbox can serve many independent purposes:

```sql
-- The relay delivers to NATS for real-time notifications
SELECT tide.create_consumer_group('nats-relay', 'orders');

-- An analytics service reads the same events for data warehouse loading
SELECT tide.create_consumer_group('analytics-etl', 'orders');

-- An audit logger persists every event to compliance storage
SELECT tide.create_consumer_group('audit-log', 'orders');

-- A search indexer updates Elasticsearch
SELECT tide.create_consumer_group('search-index', 'orders');
```

Each group progresses independently. The NATS relay might be at offset 5000 while the analytics ETL is catching up at offset 3000. They don't interfere with each other, and the outbox doesn't need to know about any of them.

### Consumer group lifecycle

```sql
-- Create a group
SELECT tide.create_consumer_group('my-group', 'events');

-- Drop a group (cascades: removes all offsets and leases)
SELECT tide.drop_consumer_group('my-group');

-- Idempotent creation (no error if already exists)
SELECT tide.create_consumer_group('my-group', 'events',
  p_if_not_exists := true);
```

---

## Relay Pipelines: Bridging PostgreSQL to the Outside World

A relay pipeline defines how messages flow between pg_tide and external systems. While consumer groups track *position*, pipelines define *destination* — where should messages actually go?

Pipelines are configured directly in the database (not in config files) and discovered by the relay binary at runtime. This means you can create, modify, and delete pipelines entirely through SQL, and the relay picks up changes automatically via PostgreSQL's LISTEN/NOTIFY mechanism.

### Two directions of flow

pg_tide supports two pipeline directions:

**Forward pipelines (Outbox → External Sink):** Messages flow from a pg_tide outbox to an external system. This is the most common pattern — your application publishes events to the outbox, and the relay delivers them to NATS, Kafka, Redis, webhooks, or any other configured sink.

```
┌──────────────┐         ┌──────────────┐         ┌──────────────┐
│  PostgreSQL  │         │  pg-tide     │         │  External    │
│  outbox      │────────▶│  relay       │────────▶│  system      │
│              │  poll   │              │ publish │  (NATS, etc) │
└──────────────┘         └──────────────┘         └──────────────┘
```

**Reverse pipelines (External Source → Inbox):** Messages flow from an external system into a pg_tide inbox. This is for receiving events from other services — the relay subscribes to an external source and writes incoming messages to an inbox table with deduplication.

```
┌──────────────┐         ┌──────────────┐         ┌──────────────┐
│  External    │         │  pg-tide     │         │  PostgreSQL  │
│  system      │────────▶│  relay       │────────▶│  inbox       │
│  (NATS, etc) │ subscribe│             │  insert │              │
└──────────────┘         └──────────────┘         └──────────────┘
```

### Configuring a forward pipeline

Forward pipelines connect an outbox to an external sink:

```sql
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-to-kafka',
    'outbox', 'orders',
    'sink_type', 'kafka',
    'config', jsonb_build_object(    -- sink-specific configuration
      'brokers', 'broker1:9092,broker2:9092',
      'topic', 'order-events',
      'acks', 'all',
      'compression', 'snappy'
    ),
    'batch_size', 200,
    'enabled', true
  )
);
```

The `config` parameter is a JSONB object whose keys depend on the sink type. Each backend (NATS, Kafka, Redis, RabbitMQ, SQS, Webhook) has its own set of configuration options — see the [Backends](../relay-guide/backends.md) page for complete details.

### Configuring a reverse pipeline

Reverse pipelines connect an external source to an inbox:

```sql
SELECT tide.relay_set_inbox_v2(
  jsonb_build_object(
    'name', 'stripe-webhooks',
    'inbox', 'payment-events',
    'source', 'webhook',
    'config', jsonb_build_object(      -- source-specific configuration
      'port', 8080,
      'path', '/webhooks/stripe',
      'auth_header', 'Bearer whsec_abc123'
    ),
    'batch_size', 50,
    'idempotent', true
  )
);
```

### Pipeline lifecycle management

Pipelines support a full lifecycle through SQL:

```sql
-- Pause processing (messages accumulate in the outbox)
SELECT tide.relay_disable('orders-to-kafka');

-- Resume processing
SELECT tide.relay_enable('orders-to-kafka');

-- Delete permanently (removes config and stops processing)
SELECT tide.relay_delete('orders-to-kafka');

-- View a pipeline's current configuration
SELECT tide.relay_get_config('orders-to-kafka');

-- List all configured pipelines
SELECT tide.relay_list_configs();
```

### Hot reload: no restart required

When you create, update, or delete a pipeline configuration, pg_tide fires a PostgreSQL notification:

```sql
pg_notify('tide_relay_config', '{"direction": "relay_outbox_config", "op": "INSERT", "name": "orders-to-kafka"}')
```

The relay binary listens for these notifications via `LISTEN tide_relay_config`. When a notification arrives, the relay:

1. Re-reads the pipeline catalog from the database
2. Starts any new pipelines
3. Stops any deleted pipelines
4. Reconfigures any modified pipelines

This means you can manage your entire pipeline lifecycle from SQL — no relay restarts, no config file deployments, no downtime. Add a new pipeline, and it starts processing within seconds.

### Advisory lock coordination: automatic failover

In production, you typically run multiple relay instances for high availability. But if two relays tried to process the same pipeline simultaneously, you'd get duplicate deliveries. pg_tide prevents this using PostgreSQL advisory locks.

Each pipeline is protected by a unique advisory lock. When a relay instance starts up:

1. It reads the pipeline catalog
2. For each pipeline, it attempts to acquire an advisory lock (non-blocking)
3. If it gets the lock, it owns that pipeline and begins processing
4. If another instance already holds the lock, it skips that pipeline and moves on

This gives you:

- **Automatic failover** — if a relay dies, its PostgreSQL session ends, the advisory locks are released, and another instance acquires them within seconds
- **No duplicate processing** — only one relay processes each pipeline at any given time
- **Horizontal distribution** — with many pipelines and many relay instances, pipelines are naturally distributed across instances

```bash
# Instance A — might own pipelines 1, 3, 5
pg-tide --relay-group-id production --postgres-url ...

# Instance B — might own pipelines 2, 4, 6
pg-tide --relay-group-id production --postgres-url ...

# If A crashes, B acquires pipelines 1, 3, 5 within seconds
```

### The relay group ID

The `relay_group_id` namespaces advisory locks. Relay instances with the **same** group ID compete for pipeline ownership — this is how HA failover works. Instances with **different** group IDs operate independently and can theoretically own the same pipeline simultaneously (though this would cause duplicate delivery and is rarely desirable).

```bash
# Production HA pair — same group, automatic failover between them
pg-tide --relay-group-id production --postgres-url ...
pg-tide --relay-group-id production --postgres-url ...

# Separate staging environment — different group, isolated
pg-tide --relay-group-id staging --postgres-url ...
```

### Supported backends

| Backend | Forward (Sink) | Reverse (Source) | Feature gate |
|---------|:--------------:|:----------------:|-------------|
| NATS | ✓ | ✓ | `nats` (default) |
| Kafka | ✓ | ✓ | `kafka` |
| Redis Streams | ✓ | ✓ | `redis` |
| RabbitMQ | ✓ | ✓ | `rabbitmq` |
| SQS | ✓ | ✓ | `sqs` |
| HTTP Webhook | ✓ | ✓ | `webhook` (default) |
| pg_tide Inbox | ✓ | — | `pg-inbox` |
| stdout | ✓ | — | `stdout` (default) |
| stdin | — | ✓ | always available |

### Putting it all together

Here's a typical production setup with multiple pipelines serving different purposes:

```sql
-- Forward: order events go to NATS for real-time microservice communication
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-realtime',
    'outbox', 'orders',
    'sink_type', 'nats',
    'config', jsonb_build_object(
      'url', 'nats://nats-cluster:4222',
      'subject', 'orders.{event_type}'
    )
  )
);

-- Forward: order events also go to Kafka for long-term analytics
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-analytics',
    'outbox', 'orders',
    'sink_type', 'kafka',
    'config', jsonb_build_object(
      'brokers', 'kafka:9092',
      'topic', 'orders-analytics',
      'compression', 'zstd'
    ),
    'batch_size', 500
  )
);

-- Reverse: incoming payment confirmations from a third-party webhook
SELECT tide.relay_set_inbox_v2(
  jsonb_build_object(
    'name', 'payment-webhooks',
    'inbox', 'payments',
    'source', 'webhook',
    'config', jsonb_build_object(
      'port', 8080,
      'path', '/webhooks/payments'
    )
  )
);

-- Forward: payment confirmations forwarded to an internal NATS subject
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'payments-internal',
    'outbox', 'payment-notifications',
    'sink_type', 'nats',
    'config', jsonb_build_object(
      'url', 'nats://nats-cluster:4222',
      'subject', 'payments.confirmed'
    )
  )
);
```

Each pipeline operates independently with its own offset tracking, retry logic, and advisory lock. The relay binary handles all of them concurrently.
