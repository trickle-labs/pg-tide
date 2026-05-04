# Choosing pg_tide

Choosing the right messaging infrastructure is one of the most consequential architectural decisions you'll make. This page helps you determine whether pg_tide is the right fit for your project by examining where it excels, where alternatives serve better, and how it compares to other tools you might be considering.

---

## When pg_tide Is a Great Fit

### You need reliable event publishing from PostgreSQL

Your application writes to PostgreSQL and needs to notify other systems about those writes — sending emails, updating search indexes, feeding analytics pipelines, triggering downstream workflows. You want guarantees that every committed transaction produces exactly one event: no lost messages, no duplicates, no manual reconciliation.

pg_tide was built specifically for this scenario. The transactional outbox pattern ensures your events are published atomically with your business data. If the transaction commits, the event is guaranteed to be delivered. If it rolls back, the event never existed.

### You want to eliminate dual-write bugs

The dual-write problem is pernicious because it's intermittent and silent. Your application might work perfectly 99.9% of the time, but during network hiccups, process restarts, or database failovers, events get lost or duplicated. These bugs are incredibly difficult to detect in testing and even harder to reproduce.

pg_tide eliminates this entire class of bugs by design. There is no dual write — only a single database write that includes both your data and the event. The relay handles delivery separately, retrying indefinitely until downstream systems acknowledge receipt.

### You prefer SQL over SDKs

pg_tide is a PostgreSQL extension. Publishing an event is a `SELECT tide.outbox_publish(...)` call. There's no client library to install, no serialization framework to learn, no connection pooling for a separate broker, no SDK version compatibility to manage. Any language or framework that can talk to PostgreSQL can publish events.

This means your Go service, Python script, dbt model, PL/pgSQL function, and psql session can all publish events using exactly the same API. The outbox is a database table — you can query it, monitor it, and manage it with standard SQL tools.

### You're already running PostgreSQL

If PostgreSQL is your primary data store — and for many teams, it is — pg_tide adds messaging capabilities without introducing new infrastructure. No Kafka cluster to operate, no ZooKeeper to babysit, no broker to monitor. The relay binary is a single static executable that reads its configuration from the same database it's delivering messages from.

This dramatically reduces operational overhead. You already know how to back up PostgreSQL, monitor its performance, manage its connections, and failover between replicas. pg_tide inherits all of that operational maturity.

### You need exactly-once delivery semantics

Many messaging systems provide at-most-once or at-least-once delivery. True exactly-once requires coordination between the sender and receiver. pg_tide achieves this through the combination of transactional publishing (no message loss), consumer offset tracking (no re-processing), and the idempotent inbox (no duplicates). The end-to-end result is effectively exactly-once — each event is processed precisely one time.

### Your throughput fits within PostgreSQL's capacity

For most applications, PostgreSQL can handle 5,000–15,000 outbox publishes per second on a single connection (depending on payload size and hardware). If your event volume fits within this range — which covers the vast majority of OLTP workloads — pg_tide provides simpler operations than dedicated streaming platforms.

---

## When to Consider Alternatives

### You need sub-millisecond propagation latency

pg_tide's relay polls the outbox at a configurable interval (it wakes immediately via pg_notify for new messages, but batching introduces slight delays). For use cases that demand microsecond-level propagation — high-frequency trading signals, real-time game state — a dedicated event streaming platform with direct in-memory writes (like NATS Core or Kafka with acks=0) will provide lower latency.

That said, pg_tide's latency is typically under 100ms end-to-end. For most applications (webhook delivery, service coordination, analytics feeds), this is more than adequate.

### You have no PostgreSQL in your stack

pg_tide is a PostgreSQL extension — that's the whole point. If your data lives in MySQL, MongoDB, DynamoDB, or another database, pg_tide can't help you. Look at:

- **Debezium** — CDC for MySQL, PostgreSQL, MongoDB, SQL Server, and more
- **Maxwell** — MySQL-specific CDC tool
- **DynamoDB Streams** — built-in change capture for DynamoDB

### You're doing pure pub/sub without durability requirements

If you need ephemeral fire-and-forget messaging — real-time typing indicators, presence updates, live dashboard refreshes — where missed messages are perfectly acceptable, a simple Redis Pub/Sub or NATS Core subscription is lighter and faster. No durability means no outbox, no offset tracking, and no relay to operate.

### Your sustained throughput exceeds PostgreSQL's write capacity

pg_tide's throughput ceiling is fundamentally PostgreSQL's INSERT performance. For sustained write rates above ~100,000 messages/second on a single outbox table (very high-volume telemetry, clickstream data, IoT sensor feeds), dedicated log-structured systems (Kafka, Redpanda, Pulsar) are purpose-built for this scale.

However, before concluding that you need more throughput, consider whether you can partition your events across multiple outboxes, which allows parallel relay consumption.

### You need automatic schema-change capture

If your use case is "capture every row change in every table automatically, without modifying application code," Debezium's CDC approach is better suited. pg_tide requires you to explicitly publish events — you choose what gets published, when, and in what format. This is a strength (explicit > implicit for event contracts), but it requires more application involvement.

---

## The Sweet Spot

pg_tide occupies the space where **transactional correctness matters more than raw throughput**, and where **operational simplicity** (no broker cluster, no JVM, no ZooKeeper) outweighs the need for a standalone streaming platform.

Typical use cases that pg_tide handles beautifully:

| Use case | Why pg_tide fits |
|----------|-----------------|
| **Order processing pipelines** | Events must never be lost; exactly-once is essential |
| **Audit event emission** | Every business action must produce a corresponding audit record |
| **Cross-service synchronization** | Services need consistent views of shared data |
| **Webhook delivery with retry** | Unreliable endpoints need persistent retry with DLQ |
| **Saga / process manager coordination** | Orchestrating multi-step workflows across services |
| **CQRS event sourcing** | Projecting command-side events to query-side read models |
| **Data warehouse loading** | Reliably streaming changes to analytics infrastructure |
| **Multi-tenant notification delivery** | Per-tenant event routing with independent tracking |

---

## Detailed Comparison with Alternatives

### pg_tide vs. Debezium

| Aspect | pg_tide | Debezium |
|--------|---------|----------|
| **Mechanism** | Application explicitly writes to outbox table | CDC via PostgreSQL logical replication (captures WAL changes) |
| **Message format** | You control the payload — publish exactly what consumers need | Mirrors row-level changes (schema-coupled to table structure) |
| **Event granularity** | Publish semantic events ("order confirmed") | Captures physical changes ("row updated in orders table") |
| **Infrastructure** | PostgreSQL + single relay binary | PostgreSQL + Kafka Connect + Kafka + ZooKeeper/KRaft |
| **Exactly-once** | Built-in via inbox dedup | Requires downstream idempotency |
| **Operational cost** | One binary (~20 MB), no JVM | JVM-based Kafka Connect, requires Kafka cluster |
| **Flexibility** | Arbitrary events, decoupled from table schema | Automatic but tied to schema changes |
| **Application changes** | Must call `outbox_publish()` | None (captures changes transparently) |
| **Latency** | <100ms (notify-driven) | ~1-5s (replication slot lag) |

**Choose pg_tide** when you want explicit, semantic events that are decoupled from your table schema, and you prefer minimal infrastructure. **Choose Debezium** when you need automatic capture of all database changes without modifying application code, and you're willing to operate the Kafka ecosystem.

### pg_tide vs. Application-Level Outbox (DIY)

| Aspect | pg_tide | Custom outbox table + homegrown relay |
|--------|---------|--------------------------------------|
| **Setup time** | `CREATE EXTENSION pg_tide;` + start relay | Design schema, build polling logic, implement retry, add dedup, build monitoring... |
| **Consumer groups** | Built-in with offsets, heartbeats, visibility leases | You build and maintain it |
| **Relay** | Multi-backend binary with metrics, backpressure, HA | You build and maintain it |
| **Idempotent inbox** | Built-in with DLQ and replay | You build and maintain it |
| **Monitoring** | Prometheus metrics + SQL views out of the box | You instrument and maintain it |
| **HA / failover** | Advisory lock coordination, automatic | You design and build it |
| **Maintenance** | Upgrade extension + relay binary | Maintain all custom code indefinitely |
| **Backends** | NATS, Kafka, Redis, RabbitMQ, SQS, Webhooks | Whatever you've implemented |

**Choose pg_tide** to avoid reinventing reliable messaging infrastructure. A DIY outbox is deceptively simple to start but grows in complexity quickly as you add retry logic, offset tracking, multiple consumers, monitoring, and failover. **Choose DIY** only when you have very specific requirements that don't map to pg_tide's model.

### pg_tide vs. pg_notify / LISTEN

| Aspect | pg_tide | pg_notify |
|--------|---------|-----------|
| **Durability** | Messages persist until consumed | Fire-and-forget (lost if no listener is active) |
| **Payload size** | JSONB (up to 1 GB, practically limited by memory) | 8,000 bytes maximum |
| **Retry** | Built-in with exponential backoff and DLQ | None — if you miss it, it's gone |
| **Consumer groups** | Independent offset tracking per consumer | No — every listener sees every notification |
| **Delivery guarantee** | At-least-once (effectively exactly-once with inbox) | At-most-once (zero-once if no listener) |
| **Cross-network** | Relay bridges to any external system | Only in-process PostgreSQL clients |
| **Ordering** | Guaranteed within an outbox (by ID) | Guaranteed within a session |
| **Backpressure** | Configurable threshold | None (notifications queue in memory) |

**Choose pg_tide** when you need durable, reliable delivery with guarantees. **Choose pg_notify** for lightweight real-time signals where message loss is acceptable — like cache invalidation hints or live UI updates where the client can refresh on reconnect.

### pg_tide vs. Writing Directly to Kafka/NATS

| Aspect | pg_tide | Direct broker writes from application |
|--------|---------|--------------------------------------|
| **Transactional safety** | Guaranteed (same database transaction) | Dual-write risk (DB commit + broker publish are independent) |
| **Application complexity** | One SQL call per event | Broker client library, connection management, error handling |
| **Operational overhead** | Extension + lightweight relay | Broker cluster management, application-side retries |
| **Throughput ceiling** | PostgreSQL write speed (~15K msg/s per connection) | Broker-native throughput (higher ceiling) |
| **Latency** | ~50-100ms (poll + delivery) | ~1-5ms (direct publish) |
| **Message loss risk** | Zero (transactional guarantee) | Non-zero (crash between DB commit and broker ack) |
| **Duplicate risk** | Handled by inbox dedup | Application must implement idempotency |

**Choose pg_tide** when transactional correctness is paramount and throughput fits within PostgreSQL's capacity. **Choose direct broker writes** when you accept the dual-write tradeoff for maximum throughput and minimum latency, or when your application already runs inside the broker ecosystem (e.g., a Kafka Streams application).

---

## Cost Analysis: pg_tide vs. Running Kafka

For teams evaluating pg_tide against a full Kafka deployment, here's a practical comparison of operational costs:

| Resource | pg_tide | Kafka (small production cluster) |
|----------|---------|----------------------------------|
| **Processes to operate** | 1-2 relay instances | 3+ brokers + ZooKeeper/KRaft + Connect + Schema Registry |
| **Memory footprint** | ~50 MB per relay | ~6 GB per broker (JVM heap) |
| **Disk** | Shared with PostgreSQL | Dedicated high-throughput storage per broker |
| **Network** | PostgreSQL connection + sink connections | Inter-broker replication, client connections, ZK communication |
| **On-call complexity** | "Is PostgreSQL healthy? Is the relay running?" | Partition rebalancing, ISR management, disk pressure, GC pauses |
| **Team expertise** | PostgreSQL DBA + basic ops | Kafka-specialized operations team |

pg_tide's total cost of ownership is dramatically lower for teams whose primary workload is a PostgreSQL-backed application with moderate event volumes (< 50K events/second).

---

## Decision Flowchart

Ask yourself these questions in order:

1. **Is PostgreSQL your primary data store?** If not → look at Debezium, platform-specific CDC
2. **Do your events need transactional guarantees?** If not → consider direct broker writes or pg_notify
3. **Is your throughput under ~50K events/second?** If not → consider Kafka/Redpanda
4. **Do you want to minimize operational overhead?** If yes → pg_tide
5. **Do you need automatic schema-change capture?** If yes → consider Debezium (or combine both)

If you answered "yes" to questions 1, 2, 3, and 4 — pg_tide is an excellent fit.

---

## Migration Paths

### Coming from pg_notify

If you're currently using `pg_notify` for event delivery and hitting its limitations (payload size, durability, reliability):

1. Install pg_tide and create outboxes for your event channels
2. Replace `PERFORM pg_notify(channel, payload)` with `SELECT tide.outbox_publish(outbox, payload, headers)`
3. Set up relay pipelines to your downstream consumers
4. Benefit from durability, retry, offset tracking, and exactly-once semantics

### Coming from a DIY outbox table

If you've built a custom outbox pattern:

1. Install pg_tide alongside your existing tables
2. Migrate pipeline logic to pg_tide's relay (eliminates your custom polling code)
3. Use pg_tide's consumer groups instead of custom offset tracking
4. Add inboxes for receiving-side deduplication
5. Decommission your custom relay code

### Coming from Debezium

If you're considering pg_tide as a complement or replacement for Debezium:

- **Complement:** Use Debezium for bulk CDC (replicating entire tables) and pg_tide for semantic business events (explicit, shaped events published by application logic)
- **Replace:** If you're using Debezium primarily for outbox-style event publishing (Debezium's outbox router), pg_tide provides the same capability with far less infrastructure
