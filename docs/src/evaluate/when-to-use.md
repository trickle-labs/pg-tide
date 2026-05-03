# When to Use pg_tide

pg_tide is designed for a specific set of problems. Understanding when it shines — and when something else might be a better fit — helps you make the right architectural choice.

---

## pg_tide is a great fit when…

### You need reliable event publishing from PostgreSQL

Your application writes to PostgreSQL and needs to notify other systems about those writes. You want guarantees that every committed transaction produces exactly one event — no lost messages, no duplicates.

### You want to avoid dual-write problems

Writing to a database and a message broker in the same request is inherently unsafe. Network partitions, process crashes, and timeout races create inconsistency. pg_tide eliminates this class of bugs by keeping event publishing inside the database transaction.

### You prefer SQL over SDKs

pg_tide is a PostgreSQL extension. Publishing an event is a `SELECT tide.outbox_publish(...)` call. No client library, no serialization framework, no connection pooling to a separate broker — just SQL.

### You're already running PostgreSQL

If PostgreSQL is your primary data store, pg_tide adds messaging capabilities without introducing new infrastructure. The relay binary is a single static executable that reads its configuration from the same database.

### You need exactly-once delivery semantics

The idempotent inbox guarantees that duplicate deliveries are detected and discarded. Combined with consumer offset tracking, pg_tide provides end-to-end exactly-once semantics across restarts and failures.

---

## Consider alternatives when…

### You need sub-millisecond latency

pg_tide's relay polls the outbox on a configurable interval (default: 100ms). If your use case demands microsecond-level propagation, a dedicated event streaming platform like Kafka or NATS JetStream with direct writes may be more appropriate.

### You have no PostgreSQL in your stack

pg_tide is a PostgreSQL extension. If your data lives in MySQL, MongoDB, or another database, look at Debezium, Maxwell, or platform-specific CDC tools.

### You're doing pure pub/sub without durability

If you need ephemeral fire-and-forget messaging (e.g., real-time notifications where missed messages are acceptable), a simple Redis Pub/Sub or NATS Core subscription is simpler and faster.

### Your throughput exceeds PostgreSQL's write capacity

pg_tide's throughput ceiling is PostgreSQL's INSERT performance. For sustained write rates above ~100K messages/second on a single table, dedicated log-structured systems (Kafka, Redpanda, Pulsar) are purpose-built.

---

## The Sweet Spot

pg_tide occupies the space where **transactional correctness matters more than raw throughput**, and where **operational simplicity** (no Zookeeper, no broker cluster) outweighs the need for a standalone streaming platform.

Typical use cases:

- Order processing pipelines
- Audit event emission
- Cross-service data synchronization
- Webhook delivery with retry semantics
- Saga / process manager coordination
- CQRS event sourcing from a relational store
