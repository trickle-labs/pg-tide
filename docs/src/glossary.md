# Glossary

**Advisory lock** — A PostgreSQL locking mechanism used by pg_tide for high-availability coordination. Each relay instance acquires advisory locks for the pipelines it owns, preventing multiple instances from processing the same pipeline.

**At-least-once delivery** — A delivery guarantee where every message is delivered one or more times. pg_tide provides at-least-once by default — messages may be re-delivered on failure recovery, but are never lost.

**Batch** — A group of messages processed together in a single poll-publish-acknowledge cycle. Larger batches improve throughput at the cost of latency.

**Circuit breaker** — A fault-tolerance pattern that stops attempting delivery when a sink is consistently failing. After a timeout, it probes with a single message to test recovery.

**Consumer group** — An independent cursor into an outbox, allowing multiple services to consume the same event stream at their own pace without interfering with each other.

**Dead letter queue (DLQ)** — A PostgreSQL table (`tide.relay_dlq`) that stores messages which failed delivery after all retry attempts. Messages can be inspected and replayed from the DLQ.

**Deduplication key (dedup_key)** — A unique identifier for inbox messages that prevents duplicate processing. If the same dedup_key arrives twice, the second write is silently ignored.

**Discovery** — The process by which the relay coordinator finds and reconciles pipeline configurations from the PostgreSQL catalog.

**Dry-run mode** — A pipeline mode where the relay performs all processing (poll, transform, route) but logs output instead of publishing to the sink.

**Envelope** — The wire format wrapper around a message payload. Determines how metadata (operation type, timestamps, source info) is encoded alongside the data.

**Exactly-once processing** — Achieved through the combination of at-least-once delivery and inbox deduplication. Each unique message is processed exactly once.

**Fan-out** — A pattern where a single event stream is delivered to multiple independent consumers (via consumer groups or multiple pipelines).

**Forward pipeline** — A pipeline that moves messages from an outbox to an external sink (outbox → sink direction).

**Graceful shutdown** — The relay's shutdown sequence: drain in-flight batches, acknowledge processed messages, release advisory locks, then exit.

**Half-open** — The circuit breaker state between open and closed, where a single probe message tests whether the sink has recovered.

**Hot-reload** — Updating pipeline configurations without restarting the relay process. Triggered by LISTEN/NOTIFY or periodic discovery.

**Inbox** — A PostgreSQL table that receives messages from external systems, providing deduplication and transactional processing guarantees.

**JMESPath** — A query language for JSON used by pg_tide for message transforms and filters.

**NATS JetStream** — NATS's persistent messaging layer. pg_tide uses JetStream for durable subscriptions with consumer groups.

**Outbox** — A PostgreSQL table that stores events published by the application, to be relayed to external systems by the relay process.

**Pipeline** — A configured relay path: source → transforms → routing → sink. Each pipeline has a name, direction, and configuration.

**Relay** — The `pg-tide` binary process that moves messages between PostgreSQL and external systems.

**Relay group** — A set of relay instances coordinating via the same `relay_group_id`. Instances within a group distribute pipelines among themselves.

**Replay** — Reprocessing a range of outbox messages, typically to backfill a new consumer or recover from a failure.

**Reverse pipeline** — A pipeline that moves messages from an external source into a pg_tide inbox (source → inbox direction).

**Routing** — Content-based routing that dynamically determines the destination subject/topic for each message based on its payload.

**Schema Registry** — A service (typically Confluent Schema Registry) that stores and manages Avro schemas for serialization/deserialization.

**Sink** — The destination for a forward pipeline: Kafka, NATS, HTTP, S3, BigQuery, etc.

**Source** — The origin for a reverse pipeline: Kafka, NATS, webhook receiver, Singer tap, etc.

**Stream table** — The logical name/category of an outbox event (e.g., "orders", "user-signups"). Used for routing and filtering.

**Subject template** — A string with `{variable}` placeholders that resolves to the final topic/subject name at runtime.

**Token bucket** — The rate limiting algorithm used by pg_tide. Allows bursts up to a configured capacity, then enforces a steady-state rate.

**Tombstone** — A null-value message (Kafka concept) that signals deletion of a key during log compaction.

**Transform** — A JMESPath expression that filters messages (drops non-matching) or reshapes payloads before publishing.

**Wire format** — The serialization format for messages on the transport layer (native, Debezium, Maxwell, Canal, CDC JSON).
