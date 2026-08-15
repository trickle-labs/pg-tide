# Documentation Plan v2 — Comprehensive Feature Documentation

> **Goal:** Build world-class documentation for pg_tide that covers every supported
> external system, every feature, and every operational concern in depth. The documentation
> should be approachable for a non-technical audience while remaining authoritative for
> experienced engineers. Written in an explanatory, narrative style with longer paragraphs
> that guide readers through concepts rather than listing bullet points.
>
> **Scope:** This plan supplements `plans/documentation.md` (structural consolidation and
> tone improvements) with detailed content plans for documenting all 30 sinks, 16 sources,
> 6 wire formats, and 20+ operational features that ship as of v0.11.0.

---

## 1. Philosophy & Writing Approach

### 1.1 Audience Segments

The documentation must serve three distinct audiences simultaneously:

1. **Decision-makers and evaluators** — CTOs, architects, and engineering managers who
   need to understand what pg_tide does, whether it fits their stack, and how it compares
   to alternatives. These readers want clear prose, architecture diagrams, and honest
   assessments of trade-offs. They should never need to read code to understand value.

2. **Application developers** — Backend engineers building services that publish events
   or consume messages. They need to understand the SQL API, how to structure their
   application code, and how to choose the right sink or source for their use case.
   Worked examples with realistic business scenarios are essential.

3. **Platform engineers and operators** — DevOps/SRE teams responsible for deploying,
   scaling, monitoring, and troubleshooting pg_tide in production. They need precise
   configuration references, alert rule templates, capacity planning guidance, and
   diagnostic procedures.

### 1.2 Writing Style Principles

- **Longer paragraphs that tell a story.** Instead of telegraphic bullet lists, write
  flowing paragraphs (4–8 sentences) that explain the "why" alongside the "what."
  Readers should feel like they're being guided by an experienced colleague.

- **Everyday language first, technical precision second.** Introduce a concept using
  familiar analogies and plain language before introducing the technical terminology.
  For example: explain what happens to a message in a system failure before naming the
  pattern "transactional outbox."

- **Progressive complexity.** Each page should be readable top-to-bottom. Start with
  the simplest possible explanation, then add nuance, configuration, and edge cases
  in later sections. A reader who stops halfway through should still have learned
  something useful.

- **Concrete before abstract.** Show a real example before explaining the general rule.
  Instead of "The relay supports configurable batch sizes," show the actual config line,
  then explain what it controls and why you might change it.

- **Honest about trade-offs.** Never oversell. If a feature has limitations, document
  them clearly. Readers who discover limitations on their own lose trust in documentation.

### 1.3 Page Template

Every documentation page should follow this structure:

```
# [Feature/System Name]

Opening paragraph: What problem does this solve? Why would a reader care?
Written as if explaining to a smart colleague over coffee.

## Overview

2–4 paragraphs explaining the concept from first principles. Use analogies.
Include a Mermaid diagram if the concept has moving parts.

## When to Use This

Describe the scenarios where this feature or system is the right choice.
Help readers self-select: "This is for me" or "I should look elsewhere."

## How It Works

Detailed walkthrough of the mechanics. Explain what happens step by step.
Include annotated code examples with prose between blocks.

## Configuration

Complete reference with every option explained in prose (not just a table).
Include a fully annotated example showing realistic production values.

## Example: [Realistic Scenario Name]

A complete worked example tied to a believable business scenario.
Include setup, execution, verification, and cleanup.

## Troubleshooting

Common problems, their symptoms, diagnostic steps, and solutions.

## Further Reading

Links to related pages, external documentation, and deeper dives.
```

---

## 2. Sink Documentation (30 Systems)

Each sink represents an external system that pg_tide can deliver messages to. The
documentation for each sink must help a reader go from "I've never connected to this
system" to "I have a working, production-ready pipeline" in a single page.

### 2.1 Per-Sink Documentation Template

Every sink page should cover:

1. **What is [System]?** — A 2–3 paragraph explanation of the external system itself,
   written for readers who may not have used it before. What does it do? Who typically
   uses it? What role does it play in a modern data stack?

2. **Why send pg_tide messages to [System]?** — Explain the value of connecting your
   PostgreSQL events to this particular system. What workflows does this enable?

3. **Prerequisites** — What the reader needs before starting: accounts, credentials,
   network access, system-specific setup (topics, queues, buckets, etc.)

4. **Configuration** — Complete TOML configuration with every option explained. Include
   both minimal and production-ready examples.

5. **Authentication & Security** — How to authenticate: API keys, OAuth, IAM roles,
   mTLS certificates. Show each method with a complete example.

6. **Message Mapping** — How pg_tide messages map to the system's native concepts
   (messages, records, documents, rows). What metadata is preserved?

7. **Delivery Guarantees** — What happens on failure? How does retry work? What
   triggers DLQ routing? Be precise about at-least-once vs. exactly-once semantics.

8. **Performance & Tuning** — Batch sizes, connection pooling, compression settings,
   and throughput expectations. Include concrete numbers where possible.

9. **Complete Example** — End-to-end walkthrough: create outbox, configure pipeline,
   start relay, verify messages arrive. Include verification commands.

10. **Troubleshooting** — System-specific error messages, connection issues, auth
    failures, and their resolutions.

### 2.2 Sink Pages to Write

#### Message Queue Sinks

| Sink | Page | Priority | Key Topics |
|------|------|----------|------------|
| **Apache Kafka** | `sinks/kafka.md` | P0 | Broker config, SASL/SCRAM/mTLS, topic auto-creation, partitioning strategy, idempotent producer, Confluent Cloud, Redpanda compatibility |
| **NATS JetStream** | `sinks/nats.md` | P0 | Stream/subject configuration, TLS, credentials file, exactly-once via dedup, NATS.io cloud |
| **RabbitMQ** | `sinks/rabbitmq.md` | P1 | Exchanges, routing keys, publisher confirms, TLS, CloudAMQP |
| **Redis Streams** | `sinks/redis.md` | P1 | Stream keys, MAXLEN trimming, Redis Cluster, Redis Cloud, Sentinel failover |
| **Amazon SQS** | `sinks/sqs.md` | P1 | Standard vs. FIFO queues, message groups, IAM auth, VPC endpoints, message attributes |
| **Amazon Kinesis** | `sinks/kinesis.md` | P1 | Shard selection, partition keys, aggregation, IAM auth, enhanced fan-out |
| **Google Cloud Pub/Sub** | `sinks/pubsub.md` | P1 | Topic config, service account auth, ordering keys, message attributes, dead-letter topics |
| **Azure Service Bus** | `sinks/servicebus.md` | P2 | Queues vs. topics, sessions, shared access keys, managed identity |
| **Azure Event Hubs** | `sinks/eventhubs.md` | P2 | Partitions, consumer groups, Kafka-compatible endpoint, managed identity, capture |
| **MQTT v5** | `sinks/mqtt.md` | P2 | QoS levels, topic hierarchy, retained messages, TLS, HiveMQ/EMQX/Mosquitto |

#### Analytics & Data Lake Sinks

| Sink | Page | Priority | Key Topics |
|------|------|----------|------------|
| **ClickHouse** | `sinks/clickhouse.md` | P0 | Table engines (MergeTree, ReplacingMergeTree), batch inserts, schema mapping, ClickHouse Cloud, deduplication |
| **Snowflake** | `sinks/snowflake.md` | P0 | Stages, COPY INTO, key-pair auth, warehouse sizing, cost optimization, role grants |
| **BigQuery** | `sinks/bigquery.md` | P0 | Streaming inserts vs. load jobs, service account, dataset/table creation, schema evolution, cost control |
| **Apache Iceberg** | `sinks/iceberg.md` | P1 | Catalog types (REST, Glue, Hive), table creation, partitioning, compaction, S3/GCS/ADLS storage |
| **Delta Lake** | `sinks/delta.md` | P1 | Delta protocol v2, storage backends, ACID transactions, time travel, Unity Catalog |
| **DuckLake** | `sinks/ducklake.md` | P1 | Parquet files + PostgreSQL metadata catalog, local and cloud storage, when to choose DuckLake |
| **MongoDB** | `sinks/mongodb.md` | P1 | Collections, document mapping, upsert mode, Atlas connection, write concern |
| **Elasticsearch / OpenSearch** | `sinks/elasticsearch.md` | P1 | Index templates, document IDs, bulk API, ILM policies, OpenSearch compatibility |
| **Object Storage** | `sinks/object-storage.md` | P1 | S3/GCS/Azure Blob, JSONL vs. Parquet format, path templates, partitioned layout, IAM roles |
| **Apache Arrow Flight** | `sinks/arrow-flight.md` | P2 | gRPC endpoint, columnar batching, use cases (analytics, ML pipelines), FlightSQL |

#### Notification Sinks

| Sink | Page | Priority | Key Topics |
|------|------|----------|------------|
| **HTTP Webhook** | `sinks/webhook.md` | P0 | URL templates, headers, retry policy, signature verification (HMAC, GitHub, Stripe, Svix), timeout config |
| **Slack** | `sinks/slack.md` | P2 | Webhook URLs, message formatting (Block Kit), channel routing, rate limits |
| **Discord** | `sinks/discord.md` | P2 | Webhook URLs, embed formatting, channel routing |
| **PagerDuty** | `sinks/pagerduty.md` | P2 | Events API v2, routing keys, severity mapping, deduplication keys, auto-resolve |

#### Connector Ecosystem Sinks

| Sink | Page | Priority | Key Topics |
|------|------|----------|------------|
| **Singer / Meltano** | `sinks/singer.md` | P0 | Target protocol, STATE persistence, SCHEMA handling, on_schema_change policy, Meltano Hub catalog, running targets |
| **Airbyte** | `sinks/airbyte.md` | P1 | Protocol compliance, destination connectors, catalog configuration, state management |
| **Fivetran HVR** | `sinks/fivetran.md` | P2 | HVR endpoint format, webhook signatures, sync modes |

#### Infrastructure Sinks

| Sink | Page | Priority | Key Topics |
|------|------|----------|------------|
| **PostgreSQL Inbox** | `sinks/pg-inbox.md` | P0 | Reverse pipeline, inbox deduplication, cross-service messaging, connection config |
| **Remote PostgreSQL Outbox** | `sinks/pg-outbox.md` | P2 | Outbox-to-outbox federation, multi-cluster patterns |
| **stdout / File** | `sinks/stdout.md` | P2 | Debugging, log capture, pipe to external tools |

### 2.3 Example Sink Page Content (Kafka)

To illustrate the expected depth, here is an outline for the Kafka sink page:

```markdown
# Apache Kafka

Apache Kafka is a distributed event streaming platform used by thousands of
organizations to build real-time data pipelines. When you connect pg_tide to
Kafka, every event published to your PostgreSQL outbox is automatically
delivered to Kafka topics — making your database changes available to any
downstream system that can consume Kafka messages.

## Why Send Events to Kafka?

[3 paragraphs explaining the value proposition: decoupling, replayability,
ecosystem of consumers, schema evolution support]

## Prerequisites

- A running Kafka cluster (self-hosted, Confluent Cloud, Redpanda, or Amazon MSK)
- Network connectivity from the pg_tide relay to the Kafka brokers
- A topic created (or auto-creation enabled)
- Authentication credentials (if secured)

## Configuration

### Minimal Configuration

[Annotated TOML showing brokers + topic]

### Production Configuration

[Full TOML with TLS, SASL, compression, batching, idempotent producer]

### Configuration Reference

[Every option in prose paragraphs, not just a table]

## Authentication

### No Authentication (Development)
### SASL/PLAIN (Confluent Cloud)
### SASL/SCRAM-SHA-256
### mTLS (Certificate-Based)
### AWS IAM (Amazon MSK)

[Each with a complete, copy-pasteable example]

## Message Format

[Explain how outbox messages map to Kafka records: key, value, headers,
topic selection via routing templates]

## Delivery Guarantees

[Explain idempotent producer, acks=all, retry semantics, DLQ integration]

## Performance Tuning

[Batch size, linger, compression, connection count, expected throughput]

## Complete Example: Order Events to Kafka

[Full walkthrough from CREATE EXTENSION to verifying with kafka-console-consumer]

## Troubleshooting

- "Connection refused" — broker address resolution, network policies
- "SASL authentication failed" — credential format, mechanism mismatch
- "Topic not found" — auto-creation disabled, ACL permissions
- "Message too large" — max.message.bytes configuration
```

---

## 3. Source Documentation (16 Systems)

Sources are the reverse of sinks: they consume messages from external systems and
deliver them into a pg_tide inbox table. The source documentation must explain both
the external system's consumer model and how pg_tide maps that to inbox rows.

### 3.1 Per-Source Documentation Template

1. **What is [System] as a message source?** — Explain the system's consumer/subscriber
   model. How do messages become available? What ordering guarantees exist?

2. **Why consume from [System] into pg_tide?** — The value of bringing external events
   into your PostgreSQL database with idempotent deduplication.

3. **Prerequisites** — Topics/queues that must exist, consumer permissions, network access.

4. **Configuration** — Complete TOML with all options explained.

5. **Offset Management** — How pg_tide tracks consumption progress. What happens on
   restart? How does exactly-once interact with the source's acknowledgment model?

6. **Deduplication** — How the inbox prevents duplicate processing when the same
   message arrives twice.

7. **Complete Example** — Publish a message in the external system, verify it arrives
   in the inbox table.

8. **Troubleshooting** — Common consumer issues specific to this system.

### 3.2 Source Pages to Write

| Source | Page | Priority | Key Topics |
|--------|------|----------|------------|
| **PostgreSQL Outbox** | `sources/outbox.md` | P0 | Polling config, batch size, notification-driven wake, consumer groups, offset tracking |
| **Apache Kafka** | `sources/kafka.md` | P0 | Consumer groups, offset commit, partition assignment, rebalancing |
| **NATS JetStream** | `sources/nats.md` | P0 | Durable consumers, ack policy, replay policy |
| **RabbitMQ** | `sources/rabbitmq.md` | P1 | Queue binding, prefetch, manual ack |
| **Redis Streams** | `sources/redis.md` | P1 | Consumer groups, XREADGROUP, pending entries |
| **Amazon SQS** | `sources/sqs.md` | P1 | Long polling, visibility timeout, message deletion |
| **Amazon Kinesis** | `sources/kinesis.md` | P1 | Shard iterators, checkpointing, resharding |
| **Google Cloud Pub/Sub** | `sources/pubsub.md` | P1 | Subscriptions, ack deadline, ordering keys |
| **Azure Service Bus** | `sources/servicebus.md` | P2 | Peek-lock, sessions, dead-letter sub-queue |
| **Azure Event Hubs** | `sources/eventhubs.md` | P2 | Consumer groups, checkpointing, epoch receivers |
| **MQTT v5** | `sources/mqtt.md` | P2 | Subscriptions, QoS, clean sessions, shared subscriptions |
| **HTTP Webhook (Receiver)** | `sources/webhook-receiver.md` | P1 | Axum HTTP server, path routing, signature validation, response codes |
| **Singer / Meltano** | `sources/singer.md` | P1 | Tap protocol, RECORD/STATE/SCHEMA messages, incremental sync |
| **Airbyte** | `sources/airbyte.md` | P2 | Source connectors, catalog discovery, state checkpointing |
| **stdin / File** | `sources/stdin.md` | P2 | Line-delimited JSON, replay from file, testing workflows |

---

## 4. Wire Format Documentation

Wire formats determine how messages are encoded on the wire between pg_tide and
external systems. This is one of pg_tide's most powerful features — it allows
the relay to speak the same protocol as established CDC tools, unlocking hundreds
of existing integrations without any code changes.

### 4.1 Wire Format Overview Page

**File:** `wire-formats/overview.md`

Content plan:

- **What is a wire format?** — Explain the concept of separating transport (Kafka, NATS)
  from encoding (how message bytes are structured). Use the analogy of shipping containers:
  the container (transport) doesn't care what's inside, but the receiver needs to know
  how the goods are packed (wire format).

- **Why multiple wire formats matter** — Explain that existing ecosystems (Debezium,
  Maxwell, Canal) have established message formats that hundreds of tools already
  understand. By speaking these formats, pg_tide becomes compatible with the entire
  ecosystem without requiring those tools to learn anything new.

- **Choosing a wire format** — Decision guide:
  - Native: simplest, full fidelity, pg_tide-to-pg_tide communication
  - Debezium: maximum ecosystem compatibility (Iceberg, Flink, ksqlDB, Materialize)
  - Maxwell: MySQL CDC ecosystem compatibility (lighter weight than Debezium)
  - Canal: Alibaba/Chinese tech ecosystem compatibility
  - Custom CDC JSON: when you need a format that doesn't match any standard

- **Direction support matrix** — Table showing which formats support encode (forward),
  decode (reverse), or both (bidirectional).

### 4.2 Individual Wire Format Pages

| Format | Page | Priority | Key Topics |
|--------|------|----------|------------|
| **Native** | `wire-formats/native.md` | P0 | Envelope structure, fields (event_id, outbox_id, stream_table, op, payload, old_payload, headers, committed_at, lsn), when to use native |
| **Debezium** | `wire-formats/debezium.md` | P0 | Bidirectional support, JSON + Avro + Protobuf encoding, Schema Registry integration, source block, transaction metadata, tombstone records, compatibility matrix with downstream tools |
| **Maxwell** | `wire-formats/maxwell.md` | P1 | MySQL CDC decode, field mapping (data, old, type, table, database, ts), using with Maxwell daemon output |
| **Canal** | `wire-formats/canal.md` | P1 | Alibaba Canal protocol, FlatMessage format, field mapping, using with Canal Server |
| **Custom CDC JSON** | `wire-formats/cdc-json.md` | P1 | User-defined path expressions, dot-notation field access, mapping arbitrary JSON to inbox rows, configuration examples |

### 4.3 Debezium Page Depth (flagship wire format)

The Debezium page deserves special attention because it unlocks the most integrations:

```markdown
# Debezium Wire Format

## What is Debezium?

[3 paragraphs explaining Debezium CDC, its role in the data ecosystem, and why
so many tools understand its message format. Explain that Debezium is to CDC what
SQL is to databases — a shared language that many tools speak.]

## How pg_tide Speaks Debezium

### Forward Direction (Producing Debezium Messages)

[Explain how pg_tide translates outbox events into Debezium-shaped messages
that downstream tools like Apache Iceberg, Flink CDC, ksqlDB, and Materialize
can consume without any custom code.]

### Reverse Direction (Consuming Debezium Messages)

[Explain how pg_tide can consume messages produced by Debezium Server from
Oracle, Db2, MongoDB, Cassandra, Vitess, and Spanner — routing them into
inbox tables with proper deduplication.]

## Encoding Options

### JSON (Default)
### Avro with Schema Registry
### Protobuf with Schema Registry

[Each with complete configuration and explanation]

## Tombstone Records

[Explain what tombstones are, why Kafka log compaction needs them, how
pg_tide emits them for DELETE operations]

## Compatibility Matrix

| Downstream Tool | Tested | Notes |
|----------------|--------|-------|
| Apache Iceberg | ✅ | via iceberg-sink connector |
| Apache Flink CDC | ✅ | flink-cdc-connectors |
| ksqlDB | ✅ | native Debezium format |
| Materialize | ✅ | Debezium envelope |
| Apache Pinot | ✅ | Debezium JSON |
| Apache Druid | ✅ | kafka-indexing-service |
| StarRocks | ✅ | Routine Load |
| Snowflake Kafka Connector | ✅ | Debezium transform |

## Complete Example: PostgreSQL → Kafka (Debezium) → Apache Iceberg

[Full walkthrough showing the entire flow]
```

---

## 5. Feature Documentation

### 5.1 Production Operations Features

Each operational feature gets its own dedicated page with full depth.

#### Dead-Letter Queue (DLQ)

**File:** `features/dead-letter-queue.md` (~3,000 words)

- What is a dead-letter queue and why every production system needs one
- How pg_tide's DLQ works: the `tide.relay_dlq` catalog table
- Message lifecycle: publish → attempt delivery → exhaust retries → DLQ
- SQL API walkthrough: `dlq_list()`, `dlq_inspect()`, `dlq_replay()`, `dlq_purge_before()`
- Investigation workflow: examining a failed message, understanding the error
- Replay strategies: single message, batch by time range, filtered replay
- Automated DLQ monitoring with alerting examples
- DLQ retention policies and cleanup
- Complete example: intentionally failing a message, inspecting it, fixing the issue, replaying

#### Circuit Breaker

**File:** `features/circuit-breaker.md` (~2,000 words)

- The circuit breaker pattern explained for non-engineers (fuse box analogy)
- Three states: Closed (healthy), Open (tripped), Half-Open (testing recovery)
- When the circuit breaker activates: consecutive failure threshold
- What happens when the circuit is open: messages queue, no delivery attempts
- Recovery: half-open probes, gradual ramp-up
- Configuration: failure_threshold, recovery_timeout_secs, half_open_probe_count
- Monitoring: Prometheus metric for circuit breaker state
- Interaction with DLQ: messages during open circuit
- Complete example: simulate backend failure, observe circuit trip, recovery

#### Rate Limiting

**File:** `features/rate-limiting.md` (~1,500 words)

- Why rate limiting matters: protecting downstream systems, respecting API quotas
- Token-bucket algorithm explained in plain language (leaky bucket analogy)
- Configuration: messages_per_second per pipeline
- Behavior when rate limited: backpressure, not message loss
- Interaction with batch processing
- Common scenarios: webhook endpoints with rate limits, shared Kafka clusters
- Monitoring rate-limited pipelines

#### Schema Registry Integration

**File:** `features/schema-registry.md` (~2,500 words)

- What is a schema registry and why it matters for evolving systems
- Supported registries: Confluent Schema Registry, Apicurio Registry
- Supported formats: Avro, Protobuf
- Schema evolution: backward/forward/full compatibility explained
- Configuration: registry URL, subject naming strategy, schema auto-registration
- Integration with Debezium wire format
- Complete example: publishing Avro-encoded messages through Confluent Schema Registry
- Troubleshooting schema compatibility errors

#### JMESPath Transforms

**File:** `features/transforms.md` (~2,500 words)

- What are message transforms and when you need them
- JMESPath query language: a gentle introduction with examples
- Filter expressions: dropping messages that don't match criteria
- Projection expressions: reshaping message payloads before delivery
- Combining filter and projection in a pipeline
- Performance implications of complex expressions
- Common transform patterns: extracting fields, renaming keys, type coercion
- Complete examples: filtering by event type, projecting to webhook-friendly shape

#### Content-Based Routing

**File:** `features/routing.md` (~2,000 words)

- What is content-based routing: sending different messages to different destinations
- Template variables: `{stream_table}`, `{op}`, `{outbox_id}`, `{refresh_id}`
- Topic/subject templating with examples
- Dynamic routing patterns: per-table topics, per-operation channels
- Combining routing with transforms for complex pipelines
- Complete example: routing INSERT/UPDATE/DELETE to different Kafka topics

#### Webhook Signature Verification

**File:** `features/webhook-signatures.md` (~2,000 words)

- Why webhook signatures matter: preventing spoofed events
- Supported schemes: HMAC-SHA256, GitHub webhooks, Stripe webhooks, Svix, Fivetran
- How verification works: header inspection, signature computation, timing-safe comparison
- Configuration for each scheme
- What happens when verification fails
- Complete example: receiving GitHub webhook events with signature validation

#### Dry-Run & Replay Modes

**File:** `features/dry-run-replay.md` (~1,500 words)

- Dry-run mode: test your pipeline configuration without delivering messages
- What dry-run shows you: transforms, routing, serialization — everything except delivery
- When to use dry-run: new pipeline setup, transform debugging, pre-deployment validation
- Replay mode: reprocess already-acknowledged messages
- When to use replay: after fixing a bug in transforms, backfilling a new sink
- Safety considerations: idempotency requirements for replay
- Complete examples of both modes

#### SIGHUP Configuration Reload

**File:** `features/config-reload.md` (~1,000 words)

- Live configuration reload without restarting the relay
- What can be changed at runtime: pipeline configs, rate limits, transforms
- What requires a restart: connection strings, metrics address
- How it works: SIGHUP signal handling, NOTIFY-based reload from PostgreSQL
- Verifying reload succeeded: log messages, metrics
- Integration with Kubernetes ConfigMap reload and systemd

#### OpenTelemetry Tracing

**File:** `features/opentelemetry.md` (~2,000 words)

- What is distributed tracing and why it matters for event pipelines
- How pg_tide integrates with OpenTelemetry: spans per message batch, trace context propagation
- Configuration: OTLP endpoint, sampling rate, service name, resource attributes
- Trace visualization: what you'll see in Jaeger/Tempo/Honeycomb
- Correlating traces across services: trace context in message headers
- Performance impact of tracing and how to control it
- Complete example: setting up Jaeger and viewing relay traces

### 5.2 Connector Ecosystem Features

#### Singer Protocol Integration

**File:** `features/singer-protocol.md` (~3,500 words)

- What is the Singer protocol: the open standard for data integration
- The Meltano ecosystem: ~500 taps (sources) and targets (destinations)
- How pg_tide implements Singer: running taps/targets as child processes
- Protocol messages: RECORD, STATE, SCHEMA explained with examples
- STATE persistence: the `tide.singer_state` table, resumable incremental syncs
- SCHEMA handling: `on_schema_change` policy (ignore, log, fail, evolve)
- Schema drift detection: `tide.singer_schema_drift()` function
- Finding connectors: browsing Meltano Hub, compatibility notes
- Complete example: running a Singer tap to extract data into pg_tide
- Complete example: running a Singer target to send outbox events to a destination

#### Airbyte Protocol Integration

**File:** `features/airbyte-protocol.md` (~2,500 words)

- What is the Airbyte protocol: connector ecosystem with ~400 connectors
- How pg_tide runs Airbyte connectors: Docker-based execution
- Catalog discovery and stream selection
- State management and incremental sync
- Full refresh vs. incremental modes
- Complete example: using an Airbyte source connector with pg_tide

#### Fivetran HVR Integration

**File:** `features/fivetran.md` (~1,500 words)

- What is Fivetran's HVR endpoint format
- Webhook-based integration: how pg_tide exposes an HVR-compatible endpoint
- Authentication and signature verification
- Mapping Fivetran events to inbox rows
- Complete example configuration

### 5.3 High-Availability & Coordination Features

#### Advisory Lock Coordination

**File:** `features/ha-coordination.md` (~2,500 words)

- The problem: multiple relay instances must not process the same pipeline
- How PostgreSQL advisory locks solve this without external coordination
- Relay group IDs: namespacing lock sets
- Automatic failover: when a relay instance dies, others claim its pipelines
- Split-brain prevention: how advisory locks are inherently safe
- Multi-instance deployment patterns: active-passive, active-active partitioned
- Monitoring lock state and detecting stuck instances
- Complete example: deploying two relay instances with automatic failover

#### Graceful Shutdown

**File:** `features/graceful-shutdown.md` (~1,200 words)

- What happens when the relay receives SIGTERM
- Drain sequence: stop accepting new batches, complete in-flight messages, flush
- Configurable drain timeout: what happens if messages don't complete in time
- Integration with Kubernetes pod termination
- Zero-message-loss shutdown guarantee (with caveats)
- Configuration and monitoring

### 5.4 Observability Features

#### Prometheus Metrics

**File:** `features/metrics.md` (~3,000 words)

- Complete metric reference with every metric explained:
  - `pg_tide_messages_forwarded_total` — what it counts, labels, alert thresholds
  - `pg_tide_inbox_received_total` — reverse pipeline throughput
  - `pg_tide_errors_total` — error rate by type
  - `pg_tide_dlq_size` — DLQ depth
  - `pg_tide_outbox_backlog` — messages waiting to be relayed
  - `pg_tide_forward_latency_seconds` — histogram with p50/p95/p99
  - `pg_tide_circuit_breaker_state` — gauge (0=closed, 1=open, 2=half-open)
  - `pg_tide_retry_attempts_total` — retry pressure indicator
  - `pg_tide_consumer_lag` — per-consumer-group lag
- Prometheus scrape configuration
- Useful PromQL queries for dashboards
- Alert rule templates with recommended thresholds

#### Relay Health Dashboard

**File:** `features/dashboards.md` (~2,000 words)

- The included Perses/Grafana dashboard: what it shows
- Importing the dashboard: step-by-step for Grafana and Perses
- Panel explanations: what each panel tells you about system health
- Customization: adding panels for your specific use case
- Screenshot walkthrough of the dashboard under normal and degraded conditions

---

## 6. Expanded Concept Documentation

### 6.1 The Transactional Outbox Pattern

**File:** `concepts/transactional-outbox.md` (~4,000 words)

This is the foundational concept. It deserves a standalone, deep-dive page that explains
the pattern from absolute first principles.

- **The problem of dual writes** — Explain with a concrete story: your application
  processes a payment and needs to send an event. If you write to the database and
  then send to Kafka, what happens if the process crashes between those two operations?
  Walk through the failure scenarios with timeline diagrams.

- **Naive solutions and why they fail** — Two-phase commit (too slow, not supported by
  most message brokers), retry with deduplication (doesn't solve the initial inconsistency),
  saga pattern (works but complex). Explain each honestly.

- **The outbox pattern** — The elegant solution: write the event to a table in the same
  database transaction as the business data. A separate process (the relay) reads the
  outbox and delivers messages to external systems. Because the write is atomic,
  consistency is guaranteed.

- **How pg_tide implements this** — The `tide.outbox_create()` function, the underlying
  tables, the relay polling mechanism, notification-driven wake-up, retention and cleanup.

- **Exactly-once delivery** — How the combination of transactional outbox + inbox
  deduplication achieves exactly-once end-to-end semantics (with honest caveats).

### 6.2 The Idempotent Inbox Pattern

**File:** `concepts/idempotent-inbox.md` (~3,000 words)

- **The problem of duplicate delivery** — Why messages can arrive more than once:
  network retries, consumer rebalancing, relay restarts. Explain with concrete examples.

- **Idempotency explained** — The mathematical concept made accessible: an operation
  that produces the same result whether applied once or multiple times.

- **How the inbox achieves idempotency** — UNIQUE constraint on dedup_key, the processing
  lifecycle (received → processed | failed), TTL-based cleanup.

- **Designing idempotent consumers** — Guidance for application code: how to use the
  inbox correctly, common patterns, what makes a good dedup_key.

### 6.3 Consumer Groups & Offset Management

**File:** `concepts/consumer-groups.md` (~2,500 words)

- **The shared newspaper analogy** — Multiple services reading the same outbox, each
  tracking their own position independently.

- **How offsets work** — Sequential IDs, commit semantics, what happens on restart.

- **Visibility leases** — Preventing duplicate processing during relay handover.

- **Auto-offset-reset policies** — What happens when a new consumer group joins:
  earliest (replay all), latest (start from now), specific offset.

---

## 7. Operational Documentation

### 7.1 Deployment Architectures

**File:** `operations/deployment-architectures.md` (~4,000 words)

- **Single-node development** — Docker Compose with PostgreSQL + relay + NATS/Kafka
  for local development. Complete docker-compose.yml with annotations.

- **Production single-relay** — One relay instance for moderate throughput. When this
  is sufficient, how to size it, monitoring setup.

- **High-availability pair** — Two relay instances with advisory lock failover. How
  pipelines are distributed, failover timing, monitoring for split-brain.

- **Horizontally scaled** — Multiple relay instances each handling a subset of pipelines.
  Pipeline assignment strategies, scaling triggers, connection pool sizing.

- **Multi-region** — Relay instances in different regions with cross-region pipeline
  distribution. Latency considerations, network partition behavior.

- **Kubernetes** — Complete manifests: Deployment, Service, ServiceMonitor, ConfigMap,
  Secret references. Health checks, resource requests/limits, HPA configuration.

- **Helm chart** — The included Helm chart explained: every values.yaml parameter
  with context and recommendations.

### 7.2 Capacity Planning

**File:** `operations/capacity-planning.md` (~2,500 words)

- **Throughput calculator** — Given message size × messages/sec × pipeline count,
  what resources does the relay need? What PostgreSQL IOPS are required?

- **PostgreSQL tuning for outbox workloads** — shared_buffers, wal_level,
  max_wal_size, checkpoint timing. Explain each parameter's impact.

- **Connection pool sizing** — Formula based on pipeline count × batch size.

- **Storage planning** — How much disk space the outbox tables consume given
  retention hours × throughput × average message size.

- **Network bandwidth** — Calculating bandwidth requirements for relay → sink traffic.

- **Cost modeling** — Back-of-envelope cost calculation for cloud deployments:
  PostgreSQL instance cost + relay compute + sink-specific costs.

### 7.3 Monitoring & Alerting Cookbook

**File:** `operations/monitoring-cookbook.md` (~3,500 words)

- **Essential alerts** — The 5 alerts every deployment should have from day one:
  relay down, DLQ growing, lag increasing, error rate spike, circuit breaker open.
  Complete AlertManager rules for each.

- **SQL monitoring queries** — 20 useful queries for inspecting system state:
  messages in flight, consumer lag per group, DLQ contents, outbox sizes, relay
  status, lock holders. Each with explanation of what it tells you.

- **Dashboard design** — What panels belong on an operational dashboard vs. a
  business dashboard. Golden signals applied to pg_tide.

- **Log analysis** — Key log messages and what they indicate. Structured logging
  fields for filtering in your log aggregator.

- **Runbook templates** — For each alert: what it means, severity, investigation
  steps, remediation, escalation criteria.

### 7.4 Troubleshooting Guide

**File:** `operations/troubleshooting-guide.md` (~4,000 words)

Structured as symptom → diagnosis → resolution:

- **No messages flowing** — relay connects but outbox messages accumulate.
  Diagnostic steps: check consumer group, verify pipeline enabled, inspect locks.

- **Duplicate messages downstream** — Messages appear more than once in the sink.
  Diagnostic steps: check idempotent producer config, verify ack handling.

- **High latency** — Messages take too long from publish to sink delivery.
  Diagnostic steps: batch size tuning, polling interval, network latency.

- **DLQ filling up** — Messages consistently failing delivery.
  Diagnostic steps: inspect DLQ entries, check sink availability, verify credentials.

- **Circuit breaker stuck open** — Relay won't deliver even after backend recovers.
  Diagnostic steps: check recovery timeout, manually close circuit.

- **Memory growth** — Relay consuming increasing memory.
  Diagnostic steps: batch size, message size, connection leaks.

- **Connection exhaustion** — PostgreSQL reports "too many connections."
  Diagnostic steps: pool configuration, pipeline count, idle connections.

- **Extension upgrade failures** — ALTER EXTENSION fails.
  Diagnostic steps: version compatibility, dependent objects, retry strategy.

---

## 8. Tutorial Documentation

### 8.1 Getting Started Tutorial (Complete Rewrite)

**File:** `tutorials/getting-started.md` (~5,000 words)

A single, comprehensive tutorial that takes a reader from zero to a working pipeline:

- **Scenario:** An e-commerce application that needs to notify a warehouse service
  when orders are placed. The application writes to PostgreSQL; the warehouse service
  consumes from NATS JetStream.

- **Act 1: Understanding the problem** — Walk through what happens today without
  pg_tide: dual writes, lost messages, inconsistency. Show the broken code.

- **Act 2: Setting up pg_tide** — Install the extension, create an outbox, modify
  the application to publish events transactionally.

- **Act 3: The relay** — Configure and start the relay binary. Watch messages flow
  from outbox to NATS. Verify with NATS CLI tools.

- **Act 4: The inbox** — Add the warehouse service with an inbox. Show how dedup
  prevents double-processing. Simulate a failure and show recovery.

- **Act 5: Production concerns** — Add monitoring, DLQ, circuit breaker. Show the
  complete production-ready configuration.

### 8.2 Backend-Specific Tutorials

| Tutorial | File | Scenario |
|----------|------|----------|
| PostgreSQL → Kafka → Flink | `tutorials/kafka-flink.md` | Real-time analytics with CDC |
| PostgreSQL → S3 (Parquet) → Athena | `tutorials/data-lake-loading.md` | Cost-effective analytics |
| Multi-service choreography | `tutorials/microservice-events.md` | 3 services coordinating via outbox/inbox |
| CDC replication via Debezium format | `tutorials/debezium-replication.md` | Using Debezium wire format for downstream compatibility |
| Singer-based ETL pipeline | `tutorials/singer-etl.md` | Using Meltano Hub connectors with pg_tide |
| Notification fan-out | `tutorials/notification-fanout.md` | Slack + PagerDuty + webhook from one outbox |
| Cross-region event relay | `tutorials/cross-region.md` | Multi-region deployment with eventual consistency |

### 8.3 Real-World Scenario Deep Dives

**File:** `tutorials/real-world-scenarios.md` (~5,000 words)

Five detailed scenarios, each ~1,000 words:

1. **E-commerce order fulfillment** — Payment → inventory → shipping → email,
   all driven by outbox events through different sinks (Kafka for inventory,
   webhook for shipping API, SES/SMTP for email via webhook).

2. **Multi-tenant SaaS webhook delivery** — Publishing customer-specific webhooks
   with per-tenant routing, rate limiting, DLQ for failed deliveries, and
   automatic retry with circuit breaking.

3. **Real-time data warehouse sync** — Outbox events → Debezium format → Kafka →
   Snowflake/BigQuery. Schema evolution handling, backfill strategy, cost control.

4. **IoT telemetry ingestion** — MQTT devices → pg_tide inbox → time-series
   processing. High-volume, small messages, batching strategies.

5. **Compliance audit trail** — Using the outbox as an immutable audit log that's
   relayed to object storage (S3 Parquet) for long-term retention and regulatory
   compliance.

---

## 9. Integration Documentation

### 9.1 Database & Infrastructure Integrations

| Integration | File | Topics |
|-------------|------|--------|
| **CloudNativePG** | `integrations/cloudnativepg.md` | Operator configuration, shared_preload_libraries, extension installation, HA failover behavior |
| **PgBouncer** | `integrations/pgbouncer.md` | Transaction mode compatibility, NOTIFY limitations, connection routing |
| **pg_trickle** | `integrations/pg-trickle.md` | Migration path, feature comparison, coexistence patterns |
| **dbt** | `integrations/dbt.md` | Using dbt to manage outbox/inbox lifecycle, macros, CI/CD |
| **Terraform** | `integrations/terraform.md` | Managing pg_tide resources with Terraform PostgreSQL provider |
| **GitHub Actions** | `integrations/github-actions.md` | CI/CD pipeline for pg_tide extension upgrades and relay deployments |

### 9.2 Observability Stack Integrations

| Integration | File | Topics |
|-------------|------|--------|
| **Prometheus + Grafana** | `integrations/prometheus-grafana.md` | Scrape config, dashboard import, alerting rules |
| **Datadog** | `integrations/datadog.md` | OpenMetrics integration, log collection, APM correlation |
| **OpenTelemetry Collector** | `integrations/otel-collector.md` | OTLP export config, trace sampling, span attributes |

---

## 10. SQL Reference Expansion

The SQL reference pages need significant expansion beyond the existing function
signature tables. Each page should become a comprehensive guide.

### 10.1 Per-Page Expansion Plan

#### Outbox API (`sql-reference/outbox-api.md`)

Expand to ~3,000 words:
- Each function gets a "When to use" paragraph, full parameter explanation, return
  value description, 2–3 usage examples, error conditions, and performance notes.
- Add a "Common Patterns" section: publishing in triggers, publishing from application
  code, batch publishing, conditional publishing.
- Add a "Migration Patterns" section: moving from DIY outbox tables to pg_tide.

#### Inbox API (`sql-reference/inbox-api.md`)

Expand to ~2,500 words:
- Processing workflow explained step-by-step: receive → process → mark processed/failed
- Replay mechanics and when to use them
- Integration patterns: polling from application, trigger-driven processing
- Deduplication key design guidance

#### Relay API (`sql-reference/relay-api.md`)

Expand to ~2,500 words:
- Pipeline lifecycle: create → configure → enable → monitor → disable → delete
- Configuration JSONB structure fully documented
- Secret interpolation patterns
- Multi-pipeline management patterns

#### Consumer Groups API (`sql-reference/consumer-groups-api.md`)

Expand to ~2,000 words:
- Consumer group lifecycle with state diagram
- Offset management: commit, reset, inspect
- Heartbeat and liveness detection
- Multi-consumer coordination patterns

#### Catalog Tables (`sql-reference/catalog-tables.md`)

Expand to ~3,000 words:
- Every table described with column definitions and relationships
- Entity-relationship diagram (Mermaid)
- 20 useful monitoring/debugging SQL queries with explanations
- Index strategy and vacuum considerations
- Direct table access patterns (when SQL functions aren't enough)

---

## 11. New Sections to Add

### 11.1 Security Guide

**File:** `reference/security-guide.md` (~3,000 words)

- Threat model: what attacks pg_tide's architecture prevents
- Role-based access control: GRANT patterns for operators, applications, read-only
- Secret management: environment variables, file references, Vault integration patterns
- Network security: TLS everywhere, mTLS for Kafka, VPC/private endpoints
- Payload encryption: encrypting sensitive data before outbox publish
- Audit logging: tracking who published what and when
- Compliance notes: GDPR data minimization, SOC2 access controls

### 11.2 Migration Guides

**File:** `guides/migrating-to-pg-tide.md` (~3,000 words)

- **From pg_notify** — Why pg_notify loses messages and how to switch to pg_tide
  with zero downtime. Step-by-step migration with rollback plan.

- **From a DIY outbox table** — You already have a custom outbox table. How to
  migrate to pg_tide's managed outbox with minimal application changes.

- **From Debezium** — When Debezium's JVM overhead or operational complexity
  becomes a burden. How pg_tide provides similar functionality with less infrastructure.

- **From application-level messaging** — You're using direct HTTP calls between
  services. How to introduce pg_tide for reliability without a big-bang rewrite.

### 11.3 Architecture Decision Records

**File:** `reference/architecture-decisions.md` (~2,500 words)

- **Why PostgreSQL advisory locks (not ZooKeeper/etcd)?** — Explain the decision
  to use PostgreSQL itself for coordination, avoiding external dependencies.

- **Why a sidecar binary (not in-process)?** — The relay is separate from the
  extension. Explain why: resource isolation, independent scaling, language freedom.

- **Why polling + notify (not logical replication)?** — Explain the trade-offs
  of polling vs. CDC-based approaches. Honest comparison.

- **Why TOML + SQL config (not just one)?** — The hybrid configuration model
  and when to use each approach.

---

## 12. Documentation Infrastructure

### 12.1 Search & Navigation

- Add a comprehensive glossary page defining all pg_tide-specific terms
- Add breadcrumb context to every page ("You are in: Relay Guide → Backends → Kafka")
- Add "Related pages" footer to every page with 3–5 contextual links
- Ensure SUMMARY.md navigation matches the expanded structure

### 12.2 Versioning

- Document which features are available in which version
- Add "Since: v0.X.0" badges to feature pages
- Maintain a version compatibility matrix: extension version × relay version

### 12.3 Examples Repository

- Create a `docs/examples/` directory with complete, runnable examples
- Docker Compose files for each tutorial scenario
- TOML configuration files for each sink/source combination
- SQL scripts for common setup patterns

---

## 13. Implementation Phases

### Phase 1: Core Feature Documentation (Weeks 1–2)

**Focus: The features that every user will encounter**

1. Rewrite getting-started tutorial (5,000 words)
2. Write transactional outbox concept page (4,000 words)
3. Write idempotent inbox concept page (3,000 words)
4. Write Kafka sink page (3,000 words) — template for all other sinks
5. Write NATS sink page (2,500 words)
6. Write PostgreSQL outbox source page (2,500 words)
7. Write webhook sink page (2,500 words)
8. Write native wire format page (1,500 words)
9. Expand outbox API reference (3,000 words)
10. Expand inbox API reference (2,500 words)

**Deliverable: ~30,000 words of core documentation**

### Phase 2: Production Operations (Weeks 3–4)

**Focus: What operators need to run pg_tide confidently**

11. Write DLQ feature page (3,000 words)
12. Write circuit breaker feature page (2,000 words)
13. Write rate limiting feature page (1,500 words)
14. Write monitoring cookbook (3,500 words)
15. Write troubleshooting guide (4,000 words)
16. Write deployment architectures (4,000 words)
17. Write capacity planning (2,500 words)
18. Write HA coordination page (2,500 words)
19. Write graceful shutdown page (1,200 words)
20. Write metrics reference (3,000 words)

**Deliverable: ~27,000 words of operational documentation**

### Phase 3: Ecosystem & Integrations (Weeks 5–6)

**Focus: All supported sinks, sources, and wire formats**

21. Write remaining message queue sinks (RabbitMQ, Redis, SQS, Kinesis, Pub/Sub, Service Bus, Event Hubs, MQTT) — 8 × 2,000 words
22. Write analytics sinks (ClickHouse, Snowflake, BigQuery, Iceberg, Delta, DuckLake, MongoDB, Elasticsearch, Object Storage, Arrow Flight) — 10 × 2,000 words
23. Write notification sinks (Slack, Discord, PagerDuty) — 3 × 1,500 words
24. Write connector ecosystem pages (Singer, Airbyte, Fivetran) — 3 × 2,500 words
25. Write all source pages — 15 × 1,500 words
26. Write wire format pages (Debezium, Maxwell, Canal, Custom CDC) — 4 × 2,500 words
27. Write wire format overview page (2,000 words)

**Deliverable: ~75,000 words of system-specific documentation**

### Phase 4: Tutorials & Advanced Topics (Weeks 7–8)

**Focus: Learning paths and advanced usage**

28. Write backend-specific tutorials — 7 × 3,000 words
29. Write real-world scenarios deep dives (5,000 words)
30. Write security guide (3,000 words)
31. Write migration guides (3,000 words)
32. Write architecture decision records (2,500 words)
33. Write schema registry feature page (2,500 words)
34. Write JMESPath transforms page (2,500 words)
35. Write content-based routing page (2,000 words)
36. Write OpenTelemetry page (2,000 words)
37. Write config reload / dry-run / replay page (1,500 words)
38. Write webhook signatures page (2,000 words)

**Deliverable: ~45,000 words of advanced documentation**

### Phase 5: Polish & Infrastructure (Week 9)

39. Add Mermaid diagrams to all concept and architecture pages (~15 diagrams)
40. Cross-reference audit: ensure every page links to related content
41. Create glossary page with all pg_tide terminology
42. Create version compatibility matrix
43. Create examples/ directory with runnable Docker Compose files
44. Final style and tone consistency pass across all pages
45. Update SUMMARY.md navigation for the complete expanded structure

---

## 14. Success Metrics

| Metric | Current (v0.11.0) | Target |
|--------|-------------------|--------|
| Total documentation words | ~18,500 | ~175,000+ |
| Sinks with dedicated pages | 0 (one combined page) | 30 |
| Sources with dedicated pages | 0 (one combined page) | 15 |
| Wire formats documented | 0 | 6 |
| Feature-specific pages | 0 | 15+ |
| Tutorials | 5 (thin) | 12 (comprehensive) |
| Runnable examples | 0 | 15+ |
| Mermaid diagrams | 0 | 15+ |
| Average page word count | ~400 | ~2,000 |
| Real-world scenarios | 5 (brief) | 12 (detailed) |

---

## 15. Content Priorities Summary

**Tier 1 — Write immediately (highest user impact):**
- Getting-started tutorial rewrite
- Kafka, NATS, Webhook sink pages (most common backends)
- Transactional outbox and inbox concept pages
- DLQ and circuit breaker operational pages
- Debezium wire format (unlocks largest ecosystem)
- Monitoring cookbook and troubleshooting guide

**Tier 2 — Write next (common use cases):**
- All cloud messaging sinks (SQS, Pub/Sub, Kinesis, Service Bus, Event Hubs)
- Analytics sinks (ClickHouse, Snowflake, BigQuery)
- Singer/Airbyte connector ecosystem pages
- Deployment architectures and capacity planning
- Schema registry and transforms features

**Tier 3 — Complete the picture (full coverage):**
- Remaining analytics sinks (Iceberg, Delta, DuckLake, MongoDB)
- Notification sinks (Slack, Discord, PagerDuty)
- All source pages
- Maxwell, Canal, Custom CDC wire formats
- Advanced tutorials and migration guides
- Security guide and architecture decisions

---

## 16. File Structure (Final)

```
docs/src/
├── introduction.md
├── SUMMARY.md
├── glossary.md                              NEW
│
├── evaluate/
│   ├── choosing-pg-tide.md
│   └── architecture.md
│
├── getting-started/
│   ├── installation.md
│   └── first-pipeline.md                   REWRITE (5,000 words)
│
├── concepts/
│   ├── transactional-outbox.md             NEW (4,000 words)
│   ├── idempotent-inbox.md                 NEW (3,000 words)
│   ├── consumer-groups.md                  NEW (2,500 words)
│   ├── message-guarantees.md               EXPAND
│   └── consumption-and-relay.md            EXPAND
│
├── sinks/                                   NEW SECTION
│   ├── overview.md                          Choosing a sink, comparison matrix
│   ├── kafka.md
│   ├── nats.md
│   ├── rabbitmq.md
│   ├── redis.md
│   ├── sqs.md
│   ├── kinesis.md
│   ├── pubsub.md
│   ├── servicebus.md
│   ├── eventhubs.md
│   ├── mqtt.md
│   ├── clickhouse.md
│   ├── snowflake.md
│   ├── bigquery.md
│   ├── iceberg.md
│   ├── delta.md
│   ├── ducklake.md
│   ├── mongodb.md
│   ├── elasticsearch.md
│   ├── object-storage.md
│   ├── arrow-flight.md
│   ├── webhook.md
│   ├── slack.md
│   ├── discord.md
│   ├── pagerduty.md
│   ├── singer.md
│   ├── airbyte.md
│   ├── fivetran.md
│   ├── pg-inbox.md
│   ├── pg-outbox.md
│   └── stdout.md
│
├── sources/                                 NEW SECTION
│   ├── overview.md                          Choosing a source, comparison matrix
│   ├── outbox.md
│   ├── kafka.md
│   ├── nats.md
│   ├── rabbitmq.md
│   ├── redis.md
│   ├── sqs.md
│   ├── kinesis.md
│   ├── pubsub.md
│   ├── servicebus.md
│   ├── eventhubs.md
│   ├── mqtt.md
│   ├── webhook-receiver.md
│   ├── singer.md
│   ├── airbyte.md
│   └── stdin.md
│
├── wire-formats/                            NEW SECTION
│   ├── overview.md
│   ├── native.md
│   ├── debezium.md
│   ├── maxwell.md
│   ├── canal.md
│   └── cdc-json.md
│
├── features/                                NEW SECTION
│   ├── dead-letter-queue.md
│   ├── circuit-breaker.md
│   ├── rate-limiting.md
│   ├── schema-registry.md
│   ├── transforms.md
│   ├── routing.md
│   ├── webhook-signatures.md
│   ├── dry-run-replay.md
│   ├── config-reload.md
│   ├── opentelemetry.md
│   ├── ha-coordination.md
│   ├── graceful-shutdown.md
│   ├── metrics.md
│   ├── dashboards.md
│   ├── singer-protocol.md
│   ├── airbyte-protocol.md
│   └── fivetran.md
│
├── sql-reference/
│   ├── outbox-api.md                       EXPAND (3,000 words)
│   ├── inbox-api.md                        EXPAND (2,500 words)
│   ├── relay-api.md                        EXPAND (2,500 words)
│   ├── consumer-groups-api.md              EXPAND (2,000 words)
│   └── catalog-tables.md                   EXPAND (3,000 words)
│
├── relay-guide/
│   ├── configuration.md                    EXPAND
│   ├── cli-reference.md                    EXPAND
│   ├── backends.md                         KEEP (overview → links to sinks/sources)
│   ├── error-handling-guide.md             EXPAND
│   └── monitoring.md                       EXPAND
│
├── operations/
│   ├── deployment-architectures.md         NEW (4,000 words)
│   ├── deployment-guide.md                 KEEP (practical setup steps)
│   ├── capacity-planning.md                NEW (2,500 words)
│   ├── monitoring-cookbook.md              NEW (3,500 words)
│   ├── troubleshooting-guide.md           NEW (4,000 words)
│   ├── scaling.md                          EXPAND
│   └── maintenance.md                      EXPAND
│
├── tutorials/
│   ├── getting-started.md                  NEW (5,000 words)
│   ├── end-to-end-pipeline.md             EXPAND
│   ├── kafka-flink.md                      NEW
│   ├── data-lake-loading.md               NEW
│   ├── microservice-events.md             NEW
│   ├── debezium-replication.md            NEW
│   ├── singer-etl.md                      NEW
│   ├── notification-fanout.md             NEW
│   ├── cross-region.md                    NEW
│   ├── real-world-scenarios.md            EXPAND
│   ├── bidirectional-sync.md              EXPAND
│   ├── fan-out-pattern.md                 EXPAND
│   └── dead-letter-queue.md               EXPAND
│
├── guides/                                  NEW SECTION
│   ├── migrating-to-pg-tide.md            NEW (3,000 words)
│   └── security-guide.md                  NEW (3,000 words)
│
├── integrations/
│   ├── cloudnativepg.md                   EXPAND
│   ├── pgbouncer.md                       EXPAND
│   ├── pg-trickle.md                      EXPAND
│   ├── dbt.md                             EXPAND
│   ├── terraform.md                       NEW
│   ├── github-actions.md                  NEW
│   ├── prometheus-grafana.md              NEW
│   ├── datadog.md                         NEW
│   └── otel-collector.md                  NEW
│
├── reference/
│   ├── security.md                        EXPAND
│   ├── architecture-decisions.md          NEW (2,500 words)
│   └── version-compatibility.md           NEW
│
└── examples/                               NEW (runnable files)
    ├── docker-compose-kafka.yml
    ├── docker-compose-nats.yml
    ├── docker-compose-full.yml
    ├── relay-config-kafka.toml
    ├── relay-config-nats.toml
    ├── relay-config-s3.toml
    ├── setup-outbox.sql
    ├── setup-inbox.sql
    └── monitoring-queries.sql
```

**Total pages: ~95** (up from 28 in current consolidated structure)
**Estimated total word count: ~175,000 words**

---

## 17. Quality Gates

Before any documentation page is considered complete:

- [ ] Opens with a problem statement or motivation (not an API signature)
- [ ] Contains at least one complete, runnable code example
- [ ] Explains "why" before "how" for every concept introduced
- [ ] Written in accessible language (no unexplained jargon)
- [ ] Includes troubleshooting section for operational pages
- [ ] Links to at least 3 related pages
- [ ] Has been reviewed for technical accuracy against source code
- [ ] Passes prose readability check (Flesch-Kincaid grade level ≤ 12)
- [ ] Contains appropriate Mermaid diagrams for visual concepts
- [ ] Every configuration option explained with realistic default rationale
