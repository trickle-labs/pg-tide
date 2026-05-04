# pg_tide Roadmap

> **Audience:** Product managers, stakeholders, and technically curious readers
> who want to understand what each release delivers and why it matters —
> without needing to read Rust code or SQL specifications.

## Versions

### Foundation (v0.1.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| v0.1.0 | The complete foundation — transactional outbox, idempotent inbox, relay catalog, and core relay binary extracted from pg_trickle | ✅ Released | Large | [CHANGELOG.md](CHANGELOG.md) |
| v0.2.0 | Post-launch hardening — observability improvements, Docker enhancements, CI fixes, pgrx compatibility | ✅ Released | Small | [CHANGELOG.md](CHANGELOG.md) |

### Relay Binary — Forward & Reverse Modes (v0.3.x – v0.4.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| v0.3.0 | Core relay framework: multi-pipeline coordinator, secret interpolation, outbox poller, Tier 1 sinks (NATS JetStream, Apache Kafka, HTTP Webhook, stdout/file), metrics, graceful shutdown | ✅ Released | Large | [plans/relay-cli-phase1.md](plans/relay-cli-phase1.md) |
| v0.4.0 | Relay completion: forward Tier 2 sinks (Redis Streams, SQS, RabbitMQ, PostgreSQL inbox), full reverse mode (all source backends writing to pg_tide inbox), subject/topic routing, integration tests, Docker distribution | ✅ Released | Large | [plans/relay-cli-phase1.md](plans/relay-cli-phase1.md) |

### Cloud & Analytics Backends (v0.5.x – v0.6.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| v0.5.0 | Cloud provider parity: Google Cloud Pub/Sub, Amazon Kinesis Data Streams, Azure Service Bus, Elasticsearch / OpenSearch | 🔜 Planned | Large | [plans/relay-cli-phase2.md](plans/relay-cli-phase2.md) |
| v0.6.0 | IoT, analytics, and data lake: MQTT v5, Azure Event Hubs, Object Storage (S3 / GCS / Azure Blob with JSONL + Parquet), ClickHouse | 🔜 Planned | Large | [plans/relay-cli-phase2.md](plans/relay-cli-phase2.md) |

### Operational Excellence (v0.7.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| v0.7.0 | Production-grade relay operations: dead-letter queue, Confluent / Apicurio schema registry (Avro + Protobuf), JMESPath message transforms, content-based routing, rate limiting, circuit breaker, SIGHUP config reload, dry-run / replay mode, OpenTelemetry tracing, webhook signature verification (HMAC / GitHub / Stripe / Svix), notification sinks (Slack, Discord, PagerDuty) | 🔜 Planned | Large | [plans/relay-cli-phase2.md](plans/relay-cli-phase2.md) |

### Analytics Sinks & GA (v0.8.x – v1.0)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| v0.8.0 | Analytics sinks: MongoDB, Snowflake, BigQuery, Apache Iceberg, Delta Lake, DuckLake | 🔜 Planned | Large | [plans/relay-cli-phase3.md](plans/relay-cli-phase3.md) |
| v1.0.0 | Production GA: relay dashboard (ratatui TUI), encryption envelope with KMS integration | 🔜 Planned | Medium | [plans/relay-cli-phase3.md](plans/relay-cli-phase3.md) |

### Extended Ecosystem & Plugin Support (v1.1.x – v1.2.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| v1.1.0 | Extended connector ecosystems: Airbyte protocol adapter (~400 connectors), dlt integration (~100 sources), Redpanda Connect / Benthos (~200 inputs/outputs), Fivetran HVR endpoint, Apache Arrow Flight / gRPC, AMQP 1.0 (Azure Service Bus, Qpid), Singer protocol (Meltano Hub — ~500 taps and targets), webhook flavors (n8n / Zapier) | 🔜 Future | Large | — |
| v1.2.0 | Plugin extensibility: WASM plugin system for custom backends | 🔜 Future | Large | — |
