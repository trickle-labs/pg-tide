# pg_tide

[![CI](https://github.com/trickle-labs/pg-tide/actions/workflows/ci.yml/badge.svg)](https://github.com/trickle-labs/pg-tide/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**Transactional outbox, idempotent inbox, and relay pipelines for PostgreSQL 18+.**

pg_tide gives your PostgreSQL database a built-in messaging backbone. Publish events atomically within your existing transactions — no dual-writes, no distributed transactions, no message broker required at the database layer.

When you're ready to fan out to Kafka, NATS, Redis Streams, or any analytics platform, the `pg-tide` relay binary bridges the gap: at-least-once delivery with deduplication, hot-reload pipeline config, and HA failover — all configured with plain SQL.

## Features

- **Transactional Outbox** — publish messages inside any transaction; no 2PC, no dual-writes
- **Idempotent Inbox** — deduplication via unique event IDs; exactly-once delivery semantics at the application layer
- **Consumer Groups** — Kafka-style offset tracking with heartbeats and visibility leases
- **Relay Binary** — standalone `pg-tide` process; config lives in PostgreSQL and hot-reloads without restart
- **30 Sink Backends** — streaming, cloud, analytics, notifications, connectors, object storage, and cross-instance pg-tide fan-out; all sinks fully registered and integration-tested
- **Pluggable Wire Formats** — native, Debezium, CloudEvents, Maxwell, Canal, and custom CDC JSON
- **Multi-Tenant** — row-level security, per-tenant Prometheus labels, per-outbox publisher ACLs, and per-tenant advisory-lock namespacing
- **Outbox Table Partitioning** — declarative daily/weekly/monthly range partitioning with live migration and relay sweep integration
- **Replay Workbench** — rewind consumer offsets, preview replays, and manage the DLQ from SQL or CLI
- **HA Ready** — advisory-lock coordination with automatic worker crash detection and restart; `--self-test` and `--expect-extension-version` flags for Kubernetes readiness probes
- **Observable** — OpenTelemetry spans, Prometheus metrics, Grafana dashboard, and pre-built alerting rules included
- **Envelope Encryption Foundation** — KMS-backed AES-256-GCM envelope encryption (AWS KMS, GCP Cloud KMS, HashiCorp Vault, local key file); `LocalKeyFile` provider fully implemented in v0.35.0; cloud providers ship in v1.0.0

## Quick Start

```sql
-- Install the extension
CREATE EXTENSION pg_tide;

-- Create an outbox
SELECT tide.outbox_create_if_not_exists('orders', 24, NULL);

-- Publish a message atomically with your business transaction
BEGIN;
  INSERT INTO orders (id, total) VALUES (42, 99.99);
  SELECT tide.outbox_publish('orders',
    '{"order_id": 42, "total": 99.99}'::jsonb,
    '{"event_type": "order.created"}'::jsonb
  );
COMMIT;

-- Configure a relay pipeline (config persists in PostgreSQL)
SELECT tide.relay_set_outbox('orders-nats', 'orders', 'nats',
  '{"url": "nats://localhost:4222", "subject": "orders.events"}'::jsonb
);
```

Start the relay:

```bash
pg-tide --postgres-url "postgres://user:pass@localhost:5432/mydb"
```

Messages flow from the outbox to NATS. Change the pipeline config in PostgreSQL — the relay picks it up without a restart. See the [documentation](https://trickle-labs.github.io/pg-tide/) for full details.

## Installation

### Extension

```sql
CREATE EXTENSION pg_tide;
```

### Relay Binary

```bash
# From GitHub releases
curl -LO https://github.com/trickle-labs/pg-tide/releases/latest/download/pg-tide-x86_64-unknown-linux-gnu.tar.gz
tar xzf pg-tide-*.tar.gz && sudo mv pg-tide /usr/local/bin/

# Or via Docker (standard build)
docker pull ghcr.io/trickle-labs/pg-tide:latest

# Full build with every optional connector enabled
docker pull ghcr.io/trickle-labs/pg-tide:latest-full
```

Release artifacts and Docker images are signed with [sigstore/cosign](https://github.com/sigstore/cosign-installer) using keyless OIDC signing.

## Sink Backends

The relay ships with connectors for every major messaging and data platform:

| Category | Sinks |
|----------|-------|
| **Streaming** | Apache Kafka, NATS, Redis Streams, RabbitMQ (AMQP) |
| **Cloud messaging** | AWS SQS, Google Cloud Pub/Sub, AWS Kinesis, Azure Service Bus, Azure Event Hubs |
| **HTTP** | Webhook (with SSRF protection), Apache Arrow Flight |
| **Notifications** | Slack, PagerDuty, Twilio SMS, SendGrid Email, Firebase Cloud Messaging |
| **Analytics** | ClickHouse, MongoDB, Snowflake, BigQuery |
| **Object storage** | Apache Iceberg v2 (Parquet), Delta Lake v2, DuckLake |
| **Search** | Elasticsearch, MQTT v5 |
| **Connectors** | Singer taps/targets, Airbyte sources/destinations, Fivetran connectors |
| **pg-tide** | pg-inbox (deliver directly into another PostgreSQL inbox) |

Optional connectors are feature-gated. The `latest-full` Docker image and `--all-features` build include all of them.

## Wire Formats

All pipelines support a pluggable wire format selected per-pipeline in the catalog:

| Format | Direction | Description |
|--------|-----------|-------------|
| `native` | bidirectional | Default pg_tide JSON envelope |
| `debezium` | bidirectional | Debezium JSON — encode outbox rows, decode from Kafka CDC topics |
| `cloudevents` | bidirectional | CloudEvents v1.0 JSON with AsyncAPI 3.0 export |
| `maxwell` | decode only | Maxwell (MySQL CDC) JSON → inbox |
| `canal` | decode only | Alibaba Canal (MySQL CDC) JSON → inbox |
| `cdc_json` | bidirectional | Custom CDC JSON with user-supplied dot-notation path mapping |

The Debezium encoder emits tombstones after DELETE so Kafka log-compacted topics compact correctly.

## Operational CLI

The `pg-tide` binary includes several operational subcommands beyond the relay run-loop:

```bash
# Check connectivity and schema health
pg-tide doctor --postgres-url "postgres://..."

# Run startup self-test (for Kubernetes initContainers / CI pre-deployment gates)
pg-tide --self-test --postgres-url "postgres://..."

# Show all configured pipelines and consumer lag at a glance
pg-tide status --postgres-url "postgres://..."

# Include per-inbox fleet summary in status output
pg-tide status --postgres-url "postgres://..." --inbox-summary

# Verify the installed extension meets a minimum version (useful in initContainers)
pg-tide --expect-extension-version 0.34.0 --self-test --postgres-url "postgres://..."

# Delete consumed outbox rows older than the retention window
pg-tide sweep --postgres-url "postgres://..."

# Validate pipeline config without processing any messages
pg-tide validate-config --pipeline orders-nats

# Replay workbench
pg-tide replay preview  --pipeline orders-nats --from-lsn 0/1000000 --to-lsn 0/2000000
pg-tide replay dlq-requeue --pipeline orders-nats --event-id abc123

# Generate an AsyncAPI 3.0 document from relay catalog metadata (with live payload schema sampling)
pg-tide asyncapi export --postgres-url "postgres://..." --full-schema

# Validate local catalog against a published AsyncAPI spec
pg-tide asyncapi validate --spec-url https://example.com/asyncapi.yaml
```

## Security

- **Fail-closed TLS** — `sslmode=require` returns an error rather than silently downgrading to plaintext
- **Publisher ACLs** — `tide.outbox_grant_publish(outbox, role)` restricts which roles can publish to each outbox
- **SSRF protection** — webhook sinks reject loopback, link-local, private ranges, and plain HTTP by default
- **Secret redaction** — `${env:…}` and `${file:…}` references are replaced with `[REDACTED]` in logs
- **Supply-chain audit** — `cargo-deny` checks every dependency for RUSTSEC advisories and license compliance in CI
- **Envelope Encryption Foundation** — `tide.outbox_encryption_config` catalog table and `EncryptionEnvelope` trait for AES-256-GCM KMS-backed payload encryption; `LocalKeyFile` provider is fully implemented (including key rotation) in v0.35.0; cloud providers (AWS KMS, GCP Cloud KMS, HashiCorp Vault) ship in v1.0.0

## Observability

- **OpenTelemetry spans** — `relay.source.poll`, `relay.sink.publish`, `relay.transform.evaluate`, `relay.routing.apply`, `relay.dlq.insert`, `relay.schema_evolution.check`, and more; works with Jaeger, Tempo, Honeycomb, or Datadog
- **Prometheus metrics** — messages published/consumed, sink latency histogram, DLQ entries, pipeline health, consumer lag, connection pool utilisation, and per-tenant labels
- **Grafana dashboard** — pre-built dashboard in `pg-tide/dashboards/relay-health.json` with pipeline health, sink latency, connection pool, and per-tenant rows; metric names validated against `metrics.rs` in CI
- **Alerting rules** — `pg-tide/dashboards/alerts.yaml` ships five production-ready Prometheus alerting rules (pipeline paused, high consumer lag, DLQ depth, DLQ write error, pool saturation)

## SQL API Overview

All functions live in the `tide` schema. Key functions by area:

**Outbox**

| Function | Description |
|----------|-------------|
| `tide.outbox_create(name, retention_hours, max_size, partition_strategy)` | Create a named outbox; `partition_strategy` is `'none'` (default), `'daily'`, `'weekly'`, or `'monthly'` |
| `tide.outbox_create_if_not_exists(name, retention_hours, max_size, partition_strategy)` | Idempotent create; returns `TRUE` when newly created |
| `tide.outbox_publish(name, payload, headers)` | Publish a message atomically |
| `tide.outbox_status(name)` | Status summary as JSONB |
| `tide.outbox_grant_publish(outbox, role)` | Grant publish permission to a role |
| `tide.outbox_truncate_delivered(name)` | Delete consumed rows older than retention window; returns row count |
| `tide.outbox_convert_to_partitioned(name, strategy, confirm_shared_table_migration)` | Live-migrate an unpartitioned outbox to declarative range partitioning |

**Inbox**

| Function | Description |
|----------|-------------|
| `tide.inbox_create(name)` | Create a named inbox |
| `tide.inbox_mark_processed(name, event_id)` | Mark message processed |
| `tide.inbox_mark_failed(name, event_id, reason)` | Record failure with retry tracking |
| `tide.inbox_status(name)` | Status JSON; pass `NULL` for fleet-wide summary |

**Relay pipelines**

| Function | Description |
|----------|-------------|
| `tide.relay_set_outbox(name, outbox, sink_type, config)` | Configure forward pipeline (outbox → sink) |
| `tide.relay_set_inbox(name, inbox, source_type, config)` | Configure reverse pipeline (source → inbox) |
| `tide.relay_set_tenant(pipeline, tenant)` | Assign a pipeline to a tenant |
| `tide.relay_grant_tenant(pipeline, tenant, role)` | Grant tenant access |

**Consumer groups**

| Function | Description |
|----------|-------------|
| `tide.create_consumer_group(name, outbox)` | Create a consumer group |
| `tide.commit_offset(group, change_id)` | Commit consumer position |
| `tide.consumer_offset_rewind(pipeline, lsn)` | Admin offset rollback (guarded) |

**Replay & DLQ**

| Function | Description |
|----------|-------------|
| `tide.relay_replay_preview(pipeline, from_lsn, to_lsn)` | Dry-run replay; no offsets committed |
| `tide.dlq_resolve(pipeline, event_id)` | Mark DLQ entry resolved |
| `tide.dlq_requeue(pipeline, event_id)` | Reschedule DLQ entry for reprocessing |

**Backfill**

| Function | Description |
|----------|-------------|
| `tide.backfill_create(outbox, sink_pipeline, chunk_size)` | Create a cataloged backfill job |
| `tide.backfill_pause(job_id)` / `tide.backfill_resume(job_id)` | Pause or resume a backfill job |
| `tide.backfill_status(job_id)` | Job status JSON; `NULL` for fleet summary |

Views: `tide.outbox_pending` · `tide.consumer_lag` · `tide.inbox_fleet_summary`

## Multi-Tenant Support

pg_tide supports multi-tenant deployments where each tenant owns isolated pipelines:

```sql
-- Assign a pipeline to a tenant
SELECT tide.relay_set_tenant('orders-nats', 'acme-corp');

-- Grant a database role access to that tenant's pipelines
SELECT tide.relay_grant_tenant('orders-nats', 'acme-corp', 'acme_app_role');
```

Row-level security on relay config tables ensures each tenant can only see and modify their own pipelines. All Prometheus metrics carry a `tenant` label so you can build per-tenant dashboards without extra filtering.

## Schema Evolution

The `SchemaEvolutionGuard` computes SHA-256 fingerprints of message payload schemas per pipeline, detects `Initial` / `Additive` / `Breaking` changes, and enforces a configurable policy:

| Policy | Effect |
|--------|--------|
| `warn` | Log a warning and continue |
| `continue` | Silently accept the change |
| `pause` | Stop the pipeline until the schema is acknowledged |
| `dlq` | Route the message to the dead-letter queue |

## Examples

**Kubernetes / CloudNativePG:**

- [Sidecar Pattern](examples/cnpg/cluster.yaml) — Deploy pg_tide with a relay sidecar alongside PostgreSQL. Works with any CloudNativePG version.
- [Image Volume Extensions](examples/cnpg/IMAGE-VOLUMES.md) — Modern pattern for CloudNativePG 1.28+ using PostgreSQL 18 Image Volume Extensions. Decouples extension distribution from base images. See also [Dockerfile](examples/cnpg/Dockerfile.extension) and [example Cluster](examples/cnpg/cluster-image-volume.yaml).

## Architecture Decision Records

Key design decisions are documented in `docs/adr/`:

| ADR | Decision |
|-----|----------|
| [ADR-001](docs/adr/adr-001-single-table-outbox.md) | Single-table outbox |
| [ADR-002](docs/adr/adr-002-advisory-lock-coordination.md) | Advisory-lock HA coordination |
| [ADR-003](docs/adr/adr-003-wire-format-abstraction.md) | Pluggable `WireFormat` trait |
| [ADR-004](docs/adr/adr-004-jsonb-catalog-config.md) | JSONB catalog config |
| [ADR-005](docs/adr/adr-005-feature-gated-binary.md) | Feature-gated binary |
| [ADR-006](docs/adr/adr-006-outbox-table-partitioning.md) | Declarative outbox table partitioning |
| [ADR-007](docs/adr/adr-007-shared-partition-table-semantics.md) | Shared partition table semantics |

## Documentation

Full documentation is at **[trickle-labs.github.io/pg-tide](https://trickle-labs.github.io/pg-tide/)**.

- [Getting Started](https://trickle-labs.github.io/pg-tide/getting-started/first-pipeline.html)
- [SQL API Reference](https://trickle-labs.github.io/pg-tide/sql-reference/outbox-api.html)
- [Relay Configuration](https://trickle-labs.github.io/pg-tide/relay-guide/configuration.html)
- [Architecture](https://trickle-labs.github.io/pg-tide/evaluate/architecture.html)

## Upgrading

Each release ships an incremental SQL migration script. To upgrade an existing installation:

```sql
ALTER EXTENSION pg_tide UPDATE TO '0.34.0';
-- or apply directly: psql -f sql/pg_tide--0.33.0--0.34.0.sql
```

See [CHANGELOG.md](CHANGELOG.md) for per-release migration tables and breaking changes.

## Integration with pg_trickle

If you use [pg_trickle](https://github.com/trickle-labs/pg-trickle) ≥ v0.46.0,
install pg_tide first and then use `pgtrickle.attach_outbox()` to automatically
publish stream table changes to an outbox:

```sql
CREATE EXTENSION pg_tide;
SELECT pgtrickle.attach_outbox('my_stream_table', retention_hours := 48);
```

## License

Apache-2.0 — see [LICENSE](LICENSE).
