# pg_tide

[![CI](https://github.com/trickle-labs/pg-tide/actions/workflows/ci.yml/badge.svg)](https://github.com/trickle-labs/pg-tide/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**Transactional outbox, idempotent inbox, and relay pipelines for PostgreSQL 18.**

pg_tide gives your PostgreSQL database a built-in messaging backbone. Publish events atomically within your existing transactions — no dual-writes, no distributed transactions, no message broker required at the database layer.

When you're ready to fan out to Kafka, NATS, Redis Streams, or any analytics
platform, the `pg-tide` relay binary bridges the gap: at-least-once transport,
stable event identities, hot-reload pipeline config, and HA failover — all
configured with plain SQL.

## Features

- **Transactional Outbox** — publish messages inside any transaction; no 2PC, no dual-writes
- **Idempotent Inbox** — durable deduplication via unique event IDs; an
  effectively exactly-once outcome when application processing is transactional
- **Consumer Groups** — Kafka-style offset tracking with heartbeats and visibility leases
- **Relay Binary** — standalone `pg-tide` process; config lives in PostgreSQL and hot-reloads without restart
- **Auditable connector surface** — maturity, ownership, build profiles, and evidence are generated from `connectors.toml`
- **Pluggable Wire Formats** — native, Debezium, CloudEvents, Maxwell, Canal, and custom CDC JSON
- **Multi-Tenant** — row-level security, per-tenant Prometheus labels, per-outbox publisher ACLs, and per-tenant advisory-lock namespacing
- **Operational storage controls** — bounded participant-aware cleanup, optional ID-range partitions, and explicit maintenance-window conversion
- **Replay Workbench** — rewind consumer offsets, preview replays, and manage the DLQ from SQL or CLI
- **HA Ready** — advisory-lock coordination with automatic worker crash detection and restart; `--self-test` and `--expect-extension-version` flags for Kubernetes readiness probes
- **Observable** — OpenTelemetry spans, Prometheus metrics, Grafana dashboard, and pre-built alerting rules included
- **Envelope Encryption Foundation** — KMS-backed AES-256-GCM envelope encryption (AWS KMS, GCP Cloud KMS, HashiCorp Vault, local key file); `LocalKeyFile` provider fully implemented in v0.35.0; cloud providers ship in v1.0.0

## Quick Start

Requires **PostgreSQL 18**. The block below runs against a database with the
`pg_tide` extension installed:

<!-- quickstart:run -->
```sql
-- Install the extension (idempotent)
CREATE EXTENSION IF NOT EXISTS pg_tide;

-- Create an outbox. This inserts a catalog row — no per-outbox table DDL.
SELECT tide.outbox_create_if_not_exists('orders');

-- Publish an event atomically with your business write.
CREATE TABLE IF NOT EXISTS orders (id BIGINT PRIMARY KEY, total NUMERIC);
BEGIN;
  INSERT INTO orders (id, total) VALUES (42, 99.99);
  SELECT tide.outbox_publish(
    'orders',
    '{"order_id": 42, "total": 99.99}'::jsonb,
    '{"event_type": "order.created"}'::jsonb
  );
COMMIT;

-- Configure a native outbox → NATS JetStream pipeline.
-- The config persists in PostgreSQL and hot-reloads; the native relay polls the
-- shared tide.tide_outbox_messages table directly (ADR-011).
SELECT tide.relay_set_outbox_v2(
  jsonb_build_object(
    'name', 'orders-nats',
    'outbox', 'orders',
    'sink_type', 'nats',
    'config', jsonb_build_object(
      'url', 'nats://localhost:4222',
      'subject', 'orders.created'
    )
  )
);

-- Confirm the installed extension version.
SELECT extversion FROM pg_extension WHERE extname = 'pg_tide';
```

Start the relay:

```bash
pg-tide --postgres-url "postgres://user:pass@localhost:5432/mydb"
```

Messages flow from the outbox to NATS JetStream. Change the pipeline config in PostgreSQL — the relay picks it up without a restart.

**Delivery semantics.** The native relay polls the canonical shared table and tracks a durable offset per relay group, pipeline, and outbox. Delivery is **at-least-once**; each forward message carries a stable deduplication identity (`outbox_<name>:<id>:<row_index>`, published as `Nats-Msg-Id`) so JetStream can deduplicate a replay. Successful native delivery is proved by the per-pipeline offset — it does **not** mark a row globally `consumed_at`, and pg_tide makes no unqualified exactly-once claim. See the [documentation](https://trickle-labs.github.io/pg-tide/) for full details.

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

# Optional evaluation build with every compiling connector
docker pull ghcr.io/trickle-labs/pg-tide:latest-experimental
```

Release artifacts and Docker images are signed with [sigstore/cosign](https://github.com/sigstore/cosign-installer) using keyless OIDC signing.

## Project policies

- [Security policy](SECURITY.md)
- [Support policy](SUPPORT.md)
- [Governance](GOVERNANCE.md)
- [Stability guarantees](docs/src/stability-guarantees.md)
- [v1 scope](docs/src/v1-scope.md)

<!-- BEGIN GENERATED CONNECTORS -->
## Connector surface

The registry contains 37 selectable or documented surfaces: 5 supported, 4 preview, and 27 experimental.
Diagnostics are labeled separately and are not production integrations.

| Connector | Direction | Maturity | Core | Tested versions | Owner | Evidence |
|---|---|---|---:|---|---|---|
| [PostgreSQL native outbox](docs/src/support/connector-compatibility.md#postgresql-outbox) | source | supported | yes | PostgreSQL 18 | @grove | [outbox_source_test.rs](pg-tide-relay/tests/outbox_source_test.rs) |
| [pg_trickle outbox compatibility](docs/src/support/connector-compatibility.md#pg-trickle-compatibility) | source | preview | no | unknown | @grove | [outbox_source_test.rs](pg-tide-relay/tests/outbox_source_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [stdin, stdout, and file diagnostics](docs/src/support/connector-compatibility.md#diagnostics) | bidirectional | supported | yes | local process | @grove | [postgres_insert_microbenchmark.rs](pg-tide-relay/tests/postgres_insert_microbenchmark.rs) |
| [PostgreSQL inbox](docs/src/support/connector-compatibility.md#postgresql-inbox) | sink | supported | yes | PostgreSQL 18 | @grove | [pg_inbox_sink_test.rs](pg-tide-relay/tests/pg_inbox_sink_test.rs), [inbox_sink_test.rs](pg-tide-relay/tests/inbox_sink_test.rs) |
| [NATS JetStream outbound](docs/src/support/connector-compatibility.md#nats-jetstream-sink) | sink | supported | yes | NATS Server 2.11.0 with JetStream | @grove | [public_api_outbox_to_nats_e2e.rs](pg-tide-relay/tests/public_api_outbox_to_nats_e2e.rs) |
| [NATS inbound](docs/src/support/connector-compatibility.md#nats-source) | source | preview | no | NATS Server 2.11.0 with JetStream | @grove | [nats_test.rs](pg-tide-relay/tests/nats_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [HTTPS webhook outbound](docs/src/support/connector-compatibility.md#webhook-sink) | sink | supported | yes | HTTP/1.1 with TLS 1.3 | @grove | [webhook_test.rs](pg-tide-relay/tests/webhook_test.rs) |
| [Webhook inbound](docs/src/support/connector-compatibility.md#webhook-source) | source | preview | no | HTTP/1.1 in-process fixture | @grove | [webhook_sig_test.rs](pg-tide-relay/tests/webhook_sig_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [Apache Kafka outbound](docs/src/support/connector-compatibility.md#kafka-sink) | sink | supported | no | Apache Kafka 3.8.0 KRaft | @grove | [public_api_outbox_to_kafka_e2e.rs](pg-tide-relay/tests/public_api_outbox_to_kafka_e2e.rs) |
| [Apache Kafka inbound](docs/src/support/connector-compatibility.md#kafka-source) | source | preview | no | Apache Kafka 3.8.0 KRaft | @grove | [kafka_test.rs](pg-tide-relay/tests/kafka_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [Redis Streams](docs/src/support/connector-compatibility.md#redis) | bidirectional | experimental | no | unknown | @grove | [redis_test.rs](pg-tide-relay/tests/redis_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [Amazon SQS](docs/src/support/connector-compatibility.md#sqs) | bidirectional | experimental | no | unknown | @grove | [sqs_test.rs](pg-tide-relay/tests/sqs_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [RabbitMQ](docs/src/support/connector-compatibility.md#rabbitmq) | bidirectional | experimental | no | unknown | @grove | [rabbitmq_test.rs](pg-tide-relay/tests/rabbitmq_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [Google Pub/Sub](docs/src/support/connector-compatibility.md#pubsub) | bidirectional | experimental | no | unknown | @grove | [pubsub_test.rs](pg-tide-relay/tests/pubsub_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [Amazon Kinesis](docs/src/support/connector-compatibility.md#kinesis) | bidirectional | experimental | no | unknown | @grove | [kinesis_test.rs](pg-tide-relay/tests/kinesis_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [Azure Service Bus](docs/src/support/connector-compatibility.md#servicebus) | bidirectional | experimental | no | unknown | @grove | [servicebus_test.rs](pg-tide-relay/tests/servicebus_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [MQTT v5](docs/src/support/connector-compatibility.md#mqtt) | bidirectional | experimental | no | unknown | @grove | [mqtt_test.rs](pg-tide-relay/tests/mqtt_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [Azure Event Hubs](docs/src/support/connector-compatibility.md#eventhubs) | bidirectional | experimental | no | unknown | @grove | [eventhubs_test.rs](pg-tide-relay/tests/eventhubs_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [Elasticsearch](docs/src/support/connector-compatibility.md#elasticsearch) | sink | experimental | no | unknown | @grove | [elasticsearch_test.rs](pg-tide-relay/tests/elasticsearch_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [Object storage](docs/src/support/connector-compatibility.md#object-storage) | sink | experimental | no | unknown | @grove | [object_storage_test.rs](pg-tide-relay/tests/object_storage_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [Slack](docs/src/support/connector-compatibility.md#slack) | sink | experimental | no | unknown | @grove | [slack_test.rs](pg-tide-relay/tests/slack_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [Discord](docs/src/support/connector-compatibility.md#discord) | sink | experimental | no | unknown | @grove | [discord_test.rs](pg-tide-relay/tests/discord_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [PagerDuty](docs/src/support/connector-compatibility.md#pagerduty) | sink | experimental | no | unknown | @grove | [pagerduty_test.rs](pg-tide-relay/tests/pagerduty_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [Apache Arrow Flight](docs/src/support/connector-compatibility.md#arrow-flight) | sink | experimental | no | unknown | @grove | [arrow_flight_test.rs](pg-tide-relay/tests/arrow_flight_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [Singer](docs/src/support/connector-compatibility.md#singer) | bidirectional | experimental | no | unknown | @grove | [singer_test.rs](pg-tide-relay/tests/singer_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [Airbyte](docs/src/support/connector-compatibility.md#airbyte) | bidirectional | experimental | no | unknown | @grove | [airbyte_test.rs](pg-tide-relay/tests/airbyte_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [ClickHouse](docs/src/support/connector-compatibility.md#clickhouse) | sink | experimental | no | unknown | @grove | [clickhouse_test.rs](pg-tide-relay/tests/clickhouse_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [MongoDB](docs/src/support/connector-compatibility.md#mongodb) | sink | experimental | no | unknown | @grove | [mongodb_test.rs](pg-tide-relay/tests/mongodb_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [Google BigQuery](docs/src/support/connector-compatibility.md#bigquery) | sink | experimental | no | unknown | @grove | [bigquery_test.rs](pg-tide-relay/tests/bigquery_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [Snowflake](docs/src/support/connector-compatibility.md#snowflake) | sink | experimental | no | unknown | @grove | [snowflake_test.rs](pg-tide-relay/tests/snowflake_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [Delta Lake](docs/src/support/connector-compatibility.md#delta) | sink | experimental | no | unknown | @grove | [delta_test.rs](pg-tide-relay/tests/delta_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [Apache Iceberg](docs/src/support/connector-compatibility.md#iceberg) | sink | experimental | no | unknown | @grove | [iceberg_test.rs](pg-tide-relay/tests/iceberg_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [DuckLake](docs/src/support/connector-compatibility.md#ducklake) | sink | experimental | no | unknown | @grove | [ducklake_test.rs](pg-tide-relay/tests/ducklake_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [RockLake](docs/src/support/connector-compatibility.md#rocklake) | bidirectional | experimental | no | RockLake v0.27.14 | @grove | [rocklake_test.rs](pg-tide-relay/tests/rocklake_test.rs), [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [Fan-in compatibility surface](docs/src/support/connector-compatibility.md#fan-in) | source | experimental | no | disabled | @grove | [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [DuckLake reverse source (unavailable)](docs/src/support/connector-compatibility.md#ducklake-reverse) | unavailable | experimental | no | not registered | @grove | [metrics.rs](pg-tide-relay/src/metrics.rs) |
| [PostgreSQL WAL logical source (groundwork)](docs/src/support/connector-compatibility.md#wal-logical-source) | source | experimental | no | not registered | @grove | [metrics.rs](pg-tide-relay/src/metrics.rs) |
<!-- END GENERATED CONNECTORS -->

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
| `tide.outbox_create(name, retention_hours, inline_threshold)` | Create a named outbox; `inline_threshold` is a deprecated compatibility value |
| `tide.outbox_create_if_not_exists(name, retention_hours, inline_threshold)` | Idempotent create; returns `TRUE` when newly created |
| `tide.outbox_publish(name, payload, headers)` | Publish a message atomically |
| `tide.outbox_status(name)` | Status summary as JSONB |
| `tide.outbox_grant_publish(outbox, role)` | Grant publish permission to a role |
| `tide.outbox_sweep(name, batch_size, dry_run)` | Bounded, participant-aware cleanup; default batch 1,000, maximum 10,000 |
| `tide.outbox_truncate_delivered(name)` | Deprecated one-batch compatibility wrapper over `outbox_sweep()` |
| `tide.admin_convert_outbox_storage(span, premake, confirm)` | Blocking global heap-to-ID-range conversion during a maintenance window |

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
| `tide.relay_set_outbox_v2(config jsonb)` | Configure forward pipeline (outbox → sink); keys: `name`, `outbox`, `sink_type`, `config`, optional `source_mode` (`native` default / `pg_trickle`) |
| `tide.relay_set_inbox_v2(config jsonb)` | Configure reverse pipeline (source → inbox) |
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

Views: `tide.outbox_retention_status` · `tide.relay_pipeline_lag` ·
`tide.outbox_cleanup_state` · `tide.inbox_fleet_summary`

## Operational evidence

The v0.43 operational contract is in
[`benchmarks/operational/`](benchmarks/operational/README.md). Criterion and
direct PostgreSQL inserts are microbenchmarks only. Capacity guidance must
come from a named profile using the public SQL API, packaged PostgreSQL 18
extension, real relay process, and NATS JetStream; the repository does not
claim universal throughput or latency.

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
| [ADR-008](docs/adr/adr-008-claim-check-native-pathway.md) | Native claim-check pathway |
| [ADR-009](docs/adr/adr-009-wal-logical-replication-source.md) | WAL logical-replication source |
| [ADR-010](docs/adr/adr-010-envelope-encryption-kms.md) | Envelope encryption with KMS |
| [ADR-011](docs/adr/adr-011-canonical-outbox-storage-and-relay-polling.md) | Canonical outbox storage and polling |
| [ADR-012](docs/adr/adr-012-relay-delivery-acknowledgment-and-offset-state-machine.md) | Relay acknowledgment and offset state machine |
| [ADR-013](docs/adr/adr-013-retention-partitioning-and-postgresql-cost.md) | Retention, ID-range partitioning, and PostgreSQL cost |

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
