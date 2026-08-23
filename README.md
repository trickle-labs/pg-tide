# pg_tide

[![CI](https://github.com/trickle-labs/pg-tide/actions/workflows/ci.yml/badge.svg)](https://github.com/trickle-labs/pg-tide/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**Transactional outbox, idempotent inbox, and relay pipelines for PostgreSQL 18.**

pg_tide gives your PostgreSQL database a built-in messaging backbone. Publish events atomically within your existing transactions — no dual-writes, no distributed transactions, no message broker required at the database layer.

When you're ready to deliver to PostgreSQL, NATS JetStream, Apache Kafka, or an
HTTPS endpoint, the `pg-tide` relay binary bridges the gap: at-least-once transport,
stable event identities, hot-reload pipeline config, and HA failover — all
configured with plain SQL.

## Features

- **Transactional Outbox** — publish messages inside any transaction; no 2PC, no dual-writes
- **Idempotent Inbox** — durable deduplication via unique event IDs; an
  effectively exactly-once outcome when application processing is transactional
- **Consumer Groups** — Kafka-style offset tracking with heartbeats and visibility leases
- **Relay Binary** — standalone `pg-tide` process; config lives in PostgreSQL and hot-reloads without restart
- **Auditable connector surface** — maturity, ownership, build profiles, and evidence are generated from `connectors.toml`
- **Stable Wire Formats** — native pg_tide JSON and CloudEvents
- **Multi-Tenant** — row-level security, per-tenant Prometheus labels, per-outbox publisher ACLs, and per-tenant advisory-lock namespacing
- **Operational storage controls** — bounded participant-aware cleanup, optional ID-range partitions, and explicit maintenance-window conversion
- **Replay Workbench** — rewind consumer offsets, preview replays, and manage the DLQ from SQL or CLI
- **HA Ready** — advisory-lock coordination with automatic worker crash detection and restart; `--self-test` includes the lifecycle compatibility gate for Kubernetes readiness probes
- **Observable** — Prometheus metrics, health checks, structured logs, Grafana dashboard, and pre-built alerting rules included
- **Local key-file encryption** — supported local envelope encryption; unsupported provider or payload variants fail closed

## Quick Start

Requires **PostgreSQL 18**. The block below runs against a database with the
`pg_tide` extension installed:

<!-- pg-tide-example: tested id=readme-quickstart-sql test=quickstart-sql-pr -->
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
      'url', 'tls://nats.example:4222',
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

The registry contains 6 selectable or documented surfaces: 5 supported, 0 preview, and 0 experimental.
Diagnostics are labeled separately and are not production integrations.

| Connector | Direction | Maturity | Core | Tested versions | Owner | Evidence |
|---|---|---|---:|---|---|---|
| [PostgreSQL native outbox](docs/src/support/connector-compatibility.md#postgresql-outbox) | source | supported | yes | PostgreSQL 18 | @grove | [outbox_source_test.rs](pg-tide-relay/tests/outbox_source_test.rs) |
| [stdout and file diagnostics](docs/src/support/connector-compatibility.md#diagnostics) | sink | supported | yes | local process | @grove | [postgres_insert_microbenchmark.rs](pg-tide-relay/tests/postgres_insert_microbenchmark.rs) |
| [PostgreSQL inbox](docs/src/support/connector-compatibility.md#postgresql-inbox) | sink | supported | yes | PostgreSQL 18 | @grove | [pg_inbox_sink_test.rs](pg-tide-relay/tests/pg_inbox_sink_test.rs), [inbox_sink_test.rs](pg-tide-relay/tests/inbox_sink_test.rs), [public_api_outbox_to_pg_inbox_e2e.rs](pg-tide-relay/tests/public_api_outbox_to_pg_inbox_e2e.rs) |
| [NATS JetStream outbound](docs/src/support/connector-compatibility.md#nats-jetstream-sink) | sink | supported | yes | NATS Server 2.11.0 with JetStream | @grove | [public_api_outbox_to_nats_e2e.rs](pg-tide-relay/tests/public_api_outbox_to_nats_e2e.rs) |
| [HTTPS webhook outbound](docs/src/support/connector-compatibility.md#webhook-sink) | sink | supported | yes | HTTP/1.1 with TLS 1.3 | @grove | [webhook_test.rs](pg-tide-relay/tests/webhook_test.rs), [public_api_outbox_to_webhook_e2e.rs](pg-tide-relay/tests/public_api_outbox_to_webhook_e2e.rs) |
| [Apache Kafka outbound](docs/src/support/connector-compatibility.md#kafka-sink) | sink | supported | no | Apache Kafka 3.8.0 KRaft | @grove | [public_api_outbox_to_kafka_e2e.rs](pg-tide-relay/tests/public_api_outbox_to_kafka_e2e.rs) |
<!-- END GENERATED CONNECTORS -->

## Wire Formats

All pipelines support a wire format selected per-pipeline in the catalog:

| Format | Direction | Description |
|--------|-----------|-------------|
| `native` | outbound | Default pg_tide JSON envelope |
| `cloudevents` | outbound | CloudEvents v1.0 JSON |

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

# Verify the installed extension is in the relay's supported lifecycle window
pg-tide --self-test --postgres-url "postgres://..."

# Replay and DLQ recovery
pg-tide replay preview --outbox orders --from-id 100 --to-id 200
pg-tide replay dlq-requeue --pipeline orders-nats --dedup-key orders:42:0
```

## Security

- **Fail-closed TLS** — `sslmode=require` returns an error rather than silently downgrading to plaintext
- **Publisher ACLs** — `tide.outbox_grant_publish(outbox, role)` restricts which roles can publish to each outbox
- **SSRF protection** — webhook sinks reject loopback, link-local, private ranges, and plain HTTP by default
- **Secret redaction** — `${env:…}` and `${file:…}` references are replaced with `[REDACTED]` in logs
- **Supply-chain audit** — `cargo-deny` checks every dependency for RUSTSEC advisories and license compliance in CI
- **Envelope encryption foundation** — the extension keeps the versioned encryption catalog contract; supported relay paths use native payloads and fail closed on unsupported encrypted payload handling

## Observability

- **Structured observability** — Prometheus metrics, health checks, structured logs, DLQ visibility, and pipeline lag tracking
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
| `tide.relay_set_outbox_v2(config jsonb)` | Configure a native forward pipeline; keys: `name`, `outbox`, `sink_type`, and `config` |
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

## License

Apache-2.0 — see [LICENSE](LICENSE).
