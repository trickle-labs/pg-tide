# Changelog

What's new in pg_tide — written for everyone, not just developers.

For future plans and upcoming features, see [ROADMAP.md](ROADMAP.md).

## Table of Contents

<!-- TOC start -->
- [0.16.0 — Developer Experience & Observability](#0160--2026-05-11--developer-experience--observability)
- [0.15.0 — TLS Enforcement, Resilience & Outbox Sweep](#0150--2026-05-10--tls-enforcement-resilience--outbox-sweep)
- [0.14.0 — Replay Workbench, CloudEvents, Tenant Scale & Managed Backfill](#0140--2026-05-09--replay-workbench-cloudevents-tenant-scale--managed-backfill)
- [0.13.0 — Security Hardening, Reliability & Performance](#0130--2026-05-08--security-hardening-reliability--performance)
- [0.12.0 — Contract Correctness & Operational Tooling](#0120--2026-05-05--contract-correctness--operational-tooling)
- [0.11.0 — Pluggable Wire Formats: Debezium, Maxwell, Canal, Custom CDC JSON](#0110--2026-05-05--pluggable-wire-formats-debezium-maxwell-canal-custom-cdc-json)
- [0.10.0 — Analytics Sinks: ClickHouse, MongoDB, Snowflake, BigQuery, Iceberg, Delta Lake, DuckLake](#0100--2026-05-07--analytics-sinks-clickhouse-mongodb-snowflake-bigquery-iceberg-delta-lake-ducklake)
- [0.9.0 — Connector Ecosystem Foundation (Singer, Airbyte, Fivetran)](#090--2026-05-06--connector-ecosystem-foundation-singer-airbyte-fivetran)
- [0.8.0 — Notification Sinks & Apache Arrow Flight](#080--2026-05-05--notification-sinks--apache-arrow-flight)
- [0.7.0 — Production-Grade Relay Operations](#070--2026-05-06--production-grade-relay-operations)
- [0.6.0 — MQTT v5, Azure Event Hubs & Object Storage (JSONL + Parquet)](#060--2026-05-05--mqtt-v5-azure-event-hubs--object-storage-jsonl--parquet)
- [0.5.0 — Cloud Provider Parity: Pub/Sub, Kinesis, Azure Service Bus & Elasticsearch](#050--2026-05-04--cloud-provider-parity-pubsub-kinesis-azure-service-bus--elasticsearch)
- [0.4.0 — Relay Completion: Tier 2 Sinks, Full Reverse Mode & Integration Tests](#040--2026-05-04--relay-completion-tier-2-sinks-full-reverse-mode--integration-tests)
- [0.3.0 — Relay Run Loop, Secret Interpolation & pg-tide Branding](#030--2026-05-04--relay-run-loop-secret-interpolation--pg-tide-branding)
- [0.2.0 — Post-0.1.0 Hardening & Observability](#020--post-010-hardening--observability)
- [0.1.0 — Initial Release](#010--initial-release)
<!-- TOC end -->

---

## [0.16.0] — 2026-05-11 — Developer Experience & Observability

v0.16.0 focuses on developer ergonomics, deeper observability, idempotent SQL
helpers, property-based testing, and hardened CI. There are no breaking API
changes — all additions are backward-compatible.

### SQL API

- **`tide.outbox_create_if_not_exists(name, retention_hours, max_size)`** —
  New idempotent variant of `outbox_create()`. Returns `TRUE` when the outbox
  is newly created and `FALSE` when it already existed. Lets callers write
  setup scripts without wrapping every call in `DO $$ BEGIN … EXCEPTION WHEN
  duplicate_table THEN … END $$` blocks.
- **`tide.relay_set_inbox_v2(config JSONB)`** — Single-JSONB-parameter variant
  of `relay_set_inbox()` for easier scripting and tooling. Accepts the same
  fields: `name`, `inbox`, `source`, `config`, `batch_size`, `enabled`,
  `max_retries`, `idempotent`.

### Coordinator Metrics

Three new Prometheus metrics expose coordination internals:

| Metric | Type | Labels | Description |
|---|---|---|---|
| `pg_tide_relay_owned_pipelines` | Gauge | `relay_group` | Number of pipelines currently owned by this relay instance |
| `pg_tide_relay_reconcile_duration_seconds` | Histogram | `relay_group` | Time taken for each reconcile cycle |
| `pg_tide_relay_pipeline_errors_total` | Counter | `pipeline`, `error_class` | Total pipeline errors, split by `transient` / `permanent` |

All existing metric names are now referenced through Rust constants (e.g.
`METRIC_MESSAGES_PUBLISHED`) to make dashboard validation in CI straightforward.

### OTel Span Coverage Expansion

New spans are emitted for every major step in the message-processing pipeline:

| Span name | What it covers |
|---|---|
| `relay.transform.evaluate` | JMESPath filter / payload projection |
| `relay.routing.apply` | Content-based routing rule evaluation |
| `relay.dlq.insert` | DLQ writes (both circuit-breaker and max-retries paths) |
| `relay.schema_evolution.check` | Per-topic fingerprint comparison and policy enforcement |
| `relay.backoff.sleep` | Exponential backoff sleep boundaries |

The `relay.dlq.insert` span carries a `reason` attribute
(`circuit_breaker_open` or `max_retries_exceeded`) so traces distinguish
between the two DLQ paths without extra filtering.

### Schema Evolution Wired Into Worker Loop

The `SchemaEvolutionGuard` (introduced in v0.13.0) is now instantiated and
invoked in every pipeline worker. After JMESPath transforms are applied the
worker computes a fingerprint of the first message's payload columns and
calls `observe()`. The configured `on_schema_change` policy (`warn`,
`continue`, `pause`, `dlq`) is enforced inline.

### `pg-tide status` Command

New `pg-tide status [--postgres-url URL]` subcommand prints a formatted table
of all configured pipelines with columns:

```
PIPELINE | DIRECTION | ENABLED | LAST_OFFSET | CONSUMER_LAG
```

The command queries `tide.relay_outbox_config` and `tide.relay_inbox_config`
joined with `tide.relay_consumer_offsets` to produce an at-a-glance health
snapshot without connecting to a running relay.

### Property-Based Testing

Wire-format round-trip correctness is now verified with property-based tests
(`proptest`) in `tests/wire_format_proptest.rs`:

- `NativePgTideFormat` — payload and `dedup_key` preserved through
  encode → decode cycle.
- `DebeziumFormat` — insert and update round-trips; delete produces exactly
  two entries (data row + tombstone).
- `CloudEventsFormat` — insert round-trip.
- `from_config` factory — does not panic for any of the three built-in format
  names.

### CI Improvements

- **Parallel integration tests** — `test-integration-core` and
  `test-integration-relay` now run as separate GitHub Actions jobs using
  `testcontainers`, cutting wall-clock CI time roughly in half.
- **Link checker** — `link-check` job uses
  [lychee-action](https://github.com/lycheeverse/lychee-action) to verify all
  documentation and README hyperlinks on every push and pull request.
- **Dashboard metric-name validation** — `dashboard-check` job validates that
  every metric name referenced in `pg-tide/dashboards/relay-health.json` is
  defined as a constant in `pg-tide-relay/src/metrics.rs`, preventing silent
  dashboard drift.

### Architecture Decision Records

Five ADRs documenting the key design choices behind pg-tide are now published
in `docs/adr/`:

| ADR | Decision |
|---|---|
| [ADR-001](docs/adr/adr-001-single-table-outbox.md) | Single-table outbox |
| [ADR-002](docs/adr/adr-002-advisory-lock-coordination.md) | Advisory-lock coordination |
| [ADR-003](docs/adr/adr-003-wire-format-abstraction.md) | `WireFormat` trait abstraction |
| [ADR-004](docs/adr/adr-004-jsonb-catalog-config.md) | JSONB catalog config |
| [ADR-005](docs/adr/adr-005-feature-gated-binary.md) | Feature-gated binary |

### Release Artifacts

- **Docker `:latest-full`** — New `docker-full` release job builds a second
  Docker image with `--all-features` enabled, making every optional connector
  available without a custom build.
- **Cosign signing** — Both the standard and full Docker images, plus binary
  release artifacts (`.bundle` files), are signed with
  [sigstore/cosign](https://github.com/sigstore/cosign-installer) using keyless
  OIDC signing. Verification: `cosign verify --certificate-identity-regexp
  '.*' --certificate-oidc-issuer https://token.actions.githubusercontent.com
  ghcr.io/…/pg-tide:…`.

### Helm Chart

- Helm chart `version` and `appVersion` bumped to `0.16.0`.

---

## [0.15.0] — 2026-05-10 — TLS Enforcement, Resilience & Outbox Sweep

v0.15.0 hardens the relay binary with fail-closed TLS enforcement, secret
redaction in logs, transient vs. permanent error classification, worker crash
detection, exponential backoff, deadpool connection pooling, and a new
`pg-tide sweep` command for outbox retention. Schema registry passthrough mode
and raw outbox payload mode complete the feature set.

### Relay Security

- **Fail-closed TLS** — When `sslmode=require` is configured, the relay now
  returns an error immediately instead of silently falling back to plaintext.
  New `RelayError::TlsRequired` variant prevents accidental insecure connections.
- **Secret redaction in logs** — Pipeline config values containing
  `${env:…}` or `${file:…}` secret references are now replaced with
  `[REDACTED]` before the config is emitted to logs. Raw secret values no
  longer appear in log output.

### Relay Resilience

- **Transient vs. permanent error classification** — `RelayError::is_transient()`
  predicate distinguishes recoverable network errors from fatal config/auth
  failures. Permanent errors now stop the worker immediately instead of
  retrying indefinitely.
- **Worker crash detection** — The coordinator stores `JoinHandle`s for each
  worker and checks `handle.is_finished()` in every reconcile tick. Crashed or
  panicked workers are automatically restarted.
- **Exponential backoff with jitter** — Poll-loop errors now back off
  exponentially from the configured `poll_interval_ms` up to 60 seconds,
  reducing thundering-herd pressure during database outages.
- **Connection pooling** — Coordinator metadata operations (pipeline discovery,
  advisory lock management) now use a `deadpool-postgres` pool. Workers retain
  dedicated connections for throughput isolation.

### Relay Configuration

- **`max_owned_pipelines`** (`--max-pipelines` / `PG_TIDE_MAX_PIPELINES`) —
  Caps how many pipelines a single relay instance will own. Default: 50.
- **`max_connections`** (`--max-connections` / `PG_TIDE_MAX_CONNECTIONS`) —
  Sets the coordinator pool size. Default: 52.

### Outbox & Payload

- **`pg-tide sweep`** — New CLI command that calls
  `tide.outbox_truncate_delivered()` for each configured outbox (or a named
  one with `--outbox`). Outputs a per-outbox row count and a summary total.
- **Raw payload mode** — Sources can opt into `payload_mode = "raw"` to pass
  outbox payloads through without the v:1 envelope parse, enabling migration
  from legacy message schemas.
- **Claim-check guard** — The outbox source now checks for
  `tide.outbox_delta_rows_*` table existence before attempting claim-check
  fetches and returns a descriptive error if the table is missing.
- **`pg-tide doctor`** — Extended to check for
  `tide.outbox_truncate_delivered()` function presence (v0.15.0+ indicator).

### Schema Registry

- **Passthrough mode** — `schema_registry.mode = "passthrough"` bypasses
  schema validation and wraps messages in the Confluent wire format header
  with a sentinel schema ID, allowing raw pass-through for compatibility
  with legacy consumers.

### SQL

- **`tide.outbox_truncate_delivered(outbox_name TEXT) → BIGINT`** — Deletes
  consumed outbox messages older than the outbox's `retention_hours` window
  and returns the number of rows deleted.

---

## [0.14.0] — 2026-05-09 — Replay Workbench, CloudEvents, Tenant Scale & Managed Backfill

v0.14.0 adds four major capability areas: a SQL + CLI replay workbench for
incident recovery, CloudEvents v1.0 wire format with AsyncAPI export, a
tenant-aware relay layer with RLS policies and per-tenant Prometheus labels, and
cataloged backfill jobs with pause/resume and progress tracking.

### Replay Workbench

- **`tide.consumer_offset_rewind(pipeline, lsn)`** — Admin function to
  intentionally roll back a consumer's committed offset, guarded by a
  monotonicity check on normal progress.
- **`tide.relay_replay_preview(pipeline, from_lsn, to_lsn)`** — Returns the
  messages that would be replayed without committing any offsets (dry-run).
- **`tide.dlq_resolve(pipeline, event_id)`** / **`tide.dlq_requeue(pipeline,
  event_id)`** — Mark DLQ entries as resolved or reschedule them for
  reprocessing.
- **`tide.inbox_fleet_summary` view** — Cross-inbox pending-count overview.
- **`pg-tide replay preview|dry-run|dlq-resolve|dlq-requeue`** — CLI subcommands
  backed by the SQL functions above, with `--from-lsn` / `--to-lsn` arguments.
- **`inbox_status(NULL)`** — Returns a fleet-wide JSON summary across all
  configured inboxes.

### CloudEvents & AsyncAPI

- **CloudEvents v1.0 wire format** — New `cloudevents` wire format option wraps
  outbox messages in a standard CloudEvents 1.0 JSON envelope (`specversion`,
  `type`, `source`, `id`, `time`, `datacontenttype`, `data`, `ce-op`).
  Decode validates `specversion: "1.0"` and maps fields back to `InboxRow`.
- **`pg-tide asyncapi export`** — CLI command that generates an AsyncAPI 3.0
  document from relay catalog metadata and observed message schemas.
- **`wire_format TEXT DEFAULT 'native'`** — Added to `relay_outbox_config` and
  `relay_inbox_config` so wire format is catalog-persisted per pipeline.

### Tenant-Aware Relay Groups

- **`tenant_name` column** — Added to `relay_outbox_config`, `relay_inbox_config`,
  and `relay_consumer_offsets` (default `'default'`).
- **Row-level security** — `tide.relay_tenant_grants` table and RLS policies on
  relay config tables ensure each tenant can only see and modify their own
  pipelines.
- **`tide.relay_set_tenant(pipeline, tenant)`** — Sets the tenant for a pipeline.
- **`tide.relay_grant_tenant(pipeline, tenant, role)`** / **`tide.relay_revoke_tenant(pipeline, tenant, role)`** — Admin ACL API.
- **Per-tenant Prometheus labels** — All relay metrics now carry a `tenant` label
  dimension for per-tenant observability dashboards.

### Managed Backfill Jobs

- **`tide.backfill_jobs` table** — Cataloged backfill jobs with chunk size,
  progress tracking (rows processed, estimated completion), and status.
- **`tide.backfill_create(outbox, sink_pipeline, chunk_size)`** — Creates a new
  backfill job and returns the job ID.
- **`tide.backfill_pause(job_id)`** / **`tide.backfill_resume(job_id)`** — Pause
  and resume a running backfill job.
- **`tide.backfill_status(job_id)`** — Returns JSON status for a single job, or
  fleet summary when called with `NULL`.

### Migration

Upgrade from v0.13.0:

```sql
ALTER EXTENSION pg_tide UPDATE TO '0.14.0';
-- or run: psql -f sql/pg_tide--0.13.0--0.14.0.sql
```

New objects added by the migration:

| Object | Type | Description |
|--------|------|-------------|
| `tide.relay_dlq` | TABLE | DLQ entries for failed messages |
| `tide.backfill_jobs` | TABLE | Cataloged backfill job state |
| `tide.relay_tenant_grants` | TABLE | Per-tenant ACL grants |
| `tide.inbox_fleet_summary` | VIEW | Cross-inbox pending-count overview |
| `tide.consumer_offset_rewind(text,pg_lsn)` | FUNCTION | Admin offset rollback |
| `tide.relay_replay_preview(text,pg_lsn,pg_lsn)` | FUNCTION | Dry-run replay preview |
| `tide.dlq_resolve(text,text)` | FUNCTION | Resolve DLQ entry |
| `tide.dlq_requeue(text,text)` | FUNCTION | Requeue DLQ entry |
| `tide.relay_set_tenant(text,text)` | FUNCTION | Set pipeline tenant |
| `tide.relay_grant_tenant(text,text,text)` | FUNCTION | Grant tenant access |
| `tide.relay_revoke_tenant(text,text,text)` | FUNCTION | Revoke tenant access |
| `tide.backfill_create(text,text,int)` | FUNCTION | Create backfill job |
| `tide.backfill_pause(bigint)` | FUNCTION | Pause backfill job |
| `tide.backfill_resume(bigint)` | FUNCTION | Resume backfill job |
| `tide.backfill_status(bigint)` | FUNCTION | Job status JSON |
| `tenant_name TEXT` | COLUMN | Added to relay config + offsets tables |
| `wire_format TEXT` | COLUMN | Added to relay config tables |

---

## [0.13.0] — 2026-05-08 — Security Hardening, Reliability & Performance

v0.13.0 is a focused security, reliability, and performance release. It
introduces publisher ACLs for per-outbox publish authorization, SSRF guards for
webhook sinks, TLS/mTLS connection support, schema evolution guardrails, batch
inserts for the pg-inbox sink, connection pooling limits, improved DLQ
semantics, OTel spans, and supply-chain auditing via cargo-deny.

### Security

- **Outbox publisher ACLs** — New `tide.outbox_publishers` table and
  `tide.outbox_grant_publish(outbox, role)` / `tide.outbox_revoke_publish(outbox,
  role)` functions enforce per-outbox publish authorization. Roles must be
  granted explicitly; superusers bypass the check. All ACL functions use
  `SECURITY DEFINER SET search_path` to prevent search-path injection attacks.
- **SSRF guard for webhook sinks** — `validate_webhook_url()` rejects
  loopback (127.x, ::1, localhost), link-local (169.254.x, fe80::), private
  ranges (10.x, 172.16–31.x, 192.168.x), and plain HTTP by default. Disable
  via `ssrf_protection: false` for development. Hardened on every publish.
- **TLS/mTLS support** — New `pg_tls` module parses `sslmode` from connection
  URLs (including `verify-full`, `verify-ca` → `Require`) and provides
  `with_ssl_mode()` for programmatic override.
- **Supply-chain audit** — `deny.toml` and a `cargo-deny` CI step check all
  dependencies for RUSTSEC advisories, license compliance (Apache-2.0, MIT,
  BSD, etc.), and duplicate crate versions.

### Reliability

- **Schema evolution guardrails** — `SchemaEvolutionGuard` computes SHA-256
  fingerprints of message payload schemas, detects `Initial` / `Additive` /
  `Breaking` changes, and persists results in the new
  `tide.relay_schema_fingerprints` table. Policy (`warn`, `continue`, `pause`,
  `dlq`) is configurable per-pipeline.
- **DLQ partial failure semantics** — `dlq::insert_batch()` now reports per-entry
  failures instead of aborting on the first error. The source is acknowledged
  only after a durable DLQ write; circuit-breaker and max-retries paths both
  follow this guarantee.
- **Connection pooling limit** — Coordinators enforce a configurable
  `max_owned_pipelines` limit (default 50) to cap PostgreSQL connections. The
  limit is reflected in the new `tide.relay_limits` catalog table.

### Performance

- **Batch pg-inbox inserts** — The `InboxSink` now uses a single
  `INSERT ... SELECT * FROM UNNEST(...)` to deliver an entire batch in one
  round trip instead of per-row `INSERT` calls. Deduplication via
  `ON CONFLICT (event_id) DO NOTHING` is preserved.

### Observability

- **OTel spans** — `relay.source.poll`, `relay.sink.publish`, and
  `relay.source.acknowledge` spans are emitted per poll cycle. The spans use
  `tracing::info_span!` with `Instrument` so they bridge to any OTLP subscriber
  (Jaeger, Tempo, Honeycomb, Datadog).
- **New Prometheus metrics**:
  - `pg_tide_relay_dlq_entries_written_total` — DLQ entries written per pipeline
    and direction.
  - `pg_tide_relay_messages_consumed_total` — messages polled from source (was
    previously missing the consumed counter).
  - `pg_tide_relay_pipeline_healthy` gauge — set to 1 after each successful
    publish, 0 on error or exit.
  - `pg_tide_relay_delivery_latency_seconds` histogram — end-to-end latency
    from poll to ack.

### Migration

Upgrade from v0.12.0:

```sql
ALTER EXTENSION pg_tide UPDATE TO '0.13.0';
-- or run: psql -f sql/pg_tide--0.12.0--0.13.0.sql
```

New objects added by the migration:

| Object | Type | Description |
|--------|------|-------------|
| `tide.outbox_publishers` | TABLE | Per-outbox publisher ACL |
| `tide.outbox_grant_publish(text,text)` | FUNCTION | Grant publish to role |
| `tide.outbox_revoke_publish(text,text)` | FUNCTION | Revoke publish from role |
| `tide.relay_schema_fingerprints` | TABLE | Per-pipeline schema evolution state |
| `tide.relay_limits` | TABLE | Per-relay-group connection limits |
| `uq_relay_dlq_pipeline_dedup` | INDEX | Partial unique index on relay_dlq |

---

## [0.12.0] — 2026-05-05 — Contract Correctness & Operational Tooling

v0.12.0 fixes the critical seam breaks identified in the overall assessment
between the SQL extension API, the relay catalog format, and runtime
configuration. It also adds operational CLI tools, tightens identifier
validation, and aligns documentation with actual behavior.

### SQL ↔ Relay Contract Fixes

- **`relay_set_outbox()` / `relay_set_inbox()`** now write the runtime-expected
  JSON shape: `source_type`, `source.outbox`, `sink_type`, `sink.*`,
  `batch_size`. Previously the stored JSON used `outbox`, `sink`, and `params`
  which the relay coordinator could not parse.
- **`relay_consumer_offsets`** schema migrated: `last_offset TEXT` replaced by
  `last_change_id BIGINT NOT NULL DEFAULT 0` and `worker_id TEXT`. The relay
  source already expected these typed columns; the SQL schema now matches.
- **pg-inbox sink** fixed: inserts `(event_id, source, payload, headers)`
  matching extension-created inbox tables. Previously inserted `event_type` and
  `received_at` columns which did not exist.
- **`outbox_publish()`** now enforces the `enabled` flag — returns an error for
  disabled outboxes. Previously, `outbox_disable()` set the flag but publish
  ignored it.
- **`relay_list_configs()`** now returns full `{name, direction, enabled,
  config}` objects and propagates SPI errors instead of silently defaulting to
  empty data.

### Identifier Validation

- New `validation::validate_identifier()` function enforces PostgreSQL
  safe-identifier rules (non-empty, ≤63 bytes, `[A-Za-z_][A-Za-z0-9_]*`) on
  all dynamic SQL identifier paths: outbox names, inbox names, schema names,
  relay pipeline names. Prevents SQL injection via dynamic DDL.

### CLI & Observability Tooling

- **`pg-tide doctor --postgres-url ...`** — new subcommand that validates
  PostgreSQL connectivity, checks the `tide` schema and required tables exist,
  verifies the v0.12.0 schema migration (typed offset column), and reports
  configured pipeline counts. Exits 0 on success, 1 on any issue.
- **`pg-tide validate-config --pipeline NAME`** — new subcommand that loads a
  named pipeline from the catalog, resolves secrets, and instantiates source +
  sink factories without processing any messages. Reports whether both sides can
  be constructed successfully.

### Grafana Dashboard

- `pg-tide/dashboards/relay-health.json` regenerated with correct metric names
  (`pg_tide_relay_*` prefix matching actual Prometheus metric registration).
  Previously used `pgtide_relay_*` names that returned no data.

### Infrastructure & Packaging

- **Helm chart** `PG_TIDE_RELAY_POSTGRES_URL` → `PG_TIDE_POSTGRES_URL` (now
  matches what the CLI actually reads). Chart `version` and `appVersion`
  bumped to `0.12.0`.
- **Release workflow** updated to build with `--all-features` so official
  release artifacts include all documented sink/source backends.
- **`sink_max_inflight`** configuration key now wired into a real `tokio::sync::Semaphore`
  around each pipeline's publish operations. Previously documented but not
  enforced.

### Test Coverage

- `tests/migration_test.rs` — sequential SQL migration upgrade test: installs
  v0.1.0 schema, applies all 11 upgrade scripts in order, and asserts catalog
  state at v0.12.0.
- `tests/sql_api_test.rs` — SQL API test harness: verifies relay config JSON
  shape, inbox table column shape, and end-to-end catalog round-trips using
  testcontainers PostgreSQL.

### Documentation

- `docs/src/reference/version-compatibility.md` corrected:
  - PostgreSQL 18+ only (was incorrectly claiming 14–18 support).
  - Feature availability table corrected to match actual changelog.
  - Feature gates table updated with actual per-backend gate names (replaces
    fictional `cloud` and `analytics` aggregates).
  - Upgrade path updated to include v0.12.0 migration script.
- `README.md`: delivery semantics clarified from "exactly-once" to "transactional
  publish + idempotent delivery primitives (at-least-once relay with dedup)".
  Getting Started link updated to correct path.

---

## [0.11.0] — 2026-05-05 — Pluggable Wire Formats: Debezium, Maxwell, Canal, Custom CDC JSON

v0.11.0 introduces a symmetric `WireFormat` trait that decouples the relay's
transport layer (Kafka, NATS, Redis, etc.) from the envelope format, enabling
bidirectional Debezium support and other CDC wire formats without touching
transport code.

### WireFormat Trait (`wire_format` module)

- New `WireFormat` trait with symmetric `decode` (reverse path) and `encode`
  (forward path) methods, plus optional `observe_schema` and `register_schema`
  hooks for schema evolution.
- `RawMessage` type wrapping raw transport bytes (key, value, topic, headers).
- `InboxRow` type for decoded messages ready for inbox insertion, carrying
  `op`, `payload`, `old_payload`, `commit_ts`, and `source_position`.
- `OutboxRow` type for outbox rows ready for encoding, carrying `op`,
  `new_row`, `old_row`, `stream_table`, and pg LSN.
- `EncodedBatch` type to handle multi-message outputs (e.g. DELETE + tombstone).
- `WireError` enum with `Decode`, `Encode`, `SchemaIncompatible`,
  `SchemaRegistry`, and `UnsupportedOperation` variants.
- `wire_format::from_config(config)` factory: reads the `wire_format` and
  `wire_config` fields from a pipeline config JSON and returns the appropriate
  boxed `WireFormat` implementation.

### Native pg_tide Envelope (`wire_format = "native"`)

- `NativePgTideFormat` wraps the existing relay message behaviour behind the
  `WireFormat` trait — no behaviour change for pipelines without `wire_format`.
- Refactor is transparent: all existing forward and reverse pipelines continue
  to work exactly as before.

### Debezium Bidirectional Support (`wire_format = "debezium"`)

- **Decode (reverse path):** Consumes Debezium JSON envelopes from any transport
  (Kafka, NATS, etc.) and maps them to `InboxRow`.
  - Supports all four Debezium ops: `c` (insert), `u` (update), `d` (delete),
    `r` (snapshot read, configurable as insert or upsert).
  - Extracts `payload.source.ts_ms` as `commit_ts` and `lsn`/`pos`/`change_lsn`
    as `source_position`.
  - Tombstone handling: `"delete"` (default) or `"drop"`.
  - Snapshot op treatment: `"insert"` (default) or `"upsert"`.
  - Heartbeats and schema-change topics are silently skipped.
- **Encode (forward path):** Emits Debezium-shaped JSON from pg_tide outbox rows.
  - INSERT → `op: "c"`, `before: null`, `after: <row>`.
  - UPDATE → `op: "u"`, `before: <old_row>`, `after: <new_row>`.
  - DELETE → `op: "d"`, `before: <old_row>`, `after: null` + optional tombstone.
  - Configurable `server_name` emitted in the `source` block with
    `connector: "pg_tide"`, `table`, `db`, `schema`, `lsn`, and `ts_ms`.
  - Tombstone emission after DELETE (`emit_tombstones: true`, default `true`).
  - Topic template: `{server}.{schema}.{stream_table}` (fully configurable).
  - No `r` (snapshot) events — documented as a difference vs. real Debezium.

### Schema Evolution Detection

- `SchemaTracker` per-topic field-set tracker on the decode side.
- `observe_schema()` is called before every inbound message; returns
  `WireError::SchemaIncompatible` when a field is **removed** (incompatible
  change), and silently accepts new fields (additive evolution).
- On schema incompatibility the pipeline surfaces an alert via the relay's
  existing error propagation path; the user resolves by updating the inbox table.

### Maxwell Decoder (`wire_format = "maxwell"`, feature `maxwell`)

- `MaxwellFormat` decodes Maxwell (https://maxwells-daemon.io) MySQL CDC JSON
  envelopes into `InboxRow`.
- Maps Maxwell types `insert`, `update`, `delete` → pg_tide ops.
- `bootstrap-insert` events configurable: treat as `insert` (default) or skip.
- Extracts `ts` (Unix epoch seconds) as `commit_ts` and `xid` as
  `source_position`.
- Decode-only: `encode()` returns `WireError::UnsupportedOperation`.
- Feature flag: `--features maxwell`.

### Canal Decoder (`wire_format = "canal"`, feature `canal`)

- `CanalFormat` decodes Alibaba Canal MySQL CDC JSON envelopes into `InboxRow`.
- Maps Canal types `INSERT`, `UPDATE`, `DELETE` → pg_tide ops (case-insensitive).
- DDL events (`isDdl: true`) are skipped by default (`skip_ddl: true`).
- Canal wraps data in arrays; the decoder takes the first element per event.
- Extracts `es` / `ts` (ms) as `commit_ts` and `id` as `source_position`.
- Decode-only: `encode()` returns `WireError::UnsupportedOperation`.
- Feature flag: `--features canal`.

### Custom CDC JSON (`wire_format = "cdc_json"`, feature `cdc-json`)

- `CdcJsonFormat` maps any CDC-shaped JSON to `InboxRow` using user-supplied
  dot-notation path expressions (`$.field.sub`).
- Configurable paths for `op`, `payload`, `old_payload`, `event_id`,
  `event_type`, `commit_ts`, and `source_position`.
- `op_map` allows remapping arbitrary source values to pg_tide op strings.
- `commit_ts_format`: `rfc3339` (default), `unix_seconds`, or `unix_millis`.
- Bidirectional: `encode()` produces a simple JSON document with `op` and
  `data` keys using the inverse op map.
- Feature flag: `--features cdc-json`.

### Tombstone Emission for Kafka Log-Compacted Topics

- Debezium encoder emits a null-value tombstone after every DELETE when
  `emit_tombstones: true` (the default).
- The tombstone carries the same key as the DELETE event so Kafka can compact
  the topic correctly.
- Disable with `emit_tombstones: false` for non-compacted topics.

### Upgrade Notes

- No SQL catalog changes. The upgrade script (`pg_tide--0.10.0--0.11.0.sql`)
  is a no-op.
- Existing pipelines without a `wire_format` field use `"native"` automatically
  — behaviour is identical to v0.10.0.
- New cargo features: `debezium` (always on), `maxwell`, `canal`, `cdc-json`.
  The default binary includes `debezium`; the others are opt-in.



---

## [0.10.0] — 2026-05-07 — Analytics Sinks: ClickHouse, MongoDB, Snowflake, BigQuery, Iceberg, Delta Lake, DuckLake

v0.10.0 delivers seven analytics sink backends for the relay, enabling
pg_tide to act as the bridge between your PostgreSQL transactional tables
and every major analytical data platform.

### ClickHouse Sink (RELAY-P3-CH)

- `sink_type: "clickhouse"` delivers relay messages to ClickHouse via its
  HTTP interface using `INSERT INTO … FORMAT JSONEachRow`.
- Messages are grouped by resolved table name and sent as NDJSON batches.
- Authentication via `X-ClickHouse-User` / `X-ClickHouse-Key` headers.
- `table_template` supports `{stream_table}` substitution.
- Feature flag: `--features clickhouse`.

### MongoDB Sink (RELAY-P3-MDB)

- `sink_type: "mongodb"` upserts relay messages as MongoDB documents using
  `replaceOne(upsert: true)` keyed by `dedup_key`.
- `op = "delete"` maps to `deleteOne`; `is_full_refresh = true` drops and
  recreates the collection.
- `collection_template` supports `{stream_table}` substitution.
- Configurable write concern (`majority` or numeric).
- Feature flag: `--features mongodb`.

### Snowflake Sink (RELAY-P3-SF)

- `sink_type: "snowflake"` delivers relay messages to Snowflake using the
  Snowpipe Streaming REST API (`insertRows`).
- Column names uppercased per Snowflake convention: `_DEDUP_KEY`, `_SUBJECT`,
  `_OP`, `_OUTBOX_ID`, `DATA`.
- `table_template` supports `{stream_table}` substitution.
- Bearer token authentication (JWT/pre-generated).
- Feature flag: `--features snowflake`.

### BigQuery Sink (RELAY-P3-BQ)

- `sink_type: "bigquery"` streams relay messages into BigQuery tables via
  the `tabledata.insertAll` REST endpoint.
- `insertId` is set to `dedup_key` for server-side deduplication.
- Checks `insertErrors` in the response and surfaces per-row failures.
- `table_template` supports `{stream_table}` substitution.
- Feature flag: `--features bigquery`.

### Apache Iceberg v2 Sink (RELAY-P3-ICE)

- `sink_type: "iceberg"` writes relay messages as Parquet data files
  (`PAR1`) conforming to the Iceberg v2 open table format spec.
- Generates `metadata/vN.metadata.json` per snapshot with full schema,
  partition spec, and snapshot manifest compatible with Iceberg readers.
- `write_mode: "append"` (default) or `"overwrite"`.
- Stores files via the `object_store` crate (S3, GCS, Azure Blob, local).
- Feature flag: `--features iceberg`.

### Delta Lake Sink (RELAY-P3-DL)

- `sink_type: "delta"` writes relay messages as Parquet files with Delta
  Lake Protocol v2 log commits under `_delta_log/`.
- Version 0 commit writes `protocol` + `metaData` actions; subsequent
  commits write `add` actions with row-count statistics.
- `change_data_feed: true` adds a `_change_type` column
  (`insert` / `delete` / `update_postimage`) for CDC consumers.
- Stores files via `object_store`; log entries are newline-delimited JSON.
- Feature flag: `--features delta`.

### DuckLake Sink (RELAY-P3-DLK)

- `sink_type: "ducklake"` writes relay messages as Parquet files to object
  storage and records snapshot metadata in a PostgreSQL catalog table
  (`tide.ducklake_snapshots`), enabling DuckDB to query data lake files
  via `CREATE TABLE … USING parquet(...)`.
- Catalog table is created automatically on first use.
- Supports `compression: "snappy"` (default), `"zstd"`, or `"none"`.
- `table_template` supports `{stream_table}` substitution.
- Feature flag: `--features ducklake`.

### Upgrade notes

This is a relay-binary-only release. No PostgreSQL catalog changes are
required. The `pg_tide--0.9.0--0.10.0.sql` migration file is a no-op.

---

## [0.9.0] — 2026-05-06 — Connector Ecosystem Foundation (Singer, Airbyte, Fivetran)

v0.9.0 brings first-class support for the three dominant open connector
ecosystems — Singer, Airbyte, and Fivetran — plus a Grafana/Perses relay
health dashboard.

### Singer Protocol Adapter (RELAY-P3-S1)

- `source_type: "singer"` spawns a Singer tap subprocess and reads
  `SCHEMA`, `RECORD`, and `STATE` messages from its stdout.
- `sink_type: "singer"` spawns a Singer target subprocess and writes
  messages to its stdin; STATE emitted by the target is captured and
  persisted automatically.
- STATE is stored in `tide.singer_state` (keyed by `pipeline_name`, `tap_name`)
  and reloaded on relay restart, enabling incremental replication.
- SCHEMA messages are logged to `tide.singer_schema_log` for drift detection;
  new SQL function `tide.singer_schema_drift()` surfaces changed streams.
- `on_schema_change` option: `log` (default), `emit_event`, or `error`.
- Feature flag: `--features singer`.

### Airbyte Protocol Adapter (RELAY-P3-A1)

- `source_type: "airbyte"` spawns an Airbyte source connector (Docker image or
  bare command) and reads `RECORD`, `STATE`, `CATALOG`, `LOG`, and `TRACE`
  messages from its stdout.
- `sink_type: "airbyte"` spawns an Airbyte destination connector and writes
  messages to its stdin; STATE is captured and persisted automatically.
- STATE is stored in `tide.relay_airbyte_state` (keyed by `pipeline_name`,
  `source_name`) and reloaded on relay restart.
- Both `image` (Docker) and `command` (bare executable) launch modes supported.
- CDC soft-delete detection via `_ab_cdc_deleted_at` metadata field.
- Feature flag: `--features airbyte`.

### Fivetran HVR Webhook Flavor (RELAY-P3-F1)

- `signature_scheme: "fivetran"` added to the webhook source.
- Validates the `X-Fivetran-Signature` header using HMAC-SHA256 with
  `sha256=<hex>` prefix format.
- Handles Fivetran HVR batch payloads: `insert`, `update`, `delete`, and
  `upsert` operation types.

### Relay Health Dashboard (RELAY-P3-D1)

- New Grafana/Perses dashboard at `pg-tide/dashboards/relay-health.json`.
- Panels: messages forwarded/sec, inbox messages received/sec, error rate,
  DLQ messages, outbox backlog depth, forward latency (p50/p95/p99),
  circuit breaker state, retry attempts/sec, inbox lag.

### SQL Migration

- `sql/pg_tide--0.7.0--0.8.0.sql` — empty upgrade for v0.8.0 (relay-only
  release, no SQL changes).
- `sql/pg_tide--0.8.0--0.9.0.sql` — adds `tide.singer_state`,
  `tide.singer_schema_log`, `tide.relay_airbyte_state` tables and the
  `tide.singer_state_list()` and `tide.singer_schema_drift()` SQL functions.

---

## [0.8.0] — 2026-05-05 — Notification Sinks & Apache Arrow Flight

v0.8.0 adds four new sinks to the relay: three notification backends
(Slack, Discord, PagerDuty) and the high-performance Apache Arrow Flight /
gRPC columnar data transport.

### Slack Notification Sink (RELAY-P3-N1)

- `sink_type: "slack"` delivers relay messages to a Slack channel via the
  Incoming Webhooks API (Block Kit format).
- Each batch is formatted as a Block Kit message with one `section` block per
  relay message — subjects, op type, dedup key, and JSON payload are included.
- Large batches are split into multiple Slack messages respecting the
  configurable `batch_limit` (default: 50 blocks per message).
- Optional `username` and `icon_emoji` override the bot display name and icon.
- Feature flag: `--features slack`.

**Configuration:**
```toml
[sink]
webhook_url   = "https://hooks.slack.com/services/T.../B.../..."
username      = "pg-tide"         # optional
icon_emoji    = ":database:"      # optional
batch_limit   = 50                # optional, default 50
```

### Discord Notification Sink (RELAY-P3-N2)

- `sink_type: "discord"` delivers relay messages to a Discord channel via the
  Discord Webhook API (Embeds format).
- Each relay message is presented as a Discord Embed with colour coding:
  green (`0x57F287`) for inserts, red (`0xED4245`) for deletes, grey otherwise.
- Embed title shows subject and op; description shows the JSON payload in a
  code block; footer contains the dedup key for tracing.
- Discord's limit of 10 embeds per message is enforced via `batch_limit` (max 10).
- Optional `username` and `avatar_url` customise the webhook bot appearance.
- Feature flag: `--features discord`.

**Configuration:**
```toml
[sink]
webhook_url  = "https://discord.com/api/webhooks/1234567890/XXXX"
username     = "pg-tide-relay"    # optional
avatar_url   = "https://..."      # optional
batch_limit  = 10                 # optional, default 10 (Discord max)
```

### PagerDuty Notification Sink (RELAY-P3-N3)

- `sink_type: "pagerduty"` triggers PagerDuty incidents via the Events API v2.
- Each relay message triggers a separate PagerDuty event with:
  - `event_action: "trigger"` — creates or deduplicates an incident.
  - `dedup_key` set from the relay message's dedup key — prevents duplicate alerts.
  - `payload.custom_details` contains the full relay message payload.
  - `payload.severity` is configurable (`critical`, `error`, `warning`, `info`);
    delete operations always use `info` regardless of the configured severity.
- Optional `source` and `component` fields populate the PagerDuty event context.
- Feature flag: `--features pagerduty`.

**Configuration:**
```toml
[sink]
routing_key = "R0000000000000000000000000000001"
severity    = "critical"          # optional, default "info"
source      = "pg-tide-relay"     # optional
component   = "orders-service"    # optional
```

### Apache Arrow Flight / gRPC Sink (RELAY-P3-2)

- `sink_type: "arrow-flight"` pushes relay messages to an Arrow Flight server
  using the `DoPut` RPC — the standard columnar high-throughput data transfer protocol.
- Messages are encoded as Arrow RecordBatches with a fixed schema:

  | Column     | Type  | Nullable |
  |------------|-------|----------|
  | dedup_key  | Utf8  | No       |
  | subject    | Utf8  | No       |
  | op         | Utf8  | No       |
  | payload    | Utf8  | No       |
  | outbox_id  | Int64 | Yes      |

- The `payload` column contains the JSON-serialized relay message payload.
- Connection is lazily established on first publish and reused across batches.
- Optional `auth_token` sets a Bearer token on the gRPC metadata.
- `descriptor_path` identifies the stream on the server (slash-separated, e.g. `"pg-tide/orders"`).
- Feature flag: `--features arrow-flight`.

**Configuration:**
```toml
[sink]
url             = "http://localhost:50051"
auth_token      = "Bearer ..."    # optional
descriptor_path = "pg-tide"       # optional, default "pg-tide"
```

---

## [0.7.0] — 2026-05-06 — Production-Grade Relay Operations

v0.7.0 delivers the full suite of production-readiness features for the relay:
dead-letter queue, JMESPath transforms, content-based routing, rate limiting,
circuit breaker, Confluent Schema Registry integration, OpenTelemetry tracing,
webhook signature verification, SIGHUP config reload, and dry-run/replay modes.

### Dead-Letter Queue (RELAY-P2-11)

- Failed messages that exceed the retry limit are routed to `tide.relay_dlq`.
- New SQL API: `tide.dlq_list()`, `tide.dlq_replay()`, `tide.dlq_drop()`,
  `tide.dlq_stats()`, `tide.dlq_purge_before()`, `tide.dlq_inspect()`.
- Per-pipeline `dlq` config block enables/disables the DLQ and sets TTL.
- SQL migration `pg_tide--0.6.0--0.7.0.sql` creates the DLQ table and functions.

### Webhook Source & Signature Verification (RELAY-P2-12)

- `source_type: "webhook"` launches an embedded HTTP server and writes
  incoming payloads to the pg-tide inbox.
- `signature_scheme` supports `hmac_sha256`, `github`, `stripe`, and `svix`.
- Constant-time comparison prevents timing attacks.

### JMESPath Message Transforms (RELAY-P2-13)

- Per-pipeline `transforms` block supports `filter` and `projection` expressions.
- `filter`: JMESPath expression evaluated against the payload; messages that do
  not match are dropped before reaching the sink.
- `projection`: JMESPath expression that reshapes the payload (e.g. extract a
  sub-object).

### Rate Limiting (RELAY-P2-15)

- Token-bucket rate limiter via `governor`. Per-pipeline `rate_limit` config
  block sets `messages_per_second`. The relay blocks until a token is available.

### Circuit Breaker (RELAY-P2-16)

- Three-state circuit breaker (`Closed` → `Open` → `HalfOpen`) prevents
  thundering-herd retries against a failing sink.
- `failure_threshold`, `recovery_timeout_secs`, and `half_open_probe_count`
  are configurable.

### Confluent Schema Registry & Avro (RELAY-P2-17)

- Optional `schema-registry` feature adds Confluent Schema Registry integration.
- Avro serialization with auto-registration of schemas.
- `SubjectNameStrategy`: `topic` (default), `record_name`, `topic_record_name`.
- Confluent wire-format framing (magic byte + 4-byte schema ID).

### SIGHUP Config Reload (RELAY-P2-18)

- Sending `SIGHUP` to the relay process triggers a live config reload without
  downtime.

### Dry-Run & Replay Modes (RELAY-P2-19)

- `--dry-run` flag: the relay reads messages and applies transforms but does
  not publish to the sink or update cursor state.
- `--replay` flag: reprocesses already-acknowledged messages from the outbox
  from the beginning without modifying inbox/DLQ state.

### OpenTelemetry Tracing (RELAY-P2-20)

- Optional `otel` feature emits OTLP traces for every relay pipeline iteration.
- `[otel]` config block sets `endpoint`, `service_name`, and `sampling_ratio`.
- Uses `opentelemetry-otlp` 0.27 with Tonic gRPC transport.

---

## [0.6.0] — 2026-05-05 — MQTT v5, Azure Event Hubs & Object Storage (JSONL + Parquet)

v0.6.0 adds three major backend families, completing the relay's IoT, Azure,
and data-lake integration story. Every new backend ships with both forward-sink
and reverse-source support where applicable, plus full integration tests.

### MQTT v5 (RELAY-P3-1)

- **Sink** (`sink_type: "mqtt"`): Publishes outbox messages to an MQTT broker
  topic. Topic is rendered from `topic_template` supporting `{stream_table}`,
  `{op}`, and `{outbox_id}` variables. QoS level is configurable
  (`at-most-once`, `at-least-once`, `exactly-once`). Uses `rumqttc` 0.24 with
  a background event-loop task for keep-alive and ack processing.
- **Source** (`source_type: "mqtt"`): Subscribes to a topic filter and writes
  incoming messages to the configured pg-tide inbox. Dedup key is derived from
  the MQTT topic and a message counter; acks are handled internally by the
  broker protocol.

### Azure Event Hubs (RELAY-P3-2)

- **Sink** (`sink_type: "eventhubs"`): Sends outbox messages to an Azure Event
  Hubs namespace via the Event Hubs REST API with SAS token authentication.
  Partition key is rendered from `partition_key_template`. Connection string
  format: `Endpoint=sb://<ns>.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...`.
- **Source** (`source_type: "eventhubs"`): Reads events from all partitions
  using ReceiveAndDelete semantics via the REST API. Partitions are polled in
  round-robin order. Dedup key encodes the namespace, event hub, consumer
  group, partition id, and sequence number for exact-once delivery into the
  inbox.

### Object Storage — S3, GCS, Azure Blob (RELAY-P3-3)

- **Sink** (`sink_type: "object-storage"`): Buffers outbox messages and flushes
  them to cloud object storage as files in JSONL or Apache Parquet format.
  - **Providers**: Amazon S3, Google Cloud Storage, Azure Blob Storage, and
    local filesystem (useful for testing).
  - **JSONL format**: One JSON object per line containing `dedup_key`,
    `subject`, `op`, `outbox_id`, and `payload`.
  - **Parquet format**: Typed columnar file with the same five columns.
    `outbox_id` is INT64 (nullable), all others are BYTE_ARRAY strings.
  - **Flush triggers**: Configurable via `buffer_max_rows`, `buffer_max_bytes`,
    and `buffer_max_seconds`. Rows are held in memory until a threshold is hit.
  - **Date partitioning**: With `partition_by_date: true`, objects are placed
    under `{prefix}/year=YYYY/month=MM/day=DD/` — compatible with Hive
    metastore, AWS Glue, BigQuery external tables, and similar.
  - **Path pattern**: `{prefix}/pgtide_{uuid}.{jsonl|parquet}` (flat) or
    `{prefix}/year=YYYY/month=MM/day=DD/pgtide_{uuid}.{ext}` (partitioned).

### No Schema Changes

v0.6.0 adds no DDL changes. All new backends are configured via the existing
`tide.relay_outbox_config` and `tide.relay_inbox_config` catalog tables.

---

## [0.5.0] — 2026-05-04 — Cloud Provider Parity: Pub/Sub, Kinesis, Azure Service Bus & Elasticsearch

v0.5.0 delivers cloud provider parity for the relay binary. Every major cloud
messaging platform is now supported as both a forward sink and (where
applicable) a reverse source. The relay can now integrate with GCP, AWS
streaming, Azure, and Elasticsearch/OpenSearch out of the box — all
configurable via the existing SQL catalog tables.

### Google Cloud Pub/Sub (RELAY-P2-1)

- **Sink** (`sink_type: "pubsub"`): Publishes outbox messages to a GCP Pub/Sub
  topic via the REST API. Each message is base64-encoded with `pgt_dedup_key`,
  `pgt_op`, and `pgt_subject` attributes for consumer-side correlation.
  Supports both real GCP (via `PUBSUB_TOKEN` env-var) and the Pub/Sub emulator
  (`PUBSUB_EMULATOR_HOST=host:port`).
- **Source** (`source_type: "pubsub"`): Pull subscription consumer. Messages
  are acknowledged only after successful inbox write. Uses `pgt_dedup_key`
  attribute as the dedup key if present; otherwise falls back to the Pub/Sub
  message ID.

### Amazon Kinesis Data Streams (RELAY-P2-2)

- **Sink** (`sink_type: "kinesis"`): Publishes outbox messages to a Kinesis
  stream using `PutRecords` (up to 500 records per call). Partition key is
  rendered from `partition_key_template` supporting the same `{stream_table}`,
  `{op}`, `{outbox_id}` variables as other sinks.
- **Source** (`source_type: "kinesis"`): Reads records from all shards using
  `GetShardIterator` + `GetRecords`. Handles shard discovery automatically.
  Dedup key is `kinesis:<shard_id>:<sequence_number>`.

### Azure Service Bus (RELAY-P2-3)

- **Sink** (`sink_type: "servicebus"`): Sends messages to an Azure Service Bus
  queue or topic using the Service Bus REST API with SAS token authentication.
  MessageId is set to the dedup key for native deduplication on queues with
  `RequiresDuplicateDetection` enabled.
- **Source** (`source_type: "servicebus"`): PeekLock-mode consumer. Messages
  are completed (deleted from the queue) only after successful inbox write.
  Abandoned messages are redelivered after the lock timeout.

### Elasticsearch / OpenSearch (RELAY-P2-4)

- **Sink** (`sink_type: "elasticsearch"`): Bulk-indexes outbox messages via
  the `_bulk` HTTP API. Document ID is set to the dedup key for idempotent
  upserts. Delete operations (`op = "delete"`) emit a `delete` bulk action.
  Compatible with both Elasticsearch 8.x and OpenSearch 2.x.
  - `index_template`: Supports `{stream_table}`, `{op}`, `{outbox_id}` template
    variables (e.g. `"pg-tide-{stream_table}"`).
  - Bulk response errors are surfaced as relay errors for retry.

### No Schema Changes

v0.5.0 adds no DDL changes. All new backends are configured via the existing
`tide.relay_outbox_config` and `tide.relay_inbox_config` catalog tables.

---

## [0.4.0] — 2026-05-04 — Relay Completion: Tier 2 Sinks, Full Reverse Mode & Integration Tests

v0.4.0 completes the relay binary. Every forward sink and every reverse source
described in the roadmap is now implemented, tested, and enabled. The relay can
now move events in both directions between PostgreSQL and any supported messaging
system, with no skipped or ignored tests.

### Forward Mode — Tier 2 Sinks (RELAY-10 to RELAY-13)

Four additional forward sinks are now fully wired and tested:

- **Redis Streams** (`sink_type: "redis"`): Publishes outbox messages to a
  Redis Stream via `XADD`. Stream key supports the same `{stream_table}`,
  `{op}`, `{outbox_id}` template variables as other sinks. Optional
  `MAXLEN ~` trimming keeps stream size bounded.
- **Amazon SQS** (`sink_type: "sqs"`): Delivers outbox messages to an SQS
  queue in batches of up to 10 (the SQS limit). FIFO queues are supported
  via `is_fifo: true`; dedup keys are used as `MessageDeduplicationId`.
- **PostgreSQL inbox** (`sink_type: "pg-inbox"`): Inserts outbox messages
  directly into a remote `tide.<inbox>` table using `ON CONFLICT (event_id)
  DO NOTHING`, providing end-to-end at-least-once delivery between two
  PostgreSQL databases.
- **RabbitMQ** (`sink_type: "rabbitmq"`): Publishes to a RabbitMQ exchange
  with per-message routing keys generated from the subject/topic template.
  Publisher confirms are awaited before acknowledging the outbox offset.

### Reverse Mode — All Source Backends (RELAY-22 to RELAY-30)

All reverse-mode source backends are now complete, enabling the relay to
consume messages from any supported system and write them to a pg-tide inbox:

- **NATS JetStream** (`source_type: "nats"`): Durable pull consumer with
  per-message ack after successful inbox write. Nats-Msg-Id header is used
  as the dedup key when present.
- **Apache Kafka** (`source_type: "kafka"`): Manual offset commit only after
  the inbox write succeeds. Per-partition offset tracking.
- **HTTP Webhook receiver** (`source_type: "webhook"`): axum-based HTTP
  server that accepts POST requests. `Idempotency-Key` header is used as the
  dedup key. Response (200) is sent synchronously.
- **Redis Streams** (`source_type: "redis"`): `XREADGROUP` with
  `XACK` after inbox write.
- **Amazon SQS** (`source_type: "sqs"`): `ReceiveMessage` with
  `DeleteMessage` after inbox write.
- **RabbitMQ** (`source_type: "rabbitmq"`): `basic_consume` with manual
  `basic_ack` after inbox write.
- **stdin / file** (`source_type: "stdin"`): Reads newline-delimited JSON
  from stdin or a file path, useful for testing and one-shot imports.
- **Inbox sink** (`sink_type: "pg-inbox"`): The reverse-mode sink that
  writes every incoming message to `tide."<inbox>_inbox"` with
  `ON CONFLICT (event_id) DO NOTHING` deduplication (RELAY-22).

### Subject / Topic Routing (RELAY-14)

- Template variables `{stream_table}`, `{op}`, `{outbox_id}`, and
  `{refresh_id}` are now documented and tested for all sink backends.
- `reverse_dedup_key()` generates stable, source-specific dedup keys
  (e.g. `kafka:<partition>:<offset>`, `nats:<msg-id>`) that survive relay
  restarts.
- `extract_event_type()` tries a configurable list of payload field names
  before falling back to the pipeline's default event type.

### Integration Tests (RELAY-15 to RELAY-18, RELAY-31 to RELAY-32)

All integration tests now run without `#[ignore]`. Every backend is exercised
against a real service running in a Docker container managed by testcontainers:

- **Redis** — Redis 7 via testcontainers-modules; forward XADD + reverse dedup.
- **RabbitMQ** — `rabbitmq:4-management` via GenericImage; forward publish +
  reverse redelivery dedup.
- **NATS** — `nats:latest` via GenericImage; forward subject publish + reverse
  inbox dedup.
- **SQS** — `softwaremill/elasticmq-native` (lightweight, in-process SQS
  mock); forward send + reverse redelivery dedup. Replaced the previous
  LocalStack dependency for faster startup.
- **Kafka** — Kafka forward and reverse tests exercise the outbox/consumer
  offset contract without spinning up a full Kafka broker (the complex Kafka
  wire tests require the `kafka` feature and a running broker).
- **Webhook** — In-process axum mock; forward POST delivery + large-payload
  handling + reverse idempotent delivery.
- All previously pure-DB tests (round-trip, inbox sink, consumer groups,
  graceful shutdown, backpressure, exactly-once) continue to pass.

### Test Infrastructure

- Switched RabbitMQ container from `testcontainers_modules::rabbitmq::RabbitMq`
  (which uses an aging 3.8 image) to `rabbitmq:4-management` with an explicit
  `WaitFor::message_on_stdout("Server startup complete")` strategy for
  reliable cross-platform startup.
- Switched SQS from LocalStack (500 MB, 30 s startup) to
  `softwaremill/elasticmq-native` (50 MB, <2 s startup), removing a
  significant source of test flakiness.

---

## [0.3.0] — 2026-05-04 — Relay Run Loop, Secret Interpolation & pg-tide Branding

The relay binary is now fully operational. Previously it would start, load
pipeline configuration from PostgreSQL, and then do nothing. This release
implements the complete coordinator run loop and pipeline worker dispatch.

### Relay — Coordinator Run Loop (RELAY-2)

- **Multi-pipeline coordinator**: The relay now spawns a worker task for every
  enabled pipeline it wins a PostgreSQL advisory lock for. Multiple relay pods
  can run simultaneously; each pod automatically claims and runs the pipelines
  not already owned by another pod.
- **Hot-reload on NOTIFY**: A dedicated LISTEN connection watches for
  `tide_relay_config` notifications. Any `INSERT`, `UPDATE`, or `DELETE` on
  `tide.relay_outbox_config` / `tide.relay_inbox_config` triggers an
  immediate pipeline reconciliation without restarting the process.
- **Periodic re-discovery**: Every `discovery_interval_secs` seconds the
  coordinator re-queries the catalog to pick up pipelines that may have
  appeared while another pod held the lock, or to restart stopped workers.
- **Graceful shutdown**: SIGTERM / Ctrl-C signals the coordinator to stop
  accepting new work and waits up to `drain_timeout` seconds for all active
  pipeline workers to finish their current batch before exiting.

### Relay — Worker Pipeline Loop

- **Poll → publish → acknowledge loop**: Each pipeline worker opens its own
  PostgreSQL connection, creates the configured source and sink backends from
  the pipeline's `config` JSONB, and runs the pull–push–ack cycle. An empty
  poll sleeps for `poll_interval_ms` (default 1 000 ms) before retrying.
- **Per-pipeline error isolation**: A publish or poll error logs a warning and
  retries after `poll_interval_ms`. One broken pipeline never affects others.
- **Source and sink factories**: All eight forward sinks (NATS JetStream,
  Apache Kafka, HTTP webhook, Redis Streams, Amazon SQS, RabbitMQ, PostgreSQL
  inbox, stdout/file) and all eight reverse sources are wired to the factory
  based on `source_type` / `sink_type` in the pipeline config JSONB.

### Relay — Secret Interpolation (RELAY-SEC)

- **`${env:VAR}` tokens**: String values inside a pipeline's `config` JSONB
  are now scanned for `${env:VAR_NAME}` tokens. At startup and on every
  hot-reload the token is replaced with the value of the named environment
  variable. Unknown variables return an error that disables only the affected
  pipeline — all others continue running.
- **`${file:/path}` tokens**: A token of the form `${file:/run/secrets/apikey}`
  is replaced with the trimmed contents of the file at that path. Useful for
  Docker/Kubernetes secret mounts.
- **Validation**: Variable names are validated (ASCII alphanumeric + `_`
  only) to prevent injection.
- **No secret leakage**: Resolved values are never written to logs or
  Prometheus metric labels.

### Relay — Branding (pg-trickle → pg-tide)

- Binary name, `--help` text, and all environment variable prefixes updated
  from `PGTRICKLE_RELAY_*` to `PG_TIDE_*` (e.g. `PG_TIDE_POSTGRES_URL`,
  `PG_TIDE_METRICS_ADDR`, `PG_TIDE_LOG_LEVEL`, `PG_TIDE_RELAY_GROUP_ID`,
  `PG_TIDE_CONFIG`, `PG_TIDE_DRAIN_TIMEOUT`).


### Observability

- **Metrics**: Added `consumer_lag` (messages behind the committed offset, per
  consumer group) and `delivery_latency_seconds` (histogram of end-to-end
  relay latency) to the Prometheus `/metrics` endpoint. Both metrics are
  emitted by the relay binary and require no extension upgrade.

### Relay Binary

- **Docker — Native ARM**: The release workflow now builds on native ARM64
  runners, cutting cross-compilation time significantly.
- **Docker — Semver tags**: Images are now tagged with the full version
  (`v0.1.0`), the minor prefix (`v0.1`), and `latest`, matching the convention
  used by most container registries.
- **Docker — OCI annotations**: Images carry standard OCI image labels
  (`org.opencontainers.image.*`) for source, revision, and creation time,
  making provenance visible in any OCI-compliant registry.

### Extension Hardening

Several pgrx compatibility issues discovered after the initial release were
fixed. None require an upgrade script — they correct the compiled extension
artefact only:

- Constraint names on inbox tables are now quoted, so inbox names containing
  hyphens (e.g. `my-service-inbox`) work correctly.
- Literal backslashes in raw SQL strings were removed, eliminating a
  `standard_conforming_strings`-dependent edge case.
- The `tide` schema is now declared via `#[pgrx::pg_schema]` rather than a
  bare `CREATE SCHEMA` in the DDL file, fixing schema creation ordering under
  `cargo-pgrx 0.18`.
- `trusted` and `schema` fields removed from the control file; neither is
  supported by `cargo-pgrx 0.18`.

### CI

- Bumped `actions/checkout` v4 → v6 and `actions/cache` v4 → v5.
- Granted the test runner write access to PostgreSQL extension directories,
  fixing pgrx test failures on GitHub-hosted runners.
- Split the `clippy` job so extension and relay linting run independently,
  giving clearer failure attribution.
- Fixed release workflow: the `CARGO_REGISTRY_TOKEN` guard is now at
  step-level, preventing a false "secret not set" error on crates.io publish.

### Documentation

- Restructured and consolidated the `docs/` tree into a MdBook with separate
  reference, relay guide, integration, operations, and tutorial sections.
- Added relay CLI phase plans (migrated from pg-trickle) covering the
  `pg-tide` binary command-line interface roadmap.

---

## [0.1.0] — 2025-05-03 — Initial Release

v0.1.0 is the founding release of `pg_tide`. The full transactional outbox,
idempotent inbox, consumer group, and relay subsystem (~6,150 Rust LOC +
~2,500 SQL LOC) was extracted from
[`pg_trickle`](https://github.com/trickle-labs/pg-trickle) v0.46.0 and
published as a standalone PostgreSQL 18 extension.

### SQL Functions — Outbox

- `tide.outbox_create(name, retention_hours, inline_threshold_rows)` — creates
  a named outbox table and registers it in the catalog.
- `tide.outbox_publish(name, payload, dedup_key)` — appends a message to the
  outbox inside the caller's transaction; the message becomes visible to the
  relay only after the transaction commits.
- `tide.outbox_drop(name)` — removes the outbox and its catalog entry.
- `tide.outbox_status(name)` — returns pending count, last publish time, and
  retention settings.
- `tide.outbox_enable(name)` / `tide.outbox_disable(name)` — pause and resume
  relay consumption without dropping the outbox.

### SQL Functions — Inbox

- `tide.inbox_create(name)` — creates a named inbox table with dedup tracking.
- `tide.inbox_drop(name)` — removes the inbox and its catalog entry.
- `tide.inbox_mark_processed(name, dedup_key)` — idempotently marks a message
  delivered; duplicate calls are silently ignored.
- `tide.inbox_mark_failed(name, dedup_key, reason)` — moves a message to the
  dead-letter queue.
- `tide.inbox_status(name)` — returns pending, processed, and DLQ counts.
- `tide.replay_inbox_messages(name, since)` — re-queues DLQ messages for
  reprocessing.

### SQL Functions — Consumer Groups

- `tide.create_consumer_group(outbox, group_name)` — registers a named
  consumer group against an outbox.
- `tide.drop_consumer_group(outbox, group_name)` — removes the group and its
  offset tracking.
- `tide.commit_offset(outbox, group_name, offset)` — advances the committed
  read position.
- `tide.consumer_heartbeat(outbox, group_name)` — updates the liveness
  timestamp; groups that miss heartbeats are marked stale.

### SQL Functions — Relay Catalog

- `tide.relay_set_outbox(relay_name, outbox_name, consumer_group)` — registers
  the source side of a relay pipeline.
- `tide.relay_set_inbox(relay_name, inbox_name)` — registers the destination
  side of a relay pipeline.
- `tide.relay_enable(relay_name)` / `tide.relay_disable(relay_name)` — start
  and stop relay processing for a named pipeline.
- `tide.relay_delete(relay_name)` — removes a pipeline from the catalog.
- `tide.relay_get_config(relay_name)` / `tide.relay_list_configs()` — inspect
  pipeline configuration.

### Views

- `tide.outbox_pending` — messages in the outbox not yet consumed by any
  registered consumer group.
- `tide.consumer_lag` — per-consumer-group lag (messages published minus
  committed offset).

### Relay Binary (`pg-tide`)

Multi-backend relay supporting **NATS**, **Kafka**, **Redis Streams**,
**RabbitMQ**, **Amazon SQS**, **Webhook**, and **stdout**. Features:

- Advisory-lock-based HA coordination — only one relay instance is active
  per pipeline at a time; additional instances stand by and take over
  automatically on failure.
- Prometheus `/metrics` endpoint and `/health` liveness probe.
- Structured JSON logging via `tracing`.
- TOML-based pipeline configuration loaded from file or the `pg_tide` relay
  catalog.

[Unreleased]: https://github.com/trickle-labs/pg-tide/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/trickle-labs/pg-tide/releases/tag/v0.1.0
