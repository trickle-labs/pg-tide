# Changelog

What's new in pg_tide — written for everyone, not just developers.

For future plans and upcoming features, see [ROADMAP.md](ROADMAP.md).

## Table of Contents

<!-- TOC start -->
- [0.38.0 — RockLake Native Ingestion, Phase 6 Integration Testing & Phase 7 Production Hardening](#0380--rocklake-native-ingestion-phase-6-integration-testing--phase-7-production-hardening)
- [0.37.0 — CloudNativePG Image Volume Extensions & RockLake Integration Phases 0–5](#0370--cloudnativepg-image-volume-extensions--rocklake-integration-phases-05)
- [0.36.0 — CLI Completeness, Test Coverage Depth & v1.0 Pre-Flight](#0360--cli-completeness-test-coverage-depth--v10-pre-flight)
- [0.35.0 — Assessment-7 P1/P2 Bug Fixes, KMS Encryption & Fan-In Performance Hardening](#0350--assessment-7-p1p2-bug-fixes-kms-encryption--fan-in-performance-hardening)
- [0.34.0 — Universal Reverse Pipeline Sinks & DuckLake Ecosystem Completeness](#0340--universal-reverse-pipeline-sinks--ducklake-ecosystem-completeness)
- [0.33.0 — Pre-GA Supply-Chain Hardening, KMS Foundation & v1.0 Readiness](#0330--2026-05-20--pre-ga-supply-chain-hardening-kms-foundation--v10-readiness)
- [0.32.0 — Performance Engineering, Code Internals Quality & Benchmark Hardening](#0320--2026-05-20--performance-engineering-code-internals-quality--benchmark-hardening)
- [0.31.0 — Assessment-6 P1/P2 Bug Fixes, Identifier Quoting Hardening & Release-Process Automation](#0310--assessment-6-p1p2-bug-fixes-identifier-quoting-hardening--release-process-automation)
- [0.30.0 — Pipeline Dependency DAG, AsyncAPI Completeness & Pre-GA Final Hardening](#0300--pipeline-dependency-dag-asyncapi-completeness--pre-ga-final-hardening)
- [0.29.0 — Pipeline Templates, Multi-Outbox Fan-In, Lifecycle Management & Backfill Completion](#0290--pipeline-templates-multi-outbox-fan-in-lifecycle-management--backfill-completion)
- [0.28.0 — Delivery Receipts, Canonical Config & Native Claim-Check](#0280--delivery-receipts-canonical-config--native-claim-check)
- [0.27.0 — Observability Expansion, CLI Ergonomics & Pre-GA Documentation Polish](#0270--2026-05-20--observability-expansion-cli-ergonomics--pre-ga-documentation-polish)
- [0.26.0 — Partition Safety, Defence-in-Depth & Test Coverage Completion](#0260--2026-05-20--partition-safety-defence-in-depth--test-coverage-completion)
- [0.25.0 — Outbox Table Partitioning, Multi-Tenant Relay Completion & Pre-GA Hardening](#0250--2026-05-20--outbox-table-partitioning-multi-tenant-relay-completion--pre-ga-hardening)
- [0.24.0 — Code Quality, Performance & Helm Production Maturity](#0240--2026-05-19--code-quality-performance--helm-production-maturity)
- [0.23.0 — Correctness, Real TLS & Full Migration Coverage](#0230--2026-05-19--correctness-real-tls--full-migration-coverage)
- [0.22.0 — DuckLake Bidirectional Flow & Ecosystem Surface](#0220--2026-05-19--ducklake-bidirectional-flow--ecosystem-surface)
- [0.21.0 — DuckLake Streaming, Inlining & Schema Evolution](#0210--2026-05-19--ducklake-streaming-inlining--schema-evolution)
- [0.20.0 — DuckLake Native Catalog Integration](#0200--2026-05-19--ducklake-native-catalog-integration)
- [0.19.0 — Supply Chain, Observability & Operational Docs](#0190--2026-05-14--supply-chain-observability--operational-docs)
- [0.18.0 — Security Completeness, LISTEN Hot-Reload & API Polish](#0180--2026-05-13--security-completeness-listen-hot-reload--api-polish)
- [0.17.0 — Catalog Integrity, DLQ Reliability & Contract Correctness](#0170--2026-05-12--catalog-integrity-dlq-reliability--contract-correctness)
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

## [0.38.0] — RockLake Native Ingestion, Phase 6 Integration Testing & Phase 7 Production Hardening

This release completes the RockLake integration started in v0.37.0, delivering a fully
production-ready `RockLakeSink` / `RockLakeSource` pair with Phase 6 end-to-end integration
tests and Phase 7 production hardening. pg-tide is now the first production-grade event
streaming system with native RockLake support — providing a **zero-infrastructure path from a
PostgreSQL transaction to a queryable, time-traveling data lake in S3** backed by RockLake
v0.27.14.

### Phase 6: Integration Testing & Verification

End-to-end integration tests using `PgWireHarness` from `rocklake-testkit` verify:

- **Catalog ready check** — `verify_catalog_ready()` returns `Ok` for an initialized catalog
  and a clear error for an uninitialized one.
- **Inline ingestion round-trip** — batches ≤ `inline_row_limit` are committed as inlined-data
  rows and readable via plain SQL snapshot queries.
- **Parquet ingestion round-trip** — batches > `inline_row_limit` are written as Parquet files
  to object storage with a `ducklake_data_file` catalog row committed.
- **Schema evolution** — sequential batches to the same table produce distinct snapshots.
- **Partition metadata registration** — tables with a partition strategy have a `pg_tide.*`
  entry in `ducklake_metadata`.
- **RockLakeSource snapshot polling** — source returns messages for new snapshots beyond the
  last-seen offset.
- **Time-travel snapshot isolation** — two sequential batches produce monotonically increasing
  `snapshot_id` values, satisfying DuckLake MVCC time-travel semantics.
- **Crash recovery** — a sink restarted against the same catalog does not duplicate data.

All tests run against the live in-process RockLake PG-Wire server (zero Docker, zero external
dependencies) using an `InMemory` object store.

### Phase 7: Production Hardening

**SQLSTATE 57P04 — writer epoch mismatch (writer takeover fencing)**

When the RockLake sidecar returns `SQLSTATE 57P04` (writer epoch mismatch — another writer has
taken over the catalog lease), `RockLakeSink::publish()` now:
1. Detects the error by inspecting `db_error().code() == SqlState::DATABASE_DROPPED`.
2. Logs a `WARN`-level trace event with the attempt count and `"57P04 writer-epoch-mismatch"`.
3. Resets the `catalog_ready` flag so the next attempt re-verifies catalog health.
4. Backs off with exponential delay + ±25 % jitter (100 ms base, 30 s cap).
5. Retries up to `max_write_retries` times (default: 5) before propagating the error.

**SQLSTATE 40001 — serialization failure (transaction conflict)**

Same retry loop with the same backoff strategy applies to `SQLSTATE 40001` (serialization
failure / transaction conflict), tagged as `"40001 serialization-failure"` in trace events.

**`max_write_retries` configuration field**

New `RockLakeConfig::max_write_retries: u32` field (default: 5). Set to 0 to disable retries.

**`read_replica_url` configuration field**

New optional `RockLakeConfig::read_replica_url: Option<String>` field. When set, read-only
operations (snapshot lookups, catalog health checks) can be routed to a replica endpoint to
shed read load from the primary writer sidecar.

**`RelayError::as_postgres_error()`**

New `RelayError::as_postgres_error() -> Option<&tokio_postgres::Error>` method exposes the
underlying `tokio_postgres::Error` for SQLSTATE inspection without wrapping it in a string.
The critical commit paths in `publish_parquet()` and `publish_inline()` now propagate
`RelayError::Postgres(e)` directly instead of `RelayError::Other(format!(...))`, preserving
the SQLSTATE code for the retry loop.

### Migration validation tests

- **`v037_validation_test.rs`** — verifies the v0.36.0→v0.37.0 migration applies cleanly and
  all v2 relay API functions remain present.
- **`v038_validation_test.rs`** — verifies the v0.37.0→v0.38.0 migration applies cleanly,
  no breaking SQL changes were introduced, and Phase 7 config fields compile correctly.

### SQL migration

`sql/pg_tide--0.37.0--0.38.0.sql` — no schema changes. All v0.38.0 work is in the relay
binary.

---

## [0.37.0] — CloudNativePG Image Volume Extensions & RockLake Integration Phases 0–5

This release introduces CloudNativePG Image Volume Extensions support for pg_tide, enabling secure, 
minimal container images with decoupled extension distribution. It also begins the RockLake ecosystem 
integration with phases 0–5, opening a zero-infrastructure path from PostgreSQL transactions to queryable 
data lakes in object storage.

### CloudNativePG Image Volume Extensions

Image Volume Extensions (CNPG 1.28+, Kubernetes 1.35+, PostgreSQL 18+) decouple extensions from base 
container images by mounting extension OCI images as read-only volumes at pod startup.

**New files:**
- `examples/cnpg/Dockerfile.extension` — Multi-stage build for pg_tide extension OCI image. Produces 
  standard layout: `/share/extension/` (control file + SQL migrations), `/lib/` (compiled `pg_tide.so`). 
  Supports `--build-arg PG_VERSION=18` for version targeting.
- `examples/cnpg/cluster-image-volume.yaml` — Complete CloudNativePG Cluster example using Image Volume 
  Extensions pattern. Mounts pg_tide extension volume alongside official CloudNativePG PostgreSQL image; 
  includes pg-tide relay sidecar.
- `examples/cnpg/IMAGE-VOLUMES.md` — Comprehensive guide covering: build, deployment, verification, 
  advanced topics (custom paths, system libraries, multi-extension images), extension updates, and 
  troubleshooting.

**Benefits:**
- **Immutable base images** — use official, minimal CloudNativePG PostgreSQL images
- **Simplified supply chain** — distribute only extension images; decouple from PostgreSQL version bumps
- **Enhanced security** — smaller base image footprint; read-only extension mounts
- **Flexible updates** — version extensions independently using declarative `Database` resources

**Backwards compatibility:** Existing sidecar pattern (`examples/cnpg/cluster.yaml`) remains 
fully supported for CNPG <1.28.

### RockLake Integration Phases 0–5

[RockLake](https://github.com/trickle-labs/rocklake) is a DuckLake catalog sidecar backed by 
SlateDB, a cloud-native embedded LSM storage engine. This release implements phases 0–5 of the 
RockLake integration, adding a `RockLakeSink` / `RockLakeSource` pair that operates on RockLake's 
bounded SQL subset (no `ON CONFLICT`, no `RETURNING`, no `nextval()`), enabling zero-infrastructure 
data lake ingestion from PostgreSQL outboxes into S3-backed catalogs.

**Phase 0: Wire corpus capture**
- Captured all SQL statement shapes emitted by current `DuckLakeSink` and `DuckLakeSource`
- Formatted as JSONL wire corpus: `tests/fixtures/wire-corpus/pgtide-rocklake-0.37.0.jsonl`
- Contributed to RockLake project as validation artifact

**Phase 1: `RockLakeSink` skeleton**
- New `pg-tide-relay/src/sink/rocklake.rs` (feature-gated `--features rocklake`)
- Replaces full `ensure_catalog()` with minimal `verify_catalog_ready()`: single `SELECT value FROM ducklake_metadata WHERE key = 'version'`
- Removes all RockLake-incompatible SQL: no `CREATE TABLE`, no `CREATE SEQUENCE`, no `nextval()`, no `ON CONFLICT`, no `RETURNING`

**Phase 2: Parquet write path**
- `RockLakeSink::publish()` for Parquet path
- Pre-allocates IDs from `ducklake_snapshot` → write Parquet to object storage → commit catalog rows in plain `BEGIN`/`COMMIT`
- No `nextval()`, no `RETURNING`, no `ON CONFLICT` — all operations RockLake-compatible

**Phase 3: Inlined data path**
- Inlined-data write path for batches ≤ `inline_row_limit`
- Direct `INSERT INTO ducklake_inlined_data_{table_id}_{schema_version}` without `ON CONFLICT`

**Phase 4: Schema evolution**
- Adapted existing `DuckLakeSink` schema evolution bridge for RockLake's bounded SQL subset
- Explicit `SELECT` → conditional `INSERT` pattern replaces `ON CONFLICT`

**Phase 5: Auto-partition via `ducklake_metadata`**
- Namespaced `ducklake_metadata` key/value entries (prefix `pg_tide.`) replace `tide.ducklake_partition_config` INSERTs
- Aligns with RockLake's native KV layout

**`RockLakeSource`**
- New `pg-tide-relay/src/source/rocklake.rs`
- Single non-JOIN query: `SELECT max(snapshot_id) FROM ducklake_snapshot WHERE snapshot_id > $1`
- Reads incremental data-file rows; delivers as `RelayMessage` objects

**Coordinator integration**
- Registers `"rocklake"` as valid `sink_type` and `source_type` in `coordinator.rs`
- Gated on `#[cfg(feature = "rocklake")]`

**Relationship to DuckLake (v0.34.0)**
- `DuckLakeSink` (v0.34.0) — PostgreSQL-backed DuckLake catalog; full SQL support
- `RockLakeSink` (v0.37.0) — RockLake PG-wire sidecar; bounded SQL subset
- Shared `ducklake_common` module for Parquet building and schema evolution
- Both paths support all source types: Kafka, NATS, Redis, SQS, RabbitMQ, webhook, stdin

---

## [0.36.0] — CLI Completeness, Test Coverage Depth & v1.0 Pre-Flight

This release completes the CLI surface, deepens integration test coverage, and performs final
v1.0 pre-flight clean-up including removal of deprecated positional API forms.

### Breaking change: positional relay SQL API forms removed

`tide.relay_set_outbox(text, text, text, jsonb, integer, boolean)` and
`tide.relay_set_inbox(text, text, jsonb, integer, text, boolean, integer, boolean)` have been
removed. These 6- and 8-parameter positional forms were deprecated in v0.34.0 when the unified
JSONB forms `relay_set_outbox_v2(jsonb)` and `relay_set_inbox_v2(jsonb)` were introduced.
The v2 forms are unchanged. Migrate any callers by wrapping your config payload in a single
`jsonb` argument.

### CLI: `pg-tide history --output json|table`

The `pg-tide history` command now accepts `--output table` (default, unchanged) or
`--output json` (emits a JSON array). This enables pipeline audit log consumption by external
scripts and monitoring tools without screen-scraping table output.

### AsyncAPI validate: distinct exit codes

`pg-tide asyncapi validate` now returns:
- **exit 0** — catalog matches spec exactly
- **exit 1** — channels present in spec but absent from live catalog (schema drift, ERROR)
- **exit 2** — live pipelines not present in spec (undocumented pipelines, WARNING)

This enables CI pipelines to differentiate schema drift (blocking) from undocumented pipelines
(advisory).

### DAG topology integration tests

Six new DAG topology integration tests cover: diamond topology, fan-out topology, mixed trigger
policies, self-loop rejection, two-node cycle rejection, and diamond back-edge rejection. All
tests run on the fully migrated schema via `install_full_schema`.

### KMS documentation

The `LocalKeyFile` AES-256-GCM provider (fully implemented in v0.35.0) is now documented in
`docs/src/relay-guide/configuration.md` with a provider availability table and example
configuration. Cloud providers (AWS KMS, GCP Cloud KMS, HashiCorp Vault) remain as stubs
returning `NotImplemented` until v1.0.0.

### KMS integration tests

New `kms_test.rs` integration tests (feature-gated `kms-local` and `kms-gcp`) cover:
key rotation across 50 messages, forward-secrecy, round-trip correctness, `LocalKeyFile`
availability, and GCP KMS startup guard (`is_available() = false`, `NotImplemented` errors,
`is_transient() = false`).

### Migration test chain

`migration_test.rs`, `sql_to_sink_e2e.rs`, and `common/mod.rs` now include the 0.35.0→0.36.0
migration script and verify that positional API functions are absent after the upgrade.

---

## [0.35.0] — Assessment-7 P1/P2 Bug Fixes, KMS Encryption & Fan-In Performance Hardening

This release addresses all P1 and P2 items identified in Overall Assessment 7: eliminates
`todo!()` panics in the KMS encryption layer, provides a complete LocalKeyFile AES-256-GCM
implementation with key rotation, hardens SQL functions against SQL injection and division-by-zero,
adds a delivery receipt background sweep, and implements the fan-in source type with UNNEST batch
offset commits.

### P1: KMS `todo!()` panic elimination

Previously, any pipeline configured with `kms_provider = "aws"`, `"gcp"`, or `"vault"` would
panic at runtime when the relay attempted to encrypt or decrypt a message. These panics have been
replaced with a `RelayError::NotImplemented` error that is logged at `WARN` and does not crash the
relay process. A new `is_available()` trait method provides a startup-time guard — pipelines with
unavailable KMS providers emit an `[INFO]` diagnostic and are skipped rather than panicking.

The `LocalKeyFile` provider is now fully implemented using AES-256-GCM (`aes-gcm 0.10`) with
SHA-256 key fingerprinting. Key rotation is supported via `key_path_previous` — the relay tries the
primary key first, then falls back to the previous key for decrypt, and always encrypts with the
primary key.

### P1: `relay_provision_tenant()` / `relay_deprovision_tenant()` role-name validation

The `EXECUTE format('CREATE ROLE %I …', p_db_role)` pathway now validates the supplied role name
against `[A-Za-z_][A-Za-z0-9_]{0,62}` before execution. A blocklist of reserved PostgreSQL and
pg_tide system roles (`postgres`, `pg_monitor`, `tide_admin`, etc.) is also checked, preventing
accidental privilege escalation.

### P2: `backfill_progress()` division-by-zero fix

`backfill_progress()` now returns `NULL` for `estimated_completion` when:
- `rows_processed = 0` (job just started),
- elapsed time < 1 second (throughput not yet measurable), or
- throughput is otherwise zero.

This replaces the previous `EXTRACT(epoch …) / 0` arithmetic error.

### P2: `relay_pipeline_dep_add()` SIMILAR TO validation

`trigger_policy` input validation was upgraded from `NOT LIKE 'on_offset_gte(%)'` (which accepted
`on_offset_gte(notanumber)`) to `SIMILAR TO 'always|on_idle|on_offset_gte\([0-9]+\)'`. A
corresponding `CHECK` constraint was also added to `relay_pipeline_deps.trigger_policy` as
defence-in-depth for direct SQL inserts.

### P2: Delivery receipt background sweep

A new SQL function `tide.relay_truncate_delivery_receipts(p_older_than INTERVAL DEFAULT '24 hours')`
deletes receipt rows older than the supplied retention interval and returns the deleted row count.
The relay coordinator now spawns a background sweep task that calls this function on a configurable
schedule (`--sweep-interval-hours` / `PG_TIDE_SWEEP_INTERVAL_HOURS`, default `24`).

The `pg-tide doctor` command reports a `[WARN]` when `relay_delivery_receipts` exceeds 1,000,000
rows so operators are alerted before storage becomes a concern.

### P2: Fan-in source type with UNNEST batch offset commits

The relay coordinator now recognises `source_type = "fanin"`, loading contributing outbox names
from `tide.relay_fanin_config`. On each `acknowledge()`, all per-member offsets are committed in a
single `INSERT … ON CONFLICT … DO UPDATE` using `UNNEST` arrays, replacing N sequential round-trips
with one. A unique partial index `uq_relay_consumer_offsets_fanin` on
`(relay_group_id, pipeline_id, fanin_member) WHERE fanin_member IS NOT NULL` supports the ON
CONFLICT target.

### `pg-tide dag show` — paused-node styling and JSON output

`pg-tide dag show` now:
- Marks disabled pipelines with `:::paused` class and adds `classDef paused fill:#f99,stroke:#c33`
  (red nodes in Mermaid).
- Accepts `--format json` for programmatic adjacency-list output
  `{"nodes":[...],"edges":[...]}`.

---

## [0.34.0] — Universal Reverse Pipeline Sinks & DuckLake Ecosystem Completeness

This release completes the relay's universal routing capability by registering all eight analytics and reverse-pipeline sinks in `build_sink()`, and delivers the DuckLake ecosystem completeness initiative with multi-engine compatibility guides and CI validation.

### Universal reverse pipeline sinks

Previously, eight production-ready sink implementations existed in `pg-tide-relay/src/sink/` but were not registered in the `build_sink()` factory function — meaning they could not be selected by any pipeline configuration. This release wires all of them up:

- **DuckLake** (`type = "ducklake"`) — writes Parquet files to local/S3/GCS/Azure object storage and commits snapshots to a DuckLake v1.0 PostgreSQL catalog. Requires `catalog_connection` (a PostgreSQL URL for the catalog database) and `storage_provider` (`local`, `s3`, `gcs`, or `azure`). Feature-gated: `ducklake`.
- **ClickHouse** (`type = "clickhouse"`) — HTTP-native insert to a ClickHouse cluster. Requires `url`, `database`, `table`, and `api_key` config fields. Feature-gated: `clickhouse`.
- **MongoDB** (`type = "mongodb"`) — inserts message documents into a MongoDB collection. Requires `uri` and `collection`. Feature-gated: `mongodb`.
- **BigQuery** (`type = "bigquery"`) — streaming inserts to a BigQuery table via the REST insertAll API. Requires `project_id`, `dataset_id`, `table_id`, and `api_key`. Feature-gated: `bigquery`.
- **Snowflake** (`type = "snowflake"`) — row inserts via the Snowflake REST API. Requires `account_identifier`, `database`, `schema`, `table`, `user`, and `api_key`. Feature-gated: `snowflake`.
- **Delta Lake** (`type = "delta"`) — writes Parquet data files and Delta Log commit JSON entries to any object store. Uses the shared `build_object_store_from_pipeline()` helper to resolve the `storage_provider`. Feature-gated: `delta`.
- **Apache Iceberg** (`type = "iceberg"`) — writes Parquet data files and Iceberg snapshot metadata JSON to any object store. Feature-gated: `iceberg`.
- **Remote pg-tide inbox** (`type = "pg_outbox"`) — routes messages from one pg-tide deployment's outbox directly into another deployment's inbox, enabling inbox-to-inbox fan-out across PostgreSQL instances. No feature gate required.

All eight sinks are covered by new integration tests in `pg-tide-relay/tests/build_sink_registration_test.rs` (18 tests total, 0 skipped). The Delta Lake and Apache Iceberg sinks are exercised with real local filesystem round-trip tests writing 50 messages and verifying the `_delta_log/` and `metadata/` directory structure. The `pg_outbox` sink is exercised with a full round-trip of 50 messages against a live PostgreSQL testcontainer, including deduplication verification.

The new `build_object_store_from_pipeline()` helper (feature-gated on `delta | iceberg | ducklake`) reads the `storage_provider` key from the sink config and constructs an `Arc<dyn ObjectStore>` for local, S3, GCS, or Azure backends.

### DuckLake ecosystem completeness

New documentation in `docs/src/guides/ducklake/`:

- **[`ecosystem-compatibility.md`](docs/src/guides/ducklake/ecosystem-compatibility.md)** — compatibility matrix covering DuckDB, DataFusion, Apache Spark, Trino, and pandas+DuckDB against all four storage backends (local, S3, GCS, Azure). Includes ready-to-run time-travel query examples for each engine.
- **[`datafusion.md`](docs/src/guides/ducklake/datafusion.md)** — DataFusion quick-start: resolving Parquet paths from the DuckLake catalog, reading snapshots with PyArrow, and performing time-travel by snapshot ID.
- **[`spark.md`](docs/src/guides/ducklake/spark.md)** — Spark quick-start: PySpark Parquet read from DuckLake catalog paths, time-travel via snapshot ID, and a polling-based micro-batch streaming pattern.
- **[`trino.md`](docs/src/guides/ducklake/trino.md)** — Trino quick-start: Hive connector configuration, Python-based catalog sync script, and SQL time-travel via versioned external tables.
- **[`pandas.md`](docs/src/guides/ducklake/pandas.md)** — Pandas quick-start: three patterns — DuckDB-native (recommended), PyArrow direct, and incremental snapshot polling with a live watch loop.

### CI additions

- **`ducklake-compat` CI job** — new GitHub Actions job (`ducklake-compat`) in `.github/workflows/ci.yml` spins up a PostgreSQL service container, installs DuckDB + psycopg2, and runs `scripts/ducklake_compat_smoke.py`. The smoke test writes two synthetic Parquet files (25 rows each), registers them in a minimal DuckLake catalog schema, then verifies DuckDB can read all 50 rows, confirms dedup key uniqueness, validates time-travel by snapshot, and optionally tests `postgres_scan` catalog access.

### Migration

- **`sql/pg_tide--0.33.0--0.34.0.sql`** — upgrades the extension comment from `v0.33.0` to `v0.34.0`. No schema changes are required; all relay changes are binary-only.

---

## [0.33.0] — 2026-05-20 — Pre-GA Supply-Chain Hardening, KMS Foundation & v1.0 Readiness

This release closes all remaining assessment-6 "Must-do" and "Should-do" items, establishes the formal v1.0.0 stability contract, and lays the structural groundwork for envelope encryption. The nine `cargo audit` ignores in `audit.toml` are formally re-evaluated against the v0.33.0 dependency tree and all confirmed to affect optional or test-only paths. ADR-010 specifies the full KMS envelope encryption design (AES-256-GCM, four provider backends, DEK caching, key rotation policy). The SQL schema gains a `tide.outbox_encryption_config` table and a SQL function skeleton ready for the v1.0.0 implementation. The relay gains a feature-gated `EncryptionEnvelope` async trait and provider stubs in `encryption.rs` behind the `kms` Cargo feature.

The v1.0.0 stability contract is now formally documented in `docs/src/stability-guarantees.md`: stable SQL function signatures (`_v2` forms), catalog table columns, Prometheus metric names, configuration keys, and wire format schemas. The v0.x → v1.0.0 migration guide is rewritten comprehensively with a 5-step rolling upgrade procedure, rollback instructions for both unencrypted and encrypted deployments, a feature compatibility matrix, and a deprecation schedule.

The relay CLI gains two new flags: `--inbox-summary` on the `status` subcommand (renders an inbox fleet table by calling `tide.inbox_status(NULL)`) and `--expect-extension-version` on the top-level binary (pre-flight check that the installed extension meets a minimum version). The Grafana relay-health dashboard gains an "Inbox Fleet" row and table panel. The justfile gains `just check-stability` (validates metric names and schema annotations match the stability contract) and `just release-notes-ga` (generates a Production GA announcement with stability guarantee, breaking changes, and migration guide link). Monitoring documentation is updated with an inbox fleet status performance contract (O(n) SQL length, ≥ 60 s panel refresh guidance). The migration test suite is extended to cover the v0.32.0 → v0.33.0 upgrade script.

### Supply-chain hardening

- **`audit.toml` re-evaluation** — all nine `cargo audit` ignore entries (RUSTSEC-2026-0119, -0118, -0104, -0098, -0099, -0049, RUSTSEC-2024-0436, RUSTSEC-2025-0134, RUSTSEC-2021-0127) are re-evaluated against the v0.33.0 dependency tree. Each entry now carries a dated re-evaluation comment confirming the affected crate is optional or test-only, with explicit confirmation that the `kms*` features are not enabled in the default release binary.

### KMS envelope encryption foundation (ADR-010)

- **ADR-010: Envelope encryption — KMS-backed AES-256-GCM** — `docs/adr/adr-010-envelope-encryption-kms.md` specifies the full encryption design: JSON envelope format `{_enc:1, kms, kid, alg, iv, edek, ct}`, four provider backends (AWS KMS, GCP Cloud KMS, HashiCorp Vault, local key file), DEK caching to amortise KMS round-trips across messages within a relay batch, key rotation by setting `next_key_id` in the outbox catalog config, and the security rationale for relay-side encryption (option C).
- **`tide.outbox_encryption_config` SQL table** — `sql/pg_tide--0.32.0--0.33.0.sql` creates `tide.outbox_encryption_config(outbox_name, kms_provider, key_id, algorithm, created_at, updated_at)` and a `tide.outbox_encryption_config(name text, ...)` SQL function skeleton. The function emits `NOTICE 'KMS encryption for outbox ... will be active from v1.0.0'` to make the feature's v1.0.0 timeline explicit. The migration is registered in `lib.rs` as `pg_tide_m_0_33`.
- **`encryption.rs` trait skeleton** — `pg-tide-relay/src/encryption.rs` defines the `EncryptedPayload` struct (serde JSON envelope), `EncryptionEnvelope` async trait, four provider structs (`AwsKms`, `GcpKms`, `VaultKms`, `LocalKeyFile`) with `todo!()` implementations, and `is_encrypted_envelope()` detection helper. The module is gated on `#[cfg(feature = "kms")]`. Four new Cargo features are declared: `kms`, `kms-aws`, `kms-gcp`, `kms-vault`, `kms-local`.

### v1.0.0 readiness documentation

- **`docs/src/stability-guarantees.md`** — new document formally defining the v1.0.0+ stability contract: stable SQL function signatures, catalog table columns, Prometheus metric names, TOML/JSONB config keys, wire format schemas. Includes tables of what is stable, what is not stable, and the deprecation policy with timelines.
- **`docs/src/operations/v1-migration-guide.md`** — comprehensive rewrite of the v0.x → v1.0.0 upgrade guide. Covers: breaking changes (positional SQL API removal, KMS envelope format), a 5-step rolling upgrade procedure with health-check commands at each step, rollback procedures for both unencrypted and encrypted outbox deployments, feature compatibility matrix (v0.25.0 through v0.33.0), deprecation schedule, and a "What is NOT in v1.0.0" section to set expectations.
- **`docs/src/operations/pre-ga-checklist.md`** — new "v1.0.0 GA Acceptance Criteria" section listing all assessment-6 "Must-do" and "Should-do" items with resolution versions.

### CLI additions

- **`pg-tide status --inbox-summary`** — the `status` subcommand gains an `--inbox-summary` boolean flag. When set, a fleet-wide inbox summary table is printed after the pipeline status table. The table is populated by calling `tide.inbox_status(NULL)` and rendered with column headers: inbox name, pending, processing, processed, failed, dlq.
- **`pg-tide --expect-extension-version`** — the top-level binary gains `--expect-extension-version <VERSION>` (also readable from `PG_TIDE_EXPECT_EXTENSION_VERSION`). During `--self-test`, step 6 queries `pg_extension` for the installed `pg_tide` version and verifies it meets the requested minimum using numeric semver comparison. Reports `[PASS]` or `[FAIL]` and exits non-zero if the constraint is not met.

### Observability

- **Grafana Inbox Fleet panel** — `pg-tide/dashboards/relay-health.json` gains an "Inbox Fleet" row header (id=214) and a table panel (id=215) with the SQL query `SELECT * FROM tide.inbox_status(NULL)`, 60 s refresh interval, and threshold-based colour coding for pending messages.
- **Inbox fleet status performance contract** — `docs/src/operations/monitoring-cookbook.md` is extended with an "Inbox Fleet Status Performance Contract" section documenting O(n) SQL query growth, recommended use cases (Grafana dashboard at ≥ 60 s refresh, `pg-tide status --inbox-summary` on demand), and anti-patterns (per-message loops, high-frequency polling < 5 s). The per-inbox variant `tide.inbox_status('name')` is always O(1).

### Automation

- **`just check-stability`** — new recipe that validates: all `#[pg_extern]` functions carry `schema = "tide"`, the 12 stable Prometheus metric names are present in `metrics.rs`, `docs/src/stability-guarantees.md` exists, and `docs/src/operations/v1-migration-guide.md` exists. Exits non-zero on any failure.
- **`just release-notes-ga`** — new recipe that generates a Production GA announcement with the full stability guarantee, breaking changes section, migration guide link, headline feature list (30 sink backends, 16 source backends, KMS encryption, DuckLake, pipeline DAG, multi-tenant groups, partitioning), and the final tagging command.

### Migration test & CI updates

- **Migration test extended to v0.33.0** — `V0_32_0_TO_0_33_0` constant and `("0.32.0 → 0.33.0", ...)` entry added to `pg-tide-relay/tests/migration_test.rs`. The `migration-test-currency` CI job validates the chain stays current.

---

## [0.32.0] — 2026-05-20 — Performance Engineering, Code Internals Quality & Benchmark Hardening

This release performs a comprehensive performance engineering sweep across both the extension and relay hot paths. The publisher-ACL authorization path in `outbox_publish()` has been issuing three sequential SPI round-trips on every call since the ACL system was introduced in v0.13.0. A single CASE-expression query now covers all three authorization decisions in one round-trip, reducing SPI overhead for ACL-checked publishes by approximately 60%. The inbox fleet summary (`tide.inbox_status(NULL)`) had an N+1 query problem: 20 configured inboxes produced 21 SPI calls. This is now a fixed 2-round-trip operation using a dynamically assembled UNION ALL query, with explicit error propagation instead of silently returning empty results on SPI failures.

The relay codebase achieves full `lint-expect` compliance with the replacement of the last bare `.expect()` call in the webhook HMAC path with `.unwrap_or_else(|_| unreachable!())`, accompanied by the appropriate `// SAFETY:` comment documenting why the branch is provably unreachable. The coordinator's secrets-logging serialization fallback now returns `"{}"` (a valid JSON object) instead of `""` (an empty string that is not valid JSON for a config object).

The Criterion benchmark suite gains a new `bench_consumer_group_poll_decode` benchmark covering the consumer-group polling path at 1 KB, 10 KB, and 100 KB payload sizes, providing a regression gate for the more complex consumer-group decode path that was previously unmeasured. The `bench_inbox_unnest_params` benchmark is extended to include a 500-row batch size alongside the existing 1/10/100/1000 sizes.

The v0.32.0 release also ships the WAL logical-replication source groundwork: a `PgLogicalSource` proof-of-concept behind a `wal-source` feature flag that validates the `tokio-postgres` replication connection lifecycle, slot creation, and LSN-to-offset mapping strategy. ADR-009 documents the full design, open questions, and the tradeoffs between ephemeral and permanent slots, establishing the design contract for the v1.1.0 full implementation.

### Extension performance improvements

- **Publisher-ACL SPI consolidation** — the three sequential SPI calls in `outbox_publish_impl()` (count publishers, check rolsuper, check allowed) are replaced with a single `CASE` expression query that returns `'no_acl'`, `'superuser'`, `'allowed'`, or `'denied'` in one round-trip. For high-frequency event streams with ACLs enabled, this reduces authorization overhead from 3 SPI calls to 1 per publish — approximately 60% fewer round-trips on the hot publish path. ACL semantics are unchanged.
- **`inbox_status()` fleet summary N+1 elimination** — `inbox_status_impl()` in fleet mode (called as `tide.inbox_status(NULL)`) previously issued one `COUNT(*)` query per configured inbox. With 20 inboxes that was 21 SPI calls. The implementation now assembles a single `UNION ALL` query across all inbox tables and executes it in one round-trip, reducing the total SPI calls to 2 regardless of inbox count. The improvement is proportional: 50 inboxes drops from 51 calls to 2.
- **Fleet `inbox_status()` SPI error propagation** — the `Spi::connect()` call in the fleet path previously used `.unwrap_or_default()`, silently returning an empty inbox list if the SPI connection failed. It now returns a `PgTideError::SpiError` to the SQL caller, making catalog access failures visible instead of silently returning `{"inboxes":[]}`.

### Relay binary improvements

- **Webhook HMAC `expect()` → `unreachable!()`** — the last bare `.expect()` call in production relay code (`<Hmac<Sha256>>::new_from_slice(key.as_bytes()).expect("HMAC accepts any key size")`) is replaced with `.unwrap_or_else(|_| unreachable!())` with a `// SAFETY: HMAC-SHA256 accepts any key length (RFC 2104 §3); this branch is unreachable.` comment. This achieves full `just lint-expect` compliance across all production relay source files.
- **Coordinator secrets-logging `unwrap_or_default` fix** — `serde_json::to_string(&mask_secrets_for_logging(&pipeline.config)).unwrap_or_default()` is replaced with `.unwrap_or_else(|_| "{}".to_string())`. The previous form returned `""` on serialization failure (an invalid JSON string); the new form returns `"{}"` (a valid empty JSON object), avoiding confusing log entries when logging pipeline config during startup errors.

### New benchmarks

- **`bench_consumer_group_poll_decode`** — new Criterion benchmark covering the consumer-group polling path decode step with 1 000-row batches at 1 KB, 10 KB, and 100 KB payload sizes. This path was previously unmeasured; the benchmark establishes a baseline for the `poll_consumer_group()` code path and serves as a regression gate alongside the existing `poll_simple` benchmark.
- **`bench_inbox_unnest_params` extended** — the `bench_inbox_unnest_params` benchmark now covers batch sizes of 1, 10, 100, **500**, and 1 000 rows. The 500-row point was previously missing from the scaling profile, leaving a gap in the sub-linear throughput assertion.

### WAL logical-replication source groundwork (`wal-source` feature)

- **`PgLogicalSource` proof-of-concept** — a new `pg-tide-relay/src/source/pg_logical.rs` module implements `PgLogicalSource` behind the `wal-source` Cargo feature flag. The spike validates: replication connection establishment (appending `replication=database` to the connection URL), temporary logical replication slot creation (`CREATE_REPLICATION_SLOT … TEMPORARY LOGICAL pgoutput`), LSN parsing and tracking, and `RelayMessage` emission equivalent to `OutboxPollerSource` for INSERT events. Not enabled in default CI; skipped unless `--features wal-source` is specified.
- **ADR-009: WAL logical-replication source** — `docs/adr/adr-009-wal-logical-replication-source.md` documents the design decisions: replication slot lifecycle (ephemeral vs. permanent), LSN-to-consumer-offset mapping strategy, at-least-once delivery guarantee via standby status updates, interaction with outbox table partitioning (requiring `publish_via_partition_root = true`), advisory lock coordination, and a comparison table of polling source vs. WAL source tradeoffs. The ADR is a prerequisite for the full v1.1.0 implementation.
- **Unit tests for WAL source** — `pg_logical.rs` includes `#[cfg(all(test, feature = "wal-source"))]` unit tests for `parse_lsn()` (zero, simple, high-word, and error cases) and the replication URL construction logic.

### Migration test & CI updates

- **Migration test extended to v0.32.0** — `V0_31_0_TO_0_32_0` constant and `("0.31.0 → 0.32.0", ...)` entry added to `pg-tide-relay/tests/migration_test.rs` and `common/mod.rs`. The `migration-test-currency` CI job validates the chain stays current.
- **Schema-diff CI updated** — `schema-diff` job now applies `sql/pg_tide--0.31.0--0.32.0.sql` in both the fresh-install and upgrade chains.

### No SQL schema changes

v0.32.0 contains no DDL changes. The upgrade script `sql/pg_tide--0.31.0--0.32.0.sql` only updates the extension version comment. All improvements are in the extension's Rust hot paths and the relay binary.

---

## [0.31.0] — Assessment-6 P1/P2 Bug Fixes, Identifier Quoting Hardening & Release-Process Automation

Every outbox and inbox name that contains a hyphen — a natural choice for system architects who follow kebab-case naming conventions — produced a silent SQL syntax error in two relay code paths. The `PgInboxSink` used an unquoted table reference (`tide.order-events_inbox`) when constructing its UNNEST batch insert, causing PostgreSQL to reject the statement with an operator-not-found error. The `poll_simple()` source path had the same problem: `SELECT id, payload FROM tide.outbox_order-events WHERE id > $1` was rejected by the parser because the hyphen was treated as arithmetic subtraction. Any cross-database inbox delivery pipeline or any relay pipeline targeting a hyphenated outbox was silently broken for the entire v0.23.0 release series. Both identifiers are now properly double-quoted, matching the convention already applied by the local `InboxSink` that has been correct since v0.13.0.

To prevent this class of regression from re-entering the codebase, this release adds a `lint-quoting` CI recipe and GitHub Actions job that flags any `format!()` call producing an unquoted `tide.{ident}` SQL pattern in production relay code. Exempt sites are annotated with a `// QUOTED:` comment that documents the upstream validation guaranteeing the identifier is safe. The check runs on every pull request, making SQL identifier injection and hyphen-in-name failures statically detectable before merge.

Two release-process gaps from overall-assessment-6 are also closed. The migration test chain was extended to cover v0.30.0→v0.31.0, and a `migration-test-currency` CI job now fails the build if the migration test does not cover the current workspace version, making future migration gaps self-detecting. The `just bump-version` recipe already updates `Cargo.toml`, `pg_tide.control`, and `helm/pg-tide/Chart.yaml` atomically; this release verifies it works correctly for v0.31.0.

### Relay binary fixes

- **`PgInboxSink` table identifier now double-quoted** — the INSERT SQL in `pg-tide-relay/src/sink/pg_outbox.rs` now uses `tide."<inbox_table>"` instead of `tide.<inbox_table>`, preventing SQL syntax errors for inbox names containing hyphens (e.g. `order-events`). Any cross-database inbox delivery pipeline using a hyphenated inbox name has been silently broken since v0.23.0; this fix restores correct operation.
- **`poll_simple()` outbox identifier now double-quoted** — the SELECT SQL in `pg-tide-relay/src/source/outbox.rs` now uses `tide."<outbox_table_name>"`, preventing SQL syntax errors for outbox names containing hyphens.
- **`fetch_claim_check_rows()` delta table identifier now double-quoted** — the cursor DECLARE in the claim-check polling path now uses `tide."outbox_delta_rows_<name>"`, closing the last unquoted identifier in the relay source.

### New tests

- **Hyphenated inbox name integration test** — `tests/pg_inbox_sink_test.rs` now includes `test_pg_inbox_sink_hyphenated_name()`: creates `tide."order-events_inbox"`, publishes 20 messages via `PgInboxSink`, and asserts correct column values and deduplication. This permanently guards the quoting fix.
- **Quoted SQL generation unit tests** — three `#[cfg(test)]` unit tests in `source/outbox.rs` assert that `poll_simple()` and `fetch_claim_check_rows()` produce syntactically valid, double-quoted SQL for both plain and hyphenated outbox names.

### CI / release-process improvements

- **`lint-quoting` justfile recipe** — scans `pg-tide-relay/src/` for unquoted `tide.{ident}` SQL format patterns; fails on any unquoted site not annotated with `// QUOTED:`.
- **`lint-quoting` CI job** — runs the `lint-quoting` check on every pull request, making the regression class statically detectable before merge.
- **`migration-test-currency` CI job** — asserts that `migration_test.rs` contains the expected upgrade label for the current workspace version; fails with a descriptive error and remediation instructions if the migration test lags behind the workspace version.
- **Migration test extended to v0.31.0** — `V0_30_0_TO_0_31_0` constant and `("0.30.0 → 0.31.0", ...)` entry added to `migration_test.rs` and `common/mod.rs`.
- **Schema-diff CI updated** — `schema-diff` job now applies `sql/pg_tide--0.30.0--0.31.0.sql` in both the fresh-install and upgrade chains.

### No SQL schema changes

v0.31.0 contains no DDL changes. The upgrade script `sql/pg_tide--0.30.0--0.31.0.sql` only updates the extension version comment. All fixes are in the relay binary.

---

## [0.30.0] — Pipeline Dependency DAG, AsyncAPI Completeness & Pre-GA Final Hardening

Complex event-driven systems rarely consist of a single pipeline working in isolation — one pipeline loads raw data, another enriches it, a third aggregates it, and a fourth forwards it to a reporting dashboard. Until now, operators had to coordinate the startup and pacing of these pipelines by hand, accepting that a downstream consumer might start running before the upstream data it depends on was actually ready. This release introduces a declarative dependency graph for pipelines: operators state which pipelines must be idle, fully caught up, or ahead of a specific message count before a downstream pipeline is allowed to start. A `pg-tide dag show` command renders the entire graph as a Mermaid diagram so the ordering is visible at a glance, and `pg-tide dag check` detects circular dependencies and exits with an error before they can cause a silent data-ordering problem in production.

This is the last feature release before pg_tide reaches its v1.0.0 general availability milestone, and it closes every remaining item on the production readiness checklist. Wire format decoders have been fuzz-tested with thousands of randomly mutated inputs to surface edge cases that no manually written test would ever reach. A sustained load test confirms the relay handles fifty thousand messages across ten outboxes without throughput degradation. High-availability failover is validated end-to-end by an integration test that crashes the active relay and asserts that a standby takes over without message loss. A complete migration guide and formal scope document give teams a clear picture of what is changing and what is not when they upgrade to v1.0.0, including advance warning that the older positional pipeline registration functions are now deprecated and will be removed in the GA release.

v0.30.0 is the last feature release before v1.0.0 GA. It closes all remaining
pre-GA items: pipeline dependency ordering, a complete AsyncAPI 3.0 catalog
export, fuzz-tested wire format decoders, HA failover validation, and the v1.0.0
scope finalization documents.

**Pipeline Dependency DAG** — operators can now declare ordered relationships
between pipelines using `tide.relay_pipeline_dep_add()`. A DAG-aware coordinator
checks upstream consumer lag and trigger policies (`always`, `on_idle`,
`on_offset_gte(N)`) before acquiring each pipeline, ensuring downstream consumers
only start when upstreams are ready. The `pg-tide dag show/check/status`
subcommands visualise and validate the graph.

**AsyncAPI 3.0 Completeness** — `pg-tide asyncapi export` now emits delivery
receipt channels for every pipeline, fan-in message schemas with `oneOf` for
contributing outboxes, and template-sourced channel descriptions.

**Pre-GA Hardening** — fuzz corpus covering all seven wire format decoders, a
sustained-throughput load test (50 000 messages / 10 outboxes), an HA failover
integration test, DAG integrity in `--self-test`, and the v1.0.0 migration guide
and scope document.

**Deprecation warnings active** — `tide.relay_set_outbox()` and
`tide.relay_set_inbox()` positional forms now emit a PostgreSQL `WARNING` on
every call. Migrate to `relay_set_outbox_v2()` / `relay_set_inbox_v2()` before
v1.0.0 which removes the positional forms entirely.

### SQL changes
- New table: `tide.relay_pipeline_deps(upstream, downstream, trigger_policy, created_at)`
- New function: `tide.relay_pipeline_dep_add(upstream, downstream, trigger_policy)`
- New function: `tide.relay_pipeline_dep_drop(upstream, downstream)`
- New function: `tide.relay_dag_check() → TABLE(cycle_path TEXT[])`

### New CLI subcommands
- `pg-tide dag show` — Mermaid diagram of the pipeline dependency graph
- `pg-tide dag check` — cycle detection (exit 1 on cycle)
- `pg-tide dag status` — per-edge upstream lag and gate state

### Documentation
- `docs/src/operations/v1-migration-guide.md` — v1.0.0 breaking changes and upgrade steps
- `docs/src/v1-scope.md` — formal v1.0.0 feature freeze announcement

---

## [0.29.0] — Pipeline Templates, Multi-Outbox Fan-In, Lifecycle Management & Backfill Completion

This release is about making pg_tide feel like a polished product for teams running it in production every day. Rather than assembling pipelines from scratch each time, operators can now pick from a library of ready-made pipeline templates — pre-built recipes for common integration patterns like mirroring data to Kafka, sinking events into a data lake, or broadcasting to multiple services at once. A few commands and a handful of overrides are all it takes to go from zero to a running pipeline, dramatically cutting the time it takes to connect a new service or data destination. Five templates ship out of the box, and teams can add their own to the library for reuse across projects.

For teams that need to aggregate events from many sources into a single destination, this release introduces fan-in pipelines that merge messages from multiple outboxes into one coordinated stream with configurable merge strategies. Operators also gain a complete audit trail of every configuration change ever made to a pipeline, plus the ability to set pipelines to automatically resume after a pause — so a brief connectivity blip no longer requires a manual intervention in the middle of the night. Large backfill jobs round out the release: operators can now pause, resume, or cancel them and watch progress tick toward completion with a linear estimated-completion time displayed in real time.

v0.29.0 delivers four major capability areas for production operators: a built-in pipeline template library for fast pipeline instantiation, multi-outbox fan-in coordination, full pipeline lifecycle history and auto-resume, and a complete relay-side backfill worker with progress tracking and CLI controls. All changes are backward-compatible.

### Pipeline Template Library

- **`tide.relay_pipeline_templates` catalog table** — stores named JSON templates with `{{placeholder}}` substitution, `description`, and `required_keys` validation.
- **`tide.relay_template_create()` / `tide.relay_template_drop()`** — CRUD functions for managing custom templates.
- **`tide.relay_template_validate(name, overrides)`** — dry-run validation returning missing or invalid keys before instantiation.
- **`tide.relay_set_outbox_from_template()` / `tide.relay_set_inbox_from_template()`** — instantiate a pipeline by merging template defaults with caller-supplied overrides, then calling `relay_set_outbox_v2()` / `relay_set_inbox_v2()`.
- **5 built-in templates** pre-seeded: `kafka-topic-mirror`, `ducklake-daily-sink`, `nats-jetstream-fanout`, `pg-inbox-relay`, `webhook-notification`.
- **`tide.relay_template_list()` / `tide.relay_template_get(name)`** — Rust-backed list and lookup helpers.
- **CLI**: `pg-tide template list`, `pg-tide template show <name>`, `pg-tide template apply <name> --outbox <outbox> --set key=value …`.

### Multi-Outbox Fan-In Pipelines

- **`tide.relay_fanin_config` catalog table** — stores fan-in pipeline definitions with `outbox_names[]`, `sink_type`, `merge_strategy` (`round_robin` | `priority` | `subject_hash`), and `tenant_name`.
- **`tide.relay_set_fanin(name, outbox_names, sink_type, config)`** — register or update a fan-in pipeline; validates all named outboxes exist.
- **`tide.relay_fanin_enable()` / `tide.relay_fanin_disable()` / `tide.relay_fanin_delete()`** — lifecycle management for fan-in pipelines.
- **`tide.relay_fanin_list()`** — returns all fan-in configs as a JSON array.
- **`fanin_member TEXT` column** on `tide.relay_consumer_offsets` — enables independent offset tracking per source outbox within a fan-in pipeline.
- **New Prometheus metrics**: `pg_tide_relay_fanin_source_lag{pipeline, outbox}` gauge and `pg_tide_relay_fanin_messages_merged_total{pipeline, outbox}` counter.
- **Grafana panels**: "Fan-In Sources" table (per-source lag) and "Fan-In Messages Merged / sec" timeseries added to `relay-health.json`.

### Pipeline Lifecycle Management

- **`tide.relay_config_audit` table** — immutable log of every pipeline config change; populated by triggers on `relay_outbox_config` and `relay_inbox_config`.
- **`tide.relay_config_history(pipeline_name)`** — SQL function returning the ordered change log with `old_config`, `new_config`, `changed_by`, and `changed_at`.
- **`tide.relay_pipeline_state` table** — runtime pause/resume state written by the relay coordinator on every worker transition (`last_error`, `error_class`, `pause_started_at`, `failure_count`).
- **`tide.relay_pipeline_state_upsert()`** — Rust-backed helper for the relay to write pause state.
- **`tide.relay_pipeline_pause_reason(pipeline_name)`** — query function returning current pause state.
- **`auto_resume_after INTERVAL` column** on `tide_outbox_config` and `tide_inbox_config` — when set, the coordinator automatically re-enables paused pipelines after the interval elapses.
- **`tide.relay_auto_resume_candidates()`** — returns pipelines currently eligible for auto-resume.
- **CLI**: `pg-tide history <pipeline> [--limit N] [--since TIMESTAMP]`.

### Managed Backfill Completion

- **`tide.backfill_cancel(job_name)`** — SQL and Rust function to cancel a pending, running, or paused backfill job permanently.
- **`tide.backfill_progress(job_name)`** — SQL function returning `(rows_processed, total_rows, pct_complete, estimated_completion, status)` with linear ETA projection.
- **CLI**: `pg-tide backfill pause|resume|cancel <job-name>` and `pg-tide backfill status [<job-name>]`.
- **Grafana panel**: "Backfill Jobs" table panel added to `relay-health.json` showing active jobs with progress and estimated completion.

---

## [0.28.0] — Delivery Receipts, Canonical Config & Native Claim-Check

Knowing that a message was published is useful; knowing that it actually arrived at its destination is transformational. Version 0.28.0 introduces delivery receipts — a permanent, queryable record written every time the relay successfully delivers a message to a sink. Whether you are auditing a financial transaction stream, debugging a missed notification, or satisfying a compliance requirement, you can now look up exactly when and where each message was delivered. A new Prometheus metric makes it equally easy to alert on receipt lag before it becomes a problem, and the `pg-tide doctor` command now verifies that the relay has the privileges it needs to write receipts before you deploy.

This release also makes configuration management dramatically simpler for teams running pg_tide at scale. The relay can now run entirely off the SQL catalog — no TOML files to distribute, no configuration drift between pods, just a single source of truth in the database. A migration helper turns an existing TOML file into the SQL commands needed to populate the catalog, and the upgrade takes minutes. Finally, applications that occasionally need to publish very large payloads no longer need a separate infrastructure layer: pg_tide transparently stores oversized messages in PostgreSQL's built-in large-object storage and fetches them at delivery time, then reclaims the storage automatically after the message is acknowledged.

v0.28.0 completes end-to-end delivery accountability for every outbox message,
introduces catalog-first configuration as a first-class deployment mode, and
adds native large-payload support via PostgreSQL `pg_largeobject` — all with
zero breaking changes to existing pipelines.

### Highlights

**Delivery receipts (v0.28.0)**
- New `tide.relay_delivery_receipts` table records every successfully delivered
  message with `pipeline_name`, `outbox_name`, `message_id`, `dedup_key`,
  `delivered_at`, `sink_type`, and `tenant_name`.
- New SQL functions `tide.outbox_delivery_confirm()` and
  `tide.relay_truncate_delivery_receipts()` for querying and pruning receipts.
- New Prometheus metric `pg_tide_relay_receipts_written_total` (labels:
  `pipeline`, `sink_type`, `tenant`) lets you alert on receipt lag.
- `pg-tide doctor` now checks INSERT privilege on the receipts table.

**Canonical catalog-first configuration (v0.28.0)**
- New `--config-mode` CLI flag (`PG_TIDE_CONFIG_MODE` env var) with values
  `toml_allowed` (default, backward-compatible) and `catalog_only`.
- In `catalog_only` mode the TOML file is ignored entirely; all pipeline config
  is loaded from the `tide` catalog at runtime.
- New `pg-tide migrate-config` subcommand prints the SQL needed to seed the
  catalog from an existing TOML file.
- Migration guide: [docs/src/relay-guide/config-migration.md](../relay-guide/config-migration.md).

**Native claim-check via pg_largeobject (v0.28.0)**
- New `tide.outbox_publish_large(name, payload, dedup_key, threshold_bytes)`
  transparently stores oversized payloads as PostgreSQL large objects and
  inserts a claim-check envelope into the outbox table.
- The relay source automatically fetches the real payload via `lo_get()` before
  forwarding to the sink, then calls `lo_unlink()` after the ack to reclaim
  storage — no application changes needed.
- Architecture rationale: [ADR-008](../../adr/adr-008-claim-check-native-pathway.md).
- `pg-tide doctor` now checks EXECUTE privilege on `lo_get`.

**Per-tenant DB roles (v0.28.0)**
- New `tide.relay_tenant_roles` catalog table stores tenant → database role
  mappings provisioned by `tide.relay_provision_tenant()` /
  `tide.relay_deprovision_tenant()`.
- A `db_role TEXT` column is added to both `tide_outbox_config` and
  `tide_inbox_config` so the relay can issue `SET ROLE` for each pipeline
  worker, enabling row-level security by tenant.

### Upgrade notes

Run the standard upgrade script:

```sql
\i pg_tide--0.27.0--0.28.0.sql
```

No changes to existing outbox/inbox tables or polling semantics. The new
tables and functions are additive.

---

## [0.27.0] — 2026-05-20 — Observability Expansion, CLI Ergonomics & Pre-GA Documentation Polish

As pg_tide approaches its first production-ready release, this version focuses on giving operators a clear, real-time picture of what is happening inside the relay. The Grafana dashboard grows substantially: there are now dedicated views for pipeline health at a glance, the slowest sinks ranked by latency, connection pool utilisation, and per-tenant breakdowns — all filterable from a single dropdown. Five alerting rules come pre-written so that teams can start monitoring for common problems immediately after deploying, without needing to author PromQL from scratch. Alerts cover paused pipelines, high consumer lag, dead-letter queue depth, write failures, and connection pool saturation.

The command line gets safer too: the relay now validates URLs and tenant identifiers before accepting them, surfacing clear error messages at startup instead of confusing failures deep inside a running pipeline. The API for describing outbox and inbox pipelines gains an optional human-readable description field, which feeds directly into the auto-generated AsyncAPI specification — making it straightforward to document what each event stream carries without maintaining a separate schema registry. A new mdBook variable keeps version numbers consistent across all documentation pages, and a new runbook walks operators through every step of managing partitioned outbox tables in production.

v0.27.0 completes the observability surface promised by v0.24.0, delivers the
`worker_inner()` decomposition with full unit-test coverage, hardens the CLI
with `clap` value-parser validation, and polishes the documentation for the
v1.0.0 GA milestone.

### Breaking changes

None.

### What's new

**Grafana dashboard — relay-health.json**
- Added **Pipeline Health** row at the top: Total/Healthy/Paused Pipelines stat
  panels, Total Consumer Lag stat, and a Pipeline Status Table.
- Added **Sink Latency** row: P50/P95/P99 latency timeseries + Top-5 Slowest
  Sinks table (sourced from `pg_tide_relay_sink_publish_duration_seconds`).
- Added **Connection Pool** row: stacked area chart for pool connections and
  P95 pool-acquire duration.
- Added **Per-Tenant Overview** row: messages/sec, consumer lag, and healthy
  pipeline count broken down by `tenant` label.
- Added `tenant` template variable for dashboard-level filtering.
- Fixed invalid JSON in existing panels (unescaped `"reverse"` in PromQL
  `direction` label selector).
- Dashboard version bumped to 3.

**Prometheus alerting rules — alerts.yaml** (new file)
- `PgTideRelayPipelinePaused` — fires after 5 min of `pipeline_healthy == 0`.
- `PgTideRelayHighConsumerLag` — fires when consumer lag > 10 000 for 2 min.
- `PgTideRelayDLQDepthHigh` — fires when DLQ intake rate > 100 msg/hr.
- `PgTideRelayDLQWriteError` — critical alert on any DLQ write failure.
- `PgTideRelayConnectionPoolSaturated` — fires when pool-waiting fraction > 10 %
  for 1 min.

**coordinator.rs — worker_inner() decomposition**
- Extracted `handle_publish_outcome()` pure function (6 unit tests).
- Extracted `apply_schema_evolution_check()` async helper.
- Added `WorkerDirective` enum to decouple decision logic from execution.

**CLI hardening (v0.27.0)**
- Added `validate_postgres_url_scheme()` value-parser: rejects any
  `--postgres-url` that does not start with `postgres://` or `postgresql://`.
- Added `validate_tenant_id_str()` value-parser: rejects tenant IDs that are
  empty, exceed 63 bytes, or contain `NUL`, `"`, or `;`.
- Applied both parsers to every `--postgres-url` and `--tenant-id` occurrence
  in the CLI tree.
- Replaced all `eprintln!` + `process::exit(1)` patterns with
  `Cli::command().error(MissingRequiredArgument, …).exit()` — consistent
  clap-formatted output with exit code 2.

**AsyncAPI catalog reflection (v0.27.0)**
- `pg-tide asyncapi export --full-schema`: samples up to 10 recent messages per
  outbox and includes observed JSON payload field names as AsyncAPI schema
  properties.
- `pg-tide asyncapi validate --spec-url <URL>`: fetches an external AsyncAPI
  spec and reports catalog/spec channel mismatches.
- `asyncapi export` now surfaces the optional `description` column (added to
  `tide.tide_outbox_config` in this release) as the AsyncAPI channel
  description.

**SQL migration (0.26.0 → 0.27.0)**
- Added optional `description TEXT` column to `tide.tide_outbox_config`.
- Added optional `description TEXT` column to `tide.tide_inbox_config`.

**Documentation**
- New runbook: [Partition Management](docs/src/operations/partition-management.md)
  covering strategy selection, `outbox_convert_to_partitioned()` prerequisites,
  rollback, monitoring with `pg-tide doctor --partition-check`, manual partition
  creation, emergency partition drop, and pruning verification with
  `EXPLAIN (PARTITIONS)` and `pg_inherits`.
- Updated `docs/src/getting-started/first-pipeline.md` to remove hardcoded
  version strings (`0.1.0`).
- Added `[preprocessor.variables]` with `current_version = "0.27.0"` to
  `book.toml`.

**reqwest promoted to non-optional dependency**
- `reqwest` is now a required dependency of `pg-tide-relay` (was previously
  optional, gated by individual sink features).  This enables the
  `asyncapi validate` command without requiring any sink feature to be active.

---

## [0.26.0] — 2026-05-20 — Partition Safety, Defence-in-Depth & Test Coverage Completion

With partitioned outbox tables arriving in v0.25.0, this release shores up the safety rails around that feature. Two critical edge cases that could have silently broken a production database during a partition conversion are now caught and rejected before any data is touched: one guards against outbox names that would exceed PostgreSQL's 63-byte identifier length limit, and another ensures that converting a shared outbox table does not accidentally disrupt every other outbox running on the same database. Both protections raise a clear, descriptive error so operators understand exactly what needs to change before retrying — and an explicit opt-in parameter is available for operators who have audited the impact and want to proceed deliberately.

Beyond correctness, this release sweeps up a collection of code-quality issues identified through internal audits: the last few places in the codebase where a programming error could cause a silent crash rather than a clean error have been replaced with proper error propagation. New integration tests verify that the PostgreSQL inbox sink correctly round-trips messages and that the dead-letter queue behaves correctly when writes fail under fault-injection conditions. A new CI job compares fresh database installs against sequentially upgraded ones and fails if the schemas diverge — permanently closing the door on silent schema drift between upgrade paths.

v0.26.0 resolves all findings from overall-assessment-5 that were scheduled for
this release: two P1 correctness/security gaps in partition table naming and the
global partition swap, four P2 code-quality items (`expect()` elimination and
`// SAFETY:` annotation), and three persistent test coverage gaps (PgInboxSink
round-trip, DLQ fault injection, and `pg_dump` schema-diff CI). Also ships
ADR-007 documenting the shared partition table semantics, a CONTRIBUTING.md
with the project `// SAFETY:` convention, and a `just lint-expect` recipe.

### Breaking changes
None.

### What's new

**P1: NAMEDATALEN guard in `outbox_convert_to_partitioned()`**
- The function now rejects outbox names long enough to produce backup or new
  table identifiers exceeding PostgreSQL's 63-byte `NAMEDATALEN` limit, emitting
  a descriptive `RAISE EXCEPTION` with the computed lengths.
- `outbox_create()` and `outbox_create_if_not_exists()` (Rust) enforce the same
  constraint at creation time when `partition_strategy <> 'none'`.

**P1: Shared-table prerequisite guard in `outbox_convert_to_partitioned()`**
- The function now rejects conversion if any other outbox still uses the
  unpartitioned shared `tide_outbox_messages` table, preventing the global rename
  from breaking concurrent writers.
- A new `confirm_shared_table_migration BOOLEAN DEFAULT FALSE` parameter allows
  operators who understand the global scope to opt in deliberately (following the
  `admin_rewind_offset()` pattern from v0.23.0).
- **Note:** The function signature changed from `(TEXT, TEXT)` to `(TEXT, TEXT,
  BOOLEAN)`. Existing callers with 2 arguments continue to work via the default
  parameter.

**P2: `expect()` elimination in Arrow Flight, Singer, and Airbyte**
- `arrow_flight.rs`: `self.channel.as_mut().expect(...)` replaced with
  `.ok_or_else(|| RelayError::Other("gRPC channel not established after ensure_connected()"))?`.
- `singer.rs`: `child.stdout.take().expect(...)` replaced with `.ok_or_else(...)?`.
- `airbyte.rs`: same pattern as singer.

**P2: `// SAFETY:` annotation for webhook HMAC**
- Updated the HMAC comment in `webhook.rs` to the project-standard format
  citing RFC 2104 §3 and the config value invariant.

**Test coverage: PgInboxSink round-trip (`tests/pg_inbox_sink_test.rs`)**
- Spins up a PostgreSQL 18 testcontainer, calls `inbox_create('pg_sink_test')`,
  publishes 100 messages in a single batch, and asserts correct column values
  and idempotent re-publishing.

**Test coverage: DLQ fault injection (`tests/dlq_fault_injection_test.rs`)**
- Tests DLQ write success, INSERT-denied failure path, empty-batch no-op,
  `ErrorKind` variant classification, and `DlqEntry::from_message()` field mapping.

**CI: `pg_dump` schema-diff job (`schema-diff`)**
- New GitHub Actions job that creates two PostgreSQL databases (fresh sequential
  install vs. same sequential chain), captures `pg_dump --schema-only --schema=tide`,
  and diffs them — failing on any schema drift.

**CI: `lint-expect` job**
- New GitHub Actions job and `just lint-expect` recipe that scan `pg-tide-relay/src/`
  for bare `.expect()` calls not preceded by a `// SAFETY:` comment.

**ADR-007: Shared Partition Table Semantics**
- `docs/adr/adr-007-shared-partition-table-semantics.md` — documents the
  interaction between ADR-001 (single-table) and ADR-006 (partitioning),
  the three options considered, and the recommended migration procedure.

**CONTRIBUTING.md**
- Added `// SAFETY:` convention documentation and CI enforcement instructions.

**SQL migration**
- `sql/pg_tide--0.25.0--0.26.0.sql` — drops and recreates
  `tide.outbox_convert_to_partitioned()` with NAMEDATALEN guard, prerequisite
  check, and `confirm_shared_table_migration` parameter.

---

## [0.25.0] — 2026-05-20 — Outbox Table Partitioning, Multi-Tenant Relay Completion & Pre-GA Hardening

As outbox tables grow over time, query performance can suffer without a strategy for keeping old data under control. This release introduces declarative table partitioning: when creating an outbox you can choose whether it should be split into daily, weekly, or monthly partitions automatically. Old partitions can be dropped on a schedule rather than running slow bulk deletes, and the relay's sweep command is aware of the partition strategy so it cleans up the right tables. Teams running busy event streams will see significantly faster query times and lower storage growth over time, and a live migration function lets existing deployments convert their outboxes to partitioned tables with minimal relay downtime and no data loss.

Multi-tenant deployments become first-class in this release. The relay coordinator now enforces tenant ownership at runtime, so a relay instance configured for one tenant will never accidentally pick up or interfere with another tenant's pipelines — even when both share the same PostgreSQL database. Advisory locks are namespaced per tenant so two tenants with identically named pipelines can coexist without conflict. A new `--self-test` flag is designed for Kubernetes readiness probes: the relay connects to PostgreSQL, checks TLS, acquires an advisory lock, verifies the schema is up to date, and exits with a clear success or failure code so the cluster knows it is safe to route traffic before the relay has processed a single message.

v0.25.0 implements ADR-006 declarative outbox table partitioning, completes
the multi-tenant relay groups runtime that has been catalog-ready since v0.14.0
but lacked coordinator-side enforcement, hardens the `pg-tide doctor` checks,
expands the Criterion.rs benchmark suite with three production-representative
hot-path benchmarks, ships the `--self-test` startup flag for Kubernetes
readiness probes, and publishes the pre-GA readiness checklist that serves as
the formal acceptance gate for the v1.0.0 Production GA release.

### Breaking changes
None.

### What's new

**Outbox table partitioning (ADR-006)**
- **`outbox_create()` gains `partition_strategy` parameter** — accepts
  `'none'` (default, existing behaviour), `'daily'`, `'weekly'`, or
  `'monthly'`.  When set, the outbox configuration records the chosen strategy
  for use by the relay sweep command and partition health checks.
- **`outbox_create_if_not_exists()` gains `partition_strategy`** — same
  parameter as `outbox_create()` for idempotent deployment scripts.
- **`tide.outbox_convert_to_partitioned(name, strategy)`** — live migration
  SQL function that converts an existing unpartitioned outbox to declarative
  range partitioning using an advisory-lock swap with minimal relay downtime.
  The original data is preserved in a backup table until manually dropped after
  verification.
- **`tide.tide_partition_events` table** — durable log of partition lifecycle
  events (created, dropped, converted) for `pg-tide doctor` health checks and
  operator auditing.
- **`partition_strategy` and `retention_partitions` columns** in
  `tide.tide_outbox_config` — record the chosen strategy and rolling retention
  window (default: 7 partitions) for each outbox.

**Multi-tenant relay groups: runtime completion**
- **Per-tenant pipeline ownership filtering** — `Coordinator` gains a
  `set_tenant_id()` method; when set, `load_pipelines()` filters
  `tide.relay_outbox_config` and `tide.relay_inbox_config` to only pipelines
  matching `tenant_name = $tenant_id`.  No cross-tenant contamination.
- **Per-tenant advisory lock namespacing** — `try_acquire_lock()` and
  `release_lock()` incorporate the tenant ID into the lock key pair, preventing
  two tenants with identical pipeline names from colliding on the same
  PostgreSQL database.
- **`--tenant-id` CLI flag** — `PG_TIDE_TENANT_ID` env var; configures the
  coordinator's tenant ID at startup.
- **Per-tenant index** — `idx_relay_outbox_config_tenant` and
  `idx_relay_inbox_config_tenant` partial indexes on `tenant_name WHERE
  enabled = true` for efficient per-tenant pipeline discovery queries.
- **Two-tenant isolation integration test** — asserts coordinator-a only
  discovers tenant-a's pipelines, coordinator-b only discovers tenant-b's
  pipelines, and both can acquire their respective advisory locks concurrently.

**Extended `pg-tide doctor` checks**
- **TLS version check** — queries `pg_ssl` for the negotiated TLS version;
  warns on TLS 1.1/1.0, flags when `sslmode=require` resolves to a plaintext
  connection.
- **DuckLake catalog health check** — verifies `ducklake_snapshot`,
  `ducklake_data_file`, and `ducklake_column` are accessible when DuckLake
  pipelines are configured; reports INFO when absent (not a failure for
  non-DuckLake deployments).
- **DLQ depth warning** — counts `tide.relay_dlq` entries written in the last
  hour and warns when the rate exceeds `--dlq-warn-threshold` (default: 100).
- **Partition capacity check** — warns when a partitioned outbox has recent
  writes, reminding operators to provision the next partition via
  `pg-tide sweep`.

**Relay benchmark suite (Criterion.rs)**
- **`bench_outbox_poll_decode`** — decodes a 1 000-row batch at 1 KB, 10 KB,
  and 100 KB payload sizes, isolating the decode overhead from PostgreSQL I/O.
- **`bench_inbox_unnest_params`** — builds the four UNNEST parameter Vecs for
  `InboxSink::publish()` at 1, 10, 100, and 1 000 row batch sizes.
- **`bench_worker_inner_orchestration`** — measures end-to-end routing +
  envelope wrapping overhead for the coordinator's `worker_inner()` hot path
  at 10, 100, and 1 000 message batches, independently of I/O.

**Pre-GA operational readiness**
- **`--self-test` flag** — connects to PostgreSQL, checks TLS state, acquires
  and releases an advisory lock, verifies the `partition_strategy` column is
  present (v0.25.0+ schema), and exits 0 on success or 1 with a descriptive
  error.  Designed for Kubernetes `initContainers` and CI/CD pre-deployment gates.
- **`docs/src/operations/pre-ga-checklist.md`** — formal acceptance gate
  covering TLS configuration, outbox partitioning strategy, consumer group
  setup, DLQ monitoring thresholds, `pg-tide doctor` output interpretation,
  Helm security context review, benchmark baseline validation, and rollback
  procedure.
- **`just release-notes` recipe** — reads `CHANGELOG.md` for the current
  workspace version and formats a GitHub Release body with upgrade notes,
  Docker pull command, and tag instruction.

**SQL migration**
- `sql/pg_tide--0.24.0--0.25.0.sql` — adds `partition_strategy` and
  `retention_partitions` to `tide.tide_outbox_config`, creates
  `tide.tide_partition_events`, adds per-tenant indexes on relay config
  tables, and defines `tide.outbox_convert_to_partitioned()`.

---

## [0.24.0] — 2026-05-19 — Code Quality, Performance & Helm Production Maturity

This release is the kind that keeps a production system healthy long-term. Several performance improvements reduce the number of database round-trips that happen for routine operations like checking outbox status or publishing a message, with the most heavily called code paths now requiring half as many queries as before. Log volume at busy deployments drops dramatically by reserving informational messages for meaningful state changes rather than printing a line for every message processed — at fifty active pipelines polling once per second, that is over four million log lines per day reclaimed. The coordinator is also decomposed into smaller, independently testable units, making the codebase substantially easier to reason about and maintain.

On the Kubernetes side, three production-readiness templates are added to the Helm chart: a PodDisruptionBudget that prevents Kubernetes from evicting all relay replicas simultaneously during a rolling upgrade, a ServiceMonitor that auto-wires the Prometheus metrics endpoint to the kube-prometheus-stack without any manual scrape configuration, and a HorizontalPodAutoscaler that scales the relay out under load while the advisory lock system ensures each pipeline is owned by exactly one replica at a time. The release also publishes the formal architecture decision record for outbox table partitioning, giving teams a clear reference document before the feature lands in the next version.

v0.24.0 lands every P2/P3 audit finding accumulated over four assessment
cycles, ships three new Helm production-maturity templates, and publishes
ADR-006 — the design contract for outbox table partitioning that will be
implemented in v0.25.0.  The headline changes are: `outbox_status()` now
executes a single SQL query with `FILTER` aggregates instead of three
round-trips, `OutboxBatch::into_messages()` eliminates a full payload clone on
every message decode, and the Helm chart gains `PodDisruptionBudget`,
`ServiceMonitor`, and `HorizontalPodAutoscaler` templates for HA and
observability.

### Breaking changes
None.

### What's new

**P2: Performance and correctness**
- **`outbox_status_impl()` single SPI call** — replaced the three sequential
  `Spi::get_one_with_args()` calls (pending count, total count, oldest age)
  with a single `SELECT … FILTER (WHERE …)` aggregate query joined to the
  config table.  Eliminates 2× SPI round-trips for every `tide.outbox_status()`
  invocation.
- **Per-batch coordinator logging** — demoted per-message dry-run logging from
  `tracing::info!` to `tracing::debug!`, reserving `info!` for state
  transitions (worker start/stop, circuit-breaker open/close, pipeline
  pause/resume).  At 50 pipelines × 1 poll/s this reduces log volume by
  approximately 4.3 million lines/day in a typical production deployment.
- **`rate_limiter.rs` safe constant** — replaced
  `NonZeroU32::new(1).expect("1 is non-zero")` with `NonZeroU32::MIN`
  (stable since Rust 1.79), removing the last production-reachable `expect()`
  call in the rate-limiter path.

**P3: SPI error handling**
- **`get_outbox_retention()` error propagation** — changed the return type from
  `Option<i32>` to `Result<Option<i32>, PgTideError>` and propagate errors with
  `?` instead of silently returning `None` on SPI failure, consistent with the
  pattern established for `outbox_exists()` in v0.17.0.
- **`outbox_publish_impl()` fold `current_user` into ACL query** — eliminated
  the preliminary `SELECT current_user` SPI call by embedding `current_user`
  directly in the ACL lookup predicate.  Saves one SPI round-trip per
  `tide.outbox_publish()` call under ACL-enforced outboxes.

**P3: Coordinator decomposition**
- **`worker_inner()` decomposition** — extracted the polling and decoding logic
  into a `poll_and_decode()` helper and the publish path into a
  `publish_with_circuit_breaker()` helper.  Both are independently unit-testable.
  Added `PollOutcome` and `PublishOutcome` enums for clean branching in the
  outer loop.
- **`OutboxBatch::into_messages()` avoid unnecessary clone** — switched from
  `.iter()` with `row.clone()` to consuming ownership via `.into_iter()`,
  eliminating a full copy of the payload `Vec` on every message decode.

**Observability improvements**
- **Per-sink publish latency histogram** — new
  `pg_tide_relay_sink_publish_duration_seconds` Histogram metric labelled by
  `pipeline` and `sink_type`, tracking wall-clock time from `Sink::publish()`
  call entry to return.
- **Connection pool health metrics** — new
  `pg_tide_relay_pool_connections{state}` gauge and
  `pg_tide_relay_pool_acquire_duration_seconds` histogram for early detection of
  connection exhaustion before `max_connections` is hit.
- **OTel `backoff_sleep` span annotation** — the `relay.backoff.sleep` span now
  carries a `next_wake_up_ms` attribute alongside `consecutive_failures` so
  distributed traces capture the full backoff trajectory for performance
  debugging.

**Helm production maturity**
- **`PodDisruptionBudget` template** (`helm/pg-tide/templates/pdb.yaml`) —
  rendered from `podDisruptionBudget.enabled` (default `false`) and
  `podDisruptionBudget.minAvailable` (default `1`).  Prevents Kubernetes from
  evicting all relay replicas simultaneously during rolling upgrades.
- **`ServiceMonitor` template** (`helm/pg-tide/templates/servicemonitor.yaml`)
  — rendered from `serviceMonitor.enabled` (default `false`).  Auto-discovers
  the `/metrics` endpoint for the Prometheus Operator (kube-prometheus-stack,
  Victoria Metrics Operator) without manual scrape configuration.
- **`HorizontalPodAutoscaler` template** (`helm/pg-tide/templates/hpa.yaml`) —
  renders an `autoscaling/v2` HPA when `autoscaling.enabled = true`, replacing
  the previous placeholder.  Advisory locks ensure safe multi-replica
  deployments with disjoint pipeline ownership.

**Architecture decision records**
- **ADR-006: Outbox Table Partitioning** — published in
  `docs/adr/adr-006-outbox-table-partitioning.md`.  Establishes the design
  contract for declarative range partitioning on `created_at` with opt-in
  `partition_strategy` parameter in `outbox_create()`, live migration tooling,
  and consumer group compatibility guarantees.  Implementation is scheduled for
  v0.25.0.

---

## [0.23.0] — 2026-05-19 — Correctness, Real TLS & Full Migration Coverage

Every release aims for correctness, but this one delivers it with unusual urgency. A critical bug introduced in v0.13.0 was silently preventing all cross-database inbox delivery — messages were being written to columns that do not exist on any real inbox table, meaning any pipeline routing events between two different PostgreSQL instances had been failing at runtime since May. This release fixes the column mapping, switches to efficient batch inserts, and adds a regression test that would have caught the issue immediately. Existing cross-database pipelines should be reviewed and any accumulated failures replayed using the dead-letter queue tooling provided in earlier releases.

Real TLS support arrives in this release: when `sslmode=require` or stronger is configured, the relay now establishes a genuine encrypted connection using the platform's OpenSSL stack. Migration coverage is extended through v0.22.0 so the CI suite verifies every upgrade path in the project's history. An important safeguard is added to consumer offset tracking: a database constraint now prevents any consumer — including a buggy one — from rolling back an already-committed offset, protecting delivery guarantees. When an intentional rewind is needed for incident recovery, a new admin function provides that capability in a controlled, auditable way that requires explicit confirmation.

v0.23.0 addresses every P0/P1/P2 finding from overall-assessment-4, completing
the audit remediation cycle before v1.0.0 GA.  The headline changes are: a
critical fix to `PgInboxSink` that restores cross-database inbox delivery (broken
since v0.13.0), real TLS connections via the new `native-tls` feature flag,
full migration test coverage through v0.22.0, and a `commit_offset()` monotonicity
guard that prevents offset rollback by buggy consumers.

### Breaking changes
None.

### What's new

**P0: Critical correctness fixes**
- **Fix `PgInboxSink` column mismatch** — the remote PostgreSQL inbox sink was
  inserting into `(event_id, event_type, payload, received_at)` instead of the
  correct `(event_id, source, payload, headers)` schema created by
  `tide.inbox_create()`.  Any cross-database inbox delivery pipeline has been
  failing at runtime since v0.13.0; this release fixes it.  Also switched from
  a per-row INSERT loop to a single UNNEST batch insert matching the local
  `InboxSink` pattern.
- **Add missing `extension_sql_file!()` for 0.21.0→0.22.0** — a fresh
  `CREATE EXTENSION pg_tide` at v0.22.0 was silently missing
  `tide.ducklake_source_config`, `tide.ducklake_replicate()`, and
  `tide.ducklake_source_last_snapshot()`.  Fixed.

**P1: Real TLS via `native-tls` feature**
- New optional Cargo feature `native-tls` (disabled by default) that uses the
  platform OpenSSL stack (`postgres-openssl`) to establish real TLS connections
  when `sslmode=require/verify-ca/verify-full`.  The default build continues to
  fail closed on `require` without establishing plaintext; the `:latest-full`
  Docker image compiles with `--features native-tls`.

**P1: Migration test coverage**
- `migration_test.rs` extended through v0.22.0 (five new migration scripts).
- `sql_to_sink_e2e.rs` extended through v0.22.0 (DuckLake catalog tables now
  present in the E2E test environment).

**P1: Security hardening**
- **Fix `ducklake_attach()` format specifiers** — single quotes in database names
  or hostnames no longer produce malformed DuckDB `ATTACH` statements; backported
  via the `pg_tide--0.22.0--0.23.0.sql` migration.
- **Fix `ctrl_c().await.expect()` in `main.rs`** — replaced with graceful
  degradation on restricted seccomp profiles.
- **`ducklake_replicate()` identifier length guard** — 63-byte identifier limit
  is now checked and raises a clear error instead of silently truncating.

**P2: Offset safety**
- **`commit_offset()` monotonicity guard** — the `ON CONFLICT DO UPDATE` clause
  now includes `WHERE committed_offset <= EXCLUDED.committed_offset`, preventing
  any consumer from rolling back an already-committed offset accidentally.  This
  finding was raised in four consecutive audit cycles; it is now fixed.
- **`tide.admin_rewind_offset()`** — new `SECURITY DEFINER` function for
  intentional offset rollback, requiring `confirm_reprocessing = TRUE` and
  `pg_tide_admin` or superuser membership.  All calls are audited.

**P2: Remote inbox sink batching**
- `PgInboxSink` now uses a single `UNNEST` batch insert instead of N individual
  round-trips per batch.

**Test coverage**
- `commit_offset()` monotonicity guard test (two cases: lower value ignored, higher advances).
- `PgInboxSink` round-trip test: 50 messages, correct column mapping, zero duplicates.
- DLQ fault-injection test + error-classification unit tests.
- `admin_rewind_offset()` verified in migration test.

---

## [0.22.0] — 2026-05-19 — DuckLake Bidirectional Flow & Ecosystem Surface

Data lakes have traditionally been write-only destinations: events flow in from operational systems and analysts query the accumulated history. This release breaks that pattern by opening a reverse channel — a DuckLake data lake can now be a source of events that flow back into pg_tide inboxes, triggering application logic whenever new data arrives. Any DuckDB-compatible engine can write to the lake and have those writes automatically propagated to application services, creating a genuinely bidirectional data pipeline with PostgreSQL as the coordination layer and with the full delivery guarantees and deduplication that pg_tide provides on every other pipeline.

The DuckLake integration also receives a full ecosystem surface in this release. A new `pg-tide ducklake` CLI family makes it easy to inspect lake state, check snapshot progress, and debug offset mapping without writing SQL. A single `docker compose up` command spins up a complete demonstration environment including PostgreSQL, object storage, the relay, a DuckDB shell, and a live Grafana dashboard — ready to explore in minutes. Five written tutorials and four conference-ready demo scripts provide everything a team needs to evaluate, adopt, or present the DuckLake integration from first principles to production deployment.

v0.22.0 completes the pg-tide × DuckLake integration by opening the reverse
direction (DuckLake → pg-tide inbox), adding cross-lake replication helpers,
and shipping the full ecosystem surface: `pg-tide ducklake` CLI subcommands,
a Docker Compose getting-started example, five written tutorials, and four
conference demo scripts.  Any DuckDB engine — DataFusion, Spark, Trino — can
now write to a DuckLake and have pg-tide stream those changes back into
application services via the familiar pg-tide inbox API.

### Reverse Relay — DuckLake Source

- **`DuckLakeSource`** — new source implementation (`source/ducklake.rs`)
  that polls `ducklake_snapshot` for new snapshots beyond the last-seen ID,
  fetches incremental data-file metadata, and delivers `RelayMessage` objects
  to a pg-tide inbox.  Feature-gated under `--features ducklake`.
- **`tide.ducklake_source_config` table** — catalog table that stores
  DuckLake reverse-relay pipeline configuration: catalog connection URL,
  `catalog_schema`, `dl_schema`, `dl_table`, `snapshot_poll_interval_ms`,
  and `consumer_group`.  Added by `sql/pg_tide--0.21.0--0.22.0.sql`.
- **`tide.relay_set_inbox_v2(..., "source_type": "ducklake", ...)`** —
  configure via the existing JSONB inbox API with keys `catalog_connection`,
  `catalog_schema`, `schema`, `table`, `snapshot_poll_interval_ms`.

### Cross-Lake Replication

- **`tide.ducklake_replicate(source_catalog, source_schema, source_table,
  dest_catalog, dest_schema, dest_table)`** — convenience SQL function that
  registers a DuckLake source config entry for the source table and returns a
  human-readable summary.  Idempotent: calling it again updates the config
  without error.  Chain with a DuckLake sink pipeline for full cross-lake
  fan-out with pg-tide handling delivery guarantees and deduplication.
- **`tide.ducklake_source_last_snapshot(pipeline_name)`** — SQL function
  that returns the last acknowledged DuckLake `snapshot_id` for a reverse
  relay pipeline (stored in `tide.ducklake_offset_map` under consumer group
  `'__ducklake_source'`).

### `pg-tide ducklake` CLI Subcommands

- **`pg-tide ducklake snapshots --pipeline <name>`** — lists DuckLake
  snapshots for a pipeline with timestamps, record counts, and file counts.
- **`pg-tide ducklake checkpoint --pipeline <name>`** — reports catalog state
  and provides DuckDB `CHECKPOINT` command for physical compaction.
- **`pg-tide ducklake flush-inlined --pipeline <name>`** — reports inlined
  data tables pending flush and provides DuckDB guidance.
- **`pg-tide ducklake offset-map --pipeline <name>`** — prints the
  `tide.ducklake_offset_map` consumer-offset-to-snapshot-ID mapping table in
  human-readable form for debugging time-travel replay scenarios.

### Docker Compose Getting-Started Example

- **`examples/ducklake/docker-compose.yml`** — single `docker compose up`
  environment with PostgreSQL 18 + pg_tide, MinIO (S3-compatible object
  storage), pg-tide relay, DuckDB shell container, and Grafana with the relay
  health dashboard.  A `docker compose run seed` publishes 1 000 synthetic
  order events and demonstrates querying the live lake from DuckDB.

### Tutorial Suite

- **`docs/src/guides/ducklake/01-from-transaction-to-data-lake.md`** —
  "From Transaction to Data Lake in 5 Minutes": end-to-end walkthrough.
- **`docs/src/guides/ducklake/02-real-time-analytics.md`** — "Real-Time
  Analytics with DuckDB": live aggregation queries and time-travel patterns.
- **`docs/src/guides/ducklake/03-multi-tenant.md`** — "Multi-Tenant Data
  Lake with Row-Level Security": tenant discriminator + RLS + bucket
  partitioning.
- **`docs/src/guides/ducklake/04-event-sourcing.md`** — "Event Sourcing
  with DuckLake as the Event Store": append-only event log, projection
  rebuilds, time-travel replay.
- **`docs/src/guides/ducklake/05-migrating-from-kafka-connect.md`** —
  "Migrating from Kafka Connect": side-by-side comparison and step-by-step
  migration checklist.

### Conference Demo Scripts

- **`examples/ducklake/demos/01-zero-to-data-lake.sh`** — "Zero to Data
  Lake" lightning demo (~5 min): publish events, check relay status, query
  from DuckDB.
- **`examples/ducklake/demos/02-impossible-guarantee.sh`** — "The Impossible
  Guarantee" crash-recovery demo (~8 min): exactly-once delivery demonstrated
  with `atomic_lake_writes = true`.
- **`examples/ducklake/demos/03-streaming-sensor-dashboard.sh`** —
  "Streaming Sensor Dashboard" interactive demo (~10 min): live sensor
  ingest with data inlining and DuckDB aggregation.
- **`examples/ducklake/demos/04-compliance-replay.sh`** — "Compliance
  Replay" enterprise demo (~12 min): audit replay using offset-map and
  DuckDB time-travel.

---

## [0.21.0] — 2026-05-19 — DuckLake Streaming, Inlining & Schema Evolution

Writing events to a data lake works beautifully for high-volume batch workloads, but streaming use cases have a well-known weakness: every small write creates a new Parquet file, and thousands of tiny files degrade query performance rapidly. This release solves the small-files problem by writing small batches directly into PostgreSQL as inlined data rather than flushing them to object storage. The data is still fully queryable by DuckDB with complete time-travel support; it simply lives in a PostgreSQL table until a flush operation merges it into larger, efficient Parquet files. Streaming throughput increases and object storage costs fall without any change to how applications publish events, and a DLQ archive option keeps the operational dead-letter queue table small while preserving unlimited auditable history in the lake.

Schemas are rarely static, and this release makes the relay's DuckLake sink intelligent about the difference between a safe change — adding a new field to a JSON payload — and a potentially disruptive one — removing or renaming a field. New fields are automatically registered in the DuckLake catalog so DuckDB can see them immediately. Breaking changes trigger a configurable policy: pause the pipeline for operator review, route affected messages to the dead-letter queue, or log a warning and continue. A new snapshot-to-offset mapping table enables precise time-travel replay from pg_tide consumer offsets, so teams can ask what the lake looked like when the system processed a specific offset and get an exact answer they can use in a DuckDB query.

v0.21.0 tackles the two hardest problems for streaming workloads on data lakes:
the small-files problem and schema drift.  DuckLake's data inlining feature
stores small writes directly in PostgreSQL — zero Parquet files created,
sub-millisecond write latency, full time-travel preserved.  The schema
evolution bridge detects new JSON fields in outbox messages and automatically
registers new `ducklake_column` entries.  A new snapshot-to-consumer-offset
mapping enables DuckDB time-travel replay from pg-tide consumer offsets, and
an optional DLQ archive sink keeps the operational DLQ table small while
preserving unlimited auditable history.

### DuckLake Sink — Data Inlining

- **`inline_row_limit` option** (default: 10) — batches at or below this
  threshold are written directly to
  `ducklake_inlined_data_{table_id}_{schema_version}` in the catalog rather
  than flushing to Parquet.  Zero Parquet files created for streaming
  workloads, sub-millisecond write latency, full time-travel preserved.
  Above the threshold, the existing Parquet-write path is used.
- **Inline snapshot transaction** — each inline batch creates a
  `ducklake_snapshot` with `change_type = 'add_inlined_rows'`, issues a
  `pg_notify('tide_ducklake_changes', …)` with `"inlined": true`, and updates
  `ducklake_table_stats` atomically.  DuckDB consumers always see the correct
  result regardless of where the data lives.

### DuckLake Sink — Automatic Schema Evolution Bridge

- **New JSON key detection** — on each relay batch the sink compares the JSON
  keys present in message payloads against the known `ducklake_column` entries.
  New keys are classified as additive (new nullable column) — type conflicts are
  treated as breaking changes.
- **Additive columns** — new nullable `VARCHAR` columns are inserted into
  `ducklake_column` with `begin_snapshot = new_snapshot_id`.  DuckDB handles
  missing values in older Parquet files transparently via column projection.
  The sink's `schema_version` counter is incremented so that new inlined tables
  use the updated schema version.
- **`on_schema_change` policy** — configurable per pipeline:
  `pause` (return a permanent error), `route_to_dlq` (skip the batch),
  `warn_and_continue` (default — register new columns), `auto_new_stream`
  (also registers new columns in the current implementation).

### DuckLake Sink — Snapshot-to-Consumer-Offset Mapping

- **`tide.ducklake_offset_map` table** — records the mapping from pg-tide
  consumer group offset to DuckLake snapshot ID, written atomically with each
  snapshot commit (both inline and Parquet paths).  Enables consumers to use
  DuckDB time-travel to replay events by offset range.
- **`tide.ducklake_replay_range(pipeline, from_offset, to_offset)`** — SQL
  function that returns the DuckDB `AT (VERSION => …)` range expression for
  the given consumer-group offset range, ready to paste into a DuckDB session.

### DuckLake Sink — Auto-Partition

- **`partition` config option** — `DuckLakePartition::Daily`,
  `DuckLakePartition::Monthly`, `DuckLakePartition::Bucket(N)`, or `None`
  (default).  When set, the sink registers the partition strategy in the new
  `tide.ducklake_partition_config` table on first use.
- **`tide.ducklake_partition_config` table** — stores partition type, catalog
  schema, namespace, and table name per pipeline.  Also serves as the registry
  used by `tide.ducklake_column_history()`.

### DuckLake Sink — DLQ Archive

- **`dlq_archive_after_hours` option** — when set, an archival sweep runs on
  each `publish()` cycle.  Entries older than the configured TTL are atomically
  moved from `tide.relay_dlq` into `{catalog_schema}.dlq_archive` — keeping
  the operational DLQ table small while preserving unlimited auditable history
  queryable via DuckDB with time-travel and filter pushdown.

### New SQL Functions & Tables (v0.21.0)

- **`tide.ducklake_offset_map`** — snapshot-to-consumer-offset mapping table.
- **`tide.ducklake_partition_config`** — partition strategy registry.
- **`tide.ducklake_replay_range(pipeline text, from_offset bigint, to_offset bigint)`**
  → `TEXT` — returns the DuckDB `AT (VERSION => …)` range expression.
- **`tide.ducklake_column_history(pipeline_name text)`** — returns every
  `ducklake_column` entry for the tables written by the given pipeline,
  together with the earliest snapshot ID at which each column appears.
  Use this to track schema evolution over time.

### Extension Migration Chain

- **`lib.rs` migration chain updated** — includes `pg_tide--0.20.0--0.21.0.sql`
  so that `CREATE EXTENSION pg_tide` (fresh install) and
  `ALTER EXTENSION pg_tide UPDATE` (upgrade) produce identical catalog schemas.

---

## [0.20.0] — 2026-05-19 — DuckLake Native Catalog Integration

Since v0.10.0, pg_tide has been able to write event data as Parquet files to object storage and track snapshots in a proprietary catalog table. This release replaces that proprietary catalog with the real DuckLake v1.0 protocol — the same catalog schema that DuckDB's native `ATTACH` command understands. The moment this release is deployed, any DuckDB instance anywhere on the network can attach to the same PostgreSQL database and query the complete event history as a first-class data lake, with time-travel, filter pushdown, and full schema evolution support — no extra software, no data migration, no glue code required. Column statistics are written alongside each data file so DuckDB can prune irrelevant Parquet files during query planning without reading them, which is critical for large event archives with selective queries.

The relay's catalog writes are fully atomic: for each batch of events, a single PostgreSQL transaction creates the snapshot entry, registers the Parquet data file, writes column statistics, and updates aggregate table statistics — everything commits together or nothing does. For teams where the relay and the pg_tide extension share the same PostgreSQL instance, enabling `atomic_lake_writes` extends that atomicity all the way from outbox publish to data lake commit, delivering exactly-once semantics from OLTP transaction to analytical query. A `pg_notify` call after each commit allows downstream services to subscribe for near-real-time lake change notifications without polling, and a helper SQL function generates the correct DuckDB `ATTACH` statement pre-filled with the right connection details.

v0.20.0 upgrades the DuckLake relay sink to speak the real DuckLake v1.0
catalog protocol, making all data written by the pg-tide relay immediately
queryable by DuckDB — with no glue code, no extra tooling, and no migration.
From the moment this release ships, any DuckDB instance can
`ATTACH 'ducklake:postgres:...'` to the same PostgreSQL database and query
the event history with full time-travel, filter pushdown, and schema
evolution support.

### DuckLake Sink — v1.0 Native Catalog Writes

- **Real DuckLake v1.0 catalog tables** — the relay sink now writes to the
  official DuckLake catalog schema: `ducklake_snapshot`,
  `ducklake_snapshot_changes`, `ducklake_data_file`, `ducklake_table_stats`,
  `ducklake_table_column_stats`, `ducklake_file_column_stats`,
  `ducklake_schema`, `ducklake_table`, `ducklake_column`, and
  `ducklake_metadata`. The proprietary `tide.ducklake_snapshots` table is no
  longer used by new installations.
- **Atomic catalog transactions** — for each relay batch, a single PostgreSQL
  transaction creates the snapshot, registers the Parquet data file, writes
  per-file column statistics, updates table-level statistics, and appends a
  snapshot change record. Either everything commits or nothing does.
- **Auto-bootstrap** — if no DuckLake schema and table exist for a given
  outbox stream, the sink creates all required catalog entries as part of the
  first batch. No manual DDL is required.
- **Column statistics for filter pushdown** — the sink computes min/max
  values, null counts, and distinct-value counts per column and writes them
  to `ducklake_file_column_stats` and `ducklake_table_column_stats`. DuckDB
  can use these to prune Parquet files during query planning without reading
  them — critical for large event archives with selective queries.
- **NOTIFY-based change notifications** — after each batch commit the sink
  issues `pg_notify('tide_ducklake_changes', {...})`. Application services,
  incremental materialized view refreshers, and downstream relay instances can
  subscribe for near-real-time lake change notifications without polling.
- **`atomic_lake_writes` config option** — set
  `"ducklake_atomic": true` in the relay pipeline config to opt in to
  same-transaction atomicity mode. When the relay connects to the same
  PostgreSQL instance as the pg_tide outbox, the consumer-offset advance and
  the DuckLake snapshot commit can be wrapped in a single transaction —
  delivering exactly-once guarantee from OLTP event to data lake.
- **`catalog_schema` config option** — the PostgreSQL schema where DuckLake
  v1.0 catalog tables live is now configurable (default: `"ducklake"`).

### New SQL Helper Functions

- **`tide.ducklake_attach(catalog_schema text DEFAULT 'ducklake', data_path text DEFAULT '')`**
  → `TEXT` — returns the DuckDB `ATTACH` statement pre-populated with the
  correct PostgreSQL connection string, removing friction for first-time users.

  ```sql
  SELECT tide.ducklake_attach();
  -- ATTACH 'ducklake:postgres:dbname=mydb host=localhost port=5432' AS ducklake;
  ```

- **`tide.ducklake_migrate_catalog(catalog_schema text DEFAULT 'ducklake')`**
  — one-time migration helper that converts any existing
  `tide.ducklake_snapshots` rows (v0.10.0 format) into the new DuckLake v1.0
  catalog format and drops the old table. Safe to call multiple times
  (idempotent).

### Extension Migration Chain

- **`lib.rs` migration chain completed** — the pgrx extension SQL chain now
  includes all migration files through v0.20.0 (0.17.0→0.18.0,
  0.18.0→0.19.0, 0.19.0→0.20.0 were previously missing). Fresh installs via
  `CREATE EXTENSION pg_tide` and upgrade paths via `ALTER EXTENSION pg_tide
  UPDATE` now produce identical catalog schemas.

## [0.19.0] — 2026-05-14 — Supply Chain, Observability & Operational Docs

Deploying software responsibly means being able to answer the question of what is running in your environment and whether it is safe. This release adds SBOM generation to every release, attaching a machine-readable inventory of all software components in the CycloneDX format — a requirement for many enterprise security programmes and a prerequisite for SOC 2 and FedRAMP compliance processes. Every Docker image is now scanned by Trivy before release, and any image containing an unfixed critical vulnerability blocks the release rather than silently shipping. Images and release binaries are signed with keyless Sigstore signatures, so operators can verify the provenance of anything they deploy without managing their own signing keys.

The relay gains a `/healthz` endpoint matching the standard Kubernetes liveness probe convention, making it trivial to integrate into any cluster health check configuration. The Grafana dashboard grows a Coordinator row showing how many pipelines are currently owned, how long each reconciliation cycle takes, and how errors are distributed across pipelines — the three numbers that answer whether a relay instance is healthy at a glance. A fully commented example TOML configuration file is baked into Docker images so operators always have a starting point within reach without consulting external documentation. Four new operations runbooks cover the scenarios that cause the most support requests: crash recovery, dead-letter queue draining, schema migration without downtime, and rolling relay upgrades.

v0.19.0 completes the supply-chain story with SBOM generation and Trivy
vulnerability scanning, adds the `/healthz` Kubernetes-standard liveness
endpoint, expands the Grafana dashboard with a Coordinator row, bakes a
fully-commented example TOML into Docker images, adds four operations
runbooks, and ships a `just bump-version` recipe that eliminates future
version-drift risk. No breaking changes; no schema migrations required.

### Supply Chain & Release Automation

- **SBOM generation** — the release workflow now runs Syft and attaches a
  CycloneDX JSON SBOM (`pg-tide-sbom.cyclonedx.json`) to every GitHub
  release. Required for SOC 2 / FedRAMP buyers.
- **Trivy image scan** — Trivy scans the final Docker image for `CRITICAL`
  CVEs after the multi-arch manifest is merged. Results are uploaded to the
  GitHub Security tab as SARIF. The release job fails if any unfixed
  CRITICAL CVE is found.
- **`just bump-version VERSION` recipe** — single command that updates the
  Cargo.toml workspace version, both `pg_tide.control` files,
  and `helm/pg-tide/Chart.yaml` `version` / `appVersion` atomically,
  eliminating version-drift risk in future releases.

### Relay — Observability

- **`/healthz` HTTP endpoint** — the metrics server now serves `/healthz`
  as a Kubernetes-standard alias for the existing `/health` endpoint.
  Both return `200 OK` when no unhealthy pipelines are tracked, or
  `503 Service Unavailable` otherwise. Enables native Kubernetes liveness
  and readiness probes without external tooling.
- **Coordinator HealthState wired** — the coordinator's `health:
  Arc<RwLock<HealthState>>` field (previously `#[allow(dead_code)]`) is
  now updated at the end of each reconcile cycle to reflect the set of
  currently owned pipelines.

### Observability

- **Grafana dashboard — Coordinator row** — `pg-tide/dashboards/relay-health.json`
  gains a new "Coordinator" row with three panels: `pg_tide_relay_owned_pipelines`
  stat gauge, `pg_tide_relay_reconcile_duration_seconds` histogram (heatmap
  view), and `pg_tide_relay_pipeline_errors_total` by `error_class` time-series.

### Packaging

- **Example TOML in Docker images** — `/etc/pg-tide/pg-tide.example.toml`
  is now baked into both `:latest` and `:latest-full` images, providing a
  fully-commented starting configuration that operators can `docker cp` without
  consulting external docs.

### Configuration Clarity

- **Canonical config documentation** — new page
  `docs/src/relay-guide/catalog-vs-toml.md` declares the catalog (SQL) as
  the single source of truth for pipeline configuration and documents the
  expected role of the TOML file (process config only). Startup warning
  behaviour for TOML-only pipelines is documented.

### Operations Runbooks

Four new runbooks added under `docs/src/operations/`:

- **Crash recovery** — explains the at-least-once guarantee, how to identify
  and clear a stuck pipeline, and stale advisory lock resolution.
- **DLQ replay** — step-by-step guide for draining a flooded DLQ using
  `pg-tide replay dlq-requeue` and `tide.dlq_requeue()`, with monitoring
  guidance.
- **Schema migration** — how to apply `ALTER EXTENSION pg_tide UPDATE`
  without relay downtime, including CNPG-specific notes.
- **Relay upgrade** — rolling upgrade procedure for Kubernetes and
  Docker/systemd deployments with multiple relay instances.

---

## [0.18.0] — 2026-05-13 — Security Completeness, LISTEN Hot-Reload & API Polish

Server-side request forgery is a class of vulnerability where an application can be tricked into making network requests to internal infrastructure on an attacker's behalf. This release extends the SSRF protection that was introduced for webhook sinks in v0.13.0 to cover every HTTP-based sink in the relay: ClickHouse, Elasticsearch, and Arrow Flight now refuse to connect to loopback addresses, link-local ranges, private networks, and cloud metadata endpoints by default. Credentials in connection strings are no longer exposed in the process list: a new `--postgres-url-file` option reads the database URL from a file on disk, keeping sensitive strings out of system logs, monitoring tools, and container orchestrator dashboards.

The SQL API receives two quality-of-life improvements: the outbox pipeline registration function now accepts a single JSON object — mirroring the inbox equivalent — making it much easier to script pipeline setup from migrations or infrastructure-as-code. The `relay_enable()` and `relay_disable()` functions now return a boolean indicating whether they found and changed a pipeline, enabling scripts to detect typos in pipeline names without a separate existence check. Under the hood, the relay binary is reorganised into focused command modules so that each subcommand lives in its own file, making the codebase substantially easier to navigate and reducing the risk that a change to one subcommand accidentally affects another.

v0.18.0 completes the SSRF guard for all HTTP-based sinks, adds a
`--postgres-url-file` flag for secure credential injection, polishes the
SQL API with `relay_set_outbox_v2()` and boolean returns for
`relay_enable`/`relay_disable`, and refactors the relay binary into focused
`cmd/` modules for better maintainability. No breaking changes.

### Security

- **SSRF guard for ClickHouse, Elasticsearch, and Arrow Flight sinks** — a
  new shared validator (`http_util::validate_url`) blocks requests to loopback
  addresses (`127.x`, `::1`), link-local ranges (`169.254.x`, `fe80::`),
  private RFC 1918 ranges (`10.x`, `172.16–31.x`, `192.168.x`), and cloud
  instance-metadata endpoints (`169.254.169.254`, `fd00:ec2::`). HTTPS is
  required by default; set `allow_http = true` only for on-premises
  deployments. Set `ssrf_protection = false` to opt out for trusted private
  networks.
- **`--postgres-url-file` / `PG_TIDE_POSTGRES_URL_FILE`** — reads the
  PostgreSQL connection URL from a file instead of a command-line argument,
  preventing credential exposure in `/proc/<pid>/cmdline`. Takes precedence
  over `--postgres-url`.

### SQL API

- **`tide.relay_set_outbox_v2(config JSONB)`** — new single-JSONB-parameter
  form symmetric with `relay_set_inbox_v2()`. Accepted keys: `name`, `outbox`,
  `sink_type`, `config`, `batch_size`, `enabled`.
- **`tide.relay_enable(name text) → boolean`** — now returns `TRUE` if the
  pipeline was found and modified, `FALSE` if the pipeline was not found.
  Previously returned `void`; callers that ignored the return value are
  unaffected.
- **`tide.relay_disable(name text) → boolean`** — same semantics as
  `relay_enable`.

### Relay — Code Quality

- **`cmd/` module split** — `doctor`, `validate-config`, `replay`, `asyncapi`,
  `sweep`, and `status` subcommands are now implemented in dedicated modules
  under `pg-tide-relay/src/cmd/`. `main.rs` shrinks to under 280 lines.
- **Identifier validation at construction time** — `InboxSink` and
  `PgInboxSink` now call `validate_relay_identifier()` in `new()`, returning
  `Err` immediately for malformed identifiers rather than failing at first use.
- **Proper jitter for retry back-off** — replaced the inline LCG PRNG with
  `rand::rng().random_range()` (the `rand = "0.9"` crate).
- **`route_to_dlq()` helper** — DLQ insert paths in the worker loop are now
  unified through a single helper that correctly classifies transient vs
  permanent errors and increments the `pg_tide_relay_dlq_write_errors_total`
  metric on permanent failure.

### Testing

- 164 relay library unit tests pass (`cargo test --package pg-tide-relay --lib`).
- ClickHouse test fixtures updated for `allow_http` / `ssrf_protection` fields.
- Batch-inbox test updated for `InboxSink::new() → Result`.

---

## [0.17.0] — 2026-05-12 — Catalog Integrity, DLQ Reliability & Contract Correctness

A dead-letter queue is only useful if you can trust that failed messages actually end up there. This release changes the relay's behaviour when a DLQ write fails: instead of silently discarding the failure and carrying on, the affected pipeline is now paused immediately. A new Prometheus metric counts permanent DLQ write failures so the silence becomes audible — operators can alert on it and investigate before messages are lost. The `pg-tide doctor` pre-flight check also gains three new validations: whether the relay role can write to the DLQ table, whether an advisory lock is available, and whether LISTEN permission has been granted. Together these make it possible to catch misconfigured deployments before they silently swallow events.

Two correctness issues lurking in the SQL functions are fixed in this release. The `grant_publish` and `revoke_publish` security functions were susceptible to search-path injection — an attacker who could control their own PostgreSQL search path could potentially redirect these privileged functions to call their own code instead of the intended implementation. The fix pins both functions to the `tide` schema explicitly. Separately, several shadow copies of SQL functions that existed in upgrade scripts but were never cleaned up are removed, eliminating a class of subtle upgrade-path bugs where the wrong version of a function could be called depending on how the extension was installed or upgraded.

v0.17.0 closes a set of correctness and reliability gaps identified in the
v0.16.0 review cycle. There are no breaking SQL API changes — all changes are
backward-compatible at the SQL level.

### SQL API

- **`tide.grant_publish` / `tide.revoke_publish`** — `SECURITY DEFINER`
  functions now include `SET search_path = tide, pg_catalog` to prevent
  search-path injection attacks (OWASP A03). The upgrade migration applies
  `ALTER FUNCTION ... SET search_path` to existing databases.

### Extension SQL Correctness

- Removed residual plpgsql duplicates of `outbox_truncate_delivered`,
  `outbox_create_if_not_exists`, and `relay_set_inbox_v2` that were shadowing
  the canonical Rust `#[pg_extern]` implementations in upgrade paths.
- The `pg_tide--0.16.0--0.17.0.sql` upgrade script defensively drops any
  plpgsql copies of these functions (safe no-op if already absent).

### Relay — Error Handling

- **`exists()` helpers return `Result<bool>`** — `outbox_exists`,
  `inbox_exists`, and `relay_exists` now return `Result<bool, PgTideError>`
  instead of plain `bool`. SPI errors are propagated rather than silently
  returning `false` (which previously masked catalog access failures).

### Relay — DLQ Reliability

- **DLQ write failures pause the pipeline** — if a DLQ write fails with a
  permanent error, the worker now returns `Err` instead of silently swallowing
  the failure. The coordinator marks the pipeline as failed and stops retrying,
  preventing silent message loss.
- **New metric `pg_tide_relay_dlq_write_errors_total`** — counts permanent DLQ
  write failures labelled by `pipeline`. Visible in the Prometheus endpoint.

### Relay — `pg-tide doctor`

- Three new pre-flight checks added to `pg-tide doctor`:
  1. **DLQ INSERT privilege** — verifies the relay role can write to
     `tide.relay_dlq`; reports FAIL if missing.
  2. **Advisory lock availability** — tests `pg_try_advisory_lock` for the
     relay group key; reports WARN if another process holds the lock.
  3. **LISTEN permission** — checks that `LISTEN tide_relay_config` succeeds;
     reports FAIL if denied.

### Testing

- **SQL → relay → file-sink E2E test** (`sql_to_sink_e2e.rs`) — a new
  end-to-end integration test spins up a real PostgreSQL 18 container, applies
  the full migration chain, creates an outbox and pipeline, starts a live
  Coordinator, publishes a message, and asserts delivery to a file sink.

### CI

- **`no-stale-env-vars` CI job** — fails the build if any documentation file
  in `docs/` or `README.md` references the deprecated `PGTRICKLE_` env-var
  prefix.
- **SQL → relay → sink E2E job** — the new `sql_to_sink_e2e` test runs as a
  dedicated CI job (`test-e2e`) in addition to being part of the core
  integration test group.

### Documentation

- All occurrences of `PGTRICKLE_RELAY_*` in docs renamed to `PG_TIDE_*` to
  match the current binary env-var prefix.
- `examples/cnpg/cluster.yaml` updated: image tag bumped to `0.17.0` and
  `PG_TIDE_RELAY_POSTGRES_URL` renamed to `PG_TIDE_POSTGRES_URL`.

---

## [0.16.0] — 2026-05-11 — Developer Experience & Observability

Writing deployment scripts for database extensions typically involves error-prone boilerplate to handle the case where an object already exists. This release introduces `outbox_create_if_not_exists()`, which returns a simple true or false indicating whether the outbox was freshly created or was already there — no exception handling, no conditional logic, no repeated code. A matching variant for inbox pipelines makes scripted deployments equally clean. A new `pg-tide status` command prints a formatted table of all configured pipelines with their current consumer lag at a glance, so operators can get a health snapshot without connecting a monitoring tool or writing a SQL query.

Three new metrics expose the coordination internals that were previously a black box: how many pipelines each relay instance currently owns, how long each reconciliation cycle takes, and how errors are distributed between transient network problems and permanent configuration failures. Matching OpenTelemetry spans are emitted for every major processing step — filtering, routing, dead-letter queue writes, schema evolution checks, and backoff sleeps — so distributed traces show exactly where time is being spent in the message pipeline. Five architecture decision records are published that formalise the key design choices behind pg_tide, giving teams confidence that the system's behaviours are intentional and documented rather than accidental.

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

Accidental plaintext database connections are one of the most common security misconfigurations in production systems — they are invisible until someone is already looking at the traffic. This release changes the relay to fail closed: if `sslmode=require` is configured and the connection cannot be established securely, the relay exits with an error rather than quietly downgrading to plaintext. Secret values in pipeline configurations — API keys, passwords, tokens — are now redacted in log output before the configuration is emitted, so they cannot leak through centralised logging systems into dashboards or incident tickets.

The relay becomes significantly more resilient in this release. Workers that crash or panic are automatically detected and restarted by the coordinator. Database errors back off exponentially with jitter rather than hammering a struggling database at full speed. A connection pool separates coordinator metadata operations from pipeline workers so a single slow query cannot stall the entire relay. A new `pg-tide sweep` command can be called from a cron job or Kubernetes CronJob to delete consumed, expired outbox messages — keeping the outbox table from growing unbounded over time without requiring application-level cleanup logic or database administrator intervention.

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

When something goes wrong in an event-driven system — a consumer bug, a deployment error, an infrastructure hiccup — the ability to replay events is what separates a recoverable incident from a data loss event. This release delivers a complete replay workbench: operators can preview which messages would be replayed before committing, roll back a consumer's read position to a known-good point, and resolve or requeue individual failed messages from the dead-letter queue — all without taking the system offline. The CloudEvents 1.0 wire format makes pg_tide messages interoperable with any platform that speaks the standard event envelope, and a CLI command generates an AsyncAPI 3.0 document from catalog metadata for documentation and tooling integration.

Teams running pg_tide to serve multiple customers from a single database instance get first-class multi-tenancy in this release. Row-level security policies ensure that each tenant can only see and modify their own pipelines, and per-tenant labels on every Prometheus metric make it straightforward to build per-customer observability dashboards. For long-running data migrations, cataloged backfill jobs provide pause, resume, and fleet-wide status tracking — so a backfill that will take hours can be safely paused for a maintenance window and resumed without losing its place, and operators have a single view of all running jobs and their estimated completion times.

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

Production deployments need clear answers to two questions: who is allowed to publish events to this outbox, and what happens when the downstream system stops accepting them? This release answers both. Per-outbox publisher ACLs let database administrators grant and revoke the right to publish to specific outboxes at the role level — the same model PostgreSQL uses for table privileges. SSRF protection on webhook sinks prevents the relay from being used as a proxy to reach internal network resources. TLS and mutual TLS connection support rounds out the security surface, and a supply-chain audit via cargo-deny checks every dependency for known vulnerabilities and license compliance on every build.

Schema evolution — the reality that the shape of events changes as applications develop — is handled gracefully for the first time in this release. The relay computes a fingerprint of each pipeline's message payload schema and detects when new fields appear or existing ones disappear. A configurable policy determines what happens: log a warning and continue, pause the pipeline for operator review, or route affected messages to the dead-letter queue. Batch inserts for the PostgreSQL inbox sink reduce the number of database round-trips per relay cycle by an order of magnitude at typical batch sizes, and OpenTelemetry spans begin covering the full processing pipeline so distributed traces are available from the moment you enable the feature.

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

A relay that cannot deliver messages because the SQL schema and the relay binary disagree on column names is not much use. This release aligns every layer of the system so that configuring a pipeline through the SQL API actually produces a running, delivering relay worker. The inbox sink columns now match the tables that `inbox_create()` actually creates. Consumer offset tracking uses typed columns that both sides agree on. Publishing to a disabled outbox raises a clear error instead of silently succeeding. These fixes make the overall system work end-to-end for the first time, turning pg_tide from a collection of well-designed components into a complete, working product.

With the core functionality working, this release adds the tooling operators need to trust it in production. `pg-tide doctor` runs a comprehensive pre-flight check and reports whether the database is correctly configured, the required schema exists, and pipelines are properly registered — with a clear exit code for use in CI and deployment pipelines. `pg-tide validate-config` loads a named pipeline and attempts to instantiate its source and sink without processing any messages, making it the quickest way to verify a configuration change before it reaches a production relay. A SQL migration test verifies the entire upgrade path from v0.1.0 onwards on every CI run, ensuring no future upgrade introduces unexpected regressions.

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

Change data capture systems speak many different dialects. Debezium, the most widely deployed CDC tool in the JVM ecosystem, uses a specific message envelope format. MySQL's Maxwell and Alibaba's Canal use their own. Applications built on Kafka Connect have come to depend on these formats. This release makes pg_tide fluent in all of them: a new wire format abstraction decouples the message envelope from the transport layer, so a Kafka topic that was previously only readable by Debezium consumers can now deliver into a pg_tide inbox — and a pg_tide outbox can publish events in Debezium format so existing consumers do not need to change their deserialization code.

The wire format system is designed for extensibility and symmetry. Each format implements an encode-decode pair, so the same configuration works in both the forward direction — outbox to Kafka to a Debezium consumer — and the reverse direction — Debezium producer to Kafka to pg_tide inbox. Tombstone emission for log-compacted Kafka topics is built in. A custom CDC JSON format handles systems that do not use any of the named formats by accepting configurable field-path mappings so teams can express their own schema. Schema evolution detection runs on the decode side, surfacing alerts when inbound message shapes change in ways that could break downstream consumers before they have a chance to fail silently.

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

Transactional databases and analytical platforms have traditionally lived in separate worlds connected by fragile ETL pipelines that require dedicated infrastructure, careful maintenance, and deep expertise to operate. This release eliminates that gap by giving the relay direct delivery capabilities to every major analytical platform: ClickHouse for real-time analytics, MongoDB for document workloads, Snowflake and BigQuery for cloud data warehouses, Apache Iceberg and Delta Lake for open table formats on object storage, and DuckLake for the new DuckDB-native lake format. Events published to a pg_tide outbox can now flow directly into any of these systems without an intermediate queue, custom connector, or transformation layer.

All seven analytics sinks are designed for operational reliability, not just functional correctness. Messages are delivered in batches to minimise API call overhead. Where the platform supports it, the relay uses idempotency keys to safely retry failed deliveries without creating duplicates. Column statistics are written alongside data files for Iceberg and DuckLake, enabling query engines to prune irrelevant Parquet files and dramatically speed up analytical queries over large event archives. Each sink is a separate optional feature that can be included or excluded from the binary, keeping image sizes and attack surfaces small for deployments that do not need the full suite.

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

The Singer, Airbyte, and Fivetran ecosystems collectively offer hundreds of pre-built connectors to databases, SaaS APIs, and data warehouses. This release lets pg_tide speak all three protocols, immediately unlocking that entire connector library as both sources and sinks. A Singer tap can ingest data from Salesforce and have it arrive in a pg_tide inbox with automatic state tracking and schema drift detection. An Airbyte source connector can stream data from any of its supported platforms into a pg_tide pipeline without custom code. Fivetran HVR webhook payloads are verified with proper signature validation and ingested idempotently.

State management — knowing where each connector left off so it can resume incremental replication after a restart — is handled automatically. Singer STATE messages and Airbyte STATE objects are persisted in the pg_tide catalog and reloaded on relay restart, so connectors do not start over from the beginning every time the relay restarts or a pod is rescheduled on a new node. Schema drift detection surfaces when a connector's output schema changes in a way that could break downstream consumers, with the event logged or raised as an error according to the configured policy. This release also ships the first Grafana dashboard, giving operators a visual overview of relay health from the moment they deploy.

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

Not every message needs to end up in a database or a data lake — some need to wake a person up. This release adds native support for Slack, Discord, and PagerDuty as relay sinks. Database events can now trigger formatted Slack messages, Discord embeds with colour-coded operations, or PagerDuty incidents with proper deduplication using the relay message's dedup key. The formatting is opinionated and useful out of the box: inserts show up in green, deletes in red, and every notification carries enough context — subject, operation type, dedup key, and payload — to understand what happened without opening a separate tool or digging through logs.

For teams building high-throughput analytical pipelines, Apache Arrow Flight support enables columnar data transfer at speeds that HTTP-based sinks cannot match. Messages are encoded as Arrow RecordBatches and pushed via the standard `DoPut` gRPC RPC, compatible with Apache Arrow Flight servers, DataFusion, and a growing ecosystem of analytical query engines. The connection is established lazily and reused across batches, so the overhead of connection setup does not accumulate over a long-running relay. Bearer token authentication on the gRPC metadata is all that is needed to connect to secured Arrow Flight services, keeping the configuration simple while maintaining security.

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

A relay that stops and waits indefinitely when a downstream system is unavailable is not ready for production. This release adds the full suite of reliability features that production deployments require: a dead-letter queue so failed messages are never silently lost, a circuit breaker that pauses retries when a sink is repeatedly failing, exponential backoff between retry attempts to avoid overwhelming a recovering system, and per-pipeline rate limiting via a token bucket. Together these mechanisms transform the relay from a basic delivery loop into an operationally sound service that behaves predictably under stress and gives operators clear signals about the state of every pipeline.

Message routing and transformation capabilities arrive in this release. JMESPath expressions let operators filter out messages that do not match a condition before they reach the sink, and reshape message payloads to match a destination's expected format — all with configuration rather than code changes. The Confluent Schema Registry integration with Avro serialisation makes pg_tide a first-class participant in schema-governed Kafka ecosystems. SIGHUP config reload means that updating a pipeline's configuration takes effect without restarting the process, and a dry-run mode lets operators verify transform logic against live data before it modifies anything downstream.

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

The IoT and industrial automation worlds speak MQTT, and this release adds full bidirectional MQTT v5 support to pg_tide. The relay can publish outbox events to broker topics with configurable quality-of-service levels, or subscribe to topic filters and write incoming messages to a pg_tide inbox with automatic deduplication. Azure Event Hubs integration brings the same bidirectional capability to the Azure ecosystem: outbox events can be streamed to Event Hubs namespaces, and Event Hubs consumer groups can feed data back into pg_tide inboxes with per-partition offset tracking and idempotent delivery.

Object storage support rounds out this release and unlocks use cases where the destination is a data lake rather than a messaging system. The relay can buffer outbox messages and flush them to Amazon S3, Google Cloud Storage, or Azure Blob Storage as either JSONL files for universal compatibility or Apache Parquet files for analytical workloads. Date-based partitioning places files under year, month, and day prefixes that are natively understood by AWS Glue, BigQuery external tables, Hive Metastore, and most data lake cataloguing tools — making it trivial to build incrementally-loaded data warehouse tables on top of a pg_tide outbox without any additional transformation infrastructure.

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

Every major cloud provider now has a first-class integration in pg_tide. Google Cloud Pub/Sub, Amazon Kinesis Data Streams, and Azure Service Bus are all supported as both forward sinks and reverse sources — messages flow out from PostgreSQL to the cloud, and messages flowing into the cloud can be routed back into pg_tide inboxes with deduplication guaranteed. This symmetry means pg_tide can sit at the centre of a hybrid architecture, acting as the coordination layer between on-premises databases and cloud-native event streams without any custom adapter code or additional middleware.

Elasticsearch and OpenSearch support completes the analytics side of this release. Outbox messages are bulk-indexed with the relay message's dedup key as the document ID, making redeliveries safe and idempotent even if the relay restarts mid-batch. The index name supports template variables so events from different streams land in appropriately named indices without configuration changes for each new stream. With this release, pg_tide has first-class integrations with the five most commonly deployed cloud messaging and search platforms, making it a viable choice for any team that has standardised on one of the major cloud providers.

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

This release marks the completion of the relay binary. Every forward sink and every reverse source described in the project roadmap is now implemented, tested against a real service running in a container, and enabled by default. Redis Streams, Amazon SQS, a direct PostgreSQL inbox sink, and RabbitMQ join the existing forward sinks, completing coverage of the most commonly deployed messaging systems. All eight reverse sources — from NATS JetStream and Kafka through to a simple stdin reader for testing and one-shot imports — are now fully wired. The relay can move events in both directions between PostgreSQL and any supported messaging system with no skipped or stubbed tests.

The integration test suite is particularly noteworthy in this release: instead of relying on mocks or stubs, every backend is tested against a real instance of its target service running in a Docker container that is spun up and torn down automatically as part of CI. RabbitMQ 4, Redis 7, and NATS are all tested against their actual wire protocols. The switch from the heavyweight LocalStack AWS emulator to a lightweight SQS-compatible server cuts SQS test startup time from around thirty seconds to under two, a meaningful improvement when the test suite runs on every pull request. Dedup key stability across relay restarts is verified for every source backend.

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

The relay binary has had the ability to read pipeline configurations from PostgreSQL since v0.1.0, but until this release it would load the configuration and then do nothing with it. This release implements the complete coordinator run loop: pipeline workers are spawned for every enabled pipeline the relay wins an advisory lock on, and the relay automatically picks up new pipelines or restarts stopped workers without any intervention. Multiple relay instances can run simultaneously and will divide the pipeline workload between themselves, so a relay pod going down does not drop message delivery — another instance takes over its pipelines automatically, typically within a single reconciliation interval.

Credentials should never appear in log files or monitoring dashboards, but they frequently need to appear in pipeline configurations. This release solves the problem with secret interpolation: configuration values can contain tokens that are replaced at runtime with environment variable values, or tokens that read their value from a file — the standard pattern for Kubernetes secret mounts and Docker secrets. Resolved values are never written to logs or metrics. The relay binary is also renamed from its pg-trickle heritage to `pg-tide` in this release, with all environment variables, help text, and documentation updated to the new branding that will carry forward through production release.

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

## [0.2.0] — Post-0.1.0 Hardening & Observability

The first release of any complex system invariably surfaces edge cases and rough edges that only appear under real-world conditions. Version 0.2.0 is the hardening release that addresses the most important issues discovered after the initial launch: pgrx compatibility fixes ensure the extension builds and installs correctly across different PostgreSQL configurations, identifier quoting edge cases are resolved so outbox and inbox names containing hyphens or other special characters work reliably, and several low-level SQL generation issues are corrected. None of these changes require a database migration — they correct the compiled extension artifact only, so existing deployments upgrade safely by reinstalling the extension.

The relay binary gains its first dedicated observability metrics in this release: consumer lag — the number of messages published but not yet consumed by each consumer group — and delivery latency as a histogram, measuring the end-to-end time from when a message appears in the outbox to when it is acknowledged by the relay. Both metrics are visible at the Prometheus `/metrics` endpoint and provide the baseline signal needed to understand whether a relay deployment is keeping up with the workload. Docker image tagging is aligned with the convention used by major container registries, with full version, minor prefix, and `latest` tags published on every release.

---

## [0.1.0] — 2025-05-03 — Initial Release

pg_tide begins life as a focused extraction of the transactional outbox and idempotent inbox system from the pg_trickle project. Rather than a raw database table and a cron job, pg_tide provides a complete, opinionated solution: a SQL API for creating named outboxes and inboxes, publishing messages inside transactions, tracking consumer progress, and managing relay pipelines — all living in the `tide` schema within an ordinary PostgreSQL 18 database. The design goal is that application code should be able to publish an event and know it will be delivered, even if the destination system is temporarily unavailable, with no additional infrastructure beyond the database the application already uses.

The relay binary that ships with this first release already supports seven messaging backends: NATS JetStream, Apache Kafka, Redis Streams, RabbitMQ, Amazon SQS, HTTP webhooks, and stdout for testing. High availability is provided through PostgreSQL advisory locks — when multiple relay instances are running, they automatically negotiate ownership of each pipeline and take over from a crashed instance without operator intervention. A Prometheus metrics endpoint and structured JSON logging are available from day one, reflecting the project's commitment to observable, production-ready software from the first release rather than as an afterthought.

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
