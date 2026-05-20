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
| v0.5.0 | Cloud provider parity: Google Cloud Pub/Sub, Amazon Kinesis Data Streams, Azure Service Bus, Elasticsearch / OpenSearch | ✅ Released | Large | [CHANGELOG.md](CHANGELOG.md) |
| v0.6.0 | IoT and data lake: MQTT v5, Azure Event Hubs, Object Storage (S3 / GCS / Azure Blob with JSONL + Parquet) | ✅ Released | Large | [plans/relay-cli-phase2.md](plans/relay-cli-phase2.md) |

### Operational Excellence (v0.7.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| v0.7.0 | Production-grade relay operations: dead-letter queue, Confluent / Apicurio schema registry (Avro + Protobuf), JMESPath message transforms, content-based routing, rate limiting, circuit breaker, SIGHUP config reload, dry-run / replay mode, OpenTelemetry tracing, webhook signature verification (HMAC / GitHub / Stripe / Svix) | ✅ Released | Large | [plans/relay-cli-phase2.md](plans/relay-cli-phase2.md) |

### Notification & Analytics Sinks (v0.8.x – v0.11.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| v0.8.0 | Notification sinks (Slack, Discord, PagerDuty), Apache Arrow Flight / gRPC | ✅ Released | Large | [plans/relay-cli-phase3.md](plans/relay-cli-phase3.md) |
| v0.9.0 | Connector ecosystems (foundation): Singer protocol adapter (Meltano Hub — ~500 taps/targets) with full protocol compliance (STATE persistence in `tide.singer_state` for resumable incremental syncs, SCHEMA drift detection with configurable `on_schema_change` policy), Airbyte protocol adapter (~400 connectors), Fivetran HVR endpoint; Perses / Grafana relay health dashboard (`pg-tide/dashboards/relay-health.json`) covering per-pipeline throughput, error rate, DLQ depth, backlog, circuit breaker state, and forward latency | ✅ Released | Large | [plans/relay-cli-phase3.md](plans/relay-cli-phase3.md) |
| v0.10.0 | Analytics sinks: ClickHouse, MongoDB, Snowflake, BigQuery, Apache Iceberg, Delta Lake, DuckLake | ✅ Released | Large | [plans/relay-cli-phase3.md](plans/relay-cli-phase3.md) |

### Pluggable Wire Formats & CDC Ecosystem Parity (v0.11.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| v0.11.0 | Pluggable wire formats: Debezium bidirectional support (JSON first, then Avro/Confluent Schema Registry, then Protobuf) unlocking long-tail CDC sources (Oracle, Db2, MongoDB, Cassandra, Vitess, Spanner) in reverse and making pg_tide a first-class CDC producer for Debezium-shaped sinks (Apache Iceberg, Pinot, Druid, StarRocks, ksqlDB, Flink CDC, Materialize); Maxwell and Canal decoders; custom CDC JSON with user-supplied path expressions; tombstone emission for Kafka log-compacted topics | ✅ Released | Large | [plans/wire-formats.md](plans/wire-formats.md) |

### Contract Correctness, Security & Scale (v0.12.x – v0.14.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| v0.12.0 | Contract correctness & operational tooling: fix all critical seam breaks between SQL API and relay runtime, add `pg-tide doctor` + `validate-config` CLI, end-to-end SQL API test harness, Grafana dashboard regeneration, slim/full artifact strategy, Helm + docs alignment | ✅ Released | Large | [plans/overall_assessment_1.md](plans/overall_assessment_1.md) |
| v0.13.0 | Security hardening, reliability & performance: PostgreSQL TLS/mTLS, outbox-level ACLs, schema evolution guardrails, correct DLQ semantics, wire formats wired into runtime, complete metrics + OTel instrumentation, batch inbox inserts, connection pooling, SSRF guards, SECURITY DEFINER hardening, supply-chain (cargo-deny, SBOM, Trivy, cosign) | ✅ Released | Large | [plans/overall_assessment_1.md](plans/overall_assessment_1.md) |
| v0.14.0 | Replay workbench, CloudEvents, tenant scale & managed backfill: SQL + CLI replay with dry-run and DLQ resolution, CloudEvents wire format + AsyncAPI export, tenant-aware relay groups with RLS + per-tenant metrics, cataloged backfill jobs with chunking and pause/resume | ✅ Released | Large | [plans/overall_assessment_1.md](plans/overall_assessment_1.md) |

#### v0.12.0 — Contract Correctness & Operational Tooling (detail)

**SQL ↔ relay contract fixes**
- Align `relay_set_outbox()` / `relay_set_inbox()` emitted JSON with the coordinator's expected shape (`source_type`, `source.outbox`, `sink_type`, `sink.*`); add a versioned JSON Schema for pipeline configs and validate in both SQL helpers and relay startup.
- Fix relay consumer-offset schema mismatch: migrate `tide.relay_consumer_offsets` to `last_change_id BIGINT NOT NULL DEFAULT 0, worker_id TEXT` (or reconcile relay code with `last_offset` TEXT consistently).
- Fix pg-inbox sink columns to match extension-created inbox tables: insert `(event_id, source, payload, headers)` instead of `(event_id, event_type, payload, received_at)`; map `msg.subject` into `source` / `headers`.
- Enforce `enabled` flag in `outbox_publish()` — return an error for disabled outboxes.
- Add shared strict identifier validation for all dynamic SQL table/schema names in extension and relay code.
- Make `relay_list_configs()` return the full config JSON and propagate SPI errors rather than silently defaulting.

**CLI & observability tooling**
- `pg-tide doctor --postgres-url ...`: validates connectivity, schema version, TLS availability, feature flags.
- `pg-tide validate-config --pipeline NAME`: dry-runs source + sink factories against catalog config without processing messages.
- Grafana/Perses dashboard regenerated from typed metric name constants, fixing the `pgtide_relay_*` → `pg_tide_relay_*` name drift.
- `sink_max_inflight` wired into a real semaphore around publish work (or removed from docs until implemented).

**Packaging & infrastructure**
- Fix Helm `PG_TIDE_RELAY_POSTGRES_URL` → `PG_TIDE_POSTGRES_URL`; add Helm template unit test.
- Bump Helm chart `version`/`appVersion` to match workspace version during release automation.
- Decide and implement slim/full artifact strategy; release and Docker builds reflect advertised feature-gate coverage.
- Add `pg-tide-ext` extension artifacts (`.so`, control file, SQL files) to GitHub release.

**Test coverage**
- End-to-end SQL API test harness: testcontainers PostgreSQL + real relay worker tasks configured exclusively via `tide.*` SQL functions for one forward and one reverse pipeline.
- Sequential SQL migration upgrade tests: install 0.1.0, apply all upgrade scripts, run catalog assertions.

**Documentation**
- Correct PostgreSQL version claim (18-only for now) in version-compatibility docs.
- Fix feature-gate names (`cloud`, `analytics` do not exist; list actual per-backend gates).
- Reconcile version-availability table with changelog and roadmap.
- Fix broken README Getting Started link; update stale CNPG example schema/image versions.

#### v0.13.0 — Security Hardening, Reliability & Performance (detail)

**Security**
- PostgreSQL TLS/mTLS profiles: rustls-backed connections for coordinator, notification listener, worker, and remote PG sink; honor `sslmode=require` and fail closed when TLS is required but unavailable.
- Outbox-level publisher ACLs: `tide.outbox_publishers(outbox_name, role_name)` table, enforced in `outbox_publish()`; revoke table-wide INSERT on `tide_outbox_messages` from application roles.
- SSRF guard for webhook sinks: `https_only` option, allow/deny CIDR lists, DNS/IP checks, default rejection of link-local and loopback targets outside dev mode.
- `SECURITY DEFINER` hardening: create `tide_security_audit` table before the functions that write to it; add `SET search_path = tide, pg_catalog` to all definer functions.
- Supply-chain: `cargo-deny` with advisories, licenses, and bans in CI; SBOM (Syft), Trivy image vulnerability scan, cosign keyless signing on Docker + release artifacts; add Dependabot/Renovate for dependency update automation.

**Schema evolution**
- Schema Evolution Guardrails: store schema fingerprints per pipeline in catalog, classify additive vs. breaking changes, configurable `on_schema_change` policy (pause pipeline, route to DLQ, warn and continue, auto-create new stream).

**Reliability**
- Correct DLQ semantics: unique idempotent keys on DLQ entries; ack/commit the source after a durable DLQ write; `insert_batch()` reports partial failures rather than aborting on first error.
- Wire v0.11 wire-format factory (`wire_format::from_config()`) into coordinator, source, and sink runtime paths so Debezium/Maxwell/Canal/CDC-JSON config is active in real pipelines.
- Complete metrics instrumentation: increment consumed-counter after poll; observe end-to-end latency after source ack; set health/circuit-breaker gauges; expose DLQ depth counters; update `HealthState` on worker start/stop/error; wire OTel spans around poll, transform, publish, ack, DLQ, and replay-filter boundaries.

**Performance**
- Batch pg-inbox inserts: replace per-row `INSERT` loop with multi-row `INSERT … UNNEST` or `COPY` with `ON CONFLICT DO NOTHING`.
- Connection pooling for relay workers (`deadpool-postgres` or equivalent); configurable max-owned-pipelines and max-connections limits to prevent connection exhaustion.

#### v0.14.0 — Replay Workbench, CloudEvents, Tenant Scale & Managed Backfill (detail)

**Replay Workbench**
- SQL functions and CLI commands for range preview (inspect messages that would be replayed), dry-run transform evaluation, targeted sink replay, and DLQ entry resolution/requeue.
- `commit_offset()` monotonicity guard (`WHERE committed_offset <= EXCLUDED.committed_offset`) with an explicit admin rewind API for intentional offset rollback.
- `inbox_status(NULL)` returns a fleet summary across all configured inboxes.

**CloudEvents & AsyncAPI**
- CloudEvents wire format (v1.0) as a first-class relay encoding option.
- `pg-tide asyncapi export`: generates AsyncAPI 3.0 documents from relay catalog metadata and observed message schemas.

**Tenant-Aware Relay Groups**
- Tenant discriminator column in `tide.relay_outbox_config`, `tide.relay_inbox_config`, and `tide.relay_consumer_offsets`.
- Row-level security policies scoped per tenant; `tide.relay_set_tenant()` / `tide.relay_grant_tenant()` admin API.
- Per-tenant Prometheus label dimension; tenant-scoped advisory lock namespacing for pipeline ownership.

**Managed Backfill Jobs**
- Cataloged backfill job table with configurable chunk size, progress tracking (rows processed, estimated completion), pause/resume, and relay-side throttling to avoid starving live CDC pipelines.

### Hardening, Scale & Developer Experience (v0.15.x – v0.16.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| v0.15.0 | Security hardening & production scale: wire TLS into all relay connections, connection pooling, max-owned-pipelines CLI config, transient/permanent error classification, relay-side identifier validation, raw outbox payload mode, exponential backoff, worker panic detection | ✅ Released | Large | [plans/overall_assessment_2.md](plans/overall_assessment_2.md) |
| v0.16.0 | Developer experience & observability: end-to-end SQL→relay integration tests, SQL migration upgrade-path tests, property-based wire-format tests, OTel span coverage, new coordinator metrics, Grafana dashboard codegen, Helm security contexts, documentation ADRs, `pg-tide status` CLI, code quality cleanup | ✅ Released | Large | [plans/overall_assessment_2.md](plans/overall_assessment_2.md) |

#### v0.15.0 — Security Hardening & Production Scale (detail)

**Security**
- **Wire `pg_tls` into all connection points** — replace all 11+ `tokio_postgres::connect(url, NoTls)` call sites in `main.rs` and `coordinator.rs` with `pg_tls::connect(url)`, which honours `sslmode=require`/`verify-full`/`verify-ca` parsed from the connection URL.  Fail closed when TLS is required but the server does not advertise it.  Relay-side identifier validation applied to all config-sourced table names before SQL construction.
- **Redact secret values from logs** — `${ENV:VAR_NAME}` resolved values are masked in structured log output so credentials do not appear in log aggregators.
- **Integration test with TLS-enabled PostgreSQL testcontainer** — new test fixture that spins up a PostgreSQL 18 testcontainer with TLS and verifies the relay connects successfully and rejects plaintext when `sslmode=require`.

**Reliability**
- **Transient vs. permanent error classification** — `RelayError` gains `is_transient()` predicate; permanent errors (bad credentials, schema mismatch, auth rejection) immediately pause the pipeline without exhausting retries.
- **Worker panic detection** — `tokio::spawn` join handles are stored; coordinator reconcile loop checks for completed/panicked tasks and cleans up `owned` map entries immediately rather than waiting up to 30s.
- **Exponential backoff with jitter on poll errors** — consecutive poll failures sleep 1 s, 2 s, 4 s … up to a configurable ceiling instead of a flat 1 s loop.

**Performance & Scale**
- **Connection pooling** — `deadpool-postgres` replaces the one-connection-per-pipeline model; configurable `max_connections` prevents PostgreSQL connection exhaustion on managed databases (e.g. RDS, Cloud SQL) with low limits.
- **`max_owned_pipelines` exposed in CLI/TOML** — `--max-pipelines N` / `PG_TIDE_MAX_PIPELINES` / `max_owned_pipelines` TOML key; default 50 unchanged.
- **Raw pg_tide outbox payload mode** — source handles messages published directly via `tide.outbox_publish()` (no `v:1` pg_trickle envelope) by treating the payload as a native JSONB event.
- **Guard claim-check paths** — runtime check detects absence of `tide.outbox_delta_rows_*` tables and returns a clear error directing users to pg_trickle; `pg-tide doctor` surfaces this pre-flight.
- **Outbox retention sweeper** — background task or `pg-tide sweep` CLI command calls `tide.outbox_truncate_delivered()` on a configurable schedule to prevent unbounded table growth.
- **Schema registry passthrough mode** — `schema_registry.mode = "passthrough"` forwards Confluent wire-format bytes directly without deserialising/re-serialising, halving overhead for Kafka→Kafka routing.

#### v0.16.0 — Developer Experience & Observability (detail)

**Test Coverage**
- **End-to-end SQL API → relay → sink test** — testcontainers test that calls `tide.relay_set_outbox()` via SQL, starts a real relay coordinator task, publishes via `tide.outbox_publish()`, and asserts delivery at the sink.
- **Sequential SQL migration upgrade-path test** — installs 0.1.0, applies all 13 incremental upgrade scripts in order, and runs catalog integrity assertions after each step.
- **Property-based wire-format tests** — `proptest` covering `WireFormat::decode` → `encode` round-trips for Debezium JSON, Maxwell, Canal, CloudEvents, and native formats across randomised payloads.
- **Parallelise integration CI** — split 40+ integration test files into parallel job groups by backend category; target CI wall-clock time under 15 minutes.

**Observability**
- **New coordinator metrics** — `pg_tide_relay_owned_pipelines` gauge, `pg_tide_relay_reconcile_duration_seconds` histogram, `pg_tide_relay_pipeline_errors_total` labelled by error class.
- **OTel span coverage expansion** — add spans for transform evaluation, content-based routing, DLQ insert, schema evolution check, and backoff sleep boundaries.
- **Grafana dashboard codegen** — generate `relay-health.json` from a Rust template that references metric name constants; a CI check validates all referenced metric names exist in `metrics.rs`.

**Developer Experience**
- **`pg-tide status` CLI** — connects to PostgreSQL and prints a table of pipeline names, directions, enabled state, last offset, consumer lag, and circuit-breaker state.
- **`securityContext` defaults in Helm chart** — `runAsNonRoot: true`, `runAsUser: 1000`, `readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false` matching the Dockerfile's non-root user.
- **Helm chart version automation** — release workflow bumps `version` and `appVersion` in `helm/pg-tide/Chart.yaml` as part of the version bump step.
- **Docker "full" image** — release workflow builds a second Docker image tagged `:latest-full` with all optional features compiled in; the slim `:latest` retains the default-feature set.
- **CI link-checker** — `mlc` or `lychee` step validates all URLs in README and docs on every PR.
- **Architecture Decision Records (ADRs)** — `docs/adr/` directory with records for: single-table outbox design, advisory-lock coordination, wire-format abstraction, JSONB catalog config, and feature-gated binary.
- **`relay_set_inbox()` parameter reduction** — refactor to accept a single JSONB config parameter with documented keys, adding a compatibility shim for the existing 8-parameter signature.
- **Code quality cleanup** — remove `#![allow(dead_code, unused_imports)]` from `main.rs` and `lib.rs`; use targeted per-item allows; extract `worker_inner()` publish/DLQ logic into standalone helper functions; replace `.expect()` in singer source and webhook with proper `RelayError` propagation.
- **`outbox_create_if_not_exists()` helper** — idempotent outbox creation for deployment scripts; existing `outbox_create()` behaviour unchanged.
- **`relay_enable()` / `relay_disable()` documentation** — clarify intentional silent no-op when pipeline does not exist.
- **Dependency review** — evaluate `jmespath` crate alternatives; track `prometheus-client` migration path.
- **Cosign signing** — add keyless cosign signing to Docker images and release binary artifacts in the release workflow.

### Contract Integrity, Security & Operational Maturity (v0.17.x – v0.19.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| v0.17.0 | Catalog integrity, DLQ reliability & contract correctness: fix fresh-install vs upgrade schema drift, deduplicate plpgsql/Rust function definitions, pause pipelines on DLQ write failure, land the SQL→relay→sink E2E test, fix CNPG example, rename PGTRICKLE_RELAY_* docs | ✅ Released | Medium | [plans/overall_assessment_3.md](plans/overall_assessment_3.md) |
| v0.18.0 | Security completeness, LISTEN hot-reload & API polish: shared SSRF validator for HTTP sinks, identifier validation in inbox sinks, coordinator subscribes to `tide_relay_config` NOTIFY for instant hot-reload, `relay_set_outbox_v2()`, extended `pg-tide doctor` checks, DLQ fault-injection tests, property tests for JMESPath/routing, jitter fix, code structure cleanup | ✅ Released | Medium | [plans/overall_assessment_3.md](plans/overall_assessment_3.md) |
| v0.19.0 | Supply chain, observability completeness & operational docs: SBOM + Trivy scan, automated `just bump-version` recipe, canonical config documentation (catalog over TOML), bake example TOML into Docker images, operations runbooks (crash recovery, DLQ replay, schema migration, relay upgrade), expand Grafana dashboard with coordinator metrics panel | ✅ Released | Small | [plans/overall_assessment_3.md](plans/overall_assessment_3.md) |

#### v0.17.0 — Catalog Integrity, DLQ Reliability & Contract Correctness (detail)

**Extension catalog integrity**
- **Fix `extension_sql_file!()` chain** — replace the two-file `pg_tide--0.1.0.sql` + `pg_tide--0.13.0--0.14.0.sql` pgrx include with an ordered chain covering every `sql/pg_tide--*.sql` migration, so that a fresh `CREATE EXTENSION pg_tide` and an `ALTER EXTENSION … UPDATE` chain produce identical catalog schemas. Add a CI test that diffs `pg_dump --schema-only` of both paths.
- **Deduplicate plpgsql vs Rust function definitions** — `outbox_truncate_delivered`, `outbox_create_if_not_exists`, and `relay_set_inbox_v2` currently exist both as `#[pg_extern]` in Rust and as `CREATE OR REPLACE FUNCTION` in migration scripts, with divergent signatures. Remove the plpgsql duplicates from the migration scripts and declare Rust as the single source of truth. Add a unit test that asserts each function name resolves to exactly one signature.
- **Harden base SQL `SECURITY DEFINER` functions** — the `grant_publish()` / `revoke_publish()` in `pg_tide--0.1.0.sql` are missing `SET search_path = tide, pg_catalog`. Fix in the base file so fresh installs get the hardened definitions, not just upgrade paths.
- **Convert `outbox_exists()` / `inbox_exists()` / `relay_exists()` to `Result<bool, PgTideError>`** — replace the silent `unwrap_or(None).unwrap_or(false)` chains with proper SPI error propagation.

**DLQ reliability**
- **Pause pipelines on DLQ write failure** — a failed `dlq::insert_batch()` currently logs at WARN and continues, causing a tight loop on poisoned batches. Classify DLQ write errors via `RelayError::is_transient()` and pause the worker on permanent failures. Add `pg_tide_relay_dlq_write_errors_total` counter labelled by `pipeline`.
- **Extend `pg-tide doctor`** — validate: (a) `tide.relay_dlq` INSERT privilege; (b) advisory-lock acquisition under the configured `relay_group_id`; (c) LISTEN permission for `tide_relay_config`.

**Test coverage**
- **SQL → relay → sink end-to-end test** — `tests/sql_to_sink_e2e.rs`: spawn coordinator task → `tide.relay_set_outbox(…, 'stdout', …)` via SQL → `tide.outbox_publish(…)` → assert message captured by the stdout sink. This test permanently locks in the v0.12.0 SQL/relay contract.
- **Schema diff CI check** — assert fresh-install and upgrade-chain `pg_dump` outputs are identical.

**Documentation & examples**
- **Rename `PGTRICKLE_RELAY_*` → `PG_TIDE_*`** across all docs (~20 references in `docs/src/getting-started/first-pipeline.md`, `operations/troubleshooting.md`, `operations/deployment-guide.md`, `relay-guide/configuration.md`). Add a CI assertion that blocks any future reintroduction of the old prefix.
- **Fix `examples/cnpg/cluster.yaml`** — bump image tag from `0.1.0` to current release; rename env var from `PG_TIDE_RELAY_POSTGRES_URL` to `PG_TIDE_POSTGRES_URL`.

#### v0.18.0 — Security Completeness, LISTEN Hot-Reload & API Polish (detail)

**Security**
- **Shared SSRF validator for HTTP sinks** — extract `webhook::validate_ssrf()` into a shared `relay::http::validate_url()` helper and apply it to ClickHouse (`sink/clickhouse.rs`), Apache Arrow Flight (`sink/arrow_flight.rs`), and Elasticsearch (`sink/elasticsearch.rs`) sinks. Add `ssrf_protection: bool` (default `true`) to each. Mitigates SSRF via compromised catalog entries.
- **Identifier validation in inbox sinks** — call `validate_relay_identifier()` at construction time in `InboxSink` (`sink/inbox.rs`) and `PgInboxSink` (`sink/pg_outbox.rs`) to close the defence-in-depth gap identified in overall_assessment_3 §2.2.
- **`--postgres-url-file` CLI flag** — add alongside `--postgres-url` and document `PG_TIDE_POSTGRES_URL` as the preferred form to avoid credential exposure in `/proc/<pid>/cmdline`.

**Coordinator hot-reload**
- **Subscribe to `tide_relay_config` LISTEN channel** — the PostgreSQL trigger `relay_config_notify()` has been emitting `pg_notify('tide_relay_config', name)` since v0.1.0; the coordinator has never listened. Wire an async LISTEN loop that triggers an immediate reconcile on receipt, reducing config-change propagation from up to 30 s to sub-second. Keep the existing 30 s poll timer as a safety net.

**SQL API**
- **`tide.relay_set_outbox_v2(config JSONB)`** — single-JSONB-parameter counterpart to `relay_set_inbox_v2()` for symmetric API ergonomics. Accepts keys: `name`, `outbox`, `sink_type`, `config`, `batch_size`, `enabled`, `wire_format`. Mark the 6-positional-parameter `relay_set_outbox()` as deprecated in the SQL comment and changelog.
- **`relay_enable()` / `relay_disable()` — return affected row count** — change from silent no-op to returning `BOOLEAN` (`TRUE` if a row was modified, `FALSE` if the pipeline didn't exist), matching the `outbox_create_if_not_exists()` pattern.

**Code quality**
- **Replace pseudo-random jitter** — substitute the LCG-based `consecutive_failures * 6_364_136_223_846_793_005_u64` jitter calculation in `coordinator.rs` with `rand::thread_rng().gen_range(…)` so that concurrent pipelines failing at the same instant do not choose identical backoff offsets.
- **Extract `worker_inner()` helpers** — split the ~507-line function into `process_batch()`, `publish_with_circuit_breaker()`, and `route_to_dlq()`, each independently unit-testable.
- **Split `main.rs` into `cmd/` modules** — move each subcommand implementation (`run_doctor`, `run_validate_config`, `run_sweep`, `run_status`, `run_replay_*`, `run_asyncapi_export`) into `cmd/doctor.rs`, `cmd/status.rs`, etc.; keep `main.rs` under 150 lines.

**Test coverage**
- **DLQ fault-injection test** — revoke INSERT on `tide.relay_dlq`; assert the worker pauses (permanent error path) rather than looping at WARN.
- **Property tests for JMESPath, identifier validation, and routing** — extend `proptest` to cover `JmespathTransform::evaluate`, `validate_relay_identifier`, and `routing::apply_routing` with randomised inputs.

#### v0.19.0 — Supply Chain, Observability & Operational Docs (detail)

**Supply chain & release automation**
- **SBOM generation** — add a Syft step to the release workflow to produce a CycloneDX or SPDX SBOM and attach it to GitHub releases. Required for SOC 2 / FedRAMP buyers.
- **Trivy image scan** — add Trivy vulnerability scanning to the release workflow; fail on `CRITICAL` CVEs in the final Docker images.
- **`just bump-version VERSION` recipe** — single command that updates `Cargo.toml` workspace version, `pg_tide.control` `default_version`, and `helm/pg-tide/Chart.yaml` `version` / `appVersion` atomically, eliminating future version-drift risk.

**Configuration clarity**
- **Canonical config documentation** — add a page to `docs/src/relay-guide/` declaring the catalog (SQL) as the single source of truth for pipeline configuration. When a TOML file configures pipelines that are not present in the catalog, emit a startup warning and document the expected resolution workflow.
- **`/healthz` HTTP endpoint** — wire the existing `health: Arc<RwLock<HealthState>>` field (currently `#[allow(dead_code)]`) to a minimal Axum route that returns `200 OK` / `503 Service Unavailable` based on coordinator state, enabling Kubernetes liveness probes without external tooling.

**Observability**
- **Grafana dashboard coordinator panel** — add a "Coordinator" row to `pg-tide/dashboards/relay-health.json` with three panels: `pg_tide_relay_owned_pipelines` gauge, `pg_tide_relay_reconcile_duration_seconds` heatmap, and `pg_tide_relay_pipeline_errors_total` by `error_class`. Regenerate via the existing Rust-constant-backed codegen CI check.

**Packaging**
- **Bake example TOML into Docker images** — add `/etc/pg-tide/pg-tide.example.toml` (fully commented) to both `:latest` and `:latest-full` images so operators can `docker cp` a working starting config without consulting external docs.

**Operations runbooks**
- **Crash recovery** — document what happens when the relay crashes mid-batch (at-least-once guarantee; no action needed beyond restart), including how to identify and clear a stuck pipeline.
- **DLQ replay** — step-by-step guide for using `pg-tide replay dlq-requeue` and `tide.dlq_requeue()` to drain a flooded DLQ, including how to monitor progress with `pg_tide_relay_dlq_entries_written_total`.
- **Schema migration** — guide for applying `ALTER EXTENSION pg_tide UPDATE` without relay downtime.
- **Relay upgrade** — rolling upgrade procedure for HA deployments with multiple relay instances.

---

### Data Lake Ecosystem — DuckLake Integration (v0.20.x – v0.22.x)

pg-tide and DuckLake share a foundational bet: PostgreSQL is the right place to coordinate critical data operations. DuckLake stores its entire lakehouse catalog — snapshots, file registrations, column statistics, schema evolution history — as ordinary PostgreSQL tables. pg-tide's outbox and relay live in the same database. This co-location makes it possible to commit "outbox offset consumed" and "DuckLake snapshot created" in a single PostgreSQL transaction, delivering an exactly-once guarantee from OLTP event to data lake that no other pipeline tool can currently offer. The three releases below build this integration progressively: first the correct catalog wire protocol, then streaming-optimised inlining and schema evolution, then bidirectional flow and the full ecosystem surface.

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| v0.20.0 | DuckLake native catalog integration: upgrade the DuckLake sink to speak the real DuckLake v1.0 spec (write `ducklake_snapshot`, `ducklake_data_file`, `ducklake_file_column_stats`), same-transaction atomicity mode, NOTIFY-based lake change notifications, column statistics for filter pushdown, `tide.ducklake_attach()` SQL helper, auto-create DuckLake schema/table on first pipeline | ✅ Released | Large | [plans/ecosystem/ducklake.md](plans/ecosystem/ducklake.md) |
| v0.21.0 | DuckLake streaming, inlining & schema evolution: data inlining for sub-threshold batches (rows written directly to `ducklake_inlined_data_*` tables instead of Parquet files — eliminating the small-files problem for streaming workloads), automatic schema evolution bridge (new JSON fields in outbox messages trigger `ducklake_column` entries), snapshot-to-consumer-offset mapping (time-travel replay via `AT (VERSION => N)`), auto-partition by time/event-type, DLQ archive to DuckLake table | ✅ Released | Large | [plans/ecosystem/ducklake.md](plans/ecosystem/ducklake.md) |
| v0.22.0 | DuckLake bidirectional flow & ecosystem surface: reverse relay source (poll new DuckLake snapshots → deliver to pg-tide inbox, enabling lake-to-application data flow), cross-lake replication (DuckLake → DuckLake fan-out via pg-tide as transport), `pg-tide ducklake` CLI sub-commands (`snapshots`, `checkpoint`, `flush-inlined`), Docker Compose getting-started example (PostgreSQL + pg-tide + DuckLake + DuckDB + Grafana in a single `docker compose up`), tutorial suite and conference demo scripts | ✅ Released | Large | [plans/ecosystem/ducklake.md](plans/ecosystem/ducklake.md) |

#### v0.20.0 — DuckLake Native Catalog Integration (detail)

This release replaces the current custom `tide.ducklake_snapshots` catalog schema with the real DuckLake v1.0 specification tables, making all data written by the pg-tide relay immediately queryable by DuckDB — with no glue code, no extra tooling, and no migration. From the moment this release ships, any DuckDB instance can `ATTACH 'ducklake:postgres:...'` to the same PostgreSQL database and query the event history with full time-travel, filter pushdown, and schema evolution support.

**Native DuckLake v1.0 catalog writes**
- Replace the `tide.ducklake_snapshots` table with the official 28-table DuckLake catalog schema (`ducklake_snapshot`, `ducklake_snapshot_changes`, `ducklake_data_file`, `ducklake_table_stats`, `ducklake_table_column_stats`, `ducklake_file_column_stats`, `ducklake_schema`, `ducklake_table`, `ducklake_column`, `ducklake_metadata`, and the remaining supporting tables per spec v1.0).
- For each relay batch, execute a single PostgreSQL transaction that: writes the Parquet file to object storage, inserts into `ducklake_data_file` with correct `begin_snapshot`, `record_count`, `file_size_bytes`, and `footer_size`; increments `ducklake_table_stats.next_row_id`; upserts global column min/max in `ducklake_table_column_stats`; writes per-file column statistics to `ducklake_file_column_stats`; creates a new `ducklake_snapshot` entry with monotonically increasing ID from a PostgreSQL sequence; and appends a `ducklake_snapshot_changes` record tagged `author = 'pg-tide-relay'`.
- Auto-bootstrap: if a DuckLake schema and table do not yet exist for a given outbox stream, the sink creates them (inserts into `ducklake_schema`, `ducklake_table`, and `ducklake_column`) as part of the first batch, requiring no manual DDL.
- Migration path: a one-time `tide.ducklake_migrate_catalog()` function converts any existing `tide.ducklake_snapshots` rows into the new format and drops the old table.

**Same-transaction atomicity mode**
- When the relay is connected to the same PostgreSQL instance that hosts both the pg-tide outbox and the DuckLake catalog, enable an opt-in `atomic_lake_writes = true` sink option that wraps the outbox consumer-offset advance and the DuckLake catalog commit inside a single database transaction.
- This makes pg-tide the only pipeline tool that can guarantee exactly-once delivery from a PostgreSQL transaction to a data lake: either the offset advances and the snapshot is committed together, or neither happens. No duplicate events survive a relay crash.
- Expose as `tide.relay_set_outbox_v2(...)` config key `"ducklake_atomic": true`; document the requirement that the relay's `--postgres-url` must point at the catalog database.

**Column statistics for filter pushdown**
- While building each Parquet batch, compute per-column min/max values, null counts, and distinct-value counts. Write these to `ducklake_file_column_stats` so that DuckDB can prune Parquet files during query planning without reading them — critical for large event archives with selective queries.
- Support statistics for all DuckLake-compatible column types that appear in pg-tide messages: `VARCHAR` (dedup key, subject, op), `BIGINT` (outbox ID), and JSONB-flattened scalar fields.

**NOTIFY-based DuckLake change notifications**
- After committing each DuckLake snapshot, issue `pg_notify('tide_ducklake_changes', json_build_object('table', table_name, 'snapshot_id', new_snapshot_id)::text)`. External consumers — application services, incremental materialized view refreshers, or downstream relay instances — subscribe and receive near-real-time notification of new lake data without polling.
- Document the LISTEN/NOTIFY pattern for DuckDB-based consumers in the operations guide.

**`tide.ducklake_attach()` SQL helper**
- A convenience function that returns the DuckDB `ATTACH` statement pre-populated with the correct PostgreSQL connection string and catalog database name, removing friction for first-time users.

```sql
SELECT tide.ducklake_attach();
-- Returns: ATTACH 'ducklake:postgres:dbname=mydb host=localhost' AS my_ducklake (DATA_PATH 's3://my-bucket/events/');
```

#### v0.21.0 — DuckLake Streaming, Inlining & Schema Evolution (detail)

This release tackles the two hardest problems for streaming workloads on data lakes: the small-files problem and schema drift. DuckLake's data inlining feature stores small writes directly in the PostgreSQL catalog database — zero Parquet files created, sub-millisecond write latency, full time-travel preserved. DuckLake's benchmarks demonstrate 926× faster query performance and 105× faster ingestion compared to Iceberg for streaming workloads. pg-tide is the perfect producer for inlining because its outbox is already in PostgreSQL: the relay can batch inlined writes without any extra network hops.

**Data inlining for streaming workloads**
- Add an `inline_row_limit` option to the DuckLake sink config (default: 10, matching DuckLake's default). Batches at or below this threshold are written directly to `ducklake_inlined_data_{table_id}_{schema_version}` in the catalog rather than flushing to Parquet.
- For inlined inserts: write rows with `row_id`, `begin_snapshot`, `end_snapshot = NULL`, and the message payload columns. For inlined deletes (DLQ-tombstone ops): set `end_snapshot` on existing inlined rows rather than creating a delete file.
- Above the threshold, the existing Parquet-write path is used. The inlining and Parquet paths are transparent to DuckDB consumers: queries always return the correct result regardless of where the data lives.
- Expose a `pg-tide ducklake flush <pipeline>` CLI command that triggers a `CHECKPOINT` equivalent — materialising all inlined data to a consolidated Parquet file and releasing the catalog storage — for use in scheduled maintenance windows.

**Automatic schema evolution bridge**
- On each relay batch, compare the JSON keys present in the current message payloads against the known `ducklake_column` entries for the target table. When new keys are detected, classify the change: additive (new nullable column) vs. breaking (type conflict on existing key).
- For additive changes: within the snapshot transaction, insert new `ducklake_column` rows with `begin_snapshot = new_snapshot_id` and `nulls_allowed = true`. DuckDB handles missing values in older Parquet files transparently via column projection. Update the Parquet schema for the current file to include the new column.
- For breaking changes: apply the pipeline's configured `on_schema_change` policy — `pause`, `route_to_dlq`, `warn_and_continue`, or `auto_new_stream` — matching the v0.13.0 guardrail semantics.
- Expose the schema fingerprint and detected-column history in a new `tide.ducklake_column_history(pipeline_name)` SQL view for observability.

**Snapshot-to-consumer-offset mapping**
- Record a mapping from pg-tide consumer group offset to DuckLake snapshot ID in a new `tide.ducklake_offset_map(pipeline_name, consumer_group, outbox_offset, snapshot_id, committed_at)` table, written atomically with each snapshot commit.
- This enables consumers to ask: "give me all events since I last checkpointed" by querying `FROM events AT (VERSION => last_snapshot_id + 1)` through the latest snapshot — turning the DuckLake table into a replayable, SQL-queryable event log with no message broker required.
- Expose a `tide.ducklake_replay_range(pipeline, from_offset, to_offset)` function that returns the corresponding DuckDB `AT (VERSION => …)` range expression, ready to paste into a DuckDB session.

**Auto-partition by time and event type**
- When creating a new DuckLake table for an outbox stream, inspect the configured `wire_format` and outbox metadata to determine whether to apply hidden partitioning. For event streams, default to daily time partitioning on `_committed_at`. For subject-routed streams, offer bucket partitioning on `_subject`.
- Register partition information in `ducklake_partition_info` and `ducklake_partition_column` so that DuckDB's query planner can prune Parquet files for time-range and event-type queries without scanning the full archive.
- Expose `"ducklake_partition": "daily" | "monthly" | "bucket:N" | "none"` in the sink config.

**DLQ archive to DuckLake**
- Add a background sweeper task that periodically moves aged-out DLQ entries (older than a configurable `dlq_archive_after` TTL) from `tide.relay_dlq` into a dedicated DuckLake table `dlq_archive` in the same catalog schema.
- Archived entries are fully queryable via DuckDB with time-travel and filter pushdown, keeping the operational DLQ table small while providing unlimited, auditable history of every failed message delivery.

#### v0.22.0 — DuckLake Bidirectional Flow & Ecosystem Surface (detail)

With the inbound pipeline (PostgreSQL → DuckLake) solid, this release opens the reverse direction and builds the full ecosystem surface: CLI tooling, getting-started materials, and community integration that positions pg-tide as the recommended ingestion and egestion layer for PostgreSQL-backed DuckLakes.

**Reverse relay: DuckLake → pg-tide inbox source**
- Implement a `ducklake` source type in the relay that watches a DuckLake table for new snapshots by polling `SELECT max(snapshot_id) FROM ducklake_snapshot WHERE snapshot_id > $last_seen`. When a new snapshot appears, fetch the incremental rows (using DuckLake's incremental scan semantics between the last-seen and current snapshot IDs) and deliver them into a pg-tide inbox with full deduplication.
- This enables lake-to-application data flow: ML model outputs, enriched analytics results, cross-system aggregations, and regulatory reports written to DuckLake by any engine (DuckDB, Spark, DataFusion, Trino) can be consumed by application services via the familiar pg-tide inbox API.
- Configure via `tide.relay_set_inbox_v2(...)` with `"source_type": "ducklake"` and keys `"catalog_connection"`, `"schema"`, `"table"`, `"snapshot_poll_interval_ms"`.

**Cross-lake replication**
- Chain two DuckLake pipelines: a DuckLake source pulling new snapshots from a source lake into a pg-tide inbox, and a DuckLake sink writing from an outbox to a destination lake. This enables multi-region or multi-cloud DuckLake federation — with the relay handling delivery guarantees, deduplication, and fan-out routing — without any external ETL tool.
- Provide a `tide.ducklake_replicate(source_catalog, source_table, dest_catalog, dest_table)` convenience function that configures both pipelines automatically.

**`pg-tide ducklake` CLI sub-commands**
- `pg-tide ducklake snapshots <pipeline>` — lists all DuckLake snapshots for a given pipeline with their timestamps, record counts, and Parquet file paths.
- `pg-tide ducklake checkpoint <pipeline>` — triggers a full checkpoint on the target DuckLake (flush inlined data, merge small Parquet files, expire old snapshots beyond the retention window).
- `pg-tide ducklake flush-inlined <pipeline>` — flushes inlined data to Parquet without full compaction, for use in low-latency archival scenarios.
- `pg-tide ducklake offset-map <pipeline>` — prints the consumer-offset-to-snapshot-ID mapping table in human-readable form, useful for debugging replay scenarios.

**Docker Compose getting-started example**
- Add `examples/ducklake/docker-compose.yml` that spins up: PostgreSQL 18 with pg_tide installed, the pg-tide relay pre-configured with a DuckLake sink writing to a local S3-compatible store (MinIO), a DuckDB shell container with the `ducklake` extension pre-installed, and a Grafana instance with the relay health dashboard. A single `docker compose up` gives any developer a working, queryable event lake in under two minutes.
- Include a seeded script that publishes 1 000 synthetic order events and demonstrates: querying the live lake from DuckDB, time-travel back to snapshot 1, and the `pg-tide ducklake snapshots` CLI output.

**Tutorial suite and conference demo scripts**
- Publish five written tutorials in `docs/src/guides/ducklake/`: "From Transaction to Data Lake in 5 Minutes," "Real-Time Analytics with DuckDB," "Multi-Tenant Data Lake with Row-Level Security," "Event Sourcing with DuckLake as the Event Store," and "Migrating from Kafka Connect."
- Include four ready-to-run conference demo scripts in `examples/ducklake/demos/`: the "Zero to Data Lake" lightning demo, the "Impossible Guarantee" crash-recovery demo, the "Streaming Sensor Dashboard" interactive demo, and the "Compliance Replay" enterprise demo — each with a speaker script, timing notes, and recovery steps.
- Submit a DuckLake community guide and request inclusion in the [awesome-ducklake](https://github.com/esadek/awesome-ducklake) repository.

---

### Audit Remediation, TLS Completeness & Pre-GA Hardening (v0.23.x – v0.25.x)

Following the four-cycle audit programme that produced `overall_assessment_1` through `overall_assessment_4`, this tranche addresses every remaining finding before the v1.0.0 Production GA: correctness bugs in the remote PostgreSQL inbox sink, catalog drift in the extension's `extension_sql_file!()` chain, a real TLS backend replacing the fail-closed placeholder, full migration test coverage through the current release, a comprehensive code-quality pass covering every P2/P3 item accumulated over multiple sprints, and the ADR-006 outbox table partitioning implementation that unblocks v1.0.0 GA.

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| v0.23.0 | Correctness, real TLS & full migration coverage: fix remote `PgInboxSink` column mismatch, add missing `0.21.0→0.22.0` `extension_sql_file!()` entry, implement `native-tls` feature, extend migration and E2E tests through v0.22.0, add `commit_offset()` monotonicity guard + `admin_rewind_offset()`, DLQ fault-injection and error-classification tests, `PgInboxSink` round-trip test, `ducklake_replicate()` relay test, `pg_dump` schema-diff CI assertion | ✅ Released | Large | [plans/overall-assessment-4.md](plans/overall-assessment-4.md) |
| v0.24.0 | Code quality, performance & Helm production maturity: all P2/P3 audit findings — `outbox_status_impl()` SPI consolidation, coordinator logging discipline, `worker_inner()` decomposition, `OutboxBatch` ownership fix, `rate_limiter` safe constant, per-sink latency metrics, `pg-tide status` TLS and pool metrics, Helm `PodDisruptionBudget` + `ServiceMonitor` + HPA, ADR-006 outbox partitioning design document | ✅ Released | Large | [plans/overall-assessment-4.md](plans/overall-assessment-4.md) |
| v0.25.0 | Outbox table partitioning, multi-tenant relay completion & pre-GA hardening: implement ADR-006 declarative partitioning with live migration tooling, complete per-tenant relay group runtime, extended `pg-tide doctor` (TLS version, DuckLake catalog, DLQ depth, partition capacity), relay Criterion.rs benchmark suite with CI regression gate, `--self-test` startup flag, pre-GA readiness checklist | 🔜 Planned | Large | [plans/overall-assessment-4.md](plans/overall-assessment-4.md) |

#### v0.23.0 — Correctness, Real TLS & Full Migration Test Coverage (detail)

**P0: Critical correctness fixes**
- **Fix `PgInboxSink` column mismatch** — change the INSERT in `pg-tide-relay/src/sink/pg_outbox.rs` from `(event_id, event_type, payload, received_at)` to `(event_id, source, payload, headers)`, matching the schema created by `tide.inbox_create()`. Map `msg.subject` to `source` and build a `{"event_type": msg.subject}` JSON object for `headers`. This is a regression of the v0.13.0 fix that was correctly applied to local `InboxSink` but missed `PgInboxSink`; any cross-database inbox delivery pipeline has been failing at runtime since v0.13.0.
- **Add missing `extension_sql_file!()` for 0.21.0→0.22.0** — add `pgrx::extension_sql_file!("../../sql/pg_tide--0.21.0--0.22.0.sql", name = "pg_tide_m_0_22", requires = ["pg_tide_m_0_21"]);` to `pg-tide-ext/src/lib.rs`. Without this entry a fresh `CREATE EXTENSION pg_tide` at v0.22.0 is missing `tide.ducklake_source_config`, `tide.ducklake_replicate()`, and `tide.ducklake_source_last_snapshot()`, silently breaking the v0.22.0 DuckLake bidirectional headline feature on all new installs while upgrades continue to work correctly.

**P1: Migration test coverage**
- **Extend `migration_test.rs` through v0.22.0** — add `const` bindings for all five missing migration scripts (`pg_tide--0.17.0--0.18.0.sql` through `pg_tide--0.21.0--0.22.0.sql`) and include them in the `UPGRADES` sequential chain so that every SQL upgrade is exercised in CI on every PR.
- **Extend `sql_to_sink_e2e.rs` through v0.22.0** — the E2E test currently applies the schema only through v0.17.0; extend `apply_full_schema()` to include all migrations up to the current release, ensuring the DuckLake catalog tables (`ducklake_source_config`, `ducklake_offset_map`, etc.) exist in the E2E test environment.
- **`pg_dump --schema-only` CI diff assertion** — add a CI job that: (1) creates a fresh `CREATE EXTENSION pg_tide` on PostgreSQL 18, (2) applies the complete upgrade chain from 0.1.0 to current on a second database, and (3) asserts the `pg_dump --schema-only` outputs are identical, making catalog drift immediately visible in CI rather than at the next audit cycle. Closes assessment-3 §6.4 which remained unimplemented through v0.19.0.

**P1: Real TLS via `native-tls` feature**
- **Compile-time `native-tls` Cargo feature** — add an optional `native-tls` feature to `pg-tide-relay/Cargo.toml` that pulls in `postgres-openssl` and swaps `pg_tls::connect()` to use `TlsConnector` when `sslmode=require`, `verify-ca`, or `verify-full` is parsed from the connection URL. The `:latest` Docker image and default feature set remain NoTls-capable (fail-closed on `require`); the `:latest-full` image compiles with `--features native-tls`.
- **Integration test with TLS testcontainer** — spin up a PostgreSQL 18 testcontainer with TLS certificates and verify the relay connects successfully with `sslmode=require` when built with `--features native-tls`, and returns a clear `TlsRequired` error when built without it.
- **TLS documentation clarification** — update README, `docs/src/relay-guide/configuration.md`, and `docs/src/operations/deployment-guide.md` to clearly state: (a) the default build fails closed on `sslmode=require` without establishing TLS, (b) real TLS requires `--features native-tls` or the `:latest-full` image, and (c) cloud-managed PostgreSQL services behind a TLS proxy work without the feature flag.

**P1: Security hardening**
- **Fix `signal::ctrl_c().await.expect()` in `main.rs`** — replace `.expect("failed to install Ctrl+C handler")` and `.expect("failed to install SIGTERM handler")` at `pg-tide-relay/src/main.rs` lines 293–299 with `?` propagation into `main()` (which returns `Result<(), RelayError>`), so that signal-registration failure on restricted seccomp profiles logs a clear error and exits cleanly rather than panicking.
- **Fix `ducklake_attach()` format specifiers** — replace `%s` with `%L` (PostgreSQL dollar-quoted literal) for the `_dbname`, `_host`, and `_port` interpolations in `sql/pg_tide--0.19.0--0.20.0.sql`, preventing malformed ATTACH statements when database names or host values contain quotes. Backport the corrected function body via a new migration script entry.
- **`ducklake_replicate()` identifier length guard** — add `IF length(_pipeline_in) > 63 THEN RAISE EXCEPTION 'generated pipeline name ''%'' exceeds 63 bytes; shorten schema or table name', _pipeline_in; END IF;` after the `regexp_replace` step in `sql/pg_tide--0.21.0--0.22.0.sql`, preventing silent PostgreSQL identifier truncation that could cause two different source tables to collide on the same pipeline name.

**P2: Remote inbox sink batching**
- **`PgInboxSink` UNNEST batch inserts** — replace the per-row `for msg in messages { client.execute(INSERT ...) }` loop with a single `INSERT INTO tide.{table} (event_id, source, payload, headers) SELECT * FROM UNNEST($1::text[], $2::text[], $3::jsonb[], $4::jsonb[]) ON CONFLICT (event_id) DO NOTHING`, building four parameter `Vec`s before executing. Mirrors the `InboxSink` UNNEST batching introduced in v0.13.0 and eliminates N database round-trips per relay batch.

**P2: Offset safety**
- **`commit_offset()` monotonicity guard** — add `WHERE tide_consumer_offsets.committed_offset <= EXCLUDED.committed_offset` to the `ON CONFLICT ... DO UPDATE` clause in `outbox_commit_offset_impl()`, preventing offset rewind by a buggy consumer without explicit admin action. This finding has been raised in four consecutive assessments since assessment-1 §7.3.
- **`tide.admin_rewind_offset(group_name, consumer_id, target_offset)` function** — provide an explicit, `SECURITY DEFINER` escape hatch for intentional offset rollback, callable only by superusers or members of the `tide_admin` role. Requires the caller to explicitly acknowledge the re-processing risk by passing a `confirm_reprocessing BOOLEAN DEFAULT FALSE` parameter.

**Test coverage: DuckLake relay paths**
- **`tide.ducklake_replicate()` relay integration test** — call `tide.ducklake_replicate()` via SQL in a testcontainers environment, then start a relay coordinator and verify the auto-generated pipeline config is valid, the DuckLake source connection succeeds, and `tide.ducklake_source_config` contains the expected row.
- **`tide.ducklake_source_last_snapshot()` test** — assert the function returns `NULL` on an empty `ducklake_snapshot` table and the correct `snapshot_id` value after a snapshot row is inserted.
- **`PgInboxSink` round-trip test** — start a testcontainers PostgreSQL instance, install the extension, call `tide.inbox_create('test_inbox')`, instantiate `PgInboxSink`, publish 50 messages via `Sink::publish`, and assert all 50 rows appear in `tide.test_inbox` with correct `event_id`, `source`, `payload`, and `headers` values and zero duplicates after re-publishing the same 50 messages.

**Test coverage: reliability paths (assessment-3 §6.2, §6.5)**
- **DLQ fault-injection test** — revoke `INSERT` on `tide.relay_dlq` for the relay role; assert the worker enters the permanent-error pause state (`DlqOutcome::PermanentError`) rather than cycling indefinitely at `WARN`. Closes assessment-3 §6.2 which remained unimplemented through v0.19.0.
- **Error classification integration test** — assert that a `RelayError::AuthRejected` (from a deliberately misconfigured sink credential) is classified as `is_transient() = false`, immediately pauses the pipeline without exhausting retries, and increments `pg_tide_relay_pipeline_errors_total{error_class="permanent"}`. Closes assessment-3 §6.5.
- **`commit_offset()` monotonicity test** — assert that calling `commit_offset()` with a lower offset than the current committed value is silently ignored (existing value unchanged) and that calling with a higher offset succeeds and updates the row.

#### v0.24.0 — Code Quality, Performance & Helm Production Maturity (detail)

**P2: Performance and correctness**
- **`outbox_status_impl()` single SPI call** — replace the three sequential `Spi::get_one_with_args()` calls (pending count, total count, oldest age) in `pg-tide-ext/src/outbox.rs` with a single query using `FILTER` aggregates: `SELECT COUNT(*) FILTER (WHERE consumed_at IS NULL), COUNT(*), EXTRACT(epoch FROM now() - MIN(created_at) FILTER (WHERE consumed_at IS NULL)) FROM tide.outbox_{name}`. Eliminates 2× SPI round-trips for every `tide.outbox_status()` invocation and simplifies future extension of the status struct.
- **Per-batch coordinator logging level** — demote `tracing::info!(pipeline = ...)` on every successful poll completion in `coordinator.rs` to `tracing::debug!()`, reserving `info!` for state transitions (worker start/stop, circuit-breaker open/close, DLQ write, pipeline pause/resume). At 50 pipelines × 1 poll/second this change reduces log volume by approximately 4.3 million lines/day in a typical production deployment.
- **`rate_limiter.rs` safe constant** — replace `NonZeroU32::new(1).expect("1 is non-zero")` in `pg-tide-relay/src/rate_limiter.rs` with `NonZeroU32::MIN` (stable since Rust 1.79), removing the last production-reachable `expect()` call in the rate-limiter path and eliminating the theoretical footgun if the literal is ever changed during refactoring.

**P3: SPI error handling**
- **`get_outbox_retention()` error propagation** — change the return type from `Option<i32>` to `Result<Option<i32>, PgTideError>` and replace `unwrap_or(None)` with `?` propagation in `pg-tide-ext/src/outbox.rs`, consistent with the pattern established for `outbox_exists()` in v0.17.0. Callers that previously silently received `None` on SPI error will now surface the error to the SQL caller.
- **`outbox_publish_impl()` fold `current_user` into ACL query** — eliminate the preliminary `Spi::get_one::<String>("SELECT current_user")` call by embedding `current_user` directly in the ACL lookup predicate: `WHERE outbox_name = $1 AND role_name = current_user::text`. Saves one SPI round-trip per `tide.outbox_publish()` call, which matters for high-frequency event streams.

**P3: Coordinator decomposition**
- **`worker_inner()` decomposition** — extract the ~500-line `worker_inner()` function in `coordinator.rs` into three focused helpers: `poll_and_decode()` (source polling and envelope decoding), `publish_with_circuit_breaker()` (sink publish with circuit-breaker state management and retry logic), and `handle_batch_error()` (DLQ routing, error classification, backoff scheduling). Each helper is independently unit-testable. The outer `worker_inner()` becomes a clean orchestration loop under 100 lines.
- **`OutboxBatch::into_messages()` avoid unnecessary clone** — switch from iterating with `.iter().cloned()` (or equivalent) to consuming ownership via `.into_iter()` in `OutboxBatch::into_messages()`, eliminating a full copy of the payload `Vec` on every message decode. This issue has been noted in four consecutive audit cycles (first in assessment-2 §4.4).

**Observability improvements**
- **Per-sink publish latency histogram** — add a `pg_tide_relay_sink_publish_duration_seconds` Histogram metric labelled by `pipeline` and `sink_type`, tracking wall-clock time from `Sink::publish()` call entry to return. Expose P50/P95/P99 quantiles. Add corresponding Grafana panels to `pg-tide/dashboards/relay-health.json`.
- **Connection pool health metrics** — expose `pg_tide_relay_pool_connections{state="idle|busy|waiting"}` gauge and `pg_tide_relay_pool_acquire_duration_seconds` histogram from the `deadpool-postgres` coordinator pool, enabling early detection of connection exhaustion and pool saturation before `max_connections` is hit.
- **OTel span coverage expansion** — add spans for: `schema_evolution_check` (before each DuckLake batch commit), `dlq_insert` (wrapping `route_to_dlq()`), and `backoff_sleep` (annotated with consecutive failure count and next-wake-up timestamp). Ensures distributed traces capture the full per-batch lifecycle for performance debugging.
- **`pg-tide status` improvements** — extend the CLI output to include: TLS state (plaintext / fail-closed / native-tls with negotiated version), connection pool metrics (idle, busy, waiting counts), and per-pipeline last-error string from in-memory coordinator state.

**Helm production maturity**
- **`PodDisruptionBudget` template** — add `helm/pg-tide/templates/pdb.yaml` rendered from `values.yaml` key `podDisruptionBudget.enabled` (default: `false`) and `podDisruptionBudget.minAvailable` (default: `1`). Documents the recommended HA deployment topology where multiple relay replicas own disjoint pipeline sets via `max_owned_pipelines`.
- **`ServiceMonitor` template for Prometheus Operator** — add `helm/pg-tide/templates/servicemonitor.yaml` rendered from `values.yaml` key `serviceMonitor.enabled` (default: `false`). Auto-discovers the `/metrics` endpoint and scrapes all `pg_tide_relay_*` metrics without manual Prometheus scrape configuration. Documents the label selectors required by common Prometheus Operator deployments (kube-prometheus-stack, Victoria Metrics Operator).
- **`HorizontalPodAutoscaler` template** — add an optional HPA template driven by `pg_tide_relay_consumer_lag_seconds` via a KEDA `ScaledObject` (or custom metrics adapter) for auto-scaling relay replica count under bursty load. Gated on `autoscaling.enabled = false` by default with documented KEDA ScaledObject configuration.

**ADR-006: Outbox table partitioning design**
- **Publish ADR-006** — write `docs/adr/adr-006-outbox-table-partitioning.md` covering: motivation (unbounded table growth for high-throughput outboxes), evaluated partition strategies (range by `created_at`, hash by `id`), the migration path for existing tables, interaction with consumer group leases and advisory locks, `relay_consumer_offsets` update semantics during partition pruning, and the tradeoffs of PostgreSQL declarative partitioning vs. TTL-based truncation (the current `outbox_truncate_delivered()` approach). This ADR establishes the design contract for the implementation in v0.25.0.

#### v0.25.0 — Outbox Table Partitioning, Multi-Tenant Relay Completion & Pre-GA Hardening (detail)

This release implements the ADR-006 outbox partitioning design, completes the multi-tenant relay groups runtime (catalog-ready since v0.14.0 but runtime routing was never shipped), and hardens the operational surface to meet the bar required for the v1.0.0 Production GA release that follows.

**Outbox table partitioning (ADR-006 implementation)**
- **Declarative range partitioning on `created_at`** — `tide.outbox_create()` gains an optional `partition_strategy` parameter (`'none'` | `'daily'` | `'weekly'` | `'monthly'`; default `'none'`). When set, the outbox backing table is created as a PostgreSQL declarative range-partitioned table with an initial partition covering the current interval and the next.
- **Automatic partition provisioning** — the relay `pg-tide sweep` command (or an optional background task) creates the next interval's partition before the current one fills, preventing insert failures during window transitions. Emits a `pg_notify('tide_partition_events', ...)` when a partition is created or dropped.
- **Partition pruning and archival** — extend `tide.outbox_truncate_delivered(name)` to detach and drop partitions whose entire retention window has expired, keeping the active partition count within a bounded rolling window (configurable via `outbox_create()`'s `retention_partitions` parameter; default 7 for daily strategy).
- **Consumer group and advisory lock compatibility** — verify and add regression tests confirming that `tide.poll_outbox()`, `tide.commit_offset()`, and `tide.consumer_lease_acquire()` work correctly on partitioned tables; pay particular attention to the `WHERE id > $last_offset` query plans to ensure partition pruning is applied.
- **Live migration tooling** — provide `tide.outbox_convert_to_partitioned(name, strategy)`: a function that copies an existing unpartitioned outbox to a new partitioned table using an advisory-lock swap with minimal relay downtime (comparable to a `RENAME TABLE` switchover). Document the procedure in the schema migration runbook.
- **`pg-tide doctor` partition health check** — warn when an outbox's most recent partition covers less than 48 hours of future capacity, enabling operators to provision new partitions before a write failure.

**Multi-tenant relay groups: runtime completion**
- **Per-tenant pipeline ownership filtering** — the coordinator's pipeline-ownership loop reads `current_setting('app.tenant_id', true)` (or a `PG_TIDE_TENANT_ID` env var) and filters `SELECT * FROM tide.relay_outbox_config WHERE tenant_id = $1` so that each relay instance owns only its tenant's pipelines without cross-tenant interference.
- **Per-tenant advisory lock namespacing** — incorporate the tenant hash into the `pg_try_advisory_lock(group_hash, pipeline_hash)` key pair, preventing two tenants' relay groups from colliding on identical pipeline names in a shared PostgreSQL database.
- **Per-tenant Prometheus metric label** — inject a `tenant` label (from `PG_TIDE_TENANT_ID` env var or CLI flag) into all relay metric series, enabling per-tenant Grafana dashboards and per-tenant alerting rules in multi-tenant deployments.
- **Integration test: two-tenant isolation** — spin up two relay coordinators with different `relay_group_id` values against the same PostgreSQL database, publish events into tenant-A and tenant-B outboxes, and assert each relay delivers only its tenant's messages without cross-contamination.

**Extended `pg-tide doctor` checks**
- **TLS version check** — when connecting with TLS enabled, query `SELECT version FROM pg_ssl` and emit a warning if the server negotiated TLS 1.1 or earlier; emit an error if `sslmode=require` resolves to a plaintext connection (fail-closed path in the default build).
- **DuckLake catalog health check** — verify that `ducklake_snapshot`, `ducklake_data_file`, and `ducklake_column` tables are accessible and owned by the expected schema, and that the sequence backing `ducklake_snapshot.snapshot_id` has at least 10% of its range remaining (overflow guard for long-running deployments).
- **DLQ depth warning** — query `COUNT(*) FROM tide.relay_dlq WHERE created_at > now() - interval '1 hour'` and emit a `WARNING` when the hourly rate exceeds a configurable `--dlq-warn-threshold` (default: 100), signalling potential upstream data quality problems before they require emergency manual DLQ replay.
- **Partition capacity check** — warn when the next partition boundary is within 48 hours of the most recently written row, providing at least one business day's notice before a write failure.

**Relay benchmark suite**
- **Criterion.rs throughput benchmarks** — implement benchmarks in `pg-tide-relay/benches/` for the three core hot paths: (1) `OutboxPollerSource::poll()` with 1 000-row batches at varying payload sizes (1 KB, 10 KB, 100 KB), (2) `InboxSink::publish()` with UNNEST batch sizes of 1, 10, 100, and 1 000 rows, (3) `coordinator::worker_inner()` end-to-end mock (source mock → JMESPath transform → sink mock) measuring orchestration overhead independently of PostgreSQL I/O.
- **CI regression gate** — record and commit baseline benchmark results to `pg-tide-relay/benches/baseline.json`; the CI job compares against the baseline and fails on regressions above 10% in any measured throughput or latency percentile.
- **Memory allocation profile** — add a `dhat`-instrumented CI job running the coordinator under a 50-pipeline mock load to identify any unbounded per-pipeline allocation growth before it becomes a production incident.

**Pre-GA operational readiness**
- **Pre-GA readiness checklist** — publish `docs/src/operations/pre-ga-checklist.md` covering: TLS configuration verification, outbox partitioning strategy selection, consumer group setup, DLQ monitoring thresholds, `pg-tide doctor` output interpretation, Helm security context review, benchmark baseline validation, and rollback procedure. This document serves as the formal acceptance gate for declaring v1.0.0 Production GA.
- **`pg-tide --self-test` flag** — connects to PostgreSQL, verifies extension version matches the compiled-in expected minimum, checks TLS state, acquires and immediately releases an advisory lock, queries `tide.outbox_pending` view, and exits `0` on success or `1` with a descriptive error on failure. Designed for use in Kubernetes `initContainers`, container health checks, and CI/CD pre-deployment gates.
- **`just release-notes` recipe** — reads `CHANGELOG.md` for the current workspace version and formats a GitHub Release body with upgrade notes, breaking changes, migration instructions, and benchmark comparison table. Eliminates manual release-note authoring and ensures every release references the relevant assessment and plan documents.

---

### Production GA & Extended Ecosystems (v1.0+)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| v1.0.0 | Production GA: encryption envelope with KMS integration, pipeline template library, delivery receipt log, outbox table partitioning by time, claim-check native pathway (or explicit pg_trickle-only scope), canonical config path enforcement | 🔜 Planned | Medium | [plans/relay-cli-phase3.md](plans/relay-cli-phase3.md) · [plans/overall_assessment_3.md](plans/overall_assessment_3.md) |
| v1.1.0 | Scale & exactly-once: logical-replication source (CDC without polling via `pgoutput`, accelerated from v1.2), Kafka exactly-once via transactions, multi-outbox fan-in pipelines, per-tenant relay groups with per-tenant DB roles, extended connector ecosystems (dlt, Redpanda Connect / Benthos, AMQP 1.0, webhook flavors) | 🔜 Future | Large | [plans/overall_assessment_3.md](plans/overall_assessment_3.md) |
| v1.2.0 | Plugin extensibility & advanced CDC: WASM transform plugin system with deterministic resource limits and stable `RelayMessage` ABI; pipeline dependency DAG; outbox table range-partitioning switchover tooling | 🔜 Future | Large | [plans/overall_assessment_2.md](plans/overall_assessment_2.md) |
| v1.3.0 | Web UI control plane: embedded Axum-served SPA (HTMX) for pipeline management, DLQ resolution, consumer lag monitoring, and replay — authenticated via PostgreSQL roles | 🔜 Future | XL | [plans/overall_assessment_2.md](plans/overall_assessment_2.md) |
