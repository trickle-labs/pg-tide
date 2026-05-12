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

### Production GA & Extended Ecosystems (v1.0+)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| v1.0.0 | Production GA: encryption envelope with KMS integration, pipeline template library, delivery receipt log, outbox table partitioning by time, claim-check native pathway (or explicit pg_trickle-only scope), canonical config path enforcement | 🔜 Planned | Medium | [plans/relay-cli-phase3.md](plans/relay-cli-phase3.md) · [plans/overall_assessment_3.md](plans/overall_assessment_3.md) |
| v1.1.0 | Scale & exactly-once: logical-replication source (CDC without polling via `pgoutput`, accelerated from v1.2), Kafka exactly-once via transactions, multi-outbox fan-in pipelines, per-tenant relay groups with per-tenant DB roles, extended connector ecosystems (dlt, Redpanda Connect / Benthos, AMQP 1.0, webhook flavors) | 🔜 Future | Large | [plans/overall_assessment_3.md](plans/overall_assessment_3.md) |
| v1.2.0 | Plugin extensibility & advanced CDC: WASM transform plugin system with deterministic resource limits and stable `RelayMessage` ABI; pipeline dependency DAG; outbox table range-partitioning switchover tooling | 🔜 Future | Large | [plans/overall_assessment_2.md](plans/overall_assessment_2.md) |
| v1.3.0 | Web UI control plane: embedded Axum-served SPA (HTMX) for pipeline management, DLQ resolution, consumer lag monitoring, and replay — authenticated via PostgreSQL roles | 🔜 Future | XL | [plans/overall_assessment_2.md](plans/overall_assessment_2.md) |
