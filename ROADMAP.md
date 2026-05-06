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
| v0.14.0 | Replay workbench, CloudEvents, tenant scale & managed backfill: SQL + CLI replay with dry-run and DLQ resolution, CloudEvents wire format + AsyncAPI export, tenant-aware relay groups with RLS + per-tenant metrics, cataloged backfill jobs with chunking and pause/resume | 🔜 Future | Large | [plans/overall_assessment_1.md](plans/overall_assessment_1.md) |

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

### Production GA & Extended Ecosystems (v1.0+)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| v1.0.0 | Production GA: encryption envelope with KMS integration | 🔜 Planned | Small | [plans/relay-cli-phase3.md](plans/relay-cli-phase3.md) |
| v1.1.0 | Extended connector ecosystems: dlt integration (~100 sources), Redpanda Connect / Benthos (~200 inputs/outputs), AMQP 1.0 (Azure Service Bus, Qpid), webhook flavors (n8n / Zapier) | 🔜 Future | Large | — |
| v1.2.0 | Plugin extensibility: WASM transform plugin system with deterministic resource limits and a stable `RelayMessage` ABI for custom routing, filtering, and encoding without recompiling the relay | 🔜 Future | Large | — |
