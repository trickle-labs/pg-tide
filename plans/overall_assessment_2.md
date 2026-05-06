# pg_tide Overall Assessment — 2026-05-06

## Executive Summary

pg_tide has matured significantly since the first assessment (2026-05-05). The v0.12.0–v0.14.0 release sprint addressed the majority of critical seam breaks between the SQL extension and the relay runtime: `relay_set_outbox()` now emits the relay's expected JSON shape (`source_type`, `source.outbox`, `sink_type`, `sink.*`); the pg-inbox sink inserts the correct columns (`event_id`, `source`, `payload`, `headers`); identifier validation covers all dynamic SQL paths; `outbox_publish()` enforces the `enabled` flag; and publisher ACLs, SSRF guards, schema evolution, and DLQ semantics are all implemented and tested. The supply-chain posture is strong: `cargo-deny`, `audit.toml`, and ignored-advisory documentation are in place. The project now ships 30 sink backends, 16 source backends, 7 wire formats, and 40+ integration test files — a remarkable feature surface for a sub-1.0 extension.

**Top 3 Remaining Risks:**

1. **TLS not wired into runtime connections.** The `pg_tls` module exists but every `tokio_postgres::connect()` call in `main.rs` and `coordinator.rs` still uses `NoTls`. This means `sslmode=require` in the connection string is silently ignored and credentials transit the network in plaintext.
2. **Helm chart version drift.** The Chart `version` / `appVersion` are pinned at `0.12.0` while the workspace is at `0.14.0`. The environment variable name (`PG_TIDE_POSTGRES_URL`) is correct but values and documentation should track workspace versions.
3. **No end-to-end SQL→relay test.** The `sql_api_test.rs` and integration tests validate SQL and relay independently, but no single test exercises `tide.relay_set_outbox()` → relay worker start → message flow → sink delivery in one process.

**Top 3 Opportunities:**

1. **Production-ready TLS** — wiring the `pg_tls` module into all connection points is the single highest-impact security improvement remaining.
2. **Encryption envelope with KMS** — the v1.0.0 roadmap feature that would give pg_tide unique differentiation vs. Debezium/Sequin.
3. **WASM transform plugins** — the v1.2.0 roadmap item, enabling an extension marketplace without recompiling the relay.

---

## Findings by Area

## 1. Correctness & Bugs

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 1 | High | `pg-tide-relay/src/coordinator.rs` L358 | Worker DB connections use `NoTls` hardcoded. The `pg_tls::parse_ssl_mode()` function is never called on the worker connection path. | Workers may fail silently to connect to TLS-only PostgreSQL servers, or transit credentials in plaintext. | Replace `tokio_postgres::connect(&db_url, tokio_postgres::NoTls)` with a call through `pg_tls::connect()`. |
| 2 | Medium | `pg-tide-relay/src/source/outbox.rs` L84–90 | Claim-check path calls `tide.outbox_rows_consumed()` and uses `tide.outbox_delta_rows_{name}` tables that are pg_trickle artefacts not present in a standalone pg_tide installation. | Claim-check mode will return a "relation does not exist" error on a pg_tide-only database without pg_trickle. | Guard claim-check mode behind a runtime existence check or document it as pg_trickle-only; add a runtime validation in `pg-tide doctor`. |
| 3 | Medium | `pg-tide-relay/src/coordinator.rs` L80 | `max_owned_pipelines` defaults to 50 and is not configurable via TOML or CLI. | Deployments with >50 pipelines will silently refuse to start additional workers without operator recourse. | Expose `max_owned_pipelines` in `RelayConfig` and the `--max-pipelines` CLI flag. |
| 4 | Medium | `pg-tide-relay/src/source/outbox.rs` L24–26 | `decode_payload` expects `v: 1` in the payload JSON; pg_tide `outbox_publish()` inserts raw user JSONB without wrapping in a versioned envelope. | When using pg_tide outbox directly (not via pg_trickle `attach_outbox`), the relay source decodes the payload as `v=0` → `UnsupportedPayloadVersion`. | Add a "raw" decode mode for native pg_tide outbox messages that treats the payload as a direct JSONB event without envelope version checking. |
| 5 | Low | `pg-tide-relay/src/envelope.rs` L119–130 | `OutboxBatch::into_messages()` clones each payload row. For large claim-check batches this doubles memory. | High-throughput claim-check pipelines use 2× expected memory. | Use `into_iter()` to take ownership without cloning. |
| 6 | Low | `pg-tide-ext/src/outbox.rs` L30–34 | `outbox_exists()` uses `unwrap_or(None).unwrap_or(false)` — the outer `unwrap_or` hides SPI errors. | A transient SPI error could cause `outbox_create` to attempt a duplicate insert (which would then correctly fail with a unique constraint). | Return `Result` from `outbox_exists()` and propagate SPI errors. Low priority as the FK/unique constraint is the safety net. |
| 7 | Info | `pg-tide-ext/src/relay.rs` L136, 172 | `relay_enable()` / `relay_disable()` silently no-op if the pipeline doesn't exist instead of returning NOT FOUND. | Non-existent pipeline names don't produce user-facing feedback. | Document intentional choice or return an error to match `relay_delete()` behaviour. |

**Resolved from Assessment 1:**
- ✅ #1 (relay config JSON shape) — Fixed in v0.12.0 (`relay.rs` now emits `source_type`/`sink_type` format).
- ✅ #2 (consumer offsets schema) — Relay now uses `last_offset TEXT` consistently.
- ✅ #3 (inbox sink column mismatch) — `InboxSink` now inserts `event_id, source, payload, headers`.
- ✅ #4 (enabled flag not enforced) — `outbox_publish()` checks `enabled` and returns error.
- ✅ #7 (relay_list_configs omits config) — Now returns full JSON object.

---

## 2. Security (OWASP Top 10 + PostgreSQL-specific)

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 1 | Critical | `pg-tide-relay/src/main.rs` L136, 255, 360, 424, 538, 584, 658, 692, 768, 1015; `coordinator.rs` L358 | All 11 `tokio_postgres::connect()` call sites use `NoTls`. The `pg_tls` module parses `sslmode` but is never invoked. | Passwords and message payloads travel unencrypted. Attackers on the network can intercept credentials and modify relay instructions. OWASP A02:2021 (Cryptographic Failures). | Wire `pg_tls::connect()` into every connection establishment point; fail closed when `sslmode=require`. |
| 2 | Medium | `pg-tide-relay/src/source/outbox.rs` L82–90 | Cursor name includes a `Uuid::new_v4()` suffix but the table name `tide.outbox_delta_rows_{outbox_name}` is constructed from pipeline config without identifier validation in the relay. | If a compromised catalog entry contains a malicious outbox_name, it could steer the cursor to an arbitrary relation. | Apply `validate_identifier()` to all configured names in the relay before constructing SQL. The extension side already validates; the relay should independently verify. |
| 3 | Medium | `pg-tide-relay/src/sink/inbox.rs` L49 | `format!("tide.\"{}\"", self.inbox_table)` — the inbox table name comes from pipeline config and is double-quoted but not validated for embedded quotes. | A config entry with `"` in the inbox name could break out of quoting (though the extension's `validate_identifier` rejects `"` chars, a relay-only user setting config directly could bypass this). | Add relay-side identifier validation; it exists in the extension but not in the relay binary. |
| 4 | Low | `pg-tide-relay/src/config.rs` L58–86 | `expand_env_vars()` reads `${ENV:VAR_NAME}` — if a pipeline config value contains this pattern and is logged, environment variable values (potentially secrets) appear in logs. | Log injection / secret leakage via crafted pipeline config. | Redact resolved `${ENV:...}` values from log output; mark sensitive config fields. |
| 5 | Low | `pg_tide.control` | `superuser = false` — extension is installable by non-superusers. Combined with `SECURITY DEFINER` functions, this could allow privilege escalation if functions are not correctly hardened. | Non-superuser who installs the extension owns the `SECURITY DEFINER` functions and can modify them. | Verify all `SECURITY DEFINER` functions have `SET search_path = tide, pg_catalog` (confirmed in v0.14.0 migration) and document that extension ownership should be restricted to a DBA role. |

**Resolved from Assessment 1:**
- ✅ Identifier validation: `validation.rs` rejects `"` and `\0` chars; used in all extension paths.
- ✅ Publisher ACLs: `outbox_publishers` table and enforcement in `outbox_publish()`.
- ✅ SSRF guard: `validate_webhook_url()` with loopback/link-local/private-range blocking.
- ✅ Supply-chain: `deny.toml` + `audit.toml` with justified ignores; CI step.
- ✅ SECURITY DEFINER hardening: functions set `search_path = tide, pg_catalog`.

---

## 3. Code Quality & Maintainability

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 1 | Medium | `pg-tide-relay/src/main.rs` L1–5 | `#![allow(dead_code, unused_imports)]` suppresses warnings globally for the binary crate. | Hides genuine dead code and import issues that clippy would catch. | Remove blanket allow; fix individual items or use `#[allow]` per item with justification. |
| 2 | Medium | `pg-tide-relay/src/lib.rs` L10 | Same `#![allow(dead_code, unused_imports)]` on the library crate. | Dead-code lint is completely disabled across the library. | Use targeted `#[allow]` on feature-gated items or `#[cfg_attr(not(feature = "x"), allow(dead_code))]`. |
| 3 | Medium | `pg-tide-relay/src/coordinator.rs` | `worker_inner()` is ~250 lines with deeply nested control flow. | Difficult to reason about correctness; modifications are error-prone. | Extract publish logic, DLQ routing, and metric updates into helper functions. |
| 4 | Low | `pg-tide-relay/src/source/singer.rs` L96 | `.expect("stdout was piped — handle is always present")` in production code. | Panic if process spawn fails in an unexpected way. | Use `ok_or(RelayError::...)` and propagate. |
| 5 | Low | `pg-tide-relay/src/source/webhook.rs` L106 | `.expect("HMAC accepts any key size")` — this is actually correct (HMAC does accept any key). | No real risk but violates project convention against `expect()` in non-test code. | Replace with `unwrap()` + `// SAFETY: HMAC-SHA256 accepts any key length` or use `unwrap_or_else(|_| unreachable!())`. |
| 6 | Info | `pg-tide-ext/src/outbox.rs`, `inbox.rs`, `relay.rs` | Helper functions `outbox_exists()`, `inbox_exists()`, `relay_exists()` return `bool` but swallow SPI errors via `unwrap_or(None).unwrap_or(false)`. | Silent degradation on transient DB errors. | Consider returning `Result<bool, PgTideError>` in a future cleanup pass. Low priority since downstream SQL operations will surface the real error. |

---

## 4. Performance & Scalability

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 1 | High | `pg-tide-relay/src/coordinator.rs` L358 | Each pipeline worker opens its own dedicated PostgreSQL connection. With 50 pipelines (the default max), this consumes 52 connections (2 shared + 50 workers). | PostgreSQL's `max_connections` can be exhausted, especially in managed DB services with low limits (e.g. 100). | Add connection pooling (`deadpool-postgres`) or a shared connection pool for workers; expose max-connections config. |
| 2 | Medium | `pg-tide-relay/src/source/outbox.rs` | The outbox poller uses `SELECT ... WHERE id > $1 ORDER BY id LIMIT $2` — efficient with the existing index. However, `consumed_at` is never set by the simple poller (only by consumer-group mode). | Over time `tide.tide_outbox_messages` grows without bound unless retention is externally enforced. The pending-messages index scans an ever-growing table. | Add a retention sweeper (either in-extension via `outbox_truncate_delivered()` or relay-side background task) and document the requirement. |
| 3 | Medium | `pg-tide-relay/src/coordinator.rs` L452 | On poll error, the worker sleeps for `poll_interval_ms` (default 1s) with no exponential backoff. | Persistent errors produce a tight retry loop at 1 msg/sec, generating log noise and wasting connections. | Apply exponential backoff with jitter on consecutive poll errors (similar to circuit breaker reset). |
| 4 | Low | `pg-tide-relay/src/envelope.rs` L119–131 | `OutboxBatch::into_messages()` clones every payload `serde_json::Value`. | Unnecessary allocation for large batches. | Use `self.inserted.into_iter()` to consume the batch in-place. |
| 5 | Low | `pg-tide-relay/benches/throughput.rs` | Only one benchmark file exists covering throughput. | Hot paths like JMESPath transforms, wire-format encode/decode, and routing resolution have no benchmarks. | Add micro-benchmarks for transform, routing, wire-format, and DLQ insert paths. |

---

## 5. Reliability & Observability

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 1 | High | `pg-tide-relay/src/coordinator.rs` L447–460, 520–530 | The worker does not distinguish transient from permanent sink errors. All errors increment `consecutive_failures` equally. | A permanent config error (e.g., bad auth credentials) leads to max_retries DLQ entries then loops forever at 1 failure/second. | Classify errors as `Transient` / `Permanent` in `RelayError`; for permanent errors, pause the pipeline immediately without retry. |
| 2 | Medium | `pg-tide-relay/src/coordinator.rs` L317–340 | `run_pipeline_worker()` is spawned with `tokio::spawn` but the join handle is never stored or awaited. | If the spawned task panics, the coordinator `owned` map retains a dead entry until the next reconciliation cycle (up to 30s). | Store join handles; on reconcile, check for completed/panicked tasks and clean up immediately. |
| 3 | Medium | `pg-tide-relay/src/metrics.rs` | No metric for the number of active/owned pipelines or the coordinator reconciliation loop duration. | Operators cannot see coordinator saturation or detect slow reconciliation. | Add `pg_tide_relay_owned_pipelines` gauge and `pg_tide_relay_reconcile_duration_seconds` histogram. |
| 4 | Low | `pg-tide-relay/src/otel.rs` | OTel spans cover `poll`, `publish`, and `acknowledge`, but not `transform`, `routing`, `dlq_insert`, or `schema_evolution_check`. | Incomplete distributed trace for debugging slow pipelines. | Add spans for transform, routing, and DLQ paths. |
| 5 | Low | `pg-tide/dashboards/relay-health.json` | Dashboard JSON is hand-maintained; metric names must match code constants exactly. | Metric renames break the dashboard without CI detection. | Generate dashboard from a template that references metric name constants, or add a CI check that validates all referenced metrics exist in `metrics.rs`. |

---

## 6. Test Coverage

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 1 | High | (missing) | No end-to-end test that exercises the SQL API → relay worker → sink delivery in a single process. `sql_api_test.rs` tests SQL only; integration tests configure pipelines by directly inserting into catalog tables. | Contract drift between SQL API and relay runtime can recur undetected. | Add a testcontainers test that calls `tide.relay_set_outbox()` via SQL, starts a relay coordinator, publishes a message, and asserts sink delivery. |
| 2 | Medium | (missing) | No SQL migration upgrade-path test. The `migration_test.rs` exists but only tests a single migration. | Chained upgrades (0.1.0 → 0.14.0) may have ordering or dependency issues undetected until user reports. | Add a test that installs 0.1.0, then applies all incremental migrations in sequence, and asserts catalog integrity. |
| 3 | Medium | (missing) | No property-based or fuzz testing of wire-format encode/decode round-trips. | Malformed inputs from external sources (Debezium, Kafka) may cause panics or data loss. | Add `proptest` or `quickcheck` tests for `WireFormat::decode` → `encode` round-trips. |
| 4 | Low | `pg-tide-relay/tests/` | No load/soak test. Tests exercise correctness but not sustained throughput under pressure. | Performance regressions are invisible until production incidents. | Add a Criterion-based throughput benchmark that simulates sustained load for configurable duration. |
| 5 | Info | `pg-tide-relay/tests/` | 40+ test files — excellent coverage of sink/source backends. Most tests use `PgTideTestDb` helper with testcontainers. | Good isolation; tests are independent. | Consider parallelizing test execution in CI using `--test-threads=4` for integration tests (currently `--test-threads=1`). |

---

## 7. API & Schema Design

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 1 | Medium | `pg-tide-ext/src/relay.rs` L95–132 | `relay_set_inbox()` has 8 parameters — too many positional args for ergonomic SQL usage. | Callers must remember parameter order; future additions make it worse. | Use a single JSONB config parameter with documented keys, or named parameters with defaults. |
| 2 | Medium | `sql/pg_tide--0.1.0.sql` | `tide.tide_outbox_messages` stores all outboxes in a single table with `outbox_name` discriminator. At scale (100M+ messages), the partial index on `(outbox_name, id) WHERE consumed_at IS NULL` may become inefficient. | Single-table design limits horizontal scaling of individual outboxes. | Document scaling guidance; consider partitioning by `outbox_name` for future versions. |
| 3 | Low | `pg-tide-ext/src/outbox.rs` | `outbox_create()` rejects duplicates with `OutboxAlreadyExists` error. Most similar systems (schemas, databases) use `IF NOT EXISTS` semantics. | Operators must wrap in exception handling for idempotent deployment scripts. | The existing `outbox_drop(p_if_exists := true)` pattern is good; consider adding `outbox_create_if_not_exists()` or making the existing function idempotent when settings match. |
| 4 | Low | Helm Chart `values.yaml` | No `securityContext` / `runAsNonRoot` / `readOnlyRootFilesystem` in values template. | Kubernetes security contexts must be manually added by operators. | Add `securityContext` defaults in the chart template matching the Dockerfile's non-root user. |
| 5 | Info | `pg-tide-relay/src/cli.rs` | CLI has `doctor`, `validate-config`, `replay`, and `asyncapi` subcommands — good completeness. | UX is clean and discoverable. | Consider adding `pg-tide status` to show running pipeline counts from the catalog. |

---

## 8. Documentation

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 1 | Medium | `README.md` L83 | Documentation links point to `trickle-labs.github.io/pg-tide` — verify the `docs.yml` workflow actually deploys there. | Broken doc links damage trust. | Add a CI check or smoke test that validates documentation URLs. |
| 2 | Medium | `docs/` directory | Large mdBook structure with many subdirectories but actual content not verified in this audit (skeleton may have placeholder pages). | Users may find empty or "TODO" pages. | Review each `docs/src/` file for completeness; add a CI link-checker. |
| 3 | Low | `CHANGELOG.md` | Comprehensive and well-structured with TOC. All versions documented. | Good. | No action needed. |
| 4 | Low | (missing) | No Architecture Decision Records (ADRs) for key design choices (single-table outbox, advisory-lock coordination, wire-format abstraction). | New contributors must reverse-engineer rationale from code. | Add an `docs/adr/` directory with records for top 5 design decisions. |
| 5 | Info | `ROADMAP.md` | Exceptionally detailed roadmap with version-by-version plan through 1.2.0. | Strong project communication. | Keep updated as features land. |

---

## 9. DevOps, CI/CD & Packaging

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 1 | High | `helm/pg-tide/Chart.yaml` L8–9 | `version: 0.12.0` / `appVersion: "0.12.0"` — workspace is at `0.14.0`. | Helm users deploy stale versions; version mismatch confuses operators. | Automate Helm version bump in release workflow (bump both `version` and `appVersion`). |
| 2 | Medium | `.github/workflows/release.yml` | Cross-compile aarch64-linux excludes `kafka` feature due to rdkafka build issues. | ARM users don't get Kafka support from prebuilt binaries. | Document the limitation clearly in release notes; investigate `rdkafka` static-linking for cross-compilation or provide a separate Kafka-capable Docker image. |
| 3 | Medium | `Dockerfile` | Builds with default features only (`nats`, `webhook`, `stdout`). | Docker image lacks most advertised backends (Kafka, Redis, SQS, etc.). | Build with `--all-features` or a curated "full" feature set in the Docker image; provide a `slim` variant with defaults only. |
| 4 | Low | `.github/workflows/ci.yml` | Integration tests are in CI but run with `--test-threads=1`. | CI is slow for 40+ test files. | Consider splitting integration tests into parallel jobs by backend category. |
| 5 | Low | `justfile` | `audit` target ignores 9 advisories — all documented in `audit.toml` with justification. | Good practice. | Periodically re-check if upstream fixes allow removing ignores. |
| 6 | Info | `.github/workflows/release.yml` | Multi-platform Docker builds, artifact uploads, 4-target matrix — comprehensive. | Strong release automation. | Add cosign signing step per roadmap. |

---

## 10. Dependency Health

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 1 | Medium | `pg-tide-ext/Cargo.toml` | `pgrx = "=0.18.0"` — exact pin is correct for extension stability, but pgrx 0.18 is relatively new. | Breaking pgrx updates require manual intervention. | Monitor pgrx releases; test against new versions in a branch before upgrading. |
| 2 | Medium | `audit.toml` | 9 ignored advisories (RUSTSEC-2026-*) — all in optional-feature transitive dependencies (hickory-proto via mongodb, rustls-webpki via AWS/MQTT, paste via parquet). | Known vulnerabilities exist in transitive deps, but only affect optional features. | Track upstream fixes; remove ignores as they become available. |
| 3 | Low | `pg-tide-relay/Cargo.toml` | `jmespath = "0.3"` — the `jmespath` crate has low download counts and the last publish was 2020. | Potential maintenance risk for a production dependency. | Evaluate `jmespath-community` or `jmesrlp` as alternatives if the crate becomes problematic. |
| 4 | Low | `pg-tide-relay/Cargo.toml` | `prometheus = "0.14"` — `prometheus` crate is in maintenance mode; community has moved to `prometheus-client`. | No immediate risk but limited future development. | Consider migrating to `prometheus-client` in a future release for OpenMetrics compatibility. |
| 5 | Info | `deny.toml` | Well-configured: excludes dev deps, allows standard licenses, warns on duplicates, restricts to crates.io. | Good supply-chain hygiene. | No action needed. |

---

## Aggregate Severity Summary

| Area | Critical | High | Medium | Low | Info |
|------|----------|------|--------|-----|------|
| 1. Correctness & Bugs | 0 | 1 | 3 | 2 | 1 |
| 2. Security | 1 | 0 | 2 | 2 | 0 |
| 3. Code Quality | 0 | 0 | 3 | 2 | 1 |
| 4. Performance | 0 | 1 | 2 | 2 | 0 |
| 5. Reliability & Observability | 0 | 1 | 2 | 2 | 0 |
| 6. Test Coverage | 0 | 1 | 2 | 1 | 1 |
| 7. API & Schema | 0 | 0 | 2 | 2 | 1 |
| 8. Documentation | 0 | 0 | 2 | 2 | 1 |
| 9. DevOps & Packaging | 0 | 1 | 2 | 2 | 1 |
| 10. Dependency Health | 0 | 0 | 2 | 2 | 1 |
| **TOTAL** | **1** | **5** | **22** | **19** | **7** |

---

## Feature & Roadmap Recommendations

1. **Encryption Envelope with KMS Integration**
   - **Problem solved:** Payload data at rest in outbox tables and in transit is visible to DBAs and network observers. Regulated industries (healthcare, finance) require field-level encryption.
   - **Sketch:** Add `tide.outbox_encrypt_config(outbox, kms_key_id, fields[])` → relay decrypts using AWS KMS / GCP Cloud KMS / HashiCorp Vault envelope encryption before publishing to sinks. Key rotation via re-encryption background job.
   - **Effort:** L
   - **Priority:** High (differentiator for enterprise)
   - **Milestone:** v1.0.0

2. **Connection Pooling for Workers**
   - **Problem solved:** Each pipeline worker opens a dedicated PostgreSQL connection. At 50 pipelines = 52 connections. Managed databases often have 100-connection limits.
   - **Sketch:** Introduce `deadpool-postgres` shared pool with configurable `max_connections`. Workers check out connections per poll cycle rather than holding them permanently.
   - **Effort:** M
   - **Priority:** High (production scaling blocker)
   - **Milestone:** v0.15.0

3. **Wire TLS into All Connection Points**
   - **Problem solved:** Production deployments cannot enforce encrypted connections despite having the `pg_tls` module.
   - **Sketch:** Replace all `tokio_postgres::connect(url, NoTls)` with `pg_tls::connect(url)` which honours `sslmode` from the URL. Add integration test with TLS-enabled PostgreSQL.
   - **Effort:** S
   - **Priority:** Critical (security)
   - **Milestone:** v0.15.0

4. **Multi-Outbox Fan-In Pipelines**
   - **Problem solved:** Currently each pipeline reads from exactly one outbox. Users with microservice architectures want a single pipeline that fans in from multiple outboxes to a single Kafka topic with ordering guarantees.
   - **Sketch:** Allow `source.outbox` to accept an array of outbox names; round-robin or priority-merge polling with per-outbox offset tracking.
   - **Effort:** M
   - **Priority:** Medium
   - **Milestone:** v1.1.0

5. **Exactly-Once Semantics via Kafka Transactions**
   - **Problem solved:** Current delivery is at-least-once. Kafka users want exactly-once semantics without downstream deduplication.
   - **Sketch:** Use Kafka's transactional producer: `begin_transaction()` → produce batch → `send_offsets_to_transaction()` → `commit_transaction()`. Track outbox offset in the same Kafka transaction.
   - **Effort:** L
   - **Priority:** Medium (Kafka-heavy enterprises)
   - **Milestone:** v1.1.0

6. **Logical Replication Source (CDC without Polling)**
   - **Problem solved:** Polling introduces latency (default 1s) and wastes resources on idle outboxes. Logical replication provides sub-millisecond change notification.
   - **Sketch:** Add a `source_type: "logical_replication"` that creates a temporary replication slot, decodes `pgoutput` WAL records for `tide.tide_outbox_messages`, and feeds them into the pipeline with LSN-based offset tracking.
   - **Effort:** XL
   - **Priority:** Medium (performance-sensitive users)
   - **Milestone:** v1.2.0

7. **Pipeline Template Library**
   - **Problem solved:** Users copy-paste pipeline configs for common patterns (outbox → Kafka, outbox → S3 archive, webhook → inbox). A template system reduces configuration errors.
   - **Sketch:** `tide.relay_apply_template('outbox-to-kafka', '{"outbox": "orders", "bootstrap_servers": "kafka:9092"}')` — templates stored in `tide.relay_templates` with variable interpolation.
   - **Effort:** S
   - **Priority:** Low (UX improvement)
   - **Milestone:** v1.0.0

8. **Pipeline Dependency Graph (DAG)**
   - **Problem solved:** Complex event processing requires pipelines to fan-out and rejoin. Currently each pipeline is independent.
   - **Sketch:** Add `depends_on` field to pipeline config; coordinator starts pipelines in topological order and pauses downstream when upstream is unhealthy.
   - **Effort:** L
   - **Priority:** Low (advanced use cases)
   - **Milestone:** v1.2.0

9. **Outbox Table Partitioning by Time**
   - **Problem solved:** Single outbox table grows without bound. Retention enforcement requires expensive `DELETE` operations.
   - **Sketch:** Implement automatic range partitioning of `tide.tide_outbox_messages` by `created_at` (weekly/daily partitions). Retention becomes `DROP PARTITION` which is instantaneous.
   - **Effort:** M
   - **Priority:** Medium (high-volume deployments)
   - **Milestone:** v1.0.0

10. **Web UI / Control Plane Dashboard**
    - **Problem solved:** Operators currently need SQL access or CLI to manage pipelines. A web UI lowers the barrier for non-DBA team members.
    - **Sketch:** Embed a lightweight Axum-served SPA (React/HTMX) in the relay binary that shows pipeline status, DLQ entries, consumer lag, and allows enable/disable/replay operations. Authenticate via PostgreSQL roles.
    - **Effort:** XL
    - **Priority:** Low (nice-to-have for adoption)
    - **Milestone:** v1.3.0

11. **Webhook Delivery Receipts & Retry Dashboard**
    - **Problem solved:** Webhook consumers can't easily see which deliveries failed and why, or manually retry specific events.
    - **Sketch:** Store delivery attempts in `tide.relay_delivery_log` with status codes, response bodies, and timestamps. Expose via `tide.delivery_log(pipeline, event_id)` and the CLI.
    - **Effort:** M
    - **Priority:** Medium (webhook users)
    - **Milestone:** v1.0.0

12. **Schema Registry Passthrough for Avro/Protobuf**
    - **Problem solved:** Currently schema registry support decodes and re-encodes. For Kafka→Kafka routing, passthrough mode avoids unnecessary serde overhead.
    - **Sketch:** Add `schema_registry.mode = "passthrough"` that forwards the Confluent wire-format bytes directly without deserialization.
    - **Effort:** S
    - **Priority:** Low
    - **Milestone:** v0.15.0

---

## Prioritised Action Plan

| # | Priority | Item | Owner | Area |
|---|----------|------|-------|------|
| 1 | P0 | Wire `pg_tls::connect()` into all 11 NoTls call sites in main.rs + coordinator.rs | relay | Security |
| 2 | P0 | Add integration test with TLS-enabled PostgreSQL testcontainer | relay | Security / Test |
| 3 | P1 | Fix Helm chart version to 0.14.0; add version bump to release automation | infra | DevOps |
| 4 | P1 | Build Docker image with all features (or documented "full" set) | infra | DevOps |
| 5 | P1 | Add end-to-end SQL API → relay → sink integration test | relay | Test |
| 6 | P1 | Add connection pooling (`deadpool-postgres`) for pipeline workers | relay | Performance |
| 7 | P1 | Expose `max_owned_pipelines` in CLI/TOML config | relay | Scalability |
| 8 | P1 | Distinguish transient vs. permanent errors in worker retry logic | relay | Reliability |
| 9 | P2 | Add relay-side identifier validation for all config-sourced table names | relay | Security |
| 10 | P2 | Handle "raw" pg_tide outbox payload (no `v:1` envelope) in outbox source | relay | Correctness |
| 11 | P2 | Guard claim-check mode against pg_trickle-only functions | relay | Correctness |
| 12 | P2 | Add exponential backoff to poll error retry | relay | Performance |
| 13 | P2 | Store worker join handles; detect panicked tasks in reconcile | relay | Reliability |
| 14 | P2 | Add `pg_tide_relay_owned_pipelines` gauge metric | relay | Observability |
| 15 | P2 | Add sequential SQL migration upgrade-path test (0.1.0 → 0.14.0) | ext | Test |
| 16 | P2 | Remove `#![allow(dead_code, unused_imports)]` from main.rs and lib.rs | relay | Quality |
| 17 | P2 | Extract `worker_inner()` publish/DLQ logic into helper functions | relay | Quality |
| 18 | P2 | Add property-based tests for wire-format round-trips | relay | Test |
| 19 | P2 | Document claim-check as pg_trickle-only in docs | docs | Documentation |
| 20 | P2 | Add outbox retention sweeper or document retention requirement | ext/relay | Performance |
| 21 | P3 | Add OTel spans for transform, routing, and DLQ paths | relay | Observability |
| 22 | P3 | Add Criterion benchmarks for transform, routing, wire-format | relay | Performance |
| 23 | P3 | Reduce `relay_set_inbox()` parameter count (JSONB config pattern) | ext | API Design |
| 24 | P3 | Add `securityContext` defaults to Helm chart templates | infra | DevOps |
| 25 | P3 | Document limitations of ARM builds (no Kafka) in release notes | docs | Documentation |
| 26 | P3 | Add architecture decision records (ADRs) for top 5 design choices | docs | Documentation |
| 27 | P3 | Generate Grafana dashboard from metric name constants | infra | Observability |
| 28 | P3 | Add CI link-checker for documentation URLs | infra | Documentation |
| 29 | P3 | Verify mdBook pages are complete (not placeholder) | docs | Documentation |
| 30 | P3 | Parallelize integration test CI execution | infra | DevOps |
| 31 | P3 | Evaluate `jmespath` crate alternatives for long-term maintenance | relay | Dependencies |
| 32 | P3 | Add `pg-tide status` CLI subcommand | relay | API/UX |
| 33 | P4 | Replace `OutboxBatch::into_messages()` clone with ownership transfer | relay | Performance |
| 34 | P4 | Change `outbox_exists()` / `inbox_exists()` to return `Result` | ext | Quality |
| 35 | P4 | Replace singer source `.expect()` with proper error propagation | relay | Quality |
| 36 | P4 | Document `relay_enable()` silent no-op behaviour | ext | API Design |
| 37 | P4 | Consider `outbox_create_if_not_exists()` for idempotent DDL | ext | API Design |
| 38 | P4 | Migrate to `prometheus-client` crate (future) | relay | Dependencies |
| 39 | P4 | Redact `${ENV:...}` resolved values from log output | relay | Security |
| 40 | P4 | Add cosign signing to release workflow | infra | DevOps |

---

## Appendix: Files Examined

### Core Configuration
- `Cargo.toml` (workspace root)
- `pg-tide-ext/Cargo.toml`
- `pg-tide-relay/Cargo.toml`
- `justfile`
- `pg_tide.control`
- `pg-tide-ext/pg_tide.control`
- `audit.toml`
- `deny.toml`
- `book.toml`

### SQL Schema & Migrations
- `sql/pg_tide--0.1.0.sql`
- `sql/pg_tide--0.13.0--0.14.0.sql`

### Extension Source (`pg-tide-ext/src/`)
- `lib.rs`
- `error.rs`
- `outbox.rs`
- `inbox.rs`
- `relay.rs`
- `validation.rs`
- `backfill.rs`

### Relay Source (`pg-tide-relay/src/`)
- `main.rs`
- `lib.rs`
- `cli.rs`
- `config.rs`
- `coordinator.rs`
- `envelope.rs`
- `error.rs`
- `metrics.rs`
- `dlq.rs`
- `circuit_breaker.rs`
- `rate_limiter.rs`
- `routing.rs`
- `jmespath_transform.rs`
- `schema_evolution.rs`
- `otel.rs`
- `pg_tls.rs`
- `transforms.rs` (via `mod.rs` references)
- `sink/mod.rs`
- `sink/inbox.rs`
- `sink/webhook.rs`
- `source/mod.rs`
- `source/outbox.rs`
- `wire_format/mod.rs`

### Tests
- `pg-tide-relay/tests/consumer_group_test.rs`
- `pg-tide-relay/tests/dlq_test.rs`
- Full test directory listing (40+ files verified)

### CI/CD & Infrastructure
- `.github/workflows/ci.yml`
- `.github/workflows/release.yml`
- `.github/workflows/docs.yml` (existence verified)
- `Dockerfile`
- `helm/pg-tide/Chart.yaml`
- `helm/pg-tide/values.yaml`

### Documentation
- `README.md`
- `ROADMAP.md`
- `CHANGELOG.md` (first 150 lines)
- `AGENTS.md`

### Previous Assessment
- `plans/overall_assessment_1.md` (cross-referenced for resolved findings)
