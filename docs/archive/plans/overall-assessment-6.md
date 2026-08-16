# pg_tide Overall Assessment — 2026-05-20

## Executive Summary

pg_tide has reached a production-ready maturity level with v0.27.0. The project now ships 30 sink backends, 16 source backends, 9 CLI subcommands, 60+ integration test files, comprehensive OTel instrumentation, a full SQL migration chain (0.1.0 through 0.27.0), real TLS via `native-tls`, outbox table partitioning (ADR-006/ADR-007), multi-tenant relay runtime, and a validated Grafana dashboard with alerting rules. The v0.26.0/v0.27.0 sprints resolved all P0 and P1 findings from assessment-5, implemented `worker_inner()` decomposition with unit tests, added `clap` value-parser validation, and delivered the Partition Management runbook. For the first time across six assessments, the codebase has **zero P0 critical findings**.

**Top 3 remaining risks:**

1. **Migration test chain omits v0.26.0→0.27.0.** The `migration_test.rs` and `extension_sql_file!()` chain are the two pillars of catalog integrity. While `lib.rs` correctly includes the 0.26.0→0.27.0 script, the integration test stops at 0.25.0→0.26.0. The v0.27.0 migration (ADD COLUMN `description`) is not exercised by CI — any DDL error in that script would be invisible until an actual database upgrade.

2. **Helm chart version drift (0.26.0 vs 0.27.0).** The `just bump-version` recipe was meant to prevent this exact issue, yet `Chart.yaml` / `appVersion` are at 0.26.0 while `Cargo.toml` and `pg_tide.control` are at 0.27.0. This will confuse operators relying on the Helm chart to deploy the matching relay binary version.

3. **`PgInboxSink` does not double-quote the inbox table name.** The SQL statement uses `tide.{table}` (unquoted) while the local `InboxSink` correctly uses `tide."{table}"`. Any inbox name containing a hyphen (valid per `validate_identifier()`) will produce a SQL syntax error on the remote sink path, silently breaking cross-database inbox delivery for common naming patterns.

**Top 3 opportunities:**

1. **Ship v1.0.0 GA.** The functional surface is complete, the hardening is done, and all high-severity findings are addressable in a single sprint. A v1.0 release signals production readiness.
2. **Envelope encryption with KMS** — the roadmap v1.0 differentiator that no competing PostgreSQL outbox offers.
3. **WAL-based logical-replication source** — eliminates polling overhead for high-throughput CDC scenarios.

**Delta vs. assessment-5:** All P1 and P2 findings from assessment-5 are resolved. No regressions detected. Four new findings documented below — primarily release-process hygiene and one table-quoting inconsistency.

---

## Regressions from Prior Assessments

| Prior Finding | Prior Status | Current Status | Evidence |
|---|---|---|---|
| Assessment-5 P1-1: NAMEDATALEN guard in `outbox_convert_to_partitioned()` | Open | ✅ Resolved | [sql/pg_tide--0.25.0--0.26.0.sql](sql/pg_tide--0.25.0--0.26.0.sql#L65-L80) — guard added |
| Assessment-5 P1-2: `poll_simple()` outbox table name not validated relay-side | Open | ✅ Resolved | [pg-tide-relay/src/source/outbox.rs](pg-tide-relay/src/source/outbox.rs#L260-L261) — `validate_relay_identifier` called in constructor |
| Assessment-5 P2-3: `expect()` in arrow_flight/singer/airbyte | Open | ✅ Resolved | Arrow Flight `.expect()` replaced with `.ok_or_else()` per CHANGELOG v0.26.0 |
| Assessment-5 P2-4: Webhook HMAC `expect()` missing `// SAFETY:` | Open | ✅ Resolved | [pg-tide-relay/src/source/webhook.rs](pg-tide-relay/src/source/webhook.rs#L105-L107) — SAFETY comment present |
| Assessment-5 P2-5: Shared-table prerequisite guard | Open | ✅ Resolved | [sql/pg_tide--0.25.0--0.26.0.sql](sql/pg_tide--0.25.0--0.26.0.sql#L87-L109) — guard with `confirm_shared_table_migration` |
| Assessment-5 P2-6: Dashboard missing v0.24.0 metric panels | Open | ✅ Resolved | CHANGELOG v0.27.0 — Sink Latency row, Connection Pool row, Per-Tenant Overview row added |

**No regressions detected.** All findings marked as resolved in assessments 1–5 remain resolved in the current codebase.

---

## Critical Findings (P0 — must fix before next release)

**None.**

For the second consecutive assessment, there are no P0 findings.

---

## High-Priority Findings (P1 — fix within 2 sprints)

### 1. Migration test does not cover v0.26.0→0.27.0

- **Severity:** P1 High
- **Location:** [pg-tide-relay/tests/migration_test.rs](pg-tide-relay/tests/migration_test.rs#L42-L68) — `UPGRADES` array ends at `("0.25.0 → 0.26.0", V0_25_0_TO_0_26_0)`.
- **Root cause:** The v0.26.0 and v0.27.0 releases added new migration scripts but the migration test was not updated to include the `V0_26_0_TO_0_27_0` constant and tuple.
- **Concrete impact:** The v0.27.0 migration (`ALTER TABLE … ADD COLUMN IF NOT EXISTS description TEXT`) is never executed in the CI integration test chain. A DDL error — for example, if a future contributor edits the script to add a NOT NULL column without a DEFAULT — would be invisible until an actual database upgrade.
- **Recommended fix:**
  ```rust
  const V0_26_0_TO_0_27_0: &str = include_str!("../../sql/pg_tide--0.26.0--0.27.0.sql");
  // Add to UPGRADES:
  ("0.26.0 → 0.27.0", V0_26_0_TO_0_27_0),
  ```
- **Test/verification:** Run `just test-integration` and verify the migration test passes with the full chain through 0.27.0.

### 2. Helm chart version drift (0.26.0 vs 0.27.0)

- **Severity:** P1 High
- **Location:** [helm/pg-tide/Chart.yaml](helm/pg-tide/Chart.yaml#L8-L9) — `version: 0.26.0` / `appVersion: "0.26.0"`.
- **Root cause:** The `just bump-version` recipe was not run (or did not update the Helm chart) during the v0.27.0 release. Workspace `Cargo.toml` shows `version = "0.27.0"` and `pg_tide.control` shows `default_version = '0.27.0'`, but Chart.yaml was not bumped.
- **Concrete impact:** An operator deploying via Helm will pull the `0.26.0` image tag by default (since `appVersion` is used when `image.tag` is empty), missing the v0.27.0 CLI hardening, dashboard updates, and documentation improvements. The version mismatch also breaks the project's documented alignment guarantee.
- **Recommended fix:** Update `Chart.yaml`:
  ```yaml
  version: 0.27.0
  appVersion: "0.27.0"
  ```
  Then verify the `just bump-version` recipe correctly updates Chart.yaml — if it doesn't, fix the recipe.
- **Test/verification:** `grep version helm/pg-tide/Chart.yaml` should show 0.27.0. Add a CI check that asserts `Chart.yaml version == Cargo.toml workspace version`.

### 3. `PgInboxSink` does not double-quote the inbox table name

- **Severity:** P1 High
- **Location:** [pg-tide-relay/src/sink/pg_outbox.rs](pg-tide-relay/src/sink/pg_outbox.rs#L79-L82) — `format!("INSERT INTO tide.{table} …", table = self.inbox_table)`.
- **Root cause:** When the `PgInboxSink` was rewritten in v0.23.0 to use UNNEST batch inserts with correct column names, the table reference was left unquoted. The local `InboxSink` at [pg-tide-relay/src/sink/inbox.rs](pg-tide-relay/src/sink/inbox.rs#L56) correctly uses `format!("tide.\"{}\"", self.inbox_table)`.
- **Concrete impact:** Any inbox name containing a hyphen (e.g., `order-events`) — which is valid per both `validate_identifier()` and `validate_relay_identifier()` — produces a SQL syntax error: `INSERT INTO tide.order-events_inbox …` (PostgreSQL interprets `-` as minus). Cross-database inbox delivery is broken for hyphenated inbox names. The `pg_inbox_sink_test.rs` only tests with `pg_sink_test` (no hyphens), so this was never caught.
- **Recommended fix:** Change line 80 to quote the table:
  ```rust
  let sql = format!(
      "INSERT INTO tide.\"{}\" (event_id, source, payload, headers) \
       SELECT * FROM UNNEST($1::text[], $2::text[], $3::jsonb[], $4::jsonb[]) \
       ON CONFLICT (event_id) DO NOTHING",
      self.inbox_table
  );
  ```
- **Test/verification:** Add a test case to `pg_inbox_sink_test.rs` that creates an inbox with a hyphenated name (e.g., `order-events`) and publishes messages via `PgInboxSink`. Verify no SQL error occurs.

---

## Medium-Priority Findings (P2 — fix within 6 sprints)

### 4. `poll_simple()` does not double-quote outbox table name in SQL

- **Severity:** P2 Medium
- **Location:** [pg-tide-relay/src/source/outbox.rs](pg-tide-relay/src/source/outbox.rs#L406-L416) — `let outbox_schema_table = format!("tide.{outbox_table_name}");` followed by `&format!("SELECT id, payload FROM {table} WHERE id > $1 ORDER BY id LIMIT $2", table = outbox_schema_table)`.
- **Root cause:** Same pattern as P1-3 — the table name is interpolated without double-quoting. While `validate_relay_identifier()` is called in the constructor (resolving assessment-5 P1-2), it does not reject hyphens (only `"` and `\0` are rejected).
- **Concrete impact:** An outbox table name containing a hyphen (e.g., `outbox_order-events`) produces invalid SQL: `SELECT id, payload FROM tide.outbox_order-events WHERE …`. The relay fails to poll that outbox. This is less likely in practice since outbox_table_name is typically `outbox_<stream_table>` constructed internally, but the relay accepts arbitrary names from pipeline config.
- **Recommended fix:** Quote the identifier:
  ```rust
  let outbox_schema_table = format!("tide.\"{}\"", outbox_table_name);
  ```
  Apply the same fix in `fetch_claim_check_rows()` where `delta_table` is also unquoted.
- **Test/verification:** Add a unit test with a hyphenated outbox_table_name and assert the generated SQL is syntactically valid.

### 5. `outbox_publish_impl()` publisher-ACL check uses three sequential SPI calls

- **Severity:** P2 Medium
- **Location:** [pg-tide-ext/src/outbox.rs](pg-tide-ext/src/outbox.rs#L188-L228) — three separate SPI calls: (1) COUNT publishers, (2) check rolsuper, (3) check allowed.
- **Root cause:** The ACL enforcement was added incrementally across v0.13.0 and v0.24.0 without consolidating the queries.
- **Concrete impact:** For hot-path publish calls on outboxes with publisher ACLs configured, each publish executes 3 SPI round-trips just for authorization. Under high throughput (1000+ publishes/s), this adds measurable latency.
- **Recommended fix:** Consolidate into a single SPI call:
  ```sql
  SELECT CASE
    WHEN NOT EXISTS(SELECT 1 FROM tide.outbox_publishers WHERE outbox_name = $1) THEN 'no_acl'
    WHEN (SELECT rolsuper FROM pg_roles WHERE rolname = current_user) THEN 'superuser'
    WHEN EXISTS(SELECT 1 FROM tide.outbox_publishers WHERE outbox_name = $1 AND role_name = current_user::text) THEN 'allowed'
    ELSE 'denied'
  END
  ```
- **Test/verification:** Benchmark `outbox_publish()` with ACLs enabled before/after; expect ~60% fewer SPI round-trips per publish.

### 6. `inbox_status_impl()` fleet summary loop generates N+1 SPI queries

- **Severity:** P2 Medium
- **Location:** [pg-tide-ext/src/inbox.rs](pg-tide-ext/src/inbox.rs#L252-L270) — loops over all configured inboxes and executes one `SELECT COUNT(*)` per inbox.
- **Root cause:** Each inbox's message table has a different name (`"{schema}"."{name}_inbox"`), so dynamic SQL is needed. The implementation chose a per-inbox loop over a more complex dynamic approach.
- **Concrete impact:** With 20 inboxes configured, calling `SELECT tide.inbox_status()` (fleet summary) fires 21 SPI queries. This is an operational convenience function called infrequently, so the impact is low, but it could be a problem for high-cardinality monitoring.
- **Recommended fix:** Use `format_type`-style dynamic SQL or a single `EXECUTE format()` call inside PL/pgSQL that aggregates all counts in one pass. Alternatively, document that the fleet summary is O(n) and recommend per-inbox status calls for high-frequency monitoring.
- **Test/verification:** Profile with 10+ inboxes; verify total SPI call count drops from N+1 to 2.

### 7. Webhook HMAC `expect()` — technically safe but blocked by `just lint-expect`

- **Severity:** P2 Medium
- **Location:** [pg-tide-relay/src/source/webhook.rs](pg-tide-relay/src/source/webhook.rs#L107) — `.expect("HMAC accepts any key size")`.
- **Root cause:** The `// SAFETY:` comment was added (assessment-5 P2-4 resolved), but the `expect()` call remains. The `just lint-expect` recipe introduced in v0.26.0 scans for bare `.expect()` calls not preceded by `// SAFETY:` within 5 lines. Since the SAFETY comment IS present, this passes lint. However, the project convention in `AGENTS.md` states "Never `unwrap()` or `panic!()` in code reachable from SQL" and the relay project aspires to eliminate all `expect()` in production paths.
- **Concrete impact:** No runtime risk — HMAC-SHA256 genuinely accepts any key size. This is a convention consistency issue. If a future contributor copies this pattern without the SAFETY comment, the lint will catch it. But the relay could hypothetically be called with an empty string key if config validation fails upstream, and `new_from_slice` would still succeed (HMAC accepts empty keys too).
- **Recommended fix:** Replace with an infallible pattern:
  ```rust
  // SAFETY: HMAC-SHA256 accepts any key length (RFC 2104 §3).
  let mut mac = <Hmac<Sha256>>::new_from_slice(key.as_bytes())
      .unwrap_or_else(|_| unreachable!());
  ```
  Or simply leave as-is given the SAFETY annotation — this is P3 cosmetic.
- **Test/verification:** `just lint-expect` continues to pass.

---

## Low-Priority / Polish (P3)

| # | Location | Description | Recommendation |
|---|----------|-------------|----------------|
| 8 | [pg-tide-relay/src/coordinator.rs](pg-tide-relay/src/coordinator.rs#L365) | `serde_json::to_string(&mask_secrets_for_logging(...)).unwrap_or_default()` — masks serialization failure | Replace with `serde_json::to_string(...).unwrap_or_else(\|_\| "{}".to_string())` for explicit empty-object fallback |
| 9 | [pg-tide-ext/src/inbox.rs](pg-tide-ext/src/inbox.rs#L252) | Fleet `inbox_status()` uses `unwrap_or_default()` on the outer `Spi::connect()` call, hiding SPI errors | Return a dedicated error in the fleet path or log a warning |
| 10 | [pg-tide-relay/src/source/outbox.rs](pg-tide-relay/src/source/outbox.rs#L133-L139) | `fetch_claim_check_rows` uses `format!("tide.outbox_delta_rows_{outbox_name}")` — unquoted identifier in dynamic SQL | Quote: `format!("tide.\"outbox_delta_rows_{outbox_name}\"")` |
| 11 | [pg-tide-relay/tests/migration_test.rs](pg-tide-relay/tests/migration_test.rs) | Uses `tokio_postgres::NoTls` directly instead of `pg_tls::connect()` — not a correctness issue but inconsistent with relay proper | Use `pg_tls::connect()` for consistency (low value since testcontainer doesn't require TLS) |

---

## Gap Analysis by Area

### 1. Correctness & Bugs

**Strong.** All prior correctness findings are resolved:

- ✅ `PgInboxSink` uses correct columns `(event_id, source, payload, headers)` with UNNEST batching.
- ✅ `extension_sql_file!` chain covers all 26 migrations from 0.1.0 through 0.27.0.
- ✅ `commit_offset()` monotonicity guard prevents accidental offset rewind.
- ✅ `outbox_status_impl()` uses single SPI call with FILTER aggregates.
- ✅ `OutboxBatch::into_messages()` uses `into_iter()` (no clone).
- ✅ NAMEDATALEN guard in `outbox_convert_to_partitioned()`.
- ✅ Shared-table prerequisite guard with `confirm_shared_table_migration`.

**New findings:** P1-1 (migration test gap), P1-3 (PgInboxSink quoting), P2-4 (poll_simple quoting).

### 2. Security

**Strong.** All prior security findings are resolved:

- ✅ `validate_identifier()` in extension, `validate_relay_identifier()` in relay.
- ✅ SSRF guard applied to all HTTP-based sinks.
- ✅ Real TLS via `native-tls` feature.
- ✅ Signal handlers use graceful degradation.
- ✅ `SECURITY DEFINER` functions hardened with `SET search_path`.
- ✅ `cargo-deny` / `audit.toml` in CI.
- ✅ Per-tenant advisory lock namespacing.
- ✅ v0.27.0: `validate_postgres_url_scheme()` and `validate_tenant_id_str()` value parsers.

**No new security findings.** The P1-3 quoting issue is a correctness/availability issue rather than a security vulnerability, since `validate_relay_identifier()` already rejects `"` and `\0` chars that could enable injection.

### 3. Performance & Scalability

**Strong.** All prior performance findings resolved:

- ✅ `PgInboxSink` batch insert via UNNEST.
- ✅ Connection pooling via `deadpool-postgres`.
- ✅ Coordinator subscribes to LISTEN/NOTIFY for instant hot-reload.
- ✅ `sink_max_inflight` semaphore enforced.
- ✅ Outbox table partitioning for high-volume scenarios.
- ✅ `OutboxBatch::into_messages()` avoids clone.

**New finding:** P2-5 (publisher ACL 3× SPI round-trips).

### 4. Reliability & Resilience

**Excellent.** No new findings.

- ✅ DLQ write failures classify as permanent → pipeline pauses.
- ✅ Transient/permanent error classification.
- ✅ Worker panic detection via `JoinHandle::is_finished()`.
- ✅ Graceful shutdown with `--drain-timeout`.
- ✅ Monotonicity guard on `commit_offset()`.
- ✅ Multi-tenant relay groups with isolated advisory lock namespaces.

### 5. Code Quality & Ergonomics

**Good.** Major v0.26.0/v0.27.0 improvements:

- ✅ `just lint-expect` CI recipe catches bare `expect()` in production code.
- ✅ `worker_inner()` decomposed into `handle_publish_outcome()`, `apply_schema_evolution_check()`, `poll_and_decode()`, `publish_with_circuit_breaker()`, `route_to_dlq()`.
- ✅ `WorkerDirective` enum decouples decision logic from execution.
- ✅ CLI uses `clap` value-parsers instead of post-parse `eprintln!` + `exit(1)`.
- ✅ `CONTRIBUTING.md` documents `// SAFETY:` convention.

**Remaining:** P2-7 (webhook expect cosmetic), P3-8/P3-9 (minor `unwrap_or_default` paths).

### 6. Test Coverage & Quality

**Comprehensive.** 60+ integration test files covering:

- ✅ Full migration chain 0.1.0 → 0.26.0 (but not 0.27.0 — P1-1).
- ✅ SQL→relay→sink E2E test.
- ✅ Multi-tenant isolation test.
- ✅ DLQ test and DLQ fault-injection test.
- ✅ Schema evolution test.
- ✅ TLS test.
- ✅ Wire format property tests.
- ✅ Publisher ACL test.
- ✅ SSRF test.
- ✅ PgInboxSink round-trip test.
- ✅ `pg_dump` schema-diff CI job.
- ✅ `lint-expect` CI job.
- ✅ `handle_publish_outcome()` unit tests (v0.27.0).

**Gap:** P1-1 (v0.27.0 migration not tested). Also, the `pg_inbox_sink_test.rs` does not test with a hyphenated inbox name, which would have caught P1-3.

### 7. Documentation & Specification

**Excellent.** No new findings.

- ✅ ADRs 001–007 in place and current.
- ✅ Operations runbooks including Partition Management (v0.27.0).
- ✅ Getting-started guide with `{{current_version}}` variable (no hardcoded versions).
- ✅ CHANGELOG comprehensive through v0.27.0.
- ✅ `book.toml` preprocessor variables for version strings.
- ✅ Prometheus alerting rules documented (`alerts.yaml`).

### 8. Operational Readiness

**Production-ready.** P1-2 (Helm version drift) is the only gap:

- ✅ Helm chart with PDB, ServiceMonitor, HPA.
- ✅ Docker image: multi-arch, non-root, read-only rootfs.
- ✅ Release workflow: cosign signing, SBOM, Trivy scan.
- ✅ `just bump-version` recipe (though it missed this release — P1-2).
- ✅ Operations runbooks.
- ✅ `pg-tide doctor` comprehensive checks.
- ✅ `--self-test` for Kubernetes readiness probes.
- ✅ Dashboard validated with all 18 metrics.
- ✅ Alerting rules (`alerts.yaml`) for the 5 most critical failure modes.

### 9. Architecture & Design Gaps

**Minimal.** The only v1.0 roadmap gap is envelope encryption + KMS:

| Feature | Status | Risk |
|---------|--------|------|
| Outbox partitioning (ADR-006/007) | ✅ Complete | None |
| Multi-tenant relay | ✅ Complete | None |
| Real TLS | ✅ Complete | None |
| DuckLake bidirectional | ✅ Complete | None |
| Envelope encryption + KMS | Not started | Low — additive config |
| WAL logical-replication source | Not started (v1.2) | None |
| Web UI | Not started (v1.3) | None |

### 10. Dependency & Supply-Chain Health

**Healthy.** `audit.toml` documents 9 ignored advisories, all in optional feature-gated dependencies. The default build is clean. `cargo-deny` configured with license, ban, and advisory checks. SBOM and Trivy scanning in release CI. No outdated dependencies with known security-critical fixes affecting the default feature set.

---

## Metrics Summary

| Category | P0 | P1 | P2 | P3 | Total |
|---|---|---|---|---|---|
| Correctness & Bugs | 0 | 2 | 1 | 1 | 4 |
| Security | 0 | 0 | 0 | 0 | 0 |
| Performance | 0 | 0 | 2 | 0 | 2 |
| Reliability | 0 | 0 | 0 | 0 | 0 |
| Code Quality | 0 | 0 | 1 | 1 | 2 |
| Test Coverage | 0 | 1 | 0 | 1 | 2 |
| Documentation | 0 | 0 | 0 | 0 | 0 |
| Operational | 0 | 1 | 0 | 0 | 1 |
| Architecture | 0 | 0 | 0 | 0 | 0 |
| Dependencies | 0 | 0 | 0 | 0 | 0 |
| **Total** | **0** | **4** | **4** | **3** | **11** |

---

## Recommended Sprint Plan

1. **Add `V0_26_0_TO_0_27_0` to `migration_test.rs`** — 10 min, completes the CI chain.
2. **Bump Helm chart to 0.27.0** — 2 min, fixes version drift.
3. **Double-quote table name in `PgInboxSink`** — 1 line change, fixes cross-database inbox delivery for hyphenated names.
4. **Double-quote table name in `poll_simple()`** — 1 line change, prevents syntax errors for hyphenated outbox names.
5. **Add hyphenated-name test case to `pg_inbox_sink_test.rs`** — catches P1-3 class of bugs permanently.
6. **Consolidate publisher-ACL SPI into single query** — reduces hot-path latency.
7. **Add CI check: Chart.yaml version == Cargo.toml workspace version** — prevents P1-2 from recurring.
8. **Replace webhook HMAC `expect()` with `unwrap_or_else`** — cosmetic but aligns with convention.
9. **Quote `delta_table` in `fetch_claim_check_rows()`** — defence-in-depth.
10. **Begin envelope encryption + KMS design** — the last v1.0 roadmap deliverable.

---

## Path to v1.0 GA

### Must-do (block release)

1. Fix P1-1 through P1-3 — migration test, Helm version, PgInboxSink quoting.
2. Fix P2-4 — poll_simple quoting (defence-in-depth).
3. Implement envelope encryption + KMS (roadmap v1.0 commitment).
4. Final pass on `cargo deny check` and `cargo audit` with all advisories re-evaluated.
5. Release notes, migration guide from v0.x to v1.0, and stability guarantee documentation.

### Should-do (strongly recommended)

6. Consolidate publisher-ACL SPI (P2-5) — performance improvement for ACL-heavy deployments.
7. Add the 5 `pg_inbox_sink_test.rs` hyphenated-name test case.
8. CI check for Chart.yaml version alignment.

### Can follow in v1.1

9. WAL-based logical-replication source.
10. WASM transform plugins.
11. Web UI.
12. `inbox_status()` fleet N+1 optimization (P2-6).

---

## What Is Already World-Class

1. **Extension SQL file chain integrity.** 26 migration scripts loaded in order on fresh install AND available for upgrade paths. The `pg_dump` schema-diff CI job guards against drift.

2. **Worker decomposition and testability.** The v0.27.0 `WorkerDirective` enum + `handle_publish_outcome()` + `apply_schema_evolution_check()` pattern makes the coordinator's decision logic pure-functional and independently unit-testable.

3. **Observability completeness.** 18 Prometheus metrics with consistent naming, per-tenant labels, per-sink latency histograms, OTel spans covering the full lifecycle, a validated Grafana dashboard, and pre-built alerting rules.

4. **Multi-tenant isolation.** Per-tenant pipeline filtering, advisory-lock namespace isolation, per-tenant Prometheus label dimensions, RLS on catalog tables, and a dedicated integration test.

5. **Defence-in-depth identifier validation.** Both extension (`validate_identifier()`) and relay (`validate_relay_identifier()`) independently validate identifiers, with `just lint-expect` preventing unsafe production code from passing CI.

6. **CI lint pipeline.** `clippy -D warnings`, `fmt --check`, `lint-expect` (bare expect detection), `cargo deny`, `cargo audit`, lychee link-checking, and `pg_dump` schema-diff — comprehensive quality gates that prevent regression.

7. **Test breadth.** 60+ integration tests, property-based tests for wire formats, DLQ fault injection, multi-tenant isolation, graceful shutdown, and a full SQL→relay→sink E2E contract test.

---

## Appendix: Metrics Snapshot

| Metric | Value |
|---|---|
| Total Rust source files (ext + relay src) | ~90 |
| Approximate lines of Rust (excluding tests) | ~26,000 |
| Approximate lines of SQL (all `sql/*.sql`) | ~2,500 |
| SQL upgrade scripts in `sql/` | 26 (0.1.0 base + 25 upgrades) |
| Migration files loaded by `extension_sql_file!()` | 25 (0.17.0→0.18.0 excluded by design) |
| Sink backends | 30 |
| Source backends | 16 |
| Wire format implementations | 7+ |
| Integration test files | 60 |
| CLI subcommands | 9 |
| Prometheus metrics exported | 18 |
| OTel spans emitted | 10+ |
| Helm chart templates | 7 (deployment, service, SA, PDB, ServiceMonitor, HPA, tests) |
| Helm chart version / pg_tide.control / Cargo.toml | 0.26.0 / 0.27.0 / 0.27.0 (DRIFT) |
| ADRs published | 7 (ADR-001 through ADR-007) |
| Operations runbooks | 5+ |
| `audit.toml` ignored advisories | 9 (all in feature-gated optional deps; default build clean) |

— End of report —
