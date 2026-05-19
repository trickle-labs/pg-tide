# pg_tide Overall Assessment — 2026-05-19

## Executive Summary

pg_tide has continued to mature rapidly since the v0.16.0/v0.17.0 sprints that addressed the bulk of assessment-3's findings. The codebase now ships 30 sink backends, 16 source backends, 8 CLI subcommands, 56 integration tests, comprehensive OTel instrumentation, DLQ with proper error classification, and a novel DuckLake streaming integration (v0.20.0–v0.22.0). The prior audits' critical P0 items (TLS wiring, identifier validation, inbox column mismatch, config contract alignment, plpgsql deduplication) are resolved in the local `InboxSink` path — but not universally.

**Top 3 remaining risks:**

1. **Remote `PgInboxSink` column mismatch (regression from assessment-1 fix).** The local `InboxSink` was fixed in v0.13.0 to insert `(event_id, source, payload, headers)`, but the remote `PgInboxSink` in `pg_outbox.rs` still inserts `(event_id, event_type, payload, received_at)` — columns that do not exist in extension-created inbox tables. Any pipeline using the remote PG inbox sink against a pg_tide-managed inbox will fail at runtime with a "column does not exist" error. This is a data-path correctness bug affecting cross-database inbox delivery.

2. **Catalog drift: `pg_tide--0.21.0--0.22.0.sql` missing from `extension_sql_file!()` chain.** The `lib.rs` chain ends at `pg_tide--0.20.0--0.21.0.sql`. A fresh `CREATE EXTENSION pg_tide` at v0.22.0 will be missing `tide.ducklake_source_config`, `tide.ducklake_replicate()`, and `tide.ducklake_source_last_snapshot()`. The DuckLake bidirectional feature (v0.22.0's headline) is silently broken on fresh installs while working fine on upgrades.

3. **Test coverage gap: migrations 0.17.0→0.22.0 not exercised by integration tests.** Both `migration_test.rs` and `sql_to_sink_e2e.rs` only apply scripts through `pg_tide--0.16.0--0.17.0.sql`. The five subsequent migration scripts (0.17.0→0.18.0 through 0.21.0→0.22.0) are never tested in the integration test suite, meaning any DDL errors in those scripts (including the missing `extension_sql_file!` for 0.22.0) are invisible to CI.

**Top 3 opportunities:**

1. Fix the remote inbox sink column mismatch — a ~5-line change that completes the v0.13.0 contract alignment across all sink implementations.
2. Add the missing `extension_sql_file!()` entry for 0.21.0→0.22.0 and update migration/e2e tests to cover the full chain through the current version.
3. Implement real TLS (via `rustls` or `native-tls`) rather than the current fail-closed-on-require pattern, which effectively means no production deployment can use `sslmode=require` without a TLS termination proxy.

---

## Critical Findings (P0 — must fix before next release)

1. **Remote `PgInboxSink` inserts wrong columns.**
   - **Evidence:** [pg-tide-relay/src/sink/pg_outbox.rs](../pg-tide-relay/src/sink/pg_outbox.rs#L53-L58) — inserts `(event_id, event_type, payload, received_at)`.
   - **Expected:** `(event_id, source, payload, headers)` — matching extension-created inbox tables ([pg-tide-ext/src/inbox.rs](../pg-tide-ext/src/inbox.rs#L76-L85)).
   - **Impact:** Any forward/reverse pipeline using `PgInboxSink` (remote PG inbox delivery) fails at runtime with `column "event_type" does not exist`. Cross-database inbox delivery is completely broken.
   - **Recommended fix:** Change the INSERT statement to `INSERT INTO tide.{table} (event_id, source, payload, headers) VALUES ($1, $2, $3, $4) ON CONFLICT (event_id) DO NOTHING` with `&[&msg.dedup_key, &msg.subject, &msg.payload, &serde_json::json!({"event_type": msg.subject})]`. Add an integration test using `PgInboxSink` against an extension-created inbox table.

2. **Catalog drift: `pg_tide--0.21.0--0.22.0.sql` not loaded on fresh install.**
   - **Evidence:** [pg-tide-ext/src/lib.rs](../pg-tide-ext/src/lib.rs#L135-L140) — the last `extension_sql_file!()` is `pg_tide--0.20.0--0.21.0.sql` (named `pg_tide_m_0_21`). The file `sql/pg_tide--0.21.0--0.22.0.sql` exists but has no corresponding `extension_sql_file!()` entry.
   - **Impact:** Fresh `CREATE EXTENSION pg_tide` at v0.22.0 will be missing: `tide.ducklake_source_config` table, `tide.ducklake_replicate()` function, `tide.ducklake_source_last_snapshot()` function. The v0.22.0 headline feature (DuckLake bidirectional flow) is broken on fresh installs.
   - **Recommended fix:** Add `pgrx::extension_sql_file!("../../sql/pg_tide--0.21.0--0.22.0.sql", name = "pg_tide_m_0_22", requires = ["pg_tide_m_0_21"]);` to `lib.rs`.

---

## High-Priority Findings (P1 — fix within 2 sprints)

3. **Integration tests only cover migrations through v0.17.0.**
   - **Evidence:** [pg-tide-relay/tests/migration_test.rs](../pg-tide-relay/tests/migration_test.rs#L14-L28) and [pg-tide-relay/tests/sql_to_sink_e2e.rs](../pg-tide-relay/tests/sql_to_sink_e2e.rs#L27-L45) — include_str constants stop at `V0_16_0_TO_0_17_0`. Five migration files (0.17.0→0.18.0 through 0.21.0→0.22.0) are not exercised.
   - **Impact:** DDL regressions, schema conflicts, and the P0-2 catalog drift are invisible to CI.
   - **Recommended fix:** Add `const V0_17_0_TO_0_18_0` through `V0_21_0_TO_0_22_0` to both test files. Add them to the `UPGRADES` array and `apply_full_schema()` function.

4. **`pg_tls::connect()` always uses `NoTls` — TLS is fail-closed but never actually provided.**
   - **Evidence:** [pg-tide-relay/src/pg_tls.rs](../pg-tide-relay/src/pg_tls.rs#L103-L117) — `connect()` returns `Err(TlsRequired)` when `sslmode=require`, but never actually establishes a TLS connection. The function signature returns `Connection<Socket, NoTlsStream>` which cannot carry TLS. There is no `native-tls` or `rustls` feature that would provide real TLS.
   - **Impact:** Production deployments cannot use `sslmode=require` without an external TLS termination proxy (PgBouncer, pgcat, cloud provider proxy). This is documented as "fixed" in assessment-2/3 but the fix is actually "fail-closed" rather than "TLS works". The relay binary currently cannot connect to a PostgreSQL server that requires TLS.
   - **Recommended fix:** Add a `native-tls` or `rustls` feature that provides `postgres-openssl` or `postgres-rustls` TLS backend. The current fail-closed behavior is correct as a safety net, but production users need real TLS.

5. **Signal handler `expect()` in production code.**
   - **Evidence:** [pg-tide-relay/src/main.rs](../pg-tide-relay/src/main.rs#L293-L299) — `signal::ctrl_c().await.expect("failed to install Ctrl+C handler")` and `.expect("failed to install SIGTERM handler")`.
   - **Impact:** On platforms where signal registration fails (rare but possible in containers with restricted seccomp profiles), the relay binary panics rather than returning a controlled error.
   - **Recommended fix:** Replace with `.map_err()` and propagate the error, or use `unwrap_or_else(|e| tracing::error!(...))` with a clean exit.

6. **`ducklake_attach()` uses `%s` format for connection params.**
   - **Evidence:** [sql/pg_tide--0.19.0--0.20.0.sql](../sql/pg_tide--0.19.0--0.20.0.sql#L50-L57) — `format('ATTACH ''ducklake:postgres:dbname=%s host=%s port=%s'' AS %I%s;', _dbname, _host, _port, catalog_schema, _data_clause)`.
   - **Impact:** The `%s` specifier does not quote or escape the values. While `_dbname` comes from `current_database()` and `_host`/`_port` from `pg_settings`, a database named `foo' --` or a `listen_addresses` value containing quotes could produce a malformed ATTACH statement. Low probability since these are system-controlled values, but violates defense-in-depth.
   - **Recommended fix:** Use `%L` (literal quoting) instead of embedding values inside a string literal manually, or validate that dbname/host/port contain only safe characters before interpolation.

---

## Medium-Priority Findings (P2 — fix within 6 sprints)

7. **`PgInboxSink` uses per-row INSERT loop (N+1 pattern).**
   - **Evidence:** [pg-tide-relay/src/sink/pg_outbox.rs](../pg-tide-relay/src/sink/pg_outbox.rs#L50-L64) — iterates `for msg in messages` with individual INSERT per message.
   - **Impact:** High round-trip overhead for remote inbox delivery; the local `InboxSink` was converted to UNNEST batch inserts in v0.13.0 but the remote sink was not.
   - **Recommended fix:** Apply the same UNNEST batching pattern from `sink/inbox.rs` to `sink/pg_outbox.rs`.

8. **`rate_limiter.rs` uses `NonZeroU32::new(1).expect("1 is non-zero")` in production code.**
   - **Evidence:** [pg-tide-relay/src/rate_limiter.rs](../pg-tide-relay/src/rate_limiter.rs#L114) — `.unwrap_or(NonZeroU32::new(1).expect("1 is non-zero"))`.
   - **Impact:** The `expect()` is technically safe (1 is always non-zero) but violates the project convention against `expect()` in non-test code. A future refactor could accidentally change the constant.
   - **Recommended fix:** Use `NonZeroU32::MIN` (Rust 1.79+) or a compile-time constant `const ONE: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(1) }; // SAFETY: 1 != 0`.

9. **`ducklake_replicate()` generates pipeline names with `regexp_replace` but does not apply `validate_identifier()`.**
   - **Evidence:** [sql/pg_tide--0.21.0--0.22.0.sql](../sql/pg_tide--0.21.0--0.22.0.sql#L71-L75) — `_pipeline_in := 'ducklake_src_' || regexp_replace(...)`. The generated name is inserted into `tide.ducklake_source_config` without length or character validation.
   - **Impact:** Very long schema/table names could produce identifiers >63 bytes, which PostgreSQL silently truncates. Two different input names could collide after truncation.
   - **Recommended fix:** Add a length check after generation: `IF length(_pipeline_in) > 63 THEN RAISE EXCEPTION ... END IF;`.

10. **`outbox_status_impl()` fires three sequential SPI queries without batching.**
    - **Evidence:** [pg-tide-ext/src/outbox.rs](../pg-tide-ext/src/outbox.rs#L269-L296) — three separate `Spi::get_one_with_args` calls for pending count, total count, and oldest age.
    - **Impact:** For high-frequency monitoring calls, this creates 3× the SPI overhead. A single query with `SELECT COUNT(*) FILTER (WHERE consumed_at IS NULL), COUNT(*), EXTRACT(...)` would be more efficient.
    - **Recommended fix:** Consolidate into a single SPI call using `FILTER` clauses.

11. **`commit_offset()` allows arbitrary offset movement without monotonicity guard.**
    - **Evidence:** [pg-tide-ext/src/outbox.rs](../pg-tide-ext/src/outbox.rs#L493-L505) — `ON CONFLICT ... DO UPDATE SET committed_offset = EXCLUDED.committed_offset` with no `WHERE committed_offset <= EXCLUDED.committed_offset` guard.
    - **Impact:** A buggy consumer can rewind the offset and re-process messages without explicit admin intent. This was noted in assessment-1 §7.3 but remains unaddressed.
    - **Recommended fix:** Add `WHERE tide_consumer_offsets.committed_offset <= EXCLUDED.committed_offset` or provide a separate `admin_rewind_offset()` function for intentional rollback.

12. **Worker per-batch info-level logging at high cardinality.**
    - **Evidence:** [pg-tide-relay/src/coordinator.rs](../pg-tide-relay/src/coordinator.rs#L520) — `tracing::info!(pipeline = %pipeline.name, ...)` on every successful poll completion.
    - **Impact:** 50 pipelines × 1 poll/second = 4.3M log lines/day of per-batch success messages. This was noted in assessment-3 §9.4 but not addressed.
    - **Recommended fix:** Demote the per-batch-success log to `debug!`; keep `info!` for state transitions only.

---

## Low-Priority / Cosmetic (P3)

13. **`outbox_publish_impl()` resolves `current_user` via a separate SPI call.**
    - **Evidence:** [pg-tide-ext/src/outbox.rs](../pg-tide-ext/src/outbox.rs#L168-L172) — `Spi::get_one::<String>("SELECT current_user")`.
    - **Recommendation:** Combine into the publisher-ACL EXISTS query: `WHERE outbox_name = $1 AND role_name = current_user`.

14. **`get_outbox_retention()` swallows SPI errors via `unwrap_or(None)`.**
    - **Evidence:** [pg-tide-ext/src/outbox.rs](../pg-tide-ext/src/outbox.rs#L35-L39).
    - **Recommendation:** Return `Result<Option<i32>, PgTideError>` for consistency with the now-fixed `outbox_exists()`.

15. **Coordinator `worker_inner()` remains ~500+ lines.**
    - **Evidence:** [pg-tide-relay/src/coordinator.rs](../pg-tide-relay/src/coordinator.rs#L451-L952).
    - **Recommendation:** The v0.18.0 `route_to_dlq()` extraction was a good start; continue extracting `publish_batch()` and `handle_publish_error()` helpers.

16. **Helm chart lacks `PodDisruptionBudget` and `ServiceMonitor` templates.**
    - **Evidence:** [helm/pg-tide/templates/](../helm/pg-tide/templates/) — only deployment, service, serviceaccount templates visible.
    - **Recommendation:** Add optional PDB (for multi-replica HA) and ServiceMonitor (for Prometheus Operator auto-discovery).

17. **`OutboxBatch::into_messages()` still clones payload rows.**
    - **Evidence:** Carried forward from assessment-2 §4.4 and assessment-3 §1.7/§5.5 — four assessments have noted this.
    - **Recommendation:** Switch to `self.inserted.into_iter()` to take ownership without cloning.

---

## Detailed Analysis by Area

### 1. Correctness & Bugs

The v0.17.0 sprint resolved the plpgsql/Rust duplication (§1.2 of assessment-3) and the DLQ write-failure swallow (§1.3). The catalog drift from assessment-3 §1.1 was addressed by adding the full migration chain to `lib.rs` — but incompletely: the chain stops at 0.21.0 while the current version is 0.22.0.

**New findings:**
- P0-1: `PgInboxSink` column mismatch — the fix from v0.13.0 only applied to `InboxSink`, not to `PgInboxSink`.
- P0-2: Missing `extension_sql_file!` for 0.21.0→0.22.0.
- P1-3: Test coverage gap for the last 5 migration scripts.
- P2-11: `commit_offset()` monotonicity guard still missing (4 assessments).

### 2. Security

The SSRF guard has been correctly extracted to `http_util.rs` (v0.18.0) and applied to ClickHouse, Elasticsearch, and Arrow Flight sinks — resolving assessment-3 §2.1. Identifier validation is in place for both local and remote inbox sinks (v0.18.0 — resolving §2.2).

**New findings:**
- P1-6: `ducklake_attach()` uses `%s` for system-derived values rather than `%L`.
- The `pg_tls` module (P1-4) provides fail-closed semantics but not actual TLS — this is a significant operational limitation rather than a security vulnerability per se, since it fails safely.

### 3. Code Quality & Maintainability

The blanket `#![allow(dead_code, unused_imports)]` was removed in v0.15.0. CLI subcommands have been extracted to `cmd/` modules (resolving assessment-3 §3.3). The `route_to_dlq()` helper was extracted (v0.18.0 — partially addressing §3.1).

**Remaining:**
- P3-15: `worker_inner()` is still monolithic (~500 lines).
- P2-8: `expect()` in `rate_limiter.rs` production code.
- P1-5: `expect()` in signal handlers.

### 4. Ergonomics & Developer Experience

The `PGTRICKLE_RELAY_*` references have been fully cleaned from docs (verified by grep). The CNPG example has been updated. `relay_set_outbox_v2()` was added in v0.18.0 for symmetric API ergonomics. CLI has env-var support via clap `env = "..."` attributes.

**No new findings in this area.** The DX improvements from v0.17.0–v0.18.0 addressed all prior assessment items.

### 5. Performance & Scalability

The coordinator now subscribes to `tide_relay_config` LISTEN/NOTIFY (resolving assessment-3 §5.2). Connection pooling via `deadpool-postgres` is in place. Backoff with `rand::thread_rng()` jitter replaces the LCG (resolving §3.5).

**New findings:**
- P2-7: `PgInboxSink` per-row INSERT loop (the local `InboxSink` was batched but the remote was not).
- P2-10: `outbox_status_impl()` uses 3 sequential SPI queries.

### 6. Reliability & Resilience

DLQ write failures now correctly pause the pipeline (v0.18.0 `route_to_dlq()` with `DlqOutcome::PermanentError`). Error classification is in place. Worker panic detection via `JoinHandle::is_finished()` works correctly.

**No new reliability findings.** The v0.17.0/v0.18.0 sprints addressed the remaining assessment-3 items.

### 7. Test Coverage

The `sql_to_sink_e2e.rs` test exists (resolving assessment-3 §6.1) and exercises the full SQL→coordinator→file-sink flow. Property-based tests exist for wire formats. DuckLake tests individually test each v0.21.0/v0.22.0 feature.

**New findings:**
- P1-3: Migration tests only go through v0.17.0 — five subsequent scripts are untested in the chain.
- The `sql_to_sink_e2e.rs` test uses a `file` sink rather than the `InboxSink` or `PgInboxSink`, which means the P0-1 column mismatch in `PgInboxSink` was not caught.

### 8. Operational Readiness

Helm chart is aligned at v0.22.0. SBOM (CycloneDX) and Trivy scanning were added in v0.19.0. The `just bump-version` recipe automates version alignment. Docker images include `/etc/pg-tide/pg-tide.example.toml`. Operations runbooks were added in v0.19.0.

**New findings:**
- P3-16: Missing PodDisruptionBudget and ServiceMonitor in Helm chart (nice-to-have for production maturity).

### 9. Missing Features

| Planned Feature | Source | Status | Forward-compat Risk |
|---|---|---|---|
| Encryption envelope + KMS | Roadmap v1.0 | Not implemented | Schema-safe; config extension only |
| WAL-based logical-replication source | Roadmap v1.2 | Not implemented | Needs new source type; no breaking changes |
| Web UI | Roadmap v1.3 | Not implemented | External; no schema impact |
| Outbox table partitioning by time | Roadmap v1.0 / ADR-001 | Not implemented | Breaking schema change; needs migration planning |
| DuckLake bidirectional flow | v0.22.0 | ⚠️ Partially broken (fresh install) | P0-2 blocks this |
| Real TLS (rustls/native-tls) | v0.15.0 claimed | Fail-closed only; no actual TLS | Feature flag extension |
| Multi-tenant per-tenant relay groups | Roadmap v1.1 | Catalog support present; relay runs as single role | Documented limitation |

### 10. Documentation Quality

Documentation has improved significantly. ADRs 001–005 are in place. Operations runbooks were added in v0.19.0. The `PGTRICKLE_RELAY_*` references are cleaned. The ROADMAP is comprehensive and accurate through v0.22.0.

**Minor gaps:**
- The README and relay-guide should clarify that `sslmode=require` causes the relay to refuse to connect (rather than establishing TLS), since no TLS backend is compiled in by default.
- The DuckLake bidirectional feature (v0.22.0) documentation likely references functions that don't exist on fresh install (P0-2).

---

## Catalog Drift Matrix

| Object | Fresh Install (via extension_sql_file chain) | Upgrade Path (0.1.0 → 0.22.0) | Match? |
|--------|----------------------------------------------|-------------------------------|--------|
| `tide.tide_outbox_config` | ✅ (0.1.0) | ✅ | ✅ |
| `tide.tide_outbox_messages` | ✅ (0.1.0) | ✅ | ✅ |
| `tide.tide_consumer_groups` | ✅ (0.1.0) | ✅ | ✅ |
| `tide.tide_consumer_offsets` | ✅ (0.1.0) | ✅ | ✅ |
| `tide.tide_consumer_leases` | ✅ (0.1.0) | ✅ | ✅ |
| `tide.tide_inbox_config` | ✅ (0.1.0) | ✅ | ✅ |
| `tide.relay_outbox_config` | ✅ (0.1.0) | ✅ | ✅ |
| `tide.relay_inbox_config` | ✅ (0.1.0) | ✅ | ✅ |
| `tide.relay_consumer_offsets` | ✅ (0.1.0) | ✅ | ✅ |
| `tide.relay_config_notify()` trigger | ✅ (0.1.0) | ✅ | ✅ |
| `tide.outbox_pending` view | ✅ (0.1.0) | ✅ | ✅ |
| `tide.consumer_lag` view | ✅ (0.1.0) | ✅ | ✅ |
| `tide.tide_security_audit` | ✅ (0.1.0) | ✅ | ✅ |
| `tide.grant_publish()` | ✅ (0.1.0) | ✅ | ✅ |
| `tide.revoke_publish()` | ✅ (0.1.0) | ✅ | ✅ |
| `tide.relay_config_audit()` trigger | ✅ (0.1.0) | ✅ | ✅ |
| `tide.outbox_publishers` | ✅ (0.12.0→0.13.0 migration) | ✅ | ✅ |
| `tide.relay_schema_fingerprints` | ✅ (0.13.0→0.14.0 migration) | ✅ | ✅ |
| `tide.relay_dlq` | ✅ (varies by migration) | ✅ | ✅ |
| `tide.backfill_jobs` | ✅ (0.13.0→0.14.0 migration) | ✅ | ✅ |
| `tide.ducklake_offset_map` | ✅ (0.20.0→0.21.0 migration) | ✅ | ✅ |
| `tide.ducklake_partition_config` | ✅ (0.20.0→0.21.0 migration) | ✅ | ✅ |
| `tide.ducklake_source_config` | ❌ **MISSING** | ✅ (0.21.0→0.22.0) | ❌ |
| `tide.ducklake_replicate()` | ❌ **MISSING** | ✅ (0.21.0→0.22.0) | ❌ |
| `tide.ducklake_source_last_snapshot()` | ❌ **MISSING** | ✅ (0.21.0→0.22.0) | ❌ |
| `tide.relay_enable()` plpgsql | ❌ (by design — Rust provides it) | ✅ (0.17.0→0.18.0 then Rust replaces) | ⚠️ By design |
| `tide.relay_disable()` plpgsql | ❌ (by design — Rust provides it) | ✅ (same) | ⚠️ By design |
| `tide.relay_set_outbox_v2()` plpgsql | ❌ (by design — Rust provides it) | ✅ (same) | ⚠️ By design |

---

## Untested Surface Matrix

| Function/Component | Test Exists | Test Type | Notes |
|---|---|---|---|
| `PgInboxSink` (remote) | ❌ | — | No integration test exercises it against a real inbox table schema |
| `tide.ducklake_replicate()` | ❌ | — | Only tested via `ducklake_test.rs` with manual SQL, not via the relay |
| `tide.ducklake_source_last_snapshot()` | ❌ | — | New in v0.22.0, no dedicated test |
| Migrations 0.17.0→0.22.0 (chained) | ❌ | — | `migration_test.rs` stops at 0.17.0 |
| `commit_offset()` monotonicity | ❌ | — | No test verifies offset cannot be rewound |
| `outbox_truncate_delivered()` on all outboxes | Partial | Unit test | No test with multiple outboxes and varied retention |
| DLQ-write permanent-error → pipeline pause | ❌ | — | assessment-3 §6.2 still unimplemented |
| Coordinator LISTEN/NOTIFY hot-reload | ✅ | Integration | `sql_to_sink_e2e.rs` exercises it |
| Schema evolution breaking-change pause | ✅ | Integration | `schema_evolution_test.rs` |
| DuckLake inlining | ✅ | Integration | `ducklake_test.rs` |
| DuckLake auto-partition | ✅ | Integration | `ducklake_test.rs` |

---

## Feature Gap Table

| Planned Feature | Source | Status | Forward-compat Risk |
|---|---|---|---|
| DuckLake bidirectional | Roadmap v0.22.0 | ⚠️ Broken on fresh install (P0-2) | None once lib.rs fixed |
| Encryption envelope + KMS | Roadmap v1.0.0 | Not started | Low — additive config |
| Outbox partitioning | Roadmap v1.0.0 | Not started | High — schema migration |
| Real TLS (rustls) | Roadmap v0.15.0 (claimed) | Partial (fail-closed only) | Low — feature flag |
| WAL logical-replication source | Roadmap v1.2.0 | Not started | Low — new source type |
| Web UI | Roadmap v1.3.0 | Not started | None |
| WASM transform plugins | Roadmap v1.2.0 | Not started | Low — new transform type |
| Multi-tenant per-tenant relay | Roadmap v1.1.0 | Partial (catalog ready) | Medium — runtime arch |
| `pg_dump` schema diff CI test | Assessment-3 §6.4 | Not implemented | — |
| Error classification integration test | Assessment-3 §6.5 | Not implemented | — |
| DLQ fault-injection test | Assessment-3 §6.2 | Not implemented | — |

---

## Recommendations Roadmap

### Sprint 1 — Critical Fixes (block v0.23.0)

**Exit criteria:** All P0 items resolved, CI green with full migration chain.

1. Add `extension_sql_file!("../../sql/pg_tide--0.21.0--0.22.0.sql", ...)` to `lib.rs`.
2. Fix `PgInboxSink` column names in `pg_outbox.rs`.
3. Update `migration_test.rs` and `sql_to_sink_e2e.rs` to include all migrations through 0.22.0.
4. Add an integration test for `PgInboxSink` against an extension-created inbox table.

### Sprint 2 — P1 Hardening

**Exit criteria:** Signal handlers safe, TLS clarified, DuckLake SQL validated.

5. Replace signal-handler `expect()` with non-panicking error propagation.
6. Fix `ducklake_attach()` to use `%L` for system-derived values.
7. Document the TLS limitation clearly in README and relay-guide; add a `native-tls` feature flag stub.
8. Add `pg_dump --schema-only` diff assertion between fresh install and upgrade chain (assessment-3 §6.4).

### Sprint 3 — P2 Quality

**Exit criteria:** Performance and correctness improvements landed.

9. Batch `PgInboxSink` inserts via UNNEST (matching `InboxSink`).
10. Add `commit_offset()` monotonicity guard.
11. Consolidate `outbox_status_impl()` into a single SPI query.
12. Replace `expect()` in `rate_limiter.rs` with a safe constant.
13. Demote per-batch success log from `info!` to `debug!`.

### Sprint 4 — Strategic (medium-term)

14. Implement real TLS via `rustls` or `native-tls` feature flag.
15. Add PodDisruptionBudget and ServiceMonitor to Helm chart.
16. Continue `worker_inner()` decomposition.
17. Add DLQ fault-injection test (assessment-3 §6.2).
18. Begin outbox partitioning design (ADR-006).

---

## Delta from Previous Assessments

### Fixed since overall_assessment_3.md (2026-05-12)

| Old Finding | Status | Evidence |
|---|---|---|
| §1.1 — Fresh-install vs upgrade catalog drift (0.1.0 → 0.16.0 chain) | ✅ Fixed | All migrations 0.1.0→0.21.0 now in `extension_sql_file!()` chain |
| §1.2 — Duplicate plpgsql/Rust definitions | ✅ Fixed | 0.16.0→0.17.0 migration drops plpgsql residuals; 0.17.0→0.18.0 excluded from fresh install by design |
| §1.3 — DLQ write-failure swallowed | ✅ Fixed | `route_to_dlq()` helper returns `DlqOutcome::PermanentError` → pipeline pauses |
| §2.1 — SSRF not applied to ClickHouse/ES/ArrowFlight | ✅ Fixed | `http_util::validate_url()` applied in all three constructors (v0.18.0) |
| §2.2 — Missing `validate_relay_identifier()` in InboxSink/PgInboxSink | ✅ Fixed | Both constructors now call it |
| §3.5 — Pseudo-random jitter (LCG) | ✅ Fixed | `rand::rng().random_range()` used (v0.18.0) |
| §4.2 / §7.1 — Stale PGTRICKLE_RELAY_* in docs | ✅ Fixed | No references remain in `docs/` |
| §4.3 / §7.2 — CNPG example outdated | ✅ Fixed | (per ROADMAP v0.17.0 release notes) |
| §5.2 — Coordinator not subscribing to LISTEN | ✅ Fixed | `main.rs` spawns LISTEN connection; `notif_rx` wired into coordinator |
| §6.1 — Missing SQL→relay→sink E2E test | ✅ Fixed | `sql_to_sink_e2e.rs` exists and exercises the full flow |
| §8.2 — No SBOM/Trivy | ✅ Fixed | v0.19.0 release notes confirm addition |
| §8.3 — No `just bump-version` recipe | ✅ Fixed | v0.19.0 |
| §9.3 — New coordinator metrics not in dashboard | ✅ Fixed | v0.19.0 |

### New findings in this audit

| # | Severity | Summary |
|---|---|---|
| P0-1 | Critical | `PgInboxSink` still uses wrong column names (regression not caught) |
| P0-2 | Critical | `pg_tide--0.21.0--0.22.0.sql` missing from `extension_sql_file!` chain |
| P1-3 | High | Integration tests don't cover migrations 0.17.0→0.22.0 |
| P1-4 | High | TLS is fail-closed but not actually implemented (no TLS backend compiled) |
| P1-5 | High | Signal handler `expect()` in main.rs |
| P1-6 | High | `ducklake_attach()` `%s` format specifier for connection params |
| P2-7 | Medium | `PgInboxSink` per-row INSERT loop |
| P2-8 | Medium | `expect()` in rate_limiter.rs |
| P2-9 | Medium | `ducklake_replicate()` generated names not length-validated |
| P2-10 | Medium | `outbox_status_impl()` 3× sequential SPI |
| P2-11 | Medium | `commit_offset()` monotonicity guard missing (4th assessment) |
| P2-12 | Medium | Per-batch info-level logging at high cardinality |

### Regressions

- **P0-1 is a partial regression of assessment-1 finding #3.** The fix was applied to `InboxSink` but missed `PgInboxSink`. Both use the same `super::Sink` trait but have divergent INSERT statements. The test suite does not exercise `PgInboxSink` against an actual extension-created inbox schema.

---

## Appendix: Metrics Snapshot

| Metric | Value |
|---|---|
| Total Rust source files (ext + relay src) | ~85 |
| Approximate lines of Rust (excluding tests) | ~24,000 |
| Approximate lines of SQL (all `sql/*.sql`) | ~2,100 |
| SQL upgrade scripts in `sql/` | 21 (0.1.0 base + 20 upgrades) |
| Migration files loaded by `extension_sql_file!()` | 20 (missing 0.17.0→0.18.0 by design + missing 0.21.0→0.22.0 **BUG**) |
| Sink backends | 30 |
| Source backends | 16 |
| Wire format implementations | 6+ |
| Integration test files | 56 |
| CLI subcommands | 8+ (run, doctor, validate-config, replay, asyncapi, sweep, status, ducklake) |
| Prometheus metrics exported | 12+ |
| Helm chart version / pg_tide.control / Cargo.toml | 0.22.0 / 0.22.0 / 0.22.0 (aligned) |
| ADRs published | 5 |
| Operations runbooks | 4 |

— End of report —
