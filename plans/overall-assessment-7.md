# pg_tide Overall Assessment — 2026-05-26

## Executive Summary

pg_tide has reached a strong post-GA baseline with v0.34.0. The Universal Reverse Pipeline Sinks release wires the last eight implemented-but-unregistered sink backends into `build_sink()`, making every sink available for both forward and reverse pipelines. The DuckLake multi-engine ecosystem compatibility guides, the community engagement deliverables, and the CI compatibility smoke-test are all present. All P0 and P1 findings from assessment-6 were resolved in v0.31.0–v0.33.0; no regressions have been detected from any prior assessment.

The current state is **strong, not yet world-class**. Four new issues stand out.

First, the KMS encryption skeleton introduced in v0.33.0 contains `todo!()` implementations in all four KMS provider structs (`AwsKms`, `GcpKms`, `VaultKms`, `LocalKeyFile`). These panic at runtime if a user enables `--features kms` and configures an encrypted outbox. The v0.33.0 plan explicitly deferred full implementation to v1.0.0 with the assumption that the skeleton would be safe-to-compile-but-not-call. It is not: `todo!()` panics. The providers must return a structured `RelayError::NotImplemented` instead.

Second, the migration test chain gap has recurred. The `migration_test.rs` UPGRADES array covers through v0.31.0 but does not include entries for `v0.32.0→0.33.0` or `v0.33.0→0.34.0`. Each of those migrations adds new catalog tables and SQL functions that are not exercised in CI.

Third, the fan-in coordinator worker introduced in v0.29.0 writes per-source offsets to `tide.relay_consumer_offsets` on every batch commit. With 10 contributing outboxes running at 1 000 messages/s each, this creates a hot write path on a single table row per source per pipeline, with risk of lock contention on high-cardinality fan-in configurations.

Fourth, the `relay_provision_tenant()` SQL function uses `EXECUTE format('CREATE ROLE %I ...', db_role)` but the `db_role NAME` input is accepted from the SQL caller without validating that it does not contain characters that `%I` cannot quote correctly in all PostgreSQL configurations. A malformed role name can cause a PL/pgSQL runtime error that exposes partial internal state.

**Top 3 remaining risks after v0.34.0:**

1. **KMS `todo!()` panics.** The `--features kms` build ships `todo!()` in all four providers. Any operator who reads v0.33.0's ADR-010, enables the KMS feature flag, and configures `outbox_encryption_config()` will get a relay panic rather than a helpful error.
2. **Migration test chain gap (v0.32.0→0.34.0).** Two consecutive migration scripts are untested in CI. The v0.33.0 and v0.34.0 migrations add `tide.relay_config_audit`, `tide.relay_pipeline_state`, `tide.relay_tenant_roles`, `tide.relay_delivery_receipts`, `tide.relay_fanin_config`, `tide.relay_pipeline_deps`, and several new columns — none of which are exercised by the migration test.
3. **Fan-in `relay_consumer_offsets` write contention.** Ten concurrent fan-in sources at high throughput share a table with a single write lock; at extreme cardinality this becomes a bottleneck that is difficult to observe because it manifests as elevated transaction wait time, not a relay error.

**Top 3 opportunities:**

1. **Complete KMS encryption implementation.** The ADR is written, the trait is designed, and the dependency scaffolding is in place. Completing the four providers in v0.35.0 delivers the v1.0.0 headline feature and unlocks the Production GA announcement.
2. **Fan-in performance architecture.** Moving from per-source sequential offset commits to a batched UNNEST upsert (analogous to the v0.13.0 inbox improvement) eliminates the contention bottleneck without a schema change.
3. **`pg-tide dag` Mermaid export.** The DAG is stored in `relay_pipeline_deps`, the cycle-detection CTE is written, but the `pg-tide dag show` Mermaid output is a stub. Completing it makes the DAG a first-class operational visibility tool.

**Delta vs. assessment-6:** All P0–P1 findings from assessment-6 are resolved. No regressions detected. Eight new findings documented below; the project retains zero P0 critical findings.

---

## Regressions from Prior Assessments

| Prior Finding | Prior Status | Current Status | Evidence |
|---|---|---|---|
| Assessment-6 P1-1: migration test gap v0.26.0→0.27.0 | Open | ✅ Resolved | [pg-tide-relay/tests/migration_test.rs](pg-tide-relay/tests/migration_test.rs) — `V0_26_0_TO_0_27_0` added in v0.31.0 |
| Assessment-6 P1-2: Helm chart version drift 0.26.0 vs 0.27.0 | Open | ✅ Resolved | [helm/pg-tide/Chart.yaml](helm/pg-tide/Chart.yaml) — bumped to 0.34.0; `just bump-version` now updates Chart.yaml |
| Assessment-6 P1-3: `PgInboxSink` unquoted table name | Open | ✅ Resolved | [pg-tide-relay/src/sink/pg_outbox.rs](pg-tide-relay/src/sink/pg_outbox.rs) — double-quotes applied in v0.31.0 |
| Assessment-6 P2-4: `poll_simple()` unquoted outbox name | Open | ✅ Resolved | [pg-tide-relay/src/source/outbox.rs](pg-tide-relay/src/source/outbox.rs) — double-quotes applied in v0.31.0 |
| Assessment-6 P2-5: publisher-ACL 3× SPI round-trips | Open | ✅ Resolved | [pg-tide-ext/src/outbox.rs](pg-tide-ext/src/outbox.rs) — consolidated CASE query in v0.32.0 |
| Assessment-6 P2-6: `inbox_status()` fleet N+1 | Open | ✅ Resolved | [pg-tide-ext/src/inbox.rs](pg-tide-ext/src/inbox.rs) — UNION ALL rewrite in v0.32.0 |
| Assessment-6 P2-7: webhook HMAC `.expect()` | Open | ✅ Resolved | [pg-tide-relay/src/source/webhook.rs](pg-tide-relay/src/source/webhook.rs) — `unreachable!()` + `// SAFETY:` in v0.32.0 |
| Assessment-6 P3-8: coordinator `unwrap_or_default` | Open | ✅ Resolved | [pg-tide-relay/src/coordinator.rs](pg-tide-relay/src/coordinator.rs) — explicit fallback `"{}".to_string()` in v0.32.0 |
| Assessment-6 P3-9: fleet `inbox_status` SPI propagation | Open | ✅ Resolved | [pg-tide-ext/src/inbox.rs](pg-tide-ext/src/inbox.rs) — `pgrx::error!()` on outer SPI failure in v0.32.0 |

**No regressions detected.** All findings marked resolved in assessments 1–6 remain resolved in the current codebase.

---

## Critical Findings (P0 — must fix before next release)

**None.**

For the third consecutive assessment, there are no P0 findings.

---

## High-Priority Findings (P1 — fix within 2 sprints)

### 1. KMS provider `todo!()` panics at runtime when `--features kms` is enabled

- **Severity:** P1 High
- **Location:** [pg-tide-relay/src/encryption.rs](pg-tide-relay/src/encryption.rs) — `AwsKms`, `GcpKms`, `VaultKms`, `LocalKeyFile` structs implementing `EncryptionEnvelope`; all four `fn encrypt()` and `fn decrypt()` bodies contain `todo!()`.
- **Root cause:** The v0.33.0 plan explicitly deferred KMS implementation to v1.0.0 and declared the skeleton "safe to compile." `todo!()` is not safe to call — it expands to `panic!()`. The ADR-010 contract document does not warn operators that the feature flag is non-functional.
- **Concrete impact:** Any operator who reads ADR-010, runs `cargo build --features kms`, deploys the resulting binary, and configures `tide.outbox_encryption_config()` will encounter a relay panic on the first encrypted publish. The process exits with a `thread '...' panicked at 'not yet implemented'` message rather than a structured `RelayError`.
- **Recommended fix:** Replace `todo!()` with a structured error in all eight provider methods:
  ```rust
  fn encrypt(&self, _plaintext: &[u8]) -> Result<EncryptedPayload, RelayError> {
      Err(RelayError::not_implemented(
          "kms", "AwsKms encryption is not yet implemented; available in v1.0.0"
      ))
  }
  ```
  Add a `RelayError::NotImplemented { provider: String, message: String }` variant to `error.rs`. Add a startup check in the coordinator: if a pipeline has `encryption_config` set and the KMS feature is enabled, validate the provider is implemented; if not, log an error and decline to acquire the pipeline rather than starting a worker that will panic mid-stream.

### 2. Migration test chain does not cover v0.32.0→0.33.0 or v0.33.0→0.34.0

- **Severity:** P1 High
- **Location:** [pg-tide-relay/tests/migration_test.rs](pg-tide-relay/tests/migration_test.rs) — `UPGRADES` array ends at the v0.31.0→0.32.0 entry.
- **Root cause:** The same recurring pattern: new migration scripts are added in each release but the migration test `UPGRADES` array is not updated. The CI lint step added in v0.31.0 that checks the workspace version against the last `UPGRADES` label appears to check `Cargo.toml` but may compare against the wrong label format, allowing the gap to persist undetected.
- **Concrete impact:** The v0.32.0→0.33.0 migration (which adds `tide.relay_pipeline_state`, `tide.relay_config_audit`, `audit.toml` deprecation activation) and the v0.33.0→0.34.0 migration (which adds `tide.relay_pipeline_templates` entries, `tide.relay_delivery_receipts`, and the `ducklake-source-to-lake` template) are never exercised in CI. Any DDL error — a malformed `ALTER TABLE`, a missing `CREATE INDEX`, a typo in a function body — would be invisible until a production upgrade.
- **Recommended fix:**
  ```rust
  const V0_32_0_TO_0_33_0: &str = include_str!("../../sql/pg_tide--0.32.0--0.33.0.sql");
  const V0_33_0_TO_0_34_0: &str = include_str!("../../sql/pg_tide--0.33.0--0.34.0.sql");
  // Add to UPGRADES:
  ("0.32.0 → 0.33.0", V0_32_0_TO_0_33_0),
  ("0.33.0 → 0.34.0", V0_33_0_TO_0_34_0),
  ```
  Also audit the CI lint step that asserts `migration_test.rs` contains the current version label — if it is comparing against the wrong string format, fix the comparison.

### 3. `relay_provision_tenant()` dynamic role name not validated before `EXECUTE format()`

- **Severity:** P1 High
- **Location:** [sql/pg_tide--0.28.0--0.29.0.sql](sql/pg_tide--0.28.0--0.29.0.sql) — `tide.relay_provision_tenant()` uses `EXECUTE format('CREATE ROLE %I WITH LOGIN', p_db_role)` but `p_db_role NAME` accepts any 63-byte PostgreSQL identifier without application-level validation before the `EXECUTE`.
- **Root cause:** PostgreSQL's `%I` format specifier double-quotes identifiers correctly for legal identifiers, but role names in PostgreSQL can contain characters that are legal in `NAME` type but produce unexpected SQL when used with `%I` in some edge cases (e.g., names beginning with a digit when quoted). More critically, the function has no check that `p_db_role` does not collide with existing PostgreSQL system roles (`pg_monitor`, `pg_read_all_data`, etc.) or the `tide_admin` role itself.
- **Concrete impact:** A caller with `tide_admin` privileges can accidentally provision a tenant against a system role name, granting the relay's GRANT statements to a role with system-wide read access. Also, role names beginning with digits produce PostgreSQL identifiers that may behave unexpectedly in `search_path` contexts.
- **Recommended fix:**
  ```sql
  IF p_db_role ~ '^[0-9]' OR p_db_role = ANY(ARRAY['pg_monitor','pg_read_all_data',
      'pg_read_all_settings','pg_signal_backend','pg_write_all_data','tide_admin']) THEN
    RAISE EXCEPTION 'invalid or reserved role name: %', p_db_role;
  END IF;
  IF NOT (p_db_role ~ '^[A-Za-z_][A-Za-z0-9_]{0,62}$') THEN
    RAISE EXCEPTION 'role name must match [A-Za-z_][A-Za-z0-9_]{0,62}: %', p_db_role;
  END IF;
  ```
  Apply the same validation to `relay_deprovision_tenant()`.

---

## Medium-Priority Findings (P2 — fix within current quarter)

### 4. Fan-in coordinator writes per-source offset rows with per-batch individual UPSERTs

- **Severity:** P2 Medium
- **Location:** [pg-tide-relay/src/coordinator.rs](pg-tide-relay/src/coordinator.rs) — fan-in worker loop; the `commit_offset()` call is made once per source per batch, serially inside the `tokio::select!` merge loop.
- **Root cause:** The fan-in worker was implemented by reusing the existing single-source offset-commit path for each member source. No batching or UNNEST consolidation was applied.
- **Concrete impact:** A fan-in pipeline with 10 contributing outboxes each delivering 1 000 messages/s generates 10 000 sequential `UPDATE tide.relay_consumer_offsets` calls per second. At 50+ fan-in pipelines, this creates measurable lock contention on the `relay_consumer_offsets` table, increasing per-batch latency and potentially causing pipeline workers to queue behind each other's offset commits.
- **Recommended fix:** Accumulate all per-source offset updates for a single fan-in pipeline batch into a single UNNEST upsert:
  ```sql
  INSERT INTO tide.relay_consumer_offsets
    (pipeline_name, fanin_member, committed_offset, updated_at)
  SELECT * FROM UNNEST($1::text[], $2::text[], $3::bigint[], $4::timestamptz[])
  ON CONFLICT (pipeline_name, fanin_member)
  DO UPDATE SET committed_offset = EXCLUDED.committed_offset,
    updated_at = EXCLUDED.updated_at
  WHERE relay_consumer_offsets.committed_offset <= EXCLUDED.committed_offset
  ```
  This reduces 10 sequential UPDATEs to 1 UNNEST upsert per fan-in batch, consistent with the pattern used by the inbox sink since v0.13.0.

### 5. `backfill_progress()` estimated completion divides by zero when throughput is zero

- **Severity:** P2 Medium
- **Location:** [pg-tide-ext/src/outbox.rs](pg-tide-ext/src/outbox.rs) or the migration SQL — `tide.backfill_progress(job_id)` computes `estimated_completion = now() + ((total_rows - rows_processed) / throughput) * interval '1 second'` where `throughput` is derived from `rows_processed / EXTRACT(epoch FROM now() - job_started_at)`.
- **Root cause:** When `job_started_at = now()` (the job was just created), `EXTRACT(epoch FROM interval)` returns `0` or a very small value, making `throughput = 0` and the division undefined. PostgreSQL raises a division-by-zero exception.
- **Concrete impact:** Calling `tide.backfill_progress()` immediately after `tide.backfill_schedule()` or after a pause-resume cycle raises `ERROR: division by zero` and surfaces as an unhandled exception to callers. The `pg-tide status` command, which queries progress for all active backfill jobs, would crash on any newly-created or recently-resumed job.
- **Recommended fix:**
  ```sql
  CASE WHEN EXTRACT(epoch FROM (now() - job_started_at)) < 1 THEN NULL
       WHEN rows_processed = 0 THEN NULL
       ELSE now() + ((total_rows - rows_processed)::float /
            (rows_processed::float /
             NULLIF(EXTRACT(epoch FROM (now() - job_started_at)), 0))
            ) * interval '1 second'
  END AS estimated_completion
  ```
  Return `NULL` estimated_completion when throughput is not yet measurable rather than raising an exception.

### 6. Delivery receipt table grows unboundedly when `pg-tide sweep` is not scheduled

- **Severity:** P2 Medium
- **Location:** [pg-tide-relay/src/coordinator.rs](pg-tide-relay/src/coordinator.rs) — `tide.relay_delivery_receipts` is written on every successful batch ack but is only pruned by `tide.relay_truncate_delivery_receipts()`, which is called by `pg-tide sweep` on its schedule.
- **Root cause:** The `delivery_receipt_retention` config key defaults to `30 days` and `pg-tide sweep` must be explicitly invoked (scheduled or via the relay background task). In deployments where the sweep is not configured — which is the common case for new adopters who do not read the sweep documentation — the table grows without bound.
- **Concrete impact:** On a pipeline delivering 10 000 messages/day, the `relay_delivery_receipts` table accumulates 300 000 rows/month. After 12 months of unattended operation, the table exceeds 3.6 million rows with a corresponding index on `(pipeline_name, delivered_at)`, causing `commit_offset()` transaction times to increase as the table bloats.
- **Recommended fix:** The relay coordinator should automatically schedule a background sweep task on startup using `tokio::spawn` with a configurable interval (default: daily). The task should call `tide.relay_truncate_delivery_receipts(delivery_receipt_retention)` independently of the `pg-tide sweep` CLI command. Add a `sweep_interval_hours` config key (default: `24`) and a `pg-tide doctor` check that warns when the receipt table exceeds 1 million rows.

### 7. DAG `relay_pipeline_dep_add()` trigger does not validate `trigger_policy` enum at SQL level

- **Severity:** P2 Medium
- **Location:** [sql/pg_tide--0.30.0--0.31.0.sql](sql/pg_tide--0.30.0--0.31.0.sql) — `tide.relay_pipeline_dep_add()` accepts `trigger_policy TEXT` but does not validate it against the documented set (`always`, `on_idle`, `on_offset_gte(N)`).
- **Root cause:** The trigger policy is stored as free-form TEXT and parsed by the coordinator at runtime. An invalid policy string (e.g., a typo `"on_idel"`) is silently stored and never matched by the coordinator's pattern-match arm, meaning the downstream pipeline never acquires, silently and permanently stalled.
- **Concrete impact:** A pipeline configured with a misspelled `trigger_policy` will never start its downstream pipelines. The operator has no immediate feedback; the `pg-tide dag status` command shows the downstream as "gated" but does not indicate the policy is invalid. This is particularly dangerous in production environments where DAG-ordered backfill sequences are expected to progress automatically.
- **Recommended fix:** Add a SQL-level CHECK constraint or a validation block in `relay_pipeline_dep_add()`:
  ```sql
  IF p_trigger_policy NOT SIMILAR TO 'always|on_idle|on_offset_gte\([0-9]+\)' THEN
    RAISE EXCEPTION 'invalid trigger_policy ''%''; valid values: always, on_idle, on_offset_gte(N)',
      p_trigger_policy;
  END IF;
  ```
  Add a corresponding unit test that inserts an invalid policy and asserts the exception is raised.

### 8. `pg-tide dag show` Mermaid diagram export is not implemented (stub)

- **Severity:** P2 Medium
- **Location:** [pg-tide-relay/src/main.rs](pg-tide-relay/src/main.rs) or the equivalent `cmd/dag.rs` module — `pg-tide dag show` emits a placeholder string `"# DAG export not yet implemented"` rather than a valid Mermaid diagram.
- **Root cause:** The DAG CLI was scaffolded in v0.30.0 with `pg-tide dag check` and `pg-tide dag status` fully implemented, but `pg-tide dag show` was explicitly deferred as a post-v0.30.0 polish item and the stub was not converted.
- **Concrete impact:** Operators and SREs who rely on the documented `pg-tide dag show` output for operational runbooks and incident diagrams receive a placeholder. Documentation that references `pg-tide dag show | mermaid` pipelines silently produces empty diagrams.
- **Recommended fix:** Implement the Mermaid export by querying `tide.relay_pipeline_deps` and formatting the result as a directed graph:
  ```rust
  println!("```mermaid");
  println!("graph LR");
  for (upstream, downstream, policy) in rows {
      println!("  {upstream} -->|{policy}| {downstream}");
  }
  println!("```");
  ```
  Add a test that asserts the output matches the expected Mermaid syntax for a known three-node DAG.

---

## Low-Priority Findings (P3 — backlog)

### 9. DAG integration tests cover only linear chains; branching and diamond topologies untested

- **Severity:** P3 Low
- **Location:** [pg-tide-relay/tests/dag_integration_test.rs](pg-tide-relay/tests/dag_integration_test.rs) — three-pipeline linear `A → B → C` test only.
- **Recommended fix:** Add test cases for: (a) diamond topology `A → B, A → C, B → D, C → D`; (b) fan-out `A → B, A → C, A → D`; (c) multi-level with `on_idle` and `on_offset_gte(N)` policies in the same graph. Assert each topology produces the expected acquisition order and no deadlock.

### 10. KMS feature-gate documentation does not warn that providers are not yet implemented

- **Severity:** P3 Low
- **Location:** [README.md](README.md) — the feature-gate table lists `kms` with description "Envelope encryption with KMS" but no "not yet implemented" caveat.
- **Recommended fix:** Add a `> ⚠️ Not yet available — implementation lands in v1.0.0` warning after the `kms` row in the feature table and in `docs/src/relay-guide/configuration.md` KMS section.

### 11. `pg-tide history <pipeline> --since TIMESTAMP` flag missing

- **Severity:** P3 Low
- **Location:** [pg-tide-relay/src/main.rs](pg-tide-relay/src/main.rs) — `pg-tide history` accepts `--limit N` but not `--since TIMESTAMP` despite the roadmap specifying both flags.
- **Recommended fix:** Add `--since` as a `NaiveDateTime` `clap` argument with a `value_parser` that accepts ISO-8601 timestamps and passes it as a `$since TIMESTAMPTZ` bind parameter to the `tide.relay_config_history()` query.

### 12. AsyncAPI `validate` exit code does not distinguish schema-mismatch from pipeline-not-found

- **Severity:** P3 Low
- **Location:** [pg-tide-relay/src/main.rs](pg-tide-relay/src/main.rs) — `pg-tide asyncapi validate` exits with code 1 for both "channel in spec absent from relay" and "channel schema doesn't match observed payload."
- **Recommended fix:** Use exit code 1 for missing channels and exit code 2 for schema mismatches, enabling CI scripts to handle the two cases differently (fail on missing channels; warn on schema drift).

---

## Area Summaries

### 1. Correctness & Bugs

Good overall. P1-1 (KMS `todo!()`) and P1-2 (migration test gap) are the two systemic correctness risks. P1-3 (`relay_provision_tenant` role validation) is a security-adjacent correctness issue. P2-5 (`backfill_progress` division by zero) is a data-quality risk. No data-loss scenarios identified.

### 2. Security

**Excellent.** No new security findings beyond P1-3 (role name validation). All prior security findings remain resolved:
- ✅ `validate_identifier()` in extension, `validate_relay_identifier()` in relay.
- ✅ SSRF guard on all HTTP sinks.
- ✅ TLS via `native-tls`.
- ✅ `SECURITY DEFINER` functions hardened.
- ✅ `cargo-deny` / `audit.toml` in CI.
- ✅ Per-tenant advisory lock namespacing.
- ✅ Cosign signing + SBOM + Trivy.
- ✅ Delivery receipt write privilege checked by `pg-tide doctor`.
- ✅ Claim-check `pg_largeobject` ownership granted to relay role on creation.

### 3. Performance & Scalability

Strong at baseline load. P2-4 (fan-in offset write contention) is the one new scalability risk and is directly addressable with the UNNEST upsert pattern already used elsewhere. P2-6 (delivery receipt table growth) is operational rather than a hotpath issue but has a clear mitigation.

### 4. Ergonomics & Developer Experience

High quality. P2-8 (`pg-tide dag show` stub) and P3-11 (`--since` flag) are the only ergonomics gaps. The backfill CLI, fan-in status display, and DAG `check` / `status` are all functional. The pipeline template `pg-tide template apply` and `pg-tide migrate-config` commands lower the adoption barrier.

### 5. Test Coverage

**Good.** 70+ integration test files. Known gaps:
- P1-2: migration test chain not current.
- P3-9: DAG tests cover linear only.
- KMS providers have no test asserting that `todo!()` replacement returns `RelayError::NotImplemented` (after the P1-1 fix, add tests).
- Fan-in `priority` and `subject_hash` merge strategies are integration-tested but not covered by property-based tests.

### 6. Documentation

**Excellent.** ADRs 001–010 published, migration guide, stability guarantees, and per-sink reference pages all present. P3-10 (KMS feature flag caveat) is the only documentation gap.

### 7. Observability

**Complete.** 20+ Prometheus metrics, per-tenant labels, per-pipeline Grafana panels, fan-in source lag panels, backfill progress panel, delivery receipt rate panel, alerting rules for five failure modes. No new observability gaps detected.

### 8. Release & Packaging

**Strong.** `just bump-version` now covers all four files. Helm chart version-alignment CI check is in place. One latent risk: the `--features kms` Docker image build (`:latest-full`) includes the panic-prone `todo!()` providers — this is addressed by P1-1.

### 9. Missing Features & Roadmap Gaps

The only roadmap item not yet delivered is KMS encryption implementation (scoped to v1.0.0, deferred by design but blocked by the `todo!()` panic risk). `pg-tide dag show` Mermaid export (P2-8) is a gap between the documented capability and the shipped implementation.

---

## Recommended Sprint Plan

1. **Replace KMS `todo!()` with `RelayError::NotImplemented` (P1-1)** — 1 hour; blocks safe `:latest-full` distribution.
2. **Add v0.32.0→0.33.0 and v0.33.0→0.34.0 to `migration_test.rs` (P1-2)** — 20 minutes; closes the recurring CI gap permanently.
3. **Add role-name validation to `relay_provision_tenant()` (P1-3)** — 30 minutes; prevents accidental system-role grants.
4. **Implement fan-in UNNEST offset upsert (P2-4)** — 4 hours; eliminates high-cardinality contention.
5. **Fix `backfill_progress()` division-by-zero (P2-5)** — 1 hour; prevents `pg-tide status` crash on new jobs.
6. **Add coordinator background sweep for delivery receipts (P2-6)** — 2 hours; prevents table growth in default deployments.
7. **Add `trigger_policy` CHECK constraint to `relay_pipeline_dep_add()` (P2-7)** — 30 minutes; prevents silent DAG stalls.
8. **Implement `pg-tide dag show` Mermaid export (P2-8)** — 2 hours; delivers documented capability.
9. **Add KMS feature-gate caveat to README and docs (P3-10)** — 15 minutes; prevents operator confusion.
10. **Implement KMS encryption (v1.0.0 blocker)** — 2–3 sprints; enables Production GA.

---

## Path to v1.0 GA

### Must-do (block release)

1. Fix P1-1 (KMS `todo!()` panic) — required before the `:latest-full` image can be distributed with `--features kms`.
2. Fix P1-2 (migration test gap) — required to trust the migration chain through v0.34.0.
3. Fix P1-3 (role name validation) — required before multi-tenant provisioning is safe in shared-database deployments.
4. Implement KMS encryption (four providers: AWS, GCP, Vault, LocalKeyFile) — the last v1.0.0 roadmap commitment.
5. Remove deprecated positional SQL API variants (`relay_set_outbox()` 6-param, `relay_set_inbox()` 8-param) — deprecation warnings have been active since v0.33.0.

### Should-do (strongly recommended)

6. Fix P2-4 (fan-in offset write contention) — performance regression at scale.
7. Fix P2-5 (backfill division-by-zero) — prevents user-facing error in common workflow.
8. Fix P2-6 (delivery receipt table growth) — operational hygiene for default deployments.
9. Implement `pg-tide dag show` Mermaid export — complete the documented CLI surface.
10. Final `cargo deny check` re-evaluation and `audit.toml` refresh.

### Can follow in v1.1

11. DAG branching/diamond topology tests (P3-9).
12. `--since` flag for `pg-tide history` (P3-11).
13. AsyncAPI `validate` exit code distinction (P3-12).
14. WAL logical-replication source full implementation (groundwork laid in v0.32.0).

---

## What Is Already World-Class

1. **Universal sink and source matrix.** 30 sink backends and 16 source backends, every one registered in `build_sink()` / `build_source()`. No more "implemented but unregistered" gaps after v0.34.0.

2. **Pipeline lifecycle management.** Config history audit trail, pause/resume with `auto_resume_after`, pipeline state persistence, and the `pg-tide history` and `pg-tide status` CLIs give operators complete visibility and control over every pipeline's lifecycle.

3. **DuckLake ecosystem completeness.** Exactly-once PostgreSQL→DuckLake delivery, multi-engine compatibility guides (DataFusion, Spark, Trino, Pandas), CI compatibility smoke-test, five tutorials, four conference demo scripts, and the awesome-ducklake community submission. The reverse pipeline (DuckLake → pg-tide inbox) and cross-lake replication are also fully operational.

4. **Defence-in-depth identifier validation.** `validate_identifier()` in extension, `validate_relay_identifier()` in relay, `lint-quoting` CI recipe for `format!()` SQL interpolation, and double-quoting applied uniformly to all dynamic SQL construction sites.

5. **Release automation discipline.** `just bump-version` updates all four version locations atomically. CI asserts `Chart.yaml == Cargo.toml` version. Migration test lint step detects chain gaps at PR time. `just release-notes` generates the full release body automatically.

6. **Observability completeness.** 20+ Prometheus metrics with per-tenant labels, OTel spans covering the full message lifecycle, validated Grafana dashboard with per-tenant drill-down, delivery receipt rate panels, backfill progress panels, and alert rules for five critical failure modes.

---

## Appendix: File-by-File Findings Index

| File | Finding IDs |
|---|---|
| [pg-tide-relay/src/encryption.rs](pg-tide-relay/src/encryption.rs) | P1-1, P3-10 |
| [pg-tide-relay/tests/migration_test.rs](pg-tide-relay/tests/migration_test.rs) | P1-2 |
| [sql/pg_tide--0.28.0--0.29.0.sql](sql/pg_tide--0.28.0--0.29.0.sql) | P1-3 |
| [pg-tide-relay/src/coordinator.rs](pg-tide-relay/src/coordinator.rs) | P2-4, P2-6 |
| [pg-tide-ext/src/outbox.rs](pg-tide-ext/src/outbox.rs) | P2-5 |
| [sql/pg_tide--0.30.0--0.31.0.sql](sql/pg_tide--0.30.0--0.31.0.sql) | P2-7 |
| [pg-tide-relay/src/main.rs](pg-tide-relay/src/main.rs) | P2-8, P3-11, P3-12 |
| [pg-tide-relay/tests/dag_integration_test.rs](pg-tide-relay/tests/dag_integration_test.rs) | P3-9 |
| [README.md](README.md) | P3-10 |

---

## Metrics Snapshot

| Metric | Value |
|---|---|
| Total Rust source files (ext + relay src) | ~100 |
| Approximate lines of Rust (excluding tests) | ~30 000 |
| Approximate lines of SQL (all `sql/*.sql`) | ~3 200 |
| SQL upgrade scripts in `sql/` | 34 (0.1.0 base + 33 upgrades) |
| Sink backends registered in `build_sink()` | 30 (all registered as of v0.34.0) |
| Source backends registered in `build_source()` | 16 |
| Wire format implementations | 8+ |
| Integration test files | 70+ |
| CLI subcommands | 14 |
| Prometheus metrics exported | 22 |
| OTel spans emitted | 14+ |
| Helm chart templates | 9 |
| Helm chart version / pg_tide.control / Cargo.toml | 0.34.0 / 0.34.0 / 0.34.0 (aligned) |
| ADRs published | 10 (ADR-001 through ADR-010) |
| Operations runbooks | 8+ |
| `audit.toml` ignored advisories | 9 (all in feature-gated optional deps; default build clean) |
| P0 findings | 0 |
| P1 findings | 3 |
| P2 findings | 5 |
| P3 findings | 4 |
| Total new findings | 12 |

— End of report —
