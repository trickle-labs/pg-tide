# pg_tide Overall Assessment — 2026-05-12

> Third independent audit, performed after the v0.15.0 (TLS / resilience) and v0.16.0
> (DX / observability) sprints. Previous assessments:
> [overall_assessment_1.md](overall_assessment_1.md) (2026-05-05),
> [overall_assessment_2.md](overall_assessment_2.md) (2026-05-06).

---

## Executive Summary

**Overall health: strong.** The two release sprints since
[overall_assessment_2.md](overall_assessment_2.md) closed every P0 and the bulk
of the P1/P2 items it identified. TLS is wired through every
`tokio_postgres::connect()` call site that touches the database; transient vs.
permanent error classification is in place and feeds the `pipeline_errors_total`
metric; the coordinator stores `JoinHandle`s and detects worker panics on
reconcile; `deadpool-postgres` is used for coordinator metadata operations;
exponential backoff with jitter is implemented; secret values are redacted before
logging; the OTel span set has been broadened to cover transforms, routing, DLQ
insert, schema-evolution check, and backoff sleep; the three new coordinator
metrics (`owned_pipelines`, `reconcile_duration_seconds`, `pipeline_errors_total`)
are exported; the Helm chart now sets a hardened `securityContext`; cosign signs
release artefacts; lychee link-checks docs; and `migration_test.rs` walks the
full 0.1.0 → 0.16.0 upgrade chain.

**Top 3 remaining risks**

1. **Catalog drift between fresh installs and upgrades.** `pg-tide-ext/src/lib.rs`
   only loads `sql/pg_tide--0.1.0.sql` and `sql/pg_tide--0.13.0--0.14.0.sql`
   via `pgrx::extension_sql_file!()`. The 0.2.0 → 0.13.0 and the 0.14.0 → 0.16.0
   migration files are **never executed** on a fresh `CREATE EXTENSION pg_tide`.
   Most missing-table risk is masked because `sql/pg_tide--0.1.0.sql` has been
   retrofitted with `outbox_publishers`, `relay_schema_fingerprints`, and
   `relay_limits`, but several artefacts introduced in 0.14.0 → 0.15.0 and
   0.15.0 → 0.16.0 (the plpgsql `outbox_truncate_delivered`,
   `outbox_create_if_not_exists`, and `relay_set_inbox_v2` definitions, plus
   the documentation comments) are silently dropped on fresh installs.
   The pgrx-generated `#[pg_extern]` versions cover the function calls in
   practice, but this is a fragile contract: the base SQL is now a hand-edited
   superset of v0.1.0 and the migration scripts duplicate logic that lives in
   Rust.

2. **Duplicate SQL function definitions (Rust ⇄ plpgsql).** Several functions
   exist both as `#[pg_extern]` in Rust and as `CREATE OR REPLACE FUNCTION` in
   the migration scripts — notably `outbox_create_if_not_exists`,
   `relay_set_inbox_v2`, and `outbox_truncate_delivered`. On upgrade, the
   plpgsql migration overwrites the Rust-pgrx implementation; on fresh install
   the Rust implementation wins because the migration is never loaded. The two
   implementations have diverged signatures
   (`outbox_truncate_delivered(TEXT NULL)` in Rust returns `INT64`, while the
   plpgsql variant takes `TEXT NOT NULL` and returns `BIGINT`), creating a
   real overload ambiguity.

3. **No SQL → relay → sink integration test.** Despite a `sql_api_test.rs`,
   `round_trip_test.rs`, and 30+ sink-specific tests, no single test calls
   `tide.relay_set_outbox()` via SQL, starts a real coordinator task,
   `tide.outbox_publish()`-es a message, and asserts the message arrived at a
   sink. The risk identified in
   [overall_assessment_2.md](overall_assessment_2.md) §6.1 is still open.
   Contract drift between the SQL API and the relay runtime — the exact
   problem that motivated the v0.12.0 sprint — remains undetectable by CI.

**Top 3 opportunities**

1. Replace the hand-maintained `pg_tide--0.1.0.sql` superset with a layered
   `extension_sql_file!` chain that loads every migration in order, eliminating
   the fresh-install vs upgrade drift and removing the duplicate plpgsql
   definitions in 0.14.0 → 0.16.0 migrations.
2. Build a single end-to-end integration test fixture in `tests/sql_to_sink_e2e.rs`
   that wires SQL → coordinator task → stdout sink in one process, locking in
   the v0.12.0 contract permanently.
3. Cut DLQ-write-failure backpressure into the coordinator: today a constraint
   violation on `tide.relay_dlq` is logged at WARN and silently swallowed,
   letting the pipeline loop on the same poisoned message forever
   ([coordinator.rs L776–L793](../pg-tide-relay/src/coordinator.rs#L776)).

**Delta vs. previous assessments.** Twelve P0/P1/P2 items from
overall_assessment_2 are resolved; none have regressed; three new findings of
note are catalogued in §1 / §3 below (catalog drift, plpgsql-Rust duplication,
DLQ swallow). The codebase has crossed the "feature complete for 1.0"
threshold; remaining work is hardening, ergonomics, and the integration-test
gap.

---

## Findings by Area

### 1. Correctness & Bugs

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 1.1 | High | [pg-tide-ext/src/lib.rs#L32-L44](../pg-tide-ext/src/lib.rs#L32) | `extension_sql_file!()` loads only `pg_tide--0.1.0.sql` and `pg_tide--0.13.0--0.14.0.sql`. Migrations 0.2.0 → 0.13.0 and 0.14.0 → 0.16.0 are not executed on fresh installs. The 0.1.0 file is a hand-maintained superset of v0.1.0; if a future migration adds a table that is not retrofitted into 0.1.0, fresh installs will be broken. | Future contributors may add an object only in an upgrade script and fresh `CREATE EXTENSION pg_tide` deployments will diverge from upgrade deployments. | Replace the two-file include with an ordered chain that includes every `sql/pg_tide--*.sql` in version order via `requires =` dependencies. Add a CI test that snapshots `pg_dump --schema-only` of a fresh install and a 0.1.0 → 0.16.0 upgrade and diffs them. |
| 1.2 | High | [sql/pg_tide--0.14.0--0.15.0.sql#L20](../sql/pg_tide--0.14.0--0.15.0.sql#L20), [sql/pg_tide--0.15.0--0.16.0.sql#L24](../sql/pg_tide--0.15.0--0.16.0.sql#L24), [sql/pg_tide--0.15.0--0.16.0.sql#L75](../sql/pg_tide--0.15.0--0.16.0.sql#L75) | `outbox_truncate_delivered`, `outbox_create_if_not_exists`, and `relay_set_inbox_v2` are defined as plpgsql in migrations and as `#[pg_extern]` in Rust ([outbox.rs#L79](../pg-tide-ext/src/outbox.rs#L79), [outbox.rs#L350](../pg-tide-ext/src/outbox.rs#L350), [relay.rs#L172](../pg-tide-ext/src/relay.rs#L172)). On upgrade, the plpgsql definition replaces the Rust-pgrx C wrapper; on fresh install only the Rust version exists (because §1.1 means 0.15/0.16 migrations don't run). The signatures even diverge: Rust `outbox_truncate_delivered` takes `Option<String>`/returns `INT64`, plpgsql takes `TEXT NOT NULL`/returns `BIGINT`, which creates a function-overload pair the user must disambiguate. | Confusing behaviour across deployment shapes; potential overload ambiguity for callers; logic that exists in two places will inevitably diverge. | Pick one source of truth per function. Delete the plpgsql versions in migrations and let the Rust `#[pg_extern]` define them (consistent with the rest of the API), or vice versa. Add a unit test that asserts each function name resolves to exactly one signature. |
| 1.3 | Medium | [pg-tide-relay/src/coordinator.rs#L776-L793](../pg-tide-relay/src/coordinator.rs#L776) | A failed `dlq::insert_batch()` (e.g. permissions error on `tide.relay_dlq`, unique violation, full disk on the WAL volume) is logged at WARN and ignored. The source is *not* acknowledged, so the worker re-reads the same poisoned batch on the next poll and retries forever. | Tight loop with WARN-level log spam on every poll; pipeline never makes progress; metrics show steady `messages_consumed_total` increments with zero `messages_published_total`. | Treat a DLQ insert error as a permanent pipeline error: classify with `RelayError::is_transient()` and pause the worker via the same path used for transient-error backoff. Add an alerting metric `pg_tide_relay_dlq_write_errors_total`. |
| 1.4 | Medium | [pg-tide-ext/src/outbox.rs#L26](../pg-tide-ext/src/outbox.rs#L26), [outbox.rs#L39](../pg-tide-ext/src/outbox.rs#L39), [inbox.rs#L10](../pg-tide-ext/src/inbox.rs#L10), [inbox.rs#L217](../pg-tide-ext/src/inbox.rs#L217), [relay.rs#L17](../pg-tide-ext/src/relay.rs#L17) | `outbox_exists()`, `inbox_exists()`, `relay_exists()`, `get_outbox_retention()`, and `inbox_status()` fleet loop all use the pattern `Spi::get_one_with_args(...).unwrap_or(None).unwrap_or(false)`. A real SPI error (deadlock, transient outage) is masked as "row not found" / `0`. | False negative on existence checks causes `outbox_create()` to attempt a duplicate insert (caught by the unique constraint, but with a confusing error); `inbox_status()` can return `pending = 0` during a real outage. | Convert helpers to `Result<T, PgTideError>` and propagate. Carried over from overall_assessment_2 §1.6 — was deferred. |
| 1.5 | Medium | [pg-tide-relay/src/source/outbox.rs](../pg-tide-relay/src/source/outbox.rs) (claim-check decode path) | Per overall_assessment_2 §1.2 the claim-check path references `tide.outbox_delta_rows_*` and `tide.outbox_rows_consumed()` which are pg_trickle artefacts. v0.15.0 added a "guard" in `pg-tide doctor` but the source itself still attempts the SQL — failure is surfaced as a generic relation-not-found error at runtime. | A pg_tide-only installation that configures claim-check (any `outbox.inline_threshold` exceeded) errors at first publish. | Add `RelayError::ClaimCheckUnavailable` and detect missing relation in the source; document claim-check as pg_trickle-only in `docs/src/concepts/`. |
| 1.6 | Low | [pg-tide-ext/src/relay.rs#L211-L236](../pg-tide-ext/src/relay.rs#L211) | `relay_enable()` / `relay_disable()` silently no-op when the pipeline doesn't exist. v0.16.0 added a documentation comment but the behaviour is still surprising compared to `relay_delete()` which raises. | Operators silently lose typo'd enable/disable calls. | Either raise `PipelineNotFound` or return a `BOOLEAN` indicating whether a row was affected (matches the new `outbox_create_if_not_exists()` pattern). |
| 1.7 | Low | [pg-tide-relay/src/envelope.rs](../pg-tide-relay/src/envelope.rs) (`OutboxBatch::into_messages()`) | Carried over from overall_assessment_2 §1.5: each payload row is cloned. | 2× memory pressure on large claim-check batches. | Switch to `self.inserted.into_iter()`. |
| 1.8 | Low | [pg-tide-ext/src/outbox.rs#L156-L183](../pg-tide-ext/src/outbox.rs#L156) | The publisher-ACL check resolves `current_user` via a separate SPI call before the `WHERE role_name = $2` check; if `current_user` returns NULL (impossible in PG, but the code unwraps), the check is bypassed by virtue of `unwrap_or("")`. | Theoretical only — no real path produces NULL. | Use `SESSION_USER` directly via a single SQL statement (`SELECT EXISTS(SELECT 1 FROM tide.outbox_publishers WHERE outbox_name = $1 AND role_name = current_user)`). |

### 2. Security

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 2.1 | Medium | [pg-tide-relay/src/sink/clickhouse.rs#L25](../pg-tide-relay/src/sink/clickhouse.rs#L25), [arrow_flight.rs#L83](../pg-tide-relay/src/sink/arrow_flight.rs#L83), [elasticsearch.rs](../pg-tide-relay/src/sink/elasticsearch.rs) | The webhook sink applies a full SSRF allow/deny check ([webhook.rs#L19-L84](../pg-tide-relay/src/sink/webhook.rs#L19)), but the ClickHouse, Arrow Flight, and Elasticsearch sinks accept arbitrary URLs from pipeline config with no validation. A relay running with these features enabled and a compromised catalog row could be steered at `http://169.254.169.254/...` (cloud metadata) or `http://localhost:6379`. | SSRF via crafted catalog entry; affects any deployment where the catalog is writable by a less-trusted role than the relay's network position. | Extract the webhook SSRF helper into a shared `relay::http::validate_url()` and call it from every HTTP-based sink. Add `ssrf_protection: bool` (default `true`) to all three sinks. |
| 2.2 | Medium | [pg-tide-relay/src/sink/inbox.rs#L56](../pg-tide-relay/src/sink/inbox.rs#L56), [pg-tide-relay/src/sink/pg_outbox.rs#L43](../pg-tide-relay/src/sink/pg_outbox.rs#L43) | The local-`InboxSink` and remote-`PgInboxSink` interpolate `inbox_table` into `format!("tide.\"{}\"", ...)` without calling `validate_relay_identifier()`. Double-quoting blocks ASCII `"` injection, but the validator exists ([config.rs#L206-L227](../pg-tide-relay/src/config.rs#L206)) and is applied elsewhere — defence-in-depth is missing here. | Defence-in-depth gap; only exploitable if catalog writes are not gated. | Call `validate_relay_identifier()` in the constructors of both sinks. |
| 2.3 | Low | [sql/pg_tide--0.1.0.sql#L293-L326](../sql/pg_tide--0.1.0.sql#L293) | The base-file `grant_publish()` / `revoke_publish()` are `SECURITY DEFINER` but lack `SET search_path = tide, pg_catalog`. They are redefined with `SET search_path` in [sql/pg_tide--0.12.0--0.13.0.sql#L150-L182](../sql/pg_tide--0.12.0--0.13.0.sql#L150). Because lib.rs does not run the 0.12.0 → 0.13.0 migration on a fresh install (§1.1), fresh installs at v0.16.0 still have the unhardened definitions until something else replaces them. | Search-path injection vector for a fresh-install superuser-installed extension. | Hardening must live in the base `0.1.0.sql` file (or fix §1.1). |
| 2.4 | Low | [pg-tide-relay/src/cli.rs](../pg-tide-relay/src/cli.rs) | `--postgres-url` is consumed as a CLI argument; on Linux this appears in `/proc/<pid>/cmdline`. There is no warning in `--help` and no encouragement to use `PG_TIDE_POSTGRES_URL` or `--postgres-url-file`. | Credentials visible to any local user via `ps` / `/proc`. | Add `--postgres-url-file <PATH>` and document `PG_TIDE_POSTGRES_URL` as the preferred form in `--help`. |
| 2.5 | Low | [pg_tide.control#L7](../pg_tide.control#L7) | `superuser = false` — anyone with `CREATE` on a database can install the extension and own its `SECURITY DEFINER` functions, then redefine them. Documented in overall_assessment_2 §2.5 but no change shipped. | Privilege escalation in shared/multi-tenant clusters. | Either flip to `superuser = true` for the 1.0 release or document that the extension owner role must be locked to a DBA. |

### 3. Code Quality & Maintainability

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 3.1 | Medium | [pg-tide-relay/src/coordinator.rs#L433-L940](../pg-tide-relay/src/coordinator.rs#L433) | `worker_inner()` is ~507 lines, mixing poll loop, schema check, transform, routing, rate-limit, circuit-breaker, DLQ, and metric paths. v0.16.0 release notes claimed "extract `worker_inner()` publish/DLQ logic into standalone helper functions" — only partial extraction shipped (publish path is still inline). | Hard to add features without regression; tests can only exercise the path end-to-end. | Extract three helpers: `process_batch()`, `publish_with_circuit_breaker()`, `route_to_dlq()`. Add unit tests for each. |
| 3.2 | Medium | (function/SQL duplication) | See §1.2 — duplicate plpgsql vs Rust definitions of `outbox_truncate_delivered`, `outbox_create_if_not_exists`, `relay_set_inbox_v2`. | Maintenance hazard: every behaviour change must be applied twice. | Single source of truth (Rust). |
| 3.3 | Low | [pg-tide-relay/src/main.rs](../pg-tide-relay/src/main.rs) (~1,196 lines) | `main.rs` still owns ~12 CLI subcommand implementations (`run_doctor`, `run_validate_config`, `run_sweep`, `run_status`, `run_replay_*`, `run_asyncapi_export`, …). | Single file is the second-largest in the relay and concentrates DB connection setup, CLI parsing, and business logic. | Split `cmd/doctor.rs`, `cmd/status.rs`, `cmd/sweep.rs`, etc. — each module ~80 lines. |
| 3.4 | Low | [pg-tide-relay/src/coordinator.rs#L32](../pg-tide-relay/src/coordinator.rs#L32) | `#[allow(dead_code)]` on `health: Arc<RwLock<HealthState>>`. The field exists for a future HTTP health endpoint but is never read. | Lint suppression hides a real "not yet wired" state. | Either implement the `/healthz` endpoint that reads it, or remove the field until that work is scheduled. |
| 3.5 | Low | [pg-tide-relay/src/coordinator.rs#L585-L592](../pg-tide-relay/src/coordinator.rs#L585) | Pseudo-random jitter uses `consecutive_failures * 6_364_136_223_846_793_005_u64` — a single LCG step seeded by the failure count. For identical failure counts across pipelines (e.g. a global outage), all workers will choose the same jitter offset and thunder. | Mitigates thundering-herd only weakly. | Use `rand::thread_rng().gen_range(-jitter_range..=jitter_range)` (the `rand` crate is already in the relay's dependency graph via `tokio-rustls`). |
| 3.6 | Low | [pg-tide-ext/src/](../pg-tide-ext/src/) | Five `unwrap_or(None).unwrap_or(...)` chains (see §1.4). The double-unwrap reads as deliberate, but pattern is repeated five times — a `try_exists_or_false()` helper would clarify intent. | Readability. | Extract a `fn spi_exists(sql, args) -> Result<bool, PgTideError>` helper. |
| 3.7 | Informational | repo-wide | Exactly 1 `TODO`/`FIXME`/`HACK`/`XXX` comment across both crates' src trees (per terminal grep). | Excellent comment hygiene. | None. |

### 4. API Ergonomics & Developer Experience

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 4.1 | Medium | [pg-tide-ext/src/relay.rs#L70-L81](../pg-tide-ext/src/relay.rs#L70) | `relay_set_inbox()` retains its 8-positional-parameter signature; `relay_set_inbox_v2()` exists as the JSONB-config replacement but `relay_set_outbox()` was *not* given the same treatment. Inconsistent API surface. | Users learn two patterns for two near-identical functions. | Add `relay_set_outbox_v2(JSONB)` and document v2 as canonical; mark v1 deprecated in 1.0 release notes. |
| 4.2 | Medium | [docs/src/getting-started/first-pipeline.md#L44-L46](../docs/src/getting-started/first-pipeline.md#L44), [operations/troubleshooting.md#L16](../docs/src/operations/troubleshooting.md#L16), [operations/deployment-guide.md#L129](../docs/src/operations/deployment-guide.md#L129) (×7), [relay-guide/configuration.md#L6-L24](../docs/src/relay-guide/configuration.md#L6) | ~20 doc references to `PGTRICKLE_RELAY_*` env vars survive from the pg_trickle origin. The correct prefix is `PG_TIDE_*` ([cli.rs#L24-L60](../pg-tide-relay/src/cli.rs#L24)). | A reader following getting-started copies env vars that the binary does not honour. | Repo-wide find-and-replace `PGTRICKLE_RELAY_` → `PG_TIDE_`; add a `lychee`-style smoke test that asserts no docs reference `PGTRICKLE_`. |
| 4.3 | Medium | [examples/cnpg/cluster.yaml#L70](../examples/cnpg/cluster.yaml#L70), [#L117](../examples/cnpg/cluster.yaml#L117) | Example pins the relay image at `0.1.0` and uses `PG_TIDE_RELAY_POSTGRES_URL` (incorrect name; correct is `PG_TIDE_POSTGRES_URL`). | The flagship example fails to start. | Bump image tag to `0.16.0` and rename the env var. |
| 4.4 | Low | [pg-tide-ext/src/relay.rs#L211-L236](../pg-tide-ext/src/relay.rs#L211) | `relay_enable()` / `relay_disable()` silent no-op (see §1.6). | UX paper cut. | See §1.6. |
| 4.5 | Low | [pg-tide-relay/src/cli.rs](../pg-tide-relay/src/cli.rs) | `--help` does not describe environment-variable fallbacks for every flag (clap supports `env = "PG_TIDE_…"` discovery but only some flags have it set). | Operators don't know what's overrideable. | Add `env = "PG_TIDE_…"` and `--help` text for every flag that supports env. |
| 4.6 | Informational | [pg-tide-relay/src/metrics.rs](../pg-tide-relay/src/metrics.rs) | All metrics use the `pg_tide_relay_` prefix; histogram buckets for `delivery_latency_seconds` (1ms → 30s) and `reconcile_duration_seconds` (1ms → 2.5s) are sensible. | None. | None. |

### 5. Performance & Scalability

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 5.1 | Medium | [pg-tide-relay/src/coordinator.rs](../pg-tide-relay/src/coordinator.rs) (worker connection) | Each pipeline worker holds one dedicated PG connection for life; the `deadpool` pool is only used by the coordinator. With the default `max_owned_pipelines = 50` that is still ~52 connections per relay process — fine, but the design assumes one relay per database. | Managed Postgres with 100-connection cap can host at most one relay plus a couple of app pods. | Document the per-relay connection cost prominently in `docs/src/operations/`. Consider an opt-in `worker_pool` mode that reuses connections across workers behind a tokio mutex for connection-constrained environments. |
| 5.2 | Medium | [pg-tide-relay/src/coordinator.rs#L1726-line-count](../pg-tide-relay/src/coordinator.rs) | The reconcile loop runs `reconcile()` every `discovery_interval_secs` (default 30 s) and re-queries `tide.relay_outbox_config` + `tide.relay_inbox_config` in full each time. There is no LISTEN/NOTIFY trigger to react to config changes faster than 30 s. The base SQL defines `relay_config_notify()` ([sql/pg_tide--0.1.0.sql#L140-L152](../sql/pg_tide--0.1.0.sql#L140)) but the relay does not subscribe — the trigger fires `pg_notify('tide_relay_config', …)` into the void. | Operators changing pipeline config wait up to 30 s for hot-reload; `relay_set_inbox_v2()` ([sql/pg_tide--0.15.0--0.16.0.sql#L114](../sql/pg_tide--0.15.0--0.16.0.sql#L114)) even calls `pg_notify` — nobody is listening. | Subscribe to the `tide_relay_config` channel in the coordinator and trigger reconcile on receipt; keep the 30 s timer as a safety net. |
| 5.3 | Medium | [pg-tide-relay/benches/throughput.rs](../pg-tide-relay/benches/throughput.rs) | Single benchmark file; no coverage of transform, routing, wire-format encode/decode, or DLQ insert hot paths. | Performance regressions in those paths are invisible to CI. | Add Criterion benches for each path; wire `cargo bench` into a nightly job. |
| 5.4 | Low | [pg-tide-relay/src/source/outbox.rs](../pg-tide-relay/src/source/outbox.rs) (poll query) | `SELECT … WHERE id > $1 ORDER BY id LIMIT $2` is correct, but does not use `SKIP LOCKED`. For a single-relay deployment this is fine; for multiple relays in the same `relay_group_id` competing for the same outbox, the advisory-lock ownership protocol gates this. | None today; would matter for multi-tenant fan-in (roadmap v1.1.0). | Note as future-work in ADR-002. |
| 5.5 | Low | [pg-tide-relay/src/source/outbox.rs](../pg-tide-relay/src/source/outbox.rs) (envelope cloning) | Carried over from overall_assessment_2 §4.4: `OutboxBatch::into_messages()` clones. | 2× allocation on large batches. | Switch to `into_iter()`. |

### 6. Test Coverage & Quality

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 6.1 | High | (missing) | No end-to-end test exercises SQL→relay→sink in a single process. `sql_api_test.rs` asserts JSON shape only; `round_trip_test.rs` simulates outbox→inbox in pure SQL without the relay binary. v0.16.0 release notes claim such a test "shipped" but the file does not exist. | Contract drift between SQL helpers and the relay coordinator is undetectable. | Add `tests/sql_to_sink_e2e.rs`: spawn coordinator task → `tide.relay_set_outbox(..., 'stdout', …)` → `tide.outbox_publish(...)` → assert via channel/temp-file. |
| 6.2 | Medium | (missing) | No fault-injection test for the DLQ-write-error path (see §1.3). | Critical reliability path unverified. | Mock DLQ table with `pg_event_trigger`-revoked INSERT permissions; assert the worker pauses instead of looping. |
| 6.3 | Medium | (missing) | No fuzz / property test for JMESPath transform evaluation, identifier validation, or routing-template rendering — only wire-format encode/decode round-trips are property-tested ([wire_format_proptest.rs](../pg-tide-relay/tests/wire_format_proptest.rs)). | Crafted catalog entries (transforms, routes) may panic at runtime. | Extend `proptest` coverage to `JmespathTransform::evaluate`, `validate_relay_identifier`, and `routing::apply_routing`. |
| 6.4 | Low | [pg-tide-relay/tests/migration_test.rs](../pg-tide-relay/tests/migration_test.rs) | Walks the success path 0.1.0 → 0.16.0, but does not assert that a fresh `CREATE EXTENSION pg_tide` at 0.16.0 produces the same schema as the upgrade chain. | Drift identified in §1.1 unobservable. | Add a `pg_dump --schema-only` diff assertion. |
| 6.5 | Low | (missing) | No test exercises a permanent error (bad credentials, missing schema) and asserts `pipeline_errors_total{error_class="permanent"}` increments and the pipeline pauses. | Transient/permanent classification (the headline v0.15.0 feature) is unverified at runtime. | Add `tests/error_classification_test.rs`. |
| 6.6 | Low | [pg-tide-relay/tests/](../pg-tide-relay/tests/) (55 files) | Tests use unique table names per test but no `#[serial]` attribute; CI relies on testcontainers isolation. Working today, but if anyone runs tests with `cargo test` (no `--test-threads=1`) without testcontainers, races appear. | Local-dev surprise; CI is fine. | Document in `AGENTS.md` that integration tests require `--test-threads=1` or testcontainers. |

### 7. Documentation Completeness & Accuracy

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 7.1 | Medium | docs (×20 refs) | Stale `PGTRICKLE_RELAY_*` env var references — see §4.2. | Users mis-configure on first try. | Bulk rename. |
| 7.2 | Medium | [examples/cnpg/cluster.yaml](../examples/cnpg/cluster.yaml) | Outdated image tag and env var — see §4.3. | Example does not deploy. | Refresh. |
| 7.3 | Low | [docs/src/concepts/](../docs/src/concepts/) | Claim-check is documented as a pg_tide feature with no caveat that it requires pg_trickle (see §1.5). | Confusion at integration time. | Add a "Requires pg_trickle" callout to the claim-check section. |
| 7.4 | Low | [docs/src/operations/](../docs/src/operations/) | No runbook for "relay crashed mid-batch" (recovery is automatic at-least-once but operators benefit from a documented expectation) or "DLQ flooded" (manual replay via `pg-tide replay dlq-requeue`). | Operators improvise during incidents. | Add `docs/src/operations/runbooks/` with crash-recovery, DLQ-replay, schema-migration runbooks. |
| 7.5 | Low | [README.md#L72-L76](../README.md#L72) | README claims "exactly-once delivery semantics at the application layer" via inbox dedup — accurate, but the qualifier "at the application layer" is easy to miss and at-least-once is the actual transport guarantee. | Marketing-vs-truth concern with regulated buyers. | Reword to "at-least-once transport with application-layer dedup yielding effective exactly-once". |
| 7.6 | Informational | [docs/adr/](../docs/adr/) | Five ADRs land in v0.16.0 (ADR-001 .. ADR-005). Strong addition. | None. | Continue: add ADR-006 once the §1.1 catalog drift is resolved. |

### 8. Packaging & Release Integrity

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 8.1 | Medium | [.github/workflows/release.yml](../.github/workflows/release.yml) (per overall_assessment_2 §9.2) | aarch64-linux build still excludes the `kafka` feature due to `rdkafka` cross-compile issues. | ARM users can't `cargo install` with Kafka support; the `:latest-full` Docker image still works because Docker builds natively per arch. | Document in release notes (overall_assessment_2 noted this; not addressed). |
| 8.2 | Low | [.github/workflows/](../.github/workflows/) | No SBOM (Syft) or Trivy image-vulnerability scan in CI. cosign signing is in place. | Compliance buyers (SOC2, FedRAMP) will ask for SBOM. | Add SBOM generation + Trivy scan to release workflow. |
| 8.3 | Low | release automation | Helm `version` / `appVersion` were aligned to 0.16.0 manually for this release. There is still no automation that bumps them as part of the version-bump step. | Future versions risk the v0.13.x drift recurring. | Add a `just bump-version VERSION` recipe that updates `Cargo.toml`, `pg_tide.control`, `helm/pg-tide/Chart.yaml` together. |
| 8.4 | Low | [Dockerfile](../Dockerfile) | The "full" Docker image (`:latest-full`) builds with `--all-features`; the slim image uses defaults. Both work, but neither contains a sample `pg-tide.toml` or `README` baked into `/etc/pg-tide/` — operators must consult external docs to find the relay config layout. | Convenience. | Bake a commented `/etc/pg-tide/pg-tide.example.toml` into both images. |
| 8.5 | Informational | [audit.toml](../audit.toml) | 9 RUSTSEC ignores, all in feature-gated optional deps (hickory-proto via mongodb, rustls-webpki via AWS/MQTT, paste via parquet). Default build is clean. Documented justifications present. | None. | Re-evaluate quarterly. |

### 9. Operational Readiness

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 9.1 | Medium | (operational gap) | DLQ insert failures are not surfaced as a pipeline-pausing condition (see §1.3). | Silent stuck pipelines. | See §1.3. |
| 9.2 | Medium | (operational gap) | `pg-tide doctor` checks connectivity and v0.15.0+ marker function presence, but does not validate: (a) `tide.relay_dlq` insert privileges; (b) advisory-lock acquisition under the configured `relay_group_id`; (c) `LISTEN` permission for `tide_relay_config`. | Failures discovered at first publish rather than during deployment validation. | Extend `doctor` with these checks. |
| 9.3 | Low | [pg-tide/dashboards/relay-health.json](../pg-tide/dashboards/relay-health.json) | Dashboard tracks the six "core" metrics; v0.16.0's three new coordinator metrics (`owned_pipelines`, `reconcile_duration_seconds`, `pipeline_errors_total`) are exported but not visualised. | Operators can't see the data the team added. | Add a "Coordinator" row to the dashboard with three new panels. |
| 9.4 | Low | (logging) | The worker emits `info!` on every successful poll completion at high cardinality (per-pipeline, per-batch). For 50 pipelines polling at 1 Hz that's 4.3 M log lines/day. | Log-volume cost. | Demote per-batch-success log line to `debug!`; keep `info!` for state transitions. |
| 9.5 | Informational | [coordinator.rs#L36](../pg-tide-relay/src/coordinator.rs#L36), [§9 v0.15.0 changes] | Worker panic detection via `JoinHandle::is_finished()` is in place. | Strong. | None. |

### 10. Architecture & Strategic Gaps

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 10.1 | Strategic | [sql/pg_tide--0.1.0.sql](../sql/pg_tide--0.1.0.sql) | Single-table outbox (`tide.tide_outbox_messages`) with `outbox_name` discriminator is documented in ADR-001 as a deliberate choice. At ~100 M rows the partial index `(outbox_name, id) WHERE consumed_at IS NULL` will degrade. | Caps single-database scale at moderate-high throughput. | Roadmap v1.0 calls for partitioning by time — pull that work into v1.0 explicitly and write the partition switchover runbook. |
| 10.2 | Strategic | [pg-tide-relay/src/](../pg-tide-relay/src/) | TOML pipeline-config support (`config.rs` `PipelineConfig::from_toml`) coexists with catalog-stored config. The coordinator prefers catalog values, but a TOML-only deployment (no catalog config rows) silently runs nothing. | Two configuration surfaces; risk of drift. | Document a single "canonical" config path; add a `pg-tide validate-config` mode that compares TOML against catalog and reports divergence. |
| 10.3 | Strategic | (claim-check status) | Claim-check support exists in `OutboxPollerSource` but assumes pg_trickle-managed `tide.outbox_delta_rows_*` tables. README advertises it as a pg_tide feature. | Misleading capability claim. | Either implement a native pg_tide claim-check pathway or explicitly mark the feature as "via pg_trickle integration" in README. |
| 10.4 | Strategic | (multi-tenant) | RLS policies on relay-config tables ([sql/pg_tide--0.13.0--0.14.0.sql#L231](../sql/pg_tide--0.13.0--0.14.0.sql#L231)) implement per-tenant config visibility, but the relay process still runs as a single high-privilege role — it sees every tenant. There is no per-tenant connection pool. | A compromised relay leaks all tenant data. | For roadmap v1.1.0 hardening, support per-tenant relay groups with per-tenant DB roles (one relay instance per tenant role) and document the deployment pattern. |
| 10.5 | Strategic | (competitive gap) | Debezium provides change-event sourcing from MySQL/Oracle/MongoDB; Sequin offers a managed UI; PGOutbox is a thin app library. pg_tide's unique strength is the transactional-outbox + relay catalog + multi-sink ergonomics, but it lacks: (a) WAL-based logical-replication source (roadmap v1.2); (b) a web UI (roadmap v1.3); (c) envelope encryption (roadmap v1.0). | Buyer comparison gaps. | Prioritise envelope encryption for v1.0 (smallest effort, biggest enterprise differentiator), then logical-replication source for v1.1. |

---

## Prioritised Action Plan

### Immediate (block v0.17.0 / v1.0 GA)

1. Fix catalog drift (§1.1): include every migration in `pg-tide-ext/src/lib.rs` `extension_sql_file!` chain and add a CI test that diffs fresh-install vs upgrade-chain schemas.
2. Deduplicate plpgsql vs Rust function definitions (§1.2) — pick Rust as the single source of truth.
3. Pause pipelines on DLQ write failure (§1.3); add `pg_tide_relay_dlq_write_errors_total` metric.
4. Land the missing SQL → relay → sink E2E test (§6.1).
5. Fix `examples/cnpg/cluster.yaml` (§4.3, §7.2).
6. Bulk-rename `PGTRICKLE_RELAY_*` references in docs to `PG_TIDE_*` (§4.2, §7.1).

### Short-term (next sprint)

7. Extract a shared SSRF validator and apply it to ClickHouse, Arrow Flight, Elasticsearch sinks (§2.1).
8. Add `validate_relay_identifier()` calls in `InboxSink` / `PgInboxSink` constructors (§2.2).
9. Subscribe to `tide_relay_config` LISTEN channel in coordinator for hot-reload (§5.2).
10. Add `relay_set_outbox_v2(JSONB)` matching `relay_set_inbox_v2()` (§4.1).
11. Extend `pg-tide doctor` with DLQ insert / advisory-lock / LISTEN-permission checks (§9.2).
12. Add fault-injection test for DLQ permission failures (§6.2).
13. Add property tests for JMESPath transform, identifier validation, routing (§6.3).
14. Replace pseudo-random jitter with `rand::thread_rng()` (§3.5).
15. Split `worker_inner()` into three helpers (§3.1) and split `main.rs` into `cmd/` modules (§3.3).
16. Convert `outbox_exists()` / `inbox_exists()` / `relay_exists()` to `Result<bool, _>` (§1.4).

### Medium-term (next 2-3 milestones)

17. Add SBOM + Trivy scanning to release workflow (§8.2); add `just bump-version` recipe (§8.3).
18. Document one canonical config path (catalog) and emit a warning when TOML pipeline configs are detected (§10.2).
19. Bake `/etc/pg-tide/pg-tide.example.toml` into Docker images (§8.4).
20. Add operations runbooks for crash recovery, DLQ replay, schema migration, relay upgrade (§7.4).
21. Implement outbox table partitioning by time (§10.1) — required for v1.0 scale claims.
22. Add envelope encryption + KMS integration (roadmap v1.0).
23. Begin logical-replication source prototype (roadmap v1.2 — accelerate to v1.1).
24. Visualise the three new v0.16.0 coordinator metrics in the Grafana dashboard (§9.3).

---

## Delta from Previous Assessments

### Fixed since overall_assessment_2.md (2026-05-06)

| Old finding | Status | Evidence |
|---|---|---|
| §1.1 / §2.1 — TLS not wired into 11 `tokio_postgres::connect()` sites | ✅ Fixed | All 11 sites now route through `pg_tls::connect()` ([main.rs](../pg-tide-relay/src/main.rs#L146), [coordinator.rs#L443](../pg-tide-relay/src/coordinator.rs#L443)). One remaining `NoTls` is the internal coordinator metadata pool ([main.rs#L1100](../pg-tide-relay/src/main.rs#L1100)) — justified, not a regression. |
| §4.1 — No connection pooling | ✅ Fixed | `deadpool-postgres` for coordinator metadata; `--max-connections` CLI flag. |
| §5.1 — No transient/permanent error classification | ✅ Fixed | `RelayError::is_transient()` ([error.rs#L114-L135](../pg-tide-relay/src/error.rs#L114)); used at [coordinator.rs#L568-L572](../pg-tide-relay/src/coordinator.rs#L568). |
| §5.2 — Worker JoinHandle not tracked | ✅ Fixed | `owned: HashMap<String, (watch::Sender, JoinHandle)>` ([coordinator.rs#L36](../pg-tide-relay/src/coordinator.rs#L36)); panic detection in reconcile. |
| §4.3 — No exponential backoff | ✅ Fixed | Backoff with jitter at [coordinator.rs#L576-L596](../pg-tide-relay/src/coordinator.rs#L576). |
| §1.3 — `max_owned_pipelines` not configurable | ✅ Fixed | `--max-pipelines` / `PG_TIDE_MAX_PIPELINES`. |
| §1.4 — Raw pg_tide payload (no `v:1`) handling | ✅ Fixed | `payload_mode = "raw"` source option. |
| §9.1 — Helm chart version drift | ✅ Fixed | Chart `version`/`appVersion` at 0.16.0; `securityContext` hardened. |
| §3.1 / §3.2 — Blanket `#![allow(dead_code, unused_imports)]` | ✅ Fixed | Replaced with targeted allows. |
| §5.3 — No coordinator metrics | ✅ Fixed | `owned_pipelines`, `reconcile_duration_seconds`, `pipeline_errors_total` shipped in v0.16.0. |
| §5.4 — Limited OTel span coverage | ✅ Fixed | Spans for transform, routing, dlq, schema-evolution, backoff added. |
| §6.2 — Migration upgrade test | ✅ Fixed | `migration_test.rs` walks 0.1.0 → 0.16.0. |
| §6.3 — Property-based wire-format tests | ✅ Fixed | `wire_format_proptest.rs`. |
| §8.4 — No ADRs | ✅ Fixed | ADR-001 through ADR-005 in `docs/adr/`. |
| §9.4 — Slim/full Docker image | ✅ Fixed | `:latest` + `:latest-full` published. |
| §9.6 — No cosign | ✅ Fixed | Keyless cosign on images + artefacts. |
| §1.7 / §4.4 — `OutboxBatch::into_messages()` clone | ⚠️ Still open | Not addressed in v0.15/0.16. Re-listed as §1.7 here. |
| §6.1 — End-to-end SQL → relay → sink test | ⚠️ Claimed shipped but NOT present | v0.16.0 release notes claim this, the file does not exist. Re-listed as §6.1 here. |

### Regressions or partial fixes

- **None observed** at the code level. All v0.15/0.16 features traced cleanly to their claimed locations.

### New findings (introduced or newly noticed in this audit)

- §1.1 — Fresh-install vs upgrade catalog drift (caused by partial `extension_sql_file!` chain).
- §1.2 — Duplicate plpgsql ⇄ Rust function definitions in 0.14.0 → 0.16.0 migrations.
- §1.3 — DLQ write-failure swallowed (was masked behind the v0.13.0 DLQ implementation; only surfaces on rare permission/disk errors).
- §2.1 — ClickHouse / Arrow Flight / Elasticsearch sinks accept unvalidated URLs (existed since v0.10.0 / v0.8.0 but not flagged in earlier audits).
- §5.2 — Coordinator does not subscribe to `tide_relay_config` LISTEN despite the trigger publishing on it.
- §6.1 — Promised E2E test from v0.16.0 release notes is absent.
- §10.2 — TOML-vs-catalog config dualism remains undocumented.

---

## Appendix: Metrics Snapshot

| Metric | Value |
|---|---|
| Total Rust source files (`pg-tide-ext/src` + `pg-tide-relay/src`) | ≈ 78 |
| Approximate lines of Rust (excluding tests) | 22,431 |
| Approximate lines of SQL (all `sql/*.sql`) | 1,510 |
| SQL upgrade scripts in `sql/` | 15 (`0.1.0--0.2.0` … `0.15.0--0.16.0`) |
| Migration files actually loaded by `extension_sql_file!()` | 2 (`0.1.0.sql`, `0.13.0--0.14.0.sql`) — §1.1 |
| Sink backends (files in `pg-tide-relay/src/sink/` minus `mod.rs`) | 29 |
| Source backends (files in `pg-tide-relay/src/source/` minus `mod.rs`) | 15 |
| Wire format implementations | 6 (`native`, `debezium`, `cloudevents`, `maxwell`, `canal`, `cdc_json`) |
| Integration test files in `pg-tide-relay/tests/` | 55 |
| Open `TODO` / `FIXME` / `HACK` / `XXX` comments in source | 1 |
| `pg-tide` CLI subcommands | 8 (`run` (default), `doctor`, `validate-config`, `replay`, `asyncapi`, `sweep`, `status`, + replay/asyncapi sub-subcommands) |
| Prometheus metrics exported by relay | 11 |
| OTel spans emitted by relay | 7 |
| Helm chart `appVersion` / `pg_tide.control` `default_version` / workspace `Cargo.toml` | 0.16.0 / 0.16.0 / 0.16.0 (aligned) |
| `audit.toml` ignored advisories | 9 (all in feature-gated optional deps; default build clean) |
| ADRs published | 5 (`docs/adr/adr-001` … `adr-005`) |

— End of report —
