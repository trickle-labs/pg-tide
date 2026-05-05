# pg_tide Overall Assessment — 2026-05-05

## Executive Summary

pg_tide has a strong product shape: the extension exposes the right primitives for a PostgreSQL-native outbox/inbox, and the relay codebase already sketches a broad ecosystem story covering Kafka, NATS, cloud queues, object storage, analytics sinks, connector protocols, DLQ, rate limiting, circuit breaking, and wire formats. The documentation tree is ambitious and the test suite is larger than most early extension projects. The main concern is not lack of vision; it is that several important seams between the SQL extension, relay catalog, runtime configuration, and packaged artifacts do not yet line up.

The top three risks are operationally serious. First, the SQL API for relay configuration writes a JSON shape that the relay runtime does not understand, so pipelines configured through `tide.relay_set_outbox()` / `tide.relay_set_inbox()` can be stored successfully but fail at runtime. Second, the relay source/sink code has schema mismatches against the canonical extension schema: simple relay offsets use `last_change_id`/`worker_id` while the SQL table has `last_offset`, and the pg-inbox sink inserts `event_type` even though extension-created inbox tables contain `source` and `headers`. Third, the release and deployment path is misleading: the Docker and GitHub release builds compile only default relay features, while the docs say prebuilt binaries include all feature gates; the Helm chart also sets the wrong environment variable for the database URL.

Security posture is mixed. The code consistently uses parameter binding for values, which is good, but dynamic identifiers are interpolated directly in multiple extension and relay SQL strings. PostgreSQL identifiers cannot be bound as `$1`, so the correct fix is a shared strict identifier validator plus quoting helper. PostgreSQL connections are all opened with `NoTls`, including the coordinator, notification connection, worker connections, and remote PostgreSQL sink. That means documented `sslmode=require`-style production guidance will not actually protect the control plane.

The strongest opportunities are clear: make the extension catalog and relay config contract a single source of truth, add a small set of end-to-end tests that exercise the public SQL API all the way through an actual relay worker, and turn the existing broad backend matrix into honest, feature-gated packaging. Once those are aligned, pg_tide can credibly differentiate by being the simplest PostgreSQL-native outbox/inbox with first-class relay operations, replay, schema-aware CDC formats, and production observability.

## Findings by Area

## 1. Correctness & Bugs

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 1 | Critical | [pg-tide-ext/src/relay.rs](../pg-tide-ext/src/relay.rs#L53-L59), [pg-tide-relay/src/coordinator.rs](../pg-tide-relay/src/coordinator.rs#L581-L592), [pg-tide-relay/src/coordinator.rs](../pg-tide-relay/src/coordinator.rs#L910-L913) | `relay_set_outbox()` stores `{"outbox", "sink", "batch_size", "params"}`, while the relay requires top-level `source_type`, nested `source.outbox`, `sink_type`, and nested `sink.*`. `relay_set_inbox()` has the same mismatch. | Public SQL configuration succeeds but the relay rejects the pipeline as missing required config keys. This breaks the advertised workflow. | Make SQL helper functions emit the runtime schema or make the runtime accept the SQL helper schema. Prefer a versioned catalog contract and tests that call SQL helpers then start a worker. |
| 2 | Critical | [sql/pg_tide--0.1.0.sql](../sql/pg_tide--0.1.0.sql#L131-L138), [pg-tide-relay/src/source/outbox.rs](../pg-tide-relay/src/source/outbox.rs#L390-L422) | `tide.relay_consumer_offsets` defines `last_offset TEXT`, but the relay reads and writes `last_change_id BIGINT` and `worker_id`. | Simple outbox polling will fail on first offset read/write against the canonical schema. | Migrate the table to `last_change_id BIGINT NOT NULL DEFAULT 0, worker_id TEXT` or update relay code to use `last_offset` consistently with explicit parsing. |
| 3 | Critical | [pg-tide-ext/src/inbox.rs](../pg-tide-ext/src/inbox.rs#L71-L83), [pg-tide-relay/src/sink/inbox.rs](../pg-tide-relay/src/sink/inbox.rs#L36-L52), [pg-tide-relay/src/sink/pg_outbox.rs](../pg-tide-relay/src/sink/pg_outbox.rs#L49-L60) | Extension-created inbox tables have `event_id`, `source`, `payload`, `headers`; relay pg-inbox sinks insert `event_id`, `event_type`, `payload`, `received_at`. | Reverse delivery to pg_tide inboxes fails at runtime with “column event_type does not exist”. | Change sinks to insert `(event_id, source, payload, headers)` and map `msg.subject` into `source` or `headers->event_type`; add an integration test that uses `InboxSink` directly. |
| 4 | High | [pg-tide-ext/src/outbox.rs](../pg-tide-ext/src/outbox.rs#L91-L120), [pg-tide-ext/src/outbox.rs](../pg-tide-ext/src/outbox.rs#L221-L237), [docs/src/sql-reference/outbox-api.md](../docs/src/sql-reference/outbox-api.md#L115-L118) | `outbox_disable()` sets `enabled = false`, but `outbox_publish()` checks only existence and still inserts. Docs say disabled outboxes reject publishes. | Operators cannot pause publishing; maintenance controls are ineffective. | Change the existence check to fetch `enabled` and return `InvalidArgument("outbox is disabled")` before insert. |
| 5 | High | [pg-tide-relay/src/source/outbox.rs](../pg-tide-relay/src/source/outbox.rs#L45-L49), [pg-tide-relay/src/source/outbox.rs](../pg-tide-relay/src/source/outbox.rs#L352-L360) | Relay calls `tide.outbox_rows_consumed()` and `tide.poll_outbox()`, but the extension source and canonical SQL examined do not define them. | Claim-check and consumer-group relay modes fail against a fresh install. | Add the missing SQL/pgrx functions or remove those modes until the extension exposes them. Gate with pgrx tests and relay integration tests. |
| 6 | High | [pg-tide-relay/src/wire_format/mod.rs](../pg-tide-relay/src/wire_format/mod.rs#L336-L368), [pg-tide-relay/src/coordinator.rs](../pg-tide-relay/src/coordinator.rs#L581-L592) | The v0.11 wire-format factory exists, but coordinator/source/sink factories do not call it. | Debezium/Maxwell/Canal/CDC JSON functionality is library-local and test-local, not active in real relay pipelines. | Introduce `RawMessage`/`EncodedBatch` adapters in transport sources/sinks and invoke `wire_format::from_config()` per pipeline. |
| 7 | Medium | [pg-tide-ext/src/relay.rs](../pg-tide-ext/src/relay.rs#L238-L279) | `relay_list_configs()` selects `config` but omits it from the returned JSON and suppresses SPI errors with `unwrap_or_default()`. | Monitoring tools need N+1 calls and may silently see empty data on catalog errors. | Return `{name, direction, enabled, config}` and propagate SPI errors through a `Result` boundary. |

Minimal fix sketch for the relay config contract:

```rust
let full_config = serde_json::json!({
    "source_type": "outbox",
    "source": { "outbox": outbox },
    "sink_type": sink,
    "sink": config.0,
    "batch_size": batch_size
});
```

## 2. Security

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 1 | Critical | [pg-tide-ext/src/inbox.rs](../pg-tide-ext/src/inbox.rs#L71-L86), [pg-tide-ext/src/inbox.rs](../pg-tide-ext/src/inbox.rs#L149-L155), [pg-tide-ext/src/inbox.rs](../pg-tide-ext/src/inbox.rs#L180-L187), [pg-tide-ext/src/inbox.rs](../pg-tide-ext/src/inbox.rs#L267-L275) | Schema and inbox names are interpolated into SQL identifiers without validation or escaping. Double quotes are not enough if the input contains quotes. | A caller with execute rights can attempt SQL injection or create unusable identifiers. | Add a shared `validate_identifier()` for schema/name/table suffixes and use a quoting helper. Reject empty names, names over 63 bytes, and non `[A-Za-z_][A-Za-z0-9_]*`. |
| 2 | High | [pg-tide-relay/src/source/outbox.rs](../pg-tide-relay/src/source/outbox.rs#L82-L90), [pg-tide-relay/src/source/outbox.rs](../pg-tide-relay/src/source/outbox.rs#L316-L324), [pg-tide-relay/src/sink/inbox.rs](../pg-tide-relay/src/sink/inbox.rs#L39-L50) | Relay table names are built from pipeline config via `format!()`. | A compromised catalog row can steer the relay to arbitrary relations or produce SQL syntax injection. | Validate all configured table identifiers before worker start; never build `tide.{name}` from unchecked JSON. |
| 3 | High | [pg-tide-relay/src/main.rs](../pg-tide-relay/src/main.rs#L96-L105), [pg-tide-relay/src/main.rs](../pg-tide-relay/src/main.rs#L272-L276), [pg-tide-relay/src/coordinator.rs](../pg-tide-relay/src/coordinator.rs#L303-L310), [pg-tide-relay/src/sink/pg_outbox.rs](../pg-tide-relay/src/sink/pg_outbox.rs#L16-L24) | All PostgreSQL connections use `tokio_postgres::NoTls`. | Passwords, catalog configs, offsets, and payloads can traverse the network without TLS despite production docs recommending TLS. | Support rustls/native TLS for PostgreSQL, honor `sslmode=require`, and fail closed when TLS is required but unavailable. |
| 4 | High | [sql/pg_tide--0.1.0.sql](../sql/pg_tide--0.1.0.sql#L216-L255), [sql/pg_tide--0.1.0.sql](../sql/pg_tide--0.1.0.sql#L272-L278) | RLS is applied only to config tables; `grant_publish()` grants table-wide INSERT on `tide_outbox_messages`, not per-outbox publishing. | A role granted publish access to one outbox can insert rows for any outbox unless additional constraints are imposed externally. | Add publisher ACL tables and enforce outbox-level authorization in `outbox_publish()`. Revoke direct table writes from application roles. |
| 5 | Medium | [pg-tide-relay/src/sink/webhook.rs](../pg-tide-relay/src/sink/webhook.rs#L17-L27), [pg-tide-relay/src/sink/webhook.rs](../pg-tide-relay/src/sink/webhook.rs#L50-L66) | Webhook URLs are accepted from catalog config with no scheme, host, or private-network policy. | A catalog writer can make the relay perform SSRF to metadata services or internal admin endpoints. | Add `https_only`, allow/deny CIDR lists, and DNS/IP checks; reject link-local and loopback targets by default outside explicit dev mode. |
| 6 | Medium | [sql/pg_tide--0.1.0.sql](../sql/pg_tide--0.1.0.sql#L259-L306) | `SECURITY DEFINER` functions are created before the audit table they write to; they also do not set a hardened `search_path`. | Search-path surprises and future edits can weaken auditability. | Create audit table first and add `SET search_path = tide, pg_catalog` to definer functions. |
| 7 | Low | [Cargo.lock](../Cargo.lock) | `cargo audit` was not available in this environment and no `deny.toml`, `audit.toml`, Dependabot, or Renovate config was found. | Known-vulnerability checks are not repeatable from the repo alone. | Add `cargo-deny` or `cargo audit` to CI and configure dependency update automation. |

Identifier validation sketch:

```rust
fn validate_identifier(name: &str) -> Result<(), PgTideError> {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c == '_' || c.is_ascii_alphabetic() => {}
        _ => return Err(PgTideError::InvalidArgument("invalid identifier".into())),
    }
    if name.len() > 63 || !chars.all(|c| c == '_' || c.is_ascii_alphanumeric()) {
        return Err(PgTideError::InvalidArgument("invalid identifier".into()));
    }
    Ok(())
}
```

## 3. Code Quality & Maintainability

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 1 | High | [pg-tide-ext/src/outbox.rs](../pg-tide-ext/src/outbox.rs#L24-L30), [pg-tide-ext/src/inbox.rs](../pg-tide-ext/src/inbox.rs#L109-L117), [pg-tide-ext/src/relay.rs](../pg-tide-ext/src/relay.rs#L217-L235) | Many SPI errors are collapsed to `None`, false, defaults, or ignored `_ =`. | Real database/catalog failures are misreported as “not found”, successful no-ops, or empty lists. | Replace helper functions with `Result<T, PgTideError>` and propagate at the SQL boundary. |
| 2 | Medium | [pg-tide-relay/src/sink/object_storage.rs](../pg-tide-relay/src/sink/object_storage.rs#L278-L350), [pg-tide-relay/src/sink/ducklake.rs](../pg-tide-relay/src/sink/ducklake.rs#L197-L267) | Non-test code uses `unwrap()` in Parquet column-writing paths. | Schema drift or library behavior changes can panic the relay process. | Replace with explicit `ok_or_else` / `map_err` carrying sink context. |
| 3 | Medium | [pg-tide-relay/src/main.rs](../pg-tide-relay/src/main.rs#L229-L233) | Signal handler setup uses `expect()` in runtime code. | Platform or runtime errors can panic rather than returning a controlled startup error. | Return `RelayError` from signal setup and log through tracing. |
| 4 | Medium | [pg-tide-relay/src/main.rs](../pg-tide-relay/src/main.rs#L1-L4) | The binary suppresses `dead_code` and `unused_imports` globally. | Feature-gate churn and stale module imports are hidden from CI. | Move suppressions to narrow modules or remove them after cleaning imports. |
| 5 | Low | [pg-tide-relay/src/main.rs](../pg-tide-relay/src/main.rs#L66-L70) | Startup error uses `eprintln!`, contrary to project logging conventions. | Minor inconsistency, harder structured log ingestion. | Return a CLI error through `clap` or log with `tracing::error!`. |
| 6 | Low | [pg-tide-ext/src/relay.rs](../pg-tide-ext/src/relay.rs#L82-L107) | `#[allow(clippy::too_many_arguments)]` is used on public relay APIs without a wrapper type. | API surface will keep growing argument lists. | Use a JSONB config object or composite type for advanced options. |

## 4. Performance & Scalability

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 1 | High | [pg-tide-relay/src/sink/inbox.rs](../pg-tide-relay/src/sink/inbox.rs#L41-L54) | Inbox sink loops one `INSERT` per message. | High round-trip overhead and poor reverse-ingest throughput. | Use multi-row `INSERT`, `UNNEST`, or `COPY` with `ON CONFLICT DO NOTHING`. |
| 2 | Medium | [pg-tide-relay/src/coordinator.rs](../pg-tide-relay/src/coordinator.rs#L303-L310) | Each worker opens an independent PostgreSQL connection with no pool or cap. | Many pipelines can exhaust database connection limits. | Add a configurable pool (`deadpool-postgres` or equivalent) and a max-owned-pipelines limit. |
| 3 | Medium | [pg-tide-relay/src/sink/object_storage.rs](../pg-tide-relay/src/sink/object_storage.rs#L64-L74), [pg-tide-relay/src/sink/object_storage.rs](../pg-tide-relay/src/sink/object_storage.rs#L394-L403) | Object-storage buffering is in memory; byte accounting uses a rough constant and ignores actual payload size. | Large payloads can exceed intended memory limits before flush. | Track serialized byte length, enforce hard max message size, and apply backpressure before cloning into the buffer. |
| 4 | Medium | [pg-tide-relay/src/config.rs](../pg-tide-relay/src/config.rs#L29-L35), [pg-tide-relay/src/config.rs](../pg-tide-relay/src/config.rs#L44-L48) | `sink_max_inflight` is documented/configured but not used by the coordinator. | Users believe they have backpressure controls that are not enforced. | Wire the setting into a semaphore around publish work or remove until implemented. |
| 5 | Low | [pg-tide-relay/benches/throughput.rs](../pg-tide-relay/benches/throughput.rs#L1-L20) | Benchmarks cover serialization and local construction, not real DB polling, batching, ack, or sink IO. | Capacity planning lacks hot-path evidence. | Add benchmarks for outbox poll + decode, inbox batch insert, object-storage flush, and representative sink publish paths. |

## 5. Reliability & Observability

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 1 | High | [pg-tide-relay/src/coordinator.rs](../pg-tide-relay/src/coordinator.rs#L447-L477), [pg-tide-relay/src/dlq.rs](../pg-tide-relay/src/dlq.rs#L76-L84) | When the circuit is open and DLQ is enabled, the batch is written to DLQ and then the loop continues without acknowledging the source. `insert_batch()` also aborts on the first insert error despite comments saying otherwise. | The same poison batch can be re-polled and re-DLQ'd indefinitely; transient DLQ failures can hide partial writes. | Decide DLQ semantics explicitly: after durable DLQ write, ack/commit the source; add idempotent unique keys for DLQ entries and make batch insertion report partial failures. |
| 2 | High | [pg-tide-relay/src/metrics.rs](../pg-tide-relay/src/metrics.rs#L10-L19), [pg-tide-relay/src/coordinator.rs](../pg-tide-relay/src/coordinator.rs#L497-L516) | Metrics are registered for consumed count, health, lag, latency, and dedup, but the worker updates only published and publish errors. | Dashboards and alerts give a falsely healthy picture and cannot calculate SLOs. | Increment consumed after poll, observe latency after ack, set health/circuit gauges, and expose DLQ counters. |
| 3 | High | [pg-tide/dashboards/relay-health.json](../pg-tide/dashboards/relay-health.json#L87), [pg-tide-relay/src/metrics.rs](../pg-tide-relay/src/metrics.rs#L27-L79) | Dashboard uses `pgtide_relay_*` names such as `pgtide_relay_messages_forwarded_total`; code emits `pg_tide_relay_*` names such as `pg_tide_relay_messages_published_total`. | Grafana panels return no data. | Regenerate the dashboard from metric constants or add dashboard validation in CI. |
| 4 | Medium | [pg-tide-relay/src/metrics.rs](../pg-tide-relay/src/metrics.rs#L111-L119), [pg-tide-relay/src/coordinator.rs](../pg-tide-relay/src/coordinator.rs#L25-L41) | Health state is passed into the coordinator but not updated by workers. | `/health` can remain healthy even when pipelines are failing. | Update `HealthState` on worker start/stop/error and optionally run sink `is_healthy()` checks. |
| 5 | Medium | [pg-tide-relay/src/otel.rs](../pg-tide-relay/src/otel.rs#L1-L18), [pg-tide-relay/src/coordinator.rs](../pg-tide-relay/src/coordinator.rs#L368-L501) | OpenTelemetry span names are defined, but the main worker path is not instrumented with those spans. | Trace docs overstate actual span coverage. | Add spans around poll, transform, publish, ack, DLQ, and replay filtering. |
| 6 | Low | [pg-tide-relay/src/source/outbox.rs](../pg-tide-relay/src/source/outbox.rs#L184-L192) | Worker ID silently falls back to `relay:<pid>` if `HOSTNAME` is absent. | Debugging multi-instance ownership becomes harder. | Log a warning or use a generated instance ID surfaced in metrics. |

## 6. Test Coverage

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 1 | Critical | [pg-tide-relay/tests/common/mod.rs](../pg-tide-relay/tests/common/mod.rs#L12-L48), [pg-tide-relay/tests/inbox_sink_test.rs](../pg-tide-relay/tests/inbox_sink_test.rs#L6-L18) | Integration tests load the 0.1.0 SQL and use helper SQL to deliver to inboxes; they do not instantiate `InboxSink`, so the `event_type` schema mismatch is not caught. | CI can pass while reverse delivery is broken. | Add end-to-end tests that configure via SQL API and run the actual sink/source implementations. |
| 2 | High | [pg-tide-relay/tests/common/mod.rs](../pg-tide-relay/tests/common/mod.rs#L12-L48), [sql/pg_tide--0.1.0--0.2.0.sql](../sql/pg_tide--0.1.0--0.2.0.sql#L1-L6) | No upgrade-path tests apply the 11 migration scripts sequentially. | Migration drift and missing table/column changes are easy to miss. | Add a migration test that installs 0.1.0, applies every upgrade, then runs catalog assertions. |
| 3 | High | [pg-tide-relay/tests/outbox_source_test.rs](../pg-tide-relay/tests/outbox_source_test.rs#L6-L18) | Outbox source tests inspect tables directly instead of constructing `OutboxPollerSource`; missing `poll_outbox()` and offset-column mismatches are not caught. | Core relay source can be broken with green tests. | Test `OutboxPollerSource::new_simple().poll().acknowledge()` and consumer-group mode against the schema. |
| 4 | Medium | [pg-tide-relay/src/sink/inbox.rs](../pg-tide-relay/src/sink/inbox.rs), [pg-tide-relay/src/sink/pg_outbox.rs](../pg-tide-relay/src/sink/pg_outbox.rs), [pg-tide-relay/src/sink/stdout.rs](../pg-tide-relay/src/sink/stdout.rs) | Sink modules `inbox`, `pg_outbox`, and `stdout` do not have matching integration test files; source modules `outbox` and `stdin` are similarly not exercised by direct integration tests. | Unit-only coverage misses DB contract issues. | Add focused integration tests for these modules or explicitly document unit-only status. |
| 5 | Medium | [pg-tide-relay/tests/wire_format_test.rs](../pg-tide-relay/tests/wire_format_test.rs#L1-L18) | Wire-format tests verify trait behavior but not coordinator integration. | v0.11 features can work in isolation while remaining unreachable in production. | Add transport-level tests with `wire_format` config through a real pipeline. |
| 6 | Low | [pg-tide-relay/benches/throughput.rs](../pg-tide-relay/benches/throughput.rs#L32-L126) | No fuzz/property tests for CDC JSON path parsing, schema evolution, identifier validation, or dynamic subject routing. | Edge cases can regress silently. | Add proptest/fuzz targets for parsers and template/render paths. |

## 7. API & Schema Design

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 1 | Critical | [pg-tide-ext/src/relay.rs](../pg-tide-ext/src/relay.rs#L31-L70), [docs/src/sinks/nats.md](../docs/src/sinks/nats.md#L35-L62) | SQL API takes `p_sink` plus arbitrary JSON, while docs place `sink_type` inside JSON and the runtime expects top-level runtime keys. | Users cannot reliably know which config shape is authoritative. | Publish a formal JSON schema for pipeline configs and make SQL helpers validate against it. |
| 2 | High | [sql/pg_tide--0.1.0.sql](../sql/pg_tide--0.1.0.sql#L26-L36) | Outbox messages have no immutable dedup/event key or status enum; `consumed_at` is global, while consumer groups separately track offsets. | Fan-out and replay semantics are ambiguous across multiple relays/consumers. | Keep message rows immutable; use per-consumer offsets/acks and explicit delivery state tables. |
| 3 | High | [pg-tide-ext/src/outbox.rs](../pg-tide-ext/src/outbox.rs#L386-L404) | `commit_offset()` allows arbitrary offset movement without validating group/outbox existence or monotonicity. | A buggy consumer can rewind or skip messages without guardrails. | Add `WHERE committed_offset <= EXCLUDED.committed_offset` unless an explicit admin rewind API is used. |
| 4 | Medium | [sql/pg_tide--0.1.0.sql](../sql/pg_tide--0.1.0.sql#L132-L138) | `last_offset TEXT` is too generic for a PostgreSQL outbox offset and inconsistent with relay code. | Type ambiguity creates migration and parsing risk. | Use typed columns per source kind or JSONB offsets with a source discriminator. |
| 5 | Medium | [pg-tide-ext/src/inbox.rs](../pg-tide-ext/src/inbox.rs#L198-L236) | `inbox_status(NULL)` returns an empty array instead of summarizing configured inboxes. | Fleet monitoring via SQL API is incomplete. | Iterate inbox configs and aggregate counts safely with validated identifiers. |
| 6 | Low | [pg-tide-relay/src/cli.rs](../pg-tide-relay/src/cli.rs#L16-L62) | CLI has only run-time flags; no `validate-config`, `list-pipelines`, `doctor`, or `dry-run` subcommands. | Operators must discover errors by starting the daemon. | Add read-only diagnostics subcommands backed by the same config parser. |

## 8. Documentation

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 1 | High | [README.md](../README.md#L10-L18) | README advertises exactly-once relay semantics and broad backend support, but current runtime has at-least-once behavior and packaged defaults include only NATS/webhook/stdout. | Users may overtrust delivery guarantees and packaged capabilities. | Rephrase to “transactional publish + idempotent delivery primitives” and document feature-gated builds. |
| 2 | High | [docs/src/reference/version-compatibility.md](../docs/src/reference/version-compatibility.md#L7-L12), [pg-tide-ext/Cargo.toml](../pg-tide-ext/Cargo.toml#L13-L16) | Docs claim PostgreSQL 14–18 compatibility, while the extension crate exposes only the `pg18` pgrx feature. | Users on older PostgreSQL versions may attempt unsupported installs. | Align docs to PostgreSQL 18+ or add/test pg14–pg17 pgrx features. |
| 3 | High | [docs/src/reference/version-compatibility.md](../docs/src/reference/version-compatibility.md#L52-L68) | Feature availability table assigns features to versions that conflict with the changelog and roadmap, e.g. wire formats listed as 0.8.0 while roadmap says 0.11.0. | Release history is unreliable. | Generate compatibility tables from a single manifest or audit them during release. |
| 4 | Medium | [README.md](../README.md#L73-L80) | Getting Started link points to `getting-started/quickstart.html`, but the docs tree has `getting-started/first-pipeline.md` and `tutorials/getting-started.md`. | Broken landing path for new users. | Update the link and add a link checker to docs CI. |
| 5 | Medium | [docs/src/reference/version-compatibility.md](../docs/src/reference/version-compatibility.md#L72-L84), [pg-tide-relay/Cargo.toml](../pg-tide-relay/Cargo.toml#L16-L60) | Docs refer to `cloud` and `analytics` feature gates that do not exist; Cargo uses per-backend features. | Users cannot reproduce documented builds. | Either add aggregate features or update docs to list actual features. |
| 6 | Medium | [examples/cnpg/cluster.yaml](../examples/cnpg/cluster.yaml#L11-L15), [examples/cnpg/cluster.yaml](../examples/cnpg/cluster.yaml#L68-L70) | CNPG example references `pg_tide--0.1.0.sql` and relay image `0.1.0`. | Copy/paste deployments use stale schema/image versions. | Use current version placeholders or document how to substitute the release version. |

## 9. DevOps, CI/CD & Packaging

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 1 | Critical | [helm/pg-tide/templates/deployment.yaml](../helm/pg-tide/templates/deployment.yaml#L43-L48), [pg-tide-relay/src/cli.rs](../pg-tide-relay/src/cli.rs#L16-L20) | Helm sets `PG_TIDE_RELAY_POSTGRES_URL`; CLI reads `PG_TIDE_POSTGRES_URL`. | Helm deployments start without a database URL unless users also pass `--postgres-url` or config. | Rename the environment variable in the chart and add a helm template/unit test. |
| 2 | High | [pg-tide-relay/Cargo.toml](../pg-tide-relay/Cargo.toml#L16-L60), [.github/workflows/release.yml](../.github/workflows/release.yml#L48-L57), [Dockerfile](../Dockerfile#L25-L30) | Release and Docker builds do not pass `--all-features`; only default features (`nats`, `webhook`, `stdout`) are compiled. Docs claim prebuilt binaries include all features. | Most documented sinks/sources are missing from official artifacts. | Build either `--all-features` artifacts or publish clearly named slim/full variants. |
| 3 | High | [helm/pg-tide/Chart.yaml](../helm/pg-tide/Chart.yaml#L1-L9), [Cargo.toml](../Cargo.toml#L4-L7) | Helm chart version/appVersion are `0.1.0` while workspace/control version is `0.11.0`. | Helm users install stale-looking releases and image tags. | Bump chart metadata during release automation. |
| 4 | Medium | [.github/workflows/release.yml](../.github/workflows/release.yml#L12-L57) | Release artifacts package only the relay binary, not the extension `.so`, control file, or SQL files. | Extension installation from releases is incomplete. | Add pgrx extension packaging for supported PostgreSQL versions. |
| 5 | Medium | [.github/workflows/release.yml](../.github/workflows/release.yml#L146-L170) | Docker publishing has no SBOM, provenance signing, or image vulnerability scan. | Supply-chain assurance is weak for production users. | Add Syft/Trivy and cosign keyless signing. |
| 6 | Medium | [justfile](../justfile#L12-L16) | `just lint` runs relay clippy and format check but not extension clippy, despite project guidance saying lint should cover clippy with zero warnings. | Local workflow can miss extension warnings that CI catches later. | Add a `lint-ext` recipe or make `just lint` detect pgrx setup and run extension clippy when available. |

## 10. Dependency Health

| # | Severity | Location | Description | Impact | Recommendation |
|---|----------|----------|-------------|--------|----------------|
| 1 | High | [pg-tide-relay/Cargo.toml](../pg-tide-relay/Cargo.toml#L81-L125) | The relay carries a large optional dependency graph, including cloud SDKs, Arrow/Parquet, MongoDB, object_store, and OpenTelemetry. | Build times, CVE surface, and transitive dependency churn are high. | Keep default builds slim, publish full builds separately, and run dependency policy checks by feature group. |
| 2 | Medium | [pg-tide-relay/Cargo.toml](../pg-tide-relay/Cargo.toml#L47-L48) | `singer` and `airbyte` feature gates do not enable dependencies; they spawn arbitrary configured commands/images. | Behavior depends on external runtime tools not captured by Cargo metadata. | Document runtime prerequisites in config validation and surface them in `pg-tide doctor`. |
| 3 | Medium | [pg-tide-ext/Cargo.toml](../pg-tide-ext/Cargo.toml#L13-L20) | pgrx is pinned to `=0.18.0`, which improves reproducibility but requires planned update checks. | PostgreSQL/pgrx compatibility issues can linger until manually discovered. | Track pgrx releases and add a scheduled compatibility CI job. |
| 4 | Medium | [Cargo.lock](../Cargo.lock) | Vulnerability audit tooling was not available (`cargo audit` command missing) and no dependency policy config was present. | Dependency risk cannot be assessed reproducibly from this checkout. | Add `cargo-deny` with advisories/licenses/bans and run it in CI. |
| 5 | Low | [pg-tide-relay/Cargo.toml](../pg-tide-relay/Cargo.toml#L127-L157) | Dev dependencies duplicate many optional production dependencies unconditionally. | Test builds are heavier than necessary. | Gate integration tests by feature groups or use workspace dependency tables to reduce duplication. |

## Aggregate Severity Summary

| Area | Critical | High | Medium | Low | Informational |
|------|---------:|-----:|-------:|----:|--------------:|
| Correctness & Bugs | 3 | 3 | 1 | 0 | 0 |
| Security | 1 | 3 | 2 | 1 | 0 |
| Code Quality & Maintainability | 0 | 1 | 3 | 2 | 0 |
| Performance & Scalability | 0 | 1 | 3 | 1 | 0 |
| Reliability & Observability | 0 | 3 | 2 | 1 | 0 |
| Test Coverage | 1 | 2 | 2 | 1 | 0 |
| API & Schema Design | 1 | 2 | 2 | 1 | 0 |
| Documentation | 0 | 3 | 3 | 0 | 0 |
| DevOps, CI/CD & Packaging | 1 | 2 | 3 | 0 | 0 |
| Dependency Health | 0 | 1 | 3 | 1 | 0 |
| **Total** | **7** | **21** | **24** | **8** | **0** |

## Feature & Roadmap Recommendations

1. **Pipeline Config Schema Registry**
   Problem solved: users and the relay need one authoritative schema for pipeline configs.
   Sketch of implementation: define JSON Schema files for forward/reverse pipeline configs, validate in SQL helpers and the relay, and generate docs from the schema.
   Effort estimate: M. Priority: P0. Milestone: v0.12.0.

2. **Relay Doctor CLI**
   Problem solved: operators need to validate database URL, schema version, TLS, feature availability, and pipeline configs before starting the daemon.
   Sketch of implementation: add `pg-tide doctor --postgres-url ...` and `pg-tide validate-config --pipeline NAME` commands reusing runtime factories in dry-run mode.
   Effort estimate: M. Priority: P0. Milestone: v0.12.0.

3. **End-to-End SQL API Harness**
   Problem solved: current tests miss contract breaks between SQL helpers and relay workers.
   Sketch of implementation: testcontainers PostgreSQL plus actual relay worker tasks configured only through `tide.*` SQL functions.
   Effort estimate: M. Priority: P0. Milestone: v0.12.0.

4. **TLS and mTLS Profiles**
   Problem solved: production users need enforceable transport security for PostgreSQL and sinks/sources.
   Sketch of implementation: add rustls Postgres TLS support, sink-specific TLS config structs, and “require TLS” validation policies.
   Effort estimate: L. Priority: P1. Milestone: v0.13.0.

5. **Outbox-Level ACLs**
   Problem solved: table-wide grants are too broad for multi-tenant databases.
   Sketch of implementation: add `tide.outbox_publishers(outbox_name, role_name)` and enforce inside `outbox_publish()` with admin grant/revoke functions.
   Effort estimate: M. Priority: P1. Milestone: v0.13.0.

6. **Replay Workbench**
   Problem solved: operators need safe replay without editing offsets manually.
   Sketch of implementation: add SQL and CLI commands to preview ranges, dry-run transforms, replay to selected sinks, and mark DLQ entries resolved.
   Effort estimate: L. Priority: P1. Milestone: v0.14.0.

7. **CloudEvents and AsyncAPI Export**
   Problem solved: teams want standard event contracts and discoverability.
   Sketch of implementation: add CloudEvents wire format plus `pg-tide asyncapi export` from catalog metadata and observed schemas.
   Effort estimate: M. Priority: P2. Milestone: v0.14.0.

8. **Schema Evolution Guardrails**
   Problem solved: downstream pipelines need safe behavior on incompatible schema changes.
   Sketch of implementation: store schema versions per pipeline, classify additive/breaking changes, and support policies: pause, DLQ, warn, or auto-create new stream.
   Effort estimate: L. Priority: P1. Milestone: v0.13.0.

9. **Tenant-Aware Relay Groups**
   Problem solved: SaaS deployments need isolation across tenants and teams.
   Sketch of implementation: add tenant columns to catalog tables, RLS policies, per-tenant metrics labels, and tenant-scoped advisory locks.
   Effort estimate: L. Priority: P2. Milestone: v0.15.0.

10. **WASM Transform Plugins**
    Problem solved: users need custom routing/transforms without recompiling the relay.
    Sketch of implementation: embed a WASM runtime with deterministic resource limits and a stable `RelayMessage` ABI.
    Effort estimate: XL. Priority: P3. Milestone: v1.2.0.

11. **Managed Backfill Jobs**
    Problem solved: teams often need initial snapshots and historical backfills alongside CDC streams.
    Sketch of implementation: add cataloged backfill jobs with chunking, progress, pause/resume, and relay-side throttling.
    Effort estimate: L. Priority: P2. Milestone: v0.15.0.

12. **Perses/Grafana Dashboard Generator**
    Problem solved: hand-written dashboards drift from metric names.
    Sketch of implementation: define metric constants and generate dashboards/alerts from a typed manifest.
    Effort estimate: S. Priority: P1. Milestone: v0.12.0.

## Prioritised Action Plan

| Rank | Priority | Owner | Action |
|------|----------|-------|--------|
| 1 | P0 | ext + relay | Align `relay_set_outbox()` / `relay_set_inbox()` config JSON with coordinator expectations. |
| 2 | P0 | relay + ext | Fix relay offset schema mismatch (`last_change_id`/`worker_id` vs `last_offset`). |
| 3 | P0 | relay | Fix pg-inbox sink columns to match extension-created inbox tables. |
| 4 | P0 | infra | Fix Helm env var from `PG_TIDE_RELAY_POSTGRES_URL` to `PG_TIDE_POSTGRES_URL`. |
| 5 | P0 | tests | Add SQL-helper-to-worker end-to-end tests for one forward and one reverse pipeline. |
| 6 | P1 | ext | Enforce `enabled` in `outbox_publish()`. |
| 7 | P1 | ext + relay | Add identifier validation for every dynamic SQL table/schema identifier. |
| 8 | P1 | relay | Implement PostgreSQL TLS support and document behavior accurately. |
| 9 | P1 | relay | Wire v0.11 wire formats into source/sink runtime paths. |
| 10 | P1 | relay | Correct DLQ semantics: idempotent DLQ writes plus source ack after durable DLQ routing. |
| 11 | P1 | infra | Decide slim/full artifact strategy; build release/Docker with the advertised feature set. |
| 12 | P1 | observability | Update metrics instrumentation and regenerate Grafana dashboard queries. |
| 13 | P1 | docs | Correct PostgreSQL compatibility, feature-gate, and version-availability docs. |
| 14 | P2 | ext | Add outbox-level publisher ACLs and remove table-wide publish grants for apps. |
| 15 | P2 | tests | Add sequential SQL upgrade tests for every migration. |
| 16 | P2 | relay | Add connection pooling and configurable worker concurrency. |
| 17 | P2 | infra | Add cargo-deny/cargo-audit, SBOM, Trivy, and cosign signing. |
| 18 | P2 | relay | Batch pg-inbox inserts and measure throughput. |
| 19 | P2 | docs | Fix stale CNPG/CloudNativePG deployment examples and broken README links. |
| 20 | P3 | relay | Add `pg-tide doctor` and `validate-config` CLI subcommands. |

## Appendix: Files Examined

- [AGENTS.md](../AGENTS.md)
- [README.md](../README.md)
- [ROADMAP.md](../ROADMAP.md)
- [CHANGELOG.md](../CHANGELOG.md)
- [Cargo.toml](../Cargo.toml)
- [Cargo.lock](../Cargo.lock)
- [pg_tide.control](../pg_tide.control)
- [pg-tide-ext/Cargo.toml](../pg-tide-ext/Cargo.toml)
- [pg-tide-ext/pg_tide.control](../pg-tide-ext/pg_tide.control)
- [pg-tide-ext/src/error.rs](../pg-tide-ext/src/error.rs)
- [pg-tide-ext/src/lib.rs](../pg-tide-ext/src/lib.rs)
- [pg-tide-ext/src/outbox.rs](../pg-tide-ext/src/outbox.rs)
- [pg-tide-ext/src/inbox.rs](../pg-tide-ext/src/inbox.rs)
- [pg-tide-ext/src/relay.rs](../pg-tide-ext/src/relay.rs)
- [sql/pg_tide--0.1.0.sql](../sql/pg_tide--0.1.0.sql)
- [sql/pg_tide--0.1.0--0.2.0.sql](../sql/pg_tide--0.1.0--0.2.0.sql)
- [sql/pg_tide--0.2.0--0.3.0.sql](../sql/pg_tide--0.2.0--0.3.0.sql)
- [sql/pg_tide--0.3.0--0.4.0.sql](../sql/pg_tide--0.3.0--0.4.0.sql)
- [sql/pg_tide--0.4.0--0.5.0.sql](../sql/pg_tide--0.4.0--0.5.0.sql)
- [sql/pg_tide--0.5.0--0.6.0.sql](../sql/pg_tide--0.5.0--0.6.0.sql)
- [sql/pg_tide--0.6.0--0.7.0.sql](../sql/pg_tide--0.6.0--0.7.0.sql)
- [sql/pg_tide--0.7.0--0.8.0.sql](../sql/pg_tide--0.7.0--0.8.0.sql)
- [sql/pg_tide--0.8.0--0.9.0.sql](../sql/pg_tide--0.8.0--0.9.0.sql)
- [sql/pg_tide--0.9.0--0.10.0.sql](../sql/pg_tide--0.9.0--0.10.0.sql)
- [sql/pg_tide--0.10.0--0.11.0.sql](../sql/pg_tide--0.10.0--0.11.0.sql)
- [pg-tide-relay/Cargo.toml](../pg-tide-relay/Cargo.toml)
- [pg-tide-relay/src/main.rs](../pg-tide-relay/src/main.rs)
- [pg-tide-relay/src/cli.rs](../pg-tide-relay/src/cli.rs)
- [pg-tide-relay/src/config.rs](../pg-tide-relay/src/config.rs)
- [pg-tide-relay/src/coordinator.rs](../pg-tide-relay/src/coordinator.rs)
- [pg-tide-relay/src/dlq.rs](../pg-tide-relay/src/dlq.rs)
- [pg-tide-relay/src/envelope.rs](../pg-tide-relay/src/envelope.rs)
- [pg-tide-relay/src/error.rs](../pg-tide-relay/src/error.rs)
- [pg-tide-relay/src/metrics.rs](../pg-tide-relay/src/metrics.rs)
- [pg-tide-relay/src/otel.rs](../pg-tide-relay/src/otel.rs)
- [pg-tide-relay/src/rate_limiter.rs](../pg-tide-relay/src/rate_limiter.rs)
- [pg-tide-relay/src/circuit_breaker.rs](../pg-tide-relay/src/circuit_breaker.rs)
- [pg-tide-relay/src/routing.rs](../pg-tide-relay/src/routing.rs)
- [pg-tide-relay/src/schema_registry.rs](../pg-tide-relay/src/schema_registry.rs)
- [pg-tide-relay/src/source/outbox.rs](../pg-tide-relay/src/source/outbox.rs)
- [pg-tide-relay/src/source/singer.rs](../pg-tide-relay/src/source/singer.rs)
- [pg-tide-relay/src/source/webhook.rs](../pg-tide-relay/src/source/webhook.rs)
- [pg-tide-relay/src/sink/inbox.rs](../pg-tide-relay/src/sink/inbox.rs)
- [pg-tide-relay/src/sink/pg_outbox.rs](../pg-tide-relay/src/sink/pg_outbox.rs)
- [pg-tide-relay/src/sink/webhook.rs](../pg-tide-relay/src/sink/webhook.rs)
- [pg-tide-relay/src/sink/kafka.rs](../pg-tide-relay/src/sink/kafka.rs)
- [pg-tide-relay/src/sink/nats.rs](../pg-tide-relay/src/sink/nats.rs)
- [pg-tide-relay/src/sink/object_storage.rs](../pg-tide-relay/src/sink/object_storage.rs)
- [pg-tide-relay/src/sink/ducklake.rs](../pg-tide-relay/src/sink/ducklake.rs)
- [pg-tide-relay/src/wire_format/mod.rs](../pg-tide-relay/src/wire_format/mod.rs)
- [pg-tide-relay/src/wire_format/native.rs](../pg-tide-relay/src/wire_format/native.rs)
- [pg-tide-relay/src/wire_format/debezium.rs](../pg-tide-relay/src/wire_format/debezium.rs)
- [pg-tide-relay/src/wire_format/maxwell.rs](../pg-tide-relay/src/wire_format/maxwell.rs)
- [pg-tide-relay/src/wire_format/canal.rs](../pg-tide-relay/src/wire_format/canal.rs)
- [pg-tide-relay/src/wire_format/cdc_json.rs](../pg-tide-relay/src/wire_format/cdc_json.rs)
- [pg-tide-relay/tests/common/mod.rs](../pg-tide-relay/tests/common/mod.rs)
- [pg-tide-relay/tests/outbox_source_test.rs](../pg-tide-relay/tests/outbox_source_test.rs)
- [pg-tide-relay/tests/inbox_sink_test.rs](../pg-tide-relay/tests/inbox_sink_test.rs)
- [pg-tide-relay/tests/consumer_group_test.rs](../pg-tide-relay/tests/consumer_group_test.rs)
- [pg-tide-relay/tests/wire_format_test.rs](../pg-tide-relay/tests/wire_format_test.rs)
- [pg-tide-relay/benches/throughput.rs](../pg-tide-relay/benches/throughput.rs)
- [justfile](../justfile)
- [Dockerfile](../Dockerfile)
- [.github/workflows/ci.yml](../.github/workflows/ci.yml)
- [.github/workflows/docs.yml](../.github/workflows/docs.yml)
- [.github/workflows/release.yml](../.github/workflows/release.yml)
- [helm/pg-tide/Chart.yaml](../helm/pg-tide/Chart.yaml)
- [helm/pg-tide/values.yaml](../helm/pg-tide/values.yaml)
- [helm/pg-tide/templates/deployment.yaml](../helm/pg-tide/templates/deployment.yaml)
- [examples/cnpg/cluster.yaml](../examples/cnpg/cluster.yaml)
- [pg-tide/dashboards/relay-health.json](../pg-tide/dashboards/relay-health.json)
- [docs/src/SUMMARY.md](../docs/src/SUMMARY.md)
- [docs/src/sql-reference/outbox-api.md](../docs/src/sql-reference/outbox-api.md)
- [docs/src/sql-reference/relay-api.md](../docs/src/sql-reference/relay-api.md)
- [docs/src/sql-reference/catalog-tables.md](../docs/src/sql-reference/catalog-tables.md)
- [docs/src/reference/version-compatibility.md](../docs/src/reference/version-compatibility.md)
- [docs/src/sinks/nats.md](../docs/src/sinks/nats.md)
- [docs/src/features/metrics.md](../docs/src/features/metrics.md)
- [docs/src/integration/prometheus-grafana.md](../docs/src/integration/prometheus-grafana.md)
- [docs/src/integration/cloudnativepg.md](../docs/src/integration/cloudnativepg.md)
- [docs/src/operations/deployment-guide.md](../docs/src/operations/deployment-guide.md)
