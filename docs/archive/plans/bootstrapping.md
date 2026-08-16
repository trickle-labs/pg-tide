# pg_tide Bootstrapping Plan

This plan covers all work needed to bring pg_tide from "CI passes" to
production-ready v1.0 status.

---

## Phase 1 — Test Infrastructure (Priority: Critical)

### 1.1 Testcontainers-based integration tests

The relay is a binary crate with testcontainers already in dev-dependencies.
Build a shared test harness that:

- Spins up a PostgreSQL 18 container with pg_tide extension installed
- Provides helper functions: `setup_outbox()`, `publish_messages()`,
  `assert_inbox_received()`
- Exposes a reusable `PgTideTestDb` struct

**Location:** `pg-tide-relay/tests/common/mod.rs`

**Tests to write (in `pg-tide-relay/tests/`):**

| File | Coverage |
|------|----------|
| `outbox_source_test.rs` | Poll outbox, consume messages, commit offsets |
| `inbox_sink_test.rs` | Deliver messages to inbox, dedup, mark processed |
| `round_trip_test.rs` | Outbox → relay → inbox end-to-end |
| `consumer_group_test.rs` | Multiple consumers, offset tracking, heartbeat |
| `backpressure_test.rs` | High-volume publish, verify no message loss |

### 1.2 Extension SQL tests (pgrx pg_test)

Use `#[pg_test]` in `pg-tide-ext/src/` for SQL-level smoke tests:

- `outbox_create` / `outbox_publish` / `outbox_status` round-trip
- `inbox_create` / `inbox_mark_processed` / `inbox_mark_failed`
- `relay_set_outbox` / `relay_set_inbox` / `relay_enable` / `relay_disable`
- Error paths: duplicate outbox name, non-existent inbox, etc.

### 1.3 CI: Add test jobs

Extend `.github/workflows/ci.yml`:

```yaml
test-relay-integration:
  name: Relay Integration Tests
  runs-on: ubuntu-latest
  services:
    postgres: { image: postgres:18, ... }
  steps:
    - cargo test --package pg-tide-relay --test '*' -- --test-threads=1

test-ext-pgrx:
  name: Extension pgrx Tests
  runs-on: ubuntu-latest
  steps:
    - cargo pgrx test pg18
```

---

## Phase 2 — Relay Feature Completeness

### 2.1 End-to-end sink/source tests (per backend)

Each optional backend needs at least one integration test using
testcontainers-modules:

| Backend | Container | Test file |
|---------|-----------|-----------|
| NATS | `nats:latest` | `nats_test.rs` |
| Kafka | `confluentinc/cp-kafka` | `kafka_test.rs` |
| Redis | `redis:7` | `redis_test.rs` |
| RabbitMQ | `rabbitmq:3-management` | `rabbitmq_test.rs` |
| SQS | `localstack/localstack` | `sqs_test.rs` |
| Webhook | mock HTTP server (axum) | `webhook_test.rs` |

### 2.2 Graceful shutdown & error recovery

- Verify relay handles PostgreSQL disconnects (reconnect with backoff)
- Verify relay handles sink unavailability (buffer, retry, DLQ)
- Verify SIGTERM drains in-flight messages before exit
- Add `--drain-timeout` CLI flag

### 2.3 Exactly-once delivery guarantees

- Implement and test dedup_key tracking across relay restart
- Test idempotent inbox rejects duplicate deliveries
- Test consumer offset is only committed after successful sink delivery

### 2.4 Multi-pipeline coordinator

- Test running multiple pipelines in a single relay process
- Verify per-pipeline metrics isolation
- Test hot-reload of pipeline config (SIGHUP or TOML watch)

---

## Phase 3 — Operability

### 3.1 Observability

- Prometheus metrics endpoint (`/metrics`) — already wired via axum
- Add metrics: `pg_tide_messages_published_total`, `pg_tide_messages_delivered_total`,
  `pg_tide_delivery_latency_seconds`, `pg_tide_consumer_lag`,
  `pg_tide_relay_errors_total`
- Health endpoint (`/health`) with liveness + readiness semantics
- Structured JSON logging (already have tracing-subscriber)
- OpenTelemetry trace propagation through message envelope

### 3.2 Docker image

- Multi-stage Dockerfile for the `pg-tide` relay binary
- Publish to GHCR: `ghcr.io/trickle-labs/pg-tide:latest`
- Alpine-based final image, ~20 MB
- GitHub Actions workflow for image build + push on tag

### 3.3 Helm chart / CNPG example

- Helm chart for deploying relay as a Kubernetes Deployment
- CNPG example: install pg_tide extension in CloudNativePG cluster
- Document sidecar pattern: relay container alongside CNPG pod

### 3.4 Release automation

Create `.github/workflows/release.yml`:

- Triggered on `v*` tag push
- Build relay binaries for linux-amd64, linux-arm64, macos-amd64, macos-arm64
- Build + push Docker image
- Create GitHub Release with binaries attached
- Publish crate to crates.io (pg-tide-relay only — ext is not publishable)

---

## Phase 4 — SQL Extension Hardening

### 4.1 Upgrade scripts

- Create `sql/pg_tide--0.1.0--0.2.0.sql` template
- Add `scripts/check_upgrade_completeness.sh` (port from pg-trickle)
- CI job: verify upgrade path from previous version

### 4.2 Security

- RLS policies on `tide.*` catalog tables (relay pipelines belong to owner)
- `GRANT` / `REVOKE` helpers: `tide.grant_publish(role, outbox)`
- Audit logging for relay config changes

### 4.3 Performance

- Benchmark: outbox publish throughput (messages/sec)
- Benchmark: relay end-to-end latency (publish → delivered)
- Benchmark: inbox dedup overhead at scale (1M+ processed keys)
- Index tuning for `tide.outbox_messages` partition pruning

### 4.4 Partitioning & retention

- Auto-partition outbox message tables by time (already `retention_hours` param)
- Background worker or cron job for partition drop
- `tide.outbox_truncate_delivered()` for manual cleanup

---

## Phase 5 — Documentation (mdBook)

### 5.1 Structure

Follow the same pattern as pg-trickle and pg-ripple: a `docs/` directory with
mdBook sources, built via `mdbook build` and published to GitHub Pages.

```
docs/
├── book.toml
└── src/
    ├── SUMMARY.md
    ├── introduction.md
    │
    ├── evaluate/
    │   ├── when-to-use.md
    │   ├── architecture.md
    │   └── comparison.md
    │
    ├── getting-started/
    │   ├── installation.md
    │   ├── quickstart.md
    │   └── tutorial.md
    │
    ├── concepts/
    │   ├── transactional-outbox.md
    │   ├── idempotent-inbox.md
    │   ├── consumer-groups.md
    │   ├── relay-pipelines.md
    │   └── exactly-once-delivery.md
    │
    ├── sql-reference/
    │   ├── outbox-api.md
    │   ├── inbox-api.md
    │   ├── relay-api.md
    │   ├── consumer-groups-api.md
    │   └── catalog-tables.md
    │
    ├── relay-guide/
    │   ├── configuration.md
    │   ├── cli-reference.md
    │   ├── backends/
    │   │   ├── nats.md
    │   │   ├── kafka.md
    │   │   ├── redis.md
    │   │   ├── rabbitmq.md
    │   │   ├── sqs.md
    │   │   └── webhook.md
    │   ├── error-handling.md
    │   └── monitoring.md
    │
    ├── operations/
    │   ├── deployment.md
    │   ├── docker.md
    │   ├── kubernetes.md
    │   ├── scaling.md
    │   ├── backup-and-restore.md
    │   ├── upgrading.md
    │   └── troubleshooting.md
    │
    ├── tutorials/
    │   ├── outbox-to-kafka.md
    │   ├── inbox-from-nats.md
    │   ├── bidirectional-sync.md
    │   ├── fan-out-pattern.md
    │   └── dead-letter-queue.md
    │
    ├── integration/
    │   ├── pg-trickle.md
    │   ├── dbt.md
    │   ├── cloudnativepg.md
    │   └── pgbouncer.md
    │
    └── reference/
        ├── configuration.md
        ├── errors.md
        ├── changelog.md
        └── security.md
```

### 5.2 book.toml

```toml
[book]
title       = "pg_tide Documentation"
authors     = ["The pg_tide Contributors"]
src         = "docs/src"
language    = "en"

[build]
build-dir   = "docs/book"

[preprocessor.admonish]
command     = "mdbook-admonish"
assets_version = "3.0.2"

[output.html]
default-theme           = "navy"
preferred-dark-theme    = "navy"
git-repository-url      = "https://github.com/trickle-labs/pg-tide"
edit-url-template       = "https://github.com/trickle-labs/pg-tide/edit/main/docs/src/{path}"
site-url                = "/pg-tide/"

[output.html.search]
enable              = true
limit-results       = 20
use-boolean-and     = true
boost-title         = 2
heading-split-level = 3
```

### 5.3 CI: Documentation build + deploy

Add `.github/workflows/docs.yml`:

```yaml
name: Documentation
on:
  push:
    branches: [main]
    paths: ['docs/**']
  workflow_dispatch:

jobs:
  build-deploy:
    runs-on: ubuntu-latest
    permissions:
      pages: write
      id-token: write
    steps:
      - uses: actions/checkout@v4
      - name: Install mdBook
        run: |
          cargo install mdbook --version "^0.4"
          cargo install mdbook-admonish --version "^1"
      - name: Build docs
        run: mdbook build
      - name: Deploy to Pages
        uses: actions/deploy-pages@v4
        with:
          artifact_name: docs/book
```

### 5.4 Documentation priorities

Write these pages first (they unlock adoption):

1. `introduction.md` — what is pg_tide, why use it
2. `getting-started/quickstart.md` — 5-minute walkthrough
3. `sql-reference/outbox-api.md` — full API docs
4. `relay-guide/configuration.md` — TOML config reference
5. `relay-guide/cli-reference.md` — CLI flags and env vars
6. `concepts/transactional-outbox.md` — the core pattern explained

---

## Phase 6 — Ecosystem & Adoption

### 6.1 crates.io publication

- Publish `pg-tide-relay` to crates.io (`cargo install pg-tide`)
- Add `[[bin]]` metadata: categories, keywords, readme

### 6.2 Examples directory

```
examples/
├── docker-compose.yml          # Full stack: PG + pg_tide + relay + NATS
├── outbox-to-kafka/            # Step-by-step example
├── inbox-from-webhook/         # Incoming event processing
├── bidirectional-nats/         # Bi-directional sync between services
└── kubernetes/                 # K8s manifests
```

### 6.3 dbt integration

- dbt macro: `tide_publish()` — publish from dbt model post-hook
- dbt source: expose `tide.outbox_pending` and `tide.consumer_lag`

### 6.4 pg_trickle integration testing

- E2E test in pg-trickle that installs both extensions and verifies
  `pgtrickle.attach_outbox()` works with pg_tide

---

## Milestones

| Milestone | Target | Phases |
|-----------|--------|--------|
| v0.2.0 — Tested | — | Phase 1 (tests) + Phase 4.1 (upgrades) |
| v0.3.0 — Observable | — | Phase 2 (relay) + Phase 3.1–3.2 (metrics, Docker) |
| v0.4.0 — Documented | — | Phase 5 (docs) + Phase 6.2 (examples) |
| v1.0.0 — Production | — | Phase 3.3–3.4 + Phase 4 + Phase 6 |

---

## Immediate Next Actions

1. Create `pg-tide-relay/tests/common/mod.rs` — test harness with PG container
2. Write `round_trip_test.rs` — outbox → relay → inbox E2E
3. Write `outbox_source_test.rs` — outbox polling, offset commit
4. Add `test-integration` CI job
5. Scaffold `docs/` with `book.toml` + `SUMMARY.md` + `introduction.md`
