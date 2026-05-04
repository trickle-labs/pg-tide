# Changelog

What's new in pg_tide — written for everyone, not just developers.

For future plans and upcoming features, see [plans/bootstrapping.md](plans/bootstrapping.md).

## Table of Contents

<!-- TOC start -->
- [0.2.0 — Post-0.1.0 Hardening & Observability](#020--2026-05-04--post-010-hardening--observability)
- [0.1.0 — Initial Release](#010--initial-release)
<!-- TOC end -->

---

## [0.2.0] — 2026-05-04 — Post-0.1.0 Hardening & Observability

Post-launch hardening, observability improvements, Docker enhancements,
CI fixes, and pgrx compatibility fixes.

### Observability

- **Metrics**: Added `consumer_lag` (messages behind the committed offset, per
  consumer group) and `delivery_latency_seconds` (histogram of end-to-end
  relay latency) to the Prometheus `/metrics` endpoint. Both metrics are
  emitted by the relay binary and require no extension upgrade.

### Relay Binary

- **Docker — Native ARM**: The release workflow now builds on native ARM64
  runners, cutting cross-compilation time significantly.
- **Docker — Semver tags**: Images are now tagged with the full version
  (`v0.1.0`), the minor prefix (`v0.1`), and `latest`, matching the convention
  used by most container registries.
- **Docker — OCI annotations**: Images carry standard OCI image labels
  (`org.opencontainers.image.*`) for source, revision, and creation time,
  making provenance visible in any OCI-compliant registry.

### Extension Hardening

Several pgrx compatibility issues discovered after the initial release were
fixed. None require an upgrade script — they correct the compiled extension
artefact only:

- Constraint names on inbox tables are now quoted, so inbox names containing
  hyphens (e.g. `my-service-inbox`) work correctly.
- Literal backslashes in raw SQL strings were removed, eliminating a
  `standard_conforming_strings`-dependent edge case.
- The `tide` schema is now declared via `#[pgrx::pg_schema]` rather than a
  bare `CREATE SCHEMA` in the DDL file, fixing schema creation ordering under
  `cargo-pgrx 0.18`.
- `trusted` and `schema` fields removed from the control file; neither is
  supported by `cargo-pgrx 0.18`.

### CI

- Bumped `actions/checkout` v4 → v6 and `actions/cache` v4 → v5.
- Granted the test runner write access to PostgreSQL extension directories,
  fixing pgrx test failures on GitHub-hosted runners.
- Split the `clippy` job so extension and relay linting run independently,
  giving clearer failure attribution.
- Fixed release workflow: the `CARGO_REGISTRY_TOKEN` guard is now at
  step-level, preventing a false "secret not set" error on crates.io publish.

### Documentation

- Restructured and consolidated the `docs/` tree into a MdBook with separate
  reference, relay guide, integration, operations, and tutorial sections.
- Added relay CLI phase plans (migrated from pg-trickle) covering the
  `pg-tide` binary command-line interface roadmap.

---

## [0.1.0] — 2025-05-03 — Initial Release

v0.1.0 is the founding release of `pg_tide`. The full transactional outbox,
idempotent inbox, consumer group, and relay subsystem (~6,150 Rust LOC +
~2,500 SQL LOC) was extracted from
[`pg_trickle`](https://github.com/trickle-labs/pg-trickle) v0.46.0 and
published as a standalone PostgreSQL 18 extension.

### SQL Functions — Outbox

- `tide.outbox_create(name, retention_hours, inline_threshold_rows)` — creates
  a named outbox table and registers it in the catalog.
- `tide.outbox_publish(name, payload, dedup_key)` — appends a message to the
  outbox inside the caller's transaction; the message becomes visible to the
  relay only after the transaction commits.
- `tide.outbox_drop(name)` — removes the outbox and its catalog entry.
- `tide.outbox_status(name)` — returns pending count, last publish time, and
  retention settings.
- `tide.outbox_enable(name)` / `tide.outbox_disable(name)` — pause and resume
  relay consumption without dropping the outbox.

### SQL Functions — Inbox

- `tide.inbox_create(name)` — creates a named inbox table with dedup tracking.
- `tide.inbox_drop(name)` — removes the inbox and its catalog entry.
- `tide.inbox_mark_processed(name, dedup_key)` — idempotently marks a message
  delivered; duplicate calls are silently ignored.
- `tide.inbox_mark_failed(name, dedup_key, reason)` — moves a message to the
  dead-letter queue.
- `tide.inbox_status(name)` — returns pending, processed, and DLQ counts.
- `tide.replay_inbox_messages(name, since)` — re-queues DLQ messages for
  reprocessing.

### SQL Functions — Consumer Groups

- `tide.create_consumer_group(outbox, group_name)` — registers a named
  consumer group against an outbox.
- `tide.drop_consumer_group(outbox, group_name)` — removes the group and its
  offset tracking.
- `tide.commit_offset(outbox, group_name, offset)` — advances the committed
  read position.
- `tide.consumer_heartbeat(outbox, group_name)` — updates the liveness
  timestamp; groups that miss heartbeats are marked stale.

### SQL Functions — Relay Catalog

- `tide.relay_set_outbox(relay_name, outbox_name, consumer_group)` — registers
  the source side of a relay pipeline.
- `tide.relay_set_inbox(relay_name, inbox_name)` — registers the destination
  side of a relay pipeline.
- `tide.relay_enable(relay_name)` / `tide.relay_disable(relay_name)` — start
  and stop relay processing for a named pipeline.
- `tide.relay_delete(relay_name)` — removes a pipeline from the catalog.
- `tide.relay_get_config(relay_name)` / `tide.relay_list_configs()` — inspect
  pipeline configuration.

### Views

- `tide.outbox_pending` — messages in the outbox not yet consumed by any
  registered consumer group.
- `tide.consumer_lag` — per-consumer-group lag (messages published minus
  committed offset).

### Relay Binary (`pg-tide`)

Multi-backend relay supporting **NATS**, **Kafka**, **Redis Streams**,
**RabbitMQ**, **Amazon SQS**, **Webhook**, and **stdout**. Features:

- Advisory-lock-based HA coordination — only one relay instance is active
  per pipeline at a time; additional instances stand by and take over
  automatically on failure.
- Prometheus `/metrics` endpoint and `/health` liveness probe.
- Structured JSON logging via `tracing`.
- TOML-based pipeline configuration loaded from file or the `pg_tide` relay
  catalog.

[Unreleased]: https://github.com/trickle-labs/pg-tide/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/trickle-labs/pg-tide/releases/tag/v0.1.0
