# Changelog

All notable changes to pg_tide are documented here.

---

## [0.1.0] — 2025-05-03

Initial release. Extracted from [pg_trickle](https://github.com/trickle-labs/pg-trickle) v0.46.0 as a standalone extension.

### Added

- **Transactional Outbox**
  - `tide.outbox_create()` — create named outboxes with retention policy
  - `tide.outbox_publish()` — atomic message publishing within transactions
  - `tide.outbox_drop()` — remove outboxes and their messages
  - `tide.outbox_status()` — JSONB status summary
  - `tide.outbox_enable()` / `tide.outbox_disable()` — pause/resume publishing
  - `tide.outbox_pending` view — pending messages per outbox

- **Idempotent Inbox**
  - `tide.inbox_create()` — create named inboxes with DLQ settings
  - `tide.inbox_drop()` — remove inboxes
  - `tide.inbox_mark_processed()` — mark successful processing
  - `tide.inbox_mark_failed()` — record failures with retry tracking
  - `tide.inbox_status()` — JSONB status summary
  - `tide.replay_inbox_messages()` — re-queue failed messages

- **Consumer Groups**
  - `tide.create_consumer_group()` — named groups with offset reset policy
  - `tide.drop_consumer_group()` — remove groups with cascade
  - `tide.commit_offset()` — commit processing position
  - `tide.consumer_heartbeat()` — liveness signaling
  - `tide.consumer_lag` view — per-consumer lag monitoring

- **Relay Catalog**
  - `tide.relay_set_outbox()` — configure forward pipelines
  - `tide.relay_set_inbox()` — configure reverse pipelines
  - `tide.relay_enable()` / `tide.relay_disable()` — pipeline lifecycle
  - `tide.relay_delete()` — remove pipeline config
  - `tide.relay_get_config()` / `tide.relay_list_configs()` — query config
  - LISTEN/NOTIFY triggers for hot-reload

- **Relay Binary (`pg-tide`)**
  - Multi-backend support: NATS, Kafka, Redis, RabbitMQ, SQS, Webhook, stdout
  - PostgreSQL advisory lock coordination for HA
  - Prometheus metrics endpoint (`/metrics`)
  - Health endpoint (`/health`)
  - Exponential backoff reconnection
  - Structured JSON logging
  - TOML configuration with environment variable substitution
  - Graceful shutdown on SIGTERM/SIGINT
