# Changelog

All notable changes to pg_tide are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — 2025-05-03

Initial release. Extracted from [pg_trickle](https://github.com/trickle-labs/pg-trickle) v0.46.0.

### Added

- **Transactional Outbox** — `outbox_create`, `outbox_publish`, `outbox_drop`, `outbox_status`, `outbox_enable`, `outbox_disable`
- **Idempotent Inbox** — `inbox_create`, `inbox_drop`, `inbox_mark_processed`, `inbox_mark_failed`, `inbox_status`, `replay_inbox_messages`
- **Consumer Groups** — `create_consumer_group`, `drop_consumer_group`, `commit_offset`, `consumer_heartbeat`
- **Relay Catalog** — `relay_set_outbox`, `relay_set_inbox`, `relay_enable`, `relay_disable`, `relay_delete`, `relay_get_config`, `relay_list_configs`
- **Views** — `tide.outbox_pending`, `tide.consumer_lag`
- **Relay Binary** — multi-backend relay with NATS, Kafka, Redis, RabbitMQ, SQS, Webhook, stdout support
- **Observability** — Prometheus metrics, health endpoint, structured logging
- **HA** — advisory lock coordination for multi-instance deployments

[0.1.0]: https://github.com/trickle-labs/pg-tide/releases/tag/v0.1.0
