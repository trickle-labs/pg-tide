# v1 scope

v1.0.0 will be defined only after the v0.47.0 contract freeze, external
pilots, independent review, and release evidence are complete. This is a
reviewed candidate boundary, not a GA feature list.

## Foundation

- Transactional PostgreSQL outbox and idempotent PostgreSQL inbox.
- Relay catalog configuration and the documented `core` relay path.
- PostgreSQL inbox, NATS JetStream, Apache Kafka, and HTTPS webhook outbound
  support.
- Stable, documented delivery, retry, failure, health, metrics, CLI, and
  envelope behavior covered by release evidence.

## Outside the current scope

- Inbound or reverse relay paths and all preview or experimental connectors.
- Orchestration, managed backfills, data-lake integrations, and other non-core
  roadmap surfaces.
- PostgreSQL versions below 18, arbitrary source-to-sink combinations,
  exactly-once transport, and a stable Rust or plugin ABI.
- Internal tables and Rust types, undocumented configuration, and historical
  roadmap items without current implementation and evidence.

Read the [stability guarantees](stability-guarantees.md),
[support policy](support/support-policy.md), and
[release evidence guidance](operations/release-evidence.md) together.
