# v1.0.0 Feature Scope

> **Status:** Feature freeze announcement — this document defines what is and is not
> included in v1.0.0 GA and prevents scope creep in the final sprint.

## What IS in v1.0.0

- Transactional outbox (forward relay) with all v0.x sinks
- Idempotent inbox (reverse relay) with all v0.x sources
- Pipeline dependency DAG with cycle detection (`tide.relay_pipeline_deps`)
- Full AsyncAPI 3.0 catalog export and validation
- HA failover via PostgreSQL advisory locks
- Wire format support: native, Debezium JSON/Avro/Protobuf, Maxwell, Canal, CloudEvents, CDC-JSON, claim-check
- Schema registry integration (Confluent Schema Registry, Apicurio)
- Pipeline templates and multi-outbox fan-in
- Backfill job scheduling with chunking and progress tracking
- Config change history and pipeline lifecycle management
- Delivery receipts with relay integration
- Per-tenant relay groups with RLS and per-tenant metrics
- PostgreSQL TLS/mTLS profiles
- Outbox-level publisher ACLs
- SSRF guard for webhook sinks
- Supply-chain hardening (cargo-deny, SBOM, Trivy, cosign)
- `pg-tide doctor`, `validate-config`, `dag`, `template`, `backfill`, `history` CLI
- Prometheus metrics + OpenTelemetry tracing
- Grafana/Perses relay health dashboard
- Helm chart with Kubernetes deployment support

## What is NOT in v1.0.0

The following features are explicitly deferred to post-v1.0.0 releases:

| Feature | Reason for deferral |
|---|---|
| Logical-replication source (PostgreSQL WAL-based CDC) | Requires additional protocol work; tracked separately |
| Kafka exactly-once delivery (EOS transactions) | Needs producer transaction ID management; post-GA |
| WASM plugin system for custom transforms | API design not finalized |
| Web UI for pipeline management | Out of scope for server binary |
| Additional connector ecosystems (Airbyte v2, Singer v2) | Connector protocol updates needed |
| KMS envelope encryption | Interface designed in v0.33.0; implementation post-GA |
| Envelope-encrypted inbox replay | Depends on KMS implementation |

## Version Compatibility

| Component | Minimum PostgreSQL version |
|---|---|
| pg_tide extension | PostgreSQL 18 |
| pg-tide relay binary | Any; connects to PostgreSQL 18 |

## Stability Guarantees

From v1.0.0:
- The `tide.*` SQL API (v2 forms) is **stable** — no breaking changes without a major version bump.
- The relay binary CLI flags and environment variables are **stable**.
- The AsyncAPI export format (channels, messages, operations) is **stable**.
- The Prometheus metric names (`pg_tide_relay_*`) are **stable**.
- The TOML config format is **stable**.
