# pg_tide Roadmap

pg_tide's roadmap is organized around a focused, dependable, production-grade product: PostgreSQL transactional outbox publishing, idempotent inbox delivery, and a durable, highly available relay delivering events to and from NATS JetStream, Apache Kafka, and HTTPS webhooks through PostgreSQL outboxes and inboxes.

## Active Roadmap

- **[Inbound and Outbound Connector Implementation Plan (v0.55.0 – v0.59.0)](plan_outbound.md)** — The active roadmap for direction-aware relay foundations, NATS and Kafka consumers, HTTPS webhook ingress, PostgreSQL inbox delivery, and mixed-direction production validation.

The earlier **[Pre-v1.0 Hardening, Simplification, and Trust Plan](plans/PLAN_PRE_V1_0.md)** delivered v0.48.0 through v0.54.0. Its former v0.55.0 release-readiness step and v1.0.0 schedule are superseded by the connector program below. v1.0.0 is postponed indefinitely and has no target date.

## Foundational Roadmap

- **[Roadmap to a Focused, Production-Grade Product (v0.40.0 – v0.47.0)](plans/pg-tide-roadmap-to-focused-production-grade.md)** — The foundational roadmap that established the core outbox model, crash safety, resource stewardship, security model, operator tooling, four trusted connectors, and the v0.47.0 public-beta contract freeze.

---

## Roadmap at a Glance

### Phase 1: Focused Foundation (v0.40.0 – v0.47.0) — *Complete*

| Version | Theme | Main Outcome | Status |
|---|---|---|---|
| **v0.40.0** | One Real Pipeline | Canonical shared-table outbox storage model and end-to-end NATS delivery | Delivered |
| **v0.41.0** | Promise Only What We Prove | Maturity tiers (`connectors.toml`), focused build profiles, self-contained workspace | Delivered |
| **v0.42.0** | Crash-Safe by Construction | Delivery state machine (ADR-012), crash matrix, monotonic offsets, HA takeover | Delivered |
| **v0.43.0** | A Good PostgreSQL Citizen | Operational benchmarks, performance budgets, retention hardening, capacity guide | Delivered |
| **v0.44.0** | Secure by Default | Privilege model, fail-closed authorization, TLS defaults, SSRF protection, secret redaction | Delivered |
| **v0.45.0** | Operators First | Operator CLI (`doctor`, `status`, `config validate`), task-oriented metrics, runbooks | Delivered |
| **v0.46.0** | Four Connectors, Fully Trusted | Production support for PG Inbox, NATS JetStream, Kafka, and HTTPS Webhook | Delivered |
| **v0.47.0** | Public Beta and API Freeze | Frozen v1 contract, machine-readable schemas/fixtures, release gates & evidence framework | Delivered |

### Phase 2: Pre-v1.0 Hardening, Simplification, and Trust (v0.48.0 – v0.54.0) — *Complete*

| Version | Theme | Scope | Dependency | Main Outcome | Status |
|---|---|---|---|---|---|
| **v0.48.0** | Baseline Closure and Deterministic CI | Medium | v0.47.0 | Reconcile evidence baseline, authoritative clean-room extension test environment, required-test inventory, flake policy | Delivered |
| **v0.49.0** | Focused Product Surface | Large, split | v0.48.0 | Remove/relocate non-core connectors, protocols, features, and dependencies; drop `experimental-full` from release model | Delivered |
| **v0.50.0** | Delivery Correctness and Failure Semantics | Large | v0.49.0 | Prove no-silent-loss invariant across all supported destinations and crash windows; executable state machine; property tests | Delivered |
| **v0.51.0** | Upgrade, Rollback, and Recovery Integrity | Medium–Large | v0.50.0 | Declare v1 upgrade floor; fresh-install vs upgrade parity; sequential & interrupted migrations; relay/extension rollback; backup/restore | Delivered |
| **v0.52.0** | Security and Supply-Chain Assurance | Medium–Large | v0.51.0 | Reduced-product threat model, privilege review, TLS/SSRF testing, secret canaries, minimal artifacts, SBOM & provenance | Delivered |
| **v0.53.0** | Performance, Capacity, and Long-Run Stability | Medium–Large | v0.52.0 | Versioned operational budgets, reference environment, regression checks, leak detection, 24h qualification soak & 72h candidate soak | Delivered |
| **v0.54.0** | Operator Experience, Documentation, and Hygiene | Medium–Large | v0.53.0 | Consolidated operator journey, executable docs in CI, runbook drills, archive cleanup, repository hygiene | Delivered |

### Phase 3: Pre-v1 Bidirectional Connectors (v0.55.0 – v0.59.0) — *Active*

| Version | Theme | Scope | Dependency | Main Outcome | Status |
|---|---|---|---|---|---|
| **[v0.55.0](roadmap/v0.55.0.md)** | Direction-Aware Relay Foundation | Medium | v0.54.0 | Source-owned batch settlement, unchanged forward behavior, internal direction model, and closed route validation | Planned |
| **[v0.56.0](roadmap/v0.56.0.md)** | NATS JetStream Inbound | Medium–Large | v0.55.0 | Public reverse pipeline API, supported durable NATS pull-consumer delivery to PostgreSQL inbox, and deferred v0.53.0 performance qualification | Planned |
| **[v0.57.0](roadmap/v0.57.0.md)** | Apache Kafka Inbound | Large | v0.56.0 | Supported consumer-group source with per-partition commits, rebalance safety, and inbox-first acknowledgement | Planned |
| **[v0.58.0](roadmap/v0.58.0.md)** | HTTPS Webhook Inbound | Large | v0.57.0 | Supported authenticated HTTPS receiver with bounded handoff and commit-before-response behavior | Planned |
| **[v0.59.0](roadmap/v0.59.0.md)** | Bidirectional Hardening and Validation | Medium–Large | v0.58.0 | Mixed-direction regression, upgrade proof, performance and security closure, operational drills, pilots, and independent reviews | Planned |

### Deferred stable release

| Version | Status | Resumption condition |
|---|---|---|
| **[v1.0.0-rc.N](roadmap/v1.0.0-rc.md)** | Postponed indefinitely | A future roadmap decision defines a stable scope from the product and evidence available at that time |
| **[v1.0.0](roadmap/v1.0.0.md)** | Postponed indefinitely | A validated release candidate exists under a newly approved v1 plan |

---

## Historical Archive

- [`docs/archive/roadmap-pre-focused.md`](docs/archive/roadmap-pre-focused.md) — Historical roadmap prior to the focused production-grade realignment.
- [`docs/archive/`](docs/archive/) — Archived plans, assessments, and research documents.
