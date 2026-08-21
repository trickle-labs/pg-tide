# pg_tide Roadmap

pg_tide's roadmap is organized around a focused, dependable, production-grade product: PostgreSQL transactional outbox publishing, idempotent inbox delivery, and a durable, highly available relay delivering events to PostgreSQL, NATS JetStream, Apache Kafka, and HTTPS webhooks.

## Active Roadmap

- **[Pre-v1.0 Hardening, Simplification, and Trust Plan (v0.48.0 – v1.0.0)](plans/PLAN_PRE_V1_0.md)** — The active roadmap governing pre-v1 baseline closure, non-core surface removal, delivery correctness proofs, lifecycle testing, security auditing, performance budgeting, documentation execution, production pilots, and independent reviews.

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

### Phase 2: Pre-v1.0 Hardening, Simplification, and Trust (v0.48.0 – v1.0.0) — *Active*

| Version | Theme | Scope | Dependency | Main Outcome | Status |
|---|---|---|---|---|---|
| **v0.48.0** | Baseline Closure and Deterministic CI | Medium | v0.47.0 | Reconcile evidence baseline, authoritative clean-room extension test environment, required-test inventory, flake policy | Delivered |
| **v0.49.0** | Focused Product Surface | Large, split | v0.48.0 | Remove/relocate non-core connectors, protocols, features, and dependencies; drop `experimental-full` from release model | Delivered |
| **v0.50.0** | Delivery Correctness and Failure Semantics | Large | v0.49.0 | Prove no-silent-loss invariant across all supported destinations and crash windows; executable state machine; property tests | Delivered |
| **v0.51.0** | Upgrade, Rollback, and Recovery Integrity | Medium–Large | v0.50.0 | Declare v1 upgrade floor; fresh-install vs upgrade parity; sequential & interrupted migrations; relay/extension rollback; backup/restore | Planned |
| **v0.52.0** | Security and Supply-Chain Assurance | Medium–Large | v0.51.0 | Reduced-product threat model, privilege review, TLS/SSRF testing, secret canaries, minimal artifacts, SBOM & provenance | Planned |
| **v0.53.0** | Performance, Capacity, and Long-Run Stability | Medium–Large | v0.52.0 | Versioned operational budgets, reference environment, regression checks, leak detection, 24h qualification soak & 72h candidate soak | Planned |
| **v0.54.0** | Operator Experience, Documentation, and Hygiene | Medium–Large | v0.53.0 | Consolidated operator journey, executable docs in CI, runbook drills, archive cleanup, repository hygiene | Planned |
| **v0.55.0** | Independent Validation and Release Readiness | Medium | v0.54.0 | 4 production pilots, 5 independent reviews, ownership succession, full release rehearsal, zero-blocker gate | Planned |
| **v1.0.0-rc.N** | Release Candidate Series | Blocker fixes | v0.55.0 | Blocker fixes only, exact candidate artifact verification | Planned |
| **v1.0.0** | The Trust Release | Promotion | Final RC | GA promotion of the validated candidate with full production guarantees | Planned |

---

## Historical Archive

- [`docs/archive/roadmap-pre-focused.md`](docs/archive/roadmap-pre-focused.md) — Historical roadmap prior to the focused production-grade realignment.
- [`docs/archive/`](docs/archive/) — Archived plans, assessments, and research documents.
