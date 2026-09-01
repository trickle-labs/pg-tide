# pg_tide Pre-v1.0 Hardening, Simplification, and Trust Plan

**Status:** Completed through v0.54.0; remaining schedule superseded
**Starting point:** v0.47.0  
**Delivered range:** v0.48.0 through v0.54.0
**Recommended repository path:** `plans/PLAN_PRE_V1_0.md`  
**Primary objective:** Turn the v0.47.0 public-beta contract into a small, dependable, independently validated v1 product.

> **Supersession note (2026-09-01):** v0.48.0 through v0.54.0 delivered the
> reduction and hardening work in this plan. The former v0.55.0 release-readiness
> scope and v1.0.0 schedule are no longer active. The project will implement
> inbound connectors in v0.55.0 through v0.59.0 under
> [`plan_outbound.md`](../plan_outbound.md), and v1.0.0 is postponed
> indefinitely. The remaining sections below preserve the original plan and
> its historical acceptance criteria.

---

## 1. Executive Summary

pg_tide should use the period between v0.47.0 and v1.0.0 to **subtract, prove, simplify, optimize, document, and rehearse**.

The project should not use this period to add more connectors, transports, wire formats, orchestration modes, or headline features. v0.47.0 already defines the intended v1 contract around:

- PostgreSQL transactional outbox publishing
- PostgreSQL idempotent inbox delivery
- A durable, highly available relay
- PostgreSQL inbox, NATS JetStream, Apache Kafka, and HTTPS webhook destinations
- Native pg_tide and CloudEvents envelopes
- Stable SQL, configuration, metrics, health, and CLI machine-readable interfaces
- Docker, Helm, and CloudNativePG deployment paths

The existing roadmap deliberately defines a focused v1 promise and excludes broad connector coverage, arbitrary source-to-sink combinations, cloud KMS providers, data-lake ingestion, fan-in, DAG orchestration, managed backfill, and WAL logical replication unless they independently pass the same production-support gates. 

The pre-v1 program therefore has eight ordered releases:

| Version | Theme | Main outcome |
|---|---|---|
| **v0.48.0** | Baseline Closure and Deterministic CI | Establish a truthful release baseline and eliminate environment-blocked testing |
| **v0.49.0** | Focused Product Surface | Remove or relocate non-core connectors, protocols, features, dependencies, and documentation |
| **v0.50.0** | Delivery Correctness and Failure Semantics | Prove the no-silent-loss invariant across all supported destinations and crash windows |
| **v0.51.0** | Upgrade, Rollback, and Recovery Integrity | Make supported upgrades, rollback, mixed versions, backup, restore, and interruption recovery routine |
| **v0.52.0** | Security and Supply-Chain Assurance | Independently validate privileges, networking, secrets, dependencies, and release artifacts |
| **v0.53.0** | Performance, Capacity, and Long-Run Stability | Establish enforceable budgets, optimize measured bottlenecks, and complete sustained soak testing |
| **v0.54.0** | Operator Experience, Documentation, and Repository Quality | Consolidate the product experience, execute documentation, and remove repository clutter |
| **v0.55.0** | Independent Validation and Release Readiness | Complete production pilots, independent reviews, ownership, release rehearsal, and final gates |
| **v1.0.0-rc.N** | Release Candidate Series | Permit blocker fixes only and validate exact candidate artifacts |
| **v1.0.0** | The Trust Release | Promote the final validated candidate without expanding scope |

The release order is intentional:

1. Establish a trustworthy baseline.
2. Remove features that should not consume pre-v1 engineering effort.
3. Prove correctness on the reduced product.
4. Prove lifecycle compatibility and recovery.
5. Audit the stable, reduced attack surface.
6. Optimize the stable architecture.
7. Polish documentation, operations, and repository structure.
8. Validate the exact release candidate with real operators and independent reviewers.

Work on independent subsystems may overlap, but releases must occur in this order. A later release may not weaken or bypass an earlier release gate.

---

## 2. Current State

### 2.1 The contract is defined, but the evidence record is incomplete

The v0.47.0 implementation added the v1 contract manifest, machine-readable schemas and fixtures, governance policies, release gates, and evidence indexes. Its implementation pull request explicitly stated that pilot evidence, independent reviews, release-manager approval, artifact digests, and zero-blocker confirmation remained pending. 

The current release evidence index still records:

- `status: pending`
- no candidate commit
- no artifact or image digests
- no blocker count
- no release-manager approval
- all release gates as required rather than completed 

All four required pilot profiles remain pending:

- PostgreSQL outbox to NATS JetStream
- PostgreSQL outbox to Kafka
- PostgreSQL outbox to PostgreSQL inbox
- PostgreSQL outbox to HTTPS webhook 

All five required independent review disciplines also remain pending:

- PostgreSQL extension
- Rust async and concurrency
- Delivery semantics
- Security
- Operations 

The first pre-v1 task is therefore not to pretend this evidence exists. It is to make the status accurate, establish the exact baseline, and create a disciplined path to complete the evidence against later candidates.

### 2.2 The supported product is small, but the repository surface remains very large

The supported destination set is already focused, but `connectors.toml` still contains numerous preview and experimental connectors, protocols, compatibility surfaces, and unavailable groundwork. These include cloud queues, alternate messaging systems, databases, warehouses, data-lake formats, ETL protocols, notification sinks, inbound connector modes, fan-in, reverse paths, and WAL-source groundwork. Many experimental entries record their tested version as `unknown`.  

The relay manifest still defines a large `experimental-full` feature profile and carries optional dependencies for those integrations. Its package description also advertises a broader relay product than the intended v1 promise. 

The documentation navigation presents many experimental sinks, sources, wire formats, features, and tutorials alongside the supported product. A reader must understand maturity policy before knowing which pages describe the actual v1 product. 

### 2.3 Some release-critical testing remains environment-dependent

The v0.47.0 implementation reported successful relay tests and contract checks, but the full extension unit suite was locally blocked after a pgrx hard abort caused subsequent tests to fail on the test mutex. A release-critical suite must not remain dependent on one developer machine or an unreproducible local environment. 

### 2.4 Performance infrastructure exists but needs a final-product contract

The repository already has a useful reference workflow covering publishing, relay load, pipeline density, outage recovery, retention, HA interruption, and soak testing. The scheduled soak defaults to 1,800 seconds, which is appropriate for nightly signal but insufficient as the final long-run stability proof. 

### 2.5 Ownership remains concentrated

The current principal CODEOWNERS boundaries are assigned to one owner. The v1 release model requires independent review and at least two people capable of approving the core path and executing a release. 

---

## 3. Product Decision

### 3.1 v1 production-supported core

The following surfaces are retained, hardened, and frozen for v1.

| Area | Retained v1 surface |
|---|---|
| Source | PostgreSQL native transactional outbox |
| Destinations | PostgreSQL inbox, NATS JetStream, Apache Kafka, HTTPS webhook |
| Delivery model | At-least-once delivery with explicit acknowledgment and deduplication boundaries |
| Storage | One canonical PostgreSQL outbox model with durable monotonic checkpoints |
| Availability | PostgreSQL advisory-lock ownership and tested takeover |
| Failure handling | Bounded retries, circuit breaking where required, DLQ, replay, and operator-visible errors |
| Event formats | Native pg_tide JSON and CloudEvents |
| Security | Fail-closed authorization, verified TLS, SSRF controls, secret references, LocalKeyFile where encryption is required |
| Operations | CLI, stable JSON output, health endpoints, Prometheus metrics, dashboards, alerts, runbooks |
| Deployment | Binary packages, container images, Helm, CloudNativePG |
| Diagnostics | stdout and file output may remain as diagnostic sinks but are not advertised as production integrations |

### 3.2 Default pre-v1 removal and relocation decisions

Non-core features should not remain in the main repository merely because they compile. Before v0.49.0 begins, create a preservation tag such as:

```text
pre-v1-experimental-surface
```

After that tag is created, apply the following default dispositions.

| Category | Examples | Default action |
|---|---|---|
| Preview inbound connectors | NATS source, Kafka source, webhook receiver | Move to a Labs repository or remove from main |
| Alternate messaging and queues | Redis, RabbitMQ, SQS, Kinesis, Pub/Sub, Azure Service Bus, Event Hubs, MQTT | Move only implementations with an active owner and a real integration test; delete the rest |
| Databases and warehouses | Elasticsearch, MongoDB, ClickHouse, BigQuery, Snowflake | Remove from main |
| Data-lake and object-storage integrations | Object storage, Delta Lake, Iceberg, DuckLake, RockLake, Arrow Flight | Remove from main; retain historical design material only where architecturally useful |
| ETL and ecosystem protocols | Singer, Airbyte | Remove from main |
| Notification sinks | Slack, Discord, PagerDuty | Remove from main |
| Alternate wire formats | Debezium, Maxwell, Canal, generic CDC JSON | Move to Labs or remove unless an active pre-v1 pilot requires one |
| Orchestration and reverse modes | Fan-in, broad bidirectional paths, pipeline DAG orchestration, managed backfill, reverse lake sources | Remove or defer beyond v1 |
| WAL source groundwork | Experimental logical-replication source | Remove from production code; retain an ADR or research note if useful |
| Cloud KMS compatibility features | AWS, GCP, Vault providers that are not implemented and tested | Remove feature names and public configuration; retain LocalKeyFile only |
| Non-core framework features | Schema registry, content routing, arbitrary transforms, specialized telemetry integrations | Remove or relocate unless required by one of the four supported workflows |
| Stubs and unavailable compatibility entries | Disabled or “not registered” surfaces | Delete |

A feature may avoid removal only when all of the following are documented before the v0.49.0 inventory closes:

- A real current user or committed pre-v1 pilot
- A named long-term code owner
- A security contact
- A working protocol-level integration test
- A documented support boundary
- A justified reason it belongs in the main v1 repository
- No disproportionate dependency, CI, documentation, or audit cost

Popularity, historical effort, or compilation success are not sufficient reasons.

### 3.3 Labs repository rule

Code moved out of the main repository should go to a separate repository such as `pg-tide-labs` only when:

- it has an active owner;
- it has at least one meaningful test;
- it has an explicit experimental status;
- it can depend on the stable public pg_tide contract rather than internal modules;
- it does not participate in production release artifacts.

Everything else should be deleted from the main branch and preserved through Git history and the pre-removal tag.

The main repository must not acquire a permanent `archive/experimental-code/` directory. Git history is the archive for obsolete code.

---

## 4. Goals

The pre-v1 program must produce the following outcomes:

1. **A smaller product**
   - Only the supported core is built, documented, packaged, audited, and released.
   - Non-core features no longer inflate dependencies, CI, support expectations, or attack surface.

2. **A proved delivery model**
   - Every supported destination is tested through the public API.
   - Every important failure window has an explicit invariant and test.
   - No committed outbox event can be silently lost.

3. **Boring lifecycle management**
   - Fresh installation, upgrade, rollback, restore, and mixed-version operation are automated and documented.
   - Interrupted operations recover deterministically.

4. **A bounded security surface**
   - Authorization fails closed.
   - TLS, SSRF, secrets, dependencies, and artifacts receive independent review.
   - Production artifacts contain only required code and files.

5. **Measured performance**
   - Capacity and resource budgets are committed.
   - Optimization is based on profiles.
   - Long-running tests demonstrate bounded memory, storage, connection, task, and file-descriptor behavior.

6. **Executable documentation**
   - Supported examples run in CI against packaged artifacts.
   - The documentation leads with the actual v1 product.
   - Experimental and historical material cannot be mistaken for supported functionality.

7. **A maintainable repository**
   - Dead files, obsolete plans, unused scripts, stale features, unused dependencies, and duplicate documentation are removed.
   - Current plans and historical material are clearly separated.

8. **Independent release capability**
   - Reviews do not depend on the primary author approving their own work.
   - At least two people can execute and approve the release process.

---

## 5. Non-Goals

The following are not pre-v1 objectives:

- Adding another production-supported connector
- Adding another source type
- Adding another wire format
- Building a generic integration platform
- Building a workflow or DAG engine
- Implementing arbitrary bidirectional synchronization
- Adding cloud KMS providers
- Adding distributed workers or external state management
- Adding UI or control-plane products
- Supporting every PostgreSQL major version
- Rewriting the relay architecture without measured justification
- Achieving arbitrary line-coverage percentages
- Preserving unused experimental code in the main repository
- Expanding the frozen contract because an implementation already exists

New ideas should be recorded in a post-v1 backlog rather than merged into the pre-v1 plan.

---

## 6. Engineering and Release Rules

### 6.1 Contract protection

The v0.47.0 freeze covers supported surfaces, not every experimental item present in the repository.

- Supported frozen surfaces may receive compatible additive changes, defect corrections, security fixes, and documentation improvements.
- Preview and experimental surfaces may be removed.
- Any breaking correction to a frozen supported surface requires:
  - a `contract-change` issue;
  - a documented critical correctness or security reason;
  - migration or deprecation treatment;
  - updated contract artifacts;
  - targeted pilot and reviewer reapproval.

After the first v1 release candidate, no intentional breaking change is permitted.

### 6.2 Evidence over claims

Every production claim must link to:

- a black-box test;
- a release artifact;
- a pilot record;
- a benchmark result;
- an independent review;
- or another reproducible form of evidence.

A compile check is not connector evidence. A request-construction test is not an integration test. A database insert benchmark is not an end-to-end relay benchmark.

### 6.3 Issue sizing

Every release is an epic composed of focused issues.

- **Small:** one component and one focused pull request.
- **Medium:** one subsystem, normally split across two to four pull requests.
- Issues larger than Medium must be decomposed before implementation.
- Mechanical removals may be grouped by dependency family, but core and experimental changes must not be mixed in the same pull request.

### 6.4 Definition of done for every issue

Each issue must address, where applicable:

- User-visible problem
- Current behavior
- Desired behavior
- Supported contract affected
- Compatibility impact
- Security impact
- Failure modes
- Tests that fail before the change
- Metrics or logs required
- Documentation changes
- Migration or rollback impact
- Explicit non-goals
- Validation commands
- Evidence location

### 6.5 Release gates beat version targets

A release is complete only when its acceptance criteria pass on the default branch. Work that misses the gate moves to the next release. Acceptance criteria must not be weakened to preserve a schedule.

---

# 7. Roadmap at a Glance

| Release | Scope | Dependency | Exit question |
|---|---|---|---|
| v0.48.0 | Medium | v0.47.0 | Is the baseline truthful, reproducible, and testable from a clean environment? |
| v0.49.0 | Large, split into focused removals | v0.48.0 | Is the main repository now the focused v1 product rather than an integration laboratory? |
| v0.50.0 | Large | v0.49.0 | Can every supported failure window occur without silent data loss? |
| v0.51.0 | Medium–Large | v0.50.0 | Are install, upgrade, rollback, mixed versions, backup, and recovery deterministic? |
| v0.52.0 | Medium–Large | v0.51.0 | Has the reduced product passed independent security and supply-chain validation? |
| v0.53.0 | Medium–Large | v0.52.0 | Are performance, capacity, and resource growth measured and bounded? |
| v0.54.0 | Medium–Large | v0.53.0 | Can an operator and contributor understand and use the product without repository archaeology? |
| v0.55.0 | Medium | v0.54.0 | Has the exact product been independently exercised and approved for a candidate series? |
| v1.0.0-rc.N | Blocker fixes only | v0.55.0 | Is this exact candidate ready for promotion? |
| v1.0.0 | Promotion | final RC | Has pg_tide earned the stable production promise? |

---

# 8. v0.48.0 — Baseline Closure and Deterministic CI

## 8.1 Objective

Establish a truthful, reproducible starting point before deleting, refactoring, auditing, or optimizing anything.

This release adds no product functionality.

## 8.2 User promise

> The project can state exactly what v0.47.0 proved, what remains pending, and which test environment is authoritative.

## 8.3 Required work

### BASE-1: Reconcile the v0.47.0 evidence status — Small

Update the release evidence records so they accurately describe the released artifact.

Required decisions:

1. If pilots and reviews were actually completed against the exact v0.47.0 artifacts, record:
   - candidate commit;
   - artifact and image digests;
   - issue query result;
   - reviewer identity and approval;
   - pilot operator sign-off;
   - release-manager approval.

2. If they were not completed, preserve `pending` and explicitly state:
   - v0.47.0 established the contract baseline;
   - it did not complete the full production-pilot and independent-review gate;
   - those gates remain mandatory before v1.

Do not backfill evidence from later versions as though it applied to v0.47.0.

Files:

- `release-evidence/v0.47.0-index.json`
- `release-evidence/v0.47.0-pilots.json`
- `release-evidence/v0.47.0-reviews.json`
- `docs/src/operations/release-evidence.md`
- `CHANGELOG.md`

### BASE-2: Create the authoritative extension-test environment — Medium

Build one reproducible PostgreSQL 18 and pgrx test environment that runs the complete extension suite.

Requirements:

- Runs in CI or a versioned container image
- Uses pinned PostgreSQL, Rust, pgrx, and system dependencies
- Does not depend on a developer-local PostgreSQL installation
- Diagnoses the first hard abort rather than allowing mutex cascades to hide it
- Uploads PostgreSQL logs, pgrx logs, core dumps where supported, and failing test metadata
- Fails when any required test is skipped because of environment limitations
- Can be invoked locally through one documented command

Recommended commands:

```text
just test-extension-clean
just test-unit-clean
```

Recommended CI job:

```text
extension-cleanroom
```

### BASE-3: Establish a required-test inventory — Small

Create `tests/required-tests.toml` or an equivalent manifest listing:

- Required PR tests
- Required scheduled tests
- Required release tests
- Service dependencies
- Feature profiles
- Expected environment
- Owner
- Maximum retry policy
- Whether the test is allowed to be ignored locally

Add a checker that fails when a required test is removed, renamed, disabled, or changed to ignored without review.

### BASE-4: Create a flake registry and policy — Small

Create a temporary flake registry with:

- test name;
- owner;
- first observed date;
- failure signature;
- issue link;
- quarantine expiry;
- release impact.

Rules:

- No P0/P1 test may be quarantined.
- A quarantine must expire.
- Re-running a failed job until it passes is not an accepted release procedure.
- The registry must be empty before v1.0.0-rc.1.

### BASE-5: Capture the pre-simplification baseline — Small

Record:

- Full dependency graph
- Binary and container sizes
- Clean build duration
- Test counts by level
- CI duration by job
- Documentation page count
- Connector and feature count
- Production image contents
- Current operational benchmark results

Store the machine-readable record under:

```text
release-evidence/pre-v1-baseline/
```

This baseline measures the effect of later simplification.

## 8.4 Acceptance criteria

- [ ] v0.47.0 evidence accurately states completed and pending gates.
- [ ] No evidence record claims approval without identity, date, commit, and artifact digest.
- [ ] The full extension suite passes in the authoritative clean environment.
- [ ] No required suite is documented as environment-blocked.
- [ ] The required-test manifest is enforced in CI.
- [ ] The flake policy and registry exist.
- [ ] The pre-simplification dependency, artifact, CI, and benchmark baseline is committed.
- [ ] All v1 contract and artifact checks remain green.

## 8.5 Explicit non-goals

- Connector removal
- Relay refactoring
- New tests beyond those needed to stabilize the baseline
- Performance optimization
- Public API changes

---

# 9. v0.49.0 — Focused Product Surface

## 9.1 Objective

Reduce the main repository to the product pg_tide intends to support at v1.

This release should remove more code than it adds.

## 9.2 User promise

> Everything prominently built, packaged, and documented by pg_tide belongs to the supported PostgreSQL outbox relay product.

## 9.3 Required work

### SURF-1: Approve the v1 surface inventory — Medium

Generate a complete inventory from:

- `connectors.toml`
- Cargo features
- CLI commands
- SQL functions
- configuration schema variants
- documentation navigation
- container contents
- Helm values
- metrics
- wire formats
- background services
- tests

Classify every item:

```text
v1-supported
diagnostic
labs
remove
internal
```

Create:

```text
plans/V1_SURFACE_DISPOSITION.md
schemas/v1-surface-disposition.json
```

The machine-readable file should be checked against the implementation.

### SURF-2: Preserve and extract viable Labs work — Medium

Before removal:

1. Create the `pre-v1-experimental-surface` tag.
2. Identify experimental components with an active owner and real test.
3. Move those components to a Labs repository.
4. Replace internal-module dependencies with stable public interfaces where practical.
5. Mark the Labs repository as unsupported and independently released.

Do not block v0.49.0 on perfect extraction. A component that cannot be cleanly separated is deleted from main and may be recovered from Git history later.

### SURF-3: Remove non-core connector families — Medium per family

Use separate pull requests for:

1. Alternate messaging and queues
2. Databases and warehouses
3. Data-lake and object-storage integrations
4. ETL and notification integrations
5. Preview inbound connectors
6. Alternate wire formats
7. Orchestration and reverse modes
8. Unsupported cloud KMS and unavailable stubs

Each pull request must remove:

- implementation modules;
- Cargo features;
- exclusive dependencies;
- registry entries;
- generated documentation;
- tests that validate only the removed feature;
- examples;
- CI jobs;
- configuration keys;
- metrics unique to the removed feature;
- feature-specific Helm values;
- stale changelog and README claims from current documentation.

Historical release notes remain intact.

### SURF-4: Remove `experimental-full` from the main release model — Small

Delete the `experimental-full` Cargo profile from the main repository.

Retain only intentional profiles such as:

```text
core
core-kafka
```

Every release artifact must use an explicitly supported profile.

Add CI checks that reject:

- release builds with non-supported features;
- undeclared feature combinations;
- production images containing Labs-only dependencies.

### SURF-5: Simplify configuration and validation — Medium

After removals:

- Remove unsupported connector variants from the active pipeline schema.
- Remove dead field bags and feature-gated keys.
- Reject removed connector types with one stable error code.
- Add migration documentation for users of experimental configurations.
- Ensure preview or removed keys cannot be silently ignored.
- Regenerate the v1 contract artifacts.

Recommended error:

```text
PGTIDE_CONFIG_UNSUPPORTED_SURFACE
```

The error should identify:

- removed connector or feature;
- last version containing it;
- Labs or migration location where applicable;
- supported alternatives.

### SURF-6: Simplify relay internals after removal — Medium

Delete abstractions, branches, registries, and generic machinery used only by removed features.

Targets include:

- direction combinations no longer supported;
- generic connector factories with only one remaining use;
- optional dependency adapters;
- feature-gated dead paths;
- broad configuration enums;
- metrics labels whose values came only from removed connectors;
- test helpers used only by removed integrations.

Do not rewrite stable core logic merely to achieve stylistic uniformity. Simplification must reduce code paths or invariants.

### SURF-7: Refocus product metadata and documentation — Small

Update:

- root `README.md`
- relay package description
- crate keywords
- website introduction
- `docs/src/SUMMARY.md`
- support matrix
- Helm descriptions
- container labels
- examples
- release notes

The first screen of every product surface should state the focused product promise.

Experimental documentation that remains useful belongs under one clearly labeled Labs or historical section, not alongside supported sinks and sources.

### SURF-8: Add surface-regression checks — Small

Create a script such as:

```text
scripts/check_v1_surface.py
```

It must fail when:

- an undeclared connector appears;
- an unsupported feature enters a production profile;
- a removed configuration variant returns;
- the documentation links an experimental connector as supported;
- a release image contains an unapproved dependency;
- package metadata advertises a removed integration.

## 9.4 Acceptance criteria

- [ ] The main connector registry contains only supported, diagnostic, or explicitly internal surfaces.
- [ ] Non-core preview and experimental connectors are moved or deleted.
- [ ] `experimental-full` no longer exists in the main release model.
- [ ] No production artifact contains a removed connector dependency.
- [ ] Pipeline schemas reject removed connector types explicitly.
- [ ] README, package metadata, documentation navigation, support policy, and release profiles describe the same product.
- [ ] The clean build and production artifact are measurably smaller than the v0.48 baseline.
- [ ] Core v1 contract tests remain green.
- [ ] Historical code is preserved by tag and Git history rather than copied into the main tree.
- [ ] No supported core destination has been removed or weakened.

## 9.5 Explicit non-goals

- Replacing supported connectors
- Adding a new Labs framework to the main repository
- General plugin architecture
- Refactoring stable core code that is unaffected by removals
- Promoting any experimental feature to avoid deleting it

---

# 10. v0.50.0 — Delivery Correctness and Failure Semantics

## 10.1 Objective

Prove the delivery state machine and the no-silent-loss invariant across every supported connector and important failure window.

## 10.2 User promise

> A committed outbox event may be retried or duplicated according to the documented connector boundary, but it is never silently lost.

## 10.3 Required work

### COR-1: Create the executable delivery model — Medium

Implement a small model of the relay state machine:

```text
Polled
  → Encoded
  → PublishStarted
  → SinkAccepted
  → SinkAcknowledged
  → CheckpointCommitted
  → CleanupEligible
```

The model must define for every transition:

- authoritative durable state;
- retry behavior;
- duplicate risk;
- checkpoint rule;
- cleanup rule;
- required metric;
- required structured event;
- required test.

The model should be executable in tests rather than existing only as prose.

### COR-2: Complete public-API end-to-end coverage — Medium

Retain the existing public API NATS and Kafka paths and add equivalent tests for PostgreSQL inbox and webhook.

Recommended test files:

```text
pg-tide-relay/tests/public_api_outbox_to_nats_e2e.rs
pg-tide-relay/tests/public_api_outbox_to_kafka_e2e.rs
pg-tide-relay/tests/public_api_outbox_to_pg_inbox_e2e.rs
pg-tide-relay/tests/public_api_outbox_to_webhook_e2e.rs
```

Every test must:

1. Start a fresh PostgreSQL instance.
2. Install the packaged extension.
3. Configure the pipeline through public APIs.
4. Start the real relay binary.
5. Publish inside an application transaction.
6. Observe the real downstream result.
7. Verify checkpoint advancement through public state.
8. Restart the relay.
9. Verify no event is lost.
10. Verify documented duplicate behavior.

No direct catalog mutation or hidden table setup may substitute for a public workflow.

### COR-3: Implement the crash-window matrix — Medium

Add controlled failpoints at:

- after poll, before encode;
- after encode, before publication;
- after network send, before acknowledgment result;
- after sink acceptance, before checkpoint commit;
- during checkpoint commit;
- after checkpoint commit, before source cleanup;
- during DLQ persistence;
- during replay;
- during graceful shutdown;
- during forced process termination;
- during HA ownership transfer.

For each supported connector and applicable failpoint, assert:

- event visibility;
- checkpoint position;
- retry classification;
- possible duplicate count;
- metrics;
- structured log code;
- final recovery.

Test failpoints must never be compiled into production profiles.

### COR-4: Add property-based checkpoint tests — Medium

Generate sequences of:

- polls;
- batch boundaries;
- transient failures;
- permanent failures;
- acknowledgments;
- checkpoint writes;
- ownership changes;
- shutdowns;
- retries.

Prove:

- checkpoint monotonicity;
- no checkpoint beyond the acknowledged frontier;
- no cleanup below an active consumer frontier;
- stable event identifiers across retry;
- idempotent recovery after repeated transitions;
- bounded retry and backoff behavior;
- deterministic classification of permanent versus transient failures.

### COR-5: Prove duplicate boundaries — Small

For each supported destination, document and test:

| Connector | Required duplicate proof |
|---|---|
| PostgreSQL inbox | Repeated delivery produces one destination effect through the event-ID constraint |
| NATS JetStream | Duplicate behavior inside and outside the configured duplicate window |
| Kafka | Duplicate behavior across producer session and relay restart boundaries |
| Webhook | Stable idempotency key and receiver-side duplicate handling |

The documentation must not use “exactly once” unless the entire user-observable path actually provides that guarantee.

### COR-6: Harden DLQ and replay semantics — Medium

Test:

- DLQ persistence failure
- DLQ full or unavailable condition
- replay interruption
- replay of an already delivered event
- replay with invalid destination configuration
- replay after connector recovery
- bounded replay batches
- checkpoint interaction
- visibility through status, metrics, and logs

A failed DLQ write must not silently discard the source event.

### COR-7: Add focused fuzz targets — Medium

Add fuzzing for:

- pipeline JSON parsing;
- native event envelopes;
- CloudEvents envelopes;
- webhook response classification;
- subject and topic template expansion;
- header handling;
- DLQ records;
- checkpoint serialization;
- CLI machine-readable output parsing.

Commit minimized regression cases for every discovered defect.

### COR-8: Enforce truthful test naming — Small

Audit tests named:

```text
e2e
integration
chaos
restart
recovery
```

Require them to satisfy the repository’s documented test-level definitions. Rename or replace tests that overstate what they prove.

## 10.4 Acceptance criteria

- [ ] All four supported destinations have public-API black-box end-to-end tests.
- [ ] The delivery state machine is executable and linked from its ADR.
- [ ] Every defined crash window has an invariant and automated test.
- [ ] Checkpoint monotonicity is property-tested.
- [ ] No checkpoint advances before the connector acknowledgment boundary.
- [ ] Duplicate behavior is documented and observed for all four destinations.
- [ ] DLQ failure cannot silently discard an event.
- [ ] HA takeover preserves the same checkpoint and duplicate invariants.
- [ ] No test called end-to-end bypasses the public API.
- [ ] Required tests pass repeatedly without rerun-dependent success.
- [ ] No silent-loss defect remains open.

## 10.5 Explicit non-goals

- Exactly-once transport claims
- New connector functionality
- Performance optimization unrelated to correctness
- General distributed model checking
- Supporting experimental connectors in the crash matrix

---

# 11. v0.51.0 — Upgrade, Rollback, and Recovery Integrity

## 11.1 Objective

Make every supported lifecycle transition automated, deterministic, and operator-recoverable.

## 11.2 User promise

> Installing, upgrading, rolling back, restoring, and recovering pg_tide does not require private knowledge or direct catalog repair.

## 11.3 Required work

### UP-1: Declare the supported v1 upgrade floor — Small

Recommended policy:

- **Supported production upgrade floor:** v0.47.0
- **Engineering compatibility evidence:** retain sequential migration testing from earlier retained versions where practical
- **Support promise:** direct support begins at the documented floor

Publish:

- supported source versions;
- unsupported direct jumps;
- required intermediate versions;
- extension versus relay compatibility;
- rollback limitations;
- end-of-support policy.

### UP-2: Build fresh-install versus upgrade parity checks — Medium

For every supported path:

1. Create a fresh install of the target.
2. Upgrade from the supported source version.
3. Normalize catalog and SQL metadata.
4. Compare:
   - schemas;
   - tables;
   - functions;
   - signatures;
   - defaults;
   - privileges;
   - ownership;
   - indexes;
   - constraints;
   - triggers;
   - extension version;
   - generated contract artifacts.

Any difference requires an explicit compatibility explanation.

### UP-3: Test sequential and interrupted migrations — Medium

For every migration:

- run sequentially;
- interrupt at controlled points where practical;
- restart PostgreSQL;
- rerun or recover;
- confirm idempotent completion or explicit refusal;
- verify relay behavior during the documented migration window.

Every migration document must state:

- transactional behavior;
- locking behavior;
- reversibility;
- expected operator steps;
- recovery steps;
- verification query.

### UP-4: Define and test mixed-version operation — Medium

Test the supported combinations of:

- old extension with new relay;
- new extension with old relay;
- multiple relay instances at different supported versions;
- rolling Helm upgrade;
- rolling CloudNativePG deployment.

Unsupported combinations must fail early with a bounded compatibility error.

### UP-5: Rehearse relay rollback — Small

Test:

- new relay to previous supported relay;
- rollback before processing;
- rollback with an existing checkpoint;
- rollback after a retryable failure;
- rollback after DLQ records exist;
- rollback when a new optional config field is present.

The previous relay must either operate safely or refuse with a documented recovery path.

### UP-6: Define extension rollback policy — Medium

For each extension migration:

- mark reversible or irreversible;
- provide reverse SQL where supported;
- refuse unsafe downgrade;
- document data or state created by the new version;
- test rollback against representative data.

A no-op reverse migration must not imply safety where runtime state is incompatible.

### UP-7: Add backup, restore, and disaster-recovery tests — Medium

Test:

- logical backup and restore;
- physical backup and restore where the deployment model supports it;
- restore to a clean cluster;
- restore with pending outbox events;
- restore with DLQ data;
- restore with checkpoint state;
- failover followed by relay recovery;
- PITR consistency boundaries.

Document whether external destinations must be resynchronized or deduplicated after restore.

### UP-8: Automate Helm and CloudNativePG lifecycle tests — Medium

Required scenarios:

- clean install;
- upgrade;
- rollback;
- PostgreSQL failover;
- relay ownership transfer;
- secret rotation;
- readiness during migration;
- unsuccessful upgrade recovery.

Tests must use released or candidate artifacts, not source-only binaries.

## 11.4 Acceptance criteria

- [ ] The supported v1 upgrade floor is explicit.
- [ ] Fresh-install and upgraded catalog states are equivalent or intentionally documented.
- [ ] Every supported migration path is automated.
- [ ] Interrupted migrations recover safely or fail with actionable instructions.
- [ ] Supported mixed-version combinations pass.
- [ ] Unsupported combinations fail before polling or publishing.
- [ ] Relay rollback is tested.
- [ ] Extension rollback behavior is explicit for every migration.
- [ ] Backup and restore preserve documented delivery invariants.
- [ ] Helm and CloudNativePG install, upgrade, rollback, and failover tests pass.
- [ ] No supported lifecycle operation requires undocumented direct catalog edits.

## 11.5 Explicit non-goals

- Supporting every historical version
- Pretending every extension migration is reversible
- Cross-major PostgreSQL upgrade automation beyond the declared platform support
- Automatic repair of arbitrary manual catalog changes

---

# 12. v0.52.0 — Security and Supply-Chain Assurance

## 12.1 Objective

Independently validate the reduced v1 product’s privilege, network, secret, dependency, and artifact boundaries.

## 12.2 User promise

> pg_tide fails closed, protects credentials, limits outbound access, and ships verifiable production artifacts.

## 12.3 Required work

### SEC-1: Refresh the threat model against the reduced product — Medium

The threat model must cover:

- publisher impersonation;
- unauthorized pipeline modification;
- relay credential theft;
- malicious event payloads;
- webhook SSRF;
- DNS and redirect manipulation;
- destination impersonation;
- checkpoint tampering;
- DLQ tampering;
- event replay;
- secret leakage;
- dependency compromise;
- CI and release compromise;
- malicious or compromised container contents;
- privilege escalation through extension functions;
- unsafe search paths;
- restore and rollback abuse.

For each threat, record prevention, detection, and recovery.

### SEC-2: Independently review the PostgreSQL privilege model — Medium

Verify:

- publisher permissions;
- relay permissions;
- operator permissions;
- reader permissions;
- extension-owner behavior;
- security-definer functions;
- locked search paths;
- object ownership;
- schema privileges;
- migration privileges;
- failure behavior when ACL lookups error.

Add negative tests for every role boundary.

### SEC-3: Harden all supported network clients — Medium

For PostgreSQL, NATS, Kafka, and webhook:

- verify certificates by default in production configurations;
- make insecure operation explicit and noisy;
- document minimum TLS behavior;
- test certificate expiry;
- test hostname mismatch;
- test untrusted authorities;
- test authentication failure;
- test proxy behavior where supported;
- test connection downgrade attempts.

For webhook specifically, test:

- loopback;
- link-local;
- private ranges;
- cloud metadata endpoints;
- redirect chains;
- redirect from public to private;
- DNS rebinding-sensitive flows;
- alternate address encodings;
- malformed URLs;
- proxy bypass paths.

### SEC-4: Complete secret-handling assurance — Medium

Secrets must not appear in:

- logs;
- error messages;
- CLI JSON;
- status output;
- metrics;
- support bundles;
- config export;
- config history;
- panic messages;
- test diagnostics;
- release evidence.

Add generated canary secrets and scan all output channels during success and failure paths.

Verify file-secret permission checks and environment-secret behavior.

### SEC-5: Reduce and audit the production dependency graph — Medium

After v0.49 removals:

- generate the production-only dependency graph;
- remove unused direct dependencies;
- remove duplicated libraries where practical;
- review all advisory exceptions;
- add expiry and owner to every exception;
- enforce license policy;
- enforce minimum supported Rust version;
- verify locked and reproducible dependency resolution.

Release policy:

- no unapproved critical vulnerability;
- no unapproved high vulnerability in the production graph;
- every approved exception has an owner, rationale, mitigation, and expiry.

### SEC-6: Minimize production artifacts — Small

Create an allowlist of files and executables permitted in:

- relay binary packages;
- extension packages;
- core container;
- core-kafka container;
- Helm chart;
- documentation artifact.

Fail the release when:

- test fixtures;
- source credentials;
- build caches;
- Labs code;
- development tools;
- unapproved executables;
- unnecessary package managers;
- unexpected shared libraries

appear in production artifacts.

### SEC-7: Verify signing, SBOM, and provenance — Medium

Automate independent verification of:

- source tag signature;
- binary checksums;
- container signatures;
- chart provenance;
- SBOM completeness;
- provenance subject digest;
- artifact-to-commit linkage;
- release-evidence linkage.

The verification must run in a separate job from artifact production.

### SEC-8: Rehearse vulnerability response — Small

Run a tabletop or test disclosure exercise covering:

- private report intake;
- triage;
- severity;
- maintainer assignment;
- embargoed fix;
- CVE or advisory handling where applicable;
- release preparation;
- backport decision;
- coordinated disclosure;
- user notification.

Record process defects as issues.

### SEC-9: Obtain independent security approval — Medium

The reviewer must inspect the exact supported core and the evidence above. Experimental code removed in v0.49 is outside the v1 security scope.

Any material post-review change to:

- privileges;
- checkpoint logic;
- networking;
- secret handling;
- dependencies;
- artifact composition;
- release workflow

invalidates the affected approval.

## 12.4 Acceptance criteria

- [ ] The reduced-product threat model is approved.
- [ ] All supported role boundaries have positive and negative tests.
- [ ] Security-sensitive lookup failures fail closed.
- [ ] Supported network paths verify TLS according to policy.
- [ ] Webhook SSRF and redirect tests cover all documented bypass classes.
- [ ] Secret canaries do not appear in any supported output path.
- [ ] Production dependency and license gates pass.
- [ ] No unapproved critical or high production vulnerability remains.
- [ ] Production artifacts match a minimal allowlist.
- [ ] Tags, binaries, images, charts, SBOMs, and provenance are independently verifiable.
- [ ] A vulnerability-response rehearsal is complete.
- [ ] Independent security review approves the exact release commit.

## 12.5 Explicit non-goals

- Auditing removed Labs connectors
- Implementing cloud KMS providers
- Building a general secret-management product
- Claiming formal verification
- Supporting insecure defaults for convenience

---

# 13. v0.53.0 — Performance, Capacity, and Long-Run Stability

## 13.1 Objective

Define the performance and resource contract of the final product, optimize measured bottlenecks, and prove long-run stability.

## 13.2 User promise

> Operators can estimate pg_tide’s cost, detect regressions, and run it for sustained periods without unbounded resource growth.

## 13.3 Required work

### PERF-1: Commit versioned operational budgets — Medium

Create:

```text
benchmarks/budgets-v1.toml
benchmarks/reference-environment.md
```

Budgets must cover:

- publish transaction overhead;
- p50, p95, and p99 delivery latency;
- sustainable events per second;
- sustainable bytes per second;
- backlog catch-up rate;
- PostgreSQL CPU;
- relay CPU;
- memory high-water mark;
- memory growth slope;
- WAL amplification;
- outbox storage growth;
- index growth;
- cleanup rate;
- active connections;
- file descriptors;
- async task count;
- graceful-shutdown duration;
- HA takeover duration;
- DLQ replay throughput.

No budget may remain `TBD` when the release exits.

### PERF-2: Standardize the reference environment — Small

Document and automate:

- CPU;
- memory;
- storage;
- operating system;
- PostgreSQL configuration;
- service versions;
- network topology;
- Rust toolchain;
- build flags;
- relay profile;
- dataset;
- event size distribution;
- repetitions;
- warm-up;
- result retention.

Reference results from incomparable environments must not update the committed baseline.

### PERF-3: Expand benchmark profiles — Medium

Retain and refine:

- publish-single;
- publish-concurrent;
- relay-core;
- relay-large;
- pipeline-density;
- outage-recovery;
- retention;
- HA-interruption.

Add:

- small-message high-rate;
- large-message bounded-rate;
- slow destination;
- intermittent destination;
- DLQ-heavy;
- checkpoint-heavy;
- sustained backlog recovery;
- mixed four-destination profile;
- graceful shutdown under load.

### PERF-4: Add statistical regression checks — Medium

Use repeated runs and documented comparison rules.

The checker should distinguish:

- noise;
- improvement;
- actionable regression;
- invalid environment;
- missing sample.

PR microbenchmarks may use wider thresholds. Scheduled reference tests and release tests should use stricter, statistically justified budgets.

### PERF-5: Add leak and growth detection — Medium

Sample over time:

- resident memory;
- heap allocations where practical;
- task count;
- connection count;
- file descriptors;
- outbox rows;
- DLQ rows;
- checkpoint records;
- temporary files;
- log volume;
- metric label cardinality.

Fail when a supposedly bounded resource grows monotonically beyond its budget.

### PERF-6: Run sustained soak profiles — Medium

Required durations:

- Nightly signal: existing short soak
- Pre-release qualification: at least 24 hours
- Final release-candidate qualification: at least 72 hours

Introduce during the soak:

- destination outage;
- relay restart;
- HA ownership transfer;
- PostgreSQL failover where supported;
- retry bursts;
- DLQ events;
- cleanup;
- configuration reload where supported.

The final candidate soak must use candidate artifacts.

### PERF-7: Optimize measured hot paths — Medium per hotspot

Optimization work begins only after profiles identify bottlenecks.

Likely candidates include:

- outbox polling query and indexes;
- batch assembly;
- serialization copies;
- connector publication concurrency;
- checkpoint commit frequency;
- connection-pool behavior;
- allocation churn;
- structured logging overhead;
- cleanup batch sizing;
- backlog recovery scheduling.

Every optimization pull request must include:

- before result;
- profile evidence;
- change;
- after result;
- correctness regression tests;
- memory or complexity tradeoff;
- rollback path.

Avoid architecture rewrites unless local optimization cannot meet a committed budget.

### PERF-8: Publish a capacity-planning guide — Medium

The guide must help operators estimate:

- event and byte volume;
- message-size limits;
- batch size;
- destination latency;
- expected backlog;
- retention requirements;
- PostgreSQL storage;
- WAL growth;
- relay CPU and memory;
- connection counts;
- number of pipelines;
- failover headroom;
- cleanup capacity.

Include conservative example configurations rather than maximum benchmark numbers alone.

## 13.4 Acceptance criteria

- [ ] Versioned v1 budgets exist for all required metrics.
- [ ] Reference-environment configuration is reproducible.
- [ ] Benchmark results use repeated runs and validated comparison rules.
- [ ] Resource-growth tests cover memory, tasks, connections, descriptors, storage, and label cardinality.
- [ ] A 24-hour qualification soak passes.
- [ ] The final release-readiness process includes a 72-hour candidate soak.
- [ ] No unexplained performance regression exceeds the committed budget.
- [ ] Optimizations are supported by before-and-after evidence.
- [ ] Capacity-planning documentation is published.
- [ ] Performance claims distinguish reference results from guarantees.

## 13.5 Explicit non-goals

- Winning synthetic benchmark competitions
- Rewriting working code for theoretical efficiency
- Optimizing removed experimental features
- Hiding correctness costs
- Promising identical results on all hardware

---

# 14. v0.54.0 — Operator Experience, Documentation, and Repository Quality

## 14.1 Objective

Make the focused product easy to install, understand, diagnose, maintain, contribute to, and audit.

## 14.2 User promise

> A normal operator can use and recover pg_tide through public interfaces and current documentation without repository archaeology.

## 14.3 Required work

### OPS-1: Define one canonical operator journey — Small

The primary documented flow should be:

```text
install
configure
validate
run
observe
diagnose
upgrade
recover
```

CLI entry points should remain focused around:

```text
pg-tide doctor
pg-tide status
pg-tide config validate
pg-tide config export
pg-tide run
pg-tide replay
pg-tide maintenance sweep
```

Compatibility aliases may remain where frozen, but should not be the primary documentation path.

### OPS-2: Improve errors without breaking machine contracts — Medium

For every supported failure, provide:

- bounded stable error code;
- affected component;
- likely cause;
- safe next action;
- runbook link or identifier;
- redacted context;
- retryability;
- CLI exit code.

Human-readable wording may improve. Frozen JSON field names and types must remain compatible.

Create an error catalog generated from code or checked against code.

### OPS-3: Reorganize documentation around the supported product — Medium

Recommended top-level structure:

```text
Evaluate
Getting Started
Concepts
SQL API
Relay
Supported Destinations
Operations
Security
Upgrades
Troubleshooting
Reference
Support
Labs and Historical Material
```

The supported-destination section contains only:

- PostgreSQL inbox
- NATS JetStream
- Apache Kafka
- HTTPS webhook
- stdout/file diagnostics, clearly labeled

Do not place removed connectors in primary navigation.

### OPS-4: Make supported documentation executable — Medium

Execute in CI:

- installation commands;
- Quick Start SQL;
- pipeline JSON;
- TOML;
- CLI examples;
- Docker examples;
- Helm examples;
- CloudNativePG examples;
- upgrade examples;
- recovery commands;
- supported connector examples.

Tests must use packaged or candidate artifacts wherever practical.

Add markers to distinguish:

```text
tested
illustrative
historical
labs
```

Only `tested` examples may appear in the supported Quick Start and runbooks.

### OPS-5: Consolidate duplicate and conflicting pages — Small

Audit and merge:

- duplicate troubleshooting guides;
- overlapping getting-started tutorials;
- duplicate deployment instructions;
- historical APIs in current guides;
- repeated connector configuration material;
- conflicting support statements;
- duplicate glossary definitions.

Add a broken-link and orphan-page check.

### OPS-6: Exercise every supported runbook — Medium

At minimum, drill:

- relay will not start;
- extension version mismatch;
- privilege failure;
- pipeline invalid;
- pipeline undiscovered;
- NATS unavailable;
- Kafka unavailable;
- PostgreSQL destination unavailable;
- webhook authentication failure;
- TLS failure;
- lag growth;
- DLQ growth;
- duplicate observation;
- ownership ambiguity;
- PostgreSQL failover;
- cleanup failure;
- disk growth;
- failed upgrade;
- rollback;
- restore.

A runbook is complete only after a person other than its author follows it successfully.

### OPS-7: Harden support bundles — Small

Support bundles must:

- include versions, bounded diagnostics, status, metrics metadata, and recent stable error codes;
- exclude payloads, secrets, connection strings, certificates, internal addresses where policy requires redaction, and unbounded logs;
- have a machine-readable manifest;
- pass secret-canary tests;
- document retention and sharing expectations.

### OPS-8: Tidy planning and historical material — Medium

Adopt:

```text
ROADMAP.md                       concise current roadmap
roadmap/                         active release summaries
plans/                           active implementation plans only
docs/archive/releases/           completed release plans with index
docs/archive/research/           retained historical research
docs/adr/                        active and superseded ADRs
```

Actions:

- Move completed implementation plans out of active `plans/`.
- Mark superseded ADRs explicitly.
- Delete plans that only restate merged code and have no historical value.
- Remove duplicate generated files.
- Remove obsolete examples and screenshots.
- Remove stale version-specific comments from manifests where the changelog or ADR is authoritative.
- Preserve current historical roadmap links through redirects or index pages where necessary.

### OPS-9: Remove unused scripts, dependencies, and files — Medium

Create repository-hygiene checks for:

- uncalled scripts;
- unused direct dependencies;
- orphan fixtures;
- orphan documentation;
- unreferenced schemas;
- stale generated outputs;
- executable files without owners;
- large committed files;
- duplicate configuration;
- broken relative links;
- unsupported feature names;
- files not included in any build, test, docs, release, or archive index.

A file should remain only when its purpose and owner are discoverable.

### OPS-10: Make clean-checkout contribution routine — Medium

From a clean checkout, document and test:

```text
just setup
just fmt
just lint
just test
just docs
just check
```

Requirements:

- no sibling repositories;
- no private dependencies;
- no undocumented services for default tests;
- clear optional service setup;
- consistent toolchain pinning;
- actionable failures;
- contributor guide matching CI.

### OPS-11: Measure simplification outcomes — Small

Compare with the v0.48 baseline:

- source lines;
- dependencies;
- Cargo features;
- binary size;
- image size;
- build duration;
- CI duration;
- test count and test quality;
- docs pages;
- supported configuration variants;
- security advisories;
- ownership coverage.

Publish the results in the release notes without treating deletion count as a goal by itself.

## 14.4 Acceptance criteria

- [ ] One canonical operator flow is documented.
- [ ] Supported failures have stable codes and actionable guidance.
- [ ] Frozen CLI JSON remains compatible.
- [ ] Primary documentation contains only the supported v1 product.
- [ ] Every supported Quick Start and operational example runs in CI.
- [ ] Duplicate and conflicting documentation is consolidated.
- [ ] Every supported runbook has been exercised by someone other than its author.
- [ ] Support bundles pass redaction tests.
- [ ] Active plans, completed plans, research, ADRs, and current documentation are clearly separated.
- [ ] Unused scripts, dependencies, schemas, fixtures, and files are removed.
- [ ] A clean checkout can build, test, and render documentation using public instructions.
- [ ] Simplification results are measured against the v0.48 baseline.

## 14.5 Explicit non-goals

- New operator UI
- Breaking frozen machine-readable output for cosmetic consistency
- Keeping every historical planning document
- Moving obsolete source code into an archive directory
- Adding more tutorials than the supported product needs

---

# 15. v0.55.0 — Independent Validation and Release Readiness

## 15.1 Objective

Validate the exact candidate product through external pilots, independent review, long-run testing, ownership checks, and complete release rehearsal.

## 15.2 User promise

> The product has been operated, failed, recovered, upgraded, reviewed, and released by people other than its primary author.

## 15.3 Required work

### REL-1: Build immutable candidate artifacts — Small

Record:

- candidate commit;
- source tag;
- binary digests;
- extension package digests;
- image digests;
- chart digest;
- SBOM digest;
- provenance digest;
- contract artifact digests;
- documentation artifact digest.

Every pilot and review must name the same candidate.

### REL-2: Complete the four production-pilot profiles — Medium per profile

Required profiles:

1. PostgreSQL outbox to NATS JetStream
2. PostgreSQL outbox to Kafka
3. PostgreSQL outbox to PostgreSQL inbox
4. PostgreSQL outbox to HTTPS webhook

Every pilot must complete:

- installation from candidate artifacts;
- configuration through public APIs;
- steady-state delivery;
- latency and resource observation;
- planned destination failure;
- relay restart;
- HA transfer where applicable;
- duplicate observation;
- operator diagnosis;
- upgrade from the supported floor;
- relay rollback;
- runbook use;
- operator sign-off.

Every finding becomes an issue.

P0 and P1 findings block the release. P2 and P3 findings may remain only when the documented contract stays true and the release manager records the disposition.

### REL-3: Complete five independent reviews — Medium

Required disciplines:

- PostgreSQL extension and migrations
- Rust async and concurrency
- Delivery semantics
- Security
- Operations

Review records must include:

- reviewer;
- scope;
- candidate commit;
- artifact digests;
- date;
- findings;
- disposition;
- approval or rejection;
- reapproval after material changes.

The primary author may not approve their own discipline.

### REL-4: Reduce single-owner risk — Medium

Before release readiness:

- At least two people can approve core-path changes.
- At least two people can execute the release checklist.
- Supported connector ownership is not purely nominal.
- Security reporting has a monitored backup contact.
- Release credentials and signing procedures have documented succession.
- A non-primary maintainer performs one complete release rehearsal.

Update CODEOWNERS to reflect real review responsibility.

### REL-5: Complete the final long-run qualification — Medium

Run the 72-hour soak using candidate artifacts and the reference environment.

During the run, exercise:

- destination outage;
- relay restart;
- HA transfer;
- PostgreSQL failover where supported;
- retry bursts;
- DLQ creation and replay;
- cleanup;
- backlog recovery;
- metrics and alerting;
- support-bundle capture.

No unexplained resource growth or contract violation may remain.

### REL-6: Rehearse the full release process — Medium

A release manager other than the primary implementation author must perform:

1. Clean clone
2. Contract checks
3. Required test matrix
4. Upgrade and rollback matrix
5. Security gates
6. Benchmark and soak validation
7. Artifact build
8. SBOM and provenance
9. Signature
10. Independent verification
11. Documentation publication
12. Changelog extraction
13. Evidence finalization
14. Draft release
15. Rollback or abort procedure

Record every undocumented step as a defect.

### REL-7: Enforce the zero-blocker gate — Small

The release workflow must fail when:

- any open `priority/P0` or `priority/P1` issue exists;
- a pilot references an unresolved blocker;
- a review references an unresolved blocker;
- evidence records are incomplete;
- the candidate commit differs across evidence;
- artifact digests are missing;
- the release manager has not approved;
- a required test is skipped;
- the final soak has not passed.

### REL-8: Publish the final v1 scope and limitations — Small

The final scope document must clearly state:

- what is supported;
- what is diagnostic;
- what was removed;
- what moved to Labs;
- what remains deliberately unsupported;
- delivery and duplicate guarantees;
- platform and service-version support;
- upgrade floor;
- deprecation rules;
- security-reporting process;
- known P2/P3 limitations;
- post-v1 roadmap boundary.

## 15.4 Acceptance criteria

- [ ] Candidate commit and all artifact digests are fixed.
- [ ] All four production pilots are complete.
- [ ] Every pilot has operator sign-off.
- [ ] All five independent review disciplines approve the candidate.
- [ ] No P0 or P1 issue remains.
- [ ] The 72-hour candidate soak passes.
- [ ] A non-primary maintainer completes the release rehearsal.
- [ ] At least two people can approve and execute a release.
- [ ] Contract, security, lifecycle, performance, documentation, and artifact evidence refer to the same candidate.
- [ ] Final scope and limitations are published.
- [ ] The repository is ready to enter a blocker-fixes-only release-candidate series.

## 15.5 Explicit non-goals

- Solving P3 polish before the candidate series
- Adding functionality discovered during pilots
- Broadening support to accommodate one-off pilot configurations
- Approving reviews against different commits
- Treating a maintainer-operated demo as an external pilot

---

# 16. v1.0.0 Release-Candidate Series

## 16.1 Candidate rules

The first candidate is cut only after v0.55.0 passes.

Allowed changes after `v1.0.0-rc.1`:

- P0 fixes
- P1 fixes
- Security fixes
- Correctness fixes required to preserve the documented contract
- Documentation corrections required to prevent unsafe operation
- Release-engineering corrections

Not allowed:

- New connectors
- New source types
- New wire formats
- New public commands
- New configuration systems
- New orchestration
- Large refactors
- Opportunistic optimization
- Scope expansion

## 16.2 Evidence invalidation

Any candidate change affecting the following requires targeted reruns and reapproval:

| Changed area | Required renewed evidence |
|---|---|
| SQL, migrations, privileges | PostgreSQL review, upgrade matrix, affected pilots |
| Checkpoint, retry, HA, shutdown | Delivery and concurrency review, crash tests, affected pilots |
| Connector implementation | Connector E2E, duplicate tests, affected pilot |
| Networking or secrets | Security review and affected pilot |
| Metrics, health, CLI JSON | Contract checks and operations review |
| Artifact composition or dependency graph | Security, SBOM, provenance, artifact verification |
| Performance-critical path | Relevant benchmarks and soak |
| Runbook or operational flow | Operations review and drill |

Documentation typo fixes that do not change meaning do not invalidate unrelated evidence.

## 16.3 Candidate progression

- `rc.1` begins the public candidate cycle.
- Any code change requires a new candidate.
- A candidate that receives no code changes may proceed after all gates complete.
- The final GA tag must point to the exact final candidate commit.
- GA artifacts must be either the already validated candidate artifacts or reproducible artifacts whose digests and provenance are independently revalidated.

---

# 17. v1.0.0 Final Exit Gate

v1.0.0 may be released only when all of the following are true.

## Product scope

- [ ] The supported product consists of the PostgreSQL outbox, PostgreSQL inbox, NATS, Kafka, webhook, and required operational infrastructure.
- [ ] Experimental integrations are absent from production profiles and primary documentation.
- [ ] Removed configurations fail explicitly.
- [ ] README, package metadata, documentation, support policy, and release artifacts state the same product promise.

## Correctness

- [ ] All supported destinations have public-API black-box tests.
- [ ] Every defined crash window preserves the no-silent-loss invariant.
- [ ] Checkpoints advance only after the documented acknowledgment boundary.
- [ ] Checkpoints are monotonic.
- [ ] Cleanup cannot remove events needed by an active supported consumer.
- [ ] Duplicate behavior is documented and tested.
- [ ] DLQ failures cannot silently discard events.
- [ ] HA takeover preserves delivery invariants.

## Testing

- [ ] The complete extension and relay suites run in reproducible environments.
- [ ] No required test is environment-blocked.
- [ ] No required test is quarantined.
- [ ] No release depends on rerunning flaky tests.
- [ ] Fuzz regressions are committed.
- [ ] Test names truthfully describe test level.

## Lifecycle

- [ ] Fresh install passes.
- [ ] Upgrade from the supported floor passes.
- [ ] Fresh-install and upgraded schemas are equivalent.
- [ ] Relay rollback passes.
- [ ] Extension rollback behavior is explicit.
- [ ] Supported mixed-version operation passes.
- [ ] Backup and restore pass.
- [ ] Interrupted migrations recover safely.
- [ ] Helm and CloudNativePG lifecycle tests pass.

## Security

- [ ] Privilege boundaries pass positive and negative tests.
- [ ] Security-sensitive failures fail closed.
- [ ] TLS and authentication behavior match policy.
- [ ] Webhook SSRF protections pass adversarial tests.
- [ ] Secret canaries remain absent from all output.
- [ ] Production dependency and license gates pass.
- [ ] No unapproved critical or high vulnerability remains.
- [ ] Production artifacts match the minimal allowlist.
- [ ] Signing, SBOM, and provenance are independently verified.

## Performance and operations

- [ ] v1 operational budgets are committed.
- [ ] Reference benchmarks pass.
- [ ] The final 72-hour soak passes.
- [ ] No unbounded memory, task, connection, descriptor, metric-label, or storage growth remains.
- [ ] Capacity-planning guidance is published.
- [ ] Every supported runbook has been exercised.
- [ ] Support bundles pass redaction checks.
- [ ] Alerts and dashboards reference emitted metrics and documented thresholds.

## Documentation and repository quality

- [ ] Supported examples run against packaged artifacts.
- [ ] Primary navigation contains the supported product.
- [ ] Duplicate and obsolete documentation is removed.
- [ ] Active plans and historical documents are clearly separated.
- [ ] Unused scripts, dependencies, fixtures, schemas, and files are removed.
- [ ] A clean public checkout can build, test, and render documentation.

## Independent assurance

- [ ] Four production pilots are complete.
- [ ] Five independent review disciplines approve the final candidate.
- [ ] At least two people can approve a release.
- [ ] A non-primary maintainer has executed the release process.
- [ ] No P0 or P1 issue remains.
- [ ] Every release claim links to evidence for the exact candidate.
- [ ] The release manager approves the final evidence index.

---

# 18. CI Structure

## 18.1 Pull-request CI

Required on every core-path pull request:

- Format
- Clippy and lint
- Unit tests
- Extension clean-room tests
- Contract drift
- Required-test inventory
- Core public Quick Start
- Focused connector tests
- Migration checks
- Documentation snippets
- Security lints
- Dependency graph policy
- Repository hygiene
- Removed-surface regression check

## 18.2 Scheduled CI

- Full four-destination integration matrix
- Crash and failpoint matrix
- Property tests
- Fuzzing
- Reference benchmarks
- Short soak
- Resource-growth checks
- Dependency and license audit
- Container-content audit
- Documentation link and example checks

## 18.3 Release CI

- Clean-room build
- Full required test matrix
- Fresh install
- Upgrade matrix
- Rollback matrix
- Mixed-version matrix
- Backup and restore
- Helm lifecycle
- CloudNativePG lifecycle
- Long soak result
- Zero-blocker query
- Pilot and review evidence
- Minimal artifact check
- SBOM
- Provenance
- Signing
- Independent verification
- Release evidence finalization

---

# 19. Milestones and Labels

Create one milestone for each release:

```text
v0.48.0
v0.49.0
v0.50.0
v0.51.0
v0.52.0
v0.53.0
v0.54.0
v0.55.0
v1.0.0-rc
v1.0.0
```

Recommended labels:

```text
priority/P0
priority/P1
priority/P2
priority/P3

area/extension
area/relay-core
area/connector
area/security
area/migrations
area/docs
area/observability
area/performance
area/release
area/repository

type/bug
type/design
type/test-gap
type/removal
type/cleanup
type/performance
type/documentation
type/review

contract/frozen
contract/preview
contract/experimental
contract/internal

evidence/pilot
evidence/review
evidence/benchmark
evidence/release

release-blocking
post-v1
```

Avoid synonymous labels that divide the same issue class.

---

# 20. Initial Issue Creation Order

Open these issues first, in this order:

1. **Record the truthful v0.47.0 release-evidence status**
2. **Create the clean PostgreSQL 18/pgrx extension-test environment**
3. **Add the required-test manifest and flake policy**
4. **Capture the pre-v1 dependency, artifact, CI, and benchmark baseline**
5. **Approve the machine-readable v1 surface disposition**
6. **Create the pre-removal experimental-surface tag**
7. **Remove `experimental-full` from the production release model**
8. **Remove unsupported messaging and queue connectors**
9. **Remove warehouse, database, and lake integrations**
10. **Remove ETL, notification, alternate wire-format, and inbound surfaces**
11. **Simplify the pipeline schema and remove dead dependencies**
12. **Refocus README, package metadata, and documentation navigation**
13. **Add public PostgreSQL-inbox end-to-end coverage**
14. **Add public HTTPS-webhook end-to-end coverage**
15. **Implement the executable delivery state-machine model**
16. **Implement the connector crash-window matrix**
17. **Add checkpoint property tests**
18. **Declare the supported v1 upgrade floor**
19. **Add fresh-install versus upgrade catalog parity**
20. **Add mixed-version, rollback, and interrupted-migration tests**
21. **Refresh the reduced-product threat model**
22. **Complete the privilege, SSRF, TLS, and secret test matrices**
23. **Commit v1 operational budgets**
24. **Add resource-growth and long-soak qualification**
25. **Reorganize and execute supported documentation**
26. **Tidy plans, scripts, fixtures, dependencies, and repository structure**
27. **Run all four production pilots**
28. **Complete the five independent reviews**
29. **Have a non-primary maintainer execute the full release rehearsal**
30. **Cut `v1.0.0-rc.1`**

The critical path begins with issues 1–7. Deep correctness, security, performance, and documentation work should not be spent on code that v0.49.0 will remove.

---

# 21. Success Measures

The program is successful when:

## Scope

- The production dependency graph and artifact size are materially smaller than at v0.48.
- The main repository no longer presents itself as a broad integration platform.
- Every retained public surface has an owner and evidence.

## Correctness

- Every supported claim has executable proof.
- Every important failure boundary has a test.
- No silent-loss defect remains.
- No misleading end-to-end test remains.

## Reliability

- All supported lifecycle operations are automated.
- No required test is environment-blocked or flaky.
- Long-running operation demonstrates bounded resources.

## Security

- The production graph passes dependency and license policy.
- Every supported HTTP path passes SSRF testing.
- Every security-sensitive failure fails closed.
- Artifacts are signed and independently verifiable.

## Operability

- Current documentation matches packaged artifacts.
- Every supported failure has an exercised runbook.
- Operators can diagnose through public interfaces.
- Upgrade and rollback do not require direct catalog edits.

## Governance

- At least two people can approve and execute a release.
- Every supported connector has real ownership.
- Reviews and pilots identify the exact candidate.
- Release claims link to exact evidence.

---

# 22. Final Direction

The strongest v1 is not the version with the largest connector registry.

It is the version where an operator can say:

> We publish an event in the same PostgreSQL transaction. pg_tide delivers it through a small, observable relay. We know exactly when it retries, when a duplicate can occur, when a checkpoint advances, how failover behaves, how to recover, and how to upgrade. The production artifact contains only the supported product. The documentation matches the binary, and the release was independently reviewed.

The pre-v1 program should therefore measure progress by:

- fewer unsupported surfaces;
- fewer dependencies;
- fewer ambiguous states;
- fewer undocumented operations;
- stronger failure evidence;
- stronger lifecycle evidence;
- stronger independent ownership;
- and a smaller, clearer production promise.

A focused, well-proved v1 is more valuable than a sprawling v1 whose reliability depends on which feature a user happens to select.