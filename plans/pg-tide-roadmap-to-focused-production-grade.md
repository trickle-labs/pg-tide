# pg_tide: Roadmap to a Focused, Production-Grade Product

**Status:** Proposed  
**Starting point:** v0.39.0 and current `main`  
**Purpose:** Turn pg_tide from an impressive, fast-moving prototype into a small, trustworthy, well-designed product that operators can safely depend on.

---

## 1. The Goal

pg_tide should become the most trustworthy PostgreSQL-native way to do this:

```text
Application transaction
        ↓
PostgreSQL transactional outbox
        ↓
pg-tide relay
        ↓
NATS, Kafka, webhook, or another PostgreSQL inbox
```

The product promise should be easy to repeat:

> **Publish an event inside your PostgreSQL transaction, then deliver it reliably through a small, observable relay.**

That promise is valuable. It is also large enough to build a successful product around. pg_tide does not need thirty production-grade connectors, a data-lake platform, a workflow engine, a KMS product, and a CDC framework on day one.

The roadmap below deliberately trades feature count for trust.

---

## 2. The Product Decision

### Production-supported core

The first production-grade product should support:

- Transactional outbox publishing through the public `tide.*` SQL API
- Idempotent inbox delivery
- One canonical outbox storage model
- Durable, monotonic relay offsets
- At-least-once relay transport with clearly documented deduplication boundaries
- High availability through PostgreSQL advisory locks
- Dead-letter handling and replay
- Prometheus metrics, health endpoints, structured logs, and practical alerts
- A small set of deeply tested connectors:
  - PostgreSQL inbox
  - NATS JetStream
  - Apache Kafka
  - HTTPS webhook
- Native pg_tide JSON and CloudEvents wire formats
- Docker, signed release artifacts, Helm, and a CloudNativePG deployment path

`stdout` and `file` may remain as development and diagnostic sinks, but they should not be presented as production integrations.

### Preview features

These may remain in the repository, but should be labeled **Preview** until they meet the same evidence standard as the core:

- Consumer groups beyond the main relay path
- Fan-in pipelines
- Pipeline dependency DAGs
- Managed backfill
- Table partitioning and live conversion
- Debezium encoding and decoding
- Replay workbench extensions
- Multi-tenant provisioning helpers

### Experimental features

These should be clearly separated from the supported product and excluded from headline claims until independently promoted:

- Analytics and warehouse connectors
- Notification connectors
- Singer, Airbyte, and Fivetran integrations
- DuckLake and RockLake integrations
- Cloud KMS providers
- WAL logical-replication source
- Broad “any source to any sink” reverse-pipeline combinations

Experimental does not mean bad. It means:

> “Useful for evaluation, but not yet covered by the product’s compatibility, security, and reliability guarantees.”

---

## 3. Non-Negotiable Engineering Principles

Every roadmap version must follow these rules.

### 3.1 One source of truth

There must be one authoritative answer for each of these questions:

- Where is an outbox message stored?
- How does the relay poll it?
- When is a delivery considered successful?
- When may an offset advance?
- When may a message be deleted?
- Which API is current?
- Which connectors are supported?

If the extension, relay, tests, and documentation disagree, the release is not done.

### 3.2 Every public promise gets a black-box test

A supported user workflow must be tested exactly as a user performs it:

1. Start a fresh PostgreSQL instance.
2. Install the extension.
3. Use public SQL functions.
4. Start the real relay.
5. Deliver to a real service or faithful emulator.
6. Verify behavior through public interfaces.
7. Restart or crash components at awkward moments.
8. Verify that the documented guarantee still holds.

No manually created hidden tables. No direct catalog inserts unless the public API being tested is itself a catalog API. No test-only bypass of the relay.

### 3.3 Security failures must fail closed

If an ACL, tenant check, TLS check, role lookup, or secret lookup errors, the safe result is refusal—not silent permission.

### 3.4 Feature maturity must be honest

A feature is not “integration-tested” merely because it compiles or returns a `Result`. Test names, README claims, and release notes must describe exactly what was proved.

### 3.5 No feature without an owner

Every supported connector or major subsystem needs:

- A named code owner
- A compatibility matrix
- A test owner
- A security contact
- A documented support boundary

If nobody owns it, it remains experimental.

### 3.6 Documentation is executable

Quick Starts, migration examples, and configuration snippets must run in CI against the version being released.

### 3.7 Release gates beat release dates

A version ships when its acceptance criteria pass. Work that misses the gate moves to a later version rather than weakening the gate.

---

## 4. Definition of “Production-Supported”

A feature or connector may only be called production-supported when all of the following are true:

- [ ] It can be configured entirely through documented public APIs.
- [ ] A black-box integration test covers the happy path.
- [ ] Failure before publish is tested.
- [ ] Failure after publish but before offset commit is tested.
- [ ] Restart recovery is tested.
- [ ] Duplicate delivery behavior is documented and tested.
- [ ] Authentication and TLS behavior are tested.
- [ ] Secrets are redacted from logs and errors.
- [ ] Metrics expose success, failure, retries, and lag.
- [ ] The operator runbook includes diagnosis and recovery.
- [ ] Upgrade and rollback behavior are known.
- [ ] The connector has a named maintainer.
- [ ] There are no tautological or “cannot panic” tests presented as protocol validation.

This checklist should live in the repository as `docs/support/production-supported.md` and be enforced during release review.

---

## 5. Roadmap at a Glance

| Version | Theme | The question it must answer |
|---|---|---|
| **v0.40.0** | One Real Pipeline | Does the documented core workflow actually work end to end? |
| **v0.41.0** | Promise Only What We Prove | Is the product surface focused, honest, and buildable from a clean checkout? |
| **v0.42.0** | Crash-Safe by Construction | Can the relay survive every important failure window without losing data? |
| **v0.43.0** | A Good PostgreSQL Citizen | Can operators understand and control load, bloat, retention, and capacity? |
| **v0.44.0** | Secure by Default | Are privileges, TLS, secrets, dependencies, and network access safe by default? |
| **v0.45.0** | Operators First | Can a normal operator install, observe, upgrade, and recover the system confidently? |
| **v0.46.0** | Four Connectors, Fully Trusted | Are the core connectors genuinely production-supported? |
| **v0.47.0** | Public Beta and API Freeze | Is the product contract ready for external production pilots and independent review? |
| **v1.0.0** | The Trust Release | Has pg_tide earned a stable production promise? |

The next version must not start until the previous version’s exit criteria pass on `main`.

---

# 6. Detailed Release Plan

## v0.40.0 — One Real Pipeline

### Objective

Make one documented outbox-to-sink pipeline unquestionably correct.

This is the most important release in the roadmap. It should contain no new connectors and no headline features. Its purpose is to align the extension, relay, tests, and documentation around one real storage and delivery contract.

### Recommended architecture decision

Keep the existing shared-table outbox model:

```text
tide.tide_outbox_messages
    outbox_name = 'orders'
```

The extension already writes this model, ADR-001 describes it, and it avoids creating a table for every logical outbox. The relay should poll the shared parent table using a parameterized `outbox_name` predicate.

Partitioning should remain an implementation detail beneath this logical contract. The relay should not need to know whether the parent is partitioned.

### Required work

#### A. Establish the canonical storage contract

Create **ADR-011: Canonical Outbox Storage and Relay Polling**.

The ADR must specify:

- The canonical outbox table
- Required indexes
- Ordering rules
- Visibility rules for uncommitted rows
- How `outbox_name` scopes a pipeline
- Offset semantics
- Cleanup semantics
- How partitioning preserves the same logical contract
- How pg_trickle compatibility is handled without changing native pg_tide behavior

#### B. Refactor the relay source

Replace per-outbox dynamic relation polling with a parameterized query similar to:

```sql
SELECT id, payload, headers, created_at
FROM tide.tide_outbox_messages
WHERE outbox_name = $1
  AND id > $2
ORDER BY id
LIMIT $3;
```

Actions:

- Remove native-path assumptions about `tide."outbox_<name>"`.
- Eliminate unnecessary dynamic SQL from the main outbox path.
- Ensure offsets are scoped by relay group, pipeline, and outbox.
- Keep offset writes monotonic.
- Decide whether `consumed_at` is pipeline-specific or global; document the consequence.
- If multiple pipelines may consume the same outbox independently, avoid a single global `consumed_at` flag as the authoritative delivery state.

That final point requires an explicit decision. Independent fan-out pipelines need independent offsets. A row cannot be globally “consumed” merely because one sink received it.

#### C. Add the authoritative end-to-end test

Create a test named something unambiguous, for example:

```text
public_api_outbox_to_nats_e2e
```

It must:

1. Start PostgreSQL 18.
2. Build and install the real extension.
3. Start NATS JetStream.
4. Call `tide.outbox_create_if_not_exists()`.
5. Configure the pipeline with `tide.relay_set_outbox_v2()`.
6. Start the real coordinator.
7. Publish messages with `tide.outbox_publish()` inside business transactions.
8. Confirm receipt from NATS.
9. Confirm the relay offset advanced.
10. Confirm a restart does not lose messages.
11. Confirm a duplicate delivery is identifiable or deduplicated according to the documented contract.

The test may not manually create an outbox table or insert directly into relay catalog tables.

Add equivalent lightweight tests for webhook and PostgreSQL inbox if they can be completed without compromising the release gate. NATS is the mandatory proof path.

#### D. Make ACL checks fail closed

Audit all permission-sensitive calls in `pg-tide-ext` and `pg-tide-relay`.

Replace patterns that turn errors into `None`, `false`, or default permission with explicit errors.

Required tests:

- ACL table unavailable
- Permission query denied
- Role lookup failure
- Unauthorized publisher
- Authorized publisher
- Superuser or extension-owner behavior

#### E. Quarantine fan-in until it uses the canonical path

Either:

- Refactor fan-in to poll the shared table correctly and add a real runtime test, or
- Mark fan-in experimental and prevent it from being advertised or enabled in the production profile.

Do not leave a partially working fan-in path presented as supported.

#### F. Repair the documentation contract

- Replace every removed positional relay API with the v2 JSONB API.
- Update README, Quick Start, tutorials, examples, migration docs, and SQL reference.
- Add a CI job that executes every marked Quick Start SQL block.
- Generate the installed version in examples rather than hard-coding historical versions.
- Remove descriptions of internal behavior that are no longer true.

#### G. Add the v0.39.0 upgrade test

Prove both paths:

- Fresh install of v0.40.0
- Upgrade from v0.39.0 to v0.40.0

The resulting schema and public behavior must match.

### Explicit non-goals

- New connectors
- New wire formats
- New data-lake features
- New KMS providers
- UI work
- Broad PostgreSQL version support

### Exit criteria

- [ ] The public Quick Start passes in CI without private setup.
- [ ] The relay polls the same logical storage model written by `outbox_publish()`.
- [ ] The main outbox path contains no user-controlled dynamic table interpolation.
- [ ] NATS end-to-end delivery passes through public APIs.
- [ ] Crash-and-restart recovery passes.
- [ ] ACL errors fail closed.
- [ ] Removed positional APIs no longer appear in current documentation.
- [ ] Fan-in is either fixed and tested or clearly disabled as experimental.
- [ ] Fresh install and v0.39.0 upgrade produce equivalent behavior.

---

## v0.41.0 — Promise Only What We Prove

### Objective

Turn the repository into a focused product with an honest support surface.

### Required work

#### A. Introduce maturity tiers

Create a machine-readable registry, for example `connectors.toml`, containing:

```toml
[name]
maturity = "supported" # supported | preview | experimental
owner = "github-handle"
source = true
sink = true
default_build = false
integration_test = "path/to/test"
auth_tested = true
tls_tested = true
restart_tested = true
```

Generate these from the registry:

- README connector table
- Documentation compatibility matrix
- Build profiles
- Release checklist

#### B. Reduce the default and release profiles

Recommended profiles:

- `core`: PostgreSQL inbox, NATS, webhook, stdout/file diagnostics
- `core-kafka`: core plus Kafka
- `experimental-full`: everything that compiles

The normal Docker image should contain supported features only. The “full” image must be renamed or clearly labeled experimental until every included connector meets the support checklist.

#### C. Make the workspace self-contained

Remove the required sibling path dependency on `../../rocklake2`.

Acceptable solutions:

- Publish and pin a crate version.
- Use a pinned Git dependency for experimental tests.
- Move the test kit into the workspace.
- Move RockLake integration tests into a separate repository or optional CI workflow.

A fresh clone must support:

```bash
cargo build --workspace
cargo test --workspace
```

without manually constructing sibling directories.

#### D. Audit test truthfulness

Review every CI job and integration test name.

Examples:

- Rename database insert throughput tests so they do not say “end to end.”
- Replace tautological `result.is_ok() || result.is_err()` assertions.
- Mark tests that only validate request construction as unit or contract tests.
- Require real protocol outcomes before calling a connector integration-tested.

Create a short standard:

```text
unit test        — one function or module
contract test    — request/response shape against a local boundary
integration test — two real components interact
end-to-end test  — public user API through final observable outcome
chaos test       — induced failure with invariant verification
```

#### E. Archive planning history

Move old assessment and implementation-plan documents into a clearly labeled archive. Keep current product docs separate from historical plans.

Recommended structure:

```text
docs/
  product/
  operations/
  reference/
  support/
  architecture/
  archive/
```

#### F. Publish a compatibility policy

Document:

- Supported PostgreSQL versions
- Supported architectures
- Supported deployment modes
- Supported connector versions
- What “best effort” means for experimental features
- How long patch releases are maintained

Also complete a PostgreSQL 17 support feasibility assessment. Support it if the extension can do so without weakening the test matrix. Otherwise document the exact PostgreSQL 18 dependency.

### Exit criteria

- [ ] A clean checkout builds without sibling repositories.
- [ ] Every connector has a maturity label and owner.
- [ ] README claims are generated from the maturity registry.
- [ ] Production images contain only supported features.
- [ ] No CI job or test name overstates what it validates.
- [ ] Historical plans are clearly separated from current product documentation.
- [ ] The PostgreSQL support policy is explicit.

---

## v0.42.0 — Crash-Safe by Construction

### Objective

Prove that the relay cannot silently lose a committed outbox event across the important failure windows.

### Required work

#### A. Write the relay delivery state machine

Create **ADR-012: Relay Delivery, Acknowledgment, and Offset State Machine**.

Define states such as:

```text
Polled
  → Encoded
  → Published
  → SinkAcknowledged
  → OffsetCommitted
  → EligibleForCleanup
```

For every transition, define:

- Durable state
- Retry behavior
- Duplicate risk
- Data-loss risk
- Required metric
- Required log event
- Required test

#### B. Add controlled failpoints

Introduce test-only failpoints at these boundaries:

- After poll, before encode
- After encode, before publish
- After network publish, before sink acknowledgment handling
- After sink acknowledgment, before offset commit
- After offset commit, before worker state update
- During DLQ write
- During advisory-lock connection loss
- During shutdown

Failpoints should terminate or interrupt the worker in a controlled way so tests can restart the real coordinator and verify invariants.

#### C. Build the failure matrix

For every supported connector, test at minimum:

| Failure point | Expected result |
|---|---|
| Before publish | Message is retried; no downstream copy exists |
| During publish | Message may be retried; no silent offset advance |
| After downstream success, before offset commit | Duplicate may occur; message is not lost |
| After offset commit | Message is not replayed unless explicitly rewound |
| Relay process crash | Another instance or restart resumes safely |
| PostgreSQL connection loss | Advisory lock releases and ownership transfers safely |
| DLQ write failure | Pipeline pauses or fails visibly; message is not discarded |

#### D. Make offsets monotonic everywhere

Apply monotonic guards to:

- Simple pipelines
- Consumer groups
- Fan-in member offsets
- Replay and rewind APIs

Rewinds must require an explicit administrative override and leave an audit record.

#### E. Verify HA with real ownership transfer

Test two relay instances against one PostgreSQL database:

- Only one owns a pipeline.
- The active instance is killed.
- The standby acquires ownership.
- No event is lost.
- Any duplicate is handled according to the connector contract.
- Metrics and logs make the transition visible.

#### F. Define the exactly-once language

Use these terms consistently:

- **Atomic outbox write**: exactly once within the PostgreSQL transaction
- **Relay transport**: at least once
- **Application outcome**: effectively exactly once only when the destination deduplicates by a stable event ID

Do not use unqualified “exactly once” in headlines.

### Exit criteria

- [ ] The state-machine ADR is approved.
- [ ] Every supported connector passes the crash matrix.
- [ ] Offset writes are monotonic by default.
- [ ] Administrative rewind is explicit and audited.
- [ ] HA takeover passes with no silent loss.
- [ ] DLQ failure is visible and fail-safe.
- [ ] Documentation uses delivery terminology consistently.

---

## v0.43.0 — A Good PostgreSQL Citizen

### Objective

Make pg_tide predictable under sustained load and safe for the PostgreSQL instance hosting application data.

### Required work

#### A. Replace vanity benchmarks with operational benchmarks

Measure the real system:

- `outbox_publish()` latency at p50, p95, and p99
- Relay end-to-end throughput
- End-to-end delivery latency
- CPU and memory per relay worker
- PostgreSQL WAL volume
- Table and index growth
- Vacuum behavior
- Retention sweep cost
- Sink outage recovery rate
- Backpressure behavior
- HA failover interruption

Keep raw insert throughput as a separate database microbenchmark.

#### B. Establish performance budgets

After collecting a baseline, define budgets for:

- Maximum acceptable publish overhead
- Maximum relay memory per in-flight message
- Maximum catalog polling frequency
- Maximum offset-write frequency
- Maximum recovery lag after a sink outage
- Maximum bloat under a representative retention policy

A release fails when it exceeds a budget without an approved explanation.

#### C. Harden retention and cleanup

- Ensure cleanup never removes messages still required by any active pipeline.
- Make retention pipeline-aware when multiple sinks consume one outbox.
- Expose cleanup progress and failures.
- Add safe batch limits.
- Document required vacuum and autovacuum settings.
- Add a dry-run mode for destructive maintenance.

#### D. Review partitioning from first principles

Partitioning must preserve the canonical shared-table contract.

Validate:

- New-message routing
- Index pruning
- Offset polling across partitions
- Partition creation ahead of time
- Safe partition removal
- Live conversion rollback
- Multiple outboxes sharing the parent
- Long-running transactions

Do not present live conversion as production-supported until failure and rollback are tested.

#### E. Add sustained and soak testing

Nightly or scheduled tests should include:

- Sustained publish and relay load
- Sink outage followed by recovery
- Repeated relay restarts
- Repeated PostgreSQL connection drops
- Retention sweeps during active delivery
- Memory-leak detection
- Catalog reload churn

#### F. Publish a capacity guide

The guide should answer:

- How many messages per second can one relay handle?
- How many pipelines can one relay own?
- How large may messages be?
- When should claim-check be used?
- How much disk should retention reserve?
- What indexes are required?
- How should autovacuum be tuned?
- When should outboxes be partitioned?

### Exit criteria

- [ ] Real end-to-end benchmarks replace misleading throughput claims.
- [ ] Performance budgets are checked in CI or scheduled tests.
- [ ] Cleanup is safe with multiple pipelines per outbox.
- [ ] Partitioning preserves the canonical storage contract.
- [ ] Scheduled soak tests run without unbounded memory or storage growth.
- [ ] A practical capacity and vacuum guide is published.

---

## v0.44.0 — Secure by Default

### Objective

Make the safe configuration the easiest configuration.

### Required work

#### A. Publish a threat model

Cover:

- Malicious database roles
- Compromised connector credentials
- SSRF through HTTP-based connectors
- Secret leakage through logs or status output
- Dynamic SQL and identifier injection
- Cross-tenant access
- Dependency compromise
- Untrusted message payloads
- Resource exhaustion
- Unauthorized offset rewind or replay

For each threat, document prevention, detection, and recovery.

#### B. Create a privilege model

Define separate roles such as:

- `tide_admin`
- `tide_publisher`
- `tide_relay`
- `tide_operator`
- `tide_reader`

Provide idempotent provisioning SQL and test that each role can perform only its intended actions.

#### C. Enforce fail-closed authorization

Audit every use of:

- `unwrap_or_default`
- `unwrap_or(false)`
- ignored SPI errors
- ignored GRANT failures
- best-effort tenant filtering

Security-sensitive failures must stop the operation.

#### D. Harden network clients

- Require verified TLS for production profiles.
- Make insecure HTTP or certificate bypass explicit and noisy.
- Apply the shared SSRF policy to every HTTP-capable connector.
- Test redirects, DNS rebinding-sensitive cases, loopback, link-local, private ranges, and metadata endpoints.
- Document proxy behavior.

#### E. Harden secrets

- Support environment and file references with strict permission checks.
- Never expose resolved secrets in logs, metrics, status output, or config history.
- Add structured secret types so accidental formatting is difficult.
- Test error paths for redaction.

#### F. Reduce dependency risk

- Re-evaluate every ignored advisory.
- Remove unsupported connectors that force vulnerable or abandoned dependency chains into production profiles.
- Generate SBOMs for every image and binary.
- Publish build provenance.
- Pin toolchain and release action versions.
- Add a documented dependency-update policy.

#### G. Clarify encryption support

For v1 scope, choose one of these:

1. Support `LocalKeyFile` only and label cloud providers experimental, or
2. Fully implement and test selected cloud KMS providers.

Do not advertise providers that return `NotImplemented` as production features.

### Exit criteria

- [ ] Threat model and privilege matrix are published.
- [ ] Security-sensitive errors fail closed.
- [ ] Supported network connectors verify TLS by default.
- [ ] SSRF tests cover every supported HTTP path.
- [ ] Secret-redaction tests cover success and failure paths.
- [ ] Production profiles pass dependency audit without unjustified ignores.
- [ ] Encryption claims match implemented provider behavior.

---

## v0.45.0 — Operators First

### Objective

Make pg_tide pleasant to install, inspect, upgrade, and recover.

### Required work

#### A. Simplify the CLI

The main operator flow should be obvious:

```bash
pg-tide doctor
pg-tide status
pg-tide config validate
pg-tide run
pg-tide replay ...
pg-tide maintenance sweep ...
```

Requirements:

- Every command supports stable JSON output where automation needs it.
- Exit codes are documented and tested.
- Errors include the failed component, likely cause, and next action.
- `doctor` checks extension version, privileges, TLS, schemas, advisory locks, configured connectors, and migration health.
- `status` shows ownership, lag, last success, last error, retry state, and DLQ depth.

#### B. Make configuration understandable

- Catalog configuration is the authoritative pipeline source.
- Process-level TOML contains only process settings.
- Add `pg-tide config export` and `pg-tide config validate`.
- Add JSON Schema for pipeline configurations.
- Validate unsupported keys and feature-gated connectors before worker startup.
- Never silently ignore misspelled keys.

#### C. Make observability task-oriented

Metrics and dashboards should answer:

- Is the relay healthy?
- Which pipelines are behind?
- Are messages failing permanently or transiently?
- Is PostgreSQL becoming the bottleneck?
- Is a sink becoming slower?
- Did HA ownership move?
- Is cleanup keeping up?

Ship a small default dashboard. Move specialized experimental panels into separate dashboards.

#### D. Rehearse upgrades and rollback

For every release candidate:

- Fresh install
- Upgrade from v0.39.0
- Upgrade from latest supported minor
- Rollback of the relay binary
- Extension rollback where supported
- Mixed-version rolling relay deployment
- CloudNativePG upgrade example

Document which migrations are reversible and which are not.

#### E. Publish operational runbooks

At minimum:

- Relay will not start
- Pipeline is not discovered
- Pipeline has lag
- Sink authentication failure
- DLQ is growing
- Advisory lock is stuck or ownership is unclear
- PostgreSQL failover occurred
- Retention is not cleaning up
- Disk usage is growing
- Upgrade failed
- Duplicate messages are observed

### Exit criteria

- [ ] Common operations require no direct catalog edits.
- [ ] CLI JSON output and exit codes are stable and tested.
- [ ] Unknown configuration keys fail validation.
- [ ] Default dashboards answer the primary operational questions.
- [ ] Upgrade paths are exercised automatically.
- [ ] Recovery runbooks are complete and tested during release review.

---

## v0.46.0 — Four Connectors, Fully Trusted

### Objective

Promote four connectors based on evidence, not implementation count.

### Supported connector candidates

1. PostgreSQL inbox
2. NATS JetStream
3. Apache Kafka
4. HTTPS webhook

### Required work for each connector

#### A. Protocol-level integration tests

Use real services or faithful emulators. Verify the actual downstream outcome rather than merely constructing a request.

#### B. Capability contract

Each connector must declare:

- Maximum batch size
- Ordering guarantee
- Acknowledgment boundary
- Deduplication support
- Retryable error classes
- Permanent error classes
- TLS behavior
- Authentication modes
- Message-size limits
- Backpressure behavior
- Shutdown behavior

Represent these capabilities in code rather than only prose where possible.

#### C. Connector-specific failure tests

**PostgreSQL inbox**

- Unique event-ID deduplication
- Transaction rollback
- Destination failover
- Permission denial
- Schema mismatch

**NATS JetStream**

- Publish acknowledgment
- Duplicate message ID behavior
- Stream unavailable
- Reconnect and recovery
- Subject validation

**Kafka**

- Producer acknowledgment settings
- Idempotent producer configuration where applicable
- Broker unavailability
- Topic authorization failure
- Message-too-large behavior
- Ordering within a partition

**Webhook**

- Success status policy
- Retry status policy
- Timeout
- TLS failure
- HMAC signing
- SSRF rejection
- Idempotency-key header
- Redirect handling

#### D. Compatibility matrix

Publish tested versions of PostgreSQL, NATS, Kafka, and relevant TLS libraries. CI should test at least the minimum and current recommended versions.

#### E. Connector support runbooks

Each supported connector needs a concise diagnosis guide and examples that run in CI.

### Exit criteria

- [ ] All four connectors satisfy the production-supported checklist.
- [ ] Connector capability contracts are documented and represented in code.
- [ ] Protocol-level outcomes are verified.
- [ ] Connector failure matrices pass.
- [ ] Compatibility versions are explicit.
- [ ] No other connector is described as production-supported.

---

## v0.47.0 — Public Beta and API Freeze

### Objective

Stop designing in public and start validating the product with real users.

This release should contain almost no new functionality.

### Required work

#### A. Freeze the v1 contract

Freeze:

- Public SQL function signatures
- Pipeline JSON schema
- Core metric names
- Health endpoint behavior
- CLI machine-readable output
- Event envelope fields
- Connector support matrix

Any remaining breaking change must happen before this release exits.

#### B. Run production pilots

Select a small number of external pilot deployments with different profiles:

- Moderate event volume to NATS
- Kafka delivery with strict operational requirements
- PostgreSQL-to-PostgreSQL inbox delivery
- Webhook delivery with failure and retry requirements

Collect:

- Installation friction
- Publish latency
- Delivery latency
- Operational incidents
- Upgrade experience
- Documentation gaps
- Resource consumption
- Duplicate behavior
- Failure recovery experience

Pilot feedback becomes tracked issues, not private anecdotes.

#### C. Require independent review

The core path should receive review from people other than the primary author, including:

- PostgreSQL extension expertise
- Rust async/concurrency expertise
- Distributed delivery semantics expertise
- Security review
- Operator review

#### D. Publish support and governance policies

Add:

- `SECURITY.md`
- `SUPPORT.md`
- `GOVERNANCE.md`
- `CODEOWNERS`
- Release manager checklist
- Vulnerability disclosure process
- Deprecation policy
- Connector promotion policy

#### E. Resolve all release-blocking debt

No open P0 or P1 issue may be deferred into v1.0.0.

### Exit criteria

- [ ] The v1 API and metric contract is frozen.
- [ ] External pilots complete the public core workflow.
- [ ] Independent reviewers approve the core design.
- [ ] Governance, support, and security policies are published.
- [ ] All P0 and P1 issues are closed.
- [ ] Upgrade and rollback are successful in pilot-like environments.

---

## v1.0.0 — The Trust Release

### Product statement

> pg_tide provides a PostgreSQL-native transactional outbox and idempotent inbox, with a small HA relay that reliably delivers events to PostgreSQL, NATS, Kafka, and HTTPS webhooks.

### v1.0.0 must guarantee

- The documented public-API workflow works on every supported platform.
- A committed outbox event is not silently lost by the relay.
- Duplicate risk is clearly documented at each connector boundary.
- Offset advancement is monotonic and occurs only after the connector’s acknowledgment boundary.
- Supported connectors meet the production-support checklist.
- Supported upgrades are automated and tested.
- Security-sensitive failures fail closed.
- Release artifacts are signed, reproducible, and accompanied by an SBOM.
- Operational dashboards, alerts, and runbooks are included.
- At least two people can review and approve a release of the core path.

### Deliberately excluded from the v1 promise

Unless separately promoted through the same gates, v1.0.0 should not promise production support for:

- Thirty connectors
- Arbitrary source-to-sink combinations
- Cloud KMS providers
- RockLake or DuckLake ingestion
- Fan-in and DAG orchestration
- Managed backfill
- WAL logical replication
- Every PostgreSQL major version

A focused v1 is stronger than a sprawling v1.

---

# 7. Project Operating Model

## 7.1 Milestones and issue structure

Create one GitHub milestone per roadmap version.

Every major item should be an epic with child issues. Each issue should contain:

- User-visible problem
- Current behavior
- Desired behavior
- Failure modes
- Public API affected
- Required tests
- Required metrics
- Documentation changes
- Migration impact
- Explicit non-goals

## 7.2 Labels

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

maturity/supported
maturity/preview
maturity/experimental

type/bug
type/design
type/test-gap
type/cleanup
type/feature
```

## 7.3 Pull-request rules

A core-path PR should not merge unless it includes, where applicable:

- Tests at the correct level
- Public documentation updates
- Migration impact
- Metrics or logs for new failure modes
- Security consideration
- Compatibility consideration
- An explanation of why the test would fail before the change

Avoid giant feature releases. Prefer small, reviewable changes that preserve a green black-box core test.

## 7.4 CI tiers

### Pull-request CI

Fast enough to run on every PR:

- Format and lint
- Unit tests
- Extension tests
- Migration tests
- Public Quick Start
- Core NATS black-box test
- Security lints
- Documentation snippets

### Scheduled CI

- Full supported connector matrix
- Chaos and failpoint tests
- Sustained load
- Memory and storage growth
- Multiple PostgreSQL versions when supported
- Dependency audit

### Release CI

- Clean-room build
- Fresh install
- Upgrade matrix
- Rollback rehearsal
- Signed artifacts
- SBOM and provenance
- Container vulnerability scan
- Helm install and upgrade
- CloudNativePG example

## 7.5 Release evidence

Every release note should link each major claim to its evidence:

```text
Claim: public SQL → NATS delivery survives relay restart
Evidence: tests/public_api_nats_e2e.rs::restart_after_publish_before_offset
```

This creates a culture where product claims are backed by executable proof.

---

# 8. The First 15 Issues to Open

These issues turn the roadmap into immediate work.

1. **ADR-011: Choose the canonical outbox storage and polling contract**
2. **Refactor `OutboxPollerSource` to poll the shared outbox table**
3. **Create public-API PostgreSQL → NATS end-to-end test**
4. **Add crash-after-sink-ack-before-offset-commit test**
5. **Define multi-pipeline consumption and cleanup semantics**
6. **Make publisher ACL and tenant checks fail closed**
7. **Replace removed relay API calls in all current documentation**
8. **Execute README and Quick Start SQL in CI**
9. **Quarantine or repair fan-in against the canonical storage model**
10. **Audit and rename misleading integration and load tests**
11. **Replace tautological connector assertions**
12. **Create connector maturity registry and generated support matrix**
13. **Remove the external sibling RockLake path dependency**
14. **Add `CODEOWNERS`, `SUPPORT.md`, and connector ownership policy**
15. **Add clean upgrade test from v0.39.0 to v0.40.0**

The first three issues are the critical path. Everything else waits if they are not green.

---

# 9. Success Metrics

The project should track a small set of trust metrics.

## Product correctness

- 100% of supported Quick Start flows run in CI
- 100% of supported connectors have real black-box integration tests
- Zero open P0 or P1 issues at release time
- Zero tests named “end to end” that bypass the public API
- Zero unsupported APIs in current documentation

## Reliability

- All defined crash windows preserve the no-silent-loss invariant
- HA takeover tests pass consistently
- Offset monotonicity tests cover every offset-writing path
- DLQ failures are visible and do not discard messages

## Security

- All supported profiles pass dependency and license audit
- Every supported HTTP path passes SSRF tests
- Every security-sensitive lookup fails closed
- Secret-redaction tests cover logs, errors, status, and config history

## Operability

- Fresh install and upgrade tests pass for every supported release path
- Core dashboards reference only real, emitted metrics
- Every supported failure mode has a runbook
- A clean checkout builds without private or sibling repositories

## Governance

- At least two release approvers for core changes
- Every supported connector has a named owner
- Release evidence is linked from release notes
- Security reports have a documented response path

---

# 10. What the Team Must Stop Doing

To reach a strong v1, the project should stop:

- Adding connectors before the core path is proven
- Counting a compiling connector as a supported connector
- Calling request-construction tests integration tests
- Calling database insert benchmarks end-to-end relay benchmarks
- Maintaining two contradictory outbox storage models
- Leaving removed APIs in Quick Starts
- Treating swallowed errors as resilience
- Expanding the v1 promise whenever a new idea appears
- Shipping a feature without a long-term owner
- Using roadmap size as a proxy for product maturity

The new measure of progress is simpler:

> **Can a user trust the small set of things we say are supported?**

---

# 11. The End State

A glorious pg_tide is not the project with the most connector logos.

It is the project where an engineer can say:

> “We publish the event in the same PostgreSQL transaction. pg_tide takes it from there. We know exactly when it retries, when it duplicates, when it advances the offset, and how to recover it. The upgrade path is tested. The dashboards tell us what is wrong. The documentation matches the binary.”

That product would be easy to understand, useful to many teams, and difficult to replace.

The path to it is not more ambition.

It is **focus, proof, and disciplined release gates**.