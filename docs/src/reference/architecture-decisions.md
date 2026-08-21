# Architecture Decision Records

This page documents the key architectural decisions made in pg_tide's design, their rationale, and the alternatives that were considered.

## ADR-1: Transactional Outbox over WAL-Based CDC

**Decision:** pg_tide uses the transactional outbox pattern rather than WAL (logical replication) for change capture.

**Context:** Most CDC tools (Debezium, pgoutput) read the PostgreSQL WAL to capture changes. This is transparent to the application but has limitations: it captures all changes (including internal ones), can't easily enrich events with application context, and requires careful management of replication slots.

**Rationale:**
- Applications control exactly what events are published and when
- Events are guaranteed to be published if and only if the business transaction commits
- No replication slot management or WAL retention concerns
- Events can include derived data not present in any single table
- Schema is explicit and application-controlled

**Trade-offs:**
- Applications must explicitly call `tide.outbox_publish()` (not transparent)
- Slightly more application code vs. transparent CDC
- Cannot capture changes from direct SQL or other tools (unless triggers are used)

## ADR-2: PostgreSQL Advisory Locks for HA Coordination

**Decision:** Use `pg_try_advisory_lock()` for pipeline ownership coordination rather than external consensus (etcd, ZooKeeper) or leader election.

**Context:** Multiple relay instances need to coordinate which instance processes which pipeline without double-processing.

**Rationale:**
- Zero additional infrastructure — PostgreSQL is already required
- Advisory locks are automatically released on connection close (crash safety)
- Non-blocking `pg_try_advisory_lock` prevents deadlocks
- Well-understood PostgreSQL primitive with decades of production use

**Trade-offs:**
- Requires all relay instances to connect to the same PostgreSQL instance
- Discovery interval determines failover speed (not instant)
- Lock granularity is per-pipeline (cannot split a single pipeline across instances)

## ADR-3: Pipeline Configuration in PostgreSQL Catalog

**Decision:** Store pipeline configurations in PostgreSQL tables (`tide.relay_outbox_config`, `tide.relay_inbox_config`) rather than in TOML/YAML files or environment variables.

**Context:** The relay needs to know which pipelines to run and how to configure each one.

**Rationale:**
- Configuration changes via SQL (hot-reload without restart)
- LISTEN/NOTIFY enables instant propagation
- Configuration is transactional (rollback on error)
- All relay instances see the same configuration (single source of truth)
- Easy to manage programmatically (Terraform, application code)

**Trade-offs:**
- Requires database access to view/change configuration
- Secrets must use `${env:...}` substitution (not stored in catalog)
- Slightly less familiar than file-based config for ops teams

## ADR-4: Single Binary Relay

**Decision:** The relay is a single statically-linked binary rather than a framework, library, or JVM application.

**Context:** The relay needs to be deployed easily across diverse environments.

**Rationale:**
- Single binary deployment (no runtime dependencies)
- Small container images (~20 MB)
- Fast startup time (< 1 second)
- Low resource consumption (Rust, no GC pauses)
- Cross-compilation for Linux/macOS/ARM

**Trade-offs:**
- Feature-gated compilation (some sinks/sources require build flags)
- Rust ecosystem less familiar than Java/Python for some teams
- Plugin system not possible (all sinks compiled in)

## ADR-5: JMESPath for Transforms

**Decision:** Use JMESPath (not JSONPath, jq, or a custom DSL) for message transforms and filters.

**Context:** Messages need lightweight filtering and reshaping without external tools.

**Rationale:**
- Well-specified language with formal grammar
- Deterministic evaluation (no side effects)
- Good balance of power and simplicity
- Fast compiled evaluation
- Familiar from AWS CLI and other tools

**Trade-offs:**
- Less powerful than jq (no recursion, limited array manipulation)
- No support for array indexing in field paths
- Users must learn JMESPath syntax

## ADR-6: At-Least-Once Delivery

**Decision:** pg_tide provides at-least-once relay transport by default, not
unqualified cross-system exactly-once delivery.

**Context:** Distributed systems cannot provide exactly-once delivery without end-to-end coordination.

**Rationale:**
- At-least-once is achievable without two-phase commit
- Simpler implementation, more reliable operation
- The inbox provides destination-side deduplication for effectively exactly-once
  processing when application work is transactional and idempotent
- Most sinks (Kafka, NATS) inherently provide at-least-once anyway
- Idempotent consumers are a well-understood pattern

**Trade-offs:**
- Consumers may receive duplicate messages (rare, only on failure/recovery)
- Applications that need an effectively exactly-once outcome must implement
  durable deduplication and idempotent processing
- The inbox pattern adds complexity for cross-system exactly-once

## ADR-7: Canonical Outbox Storage and Native Relay Polling

**Decision:** The native relay polls the single shared table `tide.tide_outbox_messages` with a static, parameterized query keyed by `outbox_name`, and tracks offsets by `(relay_group_id, pipeline_id, outbox_name)`.

**Context:** The native write path (`tide.outbox_publish()`) always wrote to `tide.tide_outbox_messages`, but the relay's native source historically polled a dynamically-named per-outbox relation (`tide."outbox_<name>"`) that native installs never create. That relation naming belongs to pg_trickle compatibility, not native pg_tide.

**Rationale:**
- The read and write paths must address the same table
- A static `WHERE outbox_name = $1 AND id > $2 ORDER BY id LIMIT $3` query with an unconditional `(outbox_name, id)` index needs no per-outbox DDL
- Per-pipeline offsets make fan-out and restart correct without relying on the global `consumed_at` timestamp
- pg_trickle compatibility remains available behind an explicit `source_type = "pg_trickle_outbox"` mode

**Trade-offs:**
- The offset key change (`(relay_group_id, pipeline_id)` → `(relay_group_id, pipeline_id, outbox_name)`) requires a stop-upgrade-start migration
- Global identity gaps within one outbox are expected; ordering is strictly increasing but not contiguous
- `consumed_at` and consumer groups remain but are non-authoritative for native delivery

See [ADR-011](https://github.com/trickle-labs/pg-tide/blob/main/docs/adr/adr-011-canonical-outbox-storage-and-relay-polling.md) for the full record.

## ADR-012: Relay Delivery Acknowledgment and Offset State Machine

**Decision:** Advance a source checkpoint only after a complete polled batch
reaches a durable terminal disposition: connector acknowledgment, atomic DLQ
storage, intentional filtering, or consuming dry-run observation.

The relay is **at-least-once transport**. Downstream success before checkpoint
commit can duplicate with the same stable event identity, but cannot silently
lose the event. **Effectively exactly once** requires durable destination
deduplication and idempotent/transactional application processing.

See [ADR-012](https://github.com/trickle-labs/pg-tide/blob/main/docs/adr/adr-012-relay-delivery-acknowledgment-and-offset-state-machine.md)
for the transition matrix, connector boundaries, ownership-session lifecycle,
and observability contract.

The canonical delivery vocabulary is `Polled -> Encoded -> PublishStarted ->
SinkAccepted -> SinkAcknowledged -> CheckpointCommitted -> CleanupEligible`.
Dry-run and intentional filtering branch from `Encoded`; failed publishing
branches through `PublishFailed -> DlqPersisted`. The pure executable model in
`pg-tide-relay/src/delivery_model.rs` tests these transitions without I/O.

## Further Reading

- [Architecture](../evaluate/architecture.md) — System design overview
- [Version Compatibility](version-compatibility.md) — Version support matrix
