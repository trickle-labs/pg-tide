# ADR-012: Relay Delivery Acknowledgment and Offset State Machine

**Status:** Accepted
**Date:** 2026-08-16
**Author:** pg_tide Contributors
**Related:** [ADR-002](adr-002-advisory-lock-coordination.md), [ADR-011](adr-011-canonical-outbox-storage-and-relay-polling.md)

## Context

The relay crosses a PostgreSQL transaction boundary and a downstream transport
boundary. Those operations are not one distributed transaction. A source
checkpoint must therefore advance only after the complete polled batch has a
durable terminal disposition. Otherwise a crash can silently skip a committed
event.

The durable unit is one polled batch and its original source checkpoint.
Transforms and routing may remove messages, but they never replace that
checkpoint. A partial downstream effect is ambiguous: the batch is retried
with the same stable identities.

## Decision

### Finite state machine

The normative delivery states are:

```text
Polled -> Prepared -> PublishInFlight -> SinkAcknowledged
        -> CheckpointCommitted -> EligibleForCleanup
```

Alternative terminal paths are:

```text
Prepared -> DryRunObserved -> CheckpointCommitted
Prepared -> IntentionallyFiltered -> CheckpointCommitted
PublishFailed -> DlqPersisted -> CheckpointCommitted
```

Errors that do not produce a terminal disposition—decode, transform, schema
policy, unresolved sink, or DLQ write errors—leave the source checkpoint
uncommitted. `EligibleForCleanup` requires every configured retention
participant to have passed the row and the outbox retention cutoff to have
elapsed; it is never inferred from one pipeline or `consumed_at`.

### Connector acknowledgment boundaries

`Sink::publish()` may return success only at the connector's durable boundary:

| Connector | Sink acknowledgment boundary |
|---|---|
| NATS JetStream | Every publish acknowledgment future completed successfully |
| Local PostgreSQL inbox | Batch `INSERT ... ON CONFLICT` completed |
| Remote PostgreSQL inbox | Remote batch `INSERT ... ON CONFLICT` completed |

Source checkpoint boundaries are:

| Source | Durable checkpoint boundary |
|---|---|
| Native PostgreSQL outbox | Monotonic `relay_consumer_offsets.last_change_id` persisted |
| PostgreSQL consumer group | Monotonic `tide_consumer_offsets.committed_offset` persisted |
| NATS JetStream | Every pending broker-message acknowledgment completed |

Client-side queuing is not an acknowledgment. A multi-message NATS publish is
not atomic: if a later publish fails, the successful prefix may be published
again. The original batch remains uncommitted and is retried.

### Transition matrix

| Transition / state | Durable fact | Retry after crash | Duplicate risk | Silent-loss risk if implemented correctly | Evidence |
|---|---|---|---|---|---|
| Start -> `Polled` | None beyond source state | Poll/redeliver | None yet | None | Pre-encode crash test |
| `Polled` -> `Prepared` | None | Poll/redeliver | None yet | None | Post-prepare crash test |
| `Prepared` -> `PublishInFlight` | Sink-dependent and possibly ambiguous | Retry original batch | Possible prefix duplicate | None; checkpoint unchanged | During-publish test |
| `PublishInFlight` -> `SinkAcknowledged` | Destination confirmed by connector contract | Retry if checkpoint absent | Yes | None | Post-sink-ack crash test |
| `SinkAcknowledged` -> `CheckpointCommitted` | Source checkpoint advanced | Resume after batch | No automatic replay | None | Post-checkpoint crash test |
| `PublishFailed` -> `DlqPersisted` | Every DLQ row durable or already present by idempotency key | Retry DLQ insert if uncertain | Duplicate DLQ insert suppressed | None | DLQ crash/failure tests |
| `DlqPersisted` -> `CheckpointCommitted` | Source checkpoint advanced past DLQ batch | Resume after batch | No message redelivery | None | DLQ acknowledgment test |
| `Prepared` -> `DryRunObserved` | Bounded structured observation emitted | Continue after checkpoint | Not applicable | Operator explicitly chose no sink delivery | Real coordinator dry-run test |
| `Prepared` -> `IntentionallyFiltered` | Whole input batch was intentionally filtered | Continue after checkpoint | None | None if original checkpoint retained | All-filtered test |
| `CheckpointCommitted` -> `EligibleForCleanup` | Every configured participant passes the row and retention age has elapsed | No relay retry | None | `tide.outbox_sweep()` | Bounded, lock-safe cleanup |

### Filtering, dry-run, DLQ, and replay

An all-filtered or fully routed-away batch is an intentional disposition, not
an empty sink publish. It commits the original polled checkpoint. A transform,
decode, or schema-policy error is not intentional filtering and does not commit.

Live dry-run is an explicit consuming mode. It logs bounded metadata about the
prepared batch, skips the sink, records `DryRunObserved`, and commits the source
checkpoint. A checkpoint error remains unhealthy and is retried or stops the
worker; it is never discarded.

Replay is checkpoint-neutral. A bounded replay reads from its explicit range,
does not mutate the live checkpoint, and exits once. Reprocessing a live range
requires the administrative rewind API; replay completion must not cause a
coordinator to restart the same replay indefinitely.

DLQ insertion is atomic for the complete failed batch. The checkpoint advances
only when every row is durably inserted or already present under its
`(pipeline_name, dedup_key)` identity. A failed or ambiguous DLQ write retains
the source batch.

### Duplicate and loss guarantees

The relay provides **at-least-once relay transport**. A crash after downstream
success but before checkpoint commit can produce a duplicate, never a skipped
event. Stable event identity is therefore required:

```text
outbox_<outbox_name>:<message_id>:<row_index>
```

Connectors must preserve that identity where their deduplication facility
supports it. PostgreSQL inboxes durably deduplicate `event_id`; NATS
JetStream deduplication is bounded by its configured window. **Effectively
exactly once** is available only when the destination durably deduplicates the
stable ID and application processing is idempotent/transactional. “Exactly
once” is never an unqualified cross-system relay claim.

Checkpoint commit precedes updating worker memory and auxiliary delivery
receipts. If the process dies in that small interval, restart loads the durable
checkpoint and does not replay automatically. Receipts are evidence and are
not authoritative delivery state.

### Ownership-session lifecycle

The ownership lock identity is:

```text
(relay_group_id, tenant_name, direction, pipeline_name)
```

The worker acquires its PostgreSQL session-level advisory lock on a dedicated
connection and keeps that same connection for the worker's lifetime. Pooled
metadata connections cannot hold or release ownership locks. Loss of the
ownership session cancels the worker before takeover; PostgreSQL then releases
the lock. Graceful shutdown drains or aborts the worker before releasing or
dropping that session. v0.41.0 and v0.42.0 relay ownership is not mixed.

### Observability and tests

The relay emits bounded stage metrics, including:

```text
pg_tide_relay_delivery_stage_total{pipeline,stage,outcome}
```

Stages include `prepared`, `sink_acknowledged`, `checkpoint_committed`,
`checkpoint_failed`, `dlq_persisted`, `dlq_failed`, `filtered`, and
`dry_run_observed`. It also reports checkpoint errors, DLQ errors, ownership
acquisition/loss/transfer, and forced shutdown with in-flight work.

Structured transitions identify pipeline, direction, relay group, tenant,
source/sink, batch count, checkpoint class, failure class, and duplicate risk.
They never log payloads, secrets, or unredacted connector credentials.

Tests must exercise every matrix boundary with real coordinator behavior:
pre-publish, during publish, post-sink-ack, post-checkpoint, DLQ
failure/post-commit, NATS source acknowledgment, ownership-session loss,
graceful shutdown, filtered batches, dry-run, and one-shot replay. Two real
relay processes must demonstrate ownership transfer without silent loss.

## Relationship to other ADRs

ADR-002 remains the advisory-lock coordination decision; this ADR specifies
that the lock belongs to the worker's dedicated session and defines its loss
and shutdown behavior. ADR-011 remains the canonical native storage and
polling decision; this ADR specifies that `last_change_id` is the greatest row
whose batch reached a durable terminal disposition. ADR-011's monotonic
outbox-scoped offsets remain authoritative, while delivery receipts are
auxiliary evidence.

Pipeline-aware retention cannot safely infer completion from one pipeline's
offset. `EligibleForCleanup` therefore uses the minimum safe checkpoint across
all configured participants plus the retention cutoff, as specified in ADR-013.
