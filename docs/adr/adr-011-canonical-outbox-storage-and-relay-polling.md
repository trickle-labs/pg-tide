# ADR-011: Canonical Outbox Storage and Relay Polling

**Status:** Accepted
**Date:** 2026-08-15
**Author:** pg_tide Contributors
**Supersedes:** none
**Refines:** [ADR-001](adr-001-single-table-outbox.md)

## Context

[ADR-001](adr-001-single-table-outbox.md) chose a single shared table,
`tide.tide_outbox_messages`, with an `outbox_name` discriminator, as the
canonical outbox storage. However, the relay binary's native outbox source
historically polled a *per-outbox relation* named `tide."outbox_<name>"`,
constructed dynamically as `format!("outbox_{outbox}")`. That naming convention
belongs to pg_trickle's claim-check delta tables, not to pg_tide's native
storage. The two designs conflicted: the public write path
(`tide.outbox_publish()`) wrote to `tide.tide_outbox_messages`, but the native
relay read from a relation that a native pg_tide install never creates.

v0.40.0 ("One Real Pipeline") makes the native PostgreSQL outbox → NATS
JetStream path correct end to end. This ADR records the storage, polling,
offset, envelope, and delivery decisions that path depends on.

## Decision

### 1. Canonical native storage

The native pg_tide outbox is the single shared table:

```text
tide.tide_outbox_messages
```

`outbox_name` is the logical stream discriminator. `outbox_create*()` creates a
**catalog row** in `tide.tide_outbox_config`, not a relation. Native relay
polling uses one static, parameterized query:

```sql
SELECT id, payload, headers, created_at
FROM tide.tide_outbox_messages
WHERE outbox_name = $1
  AND id > $2
ORDER BY id
LIMIT $3;
```

The `id` column is a global `GENERATED ALWAYS AS IDENTITY` sequence shared by
every outbox, so IDs within one `outbox_name` may contain gaps. Ordering is
therefore **strictly increasing by `id`, not contiguous**. Gaps are expected and
are not missing events.

### 2. Index contract

The canonical polling index is an **unconditional** btree index:

```sql
CREATE INDEX idx_tide_outbox_messages_poll
    ON tide.tide_outbox_messages (outbox_name, id);
```

The pre-existing partial index `idx_tide_outbox_messages_pending`
(`WHERE consumed_at IS NULL`) does **not** support the canonical query because
the native relay does not filter by `consumed_at`. The partial index is retained
only while the legacy status/cleanup APIs (`outbox_pending`,
`outbox_status()`, `outbox_truncate_delivered()`) still use it.

### 3. MVCC visibility and transaction behavior

PostgreSQL MVCC is the visibility boundary. A row becomes eligible for the relay
**only after its publishing transaction commits**. The relay must not use
`READ UNCOMMITTED`, dirty-read workarounds, or `pg_notify` payloads as message
data. Uncommitted `tide.outbox_publish()` rows are invisible to the relay's
polling connection.

### 4. Native payload and header semantics

`source_type = "outbox"` means the **native shared-table path** and decodes each
row's `payload` JSONB as a native event by default (no `v:1` pg_trickle
envelope required). The row's `headers` JSONB and `created_at` timestamp are
preserved through the relay envelope.

### 5. Stable event identity

The stable forward-message identity format is preserved:

```text
outbox_<outbox_name>:<message_id>:<row_index>
```

The `outbox_` prefix is a **logical compatibility identifier**, not a physical
relation name. NATS publishes it as `Nats-Msg-Id`. A restart or replay of the
same row generates the same value.

### 6. Offset identity, write timing, and monotonicity

The authoritative native relay offset identity is:

```text
(relay_group_id, pipeline_id, outbox_name)
```

`last_change_id` is the greatest `tide.tide_outbox_messages.id` for which the
sink acknowledged the complete relay batch. Rules:

- A pipeline name reused for another outbox must not inherit the previous
  outbox's offset.
- A lower offset write must never replace a higher offset (monotonic upsert
  using `GREATEST`).
- Restart loads the exact offset matching relay group, pipeline, and outbox.
- Offset advancement occurs only after sink acknowledgment.
- Failure to persist an offset is an explicit worker error, not a warning
  followed by a success-shaped delivery receipt.

### 7. `consumed_at` non-authoritative status

`consumed_at` is **not** authoritative native relay state. One global timestamp
cannot describe independent delivery to multiple pipelines. In v0.40.0:

- The native relay neither reads nor writes `consumed_at`.
- Native delivery is proved through per-pipeline offsets.
- `outbox_pending`, `outbox_status().pending_messages`, and
  `outbox_truncate_delivered()` are documented as legacy/global-consumer
  surfaces, not proof that every relay pipeline delivered a row.

### 8. Conservative cleanup behavior

Cleanup must remain conservative. It must not infer that one pipeline's offset
makes a row safe for every pipeline. Pipeline-aware retention is deferred to
v0.43.0. Do not add a second delivery-state table in v0.40.0; the per-pipeline
offset table already provides the required state.

### 9. Partitioned-parent transparency

When an outbox is converted to a partitioned layout, `tide.tide_outbox_messages`
remains the addressable parent relation. Native polling continues to read from
the parent; partition routing is transparent to the relay.

### 10. Explicit pg_trickle compatibility mode

pg_trickle compatibility is an **explicit** mode, never storage auto-detection:

```json
{
  "source_type": "pg_trickle_outbox",
  "source": { "outbox": "orders" }
}
```

Dynamic relation access (`tide."outbox_<name>"`), the `v:1` envelope, and
pg_trickle claim-check delta tables remain reachable **only** inside this
explicitly selected path. They are not reachable from the default native path.
`tide.relay_set_outbox_v2()` defaults to native and exposes pg_trickle only
through one explicit optional `source_mode` value.

### 11. Fan-out implications

Multiple native pipelines may consume the same `outbox_name` independently. Each
pipeline tracks its own `(relay_group_id, pipeline_id, outbox_name)` offset, so
fan-out to several sinks does not require duplicating outbox rows and does not
depend on `consumed_at`.

### 12. Fan-in quarantine

Multi-outbox fan-in is experimental and **disabled in v0.40.0**. The v0.40.0
migration disables all `relay_fanin_config` rows, `relay_fanin_enable()` returns
an experimental-feature error, and the coordinator does not start fan-in workers
in the production path. Existing fan-in catalog rows and offsets are retained for
a future release.

### 13. Subject and metadata contract

The NATS sink, not the PostgreSQL source, owns subject rendering. The v2 config
accepts either `subject` (fixed) or `subject_template` (rendered). Supported
template variables include `{outbox}`, `{op}`, `{outbox_id}`, and `{event_type}`.
`{event_type}` is sourced from a string `event_type` **header**; when the header
is missing or non-string, rendering falls back to the literal `event` token
(a documented, non-failing fallback). The sink publishes `Nats-Msg-Id` using the
stable dedup key and waits for the JetStream publish acknowledgment before
reporting success.

### 14. Upgrade and rollback constraints

Mixed v0.39.0/v0.40.0 relay operation is **not supported** during the offset-key
migration. The documented upgrade order is: stop v0.39.0 relays, back up the
database, `ALTER EXTENSION pg_tide UPDATE TO '0.40.0'`, deploy v0.40.0 relays,
verify offsets and delivery. No automatic destructive downgrade is provided.

### 15. Extension owner / superuser authorization

`tide.outbox_publish()` authorization is **fail-closed**. Any error while
reading outbox state, ACL existence, superuser/extension-owner status, or role
membership aborts the publish. Only explicit `no_acl`, `superuser`,
`extension_owner`, or `allowed` verdicts permit the insert; `denied`, unknown,
or null verdicts reject it. A superuser and the pg_tide extension owner may
publish, while other roles require an explicit ACL grant (including inherited
role membership).

## Consequences

- **Positive:** The native path is internally consistent — the write and read
  paths address the same table. A native install needs no per-outbox DDL.
- **Positive:** Per-pipeline offsets make fan-out and restart correct without
  `consumed_at` ambiguity.
- **Positive:** pg_trickle users keep a supported path behind an explicit mode.
- **Negative:** The offset primary key changes from
  `(relay_group_id, pipeline_id)` to
  `(relay_group_id, pipeline_id, outbox_name)`, which requires a stop-upgrade-start
  migration and forbids mixed-version relay operation during the change.
- **Negative:** Legacy global-consumer surfaces (`consumed_at`, consumer groups)
  remain but are explicitly non-authoritative for native delivery, which must be
  documented to avoid confusion.

## Relationship to ADR-001

ADR-001 remains **Accepted**. It is the original single-table storage decision.
ADR-011 does not replace it; it clarifies the relay polling, offset, delivery,
and compatibility consequences of that single-table choice.
