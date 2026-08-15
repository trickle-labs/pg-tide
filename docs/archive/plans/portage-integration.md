# PLAN — pg-tide × portage Integration

> **Status:** Draft
> **Created:** 2026-05-22
> **Related:**
> [plans/rest-connector-library.md](rest-connector-library.md) ·
> [pg-tide-relay/src/coordinator.rs](../pg-tide-relay/src/coordinator.rs) ·
> [sql/pg_tide--0.1.0.sql](../sql/pg_tide--0.1.0.sql)

This plan describes how **pg-tide** integrates with the **portage** library. It lives
in the pg-tide repo because it describes what pg-tide must provide — it is the
*consumer* perspective of the portage API. Writing this down before portage is built
is intentional: the integration surface will expose gaps and ambiguities in the portage
design while they are still cheap to fix.

---

## Table of Contents

- [1. Overview](#1-overview)
- [2. Adapter Crate: `pg-tide-portage`](#2-adapter-crate-pg-tide-portage)
- [3. SQL Schema](#3-sql-schema)
- [4. Trait Implementations](#4-trait-implementations)
- [5. Relay Pipeline Types](#5-relay-pipeline-types)
- [6. Ingestion Flow](#6-ingestion-flow)
- [7. Writeback Flow](#7-writeback-flow)
- [8. Event-Driven Ingestion Flow](#8-event-driven-ingestion-flow)
- [9. Issues Surfaced](#9-issues-surfaced)

---

## 1. Overview

The integration sits at three seams:

```
External API ──pull──▶ portage ──PgTideOutboxSink──▶ tide.outbox ──relay──▶ downstream
External API ◀──push── portage ◀──PgTideInboxSource── tide.inbox  ◀──relay── upstream
tide.inbox (thin events) ──PgTideEventSource──▶ portage ──re-fetch──▶ PgTideOutboxSink
```

pg-tide provides five trait implementations, all in the `pg-tide-portage` crate:

| Trait | Implementation | Purpose |
|-------|----------------|--------|
| `CheckpointStore` | `PgCheckpointStore` | Persist per-(connector, collection) watermarks |
| `RecordSink` | `PgTideOutboxSink` | Publish ingested records to a named outbox |
| `WritebackSource` | `PgTideInboxSource` | Claim desired-state records from a named inbox |
| `EventSource` | `PgTideEventSource` | Claim raw push-event records from a named inbox |
| `IdMapStore` | `PgIdMapStore` | Persist `canonical_id → external_id` mappings |
| `WebhookStore` | `PgWebhookStore` | Store registration IDs and signing secrets |

The relay drives portage sync loops as a new pipeline type (`portage_ingestion` and
`portage_writeback`), coordinated via the existing advisory-lock coordinator.

---

## 2. Adapter Crate: `pg-tide-portage`

Lives in the pg-tide repo alongside other adapters:

```
trickle-labs/pg-tide/
└── pg-tide-portage/
    ├── Cargo.toml
    └── src/
        ├── lib.rs
        ├── checkpoint.rs      # PgCheckpointStore
        ├── outbox.rs          # PgTideOutboxSink
        ├── inbox.rs           # PgTideInboxSource (WritebackSource)
        ├── event_source.rs    # PgTideEventSource (EventSource)
        ├── id_map.rs          # PgIdMapStore
        ├── webhook_store.rs   # PgWebhookStore
        └── schema.rs          # CREATE TABLE IF NOT EXISTS migrations
```

### Cargo dependencies

```toml
[package]
name = "pg-tide-portage"
version = "0.1.0"
edition = "2021"

[dependencies]
portage        = { git = "https://github.com/trickle-labs/portage", features = [] }
tokio-postgres = { version = "0.7", features = ["with-serde_json-1"] }
deadpool-postgres = "0.12"
serde_json     = "1"
tokio          = { version = "1", features = ["full"] }
tracing        = "0.1"
async-trait    = "0.1"
thiserror      = "1"
```

`pg-tide-portage` has no dependency on `pgrx` — it is relay code, not extension code.
It talks to PostgreSQL over a normal `tokio-postgres` connection, the same as the rest
of the relay.

---

## 3. SQL Schema

`pg-tide-portage` owns its own tables and creates them at startup via `schema.rs`
(`CREATE TABLE IF NOT EXISTS`). They live in the `tide` schema alongside the outbox
and inbox tables to keep all pg-tide managed state in one schema.

### 3.1 Checkpoint table

```sql
CREATE TABLE IF NOT EXISTS tide.portage_checkpoints (
    connector       text        NOT NULL,
    collection      text        NOT NULL,
    watermark       text        NOT NULL,
    watermark_type  text        NOT NULL CHECK (watermark_type IN ('timestamp', 'cursor', 'offset')),
    updated_at      timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (connector, collection)
);
```

### 3.2 ID map table

```sql
CREATE TABLE IF NOT EXISTS tide.portage_id_map (
    connector       text        NOT NULL,
    collection      text        NOT NULL,
    canonical_id    text        NOT NULL,
    external_id     text        NOT NULL,
    source          text        NOT NULL CHECK (source IN ('library', 'application')),
    created_at      timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (connector, collection, canonical_id)
    -- No reverse-unique constraint: during MDM merge transitions multiple canonical IDs
    -- may temporarily map to the same external_id. This is allowed; the application is
    -- responsible for cleaning up stale mappings after a merge completes.
);
```

### 3.3 Webhook registrations table

```sql
CREATE TABLE IF NOT EXISTS tide.portage_webhook_registrations (
    connector        text        NOT NULL,
    collection       text        NOT NULL,
    registration_id  text,                    -- captured from create response id_field
    signing_secret   text,                    -- encrypted at rest (see note below)
    status           text        NOT NULL DEFAULT 'unknown'
                                 CHECK (status IN ('unknown', 'active', 'inactive', 'error')),
    last_checked_at  timestamptz,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (connector, collection)
);
```

**Signing secret security:** `signing_secret` stores the webhook signing secret returned
by some APIs (Stripe `whsec_…`, Shopify HMAC key). It must be encrypted at rest.
The relay reads the plaintext value to verify incoming webhook signatures. Implementation
options (in order of preference): column-level encryption with `pgcrypto`, application-
level encryption before insert, or storage in an external secrets manager with only an
ID stored in the column. The exact mechanism is left to the deployment but must be
documented in the adapter.

### 3.4 No extension dependency for these tables

The pg_tide extension (`CREATE EXTENSION pg_tide`) does **not** create these tables.
The adapter creates them when the relay starts. `schema.rs` should check whether any
portage pipelines are configured before running the migrations.

---

## 4. Trait Implementations

### 4.1 PgCheckpointStore

```rust
pub struct PgCheckpointStore {
    pool: deadpool_postgres::Pool,
}

#[async_trait]
impl CheckpointStore for PgCheckpointStore {
    async fn load(&self, connector: &str, collection: &str)
        -> Result<Option<Checkpoint>, PortageError>
    {
        let client = self.pool.get().await?;
        let row = client.query_opt(
            "SELECT watermark, watermark_type, updated_at
             FROM tide.portage_checkpoints
             WHERE connector = $1 AND collection = $2",
            &[&connector, &collection],
        ).await?;
        Ok(row.map(|r| Checkpoint {
            watermark: r.get(0),
            watermark_type: r.get::<_, String>(1).parse()?,
            updated_at: r.get(2),
        }))
    }

    async fn save(&self, connector: &str, collection: &str, cp: &Checkpoint)
        -> Result<(), PortageError>
    {
        let client = self.pool.get().await?;
        client.execute(
            "INSERT INTO tide.portage_checkpoints
                 (connector, collection, watermark, watermark_type, updated_at)
             VALUES ($1, $2, $3, $4, now())
             ON CONFLICT (connector, collection)
             DO UPDATE SET watermark = EXCLUDED.watermark,
                           watermark_type = EXCLUDED.watermark_type,
                           updated_at = EXCLUDED.updated_at",
            &[&connector, &collection, &cp.watermark, &cp.watermark_type.as_str()],
        ).await?;
        Ok(())
    }
}
```

### 4.2 PgTideOutboxSink

Publishes each ingested record as an outbox message. The subject encodes the connector,
collection, and event type so downstream consumers can route by topic.

```rust
pub struct PgTideOutboxSink {
    pool:        deadpool_postgres::Pool,
    outbox_name: String,
    /// Subject template: e.g. "hubspot.contacts.upserted"
    /// Supports {{ connector }}, {{ collection }}, {{ op }} variables.
    subject_template: String,
}

#[async_trait]
impl RecordSink for PgTideOutboxSink {
    async fn publish(&self, record: &SinkRecord) -> Result<(), PortageError> {
        let client = self.pool.get().await?;
        // SinkRecord carries: connector, collection, op (upserted/deleted), payload (the raw API record)
        let subject = render_subject(&self.subject_template, record);
        client.execute(
            "SELECT tide.outbox_publish($1, $2, $3::jsonb)",
            &[&self.outbox_name, &subject, &record.payload],
        ).await?;
        Ok(())
    }
}
```

**Subject convention:** `{connector}.{collection}.{op}` — e.g.
`hubspot.contacts.upserted`, `hubspot.contacts.deleted`. Downstream consumers
(MDM matching, warehouse loader, etc.) subscribe by prefix or exact topic.

### 4.3 PgTideInboxSource

Claims batches of desired-state records from a named inbox and exposes them as a
`WritebackSource` stream.

```rust
pub struct PgTideInboxSource {
    pool:       deadpool_postgres::Pool,
    inbox_name: String,
    batch_size: i64,
}

#[async_trait]
impl WritebackSource for PgTideInboxSource {
    async fn claim(&self) -> Result<Vec<WriteRecord>, PortageError> {
        let client = self.pool.get().await?;
        let rows = client.query(
            "SELECT message_id, payload
             FROM tide.inbox_claim($1, $2)",
            &[&self.inbox_name, &self.batch_size],
        ).await?;

        rows.into_iter().map(|r| {
            let id: i64 = r.get(0);
            let payload: serde_json::Value = r.get(1);
            // Expected payload shape: { canonical_id, op, data, precondition? }
            parse_write_record(id, payload)
        }).collect()
    }

    async fn ack(&self, id: WriteRecordId) -> Result<(), PortageError> {
        let client = self.pool.get().await?;
        client.execute(
            "SELECT tide.inbox_mark_processed($1)",
            &[&(id as i64)],
        ).await?;
        Ok(())
    }

    async fn nack(&self, id: WriteRecordId, reason: &str) -> Result<(), PortageError> {
        let client = self.pool.get().await?;
        client.execute(
            "SELECT tide.inbox_mark_failed($1, $2)",
            &[&(id as i64), &reason],
        ).await?;
        Ok(())
    }
}
```

**Inbox payload contract** — the message JSON in the inbox must have:

```json
{
  "canonical_id":  "acct-789",
  "op":            "update",
  "data":          { "email": "new@example.com" },
  "precondition":  { "email": "old@example.com" }
}
```

`op` is `create | update | upsert | delete`. `precondition` is optional.
The `canonical_id` must be present; it is the caller's stable key (cluster ID, etc.)
and is never assigned by portage or by pg-tide.

### 4.4 PgIdMapStore

```rust
pub struct PgIdMapStore {
    pool: deadpool_postgres::Pool,
}

#[async_trait]
impl IdMapStore for PgIdMapStore {
    async fn get(&self, connector: &str, collection: &str, canonical_id: &str)
        -> Result<Option<String>, PortageError>
    {
        let client = self.pool.get().await?;
        let row = client.query_opt(
            "SELECT external_id FROM tide.portage_id_map
             WHERE connector = $1 AND collection = $2 AND canonical_id = $3",
            &[&connector, &collection, &canonical_id],
        ).await?;
        Ok(row.map(|r| r.get(0)))
    }

    async fn insert(&self, connector: &str, collection: &str,
                    canonical_id: &str, external_id: &str, source: &str)
        -> Result<(), PortageError>
    {
        let client = self.pool.get().await?;
        client.execute(
            "INSERT INTO tide.portage_id_map
                 (connector, collection, canonical_id, external_id, source)
             VALUES ($1, $2, $3, $4, $5)",
            &[&connector, &collection, &canonical_id, &external_id, &source],
        ).await?;
        Ok(())
    }

    async fn register(&self, connector: &str, collection: &str,
                      canonical_id: &str, external_id: &str)
        -> Result<(), PortageError>
    {
        let client = self.pool.get().await?;
        // Upsert: no-op if same external_id already registered; error if different
        let rows_affected = client.execute(
            "INSERT INTO tide.portage_id_map
                 (connector, collection, canonical_id, external_id, source)
             VALUES ($1, $2, $3, $4, 'application')
             ON CONFLICT (connector, collection, canonical_id)
             DO UPDATE SET external_id = EXCLUDED.external_id,
                           source      = 'application'
             WHERE portage_id_map.external_id = EXCLUDED.external_id",
            &[&connector, &collection, &canonical_id, &external_id],
        ).await?;
        if rows_affected == 0 {
            return Err(PortageError::MappingConflict { canonical_id: canonical_id.into() });
        }
        Ok(())
    }
}
```

`register` is intentionally usable without a `Connector` reference. An MDM process
can instantiate `PgIdMapStore` directly (just a pool + schema name) and call `register`
after completing a match/merge, without loading any connector YAML. This is the primary
path for seeding the id_map for pre-existing records (see §7 writeback bootstrap).

---

## 5. Relay Pipeline Types

Portage pipelines are YAML-file-driven, not catalog-row-driven (unlike outbox/inbox
pipelines that live in `tide.outbox_config` / `tide.inbox_config`). The chosen model
is **hybrid**: TOML declares the connector file path; the catalog records which
pipelines are active, enabling runtime enable/disable without relay restart.

### 5.1 Catalog table

```sql
CREATE TABLE IF NOT EXISTS tide.portage_pipeline_config (
    name            text        PRIMARY KEY,
    kind            text        NOT NULL CHECK (kind IN ('ingestion', 'writeback', 'events')),
    connector_file  text        NOT NULL,  -- path on relay host, or inline YAML in future
    collection      text        NOT NULL,
    -- ingestion fields
    outbox          text,                  -- target outbox name (kind = ingestion)
    subject_template text,                 -- e.g. '{{ connector }}.{{ collection }}.{{ op }}'
    sync_interval   interval,              -- how often to run (kind = ingestion)
    -- writeback / events fields
    inbox           text,                  -- source inbox name (kind = writeback | events)
    batch_size      int         NOT NULL DEFAULT 50,
    enabled         bool        NOT NULL DEFAULT true,
    tenant_name     text        NOT NULL DEFAULT 'default',
    created_at      timestamptz NOT NULL DEFAULT now()
);
```

The coordinator discovers portage pipelines from this table on the same polling
cycle as outbox/inbox pipelines. TOML `[[portage]]` blocks are still supported for
local dev but emit a warning if `config_mode = catalog_only`.

### 5.2 Coordinator changes

```rust
pub enum PipelineKind {
    Forward,            // outbox → downstream sink
    Reverse,            // upstream source → inbox
    PortageIngestion,   // portage pull loop → outbox
    PortageWriteback,   // inbox → portage write → external API
    PortageEvents,      // inbox (thin events) → portage re-fetch → outbox
}
```

Each portage task holds a loaded `portage::Connector` plus the relevant adapter
implementations (sink, source, stores). Advisory lock key incorporates pipeline name
and relay group ID, preventing duplicate ownership across relay instances.

### 5.3 TOML config (dev / override)

```toml
[[portage]]
name           = "hubspot-contacts-in"
kind           = "ingestion"
connector_file = "/etc/pg-tide/connectors/hubspot.yaml"
collection     = "contacts"
outbox         = "hubspot_contacts_in"
subject        = "hubspot.contacts.{{ op }}"
interval       = "5m"
enabled        = true

[[portage]]
name           = "hubspot-contacts-out"
kind           = "writeback"
connector_file = "/etc/pg-tide/connectors/hubspot.yaml"
collection     = "contacts"
inbox          = "hubspot_contacts_out"
enabled        = true

[[portage]]
name           = "hubspot-contacts-events"
kind           = "events"
connector_file = "/etc/pg-tide/connectors/hubspot.yaml"
collection     = "contacts"
inbox          = "hubspot_contacts_events"
outbox         = "hubspot_contacts_in"   # re-fetched records go to same outbox as ingestion
enabled        = true
```

---

## 6. Ingestion Flow

```
coordinator
  │
  ├── portage_ingestion task (hubspot-contacts-in)
  │     │
  │     ├── connector.read("contacts")     # portage pull loop
  │     │     ├── paginated GET /crm/v3/objects/contacts
  │     │     ├── watermark advance via PgCheckpointStore
  │     │     └── each record → PgTideOutboxSink.publish()
  │     │           └── tide.outbox_publish('hubspot_contacts_in',
  │     │                   'hubspot.contacts.upserted', record::jsonb)
  │     │
  │     └── sleep(interval), repeat
  │
  └── (advisory lock released on task exit)
```

Checkpoint advance ordering: portage guarantees the watermark only moves forward after
`RecordSink.publish()` returns `Ok`. Since `PgTideOutboxSink.publish()` does a
synchronous `SELECT tide.outbox_publish(...)` in its own connection, each publish is
committed before portage considers it done. The checkpoint is then saved in a
subsequent call. This gives at-least-once delivery (records may be re-published on
restart before the watermark advances) with no data loss.

**What goes in the outbox payload:** The raw API record JSON, unchanged. pg-tide
carries it opaquely. Downstream consumers (MDM, warehouse loader) parse it
themselves. The `canonical_id` is **not** in the outbox payload at publish time — it
is assigned by MDM downstream and registered into `id_map` separately.

---

## 7. Writeback Flow

```
coordinator
  │
  ├── portage_writeback task (hubspot-contacts-out)
  │     │
  │     └── connector.writeback("contacts").source(PgTideInboxSource).run()
  │           │
  │           ├── PgTideInboxSource.claim()
  │           │     └── tide.inbox_claim('hubspot_contacts_out', batch_size)
  │           │
  │           ├── for each WriteRecord:
  │           │     ├── portage looks up external_id via PgIdMapStore.get()
  │           │     ├── (optional) preflight GET via connector.fetch:
  │           │     ├── PATCH/POST/DELETE to external API
  │           │     ├── on success: PgTideInboxSource.ack(id)
  │           │     │     └── tide.inbox_mark_processed(message_id)
  │           │     └── on failure: PgTideInboxSource.nack(id, reason)
  │           │           └── tide.inbox_mark_failed(message_id, reason)
  │           │
  │           └── loop (long-poll or sleep between claims)
```

**Dead-letter routing:** When portage calls `nack(id, reason)`, pg-tide's
`inbox_mark_failed` moves the message to the dead-letter state. The dead-letter
message includes the `reason` string which portage populates with a structured error
code (e.g., `ERR_PRECONDITION_MISMATCH`, `ERR_CONFLICT_DEAD_LETTER`).

---

## 8. Event-Driven Ingestion Flow

When a collection declares an `events:` block, portage consumes from a pg-tide inbox
via `PgTideEventSource` — a separate implementation from `PgTideInboxSource` because
the payload shapes differ:

- `PgTideInboxSource` expects `{ canonical_id, op, data, precondition? }` — desired state
- `PgTideEventSource` expects raw push-event payloads from which portage extracts a
  record ID via `record_id_path`

```rust
pub struct PgTideEventSource {
    pool:       deadpool_postgres::Pool,
    inbox_name: String,
    batch_size: i64,
}

#[async_trait]
impl EventSource for PgTideEventSource {
    async fn claim(&self) -> Result<Vec<EventRecord>, PortageError> {
        let client = self.pool.get().await?;
        let rows = client.query(
            "SELECT message_id, payload FROM tide.inbox_claim($1, $2)",
            &[&self.inbox_name, &self.batch_size],
        ).await?;
        Ok(rows.into_iter().map(|r| EventRecord {
            id:      r.get::<_, i64>(0) as EventRecordId,
            payload: r.get(1),
        }).collect())
    }

    async fn ack(&self, id: EventRecordId) -> Result<(), PortageError> {
        let client = self.pool.get().await?;
        client.execute("SELECT tide.inbox_mark_processed($1)", &[&(id as i64)]).await?;
        Ok(())
    }

    async fn nack(&self, id: EventRecordId, reason: &str) -> Result<(), PortageError> {
        let client = self.pool.get().await?;
        client.execute("SELECT tide.inbox_mark_failed($1, $2)", &[&(id as i64), &reason]).await?;
        Ok(())
    }
}
```

The event re-fetch flow:
1. `PgTideEventSource.claim()` returns raw webhook payloads from the inbox
2. portage extracts the record ID via `events.record_id_path`
3. portage calls `collection.fetch:` to retrieve the full record from the external API
4. Full record is published via `PgTideOutboxSink` to the same outbox as regular ingestion
5. `PgTideEventSource.ack(id)` marks the inbox message processed

The `source_key` in `events:` maps to the `PgTideEventSource` instance; the caller
binds event sources to source keys when building the connector:

```rust
let connector = Connector::from_file("hubspot.yaml")
    .event_source("hubspot.webhook.contact", PgTideEventSource::new(&pool, "hubspot_contacts_events", 100))
    .sink(PgTideOutboxSink::new(&pool, "hubspot_contacts_in", "hubspot.contacts.{{ op }}"))
    .stores(&pg_stores)
    .build()
    .await?;
```

---

## 9. Webhook Registration

When a connector's `events:` block declares a `registration:` sub-block, the relay
calls `connector.ensure_webhooks_registered(delivery_url).await?` at task startup
(after advisory lock acquisition, before starting the event consumer loop).

### PgWebhookStore

```rust
pub struct PgWebhookStore {
    pool: deadpool_postgres::Pool,
}

#[async_trait]
impl WebhookStore for PgWebhookStore {
    async fn get_secret(&self, connector: &str, collection: &str)
        -> Result<Option<String>, PortageError>
    {
        let client = self.pool.get().await?;
        // signing_secret stored encrypted; decrypt here
        let row = client.query_opt(
            "SELECT signing_secret FROM tide.portage_webhook_registrations
             WHERE connector = $1 AND collection = $2",
            &[&connector, &collection],
        ).await?;
        Ok(row.and_then(|r| r.get::<_, Option<String>>(0)))
    }
    // set_secret, get_registration_id, set_registration_id follow same pattern
}
```

### Relay inbound webhook verification

The relay's inbound webhook handler needs the signing secret to verify payloads. It
reads the plaintext secret from `PgWebhookStore` at startup and caches it (refreshed
when the webhook registration task updates the secret):

```rust
// In relay webhook receiver pipeline config:
// secret resolved from tide.portage_webhook_registrations via PgWebhookStore
let secret = pg_webhook_store
    .get_secret("hubspot", "contacts")
    .await?
    .ok_or(RelayError::WebhookSecretNotFound { connector: "hubspot".into() })?;
```

This creates a dependency direction: the relay's inbound webhook verifier knows about
`tide.portage_webhook_registrations`. This is acceptable since both the relay and the
portage adapter are in the pg-tide repo. The relay does **not** depend on the portage
library itself (only on the pg-tide-portage adapter, which does).

### Delivery URL configuration

The webhook delivery URL (the relay's public endpoint) is configured per-pipeline in
the portage pipeline catalog:

```sql
ALTER TABLE tide.portage_pipeline_config
    ADD COLUMN webhook_delivery_url text;  -- required for kind = 'events' with registration:
```

Or in TOML:
```toml
[[portage]]
kind            = "events"
webhook_delivery_url = "https://my-relay.example.com/inbound/hubspot"
```

## 10. Issues Surfaced — Resolution Status

All issues from the original analysis have been resolved:

| Issue | Status | Resolution |
|-------|--------|------------|
| I1 — `RecordSink` undefined | ✅ Resolved | Defined in portage plan §3 with `SinkRecord` and `SinkOp` |
| I2 — Checkpoint ordering not a contract | ✅ Resolved | Documented as API contract in portage plan §8 and trait docstring |
| I3 — No `EventSource` trait | ✅ Resolved | `EventSource` trait defined in portage plan §3; `PgTideEventSource` in §8 above |
| I4 — Relay pipeline config undefined | ✅ Resolved | Hybrid model: `tide.portage_pipeline_config` catalog + TOML override; see §5 above |
| I5 — Reverse-unique constraint too strict | ✅ Resolved | Constraint dropped; merge transitions allowed; see §3.2 |
| I6 — `register` requires `Connector` ref | ✅ Resolved | `register` is on `IdMapStore` trait directly; `PgIdMapStore` usable standalone; see §4.4 |
| I7 — `claim()` blocking behaviour undefined | ✅ Resolved | `WritebackSource`/`EventSource` contract: `claim()` may block; pg-tide impls default to poll+sleep with LISTEN/NOTIFY as an opt-in config |
