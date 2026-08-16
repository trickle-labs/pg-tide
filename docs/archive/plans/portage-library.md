# PLAN — portage: Declarative REST Connector Library

> **Status:** Draft (Decisions Locked — see §10 for remaining open questions)
> **Created:** 2026-05-21
> **Related:**
> [grove/in-and-out](https://github.com/grove/in-and-out) ·
> [pg-tide-relay/src/](../pg-tide-relay/src/) ·
> [wire-formats.md](wire-formats.md)

---

## Table of Contents

- [1. Motivation](#1-motivation)
- [2. Goals & Non-Goals](#2-goals--non-goals)
- [3. API Design](#3-api-design)
- [4. Core Concepts](#4-core-concepts)
- [5. Connector Config Schema](#5-connector-config-schema)
- [6. Integration with pg-tide](#6-integration-with-pg-tide)
- [7. Architecture](#7-architecture)
- [8. Checkpoint Lifecycle](#8-checkpoint-lifecycle)
- [9. Phased Delivery](#9-phased-delivery)
- [10. Open Questions](#10-open-questions)

---

## 1. Motivation

The pg-tide relay wires messaging infrastructure (Kafka, NATS, HTTP webhooks, SQS, …)
to PostgreSQL transactional outbox/inbox tables. What the ecosystem currently lacks is
an opinionated, **declarative layer for talking to application REST APIs** — services
like HubSpot, Salesforce, Stripe, Notion, or any HTTP API that follows conventional
pagination, authentication, and delta-sync patterns.

This plan proposes a standalone library — **`portage`** — that:

1. Describes REST API integrations as **declarative YAML connector configs** (no custom
   code per connector).
2. Handles **pagination** (cursor, offset, link-header, page-number), **authentication**
   (OAuth2, API key, JWT, custom pre-request flows), **incremental sync** (high-water
   marks, continuation tokens), and **retries / circuit-breaking**.
3. **Stores checkpoints durably** so sync can resume after a restart or crash.
4. Is **usable standalone** — independent of pg-tide.
5. Optionally **integrates with pg-tide's transactional outbox and inbox**: ingest
   records are published to the outbox; desired-state records are consumed from the
   inbox and pushed back to the API.

### Prior Art

| Project | Language | What we take from it |
|---------|----------|----------------------|
| **grove/in-and-out** | Python | Full connector YAML schema (CONFIG_DESIGN.md), bidirectional design, webhook fan-out, conflict protection levels, HubSpot/Salesforce fixture examples |
| **dlt (dlthub)** | Python | Source/resource abstraction, incremental loading, schema inference, pluggable destinations |
| **pg-tide-relay** | Rust | Circuit breaker, rate limiter, envelope format, SPI patterns for outbox/inbox |

The key distinction from grove/in-and-out: this library is designed to **emit and consume
from a transactional outbox/inbox** rather than writing directly to application tables,
giving downstream consumers at-least-once delivery guarantees and replay.

---

## 2. Goals & Non-Goals

### Goals

| ID | Goal |
|----|------|
| G1 | **Dead simple by default** — fetch records from any paginated REST endpoint with a 5-line YAML and 3 lines of Rust; README leads with the YAML-first approach |
| G2 | Declarative YAML configs — full connector definition without writing code, for complex and recurring integrations |
| G3 | Stateful ingestion — durably track per-(connector, collection) watermarks; resumable after restart |
| G4 | Bidirectional — pull records from API (ingestion) *and* push changes back (writeback) |
| G5 | pg-tide integration — optional adapter: publish to a `RecordSink`, consume via a `WritebackSource`, store checkpoints via `CheckpointStore` |
| G6 | Standalone — usable outside pg-tide; the pg-tide adapter is an optional feature/crate |
| G7 | Observable — Prometheus metrics, structured logging, circuit breakers, dead-letter routing |
| G8 | Agent-friendly — connector configs are machine-generatable; stable JSON Schema + stable error codes |

### Non-Goals

| ID | Non-Goal |
|----|----------|
| N1 | Identity resolution / MDM (matching records across systems — that is OSI-Mapping's job) |
| N2 | Complex ETL transforms (beyond template interpolation and field renaming) |
| N3 | Native protocol sources (Kafka, NATS, CDC — those belong in pg-tide-relay) |
| N4 | Graphical UI |
| N5 | Inbound HTTP webhook server — pg-tide's relay receives, verifies, and fan-outs webhook events into the inbox. portage processes those inbox records: for thin notification events (`payload_type: notification`) it re-fetches the full record from the API using the record ID from the event payload |
| N6 | Non-HTTP protocols — SOAP and XML-RPC are out of scope; both require XML envelope handling (WSDL, fault parsing) incompatible with this library’s HTTP/JSON model. GraphQL is HTTP/JSON and will be supported in Phase 2 via `method: POST` + `body:` on the `read` block with `{{ cursor }}` for pagination injection into the request body |

---

## 3. API Design

The library has **one surface**: a declarative YAML connector file. The Rust API loads
and runs connectors — it does not replicate the YAML schema in code. The README leads
with a minimal YAML example followed by three lines of Rust.

### Code API

Load a connector YAML file (or an inline string) and iterate its collections:

```rust
use portage::prelude::*;

// Load from a file on disk
let connector = Connector::from_file("hubspot.yaml").await?;

// Stream records from one collection
let mut records = connector.read("contacts").await?;
while let Some(record) = records.next().await? {
    println!("{}", record);
}

// Collect all pages at once
let all = connector.read("contacts").collect().await?;
```

For ad-hoc fetches without a separate file, embed the config inline:

```rust
let connector = Connector::from_str(r#"
  base_url: https://api.example.com
  headers:
    Authorization: "Bearer {{ env('API_TOKEN') }}"
  collections:
    items:
      list:
        url: /items
        records_at: data
        next_page_link: "{{ body.next }}"
        termination: [next-page-link-empty]
"#)?;
let items = connector.read("items").collect().await?;
```

### Writeback (direct)

Push desired-state records directly to the API without a pg-tide inbox:

```rust
use portage::prelude::*;

let connector = Connector::from_file("hubspot.yaml").await?;
let w = connector.write("contacts");

// Upsert — library checks id_map; creates if absent, updates if present
w.upsert(WriteRecord {
    canonical_id: "acct-789",
    data: json!({
        "email":      "alice@example.com",
        "first_name": "Alice",
        "last_name":  "Smith",
    }),
    precondition: None,   // no CAS check; write unconditionally
}).await?;

// Update with a precondition (CAS) — aborts if email has changed externally
w.update(WriteRecord {
    canonical_id: "acct-789",
    data: json!({ "email": "new@example.com" }),
    precondition: Some(json!({ "email": "old@example.com" })),
}).await?;

// Delete
w.delete("acct-789").await?;
```

`WriteRecord.canonical_id` is the caller's stable key (cluster ID, natural key, etc.);
the library looks up the external API ID from `id_map`. After a successful `create`,
the new `external_id` is returned in `WriteResult` and persisted automatically.

### Writeback (source-driven)

Writeback can be driven by any `WritebackSource` — pg-tide's inbox is one example.
Writeback records carry the same fields as `WriteRecord`; the application does not call
`write()` directly:

```rust
use portage::prelude::*;
use pg_tide_portage::PgTideInboxSource;

let connector = Connector::from_file("hubspot.yaml").await?;
let source    = PgTideInboxSource::new(&pg_pool, "hubspot_contacts_out");

// Run until shutdown signal — claims messages, writes to API, marks processed/failed
connector
    .writeback("contacts")
    .source(source)
    .run()
    .await?;
```

Each writeback record must be a JSON object with `canonical_id`, `op`, `data`, and
optionally `precondition` (see §6.2 for the pg-tide payload format and §4.10 for routing logic).

### Quickstart — Minimal Trait Implementations

The traits that need wiring before a stateful run can proceed:

```rust
// portage exposes these trait definitions:

/// Persist per-(connector, collection) watermarks.
/// CONTRACT: portage only calls `save()` after all `publish()` calls for the same
/// checkpoint interval have returned `Ok`. Adapters may rely on this ordering —
/// the checkpoint never advances beyond what the sink has successfully committed.
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn load(&self, connector: &str, collection: &str)
        -> Result<Option<Checkpoint>, PortageError>;
    async fn save(&self, connector: &str, collection: &str, cp: &Checkpoint)
        -> Result<(), PortageError>;
}

/// Persist canonical_id → external_id mappings.
/// `register` is intentionally callable without a `Connector` reference so that
/// external processes (e.g. an MDM matcher) can seed mappings for pre-existing records.
#[async_trait]
pub trait IdMapStore: Send + Sync {
    async fn get(&self, connector: &str, collection: &str, canonical_id: &str)
        -> Result<Option<String>, PortageError>;
    async fn insert(&self, connector: &str, collection: &str,
                    canonical_id: &str, external_id: &str, source: &str)
        -> Result<(), PortageError>;
    /// Upsert; errors if a *different* external_id already exists for this canonical_id.
    async fn register(&self, connector: &str, collection: &str,
                      canonical_id: &str, external_id: &str)
        -> Result<(), PortageError>;
}

/// Publish ingested records downstream (outbox, file, stdout, …).
/// Called by the ingestion loop after each record is fetched.
#[async_trait]
pub trait RecordSink: Send + Sync {
    async fn publish(&self, record: &SinkRecord) -> Result<(), PortageError>;
}

pub struct SinkRecord {
    pub connector:   String,
    pub collection:  String,
    pub op:          SinkOp,
    pub external_id: String,
    pub payload:     serde_json::Value,  // raw API record, unchanged
}

pub enum SinkOp { Upserted, Deleted }

/// Consume raw push events from any source (webhook queue, inbox, SSE, …).
/// Distinct from `WritebackSource`: events carry notification payloads (just an ID +
/// event type), not desired-state records. portage extracts the record ID, re-fetches
/// the full record via `collection.fetch:`, and passes it to `RecordSink`.
#[async_trait]
pub trait EventSource: Send + Sync {
    /// Claim a batch of raw event payloads. May block until events are available.
    async fn claim(&self) -> Result<Vec<EventRecord>, PortageError>;
    async fn ack(&self, id: EventRecordId) -> Result<(), PortageError>;
    async fn nack(&self, id: EventRecordId, reason: &str) -> Result<(), PortageError>;
}

pub struct EventRecord {
    pub id:      EventRecordId,
    pub payload: serde_json::Value,  // portage extracts record_id_path from this
}

/// Drive writeback from any source of desired-state records.
/// `claim()` may block until records are available (implementation-defined).
#[async_trait]
pub trait WritebackSource: Send + Sync {
    async fn claim(&self) -> Result<Vec<WriteRecord>, PortageError>;
    async fn ack(&self, id: WriteRecordId) -> Result<(), PortageError>;
    async fn nack(&self, id: WriteRecordId, reason: &str) -> Result<(), PortageError>;
}
```

Minimal `HashMap`-backed implementations — copy-paste into your project:

```rust
use std::collections::HashMap;
use tokio::sync::RwLock;
use portage::{CheckpointStore, Checkpoint, IdMapStore, PortageError};

// ── CheckpointStore ──────────────────────────────────────────────────────────

#[derive(Default)]
struct MemCheckpoints(RwLock<HashMap<(String, String), Checkpoint>>);

#[async_trait]
impl CheckpointStore for MemCheckpoints {
    async fn load(&self, connector: &str, collection: &str)
        -> Result<Option<Checkpoint>, PortageError>
    {
        Ok(self.0.read().await.get(&(connector.into(), collection.into())).cloned())
    }

    async fn save(&self, connector: &str, collection: &str, cp: &Checkpoint)
        -> Result<(), PortageError>
    {
        self.0.write().await.insert((connector.into(), collection.into()), cp.clone());
        Ok(())
    }
}

// ── IdMapStore ───────────────────────────────────────────────────────────────

#[derive(Default)]
struct MemIdMap(RwLock<HashMap<(String, String, String), String>>);

#[async_trait]
impl IdMapStore for MemIdMap {
    async fn get(&self, connector: &str, collection: &str, canonical_id: &str)
        -> Result<Option<String>, PortageError>
    {
        Ok(self.0.read().await
            .get(&(connector.into(), collection.into(), canonical_id.into()))
            .cloned())
    }

    async fn insert(&self, connector: &str, collection: &str,
                    canonical_id: &str, external_id: &str, _source: &str)
        -> Result<(), PortageError>
    {
        self.0.write().await
            .insert((connector.into(), collection.into(), canonical_id.into()),
                    external_id.into());
        Ok(())
    }

    async fn register(&self, connector: &str, collection: &str,
                      canonical_id: &str, external_id: &str)
        -> Result<(), PortageError>
    {
        let mut map = self.0.write().await;
        let key = (connector.into(), collection.into(), canonical_id.into());
        match map.get(&key) {
            Some(existing) if existing != external_id =>
                Err(PortageError::MappingConflict { canonical_id: canonical_id.into() }),
            _ => { map.insert(key, external_id.into()); Ok(()) }
        }
    }
}
```

Wiring them into a connector (tests / one-shot scripts only — use `JsonFileStore` for anything that needs to survive a restart):

```rust
use portage::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let connector = Connector::from_file("hubspot.yaml")
        .checkpoint_store(MemCheckpoints::default())
        .id_map_store(MemIdMap::default())
        .build()
        .await?;

    // Stateful ingestion — watermark persisted across calls via MemCheckpoints
    let records = connector.read("contacts").collect().await?;
    println!("fetched {} contacts", records.len());

    // Writeback — id_map lookups/writes go through MemIdMap
    connector.write("contacts").upsert(WriteRecord {
        canonical_id: "acct-789",
        data: json!({ "email": "alice@example.com" }),
        precondition: None,
    }).await?;

    Ok(())
}
```

For a completely stateless one-shot fetch (no watermark, no writeback), neither trait
is needed — `Connector::from_file("hubspot.yaml").await?` with no chained options
works as shown in the earlier examples. The `.build()` form is only required when you
want durable checkpoints or writeback ID tracking. For durable standalone use, pass a
`JsonFileStore` instead of the `HashMap` impls shown above.

### Connector YAML

A YAML file declares the base URL, auth, and a map of **collections** — one entry per
entity type. Each collection has a `list` block (bulk ingestion) and optional write
operation blocks (`create`, `update`, `delete`, `upsert`) at the same level.

```yaml
# stripe.yaml
base_url: https://api.stripe.com/v1
headers:
  Authorization: "Bearer {{ env('STRIPE_SECRET_KEY') }}"

collections:
  charges:
    list:
      url: /charges
      params:
        limit: 100
      records_at: data
      id_field: id
      updated_field: created
      since_param: "created[gte]"
      next_page_link: "{{ body.next_page }}"
      next_page_param: starting_after
      termination: [next-page-link-empty]

  customers:
    list:
      url: /customers
      params:
        limit: 100
      records_at: data
      id_field: id
      next_page_link: "{{ body.next_page }}"
      next_page_param: starting_after
      termination: [next-page-link-empty]
```

See §5 for the full config reference including auth and writeback.

---

## 4. Core Concepts

### 4.1 Connector

A *connector* describes one external system — a YAML file loaded via
`Connector::from_file` or `Connector::from_str`. Config files are safe to commit to
version control — **no credentials inline**, only `env('VAR_NAME')` references or
`$SECRET(name)` lookups resolved at startup.

The connector file describes *what* to fetch and *how*. **Scheduling — how often to
run — is the caller's responsibility** and is not expressed in the connector file. The
embedding application (daemon, cron job, pg-tide-relay task) controls invocation
frequency. The CLI accepts `--interval` for simple polling loops.

### 4.2 Collection

A named entity type within a connector (e.g., `contacts`, `companies`, `deals`).
In REST a collection is the plural endpoint (`/contacts`, `/companies`) that returns
a set of member objects. Each collection independently declares a `list` block (bulk ingestion) and optional
write operation blocks (`create`, `update`, `delete`, `upsert`). A collection with only
a `list` block is ingestion-only; write-only collections (without `list:`) are also
supported.

`list:` always addresses the **list endpoint** — the paginated bulk fetch that returns
many records. Single-record GET by external ID is declared in a top-level `fetch:`
block, shared by three consumers: stub list expansion, event re-fetches
(`payload_type: notification`), and write pre-flight checks (`conflict_detection:
preflight`). `list.url` never contains `{{ id }}`.

Checkpoints are tracked per `(connector, collection)` pair.

An optional `events` sub-block enables processing push events from any source that
delivers to pg-tide's inbox (webhook, message queue, CDC, SSE, etc.): the library
extracts the record ID from the event payload and re-fetches the full record from the
API before emitting it downstream (see §5.6).

### 4.3 Checkpoint / Watermark

For incremental sync the library tracks a **watermark** per `(connector, collection)`:

| Watermark type | Example value | Use case |
|----------------|---------------|----------|
| `timestamp` | `2026-05-20T12:34:56Z` | `updated_at >= ${watermark}` |
| `cursor` | `"AoJ5fHR5cGU..."` | Opaque token returned by the API |
| `offset` | `4200` | Integer sequence, offset pagination |

Checkpoints are stored via a pluggable `CheckpointStore` trait. Implementations are
provided by the caller — the quickstart guide shows a minimal `HashMap`-backed
implementation (~10 lines) that is sufficient for tests and one-shot scripts.

### 4.4 Pagination

| Strategy | Mechanism |
|----------|-----------|
| `cursor` | Extract next-page token from response body or header |
| `offset` | Increment offset by page_size each page |
| `link_header` | Follow RFC 5988 `Link: <url>; rel="next"` |
| `page_number` | Increment page counter |

Termination conditions: `empty_results`, `missing_next_link`, `not_full_page`,
`max_pages` (safety cap).

### 4.5 Authentication

| Type | Mechanism |
|------|-----------|
| `oauth2` | `authorization_code` or `client_credentials` with automatic token refresh |
| `api_key` | Injected as header or query parameter |
| `jwt` | Library-signed JWT (RS256/HS256) with configurable claims |
| `custom` | Arbitrary pre-request HTTP step sequence with response extraction |

Credentials are resolved from environment variables, an encrypted database column, or
an external secrets backend — never embedded in connector files.

### 4.6 Incremental Sync

After the initial full sync, subsequent runs send the stored watermark to the API:

```
query_param  →  GET /contacts?updated_since=<watermark>
body_param   →  POST /contacts/search  body: { filter: { updated_at: { gte: <watermark> } } }
sort_filter  →  GET /contacts?sort=updated_at:asc  (stop when records older than watermark)
```

A **lookback window** (e.g., `30s`) re-fetches records near the previous boundary to
handle clock skew and race conditions.

### 4.7 Writeback

The library reads *desired-state* records from a pluggable `WritebackSource` and pushes
them to the API using write operation blocks declared at the collection level:
`create`, `update`, `delete`, `archive`, `upsert`.

#### Conflict Detection (`precondition`)

When `conflict_detection` is `preflight` or `preflight_and_verify`, the writeback
engine performs a GET on the record before writing. The writeback record may carry an
optional `precondition` — a **partial field map** of values that must still hold in
the current API record for the write to proceed:

```json
{
  "data":        { "email": "new@example.com", "phone": "+47 555 0100" },
  "precondition": { "email": "old@example.com" }
}
```

`precondition` is checked field-by-field against the normalized current state (via
`fetch.mapping`). Only the fields present in `precondition` are compared; changes to
any other field never trigger a conflict. This is deliberate: in MDM scenarios an
application owns a small subset of fields (`email`, `phone`) while other systems own
the rest (`lifecycle_stage`, `owner`). Only the owned fields go into `precondition`;
the other systems' changes are invisible to the check.

`precondition` is **optional**. If absent from the message, no field comparison is
made — the pre-flight GET still runs for `preflight_and_verify` but the write always
proceeds.

| `conflict_detection` value | Mechanism |
|----------------------------|-----------|
| `none` | Write unconditionally; no conflict check |
| `etag` | Use HTTP `If-Match: <etag>`; the API performs the CAS check |
| `preflight` | GET current record via `collection.fetch:`; compare `precondition` fields if present; abort on mismatch |
| `preflight_and_verify` | `preflight` + post-write GET (also via `collection.fetch:`) to confirm the write landed |

On mismatch, `conflict_resolution` determines the outcome portage signals to the source via `nack(id, reason)`: `dead_letter`, `last_writer_wins`, or `skip_and_warn`. What the source does with that signal is its own concern — pg-tide's inbox moves the record to a dead-letter table; other sources may retry, log, or discard.

#### Read/Write Asymmetry

REST APIs frequently have asymmetric schemas: the GET response and POST/PATCH payload
use different field names or nesting (e.g., HubSpot returns `properties.firstname` but
accepts `properties: { firstname: … }` in writes). Two mechanisms handle this:

- **`mapping:`** (on `fetch:`) — dotpath rename only (v1): maps response field paths to
  canonical names before comparing against the `precondition`. E.g. `properties.email:
  email` renames the `properties.email` dotpath to the canonical name `email`. Only the
  fields present in `precondition` need entries; unlisted fields pass through unchanged.
  Value transforms (extracting a scalar from a nested object, coercing types) are not
  supported in v1 — only field-path renaming.
- **`payload:` templates** — each write operation (`create`, `update`, …) has its own
  `payload:` template that reshapes canonical desired-state fields into the API’s
  write-specific envelope.

### 4.8 Sub-collections

Some APIs require fetching child records scoped to each parent — order lines per order,
invoice items per invoice, comments per post. Declare `parent:` in the child collection's
`read` block. The engine iterates over the **already-fetched** parent records; the parent
collection is not re-fetched.

```yaml
collections:
  orders:
    list:
      url: /orders
      records_at: data
      id_field: id
      updated_field: updated_at
      since_param: updated_since

  order_lines:
    list:
      parent: orders                      # drives iteration; no re-fetch
      url: /orders/{{ parent.id }}/lines
      records_at: data
      id_field: id
```

For APIs that return stub records in the list endpoint (IDs only, no payload), add a
top-level `fetch:` block to retrieve the full record per ID:

```yaml
collections:
  orders:
    list:
      url: /orders                        # returns [{id: 1}, {id: 2}, …]
      id_field: id
    fetch:
      url: /orders/{{ id }}               # single-record GET; shared by stubs, events, preflight
```

`parent:` and `fetch:` compose: the list returns parent stubs → `fetch:` retrieves
the full parent record → the child collection iterates those full parent records.

### 4.9 Deletion Tracking

Three detection modes:

| Mode | Mechanism |
|------|-----------|
| `diff` | Full sync: compare fetched IDs against the previously seen set; missing = candidate deletion |
| `soft_delete` | Deleted records remain in the list with a marker field; detected during normal incremental sync. Requires `deleted_field` (field name) and `deleted_value` (the exact value — boolean, string, number, or `non_null` — that signals deletion). No heuristics. |
| `api_events` | Deletion events received via pg-tide inbox |

`soft_delete` is the most common real-world pattern — the API never removes the record
from its list endpoint but flags it. The library detects the marker during ingestion
and emits a delete event instead of an upsert.

For APIs with a dedicated deleted-records endpoint (rare — e.g., Zendesk
`GET /deleted_tickets`, Salesforce `getDeleted()`), model it as a **separate
collection** pointing at that endpoint with `diff` mode.

```yaml
collections:
  # soft_delete — timestamp marker (non-null = deleted)
  orders:
    list:
      url: /orders
      id_field: id
      deletion:
        detection: soft_delete
        deleted_field: deleted_at
        deleted_value: non_null     # explicit: any non-null value means deleted

  # soft_delete — boolean flag
  contacts:
    list:
      url: /contacts
      id_field: id
      deletion:
        detection: soft_delete
        deleted_field: archived
        deleted_value: true         # exact value match

  # soft_delete — string enum
  tickets:
    list:
      url: /tickets
      id_field: id
      deletion:
        detection: soft_delete
        deleted_field: status
        deleted_value: "deleted"    # exact string match

  # diff mode — verify via collection.fetch: before emitting delete
  legacy_orders:
    fetch:
      url: /legacy/orders/{{ id }}
      method: GET
    list:
      url: /legacy/orders
      id_field: id
      deletion:
        detection: diff
        verify: true          # requires collection.fetch: to be defined
        deleted_status: 404   # HTTP status from fetch that confirms deletion
```

For `soft_delete`, `deleted_field` and `deleted_value` are both required — there is no
implicit behaviour. The library compares the record's field value against `deleted_value`
exactly; the special keyword `non_null` means "field is present and not null".

For `diff` mode, `verify: true` enables a confirmation step before emitting a delete
event. The library reuses `collection.fetch:` (which must be defined) to fetch the
candidate record. If the response status matches `deleted_status`, deletion is confirmed
and the event is emitted; any other status suppresses it. This avoids false positives
from paginated scan gaps (eventual consistency, page-size changes, etc.).

### 4.10 ID Capture After Insert

**`canonical_id`** is the caller's stable internal identifier for a record — it is
**never assigned by portage**. The calling application includes it in every writeback
record. In an MDM solution this is typically the cluster ID / golden record ID; in
other systems it is whatever the application treats as its authoritative primary key.

When a `create` operation succeeds, the API returns a newly assigned external ID
(e.g., `{ "id": "hs-12345" }`). The library extracts it via `response_id_path` and
persists the mapping `canonical_id → external_id` in the `id_map` table (see §6.4).
Subsequent `update` and `delete` operations look up the external ID from that table
to construct the correct URL.

The presence or absence of an `id_map` entry is cross-checked against the `op` field
in the `WriteRecord`:

| `op` | `id_map` entry | Action |
|------|----------------|--------|
| `create` | absent | Insert → capture and store `external_id` |
| `create` | present | Guard: dead-letter to prevent accidental duplicate |
| `update` | present | Update using looked-up `external_id` |
| `update` | absent | Cannot update without external ID → dead-letter |
| `upsert` | absent | If no `upsert:` block: create branch (uses `create:`) |
| `upsert` | present | If no `upsert:` block: update branch (uses `update:`) |
| `upsert` | either | If `upsert:` block declared: single native-upsert call |
| `delete` | present | Delete using looked-up `external_id` |
| `delete` | absent | Record never created → skip or dead-letter |

**`id_map` is application-writable.** The library only writes to it on a successful
`create`. For records that already exist in the external system — the common case when
first connecting to a live system, or after an MDM match/merge event — the application
must seed the mapping via the `register_mapping` API (see §6.4). The library does not
distinguish library-written rows from application-written rows; both are used
identically for `update` and `delete` routing.

```rust
// Seed a pre-existing mapping (idempotent; errors if a different external_id exists)
connector.register_mapping("contacts", "cust-456", "hs-12345").await?;
```

**MDM match/merge bootstrap pattern:**

1. Ingest records from the external API → published to outbox.
2. MDM consumes the outbox, runs matching logic, assigns a `canonical_id` (cluster ID).
3. MDM calls `register_mapping(connector, collection, canonical_id, external_id)` for
   each matched record — this seeds `id_map`.
4. Writebacks now work for those records, even though portage never
   created them.

**Two-system merge:** when two records from different connectors are merged into one
cluster, register both `(connector_a, collection, cluster_id, ext_id_a)` and
`(connector_b, collection, cluster_id, ext_id_b)`. The same canonical ID maps to both
external systems simultaneously; each inbox targets its own connector, so both can be
updated or one can be deleted (to remove the duplicate from that system) independently.

**Lost-response scenario** — the POST succeeds on the server but the response is
never received (network failure, timeout). The record now exists in the API with no
known ID. A naive retry would create a duplicate. The library handles this as follows:

1. The failed capture is routed to dead-letter with error code `ERR_ID_CAPTURE_FAILED`.
2. The circuit breaker for `(connector, collection, create)` trips immediately —
   no further inserts run until an operator resolves the gap.
3. Recovery: look up the record in the API, insert the `canonical_id → external_id`
   row into `id_map` manually, then reset the breaker.

**Idempotency keys** are the preferred preventive measure when the API supports them.
Declare an `idempotency_key` template on the `create` block; the library sends it as
`Idempotency-Key` (or the API’s equivalent header). On retry, the API returns the
already-created record’s ID rather than inserting a duplicate, making the lost-response
scenario self-healing without operator intervention.

---

## 5. Connector Config Schema

Config format: **YAML** for connector files. **TOML** for daemon/process config
(consistent with pg-tide). A **JSON Schema** is shipped alongside the library for IDE
validation and CI.

### 5.1 Top-Level Structure

```
name           optional   Connector identifier used in checkpoint keys and metric labels (default: filename stem)
description    optional   Human-readable description of this connector
base_url       required   Base URL shared by all collections
headers        optional   Default headers for all requests (template values)
custom_auth    optional   Pre-request token fetch; tokens available as {{ token.* }}
collections    required   Named collection types (see §5.2)
rate_limit     optional   Default rate limit: { requests_per_second: 10, burst: 20 }
retry          optional   Default retry policy
```

**Stripe** — API key, cursor pagination (`starting_after`), incremental by creation timestamp:

```yaml
# stripe.yaml
base_url: https://api.stripe.com/v1
headers:
  Authorization: "Bearer {{ env('STRIPE_SECRET_KEY') }}"

collections:
  charges:
    list:
      url: /charges
      params:
        limit: 100
      records_at: data
      id_field: id
      updated_field: created
      since_param: "created[gte]"
      next_page_link: "{{ body.next_page }}"
      next_page_param: starting_after
      termination: [next-page-link-empty]

  customers:
    list:
      url: /customers
      params:
        limit: 100
      records_at: data
      id_field: id
      next_page_link: "{{ body.next_page }}"
      next_page_param: starting_after
      termination: [next-page-link-empty]
```

**HubSpot** — OAuth2 refresh flow, cursor pagination, ingestion + writeback:

```yaml
base_url: https://api.hubapi.com  # hubspot.yaml
name: hubspot
description: "HubSpot CRM"
custom_auth:
  token_url: /oauth/v1/token
  method: POST
  payload:
    grant_type: refresh_token
    refresh_token: "{{ env('HUBSPOT_REFRESH_TOKEN') }}"
    client_id: "$SECRET(client_id)"
    client_secret: "$SECRET(client_secret)"
  token_field: access_token
  expires_in_field: expires_in
headers:
  Authorization: "Bearer {{ token.access_token }}"
  Content-Type: application/json

collections:
  contacts:
    description: "Contact records"
    list:
      required_scopes: [crm.objects.contacts.read]
      url: /crm/v3/objects/contacts
      params:
        limit: 100
        properties: firstname,lastname,email,phone
      records_at: results
      id_field: id
      updated_field: updatedAt
      since_param: updatedAfter
      next_page_link: "{{ body.paging.next.after }}"
      next_page_param: after
      termination: [next-page-link-empty, empty-result]
    fetch:                             # single-record GET — shared by preflight + events
      url: /crm/v3/objects/contacts/{{ id }}
      params:
        properties: firstname,lastname,email,phone
      mapping:
        properties.email: email
        properties.firstname: first_name
        properties.lastname: last_name
        properties.phone: phone
    create:
      required_scopes: [crm.objects.contacts.write]
      url: /crm/v3/objects/contacts
      method: POST
      payload:
        properties:
          email: "{{ data.email }}"
          firstname: "{{ data.first_name }}"
          lastname: "{{ data.last_name }}"
      response_id_path: id
    update:
      required_scopes: [crm.objects.contacts.write]
      url: /crm/v3/objects/contacts/{{ id }}
      method: PATCH
      patch_mode: diff
      conflict_detection: preflight
      conflict_resolution: dead_letter
      payload:
        properties:
          email: "{{ data.email }}"
          firstname: "{{ data.first_name }}"
    delete:
      required_scopes: [crm.objects.contacts.write]
      url: /crm/v3/objects/contacts/{{ id }}
      method: DELETE
      conflict_detection: none

  companies:
    description: "Company records"
    list:
      required_scopes: [crm.objects.companies.read]
      url: /crm/v3/objects/companies
      params: { limit: 100 }
      records_at: results
      id_field: id
      updated_field: updatedAt
      since_param: updatedAfter
      next_page_link: "{{ body.paging.next.after }}"
      next_page_param: after
      termination: [next-page-link-empty, empty-result]
```

**WaveApps** — GraphQL API, page-number pagination, Bearer token:

```yaml
# waveapps.yaml
name: waveapps
description: "Wave financial accounting (GraphQL)"
base_url: https://gql.waveapps.com
headers:
  Authorization: "Bearer {{ env('WAVE_TOKEN') }}"

collections:
  invoices:
    description: "Customer invoices"
    list:
      url: /graphql/public
      method: POST           # all GraphQL requests are POST to one endpoint
      body:
        query: |
          query Invoices($businessId: ID!, $page: Int!, $pageSize: Int!) {
            business(id: $businessId) {
              invoices(page: $page, pageSize: $pageSize) {
                pageInfo { currentPage totalPages }
                edges {
                  node {
                    id modifiedAt status
                    amountDue { value }
                    customer { id name }
                  }
                }
              }
            }
          }
        variables:
          businessId: "{{ env('WAVE_BUSINESS_ID') }}"
          page: "{{ page }}"
          pageSize: "{{ page_size }}"
      records_at: data.business.invoices.edges[*].node
      id_field: id
      updated_field: modifiedAt
      page_size: 50
      termination: [not-full-page]  # Wave lacks a modifiedAfter filter; fetch all pages
```

Key GraphQL differences: `method: POST` on a single `/graphql/public` endpoint;
`body:` carries both the query string and a `variables` map; `{{ page }}` is injected
into `variables.page` on each request; `records_at` dereferences deeply into the
nested response.

### 5.2 Collection `list` Properties

Collection-level properties (siblings of `list`, `fetch`, and `events`):

| Property | Type | Description |
|----------|------|-------------|
| `description` | string | Optional human-readable label, surfaced in validation output and agent prompts |
| `fetch` | block | Single-record GET by external ID — shared by stub expansion, event re-fetches, and write pre-flight checks. Properties: `url` (template, uses `{{ id }}`), `method` (default: `GET`), `params`, `headers`, `mapping` (dotpath rename only: response field path → canonical name; e.g. `properties.email: email`; used when a write op uses `conflict_detection: preflight`; v1 supports renaming only — value transforms are not supported). Required when `list:` uses stubs, `events.payload_type: notification`, or a write op uses `conflict_detection: preflight`. |

`list` block properties:

| Property | Type | Description |
|----------|------|-------------|
| `url` | template string | URL path — `{{ since }}`, `{{ page }}`, `{{ body.* }}` available |
| `method` | string | HTTP method (default: `GET`) |
| `params` | object | Query parameters; values arealisre template strings |
| `body` | object | Request body for POST/PUT reads; values are template strings; use `{{ cursor }}` for cursor injection (e.g., GraphQL `variables`) |
| `headers` | object | Per-collection header overrides |
| `records_at` | dotpath | Path to the record array in the response (`results`, `data.items`, …) |
| `id_field` | string | Field name of the record’s unique identifier |
| `updated_field` | string | Field name of the record’s last-updated timestamp/cursor |
| `since_param` | string | Query param name to inject the stored watermark |
| `since_header` | string | Header name to inject the watermark (alternative to `since_param`) |
| `next_page_link` | template string | Extract the next-page cursor/URL from the response |
| `next_page_param` | string | Query param to send the extracted cursor on the next request |
| `termination` | list | `empty-result` \| `next-page-link-empty` \| `not-full-page` \| `same-response` |
| `page_size` | integer | Page size; substituted as `{{ page_size }}` in templates |
| `lookback` | duration | Re-fetch window before the watermark to handle clock skew (e.g., `30s`) |
| `required_scopes` | list | OAuth2 scopes merged into the computed token request when this collection is read |
| `parent` | string | Name of a sibling collection whose records drive iteration; `{{ parent.* }}` is available in URL/param templates |
| `deletion` | block | Deletion tracking. `detection` is one of `diff\|soft_delete\|api_events`. For `soft_delete`: `deleted_field` (required) + `deleted_value` (required — scalar or `non_null`). For `diff`: `verify` (bool, requires `collection.fetch:`) + `deleted_status` (HTTP status that confirms deletion). |

Template variables available in `list.url`, `list.params`, `list.headers`:

| Variable | When available |
|----------|----------------|
| `{{ since }}` | Incremental fetch (current watermark, adjusted by `lookback`) |
| `{{ cursor }}` | Current page cursor (null on first page; use in `body:` templates for GraphQL) |
| `{{ page }}` | All paginated requests (0-indexed) |
| `{{ is_first_page }}` | All paginated requests |
| `{{ body.* }}` | All requests after the first page |
| `{{ parent.* }}` | Sub-collection requests — fields from the driving parent record |
| `{{ token.* }}` | After `custom_auth` token fetch |
| `{{ page_size }}` | When `page_size` is set |
| `{{ env('VAR') }}` | At config load time |
| `$SECRET(name)` | Resolved from credential store at startup |

### 5.3 Write Operation Properties

Write operation blocks (`create`, `update`, `delete`, `archive`) are declared as direct
siblings of `list:`, `fetch:`, and `events:` within a collection. Only declare the
operations the API supports.

`conflict_detection` and `conflict_resolution` are per-operation — `update` and `delete`
can carry different policies. `mapping` lives on `fetch:`, not on write ops, since it
describes how to interpret the shared preflight GET response.

**`upsert:` block (native API upsert only):** Declare a `upsert:` block when the API
provides a single endpoint that handles create-or-update in one call (e.g., `PUT
/contacts/{canonical_id}` returning 201 on create, 200 on update, where the caller
supplies the record ID). When `upsert:` is declared, all `op: upsert` messages are
sent to that endpoint regardless of `id_map` state. `upsert:` and `create:`/`update:`
are mutually exclusive within a collection — use one or the other, not both. For APIs
without a native upsert endpoint, declare `create:` + `update:` and send `op: upsert`
messages; the library routes them via id_map presence automatically.

Each write operation block has:

| Property | Type | Description |
|----------|------|-------------|
| `url` | template string | URL path; `{{ id }}` = external record ID, `{{ data.* }}` = desired-state fields |
| `method` | string | HTTP method |
| `payload` | object | Request body; values are template strings |
| `payload_type` | enum | `json` \| `form` (default: `json`) |
| `patch_mode` | enum | `diff` (changed fields only) \| `full` (all fields) |
| `conflict_detection` | enum | `none` \| `etag` \| `preflight` \| `preflight_and_verify` (default: `none`; meaningful on `update`, `upsert`, `delete`) |
| `conflict_resolution` | enum | `dead_letter` \| `last_writer_wins` \| `skip_and_warn` (used when `conflict_detection` is not `none`) |
| `required_scopes` | list | OAuth2 scopes merged into the token request when this specific operation is executed |
| `response_id_path` | dotpath | Where to find the created record's ID in the response (`create` only); persisted to `id_map` |
| `idempotency_key` | template string | Template for the idempotency key header (e.g., `{{ data.canonical_id }}`); prevents duplicate inserts on retry (`create` only) |
| `success_status` | list | HTTP status codes that count as success (default: `[200, 201, 204]`) |

### 5.4 Auth

Connectors support four auth types, all credential-free in config:

| Type | Config |
|------|--------|
| Bearer token | `headers: { Authorization: "Bearer {{ env('TOKEN') }}" }` |
| API key | `headers: { X-API-Key: "$SECRET(api_key)" }` |
| OAuth2 | `oauth2: { grant_type: client_credentials, token_url: ..., default_scopes: [...] }` |
| Custom pre-request | `custom_auth: { token_url: ..., method: POST, payload: {...}, token_field: access_token, expires_in_field: expires_in }` |

#### OAuth2 Scope Computation

For `oauth2` auth, the library computes the token scope set dynamically from the active
collections rather than a static list:

```
token_scopes = oauth2.default_scopes
             ∪ { c.list.required_scopes                          for each active collection c }
             ∪ { op.required_scopes for each write operation op  of each active collection c }
```

`default_scopes` covers API-wide scopes that apply regardless of which collections are
active (e.g., `openid`, `offline_access`). Removing a collection from the connector file
automatically removes its scopes from the next token request. For `custom_auth` and
header-based auth, `required_scopes` are informational only — they document required
permissions but are not injected into the token request.

Example (Salesforce, `client_credentials`):

```yaml
name: salesforce
auth:
  type: oauth2
  oauth2:
    grant_type: client_credentials
    token_url: https://login.salesforce.com/services/oauth2/token
    default_scopes: [api, refresh_token]
    credential_ref: salesforce_oauth

collections:
  contacts:
    description: "Contact records"
    list:
      required_scopes: [Contact.read]
      ...
    update:
      required_scopes: [Contact.write]
      ...
    delete:
      required_scopes: [Contact.write]
      ...
  accounts:
    description: "Account records"
    list:
      required_scopes: [Account.read]
      ...
```

Computed token scopes for a run that reads both collections and writes contacts:
`[api, refresh_token, Contact.read, Contact.write, Account.read]`

### 5.5 Validation

Structural validation (load-time): required fields, URL template syntax, auth shape,
credential reference names.

Connectivity validation (`validate` command): resolve credentials, full auth flow,
dry-run single-page read for each resource.

Machine-readable error format:

```json
{
  "rule_id": "CFG-001",
  "severity": "error",
  "path": "$.collections.contacts.list.next_page_link",
  "message": "template references undefined variable 'body.cursor'",
  "suggested_fix": "Use 'body.paging.next.after' or another valid response path"
}
```

### 5.6 Collection `events` Properties

When a collection receives **push events**, declare an `events` block so the library
knows how to process them. The event source is injected at runtime via the
`EventSource` trait (see §3) — the `inbox_topic` field names the source, which the
caller wires to a concrete `EventSource` impl (e.g. `PgTideEventSource`).

```
payload_type      enum     notification | full_state | partial
                           notification → extract ID, re-fetch full record via collection.fetch:
                           full_state   → forward event payload directly to RecordSink
                           partial      → merge partial payload with re-fetched record
record_id_path    dotpath  Path to the affected record's ID in the event payload
source_key        string   Key used to look up the EventSource impl injected by the caller
debounce          duration Coalesce rapid events for the same record ID (e.g., 2s)
registration      block    Optional — declare how to auto-register the webhook (see §5.7)
```

Example — HubSpot contacts (thin events, re-fetch via detail endpoint):

```yaml
collections:
  contacts:
    list:
      url: /crm/v3/objects/contacts    # list endpoint — always bulk/paginated
      params:
        limit: 100
        properties: firstname,lastname,email,phone
      records_at: results
      id_field: id
      updated_field: updatedAt
      since_param: updatedAfter
      next_page_link: "{{ body.paging.next.after }}"
      next_page_param: after
      termination: [next-page-link-empty, empty-result]
    fetch:                             # single-record GET — shared by events + preflight
      url: /crm/v3/objects/contacts/{{ id }}
      params:
        properties: firstname,lastname,email,phone
    events:
      payload_type: notification
      record_id_path: body.objectId
      inbox_topic: hubspot.webhook.contact
      debounce: 2s
```

When `payload_type: full_state` the event payload is published directly to the outbox
without a re-fetch. `partial` behaves like `notification` but merges the partial
payload with the re-fetched record.

### 5.7 Webhook Registration Properties

Many SaaS APIs require a **bootstrapping step** to register the webhook delivery URL
before events start arriving. The optional `registration:` block under `events:`
declares how to perform that registration using the same auth already configured in the
connector.

portage runs the registration check at connector startup (and optionally on a
schedule), compares the current registration state against expected values, and
auto-registers or re-activates when needed.

```yaml
collections:
  contacts:
    events:
      payload_type: notification
      record_id_path: body.objectId
      source_key: hubspot.webhook.contact
      debounce: 2s
      registration:
        # Find existing registration (GET list, filter by a field value)
        list_url: /webhooks/v3/subscriptions
        filter_field: subscriptionType
        filter_value: "contact.propertyChange"

        # Create if not found or if status check fails
        create_url: /webhooks/v3/subscriptions
        create_method: POST
        create_payload:
          active: true
          subscriptionType: "contact.propertyChange"
          webhookUrl: "{{ webhook_delivery_url }}"

        # Delete stale registration before re-creating (optional)
        delete_url: /webhooks/v3/subscriptions/{{ registration_id }}
        delete_method: DELETE

        id_field: id               # capture registration ID from create response
        status_field: active       # field to check for health
        status_active_value: true  # expected healthy value
        auto_renew: true           # re-register if status_field != status_active_value
        check_interval: 1h         # how often to poll status
```

`{{ webhook_delivery_url }}` is provided by the caller at connector startup:

```rust
let connector = Connector::from_file("hubspot.yaml")
    .webhook_delivery_url("https://my-relay.example.com/inbound/hubspot")
    .stores(&store)
    .build()
    .await?;

// Runs registration check before starting the sync loop
connector.ensure_webhooks_registered().await?;
```

#### Signing secrets

Some APIs (Stripe, Shopify) return a signing secret in the registration response that
must be used to verify incoming webhook payloads. portage captures the secret via
`secret_field` and stores it in the `WebhookStore`:

```yaml
        secret_field: secret   # dotpath into the create response; e.g. Stripe's whsec_…
```

The secret is stored under key `(connector, collection, "signing_secret")` in the
`WebhookStore`. For `JsonFileStore`, it is written to the state directory (file
permissions: 0600). For the pg-tide adapter, it is stored in
`tide.portage_webhook_registrations` and surfaced to the relay for verifying incoming
webhooks.

`WebhookStore` is a narrow trait (get/set a single opaque secret string):

```rust
#[async_trait]
pub trait WebhookStore: Send + Sync {
    async fn get_secret(&self, connector: &str, collection: &str)
        -> Result<Option<String>, PortageError>;
    async fn set_secret(&self, connector: &str, collection: &str, secret: &str)
        -> Result<(), PortageError>;
    async fn get_registration_id(&self, connector: &str, collection: &str)
        -> Result<Option<String>, PortageError>;
    async fn set_registration_id(&self, connector: &str, collection: &str, id: &str)
        -> Result<(), PortageError>;
}
```

`JsonFileStore` implements `WebhookStore` alongside `CheckpointStore` and `IdMapStore`.
The pg-tide adapter's `PgWebhookStore` stores registrations in
`tide.portage_webhook_registrations` (see the integration plan).

#### Registration properties reference

| Property | Type | Description |
|----------|------|-------------|
| `list_url` | string | GET endpoint that returns existing registrations |
| `filter_field` | string | Field name to match against `filter_value` when searching the list response |
| `filter_value` | string | Expected value of `filter_field` for this registration |
| `create_url` | string | POST endpoint to create a new registration |
| `create_method` | string | HTTP method for creation (default: `POST`) |
| `create_payload` | object | Request body; `{{ webhook_delivery_url }}` available |
| `delete_url` | template | DELETE endpoint; `{{ registration_id }}` substituted from stored ID |
| `delete_method` | string | HTTP method for deletion (default: `DELETE`) |
| `id_field` | dotpath | Where to find the registration ID in the create response |
| `status_field` | dotpath | Field in the list/GET response that indicates health |
| `status_active_value` | scalar | Expected value of `status_field` when healthy |
| `secret_field` | dotpath | Optional — captures a signing secret from the create response |
| `auto_renew` | bool | Re-register if status check fails (default: `true`) |
| `check_interval` | duration | How often to poll registration status (default: `1h`) |

---

## 6. Integration with pg-tide

The library integrates at two seams:

```
External API ──pull──▶ portage ──publish──▶ pg-tide outbox ──relay──▶ downstream
External API ◀──push── portage ◀──consume── pg-tide inbox  ◀──relay── upstream
pg-tide event src   ──▶ inbox ──▶ portage [re-fetch if notification] ──▶ outbox
```

Push events from any pg-tide source (webhook, queue, CDC, SSE) land in the inbox.
For **thin notification events** (`payload_type: notification`) portage extracts
the record ID from the event payload and re-fetches the full record from the API before
publishing to the outbox. For **full-state** events the payload is forwarded directly.

### 6.1 Ingestion → Outbox

After fetching each page, the library publishes records to a named outbox:

```sql
SELECT tide.outbox_publish(
    p_outbox_name := 'hubspot_contacts_in',
    p_subject     := 'hubspot.contacts.upserted',
    p_payload     := record::jsonb
);
```

The outbox provides transactional, at-least-once delivery. Checkpoints are only
advanced after the outbox publish is committed, preventing data loss on crash.

### 6.2 Inbox → Writeback

The library polls the inbox and dispatches claimed records to the writeback engine.
The expected message payload format:

```json
{
  "canonical_id": "acct-789",
  "op":           "upsert",
  "data":         { "email": "new@example.com", "phone": "+47 555 0100" },
  "precondition": { "email": "old@example.com" }
}
```

`canonical_id` is the caller’s stable internal key (cluster ID in MDM, natural key
otherwise) — never assigned by portage. `op` is one of `create | update |
upsert | delete`; the library cross-checks it against `id_map` presence before
dispatching (see §4.10 routing table). `data` contains the desired field values.
`precondition` is optional; when present its fields are compared against the
normalized current API state before writing (see §4.7).

```sql
-- claim a batch
SELECT tide.inbox_claim(p_inbox_name := 'hubspot_contacts_out', p_limit := 50);

-- after successful HTTP write
SELECT tide.inbox_mark_processed(p_message_id := $1);

-- after max retries exhausted
SELECT tide.inbox_mark_failed(p_message_id := $1, p_reason := $2);
```

### 6.3 Checkpoint Storage

The `CheckpointStore` trait is the library's only persistence interface for watermarks.
Implementations are provided by the caller; the quickstart guide shows a minimal
`HashMap`-backed example. Durable implementations are provided by adapter crates.

A `CheckpointStore` implementation must persist and retrieve, keyed by
`(connector, collection)`:

| Field | Type | Description |
|-------|------|-------------|
| `connector` | string | Connector name (from YAML `name` or filename stem) |
| `collection` | string | Collection name |
| `watermark` | string | Opaque watermark value (timestamp, cursor, or integer offset) |
| `watermark_type` | enum | `timestamp \| cursor \| offset` |
| `updated_at` | timestamp | When this checkpoint was last persisted |

The `pg-tide-portage` adapter (in the pg-tide repo) ships a `PgCheckpointStore` that
persists into a PostgreSQL table it creates and owns. The schema is an implementation
detail of that adapter, not of this library.

For standalone use, `portage-core` ships `JsonFileStore` (see §6.4).

### 6.4 ID Map Storage

The `IdMapStore` trait persists `canonical_id → external_id` mappings. It is jointly
written by the library (on a successful `create`) and by the application (seeding
pre-existing mappings). A store implementation must support:

| Operation | Description |
|-----------|-------------|
| `get(connector, collection, canonical_id)` | Look up `external_id`; returns `None` if absent |
| `insert(connector, collection, canonical_id, external_id, source)` | Persist a new mapping |
| `register(connector, collection, canonical_id, external_id)` | Upsert; error if a *different* `external_id` already exists |

`source` is informational (`"library"` or `"application"`); the library behaves
identically regardless of which source wrote the row.

The quickstart guide includes a minimal `HashMap`-backed `IdMapStore` example to
illustrate the trait contract.

`portage-core` ships **`JsonFileStore`** — a single struct that implements both
`CheckpointStore` and `IdMapStore` by writing JSON files into a state directory. No
additional dependencies beyond `serde_json` and `tokio::fs` (both already present).
Suitable for single-process use; not safe for concurrent workers sharing the same
directory.

```rust
// Standalone quickstart — state persists across runs in .portage/
use portage::prelude::*;
use portage::stores::JsonFileStore;

let store = JsonFileStore::open(".portage").await?;

let connector = Connector::from_file("hubspot.yaml")
    .stores(&store)          // implements both CheckpointStore + IdMapStore
    .build()
    .await?;

let records = connector.read("contacts").collect().await?;
println!("fetched {} contacts (watermark advanced)", records.len());
```

The `pg-tide-portage` adapter ships a `PgIdMapStore` backed by a PostgreSQL table it
creates and owns. For direct bulk seeding (initial rollout), callers can write to the
backing store natively; `register` is for incremental post-merge use.

---

## 7. Architecture

### 7.1 Repository

**New standalone repository** (`trickle-labs/portage`). Ships with its own docs site
(mdBook), CI pipeline, and semver release tags.

**portage has no PostgreSQL dependency.** All external bindings — checkpoint
storage, outbox publishing, inbox consumption — are expressed as Rust traits that
the caller implements and injects. The quickstart guide shows minimal copy-pasteable
implementations; durable stores are provided by adapter crates or the caller.

**pg-tide depends on portage** — not the other way around. The dependency flows in
one direction only: portage knows nothing about pg-tide. The pg-tide adapter crate
(`pg-tide-portage`) lives in the pg-tide repo alongside its other adapters (Kafka,
NATS, etc.):

```toml
# trickle-labs/pg-tide — pg-tide depends on portage
[dependencies]
portage     = { git = "https://github.com/trickle-labs/portage", optional = true }
pg-tide-portage  = { path = "../pg-tide-portage", optional = true }  # adapter crate, same repo
```

**Primary deliverable: embeddable Rust library.** No standalone daemon binary — callers
drive the sync loop (e.g., pg-tide-relay calls into the library; integrators run it as
a Tokio task).

### 7.2 Language

**Rust.** Rationale: type-safe config structs (shared with pg-tide-relay via Cargo),
native async (tokio), easy reqwest/hyper HTTP, and the pg-tide adapter is a simple
Cargo feature flag rather than a language boundary. The grove/in-and-out Python project
was the original design inspiration; the portage YAML schema diverges where Rust idioms
or pg-tide integration patterns suggest a better design.

### 7.3 Crate / Module Layout

```
trickle-labs/portage/                   # standalone repo — no PostgreSQL dependency
├── Cargo.toml                    # workspace
│
├── portage-core/               # config types, engine, pagination, auth, traits, JsonFileStore
│   └── src/
│       ├── config.rs             # ConnectorConfig, CollectionConfig, AuthConfig, …
│       ├── auth.rs               # OAuth2, ApiKey, JWT, custom pre-request flows
│       ├── pagination.rs         # Cursor, Offset, LinkHeader, PageNumber
│       ├── checkpoint.rs         # CheckpointStore trait
│       ├── id_map.rs             # IdMapStore trait
│       ├── stores/
│       │   └── json_file.rs      # JsonFileStore: CheckpointStore + IdMapStore via state dir
│       ├── sink.rs               # RecordSink trait (outbox, stdout, custom)
│       ├── source.rs             # WritebackSource trait (inbox, stream, custom)
│       ├── client.rs             # HTTP execution engine (reqwest)
│       ├── ingestion.rs          # Pull loop, watermark advance, page assembly
│       ├── writeback.rs          # Desired-state consumer, write operations
│       └── error.rs              # PortageError
│
├── connectors/                   # Connector YAML examples (.example.yaml, user-provided)
│   ├── hubspot.example.yaml      # full ingestion + writeback
│   ├── stripe.example.yaml       # simple cursor pagination + API key
│   └── tripletex.example.yaml    # custom_auth example
│
├── schemas/
│   └── connector.schema.json     # JSON Schema for the connector YAML format
│
└── book/                         # mdBook documentation source
    ├── src/quickstart.md         # README headline: YAML-first, 5-line config + 3-line Rust
    └── src/config.md             # Connector YAML reference
```

The pg-tide adapter lives in the **pg-tide repo**. It implements `CheckpointStore`,
`RecordSink`, and `WritebackSource` on top of pg-tide's outbox/inbox tables:

```
trickle-labs/pg-tide/
└── pg-tide-portage/                 # adapter crate — pg-tide depends on portage
    └── src/
        ├── checkpoint.rs         # PgCheckpointStore: CheckpointStore over pg-tide tables
        ├── outbox.rs             # PgTideOutboxSink: RecordSink → outbox publish
        └── inbox.rs              # PgTideInboxSource: WritebackSource → inbox claim
```

### 7.4 Key Design Properties

- **No code per connector** — the YAML file is the entire integration definition.
- **Checkpoint-before-advance** — watermark only moves forward after successful
  downstream commit (outbox publish or writeback ack), preventing data loss.
- **Configurable checkpointing granularity** — `every_n_records: 500` or
  `every_n_pages: 10` to trade durability for throughput.
- **Circuit breaker per (connector, collection)** — trips on repeated errors or
  anomalous empty-result runs (shrink protection).
- **Hierarchical config overrides** — process defaults → connector-level overrides → per-operation overrides.
- **`tracing`-based instrumentation** — every outgoing HTTP request is wrapped in a `tracing::Span` carrying `connector`, `collection`, and `operation` fields. The embedder owns the subscriber; the library never calls `init()` or sets a global logger. Enables the embedder to route library events through whatever sink they use (stdout JSON, Loki, Datadog, etc.).
- **`User-Agent` header** — all requests carry `User-Agent: portage/<version>` by default, overridable per-connector. Allows identification of library traffic in API gateway access logs and rate-limit dashboards on the remote side.

### 7.5 Logging & Tracing

The library uses the [`tracing`](https://docs.rs/tracing) crate throughout.
`reqwest` is compiled with its `tracing` feature so connection-level events (DNS,
TCP connect, TLS handshake) are also emitted as `tracing` events at `TRACE` level.

**Span hierarchy per collection read:**

```
collection_sync{connector="hubspot", collection="contacts", op="read"}
  page{page=0, url="/crm/v3/objects/contacts?after=…"}
    http_request{method="GET", url="…", status=200, latency_ms=142}
  page{page=1, …}
    http_request{…}
```

**Span hierarchy per writeback operation:**

```
writeback{connector="hubspot", collection="contacts", op="update", canonical_id="acct-789"}
  preflight{url="/crm/v3/objects/contacts/hs-12345"}
    http_request{method="GET", status=200, latency_ms=88}
  write{url="/crm/v3/objects/contacts/hs-12345"}
    http_request{method="PATCH", status=204, latency_ms=103}
```

URL values in spans are **sanitized** before emission: auth headers and query
parameters whose names match common secret patterns (`token`, `key`, `secret`,
`password`, `client_secret`) are replaced with `[REDACTED]`. The raw URL is never
logged.

Log levels used by the library:

| Level | Events |
|-------|--------|
| `ERROR` | Request failures after all retries; circuit breaker open; config errors |
| `WARN` | Retry attempt; `precondition` conflict (skip / dead-letter); anomalous empty result |
| `INFO` | Sync start/finish per collection; records fetched / written count; watermark advance |
| `DEBUG` | Per-page details; write operation outcome; checkpoint persist |
| `TRACE` | Raw HTTP request/response (headers, body excerpt); reqwest connection events |

To see all library traffic in a development setup, set
`RUST_LOG=portage=debug,reqwest=trace`.

---

## 8. Checkpoint Lifecycle

```
FIRST RUN
  ┌─ no checkpoint found
  │   → full sync: fetch from beginning, no watermark filter
  │   → after each checkpoint interval: persist watermark
  └─ on exhaustion: watermark = latest cursor / max(timestamp) seen

SUBSEQUENT RUNS
  ┌─ load checkpoint
  │   → apply lookback: watermark - lookback_window
  │   → send incremental request with adjusted watermark
  │   → advance watermark as pages arrive
  └─ on empty page or missing next-link: sync complete

CRASH RECOVERY
  ┌─ last committed checkpoint loaded
  │   → some records may be re-fetched (at-least-once)
  └─ outbox deduplication / inbox idempotency handles duplicates

CIRCUIT BREAKER
  ┌─ N consecutive errors → open, pause for backoff duration
  └─ anomalous shrink / repeated empty results → open, emit warning
```

### Checkpoint Ordering Contract

portage guarantees that `CheckpointStore::save()` is only called after all
`RecordSink::publish()` calls for the same checkpoint interval have returned `Ok`.

- If `publish()` fails, the checkpoint is **not** advanced — those records will be
  re-fetched on the next run (at-least-once delivery).
- The reverse is never true: the checkpoint cannot advance beyond what the sink has
  committed. Adapter implementations (e.g. `PgTideOutboxSink`) may rely on this
  ordering as a documented API contract, not just an implementation detail.

---

## 9. Phased Delivery

Writeback is in scope from the start but tackled after the ingestion loop is solid.
The primary deliverable is an embeddable library; the CLI is a dev/validation tool.

### Phase 1 — Core Ingestion

Deliverables:
- `Connector::from_file` / `Connector::from_str` API; `.read(name)` → async `RecordStream`
- Connector YAML loader: `base_url`, `headers`, `collections`, `next_page_link`, `since_param`, `id_field`, `updated_field`
- Template engine for URLs, headers, params (`minijinja`); `{{ since }}`, `{{ page }}`, `{{ body.* }}` variables
- HTTP client with bearer token, API key, and basic auth
- Cursor and offset pagination; `next_page_link` expression-based pagination
- `CheckpointStore` trait; `IdMapStore` trait
- `JsonFileStore`: implements both traits via a JSON state directory; zero extra deps; the default for standalone use
- Quickstart guide includes minimal `HashMap`-backed implementations to illustrate how simple the traits are
- Per-collection sync loop driven by caller
- Example configs: stripe.example.yaml, hubspot.example.yaml
- `tracing` instrumentation: `collection_sync`, `page`, and `http_request` spans with `connector`/`collection`/`op` fields; URL sanitization for auth params; `User-Agent: portage/<version>` on all requests
- Unit tests + mock HTTP server integration tests (wiremock)

### Phase 2 — Custom Auth + Full Pagination

Deliverables:
- `custom_auth` block (pre-request token fetch, `expires_in`/`expires_at`, `initial_refresh_token`)
- OAuth2 client_credentials + authorization_code
- All pagination termination strategies: `empty-result`, `next-page-link-empty`, `not-full-page`, `same-response`
- Link-header pagination (`Link: <url>; rel="next"`)
- JSON Schema + golden fixtures (valid + invalid + expected errors)
- Example configs: tripletex.example.yaml

### Phase 3 — Writeback

Deliverables:
- Flat write operation blocks on collections (`create`, `update`, `delete`, `upsert`, `archive`) as siblings of `list:`, `fetch:`, and `events:`
- `conflict_detection` and `conflict_resolution` per write operation; `mapping:` on `fetch:`
- `WritebackSource` trait (caller-provided stream of desired-state records; `claim()` may block)
- Patch-mode diffing (`diff` vs `full`)
- Dead-letter routing
- `EventSource` trait (distinct from `WritebackSource`; carries raw event payloads for re-fetch flows)
- `RecordSink` trait (`SinkRecord` + `SinkOp`; used by both pull ingestion and event re-fetch paths)

### Phase 4 — pg-tide Integration

Deliverables:
- `pg-tide-portage` adapter crate in the **pg-tide repo**: implements `CheckpointStore`, `RecordSink`, `WritebackSource`, `EventSource`, `IdMapStore`, and `WebhookStore` on top of pg-tide's outbox/inbox tables
- `PgTideOutboxSink`: `RecordSink` impl — publishes ingested records to a named outbox (checkpoint ordering contract: watermark only advances after the outbox insert commits)
- `PgTideInboxSource`: `WritebackSource` impl — claims desired-state records from a named inbox; marks processed/failed after writeback
- `PgTideEventSource`: `EventSource` impl — claims raw push-event records from a named inbox; distinct from writeback
- `PgCheckpointStore`: `CheckpointStore` impl
- `PgIdMapStore`: `IdMapStore` impl; `register()` callable standalone (no `Connector` ref required)
- `PgWebhookStore`: `WebhookStore` impl — stores registration IDs and signing secrets in `tide.portage_webhook_registrations`; signing secrets readable by the relay for inbound verification
- Integration guide in pg-tide's docs

### Phase 5 — Webhook Registration

Deliverables:
- `WebhookStore` trait (`get/set_secret`, `get/set_registration_id`)
- `JsonFileStore` implements `WebhookStore` (secrets at mode 0600)
- `registration:` block in `events:` (§5.7): `list_url`, `filter_field`, `create_url`, `create_payload`, `id_field`, `status_field`, `auto_renew`, `check_interval`, `secret_field`
- `Connector::webhook_delivery_url(url)` builder option
- `connector.ensure_webhooks_registered().await?` — idempotent check-and-register
- Background health-check loop (driven by `check_interval`): re-registers on status failure

### Phase 6 — Hardening & Ecosystem

Deliverables:
- Prometheus metrics (request count, latency histograms, records-per-sync, circuit-breaker state) — additive on top of the `tracing` instrumentation from Phase 1
- OpenTelemetry exporter via `tracing-opentelemetry` bridge — no code change in the library; embedder wires up the OTel subscriber
- Circuit breaker (error threshold, empty-result shrink protection)
- Multi-tenant watermarks
- Schema drift detection (log new/removed fields per sync run)
- API deprecation deadline warnings
- Runtime hot-reload of config files

---

## 10. Open Questions

All architectural decisions are now locked. No open questions remain.

> Previously tracked: Q1 (config format), Q2 (standalone checkpoint storage),
> Q3 (name), Q4 (in-and-out compatibility). All resolved — see §11.

## 11. Locked Decisions

| Decision | Answer |
|----------|--------|
| Language | Rust |
| Repository | New standalone repo (`trickle-labs/portage`) |
| Name | `portage` — portaging carries your boat past an unnavigable barrier; the library does the same for REST APIs |
| Config format | YAML for connector files (better for deep nesting); TOML for daemon/process config (consistent with pg-tide) |
| Standalone storage | `JsonFileStore` ships in `portage-core`: implements both `CheckpointStore` and `IdMapStore` via a state directory; zero extra dependencies; suitable for single-process use |
| No PostgreSQL dependency | Core library is pure Rust / HTTP; all PG bindings are in the `pg-tide-portage` adapter |
| Dependency direction | pg-tide depends on portage; `pg-tide-portage` adapter lives in pg-tide repo |
| Connector schema | Diverges from grove/in-and-out as needed; in-and-out was inspiration only, not a compatibility target |
| Writeback | In scope — design for both ingestion and writeback |
| Primary artifact | Embeddable library; no CLI |
| Connector catalog | User-provided; repo ships `.example.yaml` files only |
| Collection write schema | Flat — `create`, `update`, `delete`, `upsert` are siblings of `list`, `fetch`, `events`; `conflict_detection`/`conflict_resolution` per write op; `mapping:` on `fetch:` |
| Core traits | `CheckpointStore`, `IdMapStore`, `RecordSink`, `WritebackSource`, `EventSource`, `WebhookStore` — all caller-injected; no pg-tide dependency in core |
| Checkpoint ordering | portage guarantees `save()` only called after all `publish()` calls for the interval have returned `Ok`; adapters may rely on this |
