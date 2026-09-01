# pg_tide inbound and outbound connector implementation plan

> **Status:** Proposed
> **Release placement:** Post-v1.0 product expansion, no earlier than v1.1.0
> **Starting point:** v0.54.0 and current `main`
> **Product decision:** Support four connector families with explicit roles:
> PostgreSQL outbox/inbox, NATS JetStream, Apache Kafka, and HTTPS webhooks.
> **Scope rule:** Add NATS, Kafka, and HTTPS webhook sources that deliver only
> to a PostgreSQL inbox. Preserve the current PostgreSQL outbox source and its
> four supported destinations. Do not make every source and sink freely
> composable.

## 1. Outcome

pg_tide currently has a focused forward product: a PostgreSQL outbox is the
source, and PostgreSQL inbox, NATS JetStream, Apache Kafka, or an HTTPS webhook
is the destination. This plan adds the matching reverse product without
changing what an outbox means. NATS, Kafka, and HTTPS gain inbound source roles,
while the PostgreSQL inbox remains the only destination for reverse pipelines.
The result is a four-family connector set with a narrow, supportable route
matrix and one delivery rule: pg_tide acknowledges the external source only
after the inbox transaction commits.

The implementation belongs after v1.0.0. The v0.47.0 contract freeze and the
v0.49.0 product reduction intentionally excluded inbound connectors, fan-in,
and general reverse routing so the first stable release could prove its
outbound behavior. Starting inbound work before the v1.0.0 evidence and release
gates close would reopen that settled scope. The first change under this plan
must therefore record the post-v1 contract decision and use the preserved
`pre-v1-experimental-surface` tag only as research. It must not restore the old
source modules wholesale.

The program is complete when all of the following statements are true:

1. Existing forward configurations continue to run without migration or
   changed defaults. PostgreSQL outbox to PostgreSQL inbox, NATS, Kafka, and
   webhook tests pass with the same checkpoint, retry, DLQ, wire-format,
   metrics, health, and shutdown behavior that v1.0.0 shipped.

2. A user can create, inspect, enable, disable, delete, validate, export, and
   run NATS-to-inbox, Kafka-to-inbox, and webhook-to-inbox pipelines through
   public SQL and CLI paths. These pipelines use the retained
   `tide.relay_inbox_config` catalog rather than a third pipeline registry.

3. Every successful reverse delivery follows the same ordering: receive an
   external record, normalize it into a `RelayMessage`, commit it to the
   PostgreSQL inbox, and only then acknowledge the source. A crash or error at
   any earlier point leaves the source eligible for redelivery.

4. NATS uses a durable JetStream pull consumer with explicit acknowledgement.
   Kafka uses consumer-group assignment and commits the next offset for every
   partition represented in a completed batch. The webhook receiver sends a
   success response only after the inbox commit, including the idempotent case
   where the event was already present.

5. Stable event identity survives retries, relay restarts, NATS redelivery,
   Kafka rebalancing, and repeated webhook requests. The inbox unique
   `event_id` constraint remains the PostgreSQL deduplication boundary. The
   relay does not claim exactly-once transport or exactly-once application
   processing.

6. Direction is visible and correct in ownership locks, runtime status,
   health, logs, metrics, delivery receipts, support bundles, and CLI output.
   Existing metric names and label domains remain compatible; reverse support
   fills existing direction-aware contracts where possible instead of
   introducing a second observability system.

7. Each inbound connector starts as preview and reaches supported maturity
   only after its public-API end-to-end test, crash-window tests, security
   review, performance budget, upgrade test, runbook drill, and independent
   review are recorded in the existing registries and release evidence.

## 2. Product language and route matrix

The product language in `GLOSSARY.md` is already precise and should control
code, configuration, documentation, and operator output. A forward pipeline
moves a message from a PostgreSQL outbox to a sink. A reverse pipeline moves a
message from an external source to a PostgreSQL inbox. A connector is an
adapter for one source or sink role, while a pipeline is the named route and
configuration that uses those roles. An inbox is not an outbox run backward:
the outbox records an application event in the producing business transaction,
and the inbox records a message that entered PostgreSQL from another system.

Use the following connector-family names and roles everywhere. In particular,
do not describe the PostgreSQL outbox as an inbound connector. It is pg_tide's
native forward source, and the PostgreSQL inbox is the reverse destination.

| Connector family | Forward role | Reverse role |
|---|---|---|
| PostgreSQL outbox/inbox | Outbox source and inbox destination | Inbox destination |
| NATS JetStream | Publisher sink | Durable pull-consumer source |
| Apache Kafka | Producer sink | Consumer-group source |
| HTTPS webhook | HTTP client sink | HTTP server source |

The supported route matrix is intentionally closed:

```text
forward:
  PostgreSQL outbox -> PostgreSQL inbox
  PostgreSQL outbox -> NATS JetStream
  PostgreSQL outbox -> Apache Kafka
  PostgreSQL outbox -> HTTPS webhook

reverse:
  NATS JetStream    -> PostgreSQL inbox
  Apache Kafka      -> PostgreSQL inbox
  HTTPS webhook     -> PostgreSQL inbox
```

Configuration validation must reject every other pairing before catalog
mutation and before a worker polls a source. NATS-to-Kafka,
Kafka-to-webhook, broker-to-broker routing, multiple sources in one pipeline,
multiple sinks in one pipeline, and arbitrary connector graphs remain outside
this plan. The `Source` and `Sink` traits may make later combinations possible
internally, but support is a product decision expressed by the route matrix,
not an accidental result of two factory match arms.

## 3. Compatibility and scope guard

### 3.1 Release preconditions

Do not begin implementation until v1.0.0 is tagged and its release evidence is
closed. At that point, record the exact starting commit, the generated
connector registry, the pipeline schema digest, the SQL function inventory,
the supported Cargo profiles, and the required-test manifest. Run the current
contract, lifecycle, connector, documentation, and repository checks before
changing any source or catalog behavior. A pre-existing failure blocks the
first implementation pull request because it would make later compatibility
claims ambiguous.

The baseline command set is:

```bash
just fmt
just lint
just test-unit
just check-connectors
just check-v1-contracts
just check-v1-surface
just check-required-tests
just check-lifecycle-contract
just docs
```

Keep `schemas/v1-contract-manifest.toml`, `connectors.toml`,
`tests/required-tests.toml`, `tests/flake-registry.toml`, and
`docs/runbook-evidence.toml` as the sources of truth they are today. The work
may extend their schemas when a direction field or source evidence is missing,
but it must not introduce another connector list, test registry, runbook
inventory, or release-evidence mechanism.

### 3.2 Existing contracts that remain stable

The public `tide.*` outbox and inbox APIs, the current
`tide.relay_set_outbox_v2()` behavior, pipeline schema defaults, native and
CloudEvents envelopes, CLI JSON fields, health routes, metric names, operator
error codes, and the four forward destinations remain compatible. Existing
pipeline documents with no explicit schema version still normalize to schema
version 1, missing `source_type` still means `outbox` when the source object is
present, and old `config` sink objects still normalize as they do today.

Inbound connector values are an additive post-v1 change to pipeline schema
version 1 because the JSON shape does not change. The contract-change record
must say that `source_type` gains `nats`, `kafka`, and `webhook`, while the
accepted sink for those values is only `inbox`. Do not add a duplicate
`direction` field to pipeline JSON. Direction comes from the owning catalog:
rows in `relay_outbox_config` are forward and rows in `relay_inbox_config` are
reverse. This avoids contradictory documents such as a reverse row whose JSON
says `direction = forward`.

The old `pg_outbox` sink alias remains a forward compatibility alias. New
reverse configurations use the canonical `sink_type = "inbox"` spelling. A
remote PostgreSQL inbox remains valid through `sink.postgres_url`, but the
destination still has to be a pg_tide inbox with the expected schema and
idempotency constraint.

### 3.3 Explicit non-goals

This plan does not add another connector family, revive removed wire formats,
restore fan-in or DAG orchestration, build NATS-to-Kafka routing, add managed
broker provisioning, or promise exactly-once delivery. It does not make the
relay an application processor. Application code still claims an inbox row,
changes business state, and marks the row processed in its own PostgreSQL
transaction.

The first supported NATS source binds to an existing durable consumer. The
first supported Kafka source joins an existing topic through a configured
consumer group but does not create topics. The webhook source owns request
authentication and bounded request handling, but it does not become a general
HTTP gateway, transformation service, or asynchronous job API. A request is
not accepted with `202` for later delivery under this contract.

## 4. Shared reverse delivery contract

### 4.1 Runtime sequence

All three reverse connectors must enter the existing worker at the same
normalized boundary. Connector-specific code receives bytes and transport
metadata, validates the configured wire format, and produces a
`RelayMessage`. The PostgreSQL inbox sink then performs its existing batched
`INSERT ... ON CONFLICT DO NOTHING` transaction. Only after that transaction
returns success may the worker settle the source checkpoint as successful.

```text
external source receives a record
    -> validate transport metadata and size
    -> decode native or CloudEvents envelope
    -> build RelayMessage with stable dedup_key
    -> PostgreSQL inbox transaction commits
    -> acknowledge JetStream messages, commit Kafka offsets,
       or return an HTTP success response
```

The inbox commit is successful both when it inserts the event and when the
same `event_id` already exists. This is necessary for the crash window in
which PostgreSQL committed but the relay died before source acknowledgement.
On redelivery, `ON CONFLICT DO NOTHING` observes the same event identity, the
inbox remains unchanged, and the source can then be acknowledged safely.

### 4.2 Batch and checkpoint model

The current `Source::poll()` returns `Vec<RelayMessage>`, and the coordinator
clones the last message as the checkpoint. `RelayMessage::ack_token` can hold
only an outbox offset. That representation works for the native ordered
outbox, but it cannot safely carry several JetStream acknowledgement handles,
Kafka offsets from several partitions, or a webhook request completion
channel. Transport settlement state also does not belong in the serialized
message envelope.

Refactor `Source` before adding a connector so polling returns a `SourceBatch`
with two fields: normalized messages and an opaque batch checkpoint. The
checkpoint is meaningful only to the source instance that created it. Each
source keeps its pending transport state internally, indexed by that token,
and the coordinator can ask the source to acknowledge or reject the batch
without depending on `async-nats`, `rdkafka`, or HTTP types. Remove
`RelayMessage::ack_token` only after the outbox implementation and all worker
tests use the new contract.

The target shape is conceptually:

```rust
pub struct SourceBatch {
    pub messages: Vec<RelayMessage>,
    pub checkpoint: SourceCheckpoint,
}

pub struct SourceCheckpoint(u64);

#[async_trait]
pub trait Source: Send {
    fn name(&self) -> &str;
    async fn poll(&mut self, batch_size: i64) -> Result<Option<SourceBatch>, RelayError>;
    async fn acknowledge(&mut self, checkpoint: SourceCheckpoint) -> Result<(), RelayError>;
    async fn reject(
        &mut self,
        checkpoint: SourceCheckpoint,
        error: &RelayError,
    ) -> Result<(), RelayError>;
    async fn close(&mut self) -> Result<(), RelayError>;
}
```

The exact Rust names may follow local style, but the ownership rule is fixed.
The coordinator never inspects a checkpoint, and a source never has more than
one unsettled batch in the initial implementation. The outbox source stores
its last offset, NATS stores every message handle in the fetched batch, Kafka
stores the assignment generation and next offset per topic partition, and the
webhook source stores the response sender for its single request. A stale or
foreign checkpoint is a permanent internal error and must not advance any
external position.

`reject` means that the inbox did not commit. For NATS it sends a delayed
negative acknowledgement when the protocol and error classification allow it,
otherwise it leaves the messages unacknowledged for redelivery. For Kafka it
does not commit offsets and pauses or backs off the affected assignment. For a
webhook request it completes the waiting request with a retryable non-success
response. The native outbox implementation keeps the current checkpoint
unchanged. No source treats an in-memory queue insertion as acknowledgement.

### 4.3 Identity and decoding

The native and CloudEvents wire formats remain the only supported inbound
formats. A native message must supply a stable `dedup_key`, subject or event
type, and JSON payload. A CloudEvent must supply its `id`, `source`, `type`,
and JSON data according to the existing CloudEvents contract. The decoder maps
that identity into the inbox `event_id` and preserves safe transport metadata
in inbox headers without placing credentials, authorization headers, TLS
details, or unbounded broker headers into PostgreSQL.

For NATS and Kafka, transport position provides a deterministic fallback
identity when the configured inbound mode permits payloads without an
application identity. NATS uses a stable source namespace, stream name, and
stream sequence. Kafka uses a stable source namespace, topic, partition, and
offset. The source namespace is an immutable non-secret configuration value so
two clusters feeding one inbox cannot collide and a pipeline rename does not
change deduplication. Webhook requests have no broker position, so they must
provide a native deduplication key, a CloudEvents `id`, or the configured
idempotency header. The receiver rejects a request with no stable identity; it
must never generate a random ID for a retryable webhook.

Malformed envelopes, oversized records, missing identities, and invalid event
types fail before the inbox call. Logs and DLQ records may include bounded
source coordinates, but not message payloads by default. Connector error
mapping uses the existing `ConnectorFailureCode` and `RetryClass` values. Add a
new public error code only when none of the existing bounded categories can
describe an operator action.

### 4.4 Delivery, retry, and DLQ semantics

The reverse success contract is stricter than the worker's general durable
terminal-disposition language. In the initial supported matrix, source
acknowledgement means the inbox committed. Writing a failed inbound message to
the relay DLQ does not silently count as delivery to the inbox and does not
advance a Kafka offset, acknowledge a NATS message, or return HTTP success.
This can block a Kafka partition or repeatedly deliver a poison NATS message,
but it makes data loss an explicit operator decision instead of a side effect
of enabling DLQ.

Add an operator-controlled skip or dead-letter acknowledgement only in a later
contract change that names the source position, records the durable DLQ row,
requires a reason and actor, and makes the resulting source advancement visible
in status and audit output. Until then, a permanent decode or inbox error marks
the pipeline unhealthy and preserves the source position. Webhooks return a
bounded `4xx` for a request the sender must correct and a `5xx` for a transient
relay or inbox failure; neither response claims inbox delivery.

Replay remains source-specific. NATS redelivery and Kafka offset reset use
their broker administration contracts and are documented operations, not an
integer outbox replay cursor passed to every source. Webhook replay belongs to
the sender. The existing `pg-tide replay` path remains limited to native
outbox sources unless a separate post-v1 design defines safe broker replay
commands.

### 4.5 Ordering and backpressure

Reverse pipelines preserve the ordering their source can actually provide.
NATS preserves delivery order for one durable ordered consumer subject to
redelivery. Kafka preserves order within each topic partition and makes no
cross-partition ordering claim. Webhook requests are independent; the initial
implementation processes one request per pipeline worker at a time and makes
no ordering promise between clients. The inbox receives commit order, which is
not presented as a global event order.

Every ingress path is bounded. NATS fetch size is at most the connector and
pipeline batch limit. Kafka polling must keep servicing the consumer often
enough to satisfy `max.poll.interval.ms` even while a PostgreSQL retry is in
progress, and it must pause assigned partitions when the worker's batch is
full. The webhook route uses a bounded channel and a body limit enforced before
JSON parsing. A full queue returns `503 Service Unavailable` with a bounded
`Retry-After`; it does not allocate another unbounded task or accept the body
for later work.

## 5. Catalog, configuration, and connector registry

### 5.1 Reuse the existing direction catalogs

Keep `tide.relay_outbox_config` for forward pipelines and
`tide.relay_inbox_config` for reverse pipelines. The inbox catalog already
exists in fresh-install SQL, upgrade migrations, notification triggers,
lifecycle commands, status queries, RLS policy, audit code, and configuration
export paths. The coordinator currently discovers only enabled outbox rows;
restore inbox-row discovery as a union with a literal `reverse` direction and
retain tenant filtering on both branches.

The coordinator must use `PipelineConfig.direction` rather than hard-coded
`"forward"` values in advisory-lock scope, runtime status, worker logs, metric
labels, delivery transitions, errors, and receipts. Reconciliation keys remain
globally unique because the SQL API already rejects a pipeline name present in
either catalog. Notifications from either table trigger the same bounded
reconcile pass.

Restore `tide.relay_set_inbox_v2(JSONB)` as a new post-v1 public API with a
smaller contract than the removed experimental version. It accepts a pipeline
name, an existing local inbox or remote inbox destination, one of the three
supported source types, a connector-specific source object, batch and retry
settings, an optional native or CloudEvents format, and enabled state. It
always writes canonical `sink_type = "inbox"`; callers cannot select another
reverse sink. It validates the complete route and local inbox existence before
mutation, uses short SPI calls, and returns the existing contextual pg_tide
errors at the SQL boundary.

Existing generic lifecycle functions continue to search both catalogs. Update
their tests so create, enable, disable, delete, get, list, tenant assignment,
template expansion, migration checks, and audit behavior cover a reverse row.
Do not add parallel commands named `inbound-enable` or `reverse-delete` for
operations the current pipeline lifecycle already owns.

### 5.2 Canonical reverse documents

The canonical NATS-to-inbox document has this shape:

```json
{
  "schema_version": 1,
  "source_type": "nats",
  "source": {
    "url": "tls://nats.example:4222",
    "stream": "ORDERS",
    "consumer": "pg-tide-orders",
    "source_namespace": "production-orders",
    "credentials_file": "${file:/run/secrets/nats.creds}"
  },
  "sink_type": "inbox",
  "sink": {"inbox": "orders"},
  "batch_size": 100,
  "wire_format": "cloudevents"
}
```

The canonical Kafka-to-inbox document has this shape:

```json
{
  "schema_version": 1,
  "source_type": "kafka",
  "source": {
    "brokers": "kafka-1.example:9093,kafka-2.example:9093",
    "topic": "orders",
    "group_id": "pg-tide-orders",
    "source_namespace": "production-orders",
    "security_protocol": "SASL_SSL",
    "sasl_mechanism": "SCRAM-SHA-512",
    "sasl_username": "${env:PG_TIDE_KAFKA_USER}",
    "sasl_password": "${env:PG_TIDE_KAFKA_PASSWORD}"
  },
  "sink_type": "inbox",
  "sink": {"inbox": "orders"},
  "batch_size": 100,
  "wire_format": "cloudevents"
}
```

The canonical webhook-to-inbox document has this shape. The process-level
listener address and TLS settings live in relay configuration, while the
pipeline owns only its route, identity, and authentication policy. This lets
one listener serve several paths and prevents each worker from racing to bind
the same socket.

```json
{
  "schema_version": 1,
  "source_type": "webhook",
  "source": {
    "path": "/hooks/orders",
    "idempotency_header": "Idempotency-Key",
    "signature_header": "X-Pg-Tide-Signature",
    "timestamp_header": "X-Pg-Tide-Timestamp",
    "signing_secret": "${file:/run/secrets/orders-webhook}",
    "max_body_bytes": 1048576,
    "request_timeout_secs": 30
  },
  "sink_type": "inbox",
  "sink": {"inbox": "orders"},
  "batch_size": 1,
  "wire_format": "cloudevents"
}
```

Configuration validation rejects unknown fields, insecure production defaults,
empty source namespaces, duplicate webhook paths, a webhook batch size other
than one, missing durable NATS consumer names, Kafka group IDs that would be
shared unintentionally, and connector settings whose feature is not compiled.
Secret references keep using `${env:...}` and `${file:...}` resolution and
masking. Connector constructors receive resolved values, while canonical
exports retain references and never emit secret contents.

### 5.3 Registry and generated artifacts

Keep the current sink descriptors unchanged and add separate source
descriptors for NATS JetStream, Kafka, and webhook ingress. Separate rows are
needed because source and sink roles have different acknowledgement,
backpressure, security, configuration, and evidence contracts even though they
share a connector family. Suggested IDs are `nats-jetstream-source`,
`kafka-source`, and `webhook-source`.

Use the existing `nats`, `kafka`, and `webhook` Cargo features for both roles.
The source implementations use the same client libraries already compiled for
their outbound family, and `axum` is already a runtime dependency. Do not add
`nats-source`, `kafka-source`, or `webhook-source` feature flags unless a
measured artifact or security requirement proves that source and sink code
must be shipped separately. `core` continues to contain PostgreSQL, NATS,
webhook, and diagnostics; `core-kafka` adds both Kafka roles.

Extend `scripts/generate_connector_surface.py` and its checked outputs rather
than editing generated Rust or documentation by hand. Each new source row must
declare maximum record and batch sizes, actual ordering, acknowledgement point,
deduplication identity, retryable and permanent errors, TLS behavior,
authentication modes, backpressure, shutdown behavior, tested service
versions, docs, owner, security contact, and evidence. Preview rows may appear
in post-v1 generated surfaces without being described as supported.

## 6. Foundation workstream

The foundation must land in small changes that preserve all forward behavior.
Its purpose is not to build a general connector SDK. It introduces only the
direction and batch-settlement concepts required by four source types and the
closed route matrix.

### 6.1 Source batch refactor

Add the source-owned batch checkpoint to `pg-tide-relay/src/source/mod.rs`,
move outbox offset settlement into `OutboxPollerSource`, and remove the
transport-only `ack_token` field from `RelayMessage`. Update
`poll_and_decode()`, replay filtering, dry-run behavior, checkpoint commit,
DLQ handling, and shutdown so every exit path either acknowledges, rejects, or
retains the one pending batch. A source must not poll another batch while one
is unsettled.

The first focused test is the existing outbox worker suite plus a new source
contract unit test that proves a successful inbox or external sink publish
advances the outbox offset once, a failed publish does not advance it, and a
failed checkpoint write retries the same messages. This refactor must produce
no connector-registry, SQL, schema, CLI, metric, or user-visible changes. If it
cannot preserve the current crash-window tests exactly, stop and repair the
foundation before adding an inbound module.

### 6.2 Direction-aware discovery and validation

Extend coordinator discovery to load enabled rows from both relay catalogs and
construct `PipelineDirection::Forward` or `PipelineDirection::Reverse` from the
table that owns each row. Replace hard-coded forward labels in ownership and
worker code with one bounded direction method. Route validation accepts only
the seven combinations in section 2, and source/sink factories dispatch only
after that validation succeeds.

Update `PipelineDocument` validation so connector fields are checked against
the descriptor for their role. Today a connector descriptor can be found by
searching either source or sink type, which becomes ambiguous once NATS, Kafka,
and webhook exist in both directions. Add explicit source-descriptor and
sink-descriptor lookups and pass pipeline direction into validation. This is a
small correction to ownership, not a new registry layer.

### 6.3 Inbox and observability contract

Exercise the existing local and remote PostgreSQL inbox sinks as the sole
reverse destination. Add a reverse normalization test that proves event ID,
event type, JSON payload, bounded headers, and duplicate handling produce the
same inbox row for all three source families. Any schema adjustment must be
additive and covered by fresh-install versus upgrade comparison.

Delivery receipts currently assume an outbox-oriented source name. Extend the
receipt and runtime representation with direction and bounded source identity
without reinterpreting existing forward columns. Forward rows keep their
current values. Reverse rows record connector type and a safe coordinate such
as NATS stream sequence or Kafka topic/partition/offset, never credentials or
payloads. If a legacy outbox column cannot describe reverse delivery, make it
nullable for reverse rows and keep its existing forward meaning.

Use existing metrics wherever their names already cover the event. Increment
the consumed counter for external records, retain published/delivered counters
for committed inbox rows according to their documented meaning, and populate
the existing `direction` label with `reverse`. Add a new metric only for an
operator question the current set cannot answer, such as bounded webhook queue
rejections or Kafka rebalance count. Do not add topic, subject, URL, event ID,
or source position as a Prometheus label.

### 6.4 Foundation acceptance

The foundation is complete when forward tests pass unchanged; reverse rows can
be created only through validated public SQL; catalog discovery, ownership,
status, config export, and deletion agree on direction; unsupported routes fail
before mutation; and a fake reverse source proves the exact order
`poll -> inbox commit -> acknowledge`. Separate failure tests must prove that
decode failure, inbox rollback, checkpoint failure, worker cancellation, and
ownership loss do not acknowledge the source.

## 7. NATS JetStream inbound

NATS is the first production slice because a durable pull consumer maps cleanly
to the source polling contract and `async-nats` is already used by the
supported outbound connector. Implement `pg-tide-relay/src/source/nats.rs` by
adapting current client, TLS, credential, and error-classification helpers. Use
the source module from `pre-v1-experimental-surface` as a list of lessons and
tests, but compare every borrowed behavior with current v1 security, schema,
and shutdown rules.

The initial source binds to an existing named JetStream stream and durable pull
consumer configured for explicit acknowledgement. Startup validation fetches
consumer information and rejects incompatible acknowledgement policy, filter,
delivery mode, or limits before the worker reports healthy. pg_tide does not
create, update, or delete the stream or consumer in this phase. That keeps
broker lifecycle under operator control and prevents a typo in relay
configuration from mutating production NATS state.

`poll()` fetches at most the configured batch size with a bounded expiry,
decodes each record, and retains every JetStream message handle in the pending
source batch. After the inbox transaction commits, `acknowledge()` explicitly
acknowledges every message in that batch and waits for the protocol result
needed to make the acknowledgement observable. A partial acknowledgement
failure is reported as unhealthy and the unconfirmed messages remain eligible
for redelivery. Inbox deduplication handles messages whose acknowledgement
reached NATS but whose client response was lost.

NATS identity uses the envelope ID when present. The documented fallback uses
the configured source namespace, stream, and JetStream stream sequence, which
is stable across redelivery. Preserve a bounded allowlist of application
headers plus the subject and sequence as inbox metadata. Authentication
supports the same token, user/password, credentials file, NKey, CA, and client
certificate modes that the current NATS sink can verify. Plain `nats://` stays
behind the existing explicit development-only insecure option.

NATS integration tests must use a real JetStream server and cover happy-path
delivery, duplicate redelivery, a relay restart before inbox commit, a restart
after inbox commit but before acknowledgement, acknowledgement timeout,
consumer deletion, permission denial, invalid credentials, verified TLS,
oversized messages, malformed envelopes, bounded fetch behavior, graceful
drain, and two relay instances contending for one pipeline. The public test
starts with `tide.relay_set_inbox_v2()`, publishes to NATS, observes one pending
inbox row, and verifies the durable consumer position only after PostgreSQL
commit.

NATS reaches supported maturity only when its connector descriptor points to
the public end-to-end test and crash-window evidence, its runbook covers
consumer lag, redelivery, ack pending, credential rotation, and safe relay
restart, and an independent reviewer confirms the source never acknowledges a
record that is absent from the inbox.

## 8. Apache Kafka inbound

Kafka follows NATS because the delivery sequence is similar but the source
checkpoint is a map of partitions, not one scalar offset. Implement
`pg-tide-relay/src/source/kafka.rs` with the existing `rdkafka` feature,
security options, secret handling, and error taxonomy. Disable auto commit and
auto offset store. The source joins the configured group, polls frequently,
stores each message's offset only in the pending in-memory batch, and commits
the next offset for each represented topic partition after the inbox
transaction commits.

The pending checkpoint records topic, partition, highest contiguous processed
offset, and the consumer-group assignment generation. It must never collapse a
multi-partition batch to the last message returned by the client. Before
commit, verify that the checkpoint still belongs to the active assignment. If
a rebalance revoked a partition, treat the commit as unconfirmed and allow the
new owner to redeliver. Inbox identity makes that recovery safe.

The rebalance callback pauses revoked partitions, stops adding records to the
worker batch, and waits only for the bounded in-flight inbox transaction. It
must not block the Kafka client past `max.poll.interval.ms`. Keep polling and
heartbeat responsibilities in the source task if the worker can spend longer
than that interval retrying PostgreSQL. Backpressure pauses assigned partitions
when the single pending batch is full and resumes them only after settlement.

Kafka identity uses the envelope ID when present and otherwise combines the
immutable source namespace, topic, partition, and offset. Preserve topic,
partition, offset, timestamp, and a bounded header allowlist as inbox metadata.
Do not place broker addresses, SASL values, certificate paths, or the entire
header collection in receipts or logs. Reuse the current Kafka sink's verified
TLS, mTLS, SASL/PLAIN, and SASL/SCRAM configuration rules; insecure plaintext
remains an explicit development setting.

Kafka integration tests must run against a real multi-partition KRaft cluster.
They cover all partitions in one batch, offset commits after inbox commit,
restart at both sides of the commit boundary, duplicate records, partition
revocation during an in-flight inbox write, group rebalance between two relay
instances, broker outage, coordinator outage, invalid credentials, ACL denial,
verified TLS, malformed and oversized records, graceful shutdown, and a poison
message that blocks only its partition. Assertions inspect committed offsets
per partition and reconcile them with inbox event IDs; a count-only test is not
sufficient.

Kafka remains in the `core-kafka` profile and reaches supported maturity only
after the multi-partition and rebalance tests are blocking required tests. Its
runbook must explain group lag, assignment, offset reset as an external broker
operation, poison-message diagnosis, credential rotation, and why an inbox row
may already exist when an offset appears uncommitted.

## 9. HTTPS webhook inbound

Webhook ingress is third because it is a push source with listener lifecycle,
request deadlines, and an HTTP response standing in for source
acknowledgement. Do not make each pipeline worker bind a socket. Add one
process-level webhook ingress service that owns the listener and a route
registry. The coordinator registers a bounded channel only while it owns an
enabled webhook pipeline and removes the route before draining that worker.
Requests for missing, disabled, unowned, or draining routes receive a
retryable service response.

The production endpoint must be HTTPS. Support direct TLS with certificate and
key secret references, and document TLS termination at a trusted reverse proxy
only when the relay listener is restricted to the trusted network boundary.
Do not infer trust from `X-Forwarded-*` headers sent by arbitrary clients. The
listener configuration defines bind address, direct TLS or trusted-proxy mode,
header and request timeouts, maximum concurrent connections, and global body
limit. Pipeline configuration defines only path, body limit no larger than the
global limit, identity header, HMAC policy, replay window, and processing
deadline.

Authenticate and reject the request before enqueueing it. The initial
supported method is HMAC-SHA256 over a documented canonical byte sequence that
includes the timestamp and raw request body. Verify with the existing `hmac`
and `sha2` libraries, use constant-time verification, reject timestamps outside
the configured replay window, and never log the signature or secret. Each
pipeline path is unique after normalization. Reject encoded traversal,
ambiguous slashes, query-based routing, unsupported methods, unsupported media
types, chunked bodies beyond the limit, and decompression that could bypass the
limit.

After authentication, the handler sends one request record through the bounded
route channel and waits on a one-shot result from the source checkpoint. Return
`204 No Content` only when the inbox insert or duplicate check committed.
Return `400` or `422` for malformed envelopes or missing stable identity,
`401` for failed authentication, `404` for an unknown route, `405` for the
wrong method, `413` for an oversized body, and `503` with bounded
`Retry-After` when the route is unowned, draining, full, or PostgreSQL is
temporarily unavailable. A processing deadline returns a retryable `503` or
`504`; a later inbox commit is safe because the sender's retry uses the same
event identity.

The webhook source uses `batch_size = 1` initially. This keeps request latency,
response ownership, and error mapping exact. Increase concurrency only after a
benchmark shows the single-worker path misses a recorded product budget and a
design preserves one response per committed inbox transaction. Do not batch
unrelated HTTP requests merely because broker sources use batches.

Webhook tests must use a real network listener and cover direct TLS, trusted
proxy deployment, valid and invalid signatures, stale timestamps, stable
idempotency, body and header limits, slow upload, queue saturation, handler
deadline, unknown and duplicate routes, inbox outage, commit-before-response,
crash after commit before response, graceful listener shutdown, ownership
handoff, and concurrent requests. A timing test must prove that no success
response is observable before the inbox row is committed. The public end-to-end
test creates the route through SQL and sends the same signed request twice,
receiving success both times while observing one inbox row.

## 10. Security, tenancy, and operations

Treat every external source as a trust boundary. Extend the current threat
model with broker impersonation, credential theft, malicious payload size,
header injection, event-ID collision, replay, cross-tenant route access,
webhook denial of service, Kafka group takeover, NATS durable-consumer takeover,
and source acknowledgement without an inbox commit. Each threat must map to a
configuration rule, code control, executable test, or explicit deployment
requirement.

Secret references and file-permission checks remain common. NATS and Kafka
sources share their family's outbound TLS and authentication parsers so the two
roles cannot drift on secure defaults. Webhook ingress adds server certificate
handling and inbound HMAC verification, but it uses the same masking and safe
error rules. Validation and support bundles may report that a credential mode
is configured; they never emit credential values, raw authorization headers,
private keys, signed request bodies, or broker connection strings containing
userinfo.

Tenant filtering applies to both catalogs. A relay started for one tenant must
not discover another tenant's reverse pipeline, register its webhook route,
join its Kafka group, bind its NATS consumer, expose its status, or write to its
inbox. Direction and tenant remain part of advisory-lock scope. RLS, database
roles, catalog grants, audit triggers, and remote inbox credentials receive the
same negative tests used by the forward product.

Health distinguishes process readiness from connector readiness. The process
is ready only after catalog preflight and listener startup. A reverse pipeline
is healthy only while its source is connected or serving, its inbox is
reachable according to current health policy, and it has no unsettled permanent
failure. Status includes bounded lag and source coordinates where protocols
provide them: JetStream pending and ack-pending counts, Kafka assignment and
group lag, and webhook queue depth and rejection count. High-cardinality
coordinates stay out of metric labels.

Graceful shutdown first stops accepting new webhook requests and new broker
fetches, then settles or rejects the current batch within the existing drain
deadline, closes the source, releases pipeline ownership, and closes shared
listeners. Kafka leaves the group cleanly, NATS drains its subscription without
deleting the durable consumer, and webhook handlers still waiting when the
deadline expires receive a retryable response. Forced shutdown must not run an
acknowledgement path after the inbox outcome is unknown.

## 11. Test and evidence strategy

Testing follows the risk boundary rather than the module boundary. Unit tests
cover deterministic validation, route-matrix decisions, identity derivation,
wire decoding, source checkpoint ownership, HTTP signature verification, error
classification, and direction labels. Integration tests use real PostgreSQL,
NATS, Kafka, and HTTP/TLS services. Public-API tests start with SQL instead of
constructing Rust sources directly, because catalog mutation, notification,
discovery, ownership, worker startup, inbox commit, and source acknowledgement
are all part of the product claim.

Add at least these blocking public tests:

| Required test | Claim |
|---|---|
| `public_api_nats_to_inbox_e2e` | SQL-configured JetStream message commits to inbox before explicit ack |
| `public_api_kafka_to_inbox_e2e` | SQL-configured records commit to inbox before per-partition offset commit |
| `public_api_webhook_to_inbox_e2e` | Signed request receives success only after inbox commit |
| `bidirectional_connector_regression` | Existing outbound and new inbound roles run together without registry, metric, or ownership collisions |
| `reverse_upgrade_compatibility` | A supported prior release upgrades without changing forward rows and can add reverse rows afterward |

For each source, test the two defining crash windows. A crash before inbox
commit must leave no acknowledgement and must redeliver. A crash after inbox
commit but before acknowledgement may redeliver, but the unique event identity
must leave one inbox row and allow the later acknowledgement. Inject failures
at named transitions rather than depending on process timing, and verify source
state as well as PostgreSQL state.

Register every blocking command in `tests/required-tests.toml` with zero
retries. Add temporary flake entries only under the existing time-bounded
policy; no connector can reach supported maturity while its correctness test
is quarantined. Extend connector evidence in `connectors.toml`, lifecycle
compatibility in `schemas/lifecycle-compatibility-v1.json`, operational drills
in `docs/runbook-evidence.toml`, and the release index under
`release-evidence/`. Evidence records the exact candidate commit, service
versions, test output, artifact digests, review owner, and unresolved blockers.

Performance qualification uses the reference environment and records forward
regression as well as reverse capacity. Measure inbox commit throughput and
latency for NATS and Kafka, webhook request latency and saturation response,
memory per pending batch, connection counts, source lag recovery after outage,
and graceful-drain duration. Set budgets from measured baselines before calling
a connector supported. Do not invent round-number targets in the plan and then
treat them as evidence.

## 12. Documentation and operator experience

Update the main README and mdBook only when a connector phase has executable
examples. The product overview presents the four connector families and the
closed route matrix, then links to separate source and sink pages for NATS,
Kafka, and webhook because their configuration and acknowledgement semantics
differ. PostgreSQL documentation keeps outbox source and inbox destination as
distinct concepts.

Add source pages under `docs/src/sources/` for NATS, Kafka, and webhook ingress.
Each page describes prerequisites, secure configuration, identity mapping,
wire format, acknowledgement point, ordering, backpressure, retry behavior,
shutdown, status fields, and unsupported behavior. Update the PostgreSQL inbox
page to explain its role in reverse delivery and the transaction application
code should use when processing a pending row. Examples must be runnable and
registered in the current documentation checks.

Add one runbook per inbound connector. The NATS runbook covers consumer
configuration, pending and ack-pending messages, redelivery, permissions, and
credential rotation. The Kafka runbook covers group membership, partition lag,
rebalance, poison records, safe offset inspection, and externally managed
offset reset. The webhook runbook covers TLS, signature failures, replay-window
clock skew, queue saturation, route ownership, load-balancer retries, and
graceful drain. Every runbook states the inbox-first acknowledgement rule and
how to distinguish a harmless redelivery from missing data.

CLI `config validate`, `config export`, `status`, `doctor`, and support bundles
must show reverse pipelines without adding a parallel command family. Human
output may use "inbound" as product language and "reverse" as the formal
pipeline direction. JSON output keeps existing fields and gains only reviewed,
additive fields where current direction fields cannot carry the information.

## 13. Pull request sequence

Keep changes reviewable and preserve a green forward product after each merge.
The following sequence is ordered by dependency, not by an obligation to ship
every item in one release.

1. **Decision and baseline.** Record the post-v1 contract change, exact route
   matrix, baseline digests, preservation-tag review, threats, and initial
   required-test entries as pending. This pull request changes no runtime
   behavior.

2. **Source batch settlement.** Introduce `SourceBatch` and opaque checkpoints,
   migrate the outbox source, remove acknowledgement state from
   `RelayMessage`, and prove unchanged forward behavior with focused unit and
   crash-window tests.

3. **Direction and catalog foundation.** Restore reverse catalog discovery and
   `relay_set_inbox_v2`, make ownership and observability direction-aware,
   enforce the closed route matrix, extend schema validation, and test fresh
   install plus upgrade. No external source is promoted in this pull request;
   feature-gated validation may still reject construction.

4. **NATS preview.** Add the JetStream source, source descriptor, public API
   end-to-end path, security tests, and draft docs. Keep maturity preview while
   crash, operations, and performance evidence is open.

5. **NATS support gate.** Close NATS correctness, security, performance,
   lifecycle, runbook, and independent-review evidence. Change maturity in one
   final generated-surface pull request only after every blocker is closed.

6. **Kafka preview.** Add the consumer source with per-partition checkpoints,
   rebalance handling, the `core-kafka` public test, security tests, and draft
   docs. Keep it preview until multi-partition and two-instance tests are
   blocking and stable.

7. **Kafka support gate.** Close Kafka evidence and promote only the source
   descriptor. Do not couple promotion to unrelated outbound Kafka changes.

8. **Webhook preview.** Add the process-level ingress service, route registry,
   direct TLS and trusted-proxy policy, HMAC verification, bounded handoff,
   public end-to-end test, and draft docs.

9. **Webhook support gate.** Close request-timing, saturation, security,
   shutdown, performance, runbook, and independent-review evidence before
   changing maturity.

10. **Program closeout.** Run the mixed bidirectional regression, adjacent
    upgrade suite, artifact builds for `core` and `core-kafka`, documentation
    checks, support-bundle canaries, and complete release evidence. Update the
    roadmap and changelog with measured results and remaining non-goals.

Each runtime pull request runs `just fmt`, `just lint`, and `just test-unit`,
then the narrow connector and schema tests it changes. SQL-facing changes also
run pgrx tests and fresh-install versus sequential-upgrade comparison. Registry
changes run generation in write mode once and check mode afterward. The final
support gate for a connector runs the full required profile in CI against the
exact candidate artifact.

## 14. File-level implementation map

| Area | Primary files | Planned responsibility |
|---|---|---|
| Source contract | `pg-tide-relay/src/source/mod.rs`, `pg-tide-relay/src/envelope.rs` | Source-owned batches, opaque settlement, transport-free message envelope |
| Native source regression | `pg-tide-relay/src/source/outbox.rs` | Preserve outbox polling, replay, consumer-group, and offset behavior |
| New sources | `pg-tide-relay/src/source/nats.rs`, `source/kafka.rs`, `source/webhook.rs` | Receive, normalize, retain pending settlement state, acknowledge or reject |
| Worker and ownership | `pg-tide-relay/src/coordinator.rs` | Load both catalogs, validate routes, run direction-aware worker, settle batches |
| Webhook listener | `pg-tide-relay/src/main.rs` and a focused ingress module | Own listener, TLS, route registry, bounded request handoff, graceful shutdown |
| Pipeline schema | `pg-tide-relay/src/config/schema_support.rs`, `schemas/pipeline-config-v1.schema.json` | Add source values and role-specific strict validation without changing defaults |
| Connector registry | `connectors.toml`, `scripts/generate_connector_surface.py`, generated outputs | Add source descriptors, profiles, capabilities, docs, and evidence |
| SQL API and catalog | `pg-tide-ext/src/relay.rs`, `sql/` fresh install and next migration | Restore narrow inbox setter and preserve lifecycle, RLS, audit, and notifications |
| Inbox destination | `pg-tide-relay/src/sink/inbox.rs`, `sink/pg_outbox.rs` | Preserve atomic insert and deduplication for local and remote inboxes |
| Wire formats | `pg-tide-relay/src/wire_format/` | Decode native and CloudEvents input into the same normalized message contract |
| Operations | `pg-tide-relay/src/metrics.rs`, status/doctor/config commands, support bundle | Report direction, lag, queue state, safe source coordinates, and failures |
| Required tests | `pg-tide-relay/tests/`, `tests/required-tests.toml` | Unit, service integration, public API, crash, security, lifecycle, and mixed-direction proof |
| Documentation | `README.md`, `GLOSSARY.md`, `docs/src/`, `docs/runbook-evidence.toml` | Product matrix, source guides, inbox processing, runbooks, tested examples |
| Release proof | `release-evidence/`, roadmap and changelog files | Bind claims to candidate, tests, service versions, reviews, and drills |

This map names likely ownership, not a requirement to touch every file. Reuse
existing helpers and generated outputs, and keep each pull request to the files
needed for its proof. In particular, do not copy outbound connector modules to
share a few configuration checks. Extract a helper only when both source and
sink call it and tests prove one security policy for the family.

## 15. Risks and stop conditions

The largest correctness risk is acknowledging a source position that covers
messages the inbox did not commit. Stop a connector phase if a test can produce
that state, if Kafka commits only one partition from a multi-partition batch,
if NATS drops any pending handle before settlement, or if an HTTP success can
race ahead of PostgreSQL commit. Throughput does not justify weakening this
boundary.

The largest compatibility risk is changing forward behavior while making the
worker generic. Stop after the source-batch refactor if existing outbox crash,
replay, DLQ, consumer-group, receipt, or shutdown tests change meaning. Fix the
common contract before adding conditionals around individual regressions.

The largest operational risk is source ownership split between PostgreSQL and
the external system. PostgreSQL advisory locks decide which relay worker owns a
pipeline, while NATS durable consumers and Kafka groups have their own state.
Do not promote a connector until two-instance tests prove that ownership loss,
broker reassignment, and process shutdown converge without silent loss. The
webhook route registry must similarly stop success responses on a process that
no longer owns the pipeline.

The largest security risk is turning the relay into an internet-facing HTTP
service without bounded parsing and authentication. Webhook support remains
preview if direct TLS or the trusted-proxy boundary is ambiguous, request size
can bypass its limit, signatures omit replay protection, secrets appear in
diagnostics, or a tenant can reach another tenant's route.

The program may ship NATS without Kafka or webhook. Each source has its own
support gate and maturity. Do not hold a proven connector indefinitely for the
last one, and do not promote later connectors because the shared worker passed
NATS tests. Kafka partition semantics and HTTP response timing require their
own evidence.

## 16. Final acceptance checklist

- [ ] v1.0.0 is released and the post-v1 contract expansion is approved.
- [ ] The closed seven-route matrix is generated, documented, and rejected
  consistently outside its allowed pairs.
- [ ] Existing forward pipeline documents and behavior require no migration.
- [ ] Source settlement state is no longer stored in `RelayMessage`.
- [ ] Outbox, NATS, Kafka, and webhook sources use one tested source-batch
  contract.
- [ ] Coordinator discovery, ownership, status, health, metrics, receipts, and
  shutdown use the real pipeline direction.
- [ ] `tide.relay_set_inbox_v2()` validates before mutation and writes only
  canonical inbox destinations.
- [ ] NATS explicit acknowledgements occur only after inbox commit.
- [ ] Kafka commits the next offset for every completed partition only after
  inbox commit and survives rebalancing.
- [ ] Webhook success is impossible before inbox commit, and ingress is
  authenticated, replay-bounded, size-bounded, timeout-bounded, and TLS
  protected.
- [ ] Stable identity makes every tested crash-window redelivery idempotent in
  the inbox.
- [ ] Reverse DLQ behavior never acknowledges a source merely because a DLQ
  row exists.
- [ ] Local and remote PostgreSQL inbox destinations pass the same reverse
  normalization and deduplication contract.
- [ ] NATS, Kafka, and webhook public-API end-to-end tests are blocking with
  zero retries before each connector reaches supported maturity.
- [ ] Security, performance, lifecycle, upgrade, runbook, and independent
  review evidence names the exact release candidate.
- [ ] `core` and `core-kafka` artifacts build and pass their required tests.
- [ ] README, source guides, inbox documentation, CLI output, and runbooks use
  the product terminology in section 2.
- [ ] Arbitrary connector composition, fan-in, managed broker provisioning,
  and exactly-once claims remain out of scope.

When these checks pass, pg_tide can accurately say that it supports four
connector families in both product directions: PostgreSQL provides the native
outbox source and inbox destination, NATS and Kafka publish and consume, and
HTTPS webhooks send and receive. The implementation remains deliberately
narrow. Every added path ends at the existing PostgreSQL inbox contract, and
every source keeps responsibility for its own acknowledgement.