# Message Guarantees

pg_tide separates three guarantees: an **atomic outbox write**, **at-least-once
relay transport**, and destination-side deduplication. A destination can provide
an **effectively exactly-once outcome** only when it durably deduplicates the
stable event ID and application processing is idempotent or transactional.
There is no unqualified cross-system exactly-once guarantee.

---

## The Fundamental Problem: Dual Writes

Imagine you're building an e-commerce platform. When a customer places an order, your application needs to do two things: save the order to your PostgreSQL database, and notify the warehouse service that a new order is ready to ship. The naive approach looks straightforward:

```
Application ──INSERT──▶ PostgreSQL  ✓  (order saved)
            ──publish──▶ Kafka      ✗  (network timeout!)
```

The database has the order. Kafka does not. The warehouse never learns about the order. The customer waits indefinitely for a shipment that nobody knows to send.

This is the **dual-write problem**. Any time your application writes to two separate systems — a database and a message broker — there's a window where one write can succeed and the other can fail. No amount of application-level retry logic can fully close this window, because your application itself might crash between the two writes.

The consequences are severe and insidious:

- **Silent data loss** — downstream consumers never see the event
- **Inconsistent state** — the database says one thing, the event stream says another
- **Difficult detection** — unless you actively reconcile both systems, you won't know events were lost
- **Impossible recovery** — once the transaction is committed without the event, you can't retroactively publish it without complex compensating logic

### Why retry logic isn't enough

You might think: "I'll just retry the Kafka publish until it succeeds." But consider what happens if the publish succeeds, then your application crashes before recording that success. On restart, it retries — and now the event is published twice. You've traded message loss for message duplication.

What about the reverse order — publish first, then commit? If the database commit fails after a successful publish, you've sent an event about something that never happened.

There is no safe ordering of two independent writes that guarantees
cross-system exactly-once semantics. The transactional outbox eliminates the
dual write at the application/database boundary.

---

## The Solution: The Transactional Outbox Pattern

The transactional outbox pattern eliminates dual writes by reducing two writes to one. Instead of writing to your database *and* a message broker, you write to your database *only* — and the message goes into a special outbox table within the same transaction as your business data:

```sql
BEGIN;
  -- Your business logic: save the order
  INSERT INTO orders (id, customer_id, total, status)
  VALUES (42, 'cust-123', 99.99, 'confirmed');

  -- Event publishing: same transaction, same database
  SELECT tide.outbox_publish('orders',
    '{"order_id": 42, "customer_id": "cust-123", "total": 99.99, "status": "confirmed"}'::jsonb,
    '{"event_type": "order.confirmed", "correlation_id": "req-abc-789"}'::jsonb
  );
COMMIT;
```

Both the order insert and the message insert succeed or fail together — they're part of the same PostgreSQL transaction. There is no window where one succeeds without the other. If the transaction commits, the message is guaranteed to exist. If it rolls back (for any reason — constraint violation, application crash, network disconnect), the message disappears along with the business data.

A separate **relay process** then reads committed messages from the outbox table
and delivers them to the configured downstream system. The relay retries an
uncommitted batch; the downstream system's acknowledgment and retention define
the transport boundary.

This separation of concerns gives you the best of both worlds:

- **Transactional safety** — your application only writes to one system
- **Guaranteed delivery** — the relay keeps trying until the downstream system acknowledges
- **Decoupled systems** — your application doesn't need to know about broker availability
- **Simple application code** — publishing an event is just a SQL function call

### How pg_tide implements the outbox

When you call `tide.outbox_publish(name, payload, headers)`, pg_tide:

1. **Inserts a row** into `tide.tide_outbox_messages` with your payload and headers
2. **Fires `pg_notify`** (`'tide_outbox_new'`, outbox name) to wake the relay immediately

The outbox messages table stores all messages from all named outboxes in a single table, discriminated by `outbox_name`:

| Column | Type | Purpose |
|--------|------|---------|
| `id` | BIGINT (auto-increment) | Global message ID; per-outbox delivery uses increasing IDs and allows gaps |
| `outbox_name` | TEXT | Routes messages to the correct pipeline |
| `payload` | JSONB | Your event data — whatever you want downstream consumers to see |
| `headers` | JSONB | Metadata: event type, correlation ID, schema version, etc. |
| `created_at` | TIMESTAMPTZ | When the message was published |
| `consumed_at` | TIMESTAMPTZ | Legacy/global-consumer status; not authoritative for native relay delivery |
| `consumer_group` | TEXT | Which consumer group processed this message |

The auto-incrementing `id` column is crucial: it provides a total ordering of messages within an outbox, which the relay uses to guarantee in-order delivery and to track its position.

### The relay loop

The pg-tide relay binary continuously:

1. **Polls** the shared table by logical outbox (`WHERE outbox_name = $1 AND id > last_committed_offset`)
2. **Delivers** each batch to the configured sink (NATS, Kafka, Redis, webhooks, etc.)
3. **Commits the scoped offset** — records `(relay_group_id, pipeline_id, outbox_name)` after sink acknowledgment
4. **Retries uncommitted batches** — native delivery does not write `consumed_at`
5. **Respects retention** — messages older than `retention_hours` are eligible for cleanup

If the relay crashes at any point in this loop, it restarts from the last committed offset and re-delivers any messages that weren't confirmed. This is why the relay provides **at-least-once delivery** — it never skips a message, but it might deliver one twice.

### Retention and cleanup

Each outbox has a configurable retention window. Legacy cleanup uses global `consumed_at`
state; it must not assume that one native pipeline has delivered a row for every pipeline.

```sql
-- Create an outbox with 48-hour retention
SELECT tide.outbox_create('orders', p_retention_hours := 48);
```

The `inline_threshold` parameter is retained compatibility/configuration
metadata. Native publishing does not enforce it as a pending-row cap. A sink
outage therefore accumulates committed rows; reserve disk, alert on exact
pipeline lag, and add application-level admission control when a hard outage
window is required.

---

## The Idempotent Inbox: Catching Duplicates at the Destination

The transactional outbox guarantees that every committed event will be delivered at least once. But "at least once" means duplicates are possible. Consider this scenario:

1. The relay polls the outbox and gets message #42
2. The relay delivers message #42 to the downstream system — success
3. The relay crashes *before* committing offset 42
4. The relay restarts, reads from its last committed offset (41), and delivers message #42 *again*

Without protection at the receiving end, the downstream system processes the same event twice. For an "order confirmed" event, this might trigger two shipments. For a "payment processed" event, it might charge the customer twice.

The **idempotent inbox** solves this. It's a PostgreSQL table with a `UNIQUE` constraint on an event identifier. When a message arrives:

1. The relay attempts an `INSERT` with the event's dedup key as the `event_id`
2. If the key already exists (duplicate delivery), the insert is silently skipped via `ON CONFLICT DO NOTHING`
3. Your application only sees each event once, regardless of how many times it was delivered

### How the inbox works in practice

Each named inbox gets its own message table with this structure:

```sql
CREATE TABLE tide."payment-events_inbox" (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id       TEXT NOT NULL,
    source         TEXT,
    payload        JSONB,
    headers        JSONB,
    received_at    TIMESTAMPTZ DEFAULT now(),
    processed_at   TIMESTAMPTZ,
    retry_count    INT DEFAULT 0,
    last_error     TEXT,
    CONSTRAINT uq_payment_events_event_id UNIQUE (event_id)
);
```

The `UNIQUE(event_id)` constraint is the deduplication mechanism. It's simple, reliable, and leverages PostgreSQL's proven concurrency guarantees.

### Creating an inbox

```sql
SELECT tide.inbox_create('payment-events',
  p_max_retries := 5,
  p_processed_retention_hours := 72,
  p_dlq_retention_hours := 168
);
```

| Parameter | Default | What it controls |
|-----------|---------|-----------------|
| `p_schema` | `'tide'` | Schema where the inbox table lives |
| `p_max_retries` | `3` | How many times processing can fail before the message is considered dead |
| `p_processed_retention_hours` | `72` | How long successfully processed messages are kept (for auditing) |
| `p_dlq_retention_hours` | `0` | How long dead-letter messages are kept (0 = forever) |

### Processing inbox messages

Your application reads from the inbox table and marks messages as processed after handling them:

```sql
-- Read the next batch of pending messages
SELECT id, event_id, payload, headers
FROM tide."payment-events_inbox"
WHERE processed_at IS NULL
  AND retry_count < 5
ORDER BY id
LIMIT 10;

-- After successfully processing a message
SELECT tide.inbox_mark_processed('payment-events', 'evt-001');

-- If processing fails (e.g., external API timeout)
SELECT tide.inbox_mark_failed('payment-events', 'evt-003',
  'Stripe API timeout after 30s');
```

When you call `inbox_mark_failed`, the `retry_count` is incremented and the `last_error` is recorded. Your application can retry later. After `max_retries` failures, the message is effectively in the dead-letter queue — it won't be picked up by normal processing loops.

### The dead-letter queue

Messages that exhaust their retry budget aren't deleted — they remain in the inbox table for investigation. You can query them, examine the error history, and replay them once you've fixed the underlying issue:

```sql
-- Find all dead-letter messages
SELECT event_id, payload, last_error, retry_count
FROM tide."payment-events_inbox"
WHERE processed_at IS NULL
  AND retry_count >= 5;

-- Replay specific messages after fixing the issue
SELECT tide.replay_inbox_messages('payment-events',
  ARRAY['evt-003', 'evt-007', 'evt-012']);
```

Replaying resets the `retry_count` to zero, making the messages eligible for processing again.

### Choosing dedup keys

The `event_id` should be deterministic and unique per logical event. The goal is that the same logical event always produces the same dedup key, regardless of how many times it's delivered:

| Source | Recommended dedup key | Why |
|--------|----------------------|-----|
| pg_tide outbox | `{outbox_name}:{message_id}` | Automatic — the relay generates this |
| Kafka | `{topic}:{partition}:{offset}` | Uniquely identifies a Kafka record |
| NATS JetStream | Message sequence number | Assigned by NATS |
| HTTP webhook | `X-Request-ID` header | Sender-assigned idempotency key |
| Custom sources | Any stable unique identifier | Domain-specific (e.g., `order-42:confirmed`) |

The relay automatically generates appropriate dedup keys based on the source type when operating in reverse mode (external source → inbox).

---

## Effectively exactly-once outcomes at a deduplicating destination

When you combine the transactional outbox, relay offset tracking, and an
idempotent inbox, the destination outcome can be **effectively exactly once**.
The relay transport still permits redelivery:

```
1. Application:  BEGIN; INSERT business_data; outbox_publish(); COMMIT;
       ↓
2. Relay:        Polls outbox → gets messages where id > last_committed_offset
       ↓
3. Relay:        Delivers to sink (e.g., INSERT into inbox with dedup key)
       ↓
4. Relay:        On sink acknowledgment → commit_offset(last_delivered_id)
       ↓
5. Relay:        Records auxiliary delivery evidence; the scoped offset is authoritative
```

Each stage is protected:

| Stage | What could go wrong | Protection mechanism |
|-------|--------------------|--------------------|
| Publish | Transaction rolls back | Message disappears with the business data — correct behavior |
| Relay poll | Relay crashes mid-poll | Restarts from last committed offset — no messages skipped |
| Delivery | Sink temporarily down | Relay retries with exponential backoff — message stays pending |
| Delivery | Relay crashes after delivery but before offset commit | Relay re-delivers on restart; inbox dedup key prevents duplicate processing |
| Offset commit | Database connection lost | Relay reconnects and re-commits — idempotent operation |

### Edge cases handled

**Relay crash after delivery, before offset commit:** The relay successfully
delivered message #42 to the inbox, then crashed before recording offset 42.
On restart, it re-delivers #42. The inbox's UNIQUE constraint catches the
duplicate, so a transactional application can observe one processing outcome.

**PostgreSQL failover:** If the primary PostgreSQL instance fails over to a replica, the relay's advisory locks are automatically released (they're tied to the session). Another relay instance can acquire the locks and resume from the last committed offset. In-flight messages that weren't committed are re-delivered, and the inbox dedup catches any duplicates.

**Sink temporarily unavailable:** The relay retries with exponential backoff
(100ms → 30s with jitter) while the source retains the uncommitted batch.
Once the sink recovers, delivery resumes; retention and source availability
still bound recovery.

**Duplicate outbox_publish calls:** If your application accidentally publishes the same logical event twice (due to a retry at the application level), you can include a deterministic `event_id` in the headers. The inbox dedup key will catch duplicates at the receiving end. Alternatively, design your consumers to be naturally idempotent.

### Guarantees summary

| Component | Guarantee | Mechanism |
|-----------|-----------|-----------|
| Outbox publish | Atomic outbox write | Same PostgreSQL transaction as business data |
| Relay delivery | At-least-once | Retries until sink acknowledges, resumes from last offset |
| Inbox receive | Durable deduplication | UNIQUE constraint on event_id |
| **End-to-end** | **Effectively exactly once, when applicable** | Stable ID + durable deduplication + idempotent processing |

### Limitations and honest caveats

The boundaries are important:

- **Cross-sink atomicity:** If you configure a single outbox to fan out to
  multiple sinks (e.g., Kafka *and* a webhook), one delivery can succeed while
  the other fails. Treat each destination as an independent at-least-once path.

- **External sink semantics:** PostgreSQL inboxes durably deduplicate the stable
  event ID. For external sinks (Kafka, NATS, Redis), the outcome depends on the
  sink's acknowledgment, durability, and deduplication semantics. If a sink
  acknowledges delivery but then loses the message internally, pg_tide cannot
  detect that.

- **Clock skew and retention:** Retention cleanup uses `created_at` timestamps. Extreme clock skew between PostgreSQL nodes could cause premature cleanup of messages that haven't been consumed yet. Always use NTP-synchronized hosts.

- **Transport versus outcome:** A crash after downstream success but before
  checkpoint commit can produce a duplicate with the same stable identity.
  Only durable destination deduplication turns at-least-once transport into an
  effectively exactly-once application outcome.

---

## Reverse Pipeline Guarantees (External Source → Sink)

The guarantees described above cover the forward path: PostgreSQL outbox → relay → downstream sink. pg_tide also operates in **reverse**: an external source (Kafka, NATS, Redis Streams, SQS, webhook, stdin) delivers messages to a pg_tide-managed sink.

The relay's core loop is direction-agnostic: it uses **publish-then-acknowledge**.
The source checkpoint is committed only after the sink confirms receipt. This
gives at-least-once transport; sink-side durable idempotency determines whether
the application outcome is effectively exactly once.

### The publish-then-acknowledge guarantee

```
poll source → publish batch to sink → ack source offset
                    ↑
          only on success. On failure: exponential backoff → retry → DLQ
```

If the relay crashes after a successful sink publish but before acknowledging
the source, messages are re-delivered on restart. The sink must handle this
retry idempotently for an effectively exactly-once outcome.

### The `dedup_key`

Every `RelayMessage` carries a `dedup_key` generated from the source record's stable identity:

| Source | `dedup_key` derivation |
|--------|----------------------|
| Kafka | `{topic}:{partition}:{offset}` |
| NATS JetStream | Stream sequence number |
| Redis Streams | Stream entry ID (`{stream}-{id}`) |
| SQS | Message ID |
| Webhook | `X-Request-ID` header, or SHA-256 of body |
| stdin / file | Line number within the current run |

The `dedup_key` is always included in the outbound message payload as `_dedup_key`. Sinks that support idempotent writes (MongoDB, DuckLake, inbox) use it automatically. Sinks without native dedup (ClickHouse, Snowflake) receive it as a column you can use for query-time deduplication.

### Per-sink idempotency for reverse pipelines

| Sink | On retry | Effective guarantee |
|------|----------|---------------------|
| `inbox` (local pg_tide) | `ON CONFLICT (event_id) DO NOTHING` | **Effectively exactly once** when processing is transactional |
| `pg_outbox` (remote pg_tide inbox) | `ON CONFLICT (event_id) DO NOTHING` on remote | **Effectively exactly once** when processing is transactional |
| `mongodb` | `replaceOne` upsert with `dedup_key` as `_id` | **Durable sink deduplication** |
| `ducklake` (inlined rows) | `_dedup_key` window scan before insert | **Sink-dependent deduplication** |
| `ducklake` (Parquet files) | Deterministic filename `snap_{id}_{hash}.parquet`; S3 `put` is idempotent | **Sink-dependent deduplication** |
| `delta` | Deterministic Parquet path; Delta Log commit is atomic | **Sink-dependent deduplication** |
| `iceberg` | Deterministic manifest paths; REST catalog commit is atomic | **Sink-dependent deduplication** |
| `bigquery` | `insertId` field — BigQuery deduplicates within a bounded window | **Bounded deduplication** |
| `clickhouse` | `ReplacingMergeTree` engine deduplicates eventually (not query-time) | **Eventual deduplication** |
| `kafka` sink | Idempotent producer may reduce broker-side duplicates; no consumer dedup | **At-least-once** |
| `nats` sink | No native dedup | **At-least-once** |
| `snowflake` | Snowpipe Streaming has no client-side dedup | **At-least-once** |
| `redis` | Stream append — duplicates land as separate entries | **At-least-once** |
| `sqs` | Standard: at-least-once; FIFO deduplication is bounded | **At-least-once** |
| `webhook` | Server-defined; `_dedup_key` is in the payload for server use | **At-most-once** (server-dependent) |
| `elasticsearch` | Index with `_id` = `dedup_key` — upsert is idempotent | **Durable sink deduplication** |
| `object-storage` | Deterministic object key per batch; S3/GCS `put` is idempotent | **Durable sink deduplication** |

### When to use inbox vs. direct sink

| Use case | Recommended approach |
|----------|---------------------|
| Downstream app needs to process each event in a transaction | Use `inbox` — SQL `UNIQUE` constraint + `inbox_mark_processed()` give an effectively exactly-once outcome |
| Fan-out to multiple consumers of the same event | Use `inbox` — multiple consumers read the same inbox table |
| Analytics ingestion where query-time dedup is acceptable | Use `ducklake`, `clickhouse`, `bigquery`, etc. directly — lower latency, no PostgreSQL write on the hot path |
| Multi-cluster relay (deliver to a remote pg_tide deployment) | Use `pg_outbox` — writes to a remote inbox via tokio-postgres |
| Low-criticality / best-effort delivery | Any sink — the relay's at-least-once loop is still more reliable than fire-and-forget |

### Limitations specific to reverse pipelines

- **No source-side transaction:** Unlike the forward path (where the outbox publish is part of a PostgreSQL transaction), the reverse path has no equivalent. If the external source duplicates a message upstream of the relay, the relay sees two distinct messages with different offsets. Only the sink's idempotency mechanism (via `_dedup_key`) can catch application-level duplicates.
- **No delivery receipts to outbox:** Reverse pipelines do not write rows to `tide.relay_receipts` by default (there is no outbox to update). Observability is via Prometheus metrics (`relay_messages_published_total`, `relay_delivery_latency_seconds`) and relay logs.
- **Ordered delivery:** The relay preserves batch order within a poll cycle. Across poll cycles, order is preserved for serial sources (single Kafka partition, NATS subject with `DeliverAll`). For partitioned or concurrent sources, downstream order is not guaranteed.

---

## Reverse Pipeline Sink vs. the Inbox→Outbox Bridge Pattern

pg-trickle (the project pg_tide was extracted from) provides **stream tables**: a mechanism where a stream table attached to an outbox can watch an inbox, and any new row inserted into the inbox is automatically republished to the outbox. This creates a two-hop path:

```
External source → relay → inbox (PostgreSQL) → stream table → outbox (PostgreSQL) → relay → sink
```

Compare this with pg_tide's reverse pipeline sink, which is a single relay hop:

```
External source → relay → sink
```

Both patterns route data from an external source to a downstream sink without any application business logic. They are **functionally equivalent** for the simple "move data from A to B" use case. But they have different guarantees, costs, and capabilities — and choosing the wrong one has real consequences.

### The dedup boundary is in a different system

This is the most important structural difference.

In the inbox→outbox bridge, the dedup boundary is a PostgreSQL `UNIQUE(event_id)` constraint on the inbox table. When the relay crashes after delivering to the inbox but before committing the Kafka offset, the re-delivered message hits `ON CONFLICT DO NOTHING` — the inbox row is not inserted a second time, the stream table trigger does not fire, and the outbox never sees a duplicate. **The dedup is enforced by the database engine, at the moment of receipt.** Nothing downstream of the inbox can ever receive a duplicate copy of the same event.

In the reverse pipeline sink, the dedup boundary is inside the target system: a `_dedup_key` column scan for DuckLake inlined rows, a deterministic Parquet filename, a `replaceOne` upsert key in MongoDB, and so on. These mechanisms work, but they are **softer** — they rely on correct implementation in each individual sink, and for some sinks (ClickHouse, Snowflake) they are eventually-consistent rather than immediately enforced.

```
Inbox→outbox bridge:
  Kafka → relay → [PostgreSQL UNIQUE constraint] → ... → sink
                          ↑
                  hard dedup wall — nothing past this point can be a duplicate

Reverse pipeline sink:
  Kafka → relay → [sink-specific dedup via _dedup_key]
                          ↑
                  soft dedup — depends on the sink's implementation
```

### Durability under extended downstream outage

This is where the two approaches diverge most sharply in production.

The inbox→outbox bridge writes messages to PostgreSQL before attempting to deliver them downstream. Once a message clears the inbox, it is durably stored in PostgreSQL — independent of whether the downstream sink (DuckLake, MongoDB, ClickHouse) is available. If the sink is down for hours or days, messages accumulate safely in the outbox table and are delivered in order when the sink recovers. The relay can be down, the sink can be down — the data is not at risk.

The reverse pipeline maintains **Kafka (or NATS, Redis, etc.) as the only durable buffer**. If the sink is unavailable, the relay backs off and retries. Messages remain in Kafka only as long as Kafka's retention policy allows. If the outage lasts longer than the Kafka topic's retention period, messages in the undelivered segment are deleted by Kafka — and they are gone.

| Scenario | Inbox→outbox bridge | Reverse pipeline sink |
|---|---|---|
| Sink unavailable for 2 hours | Messages accumulate in outbox; delivered when sink recovers | Relay backs off; messages safe in Kafka |
| Sink unavailable for longer than Kafka retention | Safe — messages are in PostgreSQL | **Messages lost** — Kafka has deleted them |
| Relay process restarts during backlog | Resumes from outbox offset; no data risk | Resumes from Kafka consumer-group offset; safe if within retention |
| PostgreSQL unavailable | Both paths affected — relay cannot commit offsets | Reverse pipeline unaffected (if sink doesn't need PostgreSQL) |

### Opportunity for SQL processing between receipt and publication

The inbox→outbox bridge passes data through PostgreSQL between the two hops. This opens a window for SQL work that the reverse pipeline cannot do:

- **Enrichment:** join the incoming event against a reference table (`JOIN products ON event->>'product_id' = products.id`) before publishing to the outbox
- **Routing:** publish to different outboxes based on payload content (orders over €1000 go to `high-value-orders`, others to `standard-orders`)
- **Fan-out:** one inbox can drive multiple outboxes and multiple downstream sinks with independent delivery guarantees per branch
- **Aggregation:** accumulate inbox rows and publish a summary to the outbox on a schedule

The reverse pipeline has no equivalent step. The only transforms available are wire-format template variables (`{stream_table}`, `{op}`, `{dedup_key}`) at routing time. For anything more complex, the inbox→outbox bridge is the correct tool.

### Cost and latency comparison

The inbox→outbox bridge writes every message to PostgreSQL twice — once to the inbox, once to the outbox — before the relay delivers it to the sink. For high-throughput analytics ingestion (millions of Kafka events per day landing in DuckLake), this is significant:

- **Storage:** both the inbox table and the outbox table hold the message simultaneously
- **Write load:** two PostgreSQL writes per message on the hot path, plus index maintenance for `UNIQUE(event_id)`
- **Latency:** inbox write → stream table → outbox write → relay poll interval → sink deliver; minimum two PostgreSQL round-trips added to every message

The reverse pipeline adds zero PostgreSQL writes. The relay reads from the source and writes directly to the sink.

### Decision guide

Use the **inbox→outbox bridge** when:

- You need PostgreSQL-strength durable deduplication and the sink does not have
  a strong native dedup mechanism
- You need **durability beyond the Kafka retention window** — the downstream sink could be unavailable for an extended period
- You need **SQL transforms, enrichment, or routing** between receipt and publication
- You need **fan-out** — one incoming event should drive multiple downstream sinks with independent delivery guarantees
- You need a **full PostgreSQL audit trail** of every received message (for compliance, debugging, or replay)

Use the **reverse pipeline sink** when:

- The path is a simple A→B data move with no SQL processing required
- Kafka retention is long enough (or the topic is compacted) that an extended sink outage is not a data risk
- The sink has a strong enough native dedup mechanism (`_dedup_key` in
  DuckLake/MongoDB, idempotent produces in Kafka) for an effectively
  exactly-once application outcome
- You want to minimise PostgreSQL write load — e.g. high-throughput analytics ingestion where the PostgreSQL hop is pure overhead with no business value

> **If in doubt, use the inbox→outbox bridge.** It is more expensive but its guarantees are unconditional. The reverse pipeline sink is an explicit trade-off: lower cost and latency in exchange for accepting sink-dependent dedup and Kafka-bounded durability.

---

## Comparison with Other Approaches


To understand why the transactional outbox pattern is valuable, it helps to see how it compares with alternatives:

### Two-Phase Commit (2PC)

2PC coordinates writes across multiple systems using a prepare/commit protocol. It provides true atomicity but at severe cost: high latency, reduced availability (any participant failure blocks the entire transaction), and complexity. pg_tide avoids 2PC entirely — you write to one system, and the relay handles the rest asynchronously.

### Change Data Capture (CDC) via Debezium

Debezium captures row-level changes from PostgreSQL's WAL (write-ahead log) and publishes them to Kafka. It doesn't require application changes, but you lose control over event format (events mirror table schemas, not business semantics) and require significant infrastructure (Kafka Connect, Kafka cluster, JVM). pg_tide gives you explicit control over what you publish.

### Application-Level Retry with Compensation

Some systems retry failed broker publishes and compensate for duplicates on the consumer side. This "best effort" approach is fragile: it requires every consumer to implement idempotency, provides no centralized dedup mechanism, and becomes increasingly complex as the number of consumers grows. pg_tide centralizes deduplication in the inbox.

### Direct Broker Writes (Accept the Risk)

For non-critical events (telemetry, analytics pings, real-time notifications), some teams accept the dual-write risk and publish directly to a broker. This is valid when message loss is acceptable. pg_tide is for when it isn't.

---

## Practical Patterns

### Publishing multiple events in one transaction

You can publish multiple events atomically:

```sql
BEGIN;
  UPDATE orders SET status = 'shipped' WHERE id = 42;
  UPDATE inventory SET quantity = quantity - 1 WHERE product_id = 'SKU-001';

  -- Both events are published atomically
  SELECT tide.outbox_publish('orders',
    '{"order_id": 42, "status": "shipped"}'::jsonb,
    '{"event_type": "order.shipped"}'::jsonb
  );

  SELECT tide.outbox_publish('inventory',
    '{"product_id": "SKU-001", "quantity_change": -1}'::jsonb,
    '{"event_type": "inventory.decremented"}'::jsonb
  );
COMMIT;
```

### Conditional publishing

Only publish when certain conditions are met:

```sql
BEGIN;
  UPDATE orders SET status = 'confirmed'
  WHERE id = 42 AND status = 'pending'
  RETURNING id INTO affected_id;

  -- Only publish if the update actually changed something
  IF affected_id IS NOT NULL THEN
    PERFORM tide.outbox_publish('orders',
      format('{"order_id": %s, "status": "confirmed"}', affected_id)::jsonb,
      '{"event_type": "order.confirmed"}'::jsonb
    );
  END IF;
COMMIT;
```

### Including correlation IDs for tracing

Pass request or trace IDs through the headers so downstream systems can correlate events:

```sql
SELECT tide.outbox_publish('orders',
  '{"order_id": 42}'::jsonb,
  jsonb_build_object(
    'event_type', 'order.created',
    'correlation_id', 'req-abc-123',
    'trace_id', 'trace-xyz-456',
    'schema_version', '1.0'
  )
);
```
