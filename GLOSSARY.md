# pg_tide Ubiquitous Language

This is the shared vocabulary for pg_tide. Use these words in code, SQL APIs, documentation, issue reports, dashboards, and incident notes so that a reader can tell what part of the system is involved and what guarantee it provides. The glossary describes the system as it exists: PostgreSQL stores the durable message state, and the `pg-tide` relay moves that state across process and system boundaries.

The most important distinction is between a fact being recorded and a message being delivered. An application records a fact in an outbox as part of its business transaction. The relay later transports that fact to a sink, where the receiving system decides what to do with it. Those steps have different failure modes and different guarantees, so our language should keep them separate.

## Application Event

An application event is a durable record that says something meaningful happened in the domain, such as an order being created or a payment being settled. In pg_tide, the event payload is JSONB and may be accompanied by headers that carry metadata such as an event type. The event belongs to the application that produced it; pg_tide provides the durable path for publishing and transporting it. Use "event" when discussing the business fact. Use "message" when discussing a transport record, a batch, or a delivery attempt.

## Business Transaction

A business transaction is the PostgreSQL transaction that changes application data. The application should publish the corresponding outbox event inside this same transaction. That placement is the heart of pg_tide: the database can commit the business row and its event together, or roll back both. A relay is not part of the business transaction and should not be described as if it were. It starts after commit and works from the durable record left behind by the transaction.

## Atomic Outbox Write

An atomic outbox write is the combination of the application data change and `tide.outbox_publish()` in one PostgreSQL transaction. It prevents the classic dual-write gap in which the order exists but the event was never published, or the event was published for a change that later rolled back. Atomicity ends at the database commit. It does not mean that every external sink receives the event in the same transaction, and it does not turn relay transport into exactly-once delivery.

## Outbox

An outbox is a named logical event stream owned by PostgreSQL. Applications publish events to it with `tide.outbox_publish()`, and one or more relay pipelines can read the same stream independently. pg_tide stores named outboxes in the shared `tide.tide_outbox_messages` relation, with configuration in the outbox catalog; creating an outbox does not create a private message table for every name. Say "the `orders` outbox" when referring to a logical stream, and say "the shared outbox relation" when referring to its physical storage.

An outbox is not a pipeline and it is not a sink. The outbox holds committed source data until every relevant retention and consumption rule allows cleanup. A pipeline may be disabled, replaced, or temporarily unable to reach its sink while the outbox remains the source of truth for the event. That separation is what lets applications publish without waiting for downstream systems to be healthy.

## Outbox Message

An outbox message is one durable record in an outbox. It carries a payload, optional headers, an operation such as `insert` or `delete`, a creation time, and the identifiers the relay needs to preserve ordering and identity. A single poll may turn one stored outbox record into several relay messages when the payload represents multiple rows, so do not assume that a database row and a downstream message are always the same unit. When that distinction matters, name the layer explicitly.

## Inbox

An inbox is a named PostgreSQL receiving surface for messages that originate outside the local transaction. The relay writes an incoming message to the inbox, where a unique `event_id` prevents the same logical delivery from being inserted twice. Application processing can then happen in a transaction that marks the inbox row as processed, changes business state, and commits together. An inbox absorbs at-least-once transport duplicates; it does not make an arbitrary external side effect idempotent after the application has called another system.

An inbox is not an outbox in reverse. An outbox records events at the point where a business change is made. An inbox records messages at the point where another system's event enters PostgreSQL. A reverse pipeline connects an external source to an inbox, and application code remains responsible for processing the received row.

## Message

A message is a transport-level representation of an event as it moves through a relay pipeline. In the relay, a message has a stable deduplication key, a subject or event type, a JSON payload, an operation, and source-specific acknowledgement information. A message may be transformed, routed, retried, written to a dead-letter queue, or delivered more than once. Use "event" for the domain fact and "message" for the thing a source emits or a sink accepts.

## Pipeline

A pipeline is a named, catalog-backed route through the relay. It defines the direction of travel, the source or outbox, the sink or inbox, connector configuration, batch settings, and optional transforms, routing, retry, and dead-letter behavior. The relay discovers enabled pipeline configurations from PostgreSQL and reconciles workers to match them. A pipeline is configuration plus its runtime ownership; it is not the worker process itself and it is not the destination system.

A pipeline has one direction. A forward pipeline moves messages from a PostgreSQL outbox to an external sink. A reverse pipeline moves messages from an external source into a PostgreSQL inbox. When describing a route, use the form `source -> transforms -> routing -> sink`, and name the actual boundary on each side.

## Connector

A connector is the relay adapter for one external source or sink technology. It translates between that system's client protocol and pg_tide's source, sink, and message contracts. Connector maturity, feature availability, and evidence are tracked in the generated connector surface. A connector is not the same thing as a pipeline: the connector supplies the capability, while the pipeline supplies the named route and its configuration.

## Source

A source is the boundary from which a relay pipeline obtains messages. In a forward pipeline, the native source polls PostgreSQL's shared outbox relation. In a reverse pipeline, the source may be a broker subscription, webhook receiver, file, or another external system. A source also owns the acknowledgement mechanics for its position, so source polling and source checkpointing should be discussed together.

## Sink

A sink is the boundary to which a relay pipeline delivers messages. In a forward pipeline it may be NATS, Kafka, HTTP, a data lake, or another external destination. In a reverse pipeline the PostgreSQL inbox is the sink. A sink's successful publish is one input to the relay's terminal-disposition decision; it is not automatically the same thing as a committed source checkpoint or a completed business action.

## Fan-out

Fan-out is the intentional delivery of one logical event stream to multiple independent readers. pg_tide supports this through multiple pipelines and consumer groups, each with its own progress and failure state. Fan-out is a topology decision, not a transformation and not a promise that all destinations receive an event at the same time. Describe each route separately when its sink, retention, or delivery guarantee differs.

## Forward Pipeline

A forward pipeline reads a PostgreSQL outbox and publishes messages to an external sink such as NATS, Kafka, an HTTP webhook, or object storage. Its native source reads the canonical shared outbox relation and advances a durable offset after a batch reaches a terminal disposition. The sink may have its own acknowledgement and deduplication behavior, but that behavior is outside the PostgreSQL transaction that created the outbox event.

## Reverse Pipeline

A reverse pipeline reads from an external source such as NATS, Kafka, a webhook receiver, or a file and writes messages to a PostgreSQL inbox. The source supplies a source-specific identity or acknowledgement token where possible. The inbox's unique event identity provides the durable deduplication boundary inside PostgreSQL, while the relay's source checkpoint controls when it is safe to move past the polled batch.

## Relay

The relay is the standalone `pg-tide` process that discovers pipeline configuration, claims pipeline ownership, polls sources, builds messages, applies transforms and routing, publishes to sinks, records terminal outcomes, and advances source checkpoints. It is a delivery worker, not the system of record for application events. If the relay stops, committed outbox messages remain in PostgreSQL and can be delivered after the relay returns, subject to retention and configuration.

A relay instance can own several pipelines and can be replaced by another instance in the same relay group. The process may retry a delivery after a connection failure or after a crash. Such a retry is expected behavior under at-least-once transport, not evidence that the original event was duplicated at its source.

## Graceful Shutdown

Graceful shutdown is the relay sequence for stopping without abandoning work unnecessarily. The relay stops polling, lets current work drain or abort according to its shutdown policy, waits for workers to exit, releases pipeline ownership, and then closes its resources. Shutdown does not delete source messages or reset offsets. A crash is different: the next worker relies on lease expiry, connection cleanup, and the last durable checkpoint to recover.

## Relay Group

A relay group is an independent deployment identity identified by `relay_group_id`. Relay instances with the same group ID coordinate ownership through advisory locks, divide pipeline work between themselves, and share the corresponding native outbox offsets. A different group ID represents a separate consumer of the configured pipelines and therefore has its own ownership and offset namespace. Treat the group ID as operational identity, not as an application consumer group.

## Advisory Lock

An advisory lock is a PostgreSQL lock identified by an application-defined key rather than by a table row. pg_tide uses session-scoped advisory locks to coordinate pipeline ownership within a relay group. The database session holding the lock is part of the ownership evidence, which is why a worker that loses its session cannot keep the pipeline forever. An advisory lock coordinates workers; it does not acknowledge a message and it does not record delivery.

## Pipeline Ownership

Pipeline ownership is the temporary right of one relay worker to process a pipeline for a relay group. PostgreSQL advisory locks prevent two instances in that group from actively processing the same pipeline at once. Ownership can move when a worker exits, loses its database session, or is replaced during reconciliation. Ownership is a coordination fact; it is not a delivery receipt and it does not prove that a downstream sink accepted any message.

## Consumer Group

A consumer group is an application-facing cursor into an outbox. Multiple consumer groups may read the same outbox independently, each at its own pace, without changing the progress of the others. The group has durable offsets and visibility leases, and its consumers use heartbeats while they work. A relay group coordinates relay workers. A consumer group describes an independent reader of event history. These terms are deliberately different even though both use the word "group."

## Consumer

A consumer is a registered worker within a consumer group. It claims work through a visibility lease, sends heartbeats while processing, and releases or completes its claim when the work reaches a durable outcome. Consumer registration and liveness help pg_tide distinguish an active worker from one that has disappeared. A consumer is not the same thing as a relay instance: a relay may own a pipeline, while application workers may consume an outbox through the consumer-group API.

## Offset

An offset is a source position that tells a reader how far it has progressed. For a native outbox pipeline, the offset is scoped by relay group, pipeline, and outbox, so the same outbox can have different positions for different pipelines and deployments. An offset is not a global consumed flag on an outbox row. It answers "where should this reader continue?" rather than "has every reader finished with this row?"

## Checkpoint

A checkpoint is the source position captured by a poll and later committed after the corresponding batch reaches a durable terminal disposition. The relay keeps the checkpoint tied to the original poll even if transforms remove messages or routing changes their destinations. A successful checkpoint commit is evidence that the relay has completed its responsibility for that source batch according to the configured policy. It is not, by itself, proof that every downstream system has performed its business work.

Replay uses a separate, one-shot cursor and is checkpoint-neutral. Reading an old range for replay must not silently move the live checkpoint. An authorized administrative rewind is the explicit operation that changes a live offset, and it should be described as a rewind rather than as an ordinary replay.

## Visibility Lease

A visibility lease is a time-limited claim on work held by a consumer. While the lease is valid, another consumer should not claim the same batch. The consumer renews the lease with heartbeats while it works; if the consumer disappears or fails to complete the work before the lease expires, another consumer can take over. A lease limits concurrent ownership for a period of time. It does not make a downstream operation atomic and it does not remove the need for idempotent processing.

## Batch

A batch is the set of messages returned by one source poll and handled as one acknowledgement unit. The relay may transform, route, or drop individual messages within the batch, but the source checkpoint remains associated with the original poll. Batch size is a throughput and latency setting, not a delivery guarantee. A larger batch can improve efficiency while making retries, dead-letter writes, and recovery more substantial.

## Terminal Disposition

A terminal disposition is a durable outcome that lets the relay advance the source checkpoint for a polled batch. Successful sink delivery is one terminal disposition. An atomic write of the failed batch to the configured dead-letter queue is another. A transient publish error is not terminal: the relay must retry or pause according to policy, because advancing the checkpoint at that point would lose the source messages.

## Circuit Breaker

A circuit breaker protects a failing sink from an endless stream of immediate publish attempts. In the closed state, the relay sends normally. After repeated failures it can open the breaker and fail fast for a period, allowing the sink and the relay to recover without turning one outage into a connection storm. The breaker changes when the relay attempts delivery; it does not change source data, checkpoints, or the configured delivery guarantee.

## Half-Open

Half-open is the circuit-breaker state used to test recovery after an open period. The relay permits a limited probe, often one message or one bounded attempt, instead of reopening the full flow immediately. A successful probe closes the breaker and a failed probe opens it again. Use "half-open" for this operational state, not for a partially committed batch or a consumer with an expired lease.

## At-Least-Once Delivery

At-least-once delivery means a committed source event is retried until the relay records a durable terminal disposition, but the same event may reach a sink more than once. The usual duplicate window is a crash after the sink succeeds and before the relay commits its checkpoint. pg_tide preserves stable identities so downstream systems can deduplicate when they support that operation. At-least-once is a transport guarantee, not a claim that the sink or the application's side effects are automatically idempotent.

## Stable Event Identity

A stable event identity is the identifier that remains the same when the relay retries or replays a logical event. For native forward messages, the identity is derived from the outbox name, outbox record ID, and row index. Reverse sources use a source-specific identity when one is available, such as a broker message ID or source offset. A sink should use this identity for deduplication, idempotent upsert, or correlation. A newly generated UUID on every retry would destroy the information that makes at-least-once transport manageable.

## Deduplication Key

A deduplication key, written `dedup_key` in relay messages, is the stable transport identity used to recognize repeated deliveries. In an inbox table, the corresponding durable uniqueness boundary is `event_id`. The two names describe the same role at different boundaries, but they are not interchangeable columns in every connector. When documenting a source or sink, say which identity is being generated and where it is enforced.

## Effectively Exactly Once

Effectively exactly once is an application outcome assembled from idempotent pieces. It can be achieved when the receiver durably deduplicates the stable event identity and applies its business change transactionally with that deduplication record. pg_tide can provide the durable inbox boundary needed for this pattern. It does not promise exactly-once transport across arbitrary external systems, and it does not make non-transactional side effects, such as sending an email, exactly once by itself.

## Envelope

An envelope is the serialized wrapper around a message. It carries the payload together with transport metadata such as event identity, subject, operation, timestamps, headers, and source information. The envelope gives connectors a common shape without requiring every sink to understand PostgreSQL's internal tables. Encryption may wrap a payload in a versioned envelope as well; in that context, the encryption envelope and the relay wire envelope are related layers, not synonyms.

## Wire Format

A wire format is the serialization contract used when a relay message crosses a connector boundary. pg_tide supports formats including the native JSON envelope, Debezium, CloudEvents, Maxwell, Canal, and custom CDC JSON. The wire format changes how metadata and payload are encoded. It does not change the pipeline's direction, the source checkpoint rules, or the at-least-once delivery model.

## Transform

A transform changes a message before publication. A JMESPath transform may reshape the payload or filter the message out entirely. A transform is part of the pipeline's delivery path and should be deterministic enough that a retry of the same input produces the same intended result. Filtering a message is not the same as acknowledging an unrelated source position: the batch checkpoint still records progress through the source poll.

## JMESPath

JMESPath is the JSON query language used by pg_tide's transform and filter expressions. It can select nested values, build a new payload shape, or determine that a message should be dropped from publication. JMESPath is a tool used inside a transform; it is not the wire format, the routing policy, or the source checkpoint. Document the expression and the expected input shape when a pipeline depends on it.

## Routing

Routing chooses the destination subject, topic, stream, or other sink address from message content and configured rules. It decides where a message goes; it does not change the message's stable identity or turn one pipeline into several independent checkpoints. Use "routing" for that content-based destination decision, and use "fan-out" when describing delivery of one event stream to multiple independent consumers or pipelines.

## Subject Template

A subject template is a configured string with placeholders that the relay resolves into a concrete destination name at runtime. Templates can use message and source values such as the stream table, operation, outbox ID, or refresh ID, depending on the connector. The template is configuration. The resolved subject is the actual destination on a particular delivery attempt. Keeping those names separate makes logs and incident reports much easier to read.

## Token Bucket

A token bucket is the rate-limiting model used by pg_tide. Tokens accumulate up to a configured capacity, allowing a bounded burst, and delivery consumes tokens as messages are sent. Once the bucket is empty, the relay waits for tokens to return instead of exceeding the configured steady-state rate. Rate limiting controls traffic to a sink; it does not alter ordering, deduplication identity, or the source checkpoint.

## NATS JetStream

NATS JetStream is NATS's persistent streaming layer. The pg_tide NATS connector uses JetStream when it needs durable streams, consumer state, and message acknowledgement beyond an ephemeral subscription. JetStream may deduplicate a replay when the stable event identity is sent as `Nats-Msg-Id`, but that downstream feature does not change pg_tide's own at-least-once transport contract. Keep the NATS stream and consumer names in connector configuration separate from pg_tide relay and consumer-group names.

## Discovery and Hot Reload

Discovery is the relay coordinator's process of reading pipeline configuration from the PostgreSQL catalog and reconciling the set of workers it should own. Hot reload is the user-visible result: a supported configuration change takes effect without restarting the relay. LISTEN/NOTIFY can wake discovery promptly, and periodic reconciliation remains the safety net. Hot reload changes pipeline configuration and ownership; it does not rewrite already committed outbox events or reset live offsets.

## Dead-Letter Queue

The dead-letter queue, or DLQ, is the durable PostgreSQL store for a batch that cannot be delivered under its configured failure policy. pg_tide writes the failed batch atomically with idempotency keys before advancing the source checkpoint. Operators can inspect, replay, requeue, or resolve DLQ entries through the SQL and CLI workbench. A DLQ is not a trash can and it is not a substitute for fixing a broken sink: it is a controlled terminal disposition that preserves the message and the failure context for recovery.

## Replay

Replay is a deliberate, bounded reprocessing of an explicit source range or selected DLQ entries. A replay uses a one-shot cursor and leaves the live checkpoint unchanged unless an administrator separately requests a rewind. Replaying a message may produce a second delivery, so the stable event identity remains important. Use replay for recovery, backfills, verification, and controlled reprocessing. Do not use the word as a loose synonym for retrying a transient failure in the normal relay loop.

## Dry Run

Dry run is an explicit consuming mode in which the relay polls, transforms, and routes messages while recording bounded observations instead of publishing them. It can advance the source checkpoint after observation, so it is not a harmless preview of a live pipeline. Use the replay preview command when you need a non-consuming inspection. State clearly whether a command observes a range, consumes it without publishing, or actually delivers it.

## Delivery Receipt

A delivery receipt is a durable record that a configured relay action completed successfully for a particular message or pipeline context. Receipts support observability and downstream coordination, but they are not a replacement for the source offset and they do not assert that an external business operation committed. Receipt retention can be swept independently. When describing delivery state, name the evidence being used: sink acknowledgement, delivery receipt, or committed source checkpoint.

## Retention Participant

A retention participant is a reader or coordination record whose progress still matters when pg_tide decides whether an outbox message can be cleaned up. Native pipelines, relay-group offsets, consumer groups, and active consumer state can all participate in that decision. A disabled pipeline is paused, not automatically retired, so it may remain a participant. Cleanup should be described in terms of the participant set and the configured retention window, not as a global `consumed_at` flag.

## Stream Table

"Stream table" is legacy vocabulary from older CDC and pg_trickle integrations. In current pg_tide APIs, use "outbox" for the named logical event stream and "outbox message" for a record in it. Some compatibility paths and subject templates still expose a `stream_table` field because that name is part of an older wire or configuration contract. Preserve it at those boundaries, but do not introduce it as the primary term in new APIs or documentation.

## Schema Registry

A schema registry is an external service that stores and versions serialization schemas, commonly Avro schemas. It is a connector concern, not a required part of the transactional outbox pattern. pg_tide can carry structured JSON and several wire formats without a registry. When a connector uses one, document the registry's compatibility rules and ownership separately from the pg_tide envelope and the PostgreSQL catalog.

## Tombstone

A tombstone is a message with a null value that tells a log-compacted sink to remove the record associated with a key. It is a wire-level deletion signal, not the same thing as a PostgreSQL `DELETE` and not the same thing as a failed delivery. The Debezium encoder can emit tombstones after deletes so Kafka compaction can eventually remove the old keyed value. Use "delete event" for the application fact and "tombstone" for the compaction record.

## Terms We Keep Separate

Several pairs are easy to blur in conversation. An outbox is durable source state; a pipeline is a route that reads it. A relay group coordinates relay workers; a consumer group tracks an independent reader. An offset is a position; a checkpoint is the act of durably committing the position for a polled batch. A replay reads old data with a temporary cursor; a rewind changes the live cursor. At-least-once is a transport guarantee; effectively exactly once is an application design outcome. Keeping these pairs distinct is part of using pg_tide correctly.
