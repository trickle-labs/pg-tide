# Message Guarantees

pg_tide provides transactional persistence at the PostgreSQL outbox and
idempotent processing at the PostgreSQL inbox. The relay delivers each
outbox message at least once to the configured destination.

## Forward delivery

The relay polls the canonical outbox, publishes a batch, and advances its
durable checkpoint only after the destination acknowledges it. A crash between
publication and checkpoint commit can therefore produce a duplicate. Native
message identities remain stable across retries so supported destinations can
apply their own deduplication rules.

Supported outbound destinations are PostgreSQL inbox, NATS JetStream, Apache
Kafka, and HTTPS webhook. stdout and file output are diagnostic only.

## PostgreSQL inbox

The inbox uses a unique event identity and transactionally records processing
state. Applications should process the inbox row and mark it processed in the
same transaction when their business operation is transactional.

## Failures and replay

Transient destination failures are retried with bounded backoff. Exhausted
messages can be sent to the DLQ and replayed after the underlying problem is
fixed. Checkpoints are not advanced for a batch that has not been acknowledged.

These guarantees are intentionally at-least-once. pg_tide does not claim
exactly-once transport.
