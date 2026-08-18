# Pilot evidence

Pilot evidence must describe the exact release candidate without exposing
customer data. Use a stable pseudonymous pilot ID and record the candidate
commit, artifact/image digest, PostgreSQL, relay, connector, deployment, and
platform versions; topology and workload shape; latency, event/byte counts,
resource and storage observations; installation, steady-state, induced
failure, restart, upgrade, rollback, duplicate, and operator-diagnosis
outcomes; documentation gaps, incidents, linked issues, and operator sign-off.

Do not record payloads, credentials, certificates, customer names, internal
hostnames/IPs, account IDs, or raw unsanitized logs. Use public APIs and
release artifacts; direct catalog mutation and test-only failpoints do not
count.

## Required profiles

Complete one record for each:

- PostgreSQL native outbox to NATS JetStream;
- PostgreSQL native outbox to Kafka;
- PostgreSQL native outbox to a remote PostgreSQL inbox;
- PostgreSQL native outbox to HTTPS webhook.

Include connector-specific outage, acknowledgement, retry, deduplication,
TLS/authentication, and rollback observations from the runbook.

Every finding becomes an issue before sign-off with `pilot/<profile>`, P0-P3
severity, affected contract or `non-contract`, sanitized reproduction, owner,
milestone, and disposition. P0/P1 findings cannot be accepted limitations.
