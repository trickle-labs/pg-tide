# Production-supported definition

A row is production-supported only when all of the following are true:

1. User-facing behavior is implemented and documented for a specific
   direction.
2. Its owner and security contact are recorded in `connectors.toml`.
3. Its production profile includes it intentionally.
4. Current evidence covers the real boundary: contract/integration or E2E
   behavior, authentication, TLS, redaction, duplicates, failure windows,
   restart/recovery, metrics, runbook, upgrade, and packaging where
   applicable.
5. The supported PostgreSQL 18 path and release artifact exercise it.

The v0.47.0 production-supported outbound set is PostgreSQL inbox, NATS
JetStream, Apache Kafka, and HTTPS webhook. PostgreSQL native outbox is the
supported source. Diagnostics are not production integrations; inbound
connectors and every other registry row remain preview or experimental.

“Builds successfully” or “has a unit test” is not sufficient. Missing evidence
downgrades maturity rather than weakening the gate. The generated
[release checklist](connector-release-checklist.md) shows the evidence
recorded for each registry row.
