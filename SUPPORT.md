# Support policy

## What is supported

The generated [connector compatibility matrix](docs/src/support/connector-compatibility.md)
and `connectors.toml` are authoritative. The v0.47.0 support boundary is:

- PostgreSQL 18;
- matching extension and `pg-tide` relay minor versions;
- the `core` production profile and the explicit `core-kafka` profile;
- PostgreSQL native outbox as a source;
- PostgreSQL inbox, NATS JetStream outbound, Apache Kafka outbound, and HTTPS
  webhook outbound as production-supported destinations.

Diagnostics are for local diagnosis, not a production integration. Inbound
NATS, Kafka, and webhook paths, plus all preview and experimental rows, are
best effort and outside the production support promise. Service versions,
direction, profile, owner, and evidence are listed in the generated matrix;
compiling a connector is not support evidence.

## Getting help

Use GitHub Issues for reproducible bugs, documentation corrections, and
support questions. Use repository Discussions when that channel is enabled for
design or usage questions. Security vulnerabilities belong in the private
process in [`SECURITY.md`](SECURITY.md).

Include release, commit, OS/architecture, PostgreSQL version, relay profile,
connector/service version, sanitized configuration, a minimal reproducer,
expected and actual behavior, bounded errors, metrics, and runbook steps tried.
Never include secrets, credentials, certificates, payloads, private hostnames,
or customer identifiers.

Maintainers may request a reproducer, close requests for unsupported surfaces,
or redirect deployment questions to the relevant runbook. Best-effort support
does not create a response-time or availability SLA.

## Maintenance and upgrades

Routine fixes target the latest 0.x minor line. Older minor lines are
upgrade-only unless a separate security exception is announced. Follow the
[version compatibility](docs/src/reference/version-compatibility.md) guide and
[upgrade runbook](docs/src/operations/runbook-relay-upgrade.md).

See the [production-supported definition](docs/src/support/production-supported.md),
[release checklist](docs/src/operations/release-manager-checklist.md), and
[connector evidence](docs/src/support/connector-release-checklist.md).
