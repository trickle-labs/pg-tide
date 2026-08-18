# Connector promotion policy

Promotion extends the [production-supported definition](production-supported.md)
and the generated [connector release checklist](connector-release-checklist.md).
It does not create a second connector registry.

Before promotion, the `connectors.toml` row must include a stable ID, direction,
owner, security contact, docs, tested service versions, intentional production
profile, and configuration boundary. It must declare acknowledgement, retry,
ordering, deduplication, TLS, authentication, limits, and shutdown behavior.

Evidence must cover the real protocol boundary, including happy path, failure
before and after acknowledgement, restart/recovery, duplicates,
authentication, TLS, redaction, metrics, runbook, upgrade, and packaging
checks where applicable. Evidence must use the public API and exact release
artifact.

Promotion requires independent review of the implementation and security
boundary, a release-manager decision, and generated compatibility material
that agrees with `connectors.toml`. A successful compile, unit test, or private
anecdote does not promote a connector. Direction is independent: promoting an
outbound sink does not promote its source.
