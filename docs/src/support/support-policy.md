# Support policy

Support follows the generated [connector compatibility matrix](connector-compatibility.md)
and its source of truth, [`connectors.toml`](../../../connectors.toml).

## Support levels

- **Production-supported:** documented behavior, an owner and security
  contact, an intentional production profile, and complete applicable
  contract, integration, failure, restart, duplicate, security, metrics,
  runbook, upgrade, and packaging evidence.
- **Preview:** usable for evaluation, but interfaces, compatibility, and
  upgrade behavior may change or be removed.
- **Experimental:** compile or test coverage only; no production promise.
- **Diagnostic:** local output or test assistance, not a production
  integration.

Maturity is direction-aware. A supported outbound connector does not make its
inbound path supported.

## v0.51.0 baseline

- PostgreSQL 18 is the supported PostgreSQL version.
- The v1 extension upgrade floor is v0.47.0 through the packaged adjacent
  migration chain.
- The supported rolling relay window is v0.50.0 and v0.51.0.
- `core` is the normal production profile; `core-kafka` is an explicit
  opt-in profile for Apache Kafka.
- Production-supported destinations are PostgreSQL inbox, NATS JetStream
  outbound, Apache Kafka outbound, and HTTPS webhook outbound.
- PostgreSQL native outbox is the supported source. Diagnostics are supported
  only as diagnostics.
- Linux amd64 and arm64 Docker images are the runtime-tested container targets.
  Other archives are build artifacts unless the release matrix says otherwise.
- Helm and CloudNativePG support is limited to the exact profile tested by the
  lifecycle workflow; other Kubernetes distributions remain preview.

Unlisted service versions and compiling feature combinations are not covered.
See the [version compatibility](../reference/version-compatibility.md) guide.

## Maintenance

Routine fixes target the latest 0.x minor line. Older minor lines are
upgrade-only unless a separate security exception is announced. Preview and
experimental interfaces may change in a minor release without a deprecation
period. Supported deprecations follow the
[deprecation policy](deprecation-policy.md).
