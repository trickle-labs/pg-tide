# Support policy

v0.41.0 supports the product surface that is tested and documented in this
repository. The policy is intentionally narrow: compiling a connector does not
make it production-supported.

## Supported baseline

- PostgreSQL 18 is the supported PostgreSQL version.
- Only extension/relay version combinations exercised by the fresh-install,
  upgrade, and public NATS E2E checks are supported; newer relays are not
  assumed to work with older extensions.
- Production builds use the `core` profile. Other profiles are opt-in and may
  contain preview or experimental connectors.
- The Linux amd64 and arm64 Docker images are the runtime-tested container
  targets. Linux amd64, Linux arm64, Windows MSVC, and macOS arm64 archives are
  build artifacts; compilation alone does not make an OS/runtime combination
  supported.
- Standalone binary and Docker deployment are the maintained v0.41 paths.
  Helm and CloudNativePG installation are preview deployment modes until their
  install and upgrade gates are maintained in CI.
- Connector versions and evidence are listed only in the [generated
  matrix](connector-compatibility.md); there is no blanket compatibility claim
  for unlisted service versions.

## Experimental and preview terms

Preview and experimental features are best effort. They have no compatibility,
upgrade, response-time, or retention guarantee, may change or be removed, and
must not be used to infer production support from a successful compile. Security
reports are still accepted for every surface.

## Maintenance

Patch releases address security issues, data-loss or correctness bugs, and
regressions in the supported surface. Preview and experimental interfaces may
change or be removed in a minor release and are not covered by the production
support promise.

PostgreSQL 17 is not supported in v0.41.0; see
[the feasibility report](postgresql-17-feasibility.md).

Before v1.0, routine fixes target only the latest 0.x minor line. Older minor
lines are upgrade-only unless a separate security exception is announced.

Documented supported SQL APIs and the `core` relay path are the public
supported surface. Preview and experimental configuration keys, connectors,
and protocol adapters have no deprecation-period promise.
