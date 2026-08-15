# Support policy

v0.41.0 supports the product surface that is tested and documented in this
repository. The policy is intentionally narrow: compiling a connector does not
make it production-supported.

## Supported baseline

- PostgreSQL 18 is the supported PostgreSQL version.
- The pg_tide extension and matching `pg-tide` relay release are the supported
  combination.
- Production builds use the `core` profile. Other profiles are opt-in and may
  contain preview or experimental connectors.
- Support assumes a maintained PostgreSQL deployment and a supported operating
  system/architecture published with the release artifact. Unlisted platforms
  are not promised to work.

## Maintenance

Patch releases address security issues, data-loss or correctness bugs, and
regressions in the supported surface. Preview and experimental interfaces may
change or be removed in a minor release and are not covered by the production
support promise.

PostgreSQL 17 is not supported in v0.41.0; see
[the feasibility report](postgresql-17-feasibility.md).

