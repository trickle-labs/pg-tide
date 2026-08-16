# Version compatibility

## v0.41.0 support

| Component | Supported version |
|---|---|
| PostgreSQL extension | PostgreSQL 18 |
| `pg-tide` relay | v0.41.x with the v0.41.x extension |
| Production build | `core` profile |

PostgreSQL 17 is not supported. The evidence and rejection decision are in
[the PostgreSQL 17 feasibility report](../support/postgresql-17-feasibility.md).

The relay and extension are released together. Use matching minor versions;
older relays or experimental feature profiles are not part of the production
compatibility promise.

## Build profiles

- `core` is the normal production profile.
- `core-kafka` is an explicit opt-in profile and does not imply Kafka is
  production-supported.
- `experimental-full` is for evaluation and compile coverage only.

Connector maturity and evidence belong in the generated compatibility material
for the release. A compiling backend is not automatically supported.

## Upgrades

Upgrades are sequential. For v0.40.0 deployments, use the
`0.40.0 -> 0.41.0` extension migration shipped with the release. Do not skip
versions or assume that an older relay understands newer catalog behavior.

