# Version compatibility

## v0.47.0 policy

| Component | Supported baseline |
|---|---|
| PostgreSQL extension | PostgreSQL 18 |
| `pg-tide` relay | Matching v0.47.0 minor release |
| Production profiles | `core`; `core-kafka` for Kafka |
| Container targets | Linux amd64 and arm64 images tested by the release |

Use the release's published matrix for exact connector service versions,
artifact digests, and platform evidence. PostgreSQL 17 and unlisted service
versions are not supported.

The extension and relay are released together. Do not mix minor versions
unless the release notes explicitly document the tested mixed-version window.
Preview and experimental profiles are evaluation surfaces, not compatibility
claims.

## Upgrades and rollback

Use the sequential migration shipped with the release, then follow the
[relay upgrade runbook](../operations/runbook-relay-upgrade.md). Record fresh
install, upgrade, mixed-version, and rollback evidence for the exact candidate.
Do not skip migrations or assume that an older relay understands newer catalog
or contract behavior.
