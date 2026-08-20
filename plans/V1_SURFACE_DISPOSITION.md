# v0.49.0 surface disposition

This record defines the v0.49.0 product boundary for SURF-1, SURF-7, and
SURF-8. The machine-readable record is
[`schemas/v1-surface-disposition.json`](../schemas/v1-surface-disposition.json).

<!-- disposition-sha256: 5db73ecb261e2206299de4aeb288c0e8175b8b356249a5e5311a346bb5accdb7 -->

## Product boundary

```text
PostgreSQL transactional outbox
  -> PostgreSQL inbox, NATS JetStream, Apache Kafka, or HTTPS webhook
```

The relay keeps native JSON and CloudEvents. `stdout` and file output remain
diagnostic destinations. They are not production integrations.

The disposition records the current tree before SURF-3 through SURF-6 remove
implementation surface. A removal with `state = legacy-present` is still
present in the current tree, but it cannot grow: the checker requires every
current item to be listed here and rejects unlisted additions. SURF-3 through
SURF-6 change those rows to `absent` as each removal lands.

## Baseline and provenance

- Starting commit: `8b861d29b1e565c6a431bcab492976b9803dcb1b` (`pre-v1-experimental-surface`).
- Baseline definitions: [`release-evidence/pre-v1-baseline/baseline.json`](../release-evidence/pre-v1-baseline/baseline.json).
- The available committed baseline is v0.47.0. A v0.48.0 measurement is not
  available in this checkout, so the reduction record does not claim a
  v0.48.0 percentage.
- The checker reads `connectors.toml`, `pg-tide-relay/Cargo.toml`, and the
  descriptor source. It scans only reachable active docs, deployment metadata,
  and release workflow files. It excludes changelogs, migrations, ADRs,
  archive paths, this plan, the migration guide, and generated connector output.

## Retained items

| Area | Retained items |
| --- | --- |
| Source | `outbox` |
| Production destinations | `inbox`, `nats`, `kafka`, `webhook` |
| Diagnostic destinations | `stdout`, `file` |
| Wire formats | `native`, `cloudevents` |
| Profiles | `core`, `core-kafka` |

The PostgreSQL `pg_outbox` sink alias remains a compatibility alias for the
PostgreSQL inbox behavior. It is not a fifth production destination.

## Removal rules

Every removal below has `last_version = v0.48.0` and a replacement. `none`
means that the item has no supported replacement. The full item-level record,
including all current feature flags, runtime values, connector rows, profiles,
and CLI command families, lives in the JSON record.

| Family | Classification | Replacement |
| --- | --- | --- |
| Preview inbound paths | remove | `outbox` source and the four production destinations |
| Alternate queues and messaging | remove | `nats` or `kafka` outbound |
| Warehouses, lakes, ETL, and notifications | remove | `inbox`, `nats`, `kafka`, or `webhook` |
| Alternate wire formats | remove | `native` or `cloudevents` |
| Reverse, unavailable, and compatibility modes | remove | `outbox` |
| Unsupported KMS provider names | remove | `kms-local` or `none` |
| DAG, managed backfill, and framework-only commands | remove | `none` |

No item is assigned to Labs. The repository has no independent Labs release
path with an owner, security contact, protocol test, and separate package.

## Active documentation

The active navigation describes one forward path from the PostgreSQL outbox to
the four production destinations. Historical changelogs, migrations, ADRs,
and archive material remain unchanged. Operators who upgrade from v0.48.0
must follow the [v0.49.0 migration guide](../docs/src/operations/v1-migration-guide.md).
