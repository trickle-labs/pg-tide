# Version compatibility

The v0.54.0 lifecycle policy is the machine-readable source of truth:
[`lifecycle-compatibility-v1.json`](../../../schemas/lifecycle-compatibility-v1.json).
The checker compares this matrix with packaged migrations, required tests,
release controls, examples, evidence, and this page.

## Supported lifecycle matrix

<!-- BEGIN LIFECYCLE MATRIX -->
| Extension | Relay | Status | Required test |
|---|---|---|---|
| 0.51.0 | 0.51.0 | supported | `lifecycle-adjacent-pr` |
| 0.51.0 | 0.52.0 | supported | `lifecycle-compatibility-pr` |
| 0.52.0 | 0.52.0 | supported | `lifecycle-compatibility-pr` |
| 0.52.0 | 0.53.0 | supported | `lifecycle-compatibility-pr` |
| 0.53.0 | 0.53.0 | supported | `lifecycle-compatibility-pr` |
| 0.53.0 | 0.54.0 | supported | `lifecycle-compatibility-pr` |
| 0.54.0 | 0.54.0 | supported | `lifecycle-compatibility-pr` |
| 0.53.0 | 0.52.0 | rejected | `lifecycle-compatibility-pr` |
| 0.54.0 | 0.53.0 | rejected | `lifecycle-compatibility-pr` |
| <0.53.0 | 0.54.0 | rejected | `lifecycle-compatibility-pr` |
| >0.54.0 | 0.54.0 | rejected | `lifecycle-compatibility-pr` |
<!-- END LIFECYCLE MATRIX -->

The supported production extension floor remains v0.47.0. Upgrade through
every adjacent migration. For the v0.51.0 to v0.52.0 upgrade, replace relays
with v0.52.0 before updating the extension. The v0.51.0 relay cannot run after
the extension reaches v0.52.0.

## Rollback and recovery

The v0.54.0 extension migration is transactional. If it fails before commit,
retry from the committed v0.53.0 state. After commit, restore a v0.53.0 backup
or use PITR before rolling the relay back.

PostgreSQL recovery does not roll back NATS, Kafka, or webhook destinations.
Restoring before a checkpoint commit may redeliver an accepted event. Delivery
remains at-least-once. Stable event identities and destination-side
deduplication are required.

Do not edit PostgreSQL system catalogs or `tide` tables to repair a lifecycle
transition. Follow the migration policy and retain the recorded evidence.
