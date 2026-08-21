# Security evidence index

The v0.52.0 evidence set binds each security claim to a required result,
control path, and candidate-bound release record. Records contain pointers
only. Status remains `pending` until the exact candidate passes every blocking
result and receives independent approval.

| Threat | Required result | Control and runbook | Release record |
|---|---|---|---|
| T01 | `privilege-model-pr` | `pg-tide-ext/src/outbox.rs`, SQL privilege matrix | `release-evidence/v0.52.0-security.json` |
| T02 | `privilege-model-pr` | `docs/src/sql-reference/relay-api.md` | `release-evidence/v0.52.0-security.json` |
| T03 | `secret-canary-pr` | `pg-tide-relay/src/secret.rs` | `release-evidence/v0.52.0-security.json` |
| T04 | `relay-unit-pr`, `relay-integration-pr` | `pg-tide-relay/src/envelope.rs` | `release-evidence/v0.52.0-security.json` |
| T05 | `network-security-core-pr` | `pg-tide-relay/src/http_util.rs`, webhook runbook | `release-evidence/v0.52.0-security.json` |
| T06 | `network-security-core-pr` | `pg-tide-relay/src/http_util.rs` | `release-evidence/v0.52.0-security.json` |
| T07 | `network-security-core-pr`, `network-security-kafka-pr` | TLS and connector runbooks | `release-evidence/v0.52.0-security.json` |
| T08 | `privilege-model-pr`, `delivery-model-pr` | Offset API and crash-recovery runbook | `release-evidence/v0.52.0-security.json` |
| T09 | `privilege-model-pr`, `replay-recovery-pr` | DLQ/replay runbook | `release-evidence/v0.52.0-security.json` |
| T10 | `privilege-model-pr`, `replay-recovery-pr` | `pg-tide-relay/src/cmd/replay.rs` | `release-evidence/v0.52.0-security.json` |
| T11 | `secret-canary-pr` | Config/output redaction paths | `release-evidence/v0.52.0-security.json` |
| T12 | `supply-chain-pr` | `scripts/check_supply_chain.py`, dependency policy | `release-evidence/v0.52.0-security.json` |
| T13 | `artifact-verification-release` | Release workflow and release-evidence runbook | `release-evidence/v0.52.0-security.json` |
| T14 | `artifact-policy-pr` | `scripts/check_release_artifacts.py` | `release-evidence/v0.52.0-security.json` |
| T15 | `privilege-model-pr` | SQL migration and extension function inventory | `release-evidence/v0.52.0-security.json` |
| T16 | `privilege-model-pr` | Definer path inventory and migration | `release-evidence/v0.52.0-security.json` |
| T17 | `lifecycle-contract-pr`, `lifecycle-adjacent-pr` | Lifecycle policy and relay-upgrade runbook | `release-evidence/v0.52.0-security.json` |

## Release records

- [`v0.52.0-index.json`](../../../release-evidence/v0.52.0-index.json)
- [`v0.52.0-security.json`](../../../release-evidence/v0.52.0-security.json)
- [`v0.52.0-security-review.json`](../../../release-evidence/v0.52.0-security-review.json)
- [`v0.52.0-vulnerability-response.json`](../../../release-evidence/v0.52.0-vulnerability-response.json)

The connector registry remains authoritative for the `core` and `core-kafka`
production boundary. Removed and experimental connectors have no v0.52.0
production claim. Do not copy test output, credentials, private reports, or
resolved secrets into release evidence.
