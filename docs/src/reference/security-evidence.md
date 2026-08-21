# Security evidence index

The v0.52.0 evidence set binds security claims to the candidate commit and
artifact digests. Records contain pointers only. Their status stays `pending`
until the exact candidate passes every required result and receives independent
approval.

| Threats | Required results | Control or runbook | Release record |
|---|---|---|---|
| T01, T02, T08, T09, T10, T15, T16 | `privilege-model-pr` | `sql/`, `deploy/postgres/pg_tide_roles.sql` | `release-evidence/v0.52.0-security.json` |
| T03, T11 | `secret-canary-pr` | `pg-tide-relay/src/secret.rs` | `release-evidence/v0.52.0-security.json` |
| T04 | `relay-unit-pr`, `relay-integration-pr` | `pg-tide-relay/src/` | `release-evidence/v0.52.0-security.json` |
| T05, T06, T07 | `network-security-core-pr`, `network-security-kafka-pr` | `pg-tide-relay/src/http_util.rs`, `pg-tide-relay/src/pg_tls.rs` | `release-evidence/v0.52.0-security.json` |
| T12 | `supply-chain-pr` | `deny.toml`, `supply-chain/advisory-exceptions.toml` | `release-evidence/v0.52.0-security.json` |
| T13, T14 | `artifact-policy-pr`, `artifact-verification-release` | `scripts/check_v1_artifacts.py` | `release-evidence/v0.52.0-security.json` |
| T17 | `lifecycle-contract-pr`, `lifecycle-adjacent-pr` | `docs/src/operations/vulnerability-response.md`, `docs/src/reference/version-compatibility.md` | `release-evidence/v0.52.0-security.json` |

The connector registry remains authoritative for the production boundary.
Removed and experimental connectors have no v0.52.0 production claim.

## Release records

- [`v0.52.0-index.json`](../../../release-evidence/v0.52.0-index.json)
- [`v0.52.0-security.json`](../../../release-evidence/v0.52.0-security.json)
- [`v0.52.0-security-review.json`](../../../release-evidence/v0.52.0-security-review.json)
- [`v0.52.0-vulnerability-response.json`](../../../release-evidence/v0.52.0-vulnerability-response.json)

Do not copy test output, credentials, private reports, or resolved secrets into
release evidence.
