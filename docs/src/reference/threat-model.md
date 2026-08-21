# pg_tide threat model

This model covers the v0.52.0 production profiles: `core` and `core-kafka`.
It covers the PostgreSQL 18 extension, PostgreSQL inbox, NATS JetStream,
Apache Kafka, and HTTPS webhook destinations. `stdout` and `file` are
diagnostic sinks only.

| ID | Threat and control | Detection and recovery | Owner | Evidence |
|---|---|---|---|---|
| T01 | Publisher impersonation; role matrix, `session_user`, outbox ACL | Authorization error; revoke role or ACL | Database owner; security contact | `privilege-model-pr` |
| T02 | Unauthorized pipeline modification; role grants and bounded admin API | Audit and config failure; restore reviewed config | Relay owner; security contact | `privilege-model-pr` |
| T03 | Relay credential theft; reference-only fields and strict secret files | Canary scan; rotate credential and quarantine evidence | Relay owner; security contact | `secret-canary-pr` |
| T04 | Malicious event payload; schema validation, size bounds, and redaction | Validation failure; quarantine or replay source event | Relay owner; security contact | `relay-unit-pr`, `relay-integration-pr` |
| T05 | Webhook SSRF; URL, DNS, address, redirect, and proxy policy | SSRF refusal and zero-request assertion; remove endpoint | Relay owner; security contact | `network-security-core-pr` |
| T06 | DNS or redirect manipulation; validate every answer and pin it | Policy refusal; remove endpoint and inspect DNS control | Platform owner; security contact | `network-security-core-pr` |
| T07 | Destination impersonation; verified TLS and noisy development overrides | TLS refusal; install the correct CA or certificate | Platform owner; security contact | `network-security-core-pr`, `network-security-kafka-pr` |
| T08 | Checkpoint tampering; monotonic offset API and exact grants | Rewind refusal; restore the last trusted state | Relay owner; security contact | `privilege-model-pr`, `delivery-model-pr` |
| T09 | DLQ tampering; role matrix and transactional terminal state | State check; reconcile from source events | Relay owner; security contact | `privilege-model-pr`, `replay-recovery-pr` |
| T10 | Unauthorized replay; bounded replay authorization and stable IDs | Refusal code; revoke operation access | Operations owner; security contact | `privilege-model-pr`, `replay-recovery-pr` |
| T11 | Secret leakage; `SecretString`, masking, and sanitized errors | Positive-control canary scan; rotate secrets | Relay owner; security contact | `secret-canary-pr` |
| T12 | Dependency compromise; locked graph and advisory/license policy | Graph gate failure; block and rebuild | Release owner; security contact | `supply-chain-pr` |
| T13 | CI or release compromise; immutable digests and separate verifier | Digest or verification failure; withdraw and rebuild | Release owner; security contact | `artifact-policy-pr`, `artifact-verification-release` |
| T14 | Excessive container contents; artifact allowlist and image policy | Inventory failure; withdraw and rebuild image | Release owner; security contact | `artifact-policy-pr` |
| T15 | Extension privilege escalation; fixed owners and locked definer paths | Privilege inventory; revoke execute and migrate | Database owner; security contact | `privilege-model-pr` |
| T16 | Unsafe search path; `pg_catalog, tide` for security-definer functions | Clean-room inventory; lock settings and retry | Database owner; security contact | `privilege-model-pr` |
| T17 | Restore or rollback abuse; lifecycle matrix and migration boundary | Compatibility refusal; restore matching pair | Release owner; security contact | `lifecycle-contract-pr`, `lifecycle-adjacent-pr` |

Every row has a prevention control, detection signal, recovery action, owner,
and evidence pointer. The paths for the controls are `sql/`,
`deploy/postgres/pg_tide_roles.sql`, `pg-tide-relay/src/`,
`schemas/lifecycle-compatibility-v1.json`, and `release-evidence/`.

The model excludes compromise of a PostgreSQL superuser, relay-host root,
destination administrator, or infrastructure firewall. Those actors can bypass
controls outside pg_tide. The controls still reduce the effect of lesser
compromise.
