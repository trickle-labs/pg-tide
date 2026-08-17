# pg_tide Threat Model

v0.44.0 treats the database, relay host, connector endpoints, message data,
and build inputs as separate trust boundaries. The extension protects its
catalog and API boundary; it does not replace database superuser controls,
network firewalls, destination credential management, or application payload
validation.

| Threat | Prevention | Detection | Recovery | Evidence | Owner |
|---|---|---|---|---|---|
| Unauthorized SQL mutation | Canonical roles, ACLs, `session_user`, revoked PUBLIC execute | Authorization errors and audit rows | Revoke membership/ACL; preserve offsets | Privilege matrix and migration validation | Database owner |
| Cross-tenant access | Tenant checks fail closed and run in the same transaction as mutations | Tenant-filter errors and audit rows | Reconcile tenant grants; do not restore PUBLIC access | Tenant fault tests | Database owner |
| Secret disclosure | Typed references, strict files, redacted output, sanitized status/history | Canary scans and leakage metrics | Rotate credential; protect pre-upgrade backup | Secret/redaction tests | Relay owner |
| SSRF or unsafe outbound endpoint | Parsed URL policy, DNS answer checks, redirect revalidation, proxy opt-in | Policy refusal and bounded security metric | Remove endpoint; retain delivery state | SSRF and proxy tests | Relay owner |
| Plaintext or unverified transport | Verified TLS defaults and explicit noisy development overrides | Startup warning and status | Install correct CA/certificate; do not globally disable verification | TLS tests | Platform owner |
| Delivery loss or duplicate | Transactional outbox, monotonic offsets, deferred acknowledgments | Checkpoint/DLQ metrics and audit history | Replay from retained outbox; never mutate offsets directly | Crash and end-to-end tests | Relay owner |
| Resource exhaustion | Batch, payload, retry, retention, and file-size bounds | Operational budgets and saturation metrics | Pause source, drain or replay bounded work | Budget and regression tests | Operations |
| Dependency/build compromise | Pinned toolchain/actions, graph audits, signatures, SBOM, provenance | CI and release verification failures | Withdraw artifact and rebuild from reviewed commit | Supply-chain gates | Release owner |
| Malicious connector or payload content | Connector maturity gates, schema validation, redaction, bounded templates | Sanitized errors and connector health | Disable connector; preserve source messages | Connector evidence index | Connector owner |

The relay cannot guarantee confidentiality after compromise of a PostgreSQL
superuser, relay host, destination credential, or infrastructure firewall.
