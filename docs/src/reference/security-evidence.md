# Security Evidence Index

The checked-in `connectors.toml` registry is the authority for connector
maturity. A connector is not production-supported merely because it compiles.
Supported rows must have evidence for authentication, transport security,
secret redaction, resource bounds, upgrade behavior, and an operational
runbook.

| Evidence | Location | Gate |
|---|---|---|
| Database role and ACL contract | `sql/pg_tide--0.43.0--0.44.0.sql`, `deploy/postgres/pg_tide_roles.sql` | `v044_validation_test` |
| Publisher and fail-closed behavior | `pg-tide-relay/tests/publisher_acl_test.rs` and extension tests | `just test-security` |
| Shared HTTP/SSRF policy | `pg-tide-relay/src/http_util.rs`, `ssrf_test.rs` | Unit and integration tests |
| TLS defaults | `pg-tide-relay/src/pg_tls.rs`, `tls_test.rs` | TLS test matrix |
| Secret references and files | `pg-tide-relay/src/secret.rs`, `encryption.rs` | Secret and KMS tests |
| Connector support metadata | `connectors.toml`, `scripts/generate_connector_surface.py` | `just check-connectors` |
| Production dependency graph | `rust-toolchain.toml`, `audit.toml`, `supply-chain/` | `just audit-production` |
| Release integrity | `.github/workflows/release.yml` | Signature/SBOM/provenance verification |

Security failures are release blockers. Experimental connectors remain outside
the production support claim until their evidence is complete.
