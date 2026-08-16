# PostgreSQL 17 feasibility

Owner: @grove. Assessment environment: Ubuntu latest, Rust stable, cargo-pgrx
0.18.0, and the repository's pinned PostgreSQL 18/pgrx toolchain.

## Decision

PostgreSQL 17 is rejected as a v0.41.0 supported target. The repository has no
maintained `pg17` feature or CI matrix, so there is no evidence for an upgrade
from the PostgreSQL 18 support path.

## Required gate and current result

| Check | Required command/evidence | Result in v0.41.0 |
|---|---|---|
| Versioned build | `cargo build --workspace --features pg17` | Blocked: no `pg17` feature. |
| Clippy | `cargo clippy --workspace --features pg17 --all-targets -- -D warnings` | Blocked by the missing feature. |
| Extension tests | `cargo pgrx test pg17` | Blocked: no maintained pg17 pgrx target. |
| Package/install | `cargo package`; install the extension into PostgreSQL 17 | Not established. |
| SQL smoke | Fresh install, create an outbox/inbox, publish, consume | Not established. |
| Relay/NATS E2E | Run the public outbox-to-NATS workflow against PostgreSQL 17 | Not established. |
| CI ownership | Maintain the same compile, package, SQL, and E2E jobs as PostgreSQL 18 | Absent; adds a second full matrix. |

The exact PostgreSQL 18 dependency is the only maintained pgrx/extension test
path currently present. A future support proposal must add the complete matrix
above and keep the PostgreSQL 18 gate unchanged before changing this decision.

Commands assessed: `cargo build --workspace --features pg17`,
`cargo clippy --workspace --features pg17 --all-targets -- -D warnings`,
`cargo pgrx test pg17`, `cargo package --package pg-tide-relay --locked`,
fresh extension install plus SQL smoke, and the public
`public_api_outbox_to_nats_e2e` test against PostgreSQL 17. None can be
accepted because the `pg17` feature and maintained PostgreSQL 17 target do not
exist. Adding them would duplicate the full PostgreSQL 18 CI/install/E2E
matrix, so the exact blocker is both missing pgrx configuration and unowned CI
cost, not a claimed SQL incompatibility.
