# PostgreSQL 17 feasibility

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

