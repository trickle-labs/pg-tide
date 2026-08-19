# Release evidence

The release evidence index is the claim-to-proof record for a candidate. It
links the frozen contract documents, schemas, fixtures, and checks; generated
connector material; sanitized pilot records and linked issues; independent
approvals and reapprovals; blocker/severity queries and known P2/P3
limitations; install, upgrade, mixed-version, rollback, regression, security,
and operational results; and artifact names, digests, signatures, SBOM,
provenance, and release notes.

Each record names the exact candidate commit and artifact versions. Evidence
from another candidate is invalid after a material contract or core-path
change. Missing evidence, unavailable review, or an unresolved P0/P1 blocks
release. Private security records may be referenced by status and owner
without publishing sensitive details.

## Pending versus ready

The v0.47.0 records are structurally valid but remain `pending`. The index
records the exact tag commit as historical provenance; its `candidate.commit`
and artifact digest fields remain empty because no pilot, review, approval, or
artifact proof is recorded for that candidate. The `pending_reason` fields are
part of the public evidence record and must explain an absent proof without
turning a local test or a later commit into v0.47.0 evidence.

`ready` is a separate claim. It requires the exact candidate commit and
artifact digests, completed pilot and review records with named identities and
UTC dates, a zero blocker query result, and release-manager approval. CI may
validate a pending record, but it must never promote one to `ready`.
