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

Historical release records remain `pending` until their own evidence is
complete. The v0.51.0 index likewise keeps `candidate.commit` and artifact
digests empty until the exact staged candidate artifacts have passed the
lifecycle gates; a local run or later commit cannot substitute for that proof.

`ready` is a separate claim. It requires the exact candidate commit and
artifact digests, completed pilot and review records with named identities and
UTC dates, a zero blocker query result, and release-manager approval. CI may
validate a pending record, but it must never promote one to `ready`.
