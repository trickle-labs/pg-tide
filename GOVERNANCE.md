# Governance

pg_tide uses lightweight, maintainer-led governance. The repository records
ownership in `connectors.toml`; all current connector owners and security
contacts are `@grove`.

## Roles

- **Maintainer:** steers scope, reviews changes, and resolves ordinary
  decisions.
- **Code owner:** reviews paths covered by [CODEOWNERS](.github/CODEOWNERS).
- **Connector owner:** maintains a connector's implementation, docs, evidence,
  and registry row.
- **Security contact:** receives private vulnerability reports and coordinates
  disclosure.
- **Independent reviewer:** reviews a release candidate outside the primary
  author's area.
- **Release manager:** verifies release evidence and may be the maintainer,
  but cannot replace required independent approvals.

One person may hold several roles when the work is small. A person must not
approve their own material change as the independent reviewer.

## Decisions and changes

Small bug fixes and documentation changes may be merged after normal review.
Changes to public SQL, supported configuration, connector maturity, delivery
semantics, security boundaries, or release policy require a pull request that
states compatibility impact and links evidence. Significant design decisions
are recorded as an ADR under `docs/src/reference/`.

The release manager approves a release only when the
[release checklist](docs/src/operations/release-manager-checklist.md) is
complete, no P0/P1 issue remains, and independent review findings are
dispositioned. Material changes invalidate affected approvals until targeted
re-review is recorded.

## Ownership and conflicts

Owners should keep registry metadata, documentation, and evidence current. If
an owner becomes unavailable, open an issue describing the affected surface;
the maintainer may assign an interim owner or remove a support claim. Changes
with a personal, commercial, or security conflict must disclose it in the
pull request and be reviewed by someone without that conflict.

Governance changes require a reviewed pull request and updates to this file,
CODEOWNERS, and affected process documentation. If no owner is available, use
the maintainer's public GitHub handle for ordinary escalation and the private
process in [`SECURITY.md`](SECURITY.md) for vulnerabilities.
