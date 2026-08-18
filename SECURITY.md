# Security policy

## Supported security surface

The supported security boundary is PostgreSQL 18, the production profiles
listed in [`connectors.toml`](connectors.toml), and the production-supported
connector rows in the [compatibility matrix](docs/src/support/connector-compatibility.md).
Preview, experimental, and diagnostic surfaces are not production guarantees,
but security reports are welcome for all repository code.

Supported network paths require verified TLS where the connector matrix says
it is required. Do not commit credentials, certificates, connection strings,
payloads, or unsanitized logs.

## Reporting a vulnerability

Use GitHub's private vulnerability reporting or Security Advisories for this
repository. If that channel is unavailable, contact the recorded security
owner, [@grove](https://github.com/grove), privately through GitHub. Do not
open a public issue or publish exploit details before coordination.

Include the affected release or commit, profile and connector, impact,
reproduction, sanitized evidence, and any workaround. The maintainer will
acknowledge, triage, and coordinate remediation through the private report.
Response and fix timing depends on severity and maintainer availability; this
policy is not an SLA. Security findings affecting a supported surface are
release blockers until dispositioned.

Severity follows the project issue policy:

- **P0:** active compromise, confirmed data loss, or critical remote impact;
- **P1:** serious exploitable impact or a supported-path safety failure;
- **P2:** material but bounded security weakness;
- **P3:** low-impact hardening or documentation issue.

We may request a coordinated disclosure date, publish a sanitized advisory,
and credit reporters who consent. Good-faith testing that avoids privacy
violations, service disruption, and data access outside the test scope is
welcomed.

See the [threat model](docs/src/reference/threat-model.md),
[security evidence](docs/src/reference/security-evidence.md), and
[governance](GOVERNANCE.md).
