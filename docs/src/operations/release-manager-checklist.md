# Release-manager checklist

Use this checklist for the v0.51.0 candidate and future contract-freeze
releases. Record links to the exact commit, artifact digests, and evidence.

## Contract and support

- [ ] Frozen surfaces have normative docs and machine-checkable evidence.
- [ ] Compatibility and deprecation changes have review and migration notes.
- [ ] `connectors.toml` and generated support pages agree on maturity,
      direction, owners, versions, profiles, and evidence.
- [ ] No preview or experimental row inherits a production promise.
- [ ] Documentation, examples, dashboards, alerts, and runbooks match reality.

## Pilots, review, and blockers

- [ ] NATS, Kafka, PostgreSQL inbox, and webhook pilots completed all required
      scenarios with sanitized evidence.
- [ ] Every pilot finding is a linked issue with `pilot/<profile>`, P0-P3
      severity, owner, and disposition.
- [ ] PostgreSQL, async/concurrency, delivery-semantics, security, and
      operations reviews independently approved the exact candidate.
- [ ] Material changes received targeted reapproval.
- [ ] The release query proves zero open P0/P1 issues.

## Validation and release evidence

- [ ] Fresh install, sequential upgrade, mixed-version window, and rollback
      checks pass.
- [ ] The lifecycle policy, compatibility matrix, and recovery runbook match
      the exact candidate artifacts.
- [ ] Security, dependency, connector, schema, observability, and runbook
      checks pass.
- [ ] Artifact contents match the support matrix.
- [ ] Digests, signatures, SBOM, provenance, and vulnerability results are
      recorded.
- [ ] Version, extension control files, migration, chart, image, and changelog
      metadata agree and were updated last.
- [ ] The release evidence index links contracts, pilots, reviews, blockers,
      regressions, and artifacts.

An unavailable reviewer, missing evidence, or unresolved P0/P1 is a release
blocker, not a reason to weaken the claim.
