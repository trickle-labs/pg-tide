# Independent review

Reviewers other than the primary author must approve the exact candidate
commit and artifact digests. The review index records reviewer handle,
discipline, scope, date, findings, disposition, and reapproval.

Required perspectives:

- PostgreSQL extension: SQL, transactions, privileges, SPI, locks, migrations,
  upgrade, and rollback;
- Rust async/concurrency: ownership, cancellation, retries, backpressure,
  shutdown, and races;
- delivery semantics: acknowledgements, checkpoints, duplicates, HA, and
  replay;
- security: ACLs, TLS, SSRF, secrets, dependencies, and disclosure;
- operations: install, health, metrics, alerts, capacity, runbooks, and
  rollback.

Re-review is required after changes to a frozen schema or rule, SQL
transaction/privilege behavior, checkpoint/retry/HA/shutdown behavior,
supported connector acknowledgement or deduplication, security handling, or
release artifacts. Documentation typos and evidence clarifications do not
invalidate unrelated approvals.

No P0/P1 finding may remain at release. Security details may stay private
during coordinated disclosure; the public index records blocker status and
disposition without sensitive content.
