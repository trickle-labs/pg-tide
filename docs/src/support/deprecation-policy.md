# Deprecation policy

Deprecation applies to documented, supported APIs and configuration only.
Preview and experimental surfaces may change or disappear in a minor release
without a deprecation-period promise.

For a supported surface, a deprecation must:

1. be announced in release notes and the relevant reference page;
2. identify the replacement, migration steps, and the first release where
   removal may occur;
3. emit a bounded, actionable warning where possible without exposing secrets
   or payloads;
4. remain supported for at least one subsequent supported minor release; and
5. be removed only in that announced window, unless a critical security or
   correctness issue requires an earlier exception.

The release manager records the affected contract, owner, issue, migration
evidence, and removal decision. A compatibility alias is not a reason to
freeze undocumented behavior.
