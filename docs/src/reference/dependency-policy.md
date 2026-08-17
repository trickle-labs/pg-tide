# Dependency and Release Artifact Policy

Production profiles are audited independently from experimental profiles.
`core` and `core-kafka` must pass without advisory ignores. An experimental
exception is valid only when it is unreachable from production artifacts,
owned, dated, and unexpired in
`supply-chain/advisory-exceptions.toml`.

Dependencies, Rust, GitHub Actions, downloaded tools, and container base
images are reviewed weekly and pinned immutably. Critical and high-severity
advisories are triaged immediately; an exception has a removal condition and
cannot be a permanent release policy.

Every released archive and final image digest receives a matching signature,
SBOM, and build-provenance attestation. Release verification must compare the
artifact digest to the SBOM and provenance subject before publication.

Unsupported or unimplemented connector/provider dependencies are removed from
production profiles rather than hidden behind a successful-looking fallback.
`LocalKeyFile` is the only supported encryption provider in v0.44.0; cloud KMS
names are unavailable/experimental and fail before polling.
