# Extension clean room

This harness is the authoritative PostgreSQL extension test environment for
v0.48.0. It builds a PostgreSQL 18.0 Bookworm image with Rust 1.97.1 and
`cargo-pgrx` 0.18.0, then runs the complete `pg-tide-ext` suite as the
non-root `pgtide` user.

Run it locally with:

```bash
just test-extension-clean
```

Docker is required. The command exits non-zero when Docker is unavailable,
when the image cannot be built, or when any extension test fails. A run writes
`target/extension-cleanroom/result.json`, the complete test log, the test
listing, version metadata, and any PostgreSQL logs found by pgrx. The result
records the source commit, dirty state, image ID, command, first failure, and
skipped-test count. It never retries a test.

The Docker build context excludes Git metadata and `target/`; no sibling
checkout or developer PostgreSQL installation is visible to the container.
The image tag is versioned in `environment.toml`; the result records the
content-addressed local image ID used for that run.