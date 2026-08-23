# v1 reference environment

This document defines the identity of the runner used by the v0.53 operational
contract. It is a comparability boundary, not a hardware-independent capacity
claim. The captured JSON environment must match the identity fields in
[`budgets-v1.toml`](budgets-v1.toml) before a run starts and after it ends.

## Runner contract

The maintained reference runner is a dedicated Linux host labelled
`pg-tide-reference`. Its inventory is captured by the performance workflow;
values below are the required fields and accepted policy rather than a claim
about hosted CI machines.

| Area | Captured fields | Comparability rule |
|---|---|---|
| CPU | vendor, model, architecture, physical/logical cores, frequency policy | identity |
| Memory | total bytes, swap policy | identity; free space is a preflight floor |
| Storage | device, filesystem, mount options, capacity, free-space floor | identity |
| OS | distribution, kernel, cgroup version, PID namespace | identity |
| PostgreSQL | full version, major, settings digest, locale, checksums, enabled extensions | identity |
| Services | NATS and Kafka image digests, webhook endpoint code digest, PostgreSQL inbox version | identity per destination |
| Build | Rust toolchain, locked release profile, features, compiler flags | identity |
| Relay | binary digest, pool size, batch size, poll interval, log level, metrics address | identity per instance |
| Dataset | seed, payload distribution, compressibility, headers, metadata | identity per instance |
| Network | loopback/bridge/shaping parameters | identity per instance |
| Timing | warm-up, measured duration, sample interval, repetitions | recorded; tier controls acceptance |

The benchmark role must be able to read `pg_stat_statements`, `pgstattuple`,
the required `pg_stat_*` views, and run under a unique run-scoped
`application_name`. The sampler runs in the host PID namespace and includes the
verified PostgreSQL postmaster process tree, not only the benchmark backend.
The relay uses a known metrics port and the preflight waits for `/readyz` and
`/metrics` before publishing starts.

## Default v1 inputs

- PostgreSQL 18; `pg_stat_statements` preloaded and `pgstattuple` installed.
- Release-mode relay built from the exact candidate commit with the locked
  toolchain in `rust-toolchain.toml`.
- NATS JetStream `nats:2.10.22`; Kafka and the controlled webhook endpoint are
  pinned by digest in the workflow environment.
- Payload instances are 1 KiB, 16 KiB, and 64 KiB. The default distribution is
  deterministic, seeded, and uses bounded headers and metadata.
- Default relay batch size is 100 and poll interval is 100 ms. Density uses 1,
  10, and 50 pipelines; checkpoint-heavy uses batch size 1.
- Warm-up is excluded from measured samples. Scheduled runs repeat each
  comparable instance at least three times. Samples are tagged every 60
  seconds by default.
- Every run cleans its run-scoped outbox, pipelines, destination stream/topic,
  logs, and temporary files. It records cleanup outcome and never reuses a
  partial result after a retry.

## Retention and evidence

Raw bundles stay outside Git under the immutable CI/release artifact store for
the repository retention period (30 days for scheduled diagnostics). Release
qualification summaries, checksums, environment fingerprints, and bundle
digests remain for the supported life of the release. A failed probe is
`missing_sample`; it is never serialized as zero. A changed identity field is
`invalid_environment` and cannot update the baseline.

The runner owner must review hardware, service images, PostgreSQL settings, and
filesystem changes before replacing the host. Re-run the reference inventory
and collect a new reviewed baseline after any identity change.
