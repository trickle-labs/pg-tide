# Operational benchmark contract

This directory contains the reviewed v1 contract for v0.53 operational evidence.
It is deliberately separate from `pg-tide-relay/benches/throughput.rs`, which
remains an in-process Criterion **microbenchmark** and must not be cited as
relay capacity.

## Reference profile

The scheduled runner uses PostgreSQL 18 with the packaged extension, a real
`pg-tide` process, and NATS JetStream. It publishes through
`tide.outbox_publish()` and verifies destination identities, counts, and the
native checkpoint. The reference profile is `relay-core`: 1 KiB JSON, one
pipeline, bounded batches, and a fixed poll interval. Additional profiles cover
single/concurrent publish overhead, 16/64 KiB payloads, pipeline density,
outage recovery, retention, and HA interruption.

Every result records the commit and dirty state, operating system and
architecture, CPU and memory, PostgreSQL settings/version, NATS version,
payload, batch, poll interval, pipeline count, warmup, duration, and raw
correctness counts. Results belong under `target/` or CI artifacts, never in
the source tree.

## Baseline and variance policy

`baseline-v1.json` records the reviewed v1 aggregate and profile-instance
metadata. `budgets-v1.toml` is the only active budget contract;
`budgets-v0.43.toml` and `baseline-v0.43.json` are historical files. A baseline update
must include before/after raw artifacts, the cause, any capacity-documentation
impact, and reviewer approval. It must not be used to hide a budget regression.

Use three repetitions and compare medians. Latency and throughput from
GitHub-hosted runners are not comparable with a self-hosted baseline.

Validate the contract locally:

```bash
python3 scripts/check_operational_budgets.py --check-config
```

Scheduled comparisons use at least three comparable repetitions:

```bash
python3 scripts/check_operational_budgets.py \
  --tier scheduled --profile relay-core \
  --result target/operational-benchmarks/relay-core-1.json \
  --result target/operational-benchmarks/relay-core-2.json \
  --result target/operational-benchmarks/relay-core-3.json \
  --report target/operational-benchmarks/relay-core/comparison.json
```
