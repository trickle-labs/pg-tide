# Operational benchmark contract

This directory contains the reviewed contract for v0.43 operational evidence.
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

`baseline.json` is the schema-checked slot for the reviewed v0.42 baseline. It
is intentionally marked `pending_reference_run` until the reference runner
produces measured values; nulls are not capacity claims. A baseline update
must include before/after raw artifacts, the cause, any capacity-documentation
impact, and reviewer approval. It must not be used to hide a budget regression.

Use three repetitions and compare medians. Latency and throughput from
GitHub-hosted runners are not comparable with a self-hosted baseline.

Validate the contract locally:

```bash
python3 scripts/check_operational_budgets.py --check-config
```

Once a reference result exists, check it with:

```bash
python3 scripts/check_operational_budgets.py \
  --result target/operational-benchmarks/result.json
```
