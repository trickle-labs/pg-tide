# Pre-v1 simplification baseline

This directory records the repository and artifact shape at the exact
`v0.47.0` tag before v0.49.0 simplification work. It is a comparison point,
not a capacity claim and not a reason to preserve unused product surface.

## Reproduce

The capture uses a clean tag archive and the pinned repository toolchain:

```bash
git archive v0.47.0 | tar -x -C "$TMPDIR/pg-tide-v0470"
(cd "$TMPDIR/pg-tide-v0470" && cargo metadata --locked --format-version=1)
(cd "$TMPDIR/pg-tide-v0470" && cargo tree --workspace --locked)
git ls-tree -r --name-only v0.47.0
python3 scripts/check_pre_v1_baseline.py --check
```

The exact commands, exit statuses, commit, dirty state, environment, and
retained dependency-tree digest are in `commands.json`. The full locked tree
is in `dependency-tree.txt`.

## Interpretation

`baseline.json` has `status` `complete-with-pending-fields`. Measured counts
include their method. A field that was not measured is `pending`,
`not_available`, or `not_applicable` with a reason; unknown values are never
filled with zero or an estimate. In particular, the operational benchmark is
still `pending_reference_run` with 21 null metrics and must not be cited as a
throughput, latency, memory, WAL, or recovery limit.

The committed files contain summaries and a dependency digest only. Generated
binaries, container layers, raw logs, payloads, credentials, and large
benchmark outputs remain CI artifacts or are omitted.