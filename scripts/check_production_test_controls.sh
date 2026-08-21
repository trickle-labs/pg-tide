#!/usr/bin/env bash
set -euo pipefail

if cargo tree --package pg-tide-relay --no-default-features --features core -e features \
    | grep -q 'test-failpoints'; then
    echo "production feature graph includes test-failpoints" >&2
    exit 1
fi

cargo build --package pg-tide-relay --bin pg-tide --release \
    --no-default-features --features core >/dev/null
binary="target/release/pg-tide"
if strings "$binary" | grep -Eq 'PG_TIDE_FAILPOINT|after_poll_before_encode|during_replay'; then
    echo "production relay contains failpoint controls" >&2
    exit 1
fi

PG_TIDE_FAILPOINT=after_poll_before_encode "$binary" --help >/dev/null
echo "production relay excludes failpoint controls"
