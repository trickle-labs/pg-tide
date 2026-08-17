#!/usr/bin/env bash
set -euo pipefail

# Generate from the runtime Rust types, then use git diff --exit-code in CI
# to enforce that the checked-in schema is current.
cargo run -q -p pg-tide-relay --bin generate_pipeline_schema
