# pg_tide justfile

set shell := ["bash", "-euo", "pipefail", "-c"]

PG_TIDE_EXT := "pg-tide-ext"
PG_TIDE_RELAY := "pg-tide-relay"

# Format all code
fmt:
    cargo fmt --all

# Run clippy (must pass with zero warnings)
# pg-tide-ext (pgrx) needs cargo-pgrx + PostgreSQL 18; relay is pure Rust.
lint:
    cargo clippy --package pg-tide-relay --all-targets --all-features -- -D warnings
    cargo fmt --all -- --check

# Run unit tests (no DB required)
test-unit:
    cargo test --package {{PG_TIDE_EXT}} --lib -- --test-threads=4
    cargo test --package {{PG_TIDE_RELAY}} --bins -- --test-threads=4

# Run integration tests (requires Docker for testcontainers)
test-integration:
    cargo test --package {{PG_TIDE_RELAY}} --test '*' -- --test-threads=1

# Run pgrx extension tests (requires PostgreSQL 18)
test-pgrx:
    cargo pgrx test pg18 --package {{PG_TIDE_EXT}}

# Run all tests
test-all: test-unit test-integration

# Build the relay binary
build-relay:
    cargo build --package {{PG_TIDE_RELAY}} --release

# Build all
build:
    cargo build --all

# Check all
check:
    cargo check --all

# Build documentation (requires mdbook)
docs-build:
    mdbook build

# Serve documentation locally
docs-serve:
    mdbook serve --open

# Build Docker image
docker-build:
    docker build -t ghcr.io/trickle-labs/pg-tide:latest .

# Run cargo audit (known-unfixable advisories in optional-feature deps are ignored;
# see audit.toml for justification. All ignored advisories are optional-feature only.)
audit:
    cargo audit \
        --ignore RUSTSEC-2026-0119 \
        --ignore RUSTSEC-2026-0118 \
        --ignore RUSTSEC-2026-0104 \
        --ignore RUSTSEC-2026-0098 \
        --ignore RUSTSEC-2026-0099 \
        --ignore RUSTSEC-2026-0049 \
        --ignore RUSTSEC-2024-0436 \
        --ignore RUSTSEC-2025-0134 \
        --ignore RUSTSEC-2021-0127

# Run Criterion benchmarks
bench:
    cargo bench --package pg-tide-relay

# Default: fmt + lint + test-unit
all: fmt lint test-unit

# Bump version atomically across Cargo.toml, pg_tide.control, and Helm chart.
# Usage: just bump-version 0.19.0
bump-version VERSION:
    #!/usr/bin/env bash
    set -euo pipefail
    OLD=$(grep '^version = ' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/')
    echo "Bumping ${OLD} → {{VERSION}}"
    # Cargo workspace version
    sed -i.bak "s/^version = \"${OLD}\"/version = \"{{VERSION}}\"/" Cargo.toml && rm Cargo.toml.bak
    # pg_tide.control default_version (root and extension copy)
    sed -i.bak "s/default_version = '${OLD}'/default_version = '{{VERSION}}'/" pg_tide.control && rm pg_tide.control.bak
    sed -i.bak "s/default_version = '${OLD}'/default_version = '{{VERSION}}'/" pg-tide-ext/pg_tide.control && rm pg-tide-ext/pg_tide.control.bak
    # Helm chart version and appVersion
    sed -i.bak "s/^version: ${OLD}/version: {{VERSION}}/" helm/pg-tide/Chart.yaml && rm helm/pg-tide/Chart.yaml.bak
    sed -i.bak "s/^appVersion: \"${OLD}\"/appVersion: \"{{VERSION}}\"/" helm/pg-tide/Chart.yaml && rm helm/pg-tide/Chart.yaml.bak
    echo "Done. Don't forget to create sql/pg_tide--${OLD}--{{VERSION}}.sql and update CHANGELOG.md"
