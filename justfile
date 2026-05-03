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

# Default: fmt + lint + test-unit
all: fmt lint test-unit
