# pg_tide justfile

set shell := ["bash", "-euo", "pipefail", "-c"]

PG_TIDE_EXT := "pg-tide-ext"
PG_TIDE_RELAY := "pg-tide-relay"

# Format all code
fmt:
    cargo fmt --all

# Run clippy (must pass with zero warnings)
lint:
    cargo clippy --all-targets --all-features -- -D warnings
    cargo fmt --all -- --check

# Run unit tests (no DB required)
test-unit:
    cargo test --package {{PG_TIDE_EXT}} --lib -- --test-threads=4
    cargo test --package {{PG_TIDE_RELAY}} --lib -- --test-threads=4

# Build the relay binary
build-relay:
    cargo build --package {{PG_TIDE_RELAY}} --release

# Build all
build:
    cargo build --all

# Check all
check:
    cargo check --all

# Default: fmt + lint + test-unit
all: fmt lint test-unit
