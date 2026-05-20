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

# v0.26.0: Guard against bare expect() in production (non-test) relay code.
# SAFETY-annotated expect() calls (preceded by // SAFETY: within 5 lines) are permitted.
# Test modules (#[cfg(test)] and #[cfg(all(test,...))]) are excluded.
lint-expect:
    #!/usr/bin/env python3
    import os, sys, re

    violations = []
    src_dir = "pg-tide-relay/src"

    for root, dirs, files in os.walk(src_dir):
        for fname in files:
            if not fname.endswith(".rs"):
                continue
            path = os.path.join(root, fname)
            with open(path) as f:
                lines = f.readlines()

            # Track whether we're inside a cfg(test) block using brace counting.
            in_test_depth = 0   # brace depth relative to start of cfg(test) block
            pending_test = False  # saw a #[cfg(...test...)] attribute, waiting for {

            for i, line in enumerate(lines):
                stripped = line.strip()

                # Detect #[cfg(...test...)] attributes
                if re.search(r'#\[cfg\(.*\btest\b', stripped):
                    pending_test = True

                # Detect block opens and closes
                opens = stripped.count("{")
                closes = stripped.count("}")

                if pending_test and opens > 0:
                    in_test_depth = opens - closes
                    pending_test = False
                elif in_test_depth > 0:
                    in_test_depth += opens - closes
                    if in_test_depth <= 0:
                        in_test_depth = 0

                # Skip lines in test blocks or that are comments
                if in_test_depth > 0:
                    continue
                if stripped.startswith("//"):
                    continue

                # Check for .expect( on this line
                if ".expect(" not in stripped:
                    continue

                # Check if any of the 5 preceding lines has // SAFETY:
                window_start = max(0, i - 5)
                window = lines[window_start:i]
                has_safety = any("// SAFETY:" in l for l in window)

                if not has_safety:
                    violations.append(f"{path}:{i+1}: {line.rstrip()}")

    if violations:
        print("ERROR: bare .expect() calls found in production relay code:")
        for v in violations:
            print(" ", v)
        print()
        print("Replace with .ok_or_else(|| RelayError::...) ? propagation, or")
        print("add a '// SAFETY: <reason>' comment within 5 lines before the expect() call.")
        sys.exit(1)
    else:
        print("OK: No bare expect() calls in production relay code.")

# v0.31.0: Guard against unquoted identifier interpolation in relay SQL format strings.
# Flags any format!() call that produces tide.{ident} (without surrounding double quotes).
# Permitted: tide."{ident}" (double-quoted) or static identifiers that never contain hyphens.
# Annotate permitted sites with: // QUOTED: tide."{name}" — <reason>
lint-quoting:
    #!/usr/bin/env python3
    import os, sys, re

    violations = []
    src_dir = "pg-tide-relay/src"

    # Match: tide.{ but NOT tide."{ and NOT preceded by // QUOTED:
    unquoted_pattern = re.compile(r'tide\.\{(?!")')

    for root, dirs, files in os.walk(src_dir):
        for fname in files:
            if not fname.endswith(".rs"):
                continue
            path = os.path.join(root, fname)
            with open(path) as f:
                lines = f.readlines()

            in_test_depth = 0
            pending_test = False

            for i, line in enumerate(lines):
                stripped = line.strip()

                if re.search(r'#\[cfg\(.*\btest\b', stripped):
                    pending_test = True

                opens = stripped.count("{")
                closes = stripped.count("}")

                if pending_test and opens > 0:
                    in_test_depth = opens - closes
                    pending_test = False
                elif in_test_depth > 0:
                    in_test_depth += opens - closes
                    if in_test_depth <= 0:
                        in_test_depth = 0

                if in_test_depth > 0:
                    continue
                if stripped.startswith("//"):
                    continue
                # Skip display/log lines — println!, eprintln!, tracing macros
                # These are human-readable messages, not SQL query strings.
                if re.search(r'println!\s*\(|eprintln!\s*\(|tracing::|format_args!', stripped):
                    continue

                if not unquoted_pattern.search(stripped):
                    continue

                # Allow if a // QUOTED: comment appears within 3 preceding lines
                window = lines[max(0, i-3):i]
                if any("// QUOTED:" in l for l in window):
                    continue

                violations.append(f"{path}:{i+1}: {line.rstrip()}")

    if violations:
        print("ERROR: unquoted identifier interpolation in relay SQL format strings:")
        for v in violations:
            print(" ", v)
        print()
        print("Change tide.{name} to tide.\"{name}\" (double-quoted),")
        print("or add a '// QUOTED: tide.\"{name}\" — <reason>' comment within 3 lines above.")
        sys.exit(1)
    else:
        print("OK: No unquoted SQL identifier interpolation found.")

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

# Generate GitHub Release body from CHANGELOG.md for the current workspace version.
# Usage: just release-notes
release-notes:
    #!/usr/bin/env bash
    set -euo pipefail
    VERSION=$(grep '^version = ' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/')
    echo "# Release v${VERSION}"
    echo ""
    # Extract the section for the current version from CHANGELOG.md
    awk "/^## \[${VERSION}\]/{found=1; next} found && /^## \[/{exit} found{print}" CHANGELOG.md
    echo ""
    echo "---"
    echo ""
    echo "**Migration:** \`ALTER EXTENSION pg_tide UPDATE TO '${VERSION}';\`"
    echo ""
    echo "**Docker:** \`docker pull ghcr.io/trickle-labs/pg-tide:v${VERSION}\`"
    echo ""
    echo "**Tag:** \`git tag -s v${VERSION} -m 'Release v${VERSION}' && git push origin v${VERSION}\`"
    echo ""
    # v0.30.0: For releases in the v0.28–v0.30 range, include a "Features Pulled from v1.x" section
    # that credits the original roadmap entries for traceability.
    MAJOR=$(echo "${VERSION}" | cut -d. -f1)
    MINOR=$(echo "${VERSION}" | cut -d. -f2)
    if [[ "${MAJOR}" == "0" && "${MINOR}" -ge 28 && "${MINOR}" -le 30 ]]; then
        echo "---"
        echo ""
        echo "## Features Pulled from v1.x Roadmap"
        echo ""
        echo "This release incorporates work originally planned for v1.0.0 GA, pulled forward"
        echo "to stabilise the v0.x series and give early adopters more time with these features:"
        echo ""
        # Extract bullet points from ROADMAP.md for this version's section.
        awk "/^#### v${VERSION}/,/^####/" ROADMAP.md \
          | grep '^- \*\*' \
          | sed 's/^- \*\*/- /' \
          | sed 's/\*\*.*//' \
          | head -20 || true
        echo ""
        echo "_See [ROADMAP.md](ROADMAP.md) for the full roadmap and version history._"
    fi
