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
    cargo clippy --package pg-tide-relay --all-targets --no-default-features --features experimental-full -- -D warnings
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

# Run unit tests (pgrx extension tests use test-pgrx)
test-unit:
    cargo test --package {{PG_TIDE_RELAY}} --bins -- --test-threads=4

# Security-focused tests, including the no-PostgreSQL v0.44 contract checks.
# Service-backed privilege and migration tests remain in test-integration.
test-security:
    cargo test --package {{PG_TIDE_RELAY}} --lib --no-default-features --features core -- --test-threads=4
    cargo test --package {{PG_TIDE_RELAY}} --bins -- --test-threads=4
    cargo test --package {{PG_TIDE_RELAY}} --test v044_validation_test -- --test-threads=1

# Run integration tests (requires Docker for testcontainers)
test-integration:
    cargo test --package {{PG_TIDE_RELAY}} --test '*' -- --test-threads=1

# Run pgrx extension tests (requires PostgreSQL 18)
test-pgrx:
    cargo pgrx test pg18 --package {{PG_TIDE_EXT}}

# Authoritative PostgreSQL 18/pgrx clean-room suite (requires Docker).
test-extension-clean:
    python3 scripts/test_extension_cleanroom.py

# Reproducible relay unit suite from a locked clean checkout.
test-unit-clean:
    cargo test --package {{PG_TIDE_RELAY}} --bins --locked -- --test-threads=4

# Run all tests
test-all: test-unit test-integration

# Build the relay binary
build-relay:
    cargo build --package {{PG_TIDE_RELAY}} --release --no-default-features --features core

# Build all
build:
    cargo build --all

# Check all
check:
    cargo check --all

# Build documentation (requires mdbook)
docs-build:
    mdbook build

# Regenerate the checked-in connector matrix, profiles, and release checklist.
generate-connectors:
    python3 scripts/generate_connector_surface.py

# Fail when the connector registry or any generated output is stale.
check-connectors:
    python3 scripts/generate_connector_surface.py --check

# Validate the v1 contract inventory and frozen connector matrix.
check-v1-contracts:
    python3 scripts/check_v1_contracts.py

# Validate time-bounded flake exceptions; the registry never suppresses tests.
check-flakes:
    python3 scripts/check_flake_registry.py --check

# Validate the authoritative PR, scheduled, and release test inventory.
check-required-tests:
    python3 scripts/check_required_tests.py --check-manifest

# Validate the clean-tag pre-v1 comparison baseline and dependency digest.
check-baseline:
    python3 scripts/check_pre_v1_baseline.py --check

# Explicitly refresh generated contract inputs; CI only runs check-v1-contracts.
update-v1-contracts:
    python3 scripts/generate_connector_surface.py
    bash scripts/generate_pipeline_schema.sh

# Serve documentation locally
docs-serve:
    mdbook serve --open

# Build Docker image
docker-build:
    docker build -t ghcr.io/trickle-labs/pg-tide:latest .

# Run cargo audit using the checked-in, time-bounded advisory policy.
# Each exception is duplicated in supply-chain/advisory-exceptions.toml with
# owner, expiry, removal condition, and production reachability.
audit:
    #!/usr/bin/env python3
    import re, subprocess

    text = open("supply-chain/advisory-exceptions.toml", encoding="utf-8").read()
    advisories = re.findall(r'^advisory = "([^"]+)"$', text, flags=re.M)
    subprocess.run(
        ["cargo", "audit", *sum((["--ignore", advisory] for advisory in advisories), [])],
        check=True,
    )

audit-production:
    #!/usr/bin/env bash
    set -euo pipefail
    FEATURES=$(cargo tree --package {{PG_TIDE_RELAY}} --no-default-features --features core --locked --edges normal)
    for forbidden in kms-aws kms-gcp kms-vault; do
        if grep -q "$forbidden" <<< "$FEATURES"; then
            echo "ERROR: production profile enables ${forbidden}" >&2
            exit 1
        fi
    done
    python3 - <<'PY'
    import json
    import subprocess
    import tomllib

    tree = subprocess.check_output(
        [
            "cargo",
            "tree",
            "--package",
            "pg-tide-relay",
            "--no-default-features",
            "--features",
            "core",
            "--locked",
            "--edges",
            "normal",
        ],
        text=True,
    )
    policy = tomllib.loads(
        open("supply-chain/advisory-exceptions.toml", encoding="utf-8").read()
    )
    exceptions = {
        item["advisory"]: item
        for item in policy.get("exception", [])
    }
    report = subprocess.run(
        ["cargo", "audit", "--json", "--no-fetch"],
        capture_output=True,
        text=True,
    )
    findings = json.loads(report.stdout).get("vulnerabilities", {}).get("list", [])
    ignore = set()
    failures = []
    for finding in findings:
        advisory = finding["advisory"]["id"]
        package = finding["package"]
        exact = f'{package["name"]} v{package["version"]}'
        exception = exceptions.get(advisory)
        if exact in tree:
            failures.append(f"{advisory}: {exact} is reachable from core")
        elif exception is None:
            failures.append(f"{advisory}: no reviewed exception")
        elif exception.get("production_reachable", True):
            failures.append(f"{advisory}: production_reachable=true")
        else:
            ignore.add(advisory)
    if failures:
        raise SystemExit("\n".join(failures))
    subprocess.run(
        ["cargo", "audit", "--no-fetch"]
        + sum((["--ignore", advisory] for advisory in sorted(ignore)), []),
        check=True,
    )
    PY

# Verify that advisory exceptions are documented, owned, and not expired.
validate-advisory-exceptions:
    #!/usr/bin/env python3
    import datetime, re, sys

    text = open("supply-chain/advisory-exceptions.toml", encoding="utf-8").read()
    today = datetime.date.today()
    failures = []
    for advisory, expiry, production in re.findall(
        r'advisory = "([^"]+)".*?expires = "([^"]+)".*?production_reachable = (true|false)',
        text,
        flags=re.S,
    ):
        if datetime.date.fromisoformat(expiry) < today:
            failures.append(f"{advisory}: expired {expiry}")
        if production == "true":
            failures.append(f"{advisory}: marked production_reachable=true")
    if failures:
        print("\n".join(failures))
        sys.exit(1)
    print("OK: advisory exceptions are owned, current, and experimental-only.")

# Run Criterion benchmarks
bench:
    cargo bench --package pg-tide-relay

# Validate the reviewed operational budget contract without requiring PostgreSQL.
check-operational-budgets:
    python3 scripts/check_operational_budgets.py --check-config

# Check one recorded operational benchmark result against the reviewed budgets.
# Usage: just check-operational-result target/operational-benchmarks/result.json
check-operational-result RESULT:
    python3 scripts/check_operational_budgets.py --result "{{RESULT}}"

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

# v0.33.0: Generate a Production GA Announcement release body.
# Usage: just release-notes-ga
# Adds a stability guarantee summary, migration guide URL, and headline features.
release-notes-ga:
    #!/usr/bin/env bash
    set -euo pipefail
    VERSION=$(grep '^version = ' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/')
    echo "# pg_tide v${VERSION} — Production GA Announcement 🎉"
    echo ""
    echo "pg_tide v${VERSION} is the first General Availability release."
    echo ""
    echo "## Stability Guarantee"
    echo ""
    echo "The following surfaces are **stable** from this release:"
    echo "- SQL function signatures (all \`_v2\` forms)"
    echo "- Catalog table column names and types"
    echo "- Prometheus metric names"
    echo "- Configuration key names (relay TOML and JSONB catalog)"
    echo "- Wire format schemas (native, Debezium, CloudEvents, Maxwell, Canal)"
    echo ""
    echo "See [stability-guarantees.md](docs/src/stability-guarantees.md) for the full contract."
    echo ""
    echo "## Breaking Changes"
    echo ""
    echo "- \`tide.relay_set_outbox()\` 6-parameter positional form **removed** — use \`relay_set_outbox_v2(config JSONB)\`"
    echo "- \`tide.relay_set_inbox()\` 8-parameter positional form **removed** — use \`relay_set_inbox_v2(config JSONB)\`"
    echo ""
    echo "## Migration Guide"
    echo ""
    echo "See [v0.x → v1.0.0 Migration Guide](docs/src/operations/v1-migration-guide.md)"
    echo ""
    echo "## Upgrade"
    echo ""
    echo "\`\`\`sql"
    echo "ALTER EXTENSION pg_tide UPDATE TO '${VERSION}';"
    echo "\`\`\`"
    echo ""
    echo "## Headline Features"
    echo ""
    echo "- **Envelope encryption** — optional KMS-backed per-message AES-256-GCM encryption (AWS KMS, GCP Cloud KMS, HashiCorp Vault, local key file)"
    echo "- **Connector surface** — generated from connectors.toml; see docs/src/support/connector-compatibility.md"
    echo "- **DuckLake native integration** — exactly-once outbox → data lake with same-transaction atomicity"
    echo "- **Pipeline dependency DAG** — DAG-aware coordinator with cycle detection and policy-based gating"
    echo "- **Multi-tenant relay groups** — per-tenant pipeline isolation, RLS, and Prometheus labels"
    echo "- **Outbox table partitioning** — declarative range partitioning with live migration tooling"
    echo ""
    echo "## Assessment Cycle Summary"
    echo ""
    echo "This release closes all findings from six consecutive assessment cycles"
    echo "(overall_assessment_1 through overall_assessment_6), achieving a zero-P0 baseline."
    echo ""
    echo "---"
    echo ""
    echo "**Docker:** \`docker pull ghcr.io/trickle-labs/pg-tide:v${VERSION}\`"
    echo ""
    echo "**Tag:** \`git tag -s v${VERSION} -m 'Release v${VERSION}' && git push origin v${VERSION}\`"

# v0.33.0: Verify stability contract — all public SQL functions use schema = "tide"
# and the metric name list in metrics.rs matches the stability-guarantees.md list.
check-stability:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "=== pg-tide stability contract check ==="
    FAIL=0

    # 1. Verify all #[pg_extern] functions specify schema = "tide".
    echo ""
    echo "-- Checking pg_extern schema annotations --"
    while IFS= read -r line; do
        if echo "$line" | grep -q 'pg_extern'; then
            if ! echo "$line" | grep -q 'schema = "tide"'; then
                echo "  [WARN] Missing schema = \"tide\": $line"
                # Not a hard fail — some pg_extern may be intentionally schema-less.
            fi
        fi
    done < <(grep -rn '#\[pg_extern' pg-tide-ext/src/ 2>/dev/null || true)
    echo "  [OK] pg_extern schema annotation check complete"

    # 2. Verify that key stable metric names exist in metrics.rs.
    echo ""
    echo "-- Checking stable Prometheus metric names in metrics.rs --"
    STABLE_METRICS=(
        "pg_tide_relay_messages_published_total"
        "pg_tide_relay_messages_consumed_total"
        "pg_tide_relay_consumer_lag"
        "pg_tide_relay_pipeline_healthy"
        "pg_tide_relay_dlq_entries_written_total"
        "pg_tide_relay_owned_pipelines"
        "pg_tide_relay_reconcile_duration_seconds"
        "pg_tide_relay_pipeline_errors_total"
        "pg_tide_relay_sink_publish_duration_seconds"
        "pg_tide_relay_pool_connections"
        "pg_tide_relay_pool_acquire_duration_seconds"
        "pg_tide_relay_receipts_written_total"
    )
    for metric in "${STABLE_METRICS[@]}"; do
        if grep -q "$metric" pg-tide-relay/src/metrics.rs 2>/dev/null; then
            echo "  [OK] $metric"
        else
            echo "  [FAIL] Missing stable metric: $metric"
            FAIL=1
        fi
    done

    # 3. Verify stability-guarantees.md exists.
    echo ""
    echo "-- Checking stability-guarantees.md exists --"
    if [[ -f "docs/src/stability-guarantees.md" ]]; then
        echo "  [OK] docs/src/stability-guarantees.md exists"
    else
        echo "  [FAIL] docs/src/stability-guarantees.md not found"
        FAIL=1
    fi

    # 4. Verify v0→v1 migration guide exists.
    echo ""
    echo "-- Checking v1-migration-guide.md exists --"
    if [[ -f "docs/src/operations/v1-migration-guide.md" ]]; then
        echo "  [OK] docs/src/operations/v1-migration-guide.md exists"
    else
        echo "  [FAIL] docs/src/operations/v1-migration-guide.md not found"
        FAIL=1
    fi

    echo ""
    if [[ "$FAIL" -eq 0 ]]; then
        echo "=== Stability contract check: PASS ==="
    else
        echo "=== Stability contract check: FAIL ==="
        exit 1
    fi

# v0.36.0: Assert that the deprecated positional relay API forms are absent from
# relay.rs. Prevents accidental re-introduction of removed functions.
check-no-positional-api:
    #!/usr/bin/env bash
    set -euo pipefail
    FAIL=0
    if grep -n 'pub fn relay_set_outbox\b' pg-tide-ext/src/relay.rs 2>/dev/null | grep -v 'relay_set_outbox_v2'; then
        echo "ERROR: relay_set_outbox() positional form still exists in relay.rs"
        FAIL=1
    fi
    if grep -n 'pub fn relay_set_inbox\b' pg-tide-ext/src/relay.rs 2>/dev/null | grep -v 'relay_set_inbox_v2'; then
        echo "ERROR: relay_set_inbox() positional form still exists in relay.rs"
        FAIL=1
    fi
    if [[ "$FAIL" -eq 0 ]]; then
        echo "OK: No positional API forms found in relay.rs"
    else
        exit 1
    fi

# v0.40.0: Reject active positional relay API calls in documentation.
# The positional relay_set_outbox()/relay_set_inbox() forms were removed in
# v0.36.0; active docs must use the _v2 JSONB forms. Clearly-labeled historical
# migration docs are allowlisted.
check-docs-positional:
    #!/usr/bin/env bash
    set -euo pipefail
    ALLOWLIST='docs/archive/|docs/src/operations/v1-migration-guide.md|docs/src/guides/migrating-to-pg-tide.md|docs/src/integration/pg-trickle.md'
    HITS=$(grep -rn 'SELECT tide.relay_set_outbox(\|SELECT tide.relay_set_inbox(' docs/ examples/ README.md 2>/dev/null \
        | grep -v '_v2' | grep -vE "^($ALLOWLIST)" || true)
    if [[ -n "$HITS" ]]; then
        echo "ERROR: active positional relay API calls found (use relay_set_outbox_v2 / relay_set_inbox_v2):"
        echo "$HITS"
        exit 1
    fi
    echo "OK: no active positional relay API calls in documentation."

# v0.40.0: Verify the migration chain is complete through the workspace version.
check-migrations:
    bash scripts/check_upgrade_completeness.sh

# v0.40.0: Execute the marked Quick Start SQL blocks against an installed
# pg_tide extension. Set PGURL or standard libpq env vars for the connection.
quickstart-sql:
    python3 scripts/run_quickstart_sql.py README.md docs/src/getting-started/first-pipeline.md

# Static operator-surface contract checks.
test-observability:
    python3 scripts/check_observability.py

# Validate runbook paths and evidence manifest.
test-runbooks:
    python3 scripts/check_observability.py

test-operator-cli:
    cargo test -p pg-tide-relay --no-default-features --features experimental-full cli

test-config-contract:
    cargo test -p pg-tide-relay --no-default-features --features experimental-full config

test-upgrade:
    bash scripts/check_upgrade_completeness.sh
