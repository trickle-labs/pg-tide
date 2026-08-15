#!/usr/bin/env bash
# check_upgrade_completeness.sh — Verify the migration chain is complete.
#
# Discovers the base single-version files (pg_tide--X.Y.Z.sql) — NOT the
# adjacent upgrade files (pg_tide--A--B.sql) — orders them by version, and for
# each adjacent pair asserts that an upgrade script exists. Also asserts the
# chain reaches the current workspace version from Cargo.toml.
#
# Run this in CI to catch missing upgrade scripts early.

set -euo pipefail

SQL_DIR="${1:-sql}"

# The migration chain is defined by the adjacent upgrade files
# pg_tide--A--B.sql. Collect every version that appears as an endpoint, plus the
# base version, then order them and verify each adjacent pair has an upgrade file.
mapfile -t VERSIONS < <(
    find "$SQL_DIR" -maxdepth 1 -name 'pg_tide--*--*.sql' -exec basename {} \; \
    | sed -E 's/\.sql$//' \
    | awk -F'--' '{ print $2; print $3 }' \
    | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' \
    | sort -V -u
)

if [[ ${#VERSIONS[@]} -eq 0 ]]; then
    echo "ERROR: no base version files found in $SQL_DIR"
    exit 1
fi

echo "Discovered base versions: ${VERSIONS[*]}"

ERRORS=0
for ((i = 0; i < ${#VERSIONS[@]} - 1; i++)); do
    FROM="${VERSIONS[i]}"
    TO="${VERSIONS[i + 1]}"
    UPGRADE_FILE="$SQL_DIR/pg_tide--${FROM}--${TO}.sql"

    if [[ ! -f "$UPGRADE_FILE" ]]; then
        echo "ERROR: Missing upgrade script: $UPGRADE_FILE"
        ERRORS=$((ERRORS + 1))
    else
        echo "OK: $FROM -> $TO"
    fi
done

# Assert the chain reaches the current workspace version.
WORKSPACE_VERSION="$(grep '^version = ' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/')"
LAST_VERSION="${VERSIONS[${#VERSIONS[@]} - 1]}"
echo "Workspace version: $WORKSPACE_VERSION ; last base version: $LAST_VERSION"
if [[ "$LAST_VERSION" != "$WORKSPACE_VERSION" ]]; then
    # A base file for every version is not required; the final adjacent upgrade
    # script to the workspace version is.
    if [[ ! -f "$SQL_DIR/pg_tide--${LAST_VERSION}--${WORKSPACE_VERSION}.sql" ]]; then
        echo "ERROR: migration chain ends at $LAST_VERSION but the workspace is at $WORKSPACE_VERSION"
        echo "       and no sql/pg_tide--${LAST_VERSION}--${WORKSPACE_VERSION}.sql exists."
        ERRORS=$((ERRORS + 1))
    else
        echo "OK: final upgrade $LAST_VERSION -> $WORKSPACE_VERSION exists"
    fi
fi

if [[ $ERRORS -gt 0 ]]; then
    echo ""
    echo "FAILED: $ERRORS missing upgrade script(s)"
    exit 1
fi

echo ""
echo "All upgrade paths verified through the workspace version."
