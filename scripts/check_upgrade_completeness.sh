#!/usr/bin/env bash
# check_upgrade_completeness.sh — Verify all upgrade paths exist.
#
# For each version pair (N → N+1), checks that a migration SQL file exists.
# Run this in CI to catch missing upgrade scripts early.

set -euo pipefail

SQL_DIR="${1:-sql}"

# Extract all version files matching pg_tide--X.Y.Z.sql
mapfile -t VERSIONS < <(
    find "$SQL_DIR" -name 'pg_tide--[0-9]*.[0-9]*.[0-9]*.sql' -exec basename {} \; \
    | sed 's/pg_tide--//; s/.sql//' \
    | sort -V
)

echo "Found versions: ${VERSIONS[*]}"

ERRORS=0
for ((i = 0; i < ${#VERSIONS[@]} - 1; i++)); do
    FROM="${VERSIONS[i]}"
    TO="${VERSIONS[i+1]}"
    UPGRADE_FILE="$SQL_DIR/pg_tide--${FROM}--${TO}.sql"

    if [[ ! -f "$UPGRADE_FILE" ]]; then
        echo "ERROR: Missing upgrade script: $UPGRADE_FILE"
        ERRORS=$((ERRORS + 1))
    else
        echo "OK: $FROM → $TO"
    fi
done

if [[ $ERRORS -gt 0 ]]; then
    echo ""
    echo "FAILED: $ERRORS missing upgrade script(s)"
    exit 1
fi

echo ""
echo "All upgrade paths verified."
