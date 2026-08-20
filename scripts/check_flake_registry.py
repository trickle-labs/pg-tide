#!/usr/bin/env python3
"""Validate the time-bounded test flake registry."""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from datetime import date
from pathlib import Path


ISSUE_RE = re.compile(r"(?:#\d+|https://github\.com/[^/]+/[^/]+/issues/\d+)$")
SEVERITIES = {"P0", "P1", "P2", "P3"}
STATUSES = {"active", "closed"}


def fail(message: str) -> None:
    raise ValueError(message)


def parse_date(value: object, field: str) -> date:
    if not isinstance(value, str):
        fail(f"{field} must be an ISO date")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        fail(f"{field} must be an ISO date: {error}")


def check_registry(path: Path, today: date) -> int:
    with path.open("rb") as registry_file:
        document = tomllib.load(registry_file)

    if document.get("schema_version") != 1:
        fail("schema_version must be 1")
    flakes = document.get("flake", [])
    if not isinstance(flakes, list):
        fail("flake must be an array of tables")

    seen: set[str] = set()
    for index, entry in enumerate(flakes, start=1):
        if not isinstance(entry, dict):
            fail(f"flake entry {index} must be a table")
        prefix = f"flake entry {index}"
        required = {
            "test_id",
            "owner",
            "first_observed",
            "failure_signature",
            "issue",
            "severity",
            "quarantine_status",
            "quarantine_expires",
            "release_impact",
            "last_observed",
            "attempt_count",
        }
        missing = sorted(required - entry.keys())
        if missing:
            fail(f"{prefix} missing: {', '.join(missing)}")

        test_id = entry["test_id"]
        if not isinstance(test_id, str) or not test_id:
            fail(f"{prefix}.test_id must be a non-empty string")
        if test_id in seen:
            fail(f"duplicate test_id: {test_id}")
        seen.add(test_id)

        owner = entry["owner"]
        if not isinstance(owner, str) or not owner.startswith("@"):
            fail(f"{prefix}.owner must identify a repository owner")
        signature = entry["failure_signature"]
        if not isinstance(signature, str) or not signature.strip():
            fail(f"{prefix}.failure_signature must be non-empty")
        if not isinstance(entry["issue"], str) or not ISSUE_RE.fullmatch(entry["issue"]):
            fail(f"{prefix}.issue must be #123 or a GitHub issue URL")
        if entry["severity"] not in SEVERITIES:
            fail(f"{prefix}.severity must be one of {sorted(SEVERITIES)}")
        if entry["quarantine_status"] not in STATUSES:
            fail(f"{prefix}.quarantine_status must be one of {sorted(STATUSES)}")
        first_observed = parse_date(entry["first_observed"], f"{prefix}.first_observed")
        last_observed = parse_date(entry["last_observed"], f"{prefix}.last_observed")
        expires = parse_date(entry["quarantine_expires"], f"{prefix}.quarantine_expires")
        if first_observed > last_observed:
            fail(f"{prefix}.first_observed must not be after last_observed")
        if expires <= first_observed:
            fail(f"{prefix}.quarantine_expires must be after first_observed")
        if not isinstance(entry["attempt_count"], int) or entry["attempt_count"] < 1:
            fail(f"{prefix}.attempt_count must be a positive integer")
        if not isinstance(entry["release_impact"], str) or not entry["release_impact"]:
            fail(f"{prefix}.release_impact must be non-empty")

        if entry["quarantine_status"] == "active":
            if expires <= today:
                fail(f"{test_id}: quarantine expired on {expires.isoformat()}")
            if entry["severity"] in {"P0", "P1"}:
                fail(f"{test_id}: P0/P1 tests cannot be quarantined")
            if "blocks" in entry["release_impact"].lower():
                fail(f"{test_id}: release-blocking tests cannot be quarantined")

    print(f"flake registry valid: {len(flakes)} entr{'y' if len(flakes) == 1 else 'ies'}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="validate the registry")
    parser.add_argument(
        "--path",
        type=Path,
        default=Path("tests/flake-registry.toml"),
        help="registry path",
    )
    parser.add_argument("--today", type=date.fromisoformat, default=date.today())
    args = parser.parse_args()
    if not args.check:
        parser.error("--check is required")
    try:
        return check_registry(args.path, args.today)
    except (OSError, tomllib.TOMLDecodeError, ValueError) as error:
        print(f"flake registry invalid: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())