#!/usr/bin/env python3
"""Check the executable-example marker contract for current documentation."""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DOCS = [ROOT / "README.md", *sorted((ROOT / "docs/src").rglob("*.md"))]
MARKER = re.compile(
    r"^<!-- pg-tide-example: (tested) id=([a-z0-9][a-z0-9-]*) "
    r"test=([a-z0-9][a-z0-9-]*) -->$"
)
CLASSIFICATION = re.compile(r"^<!-- pg-tide-example: (illustrative|historical|labs) -->$")
FENCE = re.compile(r"^```(?:[A-Za-z0-9_+-]+)?\s*$")


class DocumentationError(ValueError):
    pass


def required_tests() -> dict[str, dict]:
    with (ROOT / "tests/required-tests.toml").open("rb") as handle:
        rows = tomllib.load(handle).get("test", [])
    return {row.get("id"): row for row in rows if isinstance(row, dict)}


def check_document(path: Path, tests: dict[str, dict], ids: set[str]) -> None:
    lines = path.read_text(encoding="utf-8").splitlines()
    for index, line in enumerate(lines):
        text = line.strip()
        match = MARKER.fullmatch(text)
        classification = CLASSIFICATION.fullmatch(text)
        if not match and not classification and "pg-tide-example:" in text:
            raise DocumentationError(f"{path}:{index + 1}: invalid example marker")
        if not match and not classification:
            if text == "<!-- quickstart:run -->":
                previous = lines[index - 1].strip() if index else ""
                if not previous:
                    previous = lines[index - 2].strip() if index > 1 else ""
                if not MARKER.fullmatch(previous):
                    raise DocumentationError(
                        f"{path}:{index + 1}: legacy Quick Start marker lacks a tested marker"
                    )
            continue

        next_index = index + 1
        while next_index < len(lines) and not lines[next_index].strip():
            next_index += 1
        next_line = lines[next_index].strip() if next_index < len(lines) else ""
        if next_line == "<!-- quickstart:run -->":
            next_index += 1
            while next_index < len(lines) and not lines[next_index].strip():
                next_index += 1
            next_line = lines[next_index].strip() if next_index < len(lines) else ""
        if not FENCE.fullmatch(next_line):
            raise DocumentationError(f"{path}:{index + 1}: marker must precede a fenced code block")
        if not match:
            continue

        _, example_id, test_id = match.groups()
        if example_id in ids:
            raise DocumentationError(f"{path}:{index + 1}: duplicate tested example id {example_id}")
        ids.add(example_id)
        row = tests.get(test_id)
        if row is None:
            raise DocumentationError(f"{path}:{index + 1}: unknown required-test id {test_id}")
        if row.get("blocking") is not True:
            raise DocumentationError(f"{path}:{index + 1}: {test_id} is not blocking")
        evidence = row.get("evidence")
        if not isinstance(evidence, list) or not evidence or not all(isinstance(item, str) and item for item in evidence):
            raise DocumentationError(f"{path}:{index + 1}: {test_id} has no evidence path")


def check() -> None:
    tests = required_tests()
    ids: set[str] = set()
    for path in DOCS:
        if path.is_file():
            check_document(path, tests, ids)
    if not ids:
        raise DocumentationError("no tested documentation examples found")
    print(f"documentation contract valid ({len(ids)} tested examples)")


if __name__ == "__main__":
    try:
        check()
    except (DocumentationError, OSError, tomllib.TOMLDecodeError) as error:
        print(f"documentation contract invalid: {error}", file=sys.stderr)
        sys.exit(1)
