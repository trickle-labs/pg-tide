#!/usr/bin/env python3
"""Check small, repository-wide hygiene rules without mutating files."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_INDEX = ROOT / "scripts/README.md"
REMOVED = {"add_inbox_fleet_panel.py", "add_receipt_panels.py", "write_tests.py"}
HOME_PATH = re.compile(r"/(?:Users|home)/([^/\s]+)/")


def tracked_files() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files"], cwd=ROOT, check=True, capture_output=True, text=True
    )
    return [ROOT / name for name in result.stdout.splitlines()]


def check() -> None:
    errors: list[str] = []
    files = tracked_files()
    corpus = "\n".join(
        path.read_text(encoding="utf-8", errors="ignore")
        for path in files
        if path.is_file()
    )

    for name in REMOVED:
        path = ROOT / "scripts" / name
        if path.exists():
            errors.append(f"obsolete script remains: {path.relative_to(ROOT)}")

    if not SCRIPT_INDEX.is_file():
        errors.append("scripts/README.md is missing")
    else:
        for path in sorted((ROOT / "scripts").glob("*.py")) + sorted((ROOT / "scripts").glob("*.sh")):
            if path.name in REMOVED:
                continue
            if path.name not in corpus:
                errors.append(f"script has no indexed purpose or caller: {path.relative_to(ROOT)}")

    for path in files:
        if path.stat().st_size > 1024 * 1024:
            errors.append(f"tracked file exceeds 1 MiB: {path.relative_to(ROOT)}")
        if path == ROOT / "release-evidence/pre-v1-baseline/dependency-tree.txt":
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        for match in HOME_PATH.finditer(text):
            if match.group(1) not in {"pgtide"}:
                errors.append(f"developer-home path in {path.relative_to(ROOT)}")
                break

    if errors:
        raise SystemExit("\n".join(errors))
    print("repository hygiene valid")


if __name__ == "__main__":
    try:
        check()
    except (OSError, subprocess.CalledProcessError) as error:
        print(f"repository hygiene failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
