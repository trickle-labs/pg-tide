#!/usr/bin/env python3
"""Check release archives and staged trees against the path allowlist."""
from __future__ import annotations

import argparse
import fnmatch
import io
import re
import stat
import sys
import tarfile
import tomllib
import zipfile
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]


def fail(message: str) -> None:
    raise SystemExit(f"artifact check failed: {message}")


def allowed(path: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns)


def check_entries(name: str, entries: list[tuple[str, int, bool, bytes | None]], policy: dict) -> None:
    files = policy.get("files", {})
    patterns = files.get("allowed_paths", [])
    executables = set(files.get("expected_executables", []))
    forbidden = policy.get("forbidden", {})
    forbidden_names = [re.compile(p, re.I) for p in forbidden.get("filenames", [])]
    forbidden_content = [re.compile(p.encode(), re.I) for p in forbidden.get("content", [])]
    if len(entries) > files.get("max_file_count", 1000):
        fail(f"{name}: too many entries ({len(entries)})")
    total = 0
    seen = set()
    for path, mode, is_link, content in entries:
        if not path or path.startswith("/") or ".." in PurePosixPath(path).parts:
            fail(f"{name}: unsafe archive path {path!r}")
        if path in seen:
            fail(f"{name}: duplicate path {path}")
        seen.add(path)
        if not allowed(path, patterns):
            fail(f"{name}: path is not allowlisted: {path}")
        if any(pattern.search(Path(path).name) for pattern in forbidden_names):
            fail(f"{name}: forbidden filename: {path}")
        if mode & stat.S_ISUID or mode & stat.S_ISGID or mode & 0o002:
            fail(f"{name}: unsafe mode {mode:o}: {path}")
        if is_link:
            fail(f"{name}: symlink is not allowed: {path}")
        executable = bool(mode & 0o111)
        if executable != (path in executables):
            fail(f"{name}: unexpected executable mode: {path}")
        if content is not None:
            total += len(content)
            if any(pattern.search(content) for pattern in forbidden_content):
                fail(f"{name}: forbidden content: {path}")
    if total > files.get("max_unpacked_bytes", 100_000_000):
        fail(f"{name}: unpacked size exceeds policy")
    if executables and not (executables & seen):
        fail(f"{name}: none of the expected executables was present: {', '.join(sorted(executables))}")


def archive_entries(path: Path) -> list[tuple[str, int, bool, bytes | None]]:
    if path.name.endswith((".tar", ".tar.gz", ".tgz")):
        with tarfile.open(path, "r:*") as archive:
            rows = []
            for member in archive.getmembers():
                if member.isdir():
                    continue
                if not (member.isfile() or member.issym() or member.islnk()):
                    fail(f"{path}: special archive entry: {member.name}")
                content = archive.extractfile(member).read() if member.isfile() else None
                rows.append((member.name, member.mode, member.issym() or member.islnk(), content))
            return rows
    if path.suffix == ".zip":
        with zipfile.ZipFile(path) as archive:
            return [
                (item.filename, item.external_attr >> 16 or (0o755 if item.filename.endswith(".exe") else 0o644), False, archive.read(item))
                for item in archive.infolist()
                if not item.is_dir()
            ]
    fail(f"unsupported artifact type: {path}")


def directory_entries(path: Path) -> list[tuple[str, int, bool, bytes | None]]:
    rows = []
    for item in sorted(path.rglob("*")):
        relative = item.relative_to(path).as_posix()
        mode = item.lstat().st_mode
        if item.is_dir():
            continue
        rows.append((relative, mode, item.is_symlink(), item.read_bytes() if item.is_file() else None))
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="*", type=Path)
    parser.add_argument("--profile", default="core")
    parser.add_argument("--static", action="store_true", help="validate the manifest without an artifact")
    parser.add_argument("--allowlist", type=Path, default=ROOT / "supply-chain/artifact-allowlist.toml")
    args = parser.parse_args()
    manifest = tomllib.loads(args.allowlist.read_text(encoding="utf-8"))
    policy = manifest.get("artifact", {}).get(args.profile)
    if policy is None:
        fail(f"unknown profile {args.profile!r}")
    policy["forbidden"] = manifest.get("forbidden", {})
    if args.static:
        if not policy.get("files", {}).get("allowed_paths"):
            fail(f"{args.profile}: no allowed paths")
        print(f"OK static allowlist ({args.profile}); artifact evidence remains pending")
        return 0
    if not args.paths:
        parser.error("paths are required unless --static is used")
    for path in args.paths:
        path = path.resolve()
        if not path.exists():
            fail(f"missing artifact: {path}")
        check_entries(str(path), directory_entries(path) if path.is_dir() else archive_entries(path), policy)
        print(f"OK {path} ({args.profile})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
