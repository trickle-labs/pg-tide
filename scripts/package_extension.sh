#!/usr/bin/env bash
# Stage a deterministic PostgreSQL 18 pg_tide extension artifact.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION="$(sed -n '/^\[workspace.package\]/,/^\[/p' "$ROOT/Cargo.toml" | sed -n 's/^version = "\([^"]*\)"/\1/p' | head -1)"
PG_CONFIG="${PG_CONFIG:-}"
POLICY="${LIFECYCLE_POLICY:-$ROOT/schemas/lifecycle-compatibility-v1.json}"
OUTPUT="$ROOT/target/pg_tide-pg18"
ARCHIVE="$ROOT/target/pg_tide-${VERSION}-pg18.tar.gz"

usage() {
    echo "Usage: $0 [--pg-config PATH] [--policy PATH] [--output DIR] [--archive FILE]" >&2
}

while (($#)); do
    case "$1" in
        --pg-config) PG_CONFIG="${2:?missing path after --pg-config}"; shift 2 ;;
        --policy) POLICY="${2:?missing path after --policy}"; shift 2 ;;
        --output) OUTPUT="${2:?missing directory after --output}"; shift 2 ;;
        --archive) ARCHIVE="${2:?missing file after --archive}"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) usage; exit 2 ;;
    esac
done

if [[ -z "$PG_CONFIG" ]]; then
    PG_CONFIG="$(command -v pg_config18 || command -v pg_config || true)"
fi

[[ -n "$PG_CONFIG" && -x "$PG_CONFIG" ]] || {
    echo "package_extension: PostgreSQL 18 pg_config is required (use --pg-config)" >&2
    exit 2
}
[[ -f "$POLICY" ]] || {
    echo "package_extension: lifecycle policy is required: $POLICY" >&2
    exit 2
}
command -v python3 >/dev/null || {
    echo "package_extension: python3 is required for policy and archive verification" >&2
    exit 2
}
command -v cargo >/dev/null || {
    echo "package_extension: cargo is required" >&2
    exit 2
}
cargo pgrx --version >/dev/null 2>&1 || {
    echo "package_extension: cargo-pgrx is required (expected cargo pgrx)" >&2
    exit 2
}

PG_VERSION="$($PG_CONFIG --version)"
[[ "$PG_VERSION" =~ PostgreSQL[[:space:]]18([.[:space:]]|$) ]] || {
    echo "package_extension: PostgreSQL 18 required, got: $PG_VERSION" >&2
    exit 2
}

POLICY_ROWS="$(mktemp "${TMPDIR:-/tmp}/pg-tide-policy.XXXXXX")"
trap 'rm -f "$POLICY_ROWS"' EXIT
python3 - "$ROOT" "$POLICY" >"$POLICY_ROWS" <<'PY'
import json
import re
import sys
from pathlib import Path

root = Path(sys.argv[1]).resolve()
policy_path = Path(sys.argv[2]).resolve()
try:
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError) as exc:
    raise SystemExit(f"package_extension: invalid lifecycle policy {policy_path}: {exc}")

rows = []

def visit(value):
    if isinstance(value, dict):
        source = value
        old = source.get("from", source.get("from_version"))
        new = source.get("to", source.get("to_version"))
        if isinstance(old, str) and isinstance(new, str):
            forward = source.get("forward_sql", source.get("forward"))
            if forward is None:
                forward = source.get("sql", source.get("file"))
            reverse = source.get("reverse_sql", source.get("reverse"))
            if isinstance(forward, dict):
                forward = forward.get("file", forward.get("path"))
            if isinstance(reverse, dict):
                reverse = reverse.get("file", reverse.get("path"))
            if forward is None:
                forward = f"sql/pg_tide--{old.lstrip('v')}--{new.lstrip('v')}.sql"
            rows.append((old.lstrip("v"), new.lstrip("v"), forward, reverse))
        for child in value.values():
            visit(child)
    elif isinstance(value, list):
        for child in value:
            visit(child)

visit(policy)
unique = {}
for old, new, forward, reverse in rows:
    key = (old, new)
    if key in unique and unique[key] != (forward, reverse):
        raise SystemExit(f"package_extension: duplicate lifecycle policy row for {old}->{new}")
    unique[key] = (forward, reverse)

if not unique:
    raise SystemExit(f"package_extension: lifecycle policy has no migration rows: {policy_path}")

version_re = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
for (old, new), (forward, reverse) in sorted(unique.items()):
    if not version_re.fullmatch(old) or not version_re.fullmatch(new):
        raise SystemExit(f"package_extension: invalid migration endpoint {old}->{new}")
    for kind, path_value in (("forward", forward), ("reverse", reverse)):
        if path_value is None:
            continue
        if not isinstance(path_value, str) or Path(path_value).is_absolute():
            raise SystemExit(f"package_extension: {kind} migration path must be relative for {old}->{new}")
        path = (root / path_value).resolve()
        if root not in path.parents or not path.is_file():
            raise SystemExit(f"package_extension: policy {kind} file missing for {old}->{new}: {path_value}")
        match = re.fullmatch(r"pg_tide--([0-9]+\.[0-9]+\.[0-9]+)--([0-9]+\.[0-9]+\.[0-9]+)\.sql", path.name)
        expected = (old, new) if kind == "forward" else (new, old)
        if not match or match.groups() != expected:
            expected_text = f"{expected[0]}->{expected[1]}"
            raise SystemExit(f"package_extension: {kind} file endpoint mismatch for {old}->{new}: {path.name} (expected {expected_text})")
    print("\t".join((old, new, str(forward), str(reverse or ""))))
PY
mapfile -t MIGRATIONS < "$POLICY_ROWS"

TARGET_DECLARED=false
for row in "${MIGRATIONS[@]}"; do
    IFS=$'\t' read -r old new forward reverse <<<"$row"
    if [[ "$new" == "$VERSION" ]]; then
        TARGET_DECLARED=true
    fi
done
[[ "$TARGET_DECLARED" == true ]] || {
    echo "package_extension: lifecycle policy does not declare a migration ending at $VERSION" >&2
    exit 2
}

WORK="$(mktemp -d "${TMPDIR:-/tmp}/pg-tide-package.XXXXXX")"
trap 'rm -f "$POLICY_ROWS"; rm -rf "$WORK"' EXIT

echo "package_extension: building PostgreSQL 18 artifact with $PG_CONFIG"
cargo pgrx package \
    --package pg-tide-ext \
    --pg-config "$PG_CONFIG" \
    --out-dir "$WORK/pgrx"

PG_SHARE="$($PG_CONFIG --sharedir)"
EXT_DIR="$WORK/pgrx/${PG_SHARE#/}/extension"
[[ -d "$EXT_DIR" ]] || {
    echo "package_extension: cargo pgrx package produced no PostgreSQL 18 extension directory" >&2
    exit 1
}

for row in "${MIGRATIONS[@]}"; do
    IFS=$'\t' read -r old new forward reverse <<<"$row"
    cp "$ROOT/$forward" "$EXT_DIR/$(basename "$forward")"
    if [[ -n "$reverse" ]]; then
        cp "$ROOT/$reverse" "$EXT_DIR/$(basename "$reverse")"
    fi
done

INSTALL_SQL="$EXT_DIR/pg_tide--${VERSION}.sql"
CONTROL="$EXT_DIR/pg_tide.control"
LIBRARY="$(find "$WORK/pgrx" -type f \( -name 'pg_tide.so' -o -name 'pg_tide.dylib' \) -print -quit)"
[[ -f "$CONTROL" ]] || { echo "package_extension: missing $CONTROL" >&2; exit 1; }
[[ -f "$INSTALL_SQL" ]] || { echo "package_extension: missing $INSTALL_SQL" >&2; exit 1; }
[[ -n "$LIBRARY" ]] || { echo "package_extension: missing pg_tide.so" >&2; exit 1; }

# pgrx documents generated object ordering as unstable.  Its C-backed function
# blocks are independent after the schema/migration blocks, so sort only those
# blocks while preserving the dependency-ordered SQL supplied by pgrx.
python3 - "$INSTALL_SQL" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
begin = "/* <begin connected objects> */\n"
end = "/* </end connected objects> */"
chunks = text.split(begin)
blocks = [chunks[0]] + [begin + chunk for chunk in chunks[1:]]
function_indexes = []
for index, block in enumerate(blocks):
    body = block[len(begin):].split(end, 1)[0] if block.startswith(begin) else ""
    lines = body.splitlines()
    if len(lines) > 1 and lines[0].startswith("-- pg-tide-ext/") and lines[1].startswith("-- pg_tide::"):
        function_indexes.append(index)
sorted_functions = sorted((blocks[index] for index in function_indexes), key=lambda block: block[len(begin):].split(end, 1)[0])
for index, block in zip(function_indexes, sorted_functions):
    blocks[index] = block
path.write_text(begin.join(blocks), encoding="utf-8")
PY

rm -rf "$OUTPUT"
mkdir -p "$OUTPUT"
cp -a "$WORK/pgrx/." "$OUTPUT/"

python3 - "$OUTPUT" "$VERSION" <<'PY'
import hashlib
import sys
from pathlib import Path

stage = Path(sys.argv[1]).resolve()
version = sys.argv[2]
files = sorted(path for path in stage.rglob("*") if path.is_file())
if not files:
    raise SystemExit("package_extension: staged artifact is empty")
manifest = stage / "manifest.sha256"
with manifest.open("w", encoding="utf-8") as handle:
    for path in files:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        handle.write(f"{digest}  {path.relative_to(stage).as_posix()}\n")
manifest.chmod(0o644)
print(f"staged pg_tide {version}: {len(files)} files")
PY

mkdir -p "$(dirname "$ARCHIVE")"
python3 - "$OUTPUT" "$ARCHIVE" <<'PY'
import gzip
import io
import sys
import tarfile
from pathlib import Path

stage = Path(sys.argv[1]).resolve()
archive = Path(sys.argv[2]).resolve()
archive.parent.mkdir(parents=True, exist_ok=True)
with archive.open("wb") as raw:
    with gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode="w") as tar:
            for path in sorted(stage.rglob("*")):
                relative = path.relative_to(stage).as_posix()
                info = tar.gettarinfo(str(path), arcname=relative)
                info.uid = info.gid = 0
                info.uname = info.gname = ""
                info.mtime = 0
                if path.is_file() and not path.is_symlink():
                    tar.addfile(info, io.BytesIO(path.read_bytes()))
                else:
                    tar.addfile(info)
PY

python3 - "$ARCHIVE" "${ARCHIVE}.sha256" <<'PY'
import hashlib
import sys
from pathlib import Path

archive = Path(sys.argv[1])
digest = hashlib.sha256(archive.read_bytes()).hexdigest()
Path(sys.argv[2]).write_text(f"{digest}  {archive.name}\n", encoding="utf-8")
print(f"archive: {archive}")
print(f"sha256:  {digest}")
PY
