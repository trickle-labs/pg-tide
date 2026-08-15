#!/usr/bin/env python3
"""Execute marked Quick Start SQL blocks against an installed pg_tide extension.

Finds fenced ```sql blocks that are immediately preceded by the Quick Start
marker line ``<!-- quickstart:run -->`` in each given Markdown document,
concatenates them in document order into a single SQL stream (prefixed with
``\\set ON_ERROR_STOP on``), and runs the stream with ``psql``.

Standard library only. Temporary files are written next to this script (never
in /tmp) and deleted on exit.

Usage:
    run_quickstart_sql.py [--psql psql] [DOC.md ...]

Connection: pass standard libpq environment variables (PGHOST, PGPORT, PGUSER,
PGPASSWORD, PGDATABASE) or a single positional connection string via PGURL.
"""

import argparse
import os
import re
import subprocess
import sys
import tempfile

MARKER = "<!-- quickstart:run -->"
FENCE_RE = re.compile(r"^```sql\s*$")
FENCE_END_RE = re.compile(r"^```\s*$")

DEFAULT_DOCS = [
    "README.md",
    "docs/src/getting-started/first-pipeline.md",
]


def extract_blocks(path):
    """Return marked SQL blocks in document order as (block_number, sql)."""
    with open(path, encoding="utf-8") as fh:
        lines = fh.read().splitlines()

    blocks = []
    i = 0
    n = len(lines)
    while i < n:
        if lines[i].strip() == MARKER:
            # The next non-blank line must open a ```sql fence.
            j = i + 1
            while j < n and lines[j].strip() == "":
                j += 1
            if j < n and FENCE_RE.match(lines[j].strip()):
                body = []
                k = j + 1
                while k < n and not FENCE_END_RE.match(lines[k].strip()):
                    body.append(lines[k])
                    k += 1
                blocks.append("\n".join(body))
                i = k + 1
                continue
        i += 1
    return blocks


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--psql", default=os.environ.get("PSQL", "psql"))
    parser.add_argument("docs", nargs="*", default=[])
    args = parser.parse_args()

    docs = args.docs or DEFAULT_DOCS
    here = os.path.dirname(os.path.abspath(__file__))
    tmp_dir = os.path.join(here, ".quickstart-tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    failures = 0
    tmp_files = []
    try:
        for doc in docs:
            if not os.path.exists(doc):
                print(f"SKIP {doc}: not found")
                continue
            blocks = extract_blocks(doc)
            if not blocks:
                print(f"SKIP {doc}: no marked Quick Start SQL blocks")
                continue

            stream = "\\set ON_ERROR_STOP on\n"
            for idx, block in enumerate(blocks, start=1):
                stream += f"\\echo '-- {doc} block {idx}'\n{block}\n"

            fd, tmp_path = tempfile.mkstemp(suffix=".sql", dir=tmp_dir)
            tmp_files.append(tmp_path)
            with os.fdopen(fd, "w", encoding="utf-8") as out:
                out.write(stream)

            pgurl = os.environ.get("PGURL")
            cmd = [args.psql, "-v", "ON_ERROR_STOP=1", "-f", tmp_path]
            if pgurl:
                cmd = [args.psql, pgurl, "-v", "ON_ERROR_STOP=1", "-f", tmp_path]

            print(f"RUN  {doc}: {len(blocks)} marked block(s)")
            proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.returncode != 0:
                failures += 1
                # Best-effort mapping of the failing block via the \echo markers.
                print(f"FAIL {doc}: psql exited {proc.returncode}")
                sys.stderr.write(proc.stdout)
                sys.stderr.write(proc.stderr)
            else:
                print(f"OK   {doc}")
    finally:
        for f in tmp_files:
            try:
                os.remove(f)
            except OSError:
                pass
        try:
            os.rmdir(tmp_dir)
        except OSError:
            pass

    if failures:
        print(f"\nFAILED: {failures} document(s) had Quick Start SQL errors")
        return 1
    print("\nAll marked Quick Start SQL blocks executed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
