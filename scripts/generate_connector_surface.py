#!/usr/bin/env python3
"""Validate connectors.toml and render the small public connector surface."""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

import tomllib


ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "connectors.toml"
PROFILE_START = "# BEGIN GENERATED CONNECTOR PROFILES"
PROFILE_END = "# END GENERATED CONNECTOR PROFILES"
README_START = "<!-- BEGIN GENERATED CONNECTORS -->"
README_END = "<!-- END GENERATED CONNECTORS -->"
INTERNAL_FEATURES = {"test-failpoints"}
MATURITY = {"supported", "preview", "experimental"}
KINDS = {"connector", "diagnostic", "compatibility"}
EVIDENCE_FIELDS = (
    "contract_tests",
    "integration_tests",
    "e2e_tests",
    "failure_before_publish_tests",
    "failure_after_publish_tests",
    "restart_tests",
    "duplicate_tests",
    "auth_tests",
    "tls_tests",
    "redaction_tests",
    "metrics_evidence",
    "runbooks",
    "upgrade_tests",
)


def fail(message: str) -> None:
    raise ValueError(message)


def replace_block(path: Path, start: str, end: str, body: str) -> str:
    text = path.read_text()
    start_at = text.find(start)
    end_at = text.find(end)
    if start_at < 0 or end_at < start_at:
        fail(f"{path}: missing or inverted generated markers")
    before = text[:start_at]
    after = text[end_at + len(end) :]
    return f"{before}{start}\n{body.rstrip()}\n{end}{after}"


def path_list(row: dict, field: str) -> list[str]:
    value = row.get(field, [])
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        fail(f"{row.get('id', '<unknown>')}: {field} must be an array of strings")
    return value


def load_registry() -> tuple[dict, list[dict]]:
    try:
        data = tomllib.loads(REGISTRY.read_text())
    except (OSError, tomllib.TOMLDecodeError) as exc:
        fail(f"cannot read {REGISTRY}: {exc}")
    if data.get("schema_version") != 1:
        fail("connectors.toml: unsupported schema_version")
    rows = data.get("connector")
    if not isinstance(rows, list) or not rows:
        fail("connectors.toml: at least one [[connector]] row is required")
    return data, rows


def cargo_features() -> set[str]:
    data = tomllib.loads((ROOT / "pg-tide-relay/Cargo.toml").read_text())
    features = data.get("features")
    if not isinstance(features, dict):
        fail("pg-tide-relay/Cargo.toml: missing [features]")
    return set(features)


def factory_types() -> tuple[set[str], set[str]]:
    source = (ROOT / "pg-tide-relay/src/coordinator.rs").read_text()
    source_start = source.index("// ── Source factory")
    sink_start = source.index("async fn build_sink", source_start)
    helper_start = source.index("// ── Object store factory helper", sink_start)
    arm = re.compile(r'^        "([^"]+)"\s*=>', re.MULTILINE)
    return set(arm.findall(source[source_start:sink_start])), set(
        arm.findall(source[sink_start:helper_start])
    )


def validate(rows: list[dict]) -> tuple[set[str], set[str], set[str]]:
    features = cargo_features()
    ids: set[str] = set()
    source_types: dict[str, str] = {}
    sink_types: dict[str, str] = {}
    for row in rows:
        row_id = row.get("id")
        if not isinstance(row_id, str) or not row_id:
            fail("every connector needs a non-empty id")
        if row_id in ids:
            fail(f"duplicate connector id: {row_id}")
        ids.add(row_id)
        if row.get("maturity") not in MATURITY:
            fail(f"{row_id}: invalid maturity")
        if row.get("kind") not in KINDS:
            fail(f"{row_id}: invalid kind")
        owner = row.get("owner")
        if not isinstance(owner, str) or not re.fullmatch(r"@?[A-Za-z0-9][A-Za-z0-9_-]*", owner):
            fail(f"{row_id}: owner must be a real GitHub handle")
        if owner.lstrip("@").lower() in {"tbd", "todo", "unknown", "team", "placeholder"}:
            fail(f"{row_id}: placeholder owner")
        if row.get("maturity") == "supported" and row.get("kind") != "diagnostic":
            if not row.get("security_contact"):
                fail(f"{row_id}: supported connector needs security_contact")
            missing = [field for field in EVIDENCE_FIELDS if not path_list(row, field)]
            if missing:
                fail(f"{row_id}: supported connector missing evidence: {', '.join(missing)}")
        if row.get("default_build") and row.get("maturity") not in {"supported"}:
            fail(f"{row_id}: default_build requires supported maturity")
        feature = row.get("cargo_feature")
        if feature is not None and feature not in features:
            fail(f"{row_id}: unknown Cargo feature {feature!r}")
        for field, seen in (("source_types", source_types), ("sink_types", sink_types)):
            for runtime_type in path_list(row, field):
                previous = seen.get(runtime_type)
                if previous and previous != row_id:
                    fail(f"duplicate {field[:-6]} type {runtime_type!r}: {previous}, {row_id}")
                seen[runtime_type] = row_id
        for field in ("docs",) + EVIDENCE_FIELDS:
            for relative in path_list(row, field):
                path = ROOT / relative
                if not path.is_file():
                    fail(f"{row_id}: missing {field} path {relative}")
    runtime_sources, runtime_sinks = factory_types()
    registered_sources = set(source_types)
    registered_sinks = set(sink_types)
    missing_sources = sorted(runtime_sources - registered_sources)
    missing_sinks = sorted(runtime_sinks - registered_sinks)
    if missing_sources:
        fail(f"unregistered source_type(s): {', '.join(missing_sources)}")
    if missing_sinks:
        fail(f"unregistered sink_type(s): {', '.join(missing_sinks)}")
    return features, runtime_sources, runtime_sinks


def profile_lines(rows: list[dict], features: set[str]) -> str:
    profile_names = {"default", "core", "core-kafka", "experimental-full"}
    core: list[str] = []
    for row in rows:
        if row.get("default_build"):
            feature = row.get("cargo_feature")
            if feature and feature not in core:
                core.append(feature)
    core.sort()
    all_features = sorted(features - profile_names - INTERNAL_FEATURES)
    return "\n".join(
        [
            'default = ["core"]',
            f"core = [{', '.join(repr(feature) for feature in core)}]".replace("'", '"'),
            'core-kafka = ["core", "kafka"]',
            f"experimental-full = [{', '.join(repr(feature) for feature in all_features)}]".replace("'", '"'),
        ]
    )


def direction(row: dict) -> str:
    source = bool(row.get("source_types"))
    sink = bool(row.get("sink_types"))
    return "bidirectional" if source and sink else "source" if source else "sink" if sink else "unavailable"


def links(values: list[str], base: Path = ROOT) -> str:
    return ", ".join(
        f"[{Path(value).name}]({os.path.relpath(ROOT / value, base)})" for value in values
    ) or "—"


def render_readme(rows: list[dict]) -> str:
    counts = {maturity: sum(row.get("maturity") == maturity and row.get("kind") != "diagnostic" for row in rows) for maturity in MATURITY}
    lines = [
        "## Connector surface",
        "",
        f"The registry contains {len(rows)} selectable or documented surfaces: "
        f"{counts['supported']} supported, {counts['preview']} preview, and {counts['experimental']} experimental.",
        "Diagnostics are labeled separately and are not production integrations.",
        "",
        "| Connector | Direction | Maturity | Core | Tested versions | Owner | Evidence |",
        "|---|---|---|---:|---|---|---|",
    ]
    for row in rows:
        core = "yes" if row.get("default_build") else "no"
        matrix = "docs/src/support/connector-compatibility.md#" + row["id"]
        evidence = links(row.get("e2e_tests", []) or row.get("contract_tests", []))
        lines.append(
            f"| [{row['name']}]({matrix}) | {direction(row)} | {row['maturity']} | {core} | "
            f"{', '.join(row.get('tested_versions', []))} | {row['owner']} | {evidence} |"
        )
    return "\n".join(lines)


def render_matrix(rows: list[dict]) -> str:
    lines = [
        "# Connector compatibility",
        "",
        "This page is generated from [`connectors.toml`](../../../connectors.toml). "
        "Maturity follows the [production-supported policy](production-supported.md).",
        "",
        "| ID | Direction | Maturity | Cargo feature | Profiles | Tested versions | Owner | Docs | Evidence |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for row in rows:
        feature = row.get("cargo_feature", "built in")
        profiles = "core" if row.get("default_build") else "experimental-full" if feature else "built in"
        evidence = links(
            row.get("e2e_tests", []) + row.get("contract_tests", []),
            ROOT / "docs/src/support",
        )
        lines.append(
            f"| <a id=\"{row['id']}\"></a>{row['id']} | {direction(row)} | {row['maturity']} | "
            f"`{feature}` | {profiles} | {', '.join(row.get('tested_versions', []))} | {row['owner']} | "
            f"{links(row.get('docs', []), ROOT / 'docs/src/support')} | {evidence} |"
        )
    lines.extend(
        [
            "",
            "A missing evidence category is `not yet proved`; compiling a connector does not promote it.",
            "The normal relay build is `core`. `core-kafka` is explicit and inherits Kafka's preview maturity.",
        ]
    )
    return "\n".join(lines)


def render_checklist(rows: list[dict]) -> str:
    lines = [
        "# Connector release checklist",
        "",
        "Generated from [`connectors.toml`](../../../connectors.toml). This is a release-review artifact, not a promise that unchecked evidence exists.",
        "",
    ]
    for row in rows:
        lines.append(f"## {row['name']} (`{row['id']}`)")
        lines.append("")
        lines.append(f"- Maturity: **{row['maturity']}**")
        lines.append(f"- Owner: {row['owner']}")
        for field in EVIDENCE_FIELDS:
            paths = row.get(field, [])
            marker = "x" if paths else " "
            lines.append(
                f"- [{marker}] {field.replace('_', ' ')}: {links(paths, ROOT / 'docs/src/support')}"
            )
        lines.append("")
    return "\n".join(lines)


def write_or_check(path: Path, content: str, start: str | None = None, end: str | None = None, check: bool = False) -> None:
    if start and end:
        expected = replace_block(path, start, end, content)
    else:
        expected = content.rstrip() + "\n"
    actual = path.read_text() if path.exists() else ""
    if check:
        if actual != expected:
            fail(f"generated output is stale: {path}")
    else:
        path.write_text(expected)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    try:
        _, rows = load_registry()
        features, _, _ = validate(rows)
        write_or_check(
            ROOT / "README.md",
            render_readme(rows),
            README_START,
            README_END,
            args.check,
        )
        write_or_check(
            ROOT / "pg-tide-relay/Cargo.toml",
            profile_lines(rows, features),
            PROFILE_START,
            PROFILE_END,
            args.check,
        )
        write_or_check(
            ROOT / "docs/src/support/connector-compatibility.md",
            render_matrix(rows),
            check=args.check,
        )
        write_or_check(
            ROOT / "docs/src/support/connector-release-checklist.md",
            render_checklist(rows),
            check=args.check,
        )
    except (OSError, ValueError) as exc:
        print(f"connector check failed: {exc}", file=sys.stderr)
        return 1
    print("connector surface is current")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
