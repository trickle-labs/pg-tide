#!/usr/bin/env python3
"""Validate connectors.toml and render the small public connector surface."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

import tomllib


ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "connectors.toml"
DESCRIPTORS = ROOT / "pg-tide-relay/src/descriptors.rs"
SCHEMA_VERSION = 2
PROFILE_START = "# BEGIN GENERATED CONNECTOR PROFILES"
PROFILE_END = "# END GENERATED CONNECTOR PROFILES"
README_START = "<!-- BEGIN GENERATED CONNECTORS -->"
README_END = "<!-- END GENERATED CONNECTORS -->"
INTERNAL_FEATURES = {"test-failpoints"}
# KMS compatibility names are deliberately not part of the generated
# evaluation profile. LocalKeyFile remains available through `kms-local`;
# cloud provider names are unsupported until they have real implementations.
PROFILE_EXCLUDED_FEATURES = {"kms", "kms-aws", "kms-gcp", "kms-vault"}
MATURITY = {"supported", "preview", "experimental"}
KINDS = {"connector", "diagnostic", "compatibility"}
DICTIONS = {"source", "sink", "bidirectional", "unavailable"}
EVIDENCE_LEVELS = {"unit", "contract", "integration", "e2e", "chaos", "compatibility"}
CAPABILITY_FIELDS = {
    "max_batch_size",
    "max_batch_bytes",
    "max_message_bytes",
    "ordering",
    "ack_after",
    "deduplication",
    "retryable_errors",
    "permanent_errors",
    "tls",
    "authentication",
    "backpressure",
    "shutdown",
}
CAPABILITY_ENUMS = {
    "ordering": {"none", "per_pipeline", "per_subject", "per_partition"},
    "ack_after": {"source_poll", "destination_commit", "jetstream_ack", "broker_ack", "http_2xx"},
    "deduplication": {"none", "event_id_constraint", "stream_duplicate_window", "producer_session", "receiver_contract"},
    "tls": {"not_applicable", "optional_verify", "required_verify"},
    "backpressure": {"bounded_batch", "publish_timeout", "http_retry_after"},
    "shutdown": {"immediate", "flush", "drain"},
}
ERROR_CODES = {
    "unavailable",
    "timeout",
    "throttled",
    "authentication",
    "authorization",
    "tls_verification",
    "invalid_destination",
    "message_too_large",
    "protocol_rejection",
    "invalid_config",
    "shutdown",
    "unknown",
}
IMPLEMENTATION_PATHS = {
    "inbox": ROOT / "pg-tide-relay/src/sink/inbox.rs",
    "pg_outbox": ROOT / "pg-tide-relay/src/sink/pg_outbox.rs",
    "nats": ROOT / "pg-tide-relay/src/sink/nats.rs",
    "kafka": ROOT / "pg-tide-relay/src/sink/kafka.rs",
    "webhook": ROOT / "pg-tide-relay/src/sink/webhook.rs",
}
RUST_ERROR_CODES = {
    "Unavailable": "unavailable",
    "Timeout": "timeout",
    "Throttled": "throttled",
    "Authentication": "authentication",
    "Authorization": "authorization",
    "TlsVerification": "tls_verification",
    "InvalidDestination": "invalid_destination",
    "MessageTooLarge": "message_too_large",
    "ProtocolRejection": "protocol_rejection",
    "InvalidConfig": "invalid_config",
    "Shutdown": "shutdown",
    "Unknown": "unknown",
}
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
    if data.get("schema_version") != SCHEMA_VERSION:
        fail("connectors.toml: unsupported schema_version")
    rows = data.get("connector")
    if not isinstance(rows, list) or not rows:
        fail("connectors.toml: at least one [[connector]] row is required")
    return data, rows


def declared_direction(row: dict) -> str:
    source = bool(row.get("source_types"))
    sink = bool(row.get("sink_types"))
    inferred = "bidirectional" if source and sink else "source" if source else "sink" if sink else "unavailable"
    value = row.get("direction", inferred)
    if value not in DICTIONS:
        fail(f"{row.get('id', '<unknown>')}: invalid direction")
    if value != inferred:
        fail(f"{row.get('id', '<unknown>')}: direction does not match source_types/sink_types")
    return value


def evidence_records(row: dict) -> list[dict]:
    records = row.get("evidence", [])
    if not isinstance(records, list) or not all(isinstance(item, dict) for item in records):
        fail(f"{row.get('id', '<unknown>')}: evidence must be an array of records")
    result: list[dict] = []
    row_direction = declared_direction(row)
    workflow_jobs = workflow_job_ids()
    for record in records:
        required = ("scenario", "level", "direction", "test_path", "test_name", "ci_job")
        missing = [field for field in required if not isinstance(record.get(field), str) or not record[field]]
        if missing:
            fail(f"{row.get('id', '<unknown>')}: evidence missing {', '.join(missing)}")
        if record["level"] not in EVIDENCE_LEVELS:
            fail(f"{row.get('id', '<unknown>')}: invalid evidence level {record['level']!r}")
        if record["direction"] != row_direction:
            fail(f"{row.get('id', '<unknown>')}: evidence direction must be {row_direction}")
        path = ROOT / record["test_path"]
        if not path.is_file():
            fail(f"{row.get('id', '<unknown>')}: missing evidence test path {record['test_path']}")
        source = path.read_text()
        if not re.search(rf"\bfn\s+{re.escape(record['test_name'])}\b", source):
            fail(f"{row.get('id', '<unknown>')}: test not found: {record['test_path']}::{record['test_name']}")
        if record["ci_job"] not in workflow_jobs:
            fail(f"{row.get('id', '<unknown>')}: CI job not found: {record['ci_job']}")
        result.append(record)
    return result


def workflow_job_ids() -> set[str]:
    jobs: set[str] = set()
    for workflow in (ROOT / ".github/workflows").glob("*.yml"):
        for match in re.finditer(r"^  ([A-Za-z0-9_-]+):\s*$", workflow.read_text(), re.MULTILINE):
            jobs.add(match.group(1))
    return jobs


def validate_capabilities(row: dict) -> None:
    capabilities = row.get("capabilities")
    if not isinstance(capabilities, dict):
        fail(f"{row.get('id', '<unknown>')}: supported surface needs capabilities")
    missing = sorted(CAPABILITY_FIELDS - set(capabilities))
    if missing:
        fail(f"{row.get('id', '<unknown>')}: capabilities missing {', '.join(missing)}")
    for field in ("max_batch_size", "max_batch_bytes", "max_message_bytes"):
        value = capabilities[field]
        if not isinstance(value, int) or value <= 0:
            fail(f"{row.get('id', '<unknown>')}: capabilities.{field} must be a positive integer")
    for field, allowed in CAPABILITY_ENUMS.items():
        if capabilities[field] not in allowed:
            fail(f"{row.get('id', '<unknown>')}: invalid capabilities.{field}")
    for field in ("retryable_errors", "permanent_errors", "authentication"):
        value = capabilities[field]
        if not isinstance(value, list) or not value or not all(isinstance(item, str) and item for item in value):
            fail(f"{row.get('id', '<unknown>')}: capabilities.{field} must be a non-empty string array")
    if set(capabilities["retryable_errors"]) & set(capabilities["permanent_errors"]):
        fail(f"{row.get('id', '<unknown>')}: retryable and permanent error codes overlap")
    for field in ("retryable_errors", "permanent_errors"):
        unknown = sorted(set(capabilities[field]) - ERROR_CODES)
        if unknown:
            fail(f"{row.get('id', '<unknown>')}: unknown {field}: {', '.join(unknown)}")


def validate_implementation_error_codes(row: dict) -> None:
    declared = set(row["capabilities"]["retryable_errors"]) | set(row["capabilities"]["permanent_errors"])
    for sink_type in row.get("sink_types", []):
        path = IMPLEMENTATION_PATHS.get(sink_type)
        if path is None:
            continue
        emitted = {
            RUST_ERROR_CODES[variant]
            for variant in re.findall(r"ConnectorFailureCode::([A-Za-z]+)", path.read_text())
            if variant in RUST_ERROR_CODES
        }
        undeclared = sorted(emitted - declared)
        if undeclared:
            fail(f"{row['id']}: implementation emits undeclared error codes {', '.join(undeclared)}")


def config_fields(row: dict) -> list[str]:
    fields = row.get("config_fields", [])
    if not isinstance(fields, list) or not fields or not all(isinstance(field, str) and field for field in fields):
        fail(f"{row.get('id', '<unknown>')}: config_fields must be a non-empty string array")
    if len(fields) != len(set(fields)):
        fail(f"{row.get('id', '<unknown>')}: config_fields contains duplicates")
    return fields


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
        row_direction = declared_direction(row)
        owner = row.get("owner")
        if not isinstance(owner, str) or not re.fullmatch(r"@?[A-Za-z0-9][A-Za-z0-9_-]*", owner):
            fail(f"{row_id}: owner must be a real GitHub handle")
        if owner.lstrip("@").lower() in {"tbd", "todo", "unknown", "team", "placeholder"}:
            fail(f"{row_id}: placeholder owner")
        if row.get("maturity") == "supported" and row.get("kind") != "diagnostic":
            if not row.get("security_contact"):
                fail(f"{row_id}: supported connector needs security_contact")
            if not evidence_records(row):
                fail(f"{row_id}: supported connector needs semantic evidence")
            validate_capabilities(row)
            validate_implementation_error_codes(row)
            config_fields(row)
            versions = row.get("service_versions")
            if not isinstance(versions, dict) or not versions.get("minimum") or not versions.get("recommended"):
                fail(f"{row_id}: supported connector needs minimum and recommended service versions")
            if any(value in {"unknown", "latest", "mutable"} for value in row.get("tested_versions", [])):
                fail(f"{row_id}: supported connector cannot use unknown or mutable tested_versions")
            profiles = row.get("profiles")
            if not isinstance(profiles, list) or not profiles:
                fail(f"{row_id}: supported connector needs explicit profiles")
        elif row.get("evidence"):
            evidence_records(row)
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
    all_features = sorted(
        features - profile_names - INTERNAL_FEATURES - PROFILE_EXCLUDED_FEATURES
    )
    return "\n".join(
        [
            'default = ["core"]',
            f"core = [{', '.join(repr(feature) for feature in core)}]".replace("'", '"'),
            'core-kafka = ["core", "kafka"]',
            f"experimental-full = [{', '.join(repr(feature) for feature in all_features)}]".replace("'", '"'),
        ]
    )


def direction(row: dict) -> str:
    return declared_direction(row)


def evidence_paths(row: dict) -> list[str]:
    records = row.get("evidence", [])
    if records:
        return [record["test_path"] for record in records]
    return [path for field in EVIDENCE_FIELDS for path in path_list(row, field)]


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
        evidence = links(evidence_paths(row))
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
        profiles = ", ".join(row.get("profiles", [])) or ("core" if row.get("default_build") else "experimental-full" if feature else "built in")
        evidence = links(evidence_paths(row), ROOT / "docs/src/support")
        lines.append(
            f"| <a id=\"{row['id']}\"></a>{row['id']} | {direction(row)} | {row['maturity']} | "
            f"`{feature}` | {profiles} | {', '.join(row.get('tested_versions', []))} | {row['owner']} | "
            f"{links(row.get('docs', []), ROOT / 'docs/src/support')} | {evidence} |"
        )
    lines.extend(
        [
            "",
            "A missing evidence category is `not yet proved`; compiling a connector does not promote it.",
            "The normal relay build is `core`. Kafka production support is explicit in the tested `core-kafka` profile.",
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
        records = row.get("evidence", [])
        if records:
            for record in records:
                lines.append(
                    f"- [x] {record['scenario']} ({record['level']}, {record['direction']}): "
                    f"{record['test_path']}::{record['test_name']} in `{record['ci_job']}`"
                )
        else:
            for field in EVIDENCE_FIELDS:
                paths = row.get(field, [])
                marker = "x" if paths else " "
                lines.append(
                    f"- [{marker}] {field.replace('_', ' ')}: {links(paths, ROOT / 'docs/src/support')}"
                )
        lines.append("")
    return "\n".join(lines)


def rust_identifier(value: str) -> str:
    identifier = "".join(part.capitalize() for part in re.split(r"[^A-Za-z0-9]+", value) if part)
    if not identifier:
        identifier = "Unknown"
    if identifier[0].isdigit():
        identifier = f"Connector{identifier}"
    return identifier


def rust_string_list(values: list[str]) -> str:
    return "&[" + ", ".join(repr(value) for value in values).replace("'", '"') + "]"


def rust_error_code_list(values: list[str]) -> str:
    return "&[" + ", ".join(
        f"ConnectorFailureCode::{rust_enum(value)}" for value in values
    ) + "]"


def rust_enum(value: str) -> str:
    return rust_identifier(value)


def format_rust(content: str) -> str:
    result = subprocess.run(
        ["rustfmt", "--edition", "2021", "--emit", "stdout"],
        input=content,
        capture_output=True,
        check=True,
        text=True,
    )
    return result.stdout


def render_descriptors(rows: list[dict]) -> str:
    lines = [
        "//! Generated by scripts/generate_connector_surface.py; do not edit.",
        "",
        "use crate::error::ConnectorFailureCode;",
        "",
        "#[derive(Debug, Clone, Copy, PartialEq, Eq)]",
        "pub enum Direction { Source, Sink, Bidirectional, Unavailable }",
        "",
        "#[derive(Debug, Clone, Copy, PartialEq, Eq)]",
        "pub enum Ordering { None, PerPipeline, PerSubject, PerPartition }",
        "",
        "#[derive(Debug, Clone, Copy, PartialEq, Eq)]",
        "pub enum Acknowledgment { SourcePoll, DestinationCommit, JetstreamAck, BrokerAck, Http2xx }",
        "",
        "#[derive(Debug, Clone, Copy, PartialEq, Eq)]",
        "pub enum Deduplication { None, EventIdConstraint, StreamDuplicateWindow, ProducerSession, ReceiverContract }",
        "",
        "#[derive(Debug, Clone, Copy, PartialEq, Eq)]",
        "pub enum TlsMode { NotApplicable, OptionalVerify, RequiredVerify }",
        "",
        "#[derive(Debug, Clone, Copy, PartialEq, Eq)]",
        "pub enum Backpressure { BoundedBatch, PublishTimeout, HttpRetryAfter }",
        "",
        "#[derive(Debug, Clone, Copy, PartialEq, Eq)]",
        "pub enum Shutdown { Immediate, Flush, Drain }",
        "",
        "#[derive(Debug, Clone, Copy)]",
        "pub struct Capabilities {",
        "    pub max_batch_size: u32,",
        "    pub max_batch_bytes: u64,",
        "    pub max_message_bytes: u64,",
        "    pub ordering: Ordering,",
        "    pub acknowledgment: Acknowledgment,",
        "    pub deduplication: Deduplication,",
        "    pub retryable_errors: &'static [ConnectorFailureCode],",
        "    pub permanent_errors: &'static [ConnectorFailureCode],",
        "    pub tls: TlsMode,",
        "    pub authentication: &'static [&'static str],",
        "    pub backpressure: Backpressure,",
        "    pub shutdown: Shutdown,",
        "}",
        "",
        "#[derive(Debug, Clone, Copy)]",
        "pub struct ConnectorDescriptor {",
        "    pub id: &'static str,",
        "    pub direction: Direction,",
        "    pub source_types: &'static [&'static str],",
        "    pub sink_types: &'static [&'static str],",
        "    pub cargo_feature: Option<&'static str>,",
        "    pub maturity: &'static str,",
        "    pub config_fields: &'static [&'static str],",
        "    pub capabilities: Option<Capabilities>,",
        "}",
        "",
        "pub const CONNECTORS: &[ConnectorDescriptor] = &[",
    ]
    for row in rows:
        capabilities = row.get("capabilities")
        capability = "None"
        if capabilities:
            capability = "Some(Capabilities {\n"
            capability += f"        max_batch_size: {capabilities['max_batch_size']},\n"
            capability += f"        max_batch_bytes: {capabilities['max_batch_bytes']},\n"
            capability += f"        max_message_bytes: {capabilities['max_message_bytes']},\n"
            capability += f"        ordering: Ordering::{rust_enum(capabilities['ordering'])},\n"
            capability += f"        acknowledgment: Acknowledgment::{rust_enum(capabilities['ack_after'])},\n"
            capability += f"        deduplication: Deduplication::{rust_enum(capabilities['deduplication'])},\n"
            capability += f"        retryable_errors: {rust_error_code_list(capabilities['retryable_errors'])},\n"
            capability += f"        permanent_errors: {rust_error_code_list(capabilities['permanent_errors'])},\n"
            capability += f"        tls: TlsMode::{rust_enum(capabilities['tls'])},\n"
            capability += f"        authentication: {rust_string_list(capabilities['authentication'])},\n"
            capability += f"        backpressure: Backpressure::{rust_enum(capabilities['backpressure'])},\n"
            capability += f"        shutdown: Shutdown::{rust_enum(capabilities['shutdown'])},\n"
            capability += "    })"
        lines.extend(
            [
                "    ConnectorDescriptor {",
                f"        id: {row['id']!r},".replace("'", '"'),
                f"        direction: Direction::{rust_enum(declared_direction(row))},",
                f"        source_types: {rust_string_list(path_list(row, 'source_types'))},",
                f"        sink_types: {rust_string_list(path_list(row, 'sink_types'))},",
                f"        cargo_feature: {('Some(' + repr(row['cargo_feature']).replace(chr(39), chr(34)) + ')') if row.get('cargo_feature') else 'None'},",
                f"        maturity: {row['maturity']!r},".replace("'", '"'),
                f"        config_fields: {rust_string_list(row.get('config_fields', []))},",
                f"        capabilities: {capability},",
                "    },",
            ]
        )
    lines.extend(
        [
            "];",
            "",
            f"pub const RUNTIME_TYPES: &[&str] = {rust_string_list(sorted({runtime_type for row in rows for field in ('source_types', 'sink_types') for runtime_type in path_list(row, field)}))};",
            "",
            "pub fn sink_type_to_descriptor(sink_type: &str) -> Option<&'static ConnectorDescriptor> {",
            "    CONNECTORS.iter().find(|descriptor| descriptor.sink_types.contains(&sink_type))",
            "}",
            "",
            "pub fn source_type_to_descriptor(source_type: &str) -> Option<&'static ConnectorDescriptor> {",
            "    CONNECTORS.iter().find(|descriptor| descriptor.source_types.contains(&source_type))",
            "}",
            "",
            "pub fn is_supported_sink_type(sink_type: &str) -> bool {",
            "    sink_type_to_descriptor(sink_type).is_some_and(|descriptor| descriptor.direction == Direction::Sink && descriptor.maturity == \"supported\")",
            "}",
            "",
            "pub fn is_available(descriptor: &ConnectorDescriptor) -> bool {",
            "    match descriptor.cargo_feature {",
            "        None => true,",
        ]
    )
    for feature in sorted({row.get("cargo_feature") for row in rows if row.get("cargo_feature")}):
        lines.append(f'        Some("{feature}") => cfg!(feature = "{feature}"),')
    lines.extend(
        [
            "        Some(_) => false,",
            "    }",
            "}",
            "",
        ]
    )
    return format_rust("\n".join(lines))


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
        write_or_check(
            DESCRIPTORS,
            render_descriptors(rows),
            check=args.check,
        )
    except (OSError, ValueError) as exc:
        print(f"connector check failed: {exc}", file=sys.stderr)
        return 1
    print("connector surface is current")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
