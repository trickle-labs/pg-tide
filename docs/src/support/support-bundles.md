# Support bundles

`pg-tide doctor --bundle <directory>` writes a small, local support directory.
The directory is created atomically: the requested path is never replaced or
left partially populated. The command fails if the target already exists.

The fixed v1 file set is:

| File | Contents |
| --- | --- |
| `manifest.json` | schema version, creation time, file sizes, SHA-256 digests, collection results, bounds, and sharing warning |
| `versions.json` | relay and installed extension versions plus compatibility class |
| `doctor.json` | schema and required-table checks |
| `status.json` | bounded pipeline health, ownership, lag, checkpoint, retry, and DLQ metadata |
| `error-codes.json` | latest stable error code metadata already exposed by status |
| `metrics-metadata.json` | stable metric names, types, units, and labels |

Each JSON file is bounded. Pipeline rows are limited to 100 and string values
to 256 UTF-8 bytes; `manifest.json` records omitted rows and truncations. The
completed directory is limited to 1 MiB. Files use restrictive permissions
where the platform supports them.

The manifest lists the other five files; it is intentionally not self-listed
because a file cannot contain its own final digest.

Bundles contain no payloads, headers, DLQ contents, pipeline configuration,
URLs, environment variables, certificates, keys, trust-store paths, raw SQL
errors, connector responses, or logs. Values from diagnostic data are
redacted before writing. The manifest uses
[`support-bundle-v1.schema.json`](../../../schemas/support-bundle-v1.schema.json).

Review the directory before sharing it. It is not uploaded, compressed, or
retained by pg-tide; use your normal archive tool only after reviewing the
contents and applying your support process.
