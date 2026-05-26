/// CLI argument definitions for pg-tide.
use clap::{Parser, Subcommand};

/// v0.27.0: Validate that a `--postgres-url` value, when provided, begins with
/// a recognised PostgreSQL URI scheme (`postgres://` or `postgresql://`).
///
/// This runs at argument-parse time so malformed URLs produce an immediate
/// `clap`-formatted diagnostic rather than a confusing runtime `RelayError::Db`.
fn validate_postgres_url_scheme(value: &str) -> Result<String, String> {
    if value.starts_with("postgres://") || value.starts_with("postgresql://") {
        Ok(value.to_string())
    } else {
        Err(format!(
            "connection URL must begin with 'postgres://' or 'postgresql://'; got '{}'",
            value
        ))
    }
}

/// v0.27.0: Validate that `--tenant-id`, when provided, is a non-empty
/// identifier that does not contain NUL bytes, double-quotes, or
/// semicolons, and does not exceed 63 bytes (PostgreSQL `NAMEDATALEN`).
///
/// This mirrors the `validate_relay_identifier()` check applied at
/// coordinator startup and closes the defence-in-depth gap where an invalid
/// tenant ID could only be detected after the PostgreSQL connection is open.
fn validate_tenant_id_str(value: &str) -> Result<String, String> {
    if value.is_empty() {
        return Err("tenant-id must not be empty".to_string());
    }
    if value.len() > 63 {
        return Err(format!(
            "tenant-id must not exceed 63 bytes (got {} bytes)",
            value.len()
        ));
    }
    if value.contains('\0') || value.contains('"') || value.contains(';') {
        return Err(format!(
            "tenant-id contains disallowed characters (NUL, '\"', or ';'): '{}'",
            value
        ));
    }
    Ok(value.to_string())
}

#[derive(Debug, Parser)]
#[command(
    name = "pg-tide",
    version,
    about = "Bidirectional relay for pg_tide outboxes and inboxes",
    long_about = "pg-tide bridges pg_tide outboxes and inboxes with external \
                  messaging systems (NATS, Kafka, HTTP webhooks, Redis, SQS, RabbitMQ, \
                  stdout/file). All pipeline configuration lives in PostgreSQL — the only \
                  required startup parameter is --postgres-url.\n\n\
                  Forward mode: polls outbox tables → publishes to external sinks.\n\
                  Reverse mode: consumes from external sources → writes to inbox tables."
)]
pub struct Cli {
    /// PostgreSQL connection string (required for relay mode; optional for diagnostics).
    /// Example: postgres://user:pass@localhost:5432/mydb
    #[arg(
        long,
        env = "PG_TIDE_POSTGRES_URL",
        help = "PostgreSQL connection URL",
        value_parser = validate_postgres_url_scheme
    )]
    pub postgres_url: Option<String>,

    /// Path to a file containing the PostgreSQL connection string.
    ///
    /// Preferred over --postgres-url for production deployments to prevent
    /// credentials appearing in `/proc/<pid>/cmdline` or shell history.
    /// The file must contain a single line with the connection URL.
    /// If both --postgres-url and --postgres-url-file are provided,
    /// --postgres-url-file takes precedence.
    #[arg(
        long,
        env = "PG_TIDE_POSTGRES_URL_FILE",
        help = "Path to a file containing the PostgreSQL connection URL"
    )]
    pub postgres_url_file: Option<String>,

    /// Prometheus metrics + health endpoint address.
    #[arg(
        long,
        default_value = "0.0.0.0:9090",
        env = "PG_TIDE_METRICS_ADDR",
        help = "Prometheus metrics + health endpoint (default: 0.0.0.0:9090)"
    )]
    pub metrics_addr: String,

    /// Log format.
    #[arg(
        long,
        default_value = "text",
        env = "PG_TIDE_LOG_FORMAT",
        help = "Log format: text or json (default: text)"
    )]
    pub log_format: String,

    /// Log level.
    #[arg(
        long,
        default_value = "info",
        env = "PG_TIDE_LOG_LEVEL",
        help = "Log level: error, warn, info, debug, trace (default: info)"
    )]
    pub log_level: String,

    /// Relay group ID for advisory locks and offset namespacing.
    /// Use a unique value per relay deployment group.
    #[arg(
        long,
        default_value = "default",
        env = "PG_TIDE_RELAY_GROUP_ID",
        help = "Relay group ID for advisory locks (default: default)"
    )]
    pub relay_group_id: String,

    /// Optional TOML config file path.
    #[arg(
        long,
        env = "PG_TIDE_CONFIG",
        help = "Path to TOML config file (optional)"
    )]
    pub config: Option<String>,

    /// Maximum time in seconds to wait for in-flight messages to drain on shutdown.
    ///
    /// When a SIGTERM is received the relay stops accepting new work and waits
    /// up to this many seconds for active pipelines to finish their current
    /// batch before exiting. Set to 0 to exit immediately.
    #[arg(
        long,
        default_value = "30",
        env = "PG_TIDE_DRAIN_TIMEOUT",
        help = "Seconds to wait for in-flight messages to drain on SIGTERM (default: 30)"
    )]
    pub drain_timeout: u64,

    /// Maximum number of pipeline workers to own concurrently.
    ///
    /// Each pipeline worker holds one PostgreSQL connection.  Use this to
    /// limit connection exhaustion on managed databases (e.g. RDS, Cloud SQL)
    /// with low connection limits.
    #[arg(
        long = "max-pipelines",
        env = "PG_TIDE_MAX_PIPELINES",
        help = "Maximum number of concurrent pipeline workers (default: 50)"
    )]
    pub max_pipelines: Option<usize>,

    /// Maximum number of connections in the coordinator connection pool.
    ///
    /// Controls the `deadpool-postgres` pool size used for coordinator metadata
    /// operations (pipeline discovery, advisory lock management).
    #[arg(
        long = "max-connections",
        env = "PG_TIDE_MAX_CONNECTIONS",
        help = "Maximum coordinator pool connections (default: 52)"
    )]
    pub max_connections: Option<usize>,

    /// Tenant ID for multi-tenant relay groups.
    ///
    /// When set, the coordinator filters pipeline discovery to only own
    /// pipelines belonging to this tenant (`tenant_name = $TENANT_ID` in the
    /// catalog).  Advisory lock keys incorporate the tenant hash, preventing
    /// cross-tenant pipeline collisions on shared databases.
    /// Injected as the `tenant` label on all Prometheus metrics.
    #[arg(
        long = "tenant-id",
        env = "PG_TIDE_TENANT_ID",
        help = "Tenant ID for multi-tenant relay groups (default: no filtering)",
        value_parser = validate_tenant_id_str
    )]
    pub tenant_id: Option<String>,

    /// Run a self-test and exit.
    ///
    /// Connects to PostgreSQL, verifies the extension version, checks TLS
    /// state, acquires and immediately releases an advisory lock, queries
    /// `tide.relay_outbox_config`, then exits 0 on success or 1 with a
    /// descriptive error on failure.  Designed for use in Kubernetes
    /// initContainers, container health checks, and CI/CD pre-deployment gates.
    #[arg(
        long = "self-test",
        env = "PG_TIDE_SELF_TEST",
        help = "Run startup self-test and exit (0=pass, 1=fail)"
    )]
    pub self_test: bool,

    /// v0.33.0: Minimum pg_tide extension version required by the relay binary.
    ///
    /// When `--self-test` is active and this flag is provided, the self-test
    /// fails with exit code 1 if the installed pg_tide extension version does
    /// not meet the minimum (e.g. `--expect-extension-version 0.33.0` for the
    /// v0.33.0 relay binary, or `--expect-extension-version 1.0.0` for the
    /// v1.0.0 relay binary).  Designed for Kubernetes `initContainers` that
    /// should block relay startup on an incompatible extension version.
    #[arg(
        long = "expect-extension-version",
        env = "PG_TIDE_EXPECT_EXTENSION_VERSION",
        help = "Minimum pg_tide extension version required (for --self-test)"
    )]
    pub expect_extension_version: Option<String>,

    /// v0.28.0: Configuration mode for pipeline discovery.
    ///
    /// `toml_allowed` (default): TOML-defined pipelines without a matching catalog
    /// row emit a warning but do not prevent startup.
    /// `catalog_only`: Any TOML [[pipeline]] block that has no matching row in
    /// tide.tide_outbox_config or tide.tide_inbox_config causes startup to fail.
    #[arg(
        long = "config-mode",
        env = "PG_TIDE_CONFIG_MODE",
        default_value = "toml_allowed",
        help = "Pipeline config enforcement mode: toml_allowed | catalog_only (default: toml_allowed)"
    )]
    pub config_mode: String,

    /// v0.35.0: Interval in hours between automatic delivery-receipt sweep runs.
    /// The coordinator calls `tide.relay_truncate_delivery_receipts()` on this schedule.
    #[arg(
        long = "sweep-interval-hours",
        env = "PG_TIDE_SWEEP_INTERVAL_HOURS",
        default_value = "24",
        hide = true,
        help = "Hours between delivery-receipt background sweep runs (default: 24)"
    )]
    pub sweep_interval_hours: u64,

    /// Optional subcommand.  When absent the relay daemon is started.
    #[command(subcommand)]
    pub command: Option<Commands>,
}

/// Diagnostic / operational subcommands.
#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Validate PostgreSQL connectivity, schema version, and catalog health.
    ///
    /// Connects to PostgreSQL, checks that the tide schema and required tables
    /// exist, verifies the schema version recorded in the catalog, and reports
    /// the number of configured pipelines.  Exits 0 on success, 1 on any
    /// problem.
    Doctor {
        /// PostgreSQL URL to diagnose.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL", value_parser = validate_postgres_url_scheme)]
        postgres_url: Option<String>,
    },

    /// Dry-run a pipeline's source and sink factories against the catalog config.
    ///
    /// Loads the pipeline configuration from PostgreSQL, resolves secrets,
    /// constructs the source and sink (without processing any messages), then
    /// reports whether both sides can be instantiated successfully.  Useful for
    /// validating configuration before deploying a new pipeline.
    ValidateConfig {
        /// Name of the pipeline to validate.
        #[arg(long)]
        pipeline: String,

        /// PostgreSQL URL.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL", value_parser = validate_postgres_url_scheme)]
        postgres_url: Option<String>,
    },

    /// Replay workbench: preview, execute, or resolve DLQ entries.
    #[command(subcommand)]
    Replay(ReplayCommands),

    /// AsyncAPI document generation.
    #[command(subcommand)]
    Asyncapi(AsyncapiCommands),

    /// DuckLake lake management and inspection commands.
    #[command(subcommand)]
    Ducklake(DucklakeCommands),

    /// Delete consumed outbox messages that are past their retention window.
    ///
    /// Calls `tide.outbox_truncate_delivered()` for each configured outbox
    /// (or a specific outbox when `--outbox` is given).  Run this on a
    /// schedule to prevent unbounded growth of the outbox message table.
    Sweep {
        /// Outbox name to sweep.  When omitted all outboxes are swept.
        #[arg(long)]
        outbox: Option<String>,

        /// PostgreSQL URL.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL", value_parser = validate_postgres_url_scheme)]
        postgres_url: Option<String>,
    },

    /// Print the status of all configured relay pipelines.
    ///
    /// Connects to PostgreSQL and prints a table of pipeline names,
    /// direction (forward/reverse), enabled state, last committed offset,
    /// consumer lag (outbox pipelines only), and circuit-breaker state.
    /// Exits 0 on success, 1 on connection failure.
    Status {
        /// v0.33.0: Also print an inbox fleet summary via `tide.inbox_status(NULL)`.
        ///
        /// NOTE: This call scales with the number of configured inboxes (O(n)).
        /// Use on dashboards and monitoring scripts; avoid in tight polling loops.
        /// Without this flag the inbox summary is omitted to keep default output fast.
        #[arg(long = "inbox-summary", default_value = "false")]
        inbox_summary: bool,

        /// PostgreSQL URL.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL", value_parser = validate_postgres_url_scheme)]
        postgres_url: Option<String>,
    },

    /// Emit SQL statements to migrate a TOML-centric pipeline config to catalog.
    ///
    /// Reads the active TOML config file (specified via --config) and prints
    /// the equivalent `SELECT tide.relay_set_outbox_v2(...)` / `relay_set_inbox_v2(...)`
    /// SQL statements to stdout.  Does not write to the database — pipe the
    /// output into psql to apply the migration.
    MigrateConfig {
        /// PostgreSQL URL (used to look up existing catalog entries for comparison).
        #[arg(long, env = "PG_TIDE_POSTGRES_URL", value_parser = validate_postgres_url_scheme)]
        postgres_url: Option<String>,
    },

    /// Pipeline template management (v0.29.0).
    ///
    /// `pg-tide template list` — list all available templates with descriptions.
    /// `pg-tide template show <name>` — print the full config JSON for a template.
    /// `pg-tide template apply <name> --outbox <outbox> --set key=value ...` — instantiate.
    #[command(subcommand)]
    Template(TemplateCommands),

    /// Show the config change history for a pipeline (v0.29.0).
    ///
    /// Queries `tide.relay_config_history()` and prints a timestamped table
    /// of config changes with a compact diff.
    ///
    /// Exit codes: 0 = success, 1 = connection error.
    History {
        /// Pipeline name to show history for.
        pipeline: String,

        /// Maximum number of history entries to return.
        #[arg(long, default_value = "20")]
        limit: i64,

        /// Show only changes at or after this timestamp (ISO 8601).
        #[arg(long)]
        since: Option<String>,

        /// Output format: table (default) or json.
        ///
        /// Use `--output json` to get machine-readable output for CI scripts.
        #[arg(long, default_value = "table", value_parser = ["table", "json"])]
        output: String,

        /// PostgreSQL URL.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL", value_parser = validate_postgres_url_scheme)]
        postgres_url: Option<String>,
    },

    /// Managed backfill job operations (v0.29.0).
    ///
    /// `pg-tide backfill pause|resume|cancel <job-name>` — lifecycle control.
    /// `pg-tide backfill status [<job-name>]` — show progress.
    #[command(subcommand)]
    Backfill(BackfillCommands),

    /// Pipeline dependency DAG management (v0.30.0).
    ///
    /// `pg-tide dag show`   — output the pipeline dependency graph as a Mermaid diagram.
    /// `pg-tide dag check`  — run cycle detection and exit 1 if a cycle is found.
    /// `pg-tide dag status` — show each edge with upstream lag and gate state.
    #[command(subcommand)]
    Dag(DagCommands),
}

/// Replay workbench subcommands.
#[derive(Debug, Subcommand)]
pub enum ReplayCommands {
    /// Preview messages in an outbox ID range without consuming them.
    ///
    /// Prints the matching outbox messages as JSONL to stdout.
    /// No offsets are advanced; this is a read-only operation.
    Preview {
        /// Outbox name to preview.
        #[arg(long)]
        outbox: String,

        /// Start of the ID range (inclusive).
        #[arg(long, default_value = "0")]
        from_id: i64,

        /// End of the ID range (inclusive).
        #[arg(long, default_value = "9223372036854775807")]
        to_id: i64,

        /// Maximum number of messages to return.
        #[arg(long, default_value = "100")]
        limit: i32,

        /// PostgreSQL URL.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL")]
        postgres_url: Option<String>,
    },

    /// Dry-run a transform evaluation against a sampled set of outbox messages.
    ///
    /// Reads messages from the outbox, applies all configured transforms, and
    /// prints the resulting envelopes to stdout without publishing them.
    DryRun {
        /// Pipeline name whose transforms should be evaluated.
        #[arg(long)]
        pipeline: String,

        /// Start of the ID range (inclusive).
        #[arg(long, default_value = "0")]
        from_id: i64,

        /// End of the ID range (inclusive).
        #[arg(long, default_value = "9223372036854775807")]
        to_id: i64,

        /// Maximum number of messages to evaluate.
        #[arg(long, default_value = "20")]
        limit: i32,

        /// PostgreSQL URL.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL")]
        postgres_url: Option<String>,
    },

    /// Mark a DLQ entry as resolved (closed without requeue).
    DlqResolve {
        /// Pipeline name.
        #[arg(long)]
        pipeline: String,

        /// Dedup key of the DLQ entry to resolve.
        #[arg(long)]
        dedup_key: String,

        /// PostgreSQL URL.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL")]
        postgres_url: Option<String>,
    },

    /// Requeue a DLQ entry for another relay attempt.
    DlqRequeue {
        /// Pipeline name.
        #[arg(long)]
        pipeline: String,

        /// Dedup key of the DLQ entry to requeue.
        #[arg(long)]
        dedup_key: String,

        /// PostgreSQL URL.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL")]
        postgres_url: Option<String>,
    },
}

/// AsyncAPI document generation subcommands.
#[derive(Debug, Subcommand)]
pub enum AsyncapiCommands {
    /// Generate an AsyncAPI 3.0 document from relay catalog metadata.
    ///
    /// Reads all configured relay pipelines from PostgreSQL and emits an
    /// AsyncAPI 3.0 YAML or JSON document describing the channels, messages,
    /// schemas, and bindings.  Useful for API documentation and contract-first
    /// development.
    Export {
        /// Output format: yaml or json.
        #[arg(long, default_value = "yaml")]
        format: String,

        /// Output file path. Defaults to stdout when omitted.
        #[arg(long)]
        output: Option<String>,

        /// v0.27.0: Sample up to 10 recent messages per outbox and include
        /// observed payload field names as AsyncAPI schema properties.
        #[arg(long, default_value = "false")]
        full_schema: bool,

        /// PostgreSQL URL.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL", value_parser = validate_postgres_url_scheme)]
        postgres_url: Option<String>,
    },

    /// v0.27.0: Validate live relay catalog against an external AsyncAPI spec.
    ///
    /// Fetches the spec from the given URL, compares channels to the live
    /// relay catalog, and reports pipelines that are undocumented or missing.
    Validate {
        /// URL of the AsyncAPI spec to validate against.
        #[arg(long)]
        spec_url: String,

        /// PostgreSQL URL.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL", value_parser = validate_postgres_url_scheme)]
        postgres_url: Option<String>,
    },
}

/// DuckLake lake management and inspection subcommands (v0.22.0).
#[derive(Debug, Subcommand)]
pub enum DucklakeCommands {
    /// List all DuckLake snapshots for a pipeline with timestamps and record counts.
    ///
    /// Queries `ducklake_snapshot` and `ducklake_data_file` for the pipeline's
    /// DuckLake table and prints a human-readable summary of each snapshot:
    /// snapshot ID, created_at timestamp, record count, and Parquet file paths.
    Snapshots {
        /// Pipeline name to inspect.
        #[arg(long)]
        pipeline: String,

        /// Maximum number of snapshots to show (default: 50).
        #[arg(long, default_value = "50")]
        limit: i64,

        /// PostgreSQL URL.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL")]
        postgres_url: Option<String>,
    },

    /// Trigger a full DuckLake checkpoint for a pipeline.
    ///
    /// Flushes all inlined data to Parquet, merges small Parquet files, and
    /// expires snapshots beyond the configured retention window.  Safe to run
    /// at any time; the relay continues processing during the checkpoint.
    Checkpoint {
        /// Pipeline name to checkpoint.
        #[arg(long)]
        pipeline: String,

        /// PostgreSQL URL.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL")]
        postgres_url: Option<String>,
    },

    /// Flush inlined DuckLake data to Parquet without full compaction.
    ///
    /// For each row currently stored in `ducklake_inlined_data_*` tables,
    /// materialises the data into a Parquet file on object storage, registers
    /// a new `ducklake_data_file` entry, and clears the inlined rows.
    /// Lighter than a full `checkpoint`; suitable for low-latency archival
    /// maintenance windows.
    FlushInlined {
        /// Pipeline name whose inlined data to flush.
        #[arg(long)]
        pipeline: String,

        /// PostgreSQL URL.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL")]
        postgres_url: Option<String>,
    },

    /// Print the consumer-offset-to-snapshot-ID mapping table for a pipeline.
    ///
    /// Shows the `tide.ducklake_offset_map` entries for the given pipeline in
    /// human-readable form.  Useful for debugging time-travel replay scenarios.
    OffsetMap {
        /// Pipeline name to inspect.
        #[arg(long)]
        pipeline: String,

        /// Maximum number of rows to show (default: 100).
        #[arg(long, default_value = "100")]
        limit: i64,

        /// PostgreSQL URL.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL")]
        postgres_url: Option<String>,
    },
}

/// Pipeline template management subcommands (v0.29.0).
#[derive(Debug, Subcommand)]
pub enum TemplateCommands {
    /// List all pipeline templates with descriptions and required keys.
    List {
        /// PostgreSQL URL.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL", value_parser = validate_postgres_url_scheme)]
        postgres_url: Option<String>,
    },

    /// Print the full config JSON for a named template.
    Show {
        /// Template name.
        name: String,

        /// PostgreSQL URL.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL", value_parser = validate_postgres_url_scheme)]
        postgres_url: Option<String>,
    },

    /// Instantiate a template as an outbox pipeline and upsert it into the catalog.
    ///
    /// Merges the template config with `--set key=value` overrides and calls
    /// `tide.relay_set_outbox_from_template()`.
    Apply {
        /// Template name.
        name: String,

        /// Outbox name to bind the pipeline to.
        #[arg(long)]
        outbox: String,

        /// Key=value overrides for template placeholders. Repeat for multiple values.
        #[arg(long = "set", value_name = "KEY=VALUE")]
        set: Vec<String>,

        /// PostgreSQL URL.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL", value_parser = validate_postgres_url_scheme)]
        postgres_url: Option<String>,
    },
}

/// Managed backfill job subcommands (v0.29.0).
#[derive(Debug, Subcommand)]
pub enum BackfillCommands {
    /// Pause a running or pending backfill job.
    Pause {
        /// Job name.
        job_name: String,

        /// PostgreSQL URL.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL", value_parser = validate_postgres_url_scheme)]
        postgres_url: Option<String>,
    },

    /// Resume a paused backfill job.
    Resume {
        /// Job name.
        job_name: String,

        /// PostgreSQL URL.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL", value_parser = validate_postgres_url_scheme)]
        postgres_url: Option<String>,
    },

    /// Cancel a backfill job (cannot be undone).
    Cancel {
        /// Job name.
        job_name: String,

        /// PostgreSQL URL.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL", value_parser = validate_postgres_url_scheme)]
        postgres_url: Option<String>,
    },

    /// Show progress for all backfill jobs, or a specific job.
    Status {
        /// Job name (optional; when omitted shows all jobs).
        job_name: Option<String>,

        /// PostgreSQL URL.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL", value_parser = validate_postgres_url_scheme)]
        postgres_url: Option<String>,
    },
}

/// Pipeline dependency DAG subcommands (v0.30.0).
#[derive(Debug, Subcommand)]
pub enum DagCommands {
    /// Output the full pipeline dependency graph as a Mermaid diagram.
    ///
    /// Queries `tide.relay_pipeline_deps` and emits a Mermaid `graph LR`
    /// block to stdout.  Pipe the output into a Mermaid renderer or paste it
    /// into a markdown fence to visualise the DAG.
    Show {
        /// PostgreSQL URL.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL", value_parser = validate_postgres_url_scheme)]
        postgres_url: Option<String>,

        /// Output format: mermaid (default) or json.
        ///
        /// `mermaid` emits a `graph LR` block suitable for Mermaid renderers.
        /// `json` emits a JSON adjacency list `{"nodes":[...],"edges":[...]}` for
        /// programmatic consumption.
        #[arg(long, default_value = "mermaid", value_parser = ["mermaid", "json"])]
        format: String,
    },

    /// Run DAG cycle detection and exit 1 if a cycle is found.
    ///
    /// Executes `tide.relay_dag_check()` and prints the cycle path when one
    /// is detected.  Exits 0 when the graph is acyclic.  Safe to run in CI
    /// or Kubernetes `initContainers` alongside `--self-test`.
    Check {
        /// PostgreSQL URL.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL", value_parser = validate_postgres_url_scheme)]
        postgres_url: Option<String>,
    },

    /// Show each DAG edge with current upstream lag and gate state.
    ///
    /// Queries `tide.relay_pipeline_deps` and `tide.relay_consumer_offsets`
    /// to compute upstream consumer lag for every edge, then reports whether
    /// the downstream pipeline is gated or free to run.
    Status {
        /// PostgreSQL URL.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL", value_parser = validate_postgres_url_scheme)]
        postgres_url: Option<String>,
    },
}
