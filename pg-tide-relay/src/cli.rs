/// CLI argument definitions for pg-tide.
use clap::{Parser, Subcommand, ValueEnum};

#[derive(Debug, Clone, Copy, ValueEnum, Default)]
pub enum OutputFormat {
    #[default]
    Text,
    Json,
}

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
    about = "PostgreSQL outbox relay for pg_tide",
    long_about = "pg-tide polls pg_tide outboxes and publishes to PostgreSQL inbox, NATS JetStream, Apache Kafka, or HTTPS webhooks. Diagnostic stdout/file sinks are also available. Pipeline configuration lives in PostgreSQL."
)]
pub struct Cli {
    /// Operator output format.
    #[arg(long = "output", global = true, value_enum, default_value_t = OutputFormat::Text)]
    pub output_format: OutputFormat,
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
        env = "PG_TIDE_METRICS_ADDR",
        help = "Prometheus metrics + health endpoint (default: 0.0.0.0:9090)"
    )]
    pub metrics_addr: Option<String>,

    /// Log format.
    #[arg(
        long,
        env = "PG_TIDE_LOG_FORMAT",
        value_parser = ["text", "json"],
        help = "Log format: text or json (default: text)"
    )]
    pub log_format: Option<String>,

    /// Log level.
    #[arg(
        long,
        env = "PG_TIDE_LOG_LEVEL",
        help = "Log level: error, warn, info, debug, trace (default: info)"
    )]
    pub log_level: Option<String>,

    /// Relay group ID for advisory locks and offset namespacing.
    /// Use a unique value per relay deployment group.
    #[arg(
        long,
        env = "PG_TIDE_RELAY_GROUP_ID",
        help = "Relay group ID for advisory locks (default: default)"
    )]
    pub relay_group_id: Option<String>,

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
        value_parser = ["toml_allowed", "catalog_only"],
        help = "Pipeline config enforcement mode: toml_allowed | catalog_only (default: toml_allowed)"
    )]
    pub config_mode: Option<String>,

    /// v0.35.0: Interval in hours between automatic delivery-receipt sweep runs.
    /// The coordinator calls `tide.relay_truncate_delivery_receipts()` on this schedule.
    #[arg(
        long = "sweep-interval-hours",
        env = "PG_TIDE_SWEEP_INTERVAL_HOURS",
        hide = true,
        help = "Hours between delivery-receipt background sweep runs (default: 24)"
    )]
    pub sweep_interval_hours: Option<u64>,

    /// Optional subcommand.  When absent the relay daemon is started (legacy
    /// compatibility form; use `run`).
    #[command(subcommand)]
    pub command: Option<Commands>,
}

/// Diagnostic / operational subcommands.
#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Start the relay daemon.
    Run,

    /// Configuration operations.
    #[command(subcommand)]
    Config(ConfigCommands),

    /// Inventory catalog configuration before a v0.49.0 upgrade.
    MigrateConfig {
        /// PostgreSQL URL. Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL", value_parser = validate_postgres_url_scheme)]
        postgres_url: Option<String>,
    },

    /// Retention and catalog maintenance.
    #[command(subcommand)]
    Maintenance(MaintenanceCommands),

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

    /// Replay workbench: preview, execute, or resolve DLQ entries.
    #[command(subcommand)]
    Replay(ReplayCommands),

    /// Print the status of all configured forward relay pipelines.
    ///
    /// Connects to PostgreSQL and prints a table of pipeline names,
    /// enabled state, last committed offset, consumer lag, and
    /// circuit-breaker state.
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
}

#[derive(Debug, Subcommand)]
pub enum ConfigCommands {
    /// Validate a named pipeline.
    Validate {
        #[arg(long)]
        pipeline: String,
        #[arg(long, env = "PG_TIDE_POSTGRES_URL", value_parser = validate_postgres_url_scheme)]
        postgres_url: Option<String>,
    },
    /// Export catalog pipeline configuration.
    Export {
        #[arg(long)]
        pipeline: Option<String>,
        #[arg(long, env = "PG_TIDE_POSTGRES_URL", value_parser = validate_postgres_url_scheme)]
        postgres_url: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
pub enum MaintenanceCommands {
    /// Run a bounded retention sweep.
    Sweep {
        #[arg(long)]
        outbox: Option<String>,
        #[arg(long, default_value = "1000", value_parser = clap::value_parser!(i32).range(1..=10000))]
        batch_size: i32,
        #[arg(long, value_parser = clap::value_parser!(u32).range(1..=1000000))]
        max_batches: Option<u32>,
        #[arg(long)]
        dry_run: bool,
        #[arg(long, env = "PG_TIDE_POSTGRES_URL", value_parser = validate_postgres_url_scheme)]
        postgres_url: Option<String>,
    },
}

/// Replay and DLQ recovery subcommands.
#[derive(Debug, Subcommand)]
pub enum ReplayCommands {
    /// Execute a bounded replay through the configured pipeline.
    Execute {
        /// Pipeline name to run.
        #[arg(long)]
        pipeline: String,

        /// Start of the outbox ID range (inclusive).
        #[arg(long, value_parser = clap::value_parser!(i64).range(0..))]
        from_id: i64,

        /// End of the outbox ID range (inclusive).
        #[arg(long, value_parser = clap::value_parser!(i64).range(0..))]
        to_id: i64,

        /// Maximum messages per replay batch.
        #[arg(long, default_value = "100", value_parser = clap::value_parser!(i64).range(1..=10000))]
        batch_size: i64,

        /// PostgreSQL URL. Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL")]
        postgres_url: Option<String>,
    },

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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn canonical_command_tree_parses() {
        assert!(matches!(
            Cli::try_parse_from(["pg-tide", "run"]).unwrap().command,
            Some(Commands::Run)
        ));
        assert!(matches!(
            Cli::try_parse_from(["pg-tide", "config", "validate", "--pipeline", "orders"])
                .unwrap()
                .command,
            Some(Commands::Config(ConfigCommands::Validate { .. }))
        ));
        assert!(matches!(
            Cli::try_parse_from(["pg-tide", "config", "export"])
                .unwrap()
                .command,
            Some(Commands::Config(ConfigCommands::Export { .. }))
        ));
        assert!(matches!(
            Cli::try_parse_from(["pg-tide", "maintenance", "sweep"])
                .unwrap()
                .command,
            Some(Commands::Maintenance(MaintenanceCommands::Sweep { .. }))
        ));
    }

    #[test]
    fn output_selector_is_global() {
        let cli = Cli::try_parse_from(["pg-tide", "--output", "json", "status"]).unwrap();
        assert!(matches!(cli.output_format, OutputFormat::Json));
    }

    #[test]
    fn bounded_replay_arguments_are_validated() {
        assert!(Cli::try_parse_from([
            "pg-tide",
            "replay",
            "execute",
            "--pipeline",
            "orders",
            "--from-id",
            "0",
            "--to-id",
            "10",
            "--batch-size",
            "10000",
        ])
        .is_ok());
        assert!(Cli::try_parse_from([
            "pg-tide",
            "replay",
            "execute",
            "--pipeline",
            "orders",
            "--from-id",
            "-1",
            "--to-id",
            "10",
        ])
        .is_err());
        assert!(Cli::try_parse_from([
            "pg-tide",
            "replay",
            "execute",
            "--pipeline",
            "orders",
            "--from-id",
            "0",
            "--to-id",
            "10",
            "--batch-size",
            "10001",
        ])
        .is_err());
    }
}
