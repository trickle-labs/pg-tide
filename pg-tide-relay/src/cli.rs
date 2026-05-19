/// CLI argument definitions for pg-tide.
use clap::{Parser, Subcommand};

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
    #[arg(long, env = "PG_TIDE_POSTGRES_URL", help = "PostgreSQL connection URL")]
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
        #[arg(long, env = "PG_TIDE_POSTGRES_URL")]
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
        #[arg(long, env = "PG_TIDE_POSTGRES_URL")]
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
        #[arg(long, env = "PG_TIDE_POSTGRES_URL")]
        postgres_url: Option<String>,
    },

    /// Print the status of all configured relay pipelines.
    ///
    /// Connects to PostgreSQL and prints a table of pipeline names,
    /// direction (forward/reverse), enabled state, last committed offset,
    /// consumer lag (outbox pipelines only), and circuit-breaker state.
    /// Exits 0 on success, 1 on connection failure.
    Status {
        /// PostgreSQL URL.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL")]
        postgres_url: Option<String>,
    },
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

        /// PostgreSQL URL.  Overrides --postgres-url.
        #[arg(long, env = "PG_TIDE_POSTGRES_URL")]
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
