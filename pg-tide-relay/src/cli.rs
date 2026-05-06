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
}
