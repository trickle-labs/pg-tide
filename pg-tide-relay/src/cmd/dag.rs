/// `pg-tide dag` — Pipeline dependency DAG management (v0.30.0).
///
/// Subcommands:
///   `pg-tide dag show`   — output the full pipeline dependency graph as a Mermaid diagram.
///   `pg-tide dag check`  — run `tide.relay_dag_check()` and report any cycles.
///   `pg-tide dag status` — show each edge with current upstream lag and gate state.
use pg_tide_relay::pg_tls;

use crate::cli::{Cli, DagCommands};

/// Dispatch `pg-tide dag` subcommands.
pub async fn run_dag_command(
    cmd: DagCommands,
    default_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match cmd {
        DagCommands::Show { postgres_url } => {
            let url = resolve_url(postgres_url, default_url, "dag show");
            run_dag_show(&url).await
        }
        DagCommands::Check { postgres_url } => {
            let url = resolve_url(postgres_url, default_url, "dag check");
            run_dag_check(&url).await
        }
        DagCommands::Status { postgres_url } => {
            let url = resolve_url(postgres_url, default_url, "dag status");
            run_dag_status(&url).await
        }
    }
}

fn resolve_url(postgres_url: Option<String>, default_url: &str, for_cmd: &str) -> String {
    let url = postgres_url
        .or_else(|| std::env::var("PG_TIDE_POSTGRES_URL").ok())
        .unwrap_or_else(|| default_url.to_string());
    if url.is_empty() {
        use clap::CommandFactory;
        Cli::command()
            .error(
                clap::error::ErrorKind::MissingRequiredArgument,
                format!("--postgres-url (or $PG_TIDE_POSTGRES_URL) is required for `{for_cmd}`"),
            )
            .exit();
    }
    url
}

/// `pg-tide dag show` — emit the pipeline dependency graph as a Mermaid diagram.
async fn run_dag_show(url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let (client, conn) = pg_tls::connect(url).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let rows = client
        .query(
            "SELECT upstream_pipeline, downstream_pipeline, trigger_policy \
             FROM tide.relay_pipeline_deps \
             ORDER BY upstream_pipeline, downstream_pipeline",
            &[],
        )
        .await?;

    if rows.is_empty() {
        println!("graph LR");
        println!("    %% No pipeline dependencies defined.");
        return Ok(());
    }

    println!("graph LR");
    for row in &rows {
        let up: String = row.get(0);
        let down: String = row.get(1);
        let policy: String = row.get(2);
        // Sanitise names for Mermaid node IDs (replace hyphens with underscores).
        let up_id = up.replace('-', "_");
        let down_id = down.replace('-', "_");
        println!("    {up_id}[\"{up}\"] -->|{policy}| {down_id}[\"{down}\"]");
    }

    Ok(())
}

/// `pg-tide dag check` — run cycle detection and exit 1 if a cycle is found.
pub async fn run_dag_check(url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let (client, conn) = pg_tls::connect(url).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let rows = client
        .query("SELECT cycle_path FROM tide.relay_dag_check()", &[])
        .await
        .map_err(|e| {
            // relay_dag_check() may not exist on older schemas.
            format!("relay_dag_check() failed — is the schema at v0.30.0+? ({e})")
        })?;

    if rows.is_empty() {
        println!("OK: Pipeline dependency graph is acyclic.");
        Ok(())
    } else {
        let path: Vec<String> = rows[0].get(0);
        eprintln!("ERROR: Cycle detected in pipeline dependency graph:");
        eprintln!("  {}", path.join(" → "));
        std::process::exit(1);
    }
}

/// `pg-tide dag status` — show each edge with upstream consumer lag and gate state.
async fn run_dag_status(url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let (client, conn) = pg_tls::connect(url).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let rows = client
        .query(
            "SELECT
                d.upstream_pipeline,
                d.downstream_pipeline,
                d.trigger_policy,
                COALESCE(o.last_change_id, 0) AS upstream_committed_offset,
                COALESCE(
                    (SELECT MAX(id) FROM tide.tide_outbox_messages
                     WHERE stream_table = d.upstream_pipeline), 0
                ) AS upstream_max_offset
             FROM tide.relay_pipeline_deps d
             LEFT JOIN tide.relay_consumer_offsets o
               ON o.pipeline_id = d.upstream_pipeline
              AND o.relay_group_id = 'default'
             ORDER BY d.upstream_pipeline, d.downstream_pipeline",
            &[],
        )
        .await
        .or_else(|_| {
            // Fallback query without the outbox lag column (pre-v0.30.0 schema).
            Ok::<_, tokio_postgres::Error>(vec![])
        })
        .unwrap_or_default();

    // Fallback for missing relay_pipeline_deps table (pre-v0.30.0).
    let rows = if rows.is_empty() {
        let check = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
                 WHERE table_schema = 'tide' AND table_name = 'relay_pipeline_deps')",
                &[],
            )
            .await
            .map(|r| r.get::<_, bool>(0))
            .unwrap_or(false);

        if !check {
            println!("No pipeline dependencies defined (schema pre-v0.30.0 or table absent).");
            return Ok(());
        }

        client
            .query(
                "SELECT upstream_pipeline, downstream_pipeline, trigger_policy \
                 FROM tide.relay_pipeline_deps \
                 ORDER BY upstream_pipeline, downstream_pipeline",
                &[],
            )
            .await?
    } else {
        rows
    };

    if rows.is_empty() {
        println!("No pipeline dependencies defined.");
        return Ok(());
    }

    println!(
        "{:<30} {:<30} {:<20} {:<10} {:<10} GATE",
        "UPSTREAM", "DOWNSTREAM", "POLICY", "COMMITTED", "MAX_OFFSET"
    );
    println!("{}", "-".repeat(105));

    for row in &rows {
        let up: String = row.get(0);
        let down: String = row.get(1);
        let policy: String = row.get(2);

        // Columns 3 and 4 only exist in the full query path.
        let committed: i64 = row.try_get(3).unwrap_or(0);
        let max_off: i64 = row.try_get(4).unwrap_or(0);
        let lag = max_off.saturating_sub(committed);

        let gate = match policy.as_str() {
            "on_idle" if lag > 0 => format!("GATED (lag={lag})"),
            "on_idle" => "OPEN".to_string(),
            p if p.starts_with("on_offset_gte(") => {
                let threshold: i64 = p
                    .trim_start_matches("on_offset_gte(")
                    .trim_end_matches(')')
                    .parse()
                    .unwrap_or(0);
                if committed >= threshold {
                    "OPEN".to_string()
                } else {
                    format!("GATED (committed={committed} < {threshold})")
                }
            }
            _ => "OPEN".to_string(), // "always"
        };

        println!(
            "{:<30} {:<30} {:<20} {:<10} {:<10} {}",
            up, down, policy, committed, max_off, gate
        );
    }

    Ok(())
}
