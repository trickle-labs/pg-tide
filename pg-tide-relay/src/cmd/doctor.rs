/// `pg-tide doctor` — PostgreSQL connectivity and catalog health check.
use crate::cli::OutputFormat;
use pg_tide_relay::pg_tls;

/// Validate PostgreSQL connectivity, schema presence, and catalog health.
pub async fn run_doctor(
    url: &str,
    output_format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    if matches!(output_format, OutputFormat::Json) {
        let result = run_doctor_json(url).await;
        if let Err(error) = &result {
            crate::cmd::output::failure(
                "doctor",
                crate::cmd::diagnostic::from_error("postgres.catalog", error),
                output_format,
            )?;
        }
        return result;
    }
    run_doctor_with_threshold(url, 100).await
}

async fn run_doctor_json(url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let (client, conn) = match pg_tls::connect(url).await {
        Ok(connection) => connection,
        Err(error) => return Err(format!("connection failed: {error}").into()),
    };
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let extension_version = client
        .query_opt(
            "SELECT extversion::text FROM pg_extension WHERE extname = 'pg_tide'",
            &[],
        )
        .await?
        .map(|row| row.try_get::<_, String>(0))
        .transpose()?;
    let schema_exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'tide')",
            &[],
        )
        .await?
        .try_get(0)?;
    let required_tables = [
        "tide_outbox_config",
        "tide_outbox_messages",
        "tide_inbox_config",
        "relay_outbox_config",
        "relay_inbox_config",
        "relay_consumer_offsets",
        "relay_runtime_status",
    ];
    let mut table_checks = Vec::with_capacity(required_tables.len());
    let mut checks_ok = extension_version.is_some() && schema_exists;
    for table in required_tables {
        let exists: bool = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables
                 WHERE table_schema = 'tide' AND table_name = $1)",
                &[&table],
            )
            .await?
            .try_get(0)?;
        checks_ok &= exists;
        table_checks.push(serde_json::json!({
            "component": format!("postgres.{}.{}", "tide", table),
            "status": if exists { "pass" } else { "fail" },
        }));
    }

    let rows = client
        .query(
            "SELECT name::text, 'forward'::text AS direction, enabled, config,
                    COALESCE(tenant_name, 'default') AS tenant_name
               FROM tide.relay_outbox_config
             UNION ALL
             SELECT name::text, 'reverse'::text, enabled, config,
                    COALESCE(tenant_name, 'default')
               FROM tide.relay_inbox_config
             ORDER BY name::text, direction",
            &[],
        )
        .await?;
    let mut pipelines = Vec::with_capacity(rows.len());
    for row in rows {
        let direction: String = row.try_get("direction")?;
        pipelines.push(pg_tide_relay::config::PipelineConfig {
            name: row.try_get("name")?,
            direction: if direction == "forward" {
                pg_tide_relay::config::PipelineDirection::Forward
            } else {
                pg_tide_relay::config::PipelineDirection::Reverse
            },
            enabled: row.try_get("enabled")?,
            config: row.try_get("config")?,
            tenant_name: row.try_get("tenant_name")?,
        });
    }
    let preflight = pg_tide_relay::config::preflight::startup_preflight(&pipelines);
    checks_ok &= preflight.is_valid();
    let data = serde_json::json!({
        "extension_version": extension_version,
        "schema": {"status": if schema_exists { "pass" } else { "fail" }},
        "tables": table_checks,
        "pipelines": {
            "count": pipelines.len(),
            "preflight_issues": preflight.issues.iter().map(|issue| serde_json::json!({
                "pipeline": issue.pipeline,
                "severity": format!("{:?}", issue.severity).to_lowercase(),
                "reason": issue.reason,
            })).collect::<Vec<_>>(),
        },
    });
    if checks_ok {
        crate::cmd::output::success("doctor", data, OutputFormat::Json)?;
        Ok(())
    } else {
        Err("doctor found one or more failed checks".into())
    }
}

/// Run the doctor checks with a configurable DLQ warn threshold.
pub async fn run_doctor_with_threshold(
    url: &str,
    dlq_warn_threshold: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("pg-tide doctor v{}", env!("CARGO_PKG_VERSION"));
    println!("Connecting to PostgreSQL...");

    // v0.15.0: Use pg_tls::connect (honours sslmode from URL).
    let (client, conn) = pg_tls::connect(url)
        .await
        .map_err(|_| "connection failed: verify PostgreSQL reachability and TLS settings")?;

    tokio::spawn(async move {
        let _ = conn.await;
    });

    println!("  [OK] Connected to PostgreSQL");

    let extension_version = client
        .query_opt(
            "SELECT extversion::text FROM pg_extension WHERE extname = 'pg_tide'",
            &[],
        )
        .await
        .map_err(|_| "extension version check failed")?
        .and_then(|row| row.try_get::<_, String>(0).ok());
    match extension_version {
        Some(version) => println!("  [OK] pg_tide extension {version} installed"),
        None => {
            println!("  [FAIL] pg_tide extension is not installed");
            return Err("doctor found one or more failed checks".into());
        }
    }

    // v0.25.0: TLS version check — query pg_ssl for negotiated TLS version.
    let tls_row = client
        .query_opt(
            "SELECT ssl, version FROM pg_ssl WHERE pid = pg_backend_pid()",
            &[],
        )
        .await
        .ok()
        .flatten();
    if let Some(row) = tls_row {
        let ssl: bool = row.get(0);
        if ssl {
            let tls_version: String = row.get(1);
            if tls_version.contains("TLSv1.1") || tls_version.contains("TLSv1.0") {
                println!(
                    "  [WARN] TLS version {tls_version} is outdated — upgrade server to TLS 1.2+"
                );
            } else {
                println!("  [OK] TLS connection: {tls_version}");
            }
        } else {
            // Check if the URL requested TLS but we got plaintext.
            if url.contains("sslmode=require")
                || url.contains("sslmode=verify-ca")
                || url.contains("sslmode=verify-full")
            {
                println!(
                    "  [WARN] sslmode=require/verify-* requested but connection is plaintext \
                     — native-tls feature may not be compiled in"
                );
            } else {
                println!("  [INFO] Connection is plaintext (no sslmode=require)");
            }
        }
    }

    // Check schema exists.
    let schema_exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'tide')",
            &[],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or(false);

    if schema_exists {
        println!("  [OK] Schema 'tide' exists");
    } else {
        println!("  [FAIL] Schema 'tide' not found — is pg_tide installed?");
    }

    // Check required tables.
    let required_tables = [
        "tide_outbox_config",
        "tide_outbox_messages",
        "tide_inbox_config",
        "relay_outbox_config",
        "relay_inbox_config",
        "relay_consumer_offsets",
    ];
    let mut all_ok = schema_exists;
    for table in &required_tables {
        let exists: bool = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
                 WHERE table_schema = 'tide' AND table_name = $1)",
                &[table],
            )
            .await
            .map(|r| r.get(0))
            .unwrap_or(false);
        if exists {
            println!("  [OK] Table tide.{table}");
        } else {
            println!("  [FAIL] Table tide.{table} missing");
            all_ok = false;
        }
    }

    // Check relay_consumer_offsets has the correct schema (v0.12.0 migration).
    let has_change_id: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
             WHERE table_schema = 'tide' AND table_name = 'relay_consumer_offsets' \
             AND column_name = 'last_change_id')",
            &[],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or(false);
    if has_change_id {
        println!("  [OK] relay_consumer_offsets.last_change_id column present");
    } else {
        println!("  [WARN] relay_consumer_offsets.last_change_id missing — run upgrade to v0.12.0");
        all_ok = false;
    }

    // Count configured pipelines.
    let outbox_count: i64 = client
        .query_one("SELECT COUNT(*) FROM tide.relay_outbox_config", &[])
        .await
        .map(|r| r.get(0))
        .unwrap_or(0);
    let inbox_count: i64 = client
        .query_one("SELECT COUNT(*) FROM tide.relay_inbox_config", &[])
        .await
        .map(|r| r.get(0))
        .unwrap_or(0);
    println!(
        "  [INFO] {outbox_count} forward pipeline(s), {inbox_count} reverse pipeline(s) configured"
    );

    // v0.15.0: Check for tide.outbox_truncate_delivered() (v0.15.0+).
    let has_sweep_fn: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.routines \
             WHERE routine_schema = 'tide' AND routine_name = 'outbox_truncate_delivered')",
            &[],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or(false);
    if has_sweep_fn {
        println!("  [OK] tide.outbox_truncate_delivered() present (v0.15.0+)");
    } else {
        println!("  [WARN] tide.outbox_truncate_delivered() missing — upgrade to v0.15.0");
    }

    // v0.43.0: Retention state and shared-parent layout checks.
    for table in ["outbox_cleanup_state", "outbox_storage_config"] {
        let exists: bool = client
            .query_one(
                "SELECT to_regclass($1) IS NOT NULL",
                &[&format!("tide.\"{table}\"")],
            )
            .await
            .map(|row| row.get(0))
            .unwrap_or(false);
        if exists {
            println!("  [OK] Table tide.{table}");
        } else {
            println!("  [FAIL] Table tide.{table} missing — upgrade to v0.43.0");
            all_ok = false;
        }
    }

    for view in ["outbox_retention_status", "relay_pipeline_lag"] {
        let exists: bool = client
            .query_one(
                "SELECT to_regclass($1) IS NOT NULL",
                &[&format!("tide.\"{view}\"")],
            )
            .await
            .map(|row| row.get(0))
            .unwrap_or(false);
        if exists {
            println!("  [OK] View tide.{view}");
        } else {
            println!("  [FAIL] View tide.{view} missing — upgrade to v0.43.0");
            all_ok = false;
        }
    }

    let has_sweep_v43: bool = client
        .query_one(
            "SELECT to_regprocedure('tide.outbox_sweep(text,integer,boolean)') IS NOT NULL",
            &[],
        )
        .await
        .map(|row| row.get(0))
        .unwrap_or(false);
    if has_sweep_v43 {
        println!("  [OK] tide.outbox_sweep(text,integer,boolean) present");
    } else {
        println!("  [FAIL] tide.outbox_sweep(text,integer,boolean) missing — upgrade to v0.43.0");
        all_ok = false;
    }

    let has_partition_maintenance: bool = client
        .query_one(
            "SELECT to_regprocedure('tide.outbox_maintain_partitions(integer,boolean)') IS NOT NULL",
            &[],
        )
        .await
        .map(|row| row.get(0))
        .unwrap_or(false);
    if has_partition_maintenance {
        println!("  [OK] tide.outbox_maintain_partitions(integer,boolean) present");
    } else {
        println!(
            "  [WARN] partition maintenance helper missing — ID-range maintenance unavailable"
        );
    }

    if table_exists(&client, "outbox_cleanup_state").await {
        let stale: i64 = client
            .query_one(
                "SELECT COUNT(*)::bigint
                   FROM tide.outbox_cleanup_state
                  WHERE last_success_at IS NULL
                    OR last_success_at < now() - interval '24 hours'",
                &[],
            )
            .await
            .map(|row| row.get(0))
            .unwrap_or(0);
        if stale == 0 {
            println!("  [OK] Cleanup state is current");
        } else {
            println!(
                "  [WARN] {stale} outbox cleanup state row(s) are stale — run 'pg-tide sweep'"
            );
        }
    }

    let default_rows: i64 = client
        .query_one(
            "SELECT COALESCE(SUM(c.reltuples::bigint), 0)
               FROM pg_class c
               JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = 'tide' AND c.relname LIKE '%default%'",
            &[],
        )
        .await
        .map(|row| row.get(0))
        .unwrap_or(0);
    if default_rows > 0 {
        println!(
            "  [WARN] approximately {default_rows} row(s) in default-named partition(s) — \
             run 'pg-tide sweep'"
        );
    } else {
        println!("  [OK] No rows reported in default-named partitions");
    }

    let cleanup_index_count: i64 = client
        .query_one(
            "SELECT COUNT(*)::bigint
               FROM pg_indexes
              WHERE schemaname = 'tide'
                AND tablename = 'tide_outbox_messages'
                AND indexdef ILIKE '%outbox_name%'
                AND indexdef ILIKE '%id%'",
            &[],
        )
        .await
        .map(|row| row.get(0))
        .unwrap_or(0);
    if cleanup_index_count > 0 {
        println!("  [OK] Native outbox polling/cleanup index present");
    } else {
        println!("  [FAIL] Native outbox polling/cleanup index missing");
        all_ok = false;
    }

    let storage_layout = client
        .query_opt(
            "SELECT storage_layout::text
               FROM tide.outbox_storage_config
              LIMIT 1",
            &[],
        )
        .await
        .ok()
        .flatten()
        .and_then(|row| row.try_get::<_, String>(0).ok());
    let is_partitioned: bool = client
        .query_one(
            "SELECT EXISTS(
                 SELECT 1
                   FROM pg_partitioned_table p
                   JOIN pg_class c ON c.oid = p.partrelid
                   JOIN pg_namespace n ON n.oid = c.relnamespace
                  WHERE n.nspname = 'tide'
                    AND c.relname = 'tide_outbox_messages'
             )",
            &[],
        )
        .await
        .map(|row| row.get(0))
        .unwrap_or(false);
    if let Some(layout) = storage_layout {
        let expected = if is_partitioned { "id_range" } else { "heap" };
        if layout == expected {
            println!("  [OK] Storage config matches physical layout ({layout})");
        } else {
            println!("  [FAIL] Storage config says '{layout}' but physical layout is '{expected}'");
            all_ok = false;
        }
    } else {
        println!("  [FAIL] Storage layout could not be read from tide.outbox_storage_config");
        all_ok = false;
    }

    // v0.17.0: Check (a) DLQ INSERT privilege.
    let dlq_writable: bool = client
        .query_one(
            "SELECT has_table_privilege('tide.relay_dlq', 'INSERT')",
            &[],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or(false);
    if dlq_writable {
        println!("  [OK] Current role has INSERT on tide.relay_dlq");
    } else {
        println!("  [FAIL] Current role lacks INSERT on tide.relay_dlq — DLQ writes will fail");
        all_ok = false;
    }

    // v0.17.0: Check (b) advisory lock acquisition under default relay group.
    let lock_ok: bool = client
        .query_one(
            "SELECT pg_try_advisory_lock(hashtext('pg_tide_relay_group_default'))",
            &[],
        )
        .await
        .map(|r| r.get::<_, bool>(0))
        .unwrap_or(false);
    if lock_ok {
        let _ = client
            .execute(
                "SELECT pg_advisory_unlock(hashtext('pg_tide_relay_group_default'))",
                &[],
            )
            .await;
        println!("  [OK] Advisory lock acquisition succeeded");
    } else {
        println!("  [WARN] Advisory lock acquisition failed — another relay instance may hold it");
    }

    // v0.17.0: Check (c) LISTEN permission for tide_relay_config.
    let listen_ok = client.execute("LISTEN tide_relay_config", &[]).await;
    if listen_ok.is_ok() {
        let _ = client.execute("UNLISTEN tide_relay_config", &[]).await;
        println!("  [OK] LISTEN on tide_relay_config permitted");
    } else {
        println!("  [FAIL] LISTEN on tide_relay_config denied — hot-reload will not function");
        all_ok = false;
    }

    // v0.25.0: DLQ depth warning — check hourly DLQ write rate.
    let dlq_hourly: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM tide.relay_dlq \
             WHERE created_at > now() - interval '1 hour'",
            &[],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or(0);
    if dlq_hourly > dlq_warn_threshold {
        println!(
            "  [WARN] DLQ received {dlq_hourly} entries in the last hour \
             (threshold: {dlq_warn_threshold}) — check upstream data quality"
        );
    } else {
        println!("  [OK] DLQ hourly rate: {dlq_hourly} (threshold: {dlq_warn_threshold})");
    }

    // v0.25.0: Partition capacity check — warn when next partition boundary
    // is within 48 hours of the most recently written row.
    let partition_warning: Option<String> = client
        .query_opt(
            "SELECT c.outbox_name \
             FROM tide.tide_outbox_config c \
             WHERE c.partition_strategy <> 'none' \
               AND EXISTS ( \
                   SELECT 1 FROM tide.tide_outbox_messages m \
                   WHERE m.outbox_name = c.outbox_name \
                     AND m.created_at > now() - interval '48 hours' \
               ) \
             LIMIT 1",
            &[],
        )
        .await
        .ok()
        .flatten()
        .map(|r| r.get(0));
    if let Some(outbox) = partition_warning {
        println!(
            "  [WARN] Outbox '{outbox}' is partitioned and has recent writes — \
             verify next partition is provisioned (run 'pg-tide sweep')"
        );
    }

    // v0.28.0: Check INSERT privilege on tide.relay_delivery_receipts.
    let receipt_table_exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
             WHERE table_schema = 'tide' AND table_name = 'relay_delivery_receipts')",
            &[],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or(false);
    if receipt_table_exists {
        let receipt_writable: bool = client
            .query_one(
                "SELECT has_table_privilege('tide.relay_delivery_receipts', 'INSERT')",
                &[],
            )
            .await
            .map(|r| r.get(0))
            .unwrap_or(false);
        if receipt_writable {
            println!("  [OK] Current role has INSERT on tide.relay_delivery_receipts");
        } else {
            println!(
                "  [WARN] Current role lacks INSERT on tide.relay_delivery_receipts \
                 — delivery receipt writes will be skipped"
            );
        }
    } else {
        println!(
            "  [INFO] tide.relay_delivery_receipts not found — upgrade to v0.28.0 \
             to enable delivery receipt tracking"
        );
    }

    // v0.28.0: Check lo_get/lo_unlink EXECUTE privilege (for claim-check pathway).
    let lo_get_ok: bool = client
        .query_one(
            "SELECT has_function_privilege('lo_get(oid)', 'EXECUTE')",
            &[],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or(false);
    if lo_get_ok {
        println!("  [OK] Current role has EXECUTE on lo_get (claim-check pathway available)");
    } else {
        println!(
            "  [WARN] Current role lacks EXECUTE on lo_get — \
             native claim-check pathway via outbox_publish_large() will fail"
        );
    }

    // v0.35.0: Delivery receipt row count warning — warn when the table has
    // grown large enough to impact sweep performance or storage.
    if receipt_table_exists {
        let receipt_count: i64 = client
            .query_one("SELECT COUNT(*) FROM tide.relay_delivery_receipts", &[])
            .await
            .map(|r| r.get(0))
            .unwrap_or(0);
        const RECEIPT_WARN_THRESHOLD: i64 = 1_000_000;
        if receipt_count > RECEIPT_WARN_THRESHOLD {
            println!(
                "  [WARN] tide.relay_delivery_receipts has {receipt_count} rows \
                 (> {RECEIPT_WARN_THRESHOLD}) — consider running \
                 `SELECT tide.relay_truncate_delivery_receipts()` or \
                 reducing sweep_interval_hours"
            );
        } else {
            println!("  [OK] relay_delivery_receipts row count: {receipt_count}");
        }
    }

    if all_ok {
        println!("\npg-tide doctor: all checks passed.");
        Ok(())
    } else {
        println!("\npg-tide doctor: one or more checks failed.");
        Err("doctor found one or more failed checks".into())
    }
}

async fn table_exists(client: &tokio_postgres::Client, table: &str) -> bool {
    client
        .query_one(
            "SELECT to_regclass($1) IS NOT NULL",
            &[&format!("tide.\"{table}\"")],
        )
        .await
        .map(|row| row.get(0))
        .unwrap_or(false)
}
