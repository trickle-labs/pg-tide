/// `pg-tide doctor` — PostgreSQL connectivity and catalog health check.
use crate::cli::OutputFormat;
use pg_tide_relay::{compatibility, pg_tls};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const BUNDLE_SCHEMA_VERSION: u32 = 1;
const REDACTION_POLICY_VERSION: &str = "v1";
const MAX_PIPELINES: usize = 100;
const MAX_STRING_BYTES: usize = 256;
const MAX_BUNDLE_BYTES: u64 = 1024 * 1024;
const BUNDLE_FILES: [&str; 6] = [
    "manifest.json",
    "versions.json",
    "doctor.json",
    "status.json",
    "error-codes.json",
    "metrics-metadata.json",
];

#[derive(Debug)]
struct DoctorCollection {
    data: Value,
    healthy: bool,
}

/// Result returned to the CLI owner after an atomic bundle attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupportBundleResult {
    pub path: PathBuf,
    pub healthy: bool,
}

/// Collect and atomically write a support bundle.  The caller owns rendering
/// and exit-code handling so the existing doctor envelope stays unchanged.
pub async fn collect_support_bundle(
    url: &str,
    target: &Path,
) -> Result<SupportBundleResult, Box<dyn std::error::Error>> {
    let doctor = collect_doctor_data(url).await;
    let status = crate::cmd::status::collect_status(url).await;
    let healthy = doctor.as_ref().is_ok_and(|result| result.healthy) && status.is_ok();

    let doctor_ok = doctor.is_ok();
    let status_ok = status.is_ok();
    let doctor_data = doctor.map_or_else(
        |_error| failed_collection("postgres.unavailable"),
        |result| result.data,
    );
    let status_data = status.map_or_else(
        |_error| failed_collection("postgres.unavailable"),
        |data| data,
    );
    write_support_bundle(
        target,
        doctor_data,
        status_data,
        doctor_ok,
        status_ok,
        healthy,
    )
}

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
                crate::cmd::diagnostic::from_boxed_error("postgres.catalog", error.as_ref()),
                output_format,
            )?;
        }
        return result;
    }
    run_doctor_with_threshold(url, 100).await
}

async fn collect_doctor_data(url: &str) -> Result<DoctorCollection, Box<dyn std::error::Error>> {
    let (client, conn) = match pg_tls::connect(url).await {
        Ok(connection) => connection,
        Err(error) => return Err(format!("connection failed: {error}").into()),
    };
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let compatibility = compatibility::check_client(&client, env!("CARGO_PKG_VERSION")).await?;
    let extension_version = Some(compatibility.extension_version.clone());
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
    let mut checks_ok = schema_exists;
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
        "compatibility": compatibility,
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
    Ok(DoctorCollection {
        data,
        healthy: checks_ok,
    })
}

async fn run_doctor_json(url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let collection = collect_doctor_data(url).await?;
    if collection.healthy {
        crate::cmd::output::success("doctor", collection.data, OutputFormat::Json)?;
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

    let compatibility = match compatibility::check_client(&client, env!("CARGO_PKG_VERSION")).await
    {
        Ok(decision) => decision,
        Err(error) => {
            println!("  [FAIL] {error}");
            return Err(error);
        }
    };
    println!(
        "  [OK] Compatibility: relay={} extension={} policy={} class={}",
        compatibility.relay_version,
        compatibility.extension_version,
        compatibility.policy_version,
        compatibility.compatibility_class
    );

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

fn failed_collection(code: &str) -> Value {
    serde_json::json!({
        "collection": {"status": "failed", "error_code": code}
    })
}

fn write_support_bundle(
    target: &Path,
    doctor_data: Value,
    status_data: Value,
    doctor_ok: bool,
    status_ok: bool,
    healthy: bool,
) -> Result<SupportBundleResult, Box<dyn std::error::Error>> {
    let parent = target.parent().unwrap_or_else(|| Path::new("."));
    if !parent.is_dir() {
        return Err("support.bundle.write_failed: parent directory is unavailable".into());
    }
    if fs::symlink_metadata(target).is_ok() {
        return Err("support.bundle.target_exists: target directory already exists".into());
    }

    let temporary = temporary_sibling(parent)?;
    let result = write_bundle_contents(
        &temporary,
        doctor_data,
        status_data,
        doctor_ok,
        status_ok,
        healthy,
    );
    match result {
        Ok(()) => {
            if let Err(error) = fs::rename(&temporary, target) {
                let _ = fs::remove_dir_all(&temporary);
                return Err(format!("support.bundle.write_failed: rename failed: {error}").into());
            }
            Ok(SupportBundleResult {
                path: target.to_path_buf(),
                healthy,
            })
        }
        Err(error) => {
            let _ = fs::remove_dir_all(&temporary);
            Err(error)
        }
    }
}

fn write_bundle_contents(
    directory: &Path,
    doctor_data: Value,
    status_data: Value,
    doctor_ok: bool,
    status_ok: bool,
    healthy: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut truncations = 0u64;
    let total_pipelines = status_data
        .get("pipelines")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    let doctor_data = bound_json(doctor_data, &mut truncations);
    let status_data = bound_json(status_data, &mut truncations);
    let versions = versions_data(&doctor_data);
    let error_codes = error_codes_data(&status_data);
    let metrics = metrics_metadata();
    let payloads = [
        ("versions.json", versions, doctor_ok),
        ("doctor.json", doctor_data, doctor_ok),
        ("status.json", status_data, status_ok),
        ("error-codes.json", error_codes, status_ok),
        ("metrics-metadata.json", metrics, true),
    ];

    let mut entries = Vec::with_capacity(payloads.len());
    let mut total_bytes = 0u64;
    for (name, value, collected) in payloads {
        let bytes = serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": BUNDLE_SCHEMA_VERSION,
            "collection": {"status": if collected { "ok" } else { "failed" }},
            "health": if healthy { "pass" } else { "fail" },
            "data": value,
        }))?;
        total_bytes = total_bytes.saturating_add(bytes.len() as u64);
        entries.push((name, bytes, collected));
    }
    if total_bytes > MAX_BUNDLE_BYTES {
        return Err("support.bundle.write_failed: bundle exceeds 1 MiB".into());
    }

    set_private_directory(directory)?;
    for (name, bytes, _) in &entries {
        write_private_file(&directory.join(name), bytes)?;
    }

    let files = entries
        .iter()
        .map(|(name, bytes, collected)| {
            serde_json::json!({
                "filename": name,
                "bytes": bytes.len(),
                "sha256": sha256(bytes),
                "collection": {"status": if *collected { "ok" } else { "failed" }},
            })
        })
        .collect::<Vec<_>>();
    let manifest = serde_json::json!({
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "created_at": chrono::Utc::now().to_rfc3339(),
        "relay_version": env!("CARGO_PKG_VERSION"),
        "included_files": files,
        "truncation_counts": {
            "fields": truncations,
            "pipelines_total": total_pipelines,
            "pipelines_omitted": total_pipelines.saturating_sub(MAX_PIPELINES),
        },
        "redaction_policy_version": REDACTION_POLICY_VERSION,
        "sharing_warning": "Support data is bounded and redacted; review it before sharing.",
    });
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    if total_bytes + manifest_bytes.len() as u64 > MAX_BUNDLE_BYTES {
        let _ = fs::remove_dir_all(directory);
        return Err("support.bundle.write_failed: bundle exceeds 1 MiB".into());
    }
    write_private_file(&directory.join("manifest.json"), &manifest_bytes)?;
    Ok(())
}

fn versions_data(doctor: &Value) -> Value {
    let compatibility = doctor.get("compatibility").unwrap_or(&Value::Null);
    serde_json::json!({
        "relay_version": compatibility.get("relay_version").cloned().unwrap_or_else(|| Value::String(env!("CARGO_PKG_VERSION").into())),
        "extension_version": compatibility.get("extension_version").cloned().unwrap_or(Value::Null),
        "policy_version": compatibility.get("policy_version").cloned().unwrap_or(Value::Null),
        "compatibility_class": compatibility.get("compatibility_class").cloned().unwrap_or(Value::Null),
    })
}

fn error_codes_data(status: &Value) -> Value {
    let pipelines = status
        .get("pipelines")
        .and_then(Value::as_array)
        .map(|pipelines| {
            pipelines
                .iter()
                .filter_map(|pipeline| {
                    Some(serde_json::json!({
                        "pipeline_id": pipeline.get("pipeline_id")?.clone(),
                        "last_error_code": pipeline.get("last_error_code")?.clone(),
                        "last_error_component": pipeline.get("last_error_component")?.clone(),
                        "last_error_class": pipeline.get("last_error_class")?.clone(),
                        "last_error_at": pipeline.get("last_error_at")?.clone(),
                    }))
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    serde_json::json!({"pipelines": pipelines})
}

fn metrics_metadata() -> Value {
    let metrics = include_str!("../../../schemas/metrics-v1.tsv")
        .lines()
        .skip(1)
        .filter_map(|line| {
            let mut fields = line.split('\t');
            Some(serde_json::json!({
                "name": fields.next()?,
                "type": fields.next()?,
                "unit": fields.next()?,
                "labels": fields.next()?.split(',').collect::<Vec<_>>(),
            }))
        })
        .collect::<Vec<_>>();
    serde_json::json!({"metrics": metrics})
}

fn bound_json(value: Value, truncations: &mut u64) -> Value {
    match value {
        Value::String(value) => {
            if value.contains("://") {
                *truncations += 1;
                Value::String("[REDACTED]".into())
            } else if value.len() > MAX_STRING_BYTES {
                *truncations += 1;
                let mut end = MAX_STRING_BYTES;
                while !value.is_char_boundary(end) {
                    end -= 1;
                }
                Value::String(value[..end].to_string())
            } else {
                Value::String(value)
            }
        }
        Value::Array(values) => {
            let mut values = values;
            if values.len() > MAX_PIPELINES {
                *truncations += (values.len() - MAX_PIPELINES) as u64;
                values.truncate(MAX_PIPELINES);
            }
            Value::Array(
                values
                    .into_iter()
                    .map(|value| bound_json(value, truncations))
                    .collect(),
            )
        }
        Value::Object(values) => Value::Object(
            values
                .into_iter()
                .filter_map(|(key, value)| {
                    if matches!(
                        key.as_str(),
                        "payload"
                            | "headers"
                            | "config"
                            | "environment"
                            | "certificate"
                            | "certificates"
                            | "key"
                            | "keys"
                            | "logs"
                            | "message"
                            | "reason"
                            | "url"
                    ) {
                        *truncations += 1;
                        None
                    } else {
                        Some((key, bound_json(value, truncations)))
                    }
                })
                .collect(),
        ),
        value => value,
    }
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn temporary_sibling(parent: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let pid = std::process::id();
    let time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    for attempt in 0..100u32 {
        let candidate = parent.join(format!(".pg-tide-bundle-{pid}-{time}-{attempt}"));
        match fs::create_dir(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Err("support.bundle.write_failed: temporary directory unavailable".into())
}

fn set_private_directory(path: &Path) -> Result<(), std::io::Error> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn write_private_file(path: &Path, bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file: File = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod support_bundle_tests {
    use super::*;

    #[test]
    fn bounds_strings_and_arrays_without_leaking_urls() {
        let mut truncations = 0;
        let value = bound_json(
            serde_json::json!({
                "endpoint": "postgres://user:secret@example.test/db",
                "pipelines": (0..101).map(|_| "x").collect::<Vec<_>>(),
                "long": "x".repeat(MAX_STRING_BYTES + 1),
            }),
            &mut truncations,
        );
        assert_eq!(value["endpoint"], "[REDACTED]");
        assert_eq!(value["pipelines"].as_array().map(Vec::len), Some(100));
        assert!(truncations >= 2);
        assert!(!serde_json::to_string(&value).unwrap().contains("secret"));
    }

    #[test]
    fn fixed_file_set_and_digests_are_written() {
        let root = std::env::temp_dir().join(format!("pg-tide-bundle-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let result = write_support_bundle(
            &root,
            serde_json::json!({"compatibility": {"relay_version": "0.54.0"}}),
            serde_json::json!({"pipelines": []}),
            true,
            true,
            true,
        )
        .unwrap();
        let mut names = fs::read_dir(&result.path)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        names.sort();
        let mut expected = BUNDLE_FILES.map(str::to_string).to_vec();
        expected.sort();
        assert_eq!(names, expected);
        let manifest: Value =
            serde_json::from_slice(&fs::read(root.join("manifest.json")).unwrap()).unwrap();
        for entry in manifest["included_files"].as_array().unwrap() {
            let bytes = fs::read(root.join(entry["filename"].as_str().unwrap())).unwrap();
            assert_eq!(entry["bytes"], bytes.len());
            assert_eq!(entry["sha256"], sha256(&bytes));
        }
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn existing_target_is_refused_without_overwrite() {
        let root =
            std::env::temp_dir().join(format!("pg-tide-bundle-existing-{}", std::process::id()));
        fs::create_dir_all(&root).unwrap();
        let error =
            write_support_bundle(&root, Value::Null, Value::Null, false, false, false).unwrap_err();
        assert!(error.to_string().contains("target_exists"));
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn failed_collection_writes_no_secret_canary() {
        let root =
            std::env::temp_dir().join(format!("pg-tide-bundle-canary-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        write_support_bundle(
            &root,
            serde_json::json!({"error": "postgres://user:canary@example.test/db"}),
            failed_collection("postgres.unavailable"),
            false,
            false,
            false,
        )
        .unwrap();
        for entry in fs::read_dir(&root).unwrap() {
            let bytes = fs::read(entry.unwrap().path()).unwrap();
            assert!(!String::from_utf8_lossy(&bytes).contains("canary"));
        }
        let _ = fs::remove_dir_all(&root);
    }
}
