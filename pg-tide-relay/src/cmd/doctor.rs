/// `pg-tide doctor` — PostgreSQL connectivity and catalog health check.
use pg_tide_relay::pg_tls;

/// Validate PostgreSQL connectivity, schema presence, and catalog health.
pub async fn run_doctor(url: &str) -> Result<(), Box<dyn std::error::Error>> {
    run_doctor_with_threshold(url, 100).await
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
        .map_err(|e| format!("connection failed: {e}"))?;

    tokio::spawn(async move {
        let _ = conn.await;
    });

    println!("  [OK] Connected to PostgreSQL");

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
        std::process::exit(1);
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
    let mut all_ok = true;
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

    // v0.25.0: DuckLake catalog health check.
    let ducklake_tables = ["ducklake_snapshot", "ducklake_data_file", "ducklake_column"];
    let mut ducklake_ok = true;
    for table in &ducklake_tables {
        let exists: bool = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
                 WHERE table_name = $1)",
                &[table],
            )
            .await
            .map(|r| r.get(0))
            .unwrap_or(false);
        if !exists {
            ducklake_ok = false;
            break;
        }
    }
    if ducklake_ok {
        println!("  [OK] DuckLake catalog tables accessible");
    } else {
        println!(
            "  [INFO] DuckLake catalog tables not found — DuckLake sink/source requires \
             v0.20.0+ schema and DuckLake extension"
        );
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
        std::process::exit(1);
    }
}
