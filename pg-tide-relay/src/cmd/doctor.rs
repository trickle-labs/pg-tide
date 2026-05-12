/// `pg-tide doctor` — PostgreSQL connectivity and catalog health check.
use pg_tide_relay::pg_tls;

/// Validate PostgreSQL connectivity, schema presence, and catalog health.
pub async fn run_doctor(url: &str) -> Result<(), Box<dyn std::error::Error>> {
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

    if all_ok {
        println!("\npg-tide doctor: all checks passed.");
        Ok(())
    } else {
        println!("\npg-tide doctor: one or more checks failed.");
        std::process::exit(1);
    }
}
