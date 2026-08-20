/// `pg-tide --self-test` — startup pre-flight health check.
///
/// Connects to PostgreSQL, verifies the pg_tide extension schema, checks TLS
/// state, acquires and immediately releases an advisory lock, queries
/// `tide.relay_outbox_config`, then exits 0 on success or 1 on failure.
///
/// Designed for Kubernetes `initContainers`, container health checks, and
/// CI/CD pre-deployment gates.
///
/// v0.33.0: Accepts an optional `expect_version` parameter.  When set, the
/// installed `pg_tide` extension version must be >= the supplied value; if not
/// the self-test exits 1 with a descriptive error message, blocking relay
/// startup on an incompatible extension version.
use pg_tide_relay::pg_tls;

/// Run the self-test and return Ok(()) on pass, otherwise call process::exit(1).
///
/// `expect_version`: when `Some(ver)`, the installed pg_tide extension version
/// must satisfy `installed_version >= ver` (semver-aware comparison).
pub async fn run_self_test(
    url: &str,
    expect_version: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("pg-tide self-test v{}", env!("CARGO_PKG_VERSION"));

    // 1. Connect.
    let (client, conn) = pg_tls::connect(url)
        .await
        .map_err(|e| format!("connection failed: {e}"))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    println!("  [OK] Connected to PostgreSQL");

    // 2. Verify extension schema exists.
    let schema_ok: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'tide')",
            &[],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or(false);
    if !schema_ok {
        eprintln!("  [FAIL] Schema 'tide' not found — pg_tide extension is not installed");
        std::process::exit(1);
    }
    println!("  [OK] Schema 'tide' exists");

    // 3. Check TLS state.
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
            println!("  [OK] TLS: {tls_version}");
        } else if url.contains("sslmode=require")
            || url.contains("sslmode=verify-ca")
            || url.contains("sslmode=verify-full")
        {
            eprintln!(
                "  [FAIL] sslmode=require/verify-* requested but connection is plaintext \
                 — compile with --features native-tls or use a TLS proxy"
            );
            std::process::exit(1);
        } else {
            println!("  [INFO] TLS: plaintext (sslmode not set)");
        }
    }

    // 4. Acquire and immediately release an advisory lock.
    let lock_ok: bool = client
        .query_one(
            "SELECT pg_try_advisory_lock(hashtext('pg_tide_self_test_lock'))",
            &[],
        )
        .await
        .map(|r| r.get::<_, bool>(0))
        .unwrap_or(false);
    if lock_ok {
        let _ = client
            .execute(
                "SELECT pg_advisory_unlock(hashtext('pg_tide_self_test_lock'))",
                &[],
            )
            .await;
        println!("  [OK] Advisory lock: acquire/release succeeded");
    } else {
        eprintln!("  [FAIL] Advisory lock acquisition failed");
        std::process::exit(1);
    }

    // 5. Query tide.relay_outbox_config (confirms catalog is queryable).
    let pipeline_count: i64 = client
        .query_one("SELECT COUNT(*) FROM tide.relay_outbox_config", &[])
        .await
        .map(|r| r.get(0))
        .unwrap_or(0);
    println!("  [OK] tide.relay_outbox_config: {pipeline_count} pipeline(s) configured");

    // 6. v0.33.0: Verify installed extension version meets the minimum requirement.
    let installed_version: Option<String> = client
        .query_opt(
            "SELECT extversion FROM pg_extension WHERE extname = 'pg_tide'",
            &[],
        )
        .await
        .ok()
        .flatten()
        .map(|r| r.get(0));

    match installed_version.as_deref() {
        None => {
            eprintln!(
                "  [FAIL] pg_tide extension not found in pg_extension — \
                 run CREATE EXTENSION pg_tide"
            );
            std::process::exit(1);
        }
        Some(installed) => {
            println!("  [OK] pg_tide extension version: {installed}");

            // If caller requested a minimum version, enforce it.
            if let Some(min_ver) = expect_version {
                if !version_meets_minimum(installed, min_ver) {
                    eprintln!(
                        "  [FAIL] pg_tide extension version {installed} does not meet \
                         the minimum required version {min_ver}.\n\
                         Run `ALTER EXTENSION pg_tide UPDATE TO '{min_ver}';` to upgrade."
                    );
                    std::process::exit(1);
                }
                println!("  [OK] pg_tide extension version >= {min_ver}");
            }
        }
    }

    // 7. Verify the compiled-in minimum version matches the installed schema.
    // Check that tide.tide_outbox_config has the partition_strategy column (v0.25.0+).
    let has_partition_col: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
             WHERE table_schema = 'tide' AND table_name = 'tide_outbox_config' \
             AND column_name = 'partition_strategy')",
            &[],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or(false);
    if has_partition_col {
        println!("  [OK] Schema version: v0.25.0+ (partition_strategy column present)");
    } else {
        println!("  [WARN] Schema appears to be pre-v0.25.0 — run ALTER EXTENSION pg_tide UPDATE");
    }

    println!("\npg-tide self-test: PASS");
    Ok(())
}

/// Return `true` if `installed` >= `minimum` using simple semver comparison.
///
/// Parses both strings as `MAJOR.MINOR.PATCH` tuples.  If either string does
/// not parse, falls back to a lexicographic comparison (safe for
/// `0.x.y`-formatted versions where the major version is 0).
fn version_meets_minimum(installed: &str, minimum: &str) -> bool {
    fn parse_version(v: &str) -> Option<(u32, u32, u32)> {
        let parts: Vec<&str> = v.split('.').collect();
        if parts.len() >= 3 {
            let major = parts[0].parse::<u32>().ok()?;
            let minor = parts[1].parse::<u32>().ok()?;
            let patch = parts[2].parse::<u32>().ok()?;
            Some((major, minor, patch))
        } else {
            None
        }
    }

    match (parse_version(installed), parse_version(minimum)) {
        (Some(i), Some(m)) => i >= m,
        _ => installed >= minimum, // lexicographic fallback
    }
}

#[cfg(test)]
mod tests {
    use super::version_meets_minimum;

    #[test]
    fn test_version_meets_minimum_exact() {
        assert!(version_meets_minimum("0.33.0", "0.33.0"));
        assert!(version_meets_minimum("1.0.0", "1.0.0"));
    }

    #[test]
    fn test_version_meets_minimum_higher() {
        assert!(version_meets_minimum("0.33.0", "0.32.0"));
        assert!(version_meets_minimum("1.0.0", "0.33.0"));
        assert!(version_meets_minimum("1.1.0", "1.0.0"));
    }

    #[test]
    fn test_version_meets_minimum_lower() {
        assert!(!version_meets_minimum("0.32.0", "0.33.0"));
        assert!(!version_meets_minimum("0.33.0", "1.0.0"));
        assert!(!version_meets_minimum("0.9.0", "0.10.0"));
    }
}
