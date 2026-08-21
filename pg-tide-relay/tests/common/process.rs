use std::process::{Child, Command, Stdio};
use std::time::Duration;

/// A real `pg-tide` child with bounded shutdown for ignored E2E tests.
pub struct RelayProcess {
    child: Option<Child>,
}

impl RelayProcess {
    pub fn start(database_url: &str, relay_group: &str) -> Self {
        let binary = std::env::var("PG_TIDE_E2E_RELAY_BIN")
            .or_else(|_| std::env::var("PG_TIDE_RELAY_BIN"))
            .unwrap_or_else(|_| {
                option_env!("CARGO_BIN_EXE_pg-tide")
                    .unwrap_or("target/debug/pg-tide")
                    .to_string()
            });
        let child = Command::new(&binary)
            .args([
                "--postgres-url",
                database_url,
                "--relay-group-id",
                relay_group,
                "--metrics-addr",
                "127.0.0.1:0",
                "--log-level",
                "warn",
                "run",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap_or_else(|error| panic!("start relay binary {binary}: {error}"));
        Self { child: Some(child) }
    }

    pub async fn stop(mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        let _ = child.kill();
        let _ = tokio::time::timeout(
            Duration::from_secs(5),
            tokio::task::spawn_blocking(move || child.wait()),
        )
        .await;
    }
}

impl Drop for RelayProcess {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
        }
    }
}

pub async fn wait_until<F, Fut>(timeout: Duration, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if condition().await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition timed out"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
