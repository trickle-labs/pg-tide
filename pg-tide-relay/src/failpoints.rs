//! Fixed, opt-in failpoints used by the crash-safety integration tests.

#[cfg(any(feature = "test-failpoints", test))]
const ALLOWED: &[&str] = &[
    "after_poll_before_encode",
    "after_encode_before_publish",
    "after_sink_ack",
    "before_checkpoint_commit",
    "after_offset_db_commit_before_cleanup",
    "after_checkpoint_commit",
    "during_dlq_write",
    "after_dlq_commit",
    "during_replay",
    "ownership_connection_lost",
    "during_shutdown",
];

#[cfg(any(feature = "test-failpoints", test))]
pub fn is_allowed(name: &str) -> bool {
    ALLOWED.contains(&name)
}

#[macro_export]
macro_rules! test_failpoint {
    ($name:literal, $pipeline:expr) => {{
        #[cfg(feature = "test-failpoints")]
        {
            $crate::failpoints::hit($name, $pipeline).await
        }
        #[cfg(not(feature = "test-failpoints"))]
        {
            Ok::<(), $crate::error::RelayError>(())
        }
    }};
}

#[cfg(feature = "test-failpoints")]
pub async fn hit(name: &str, pipeline: &str) -> Result<(), crate::error::RelayError> {
    use std::path::Path;
    use std::time::Duration;

    if !is_allowed(name) || std::env::var("PG_TIDE_FAILPOINT").ok().as_deref() != Some(name) {
        return Ok(());
    }

    tracing::warn!(failpoint = name, pipeline, "test failpoint reached");
    if let Ok(path) = std::env::var("PG_TIDE_FAILPOINT_READY") {
        std::fs::write(path, format!("{name}\n")).map_err(crate::error::RelayError::Io)?;
    }
    if std::env::var_os("PG_TIDE_FAILPOINT_ERROR").is_some() {
        return Err(crate::error::RelayError::other(format!(
            "test failpoint injected error: {name}"
        )));
    }

    let continue_path = std::env::var("PG_TIDE_FAILPOINT_CONTINUE").ok();
    while continue_path
        .as_deref()
        .map(|path| !Path::new(path).exists())
        .unwrap_or(true)
    {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_fixed_failpoints_are_allowed() {
        assert!(is_allowed("after_poll_before_encode"));
        assert!(is_allowed("after_checkpoint_commit"));
        assert!(!is_allowed("arbitrary_command"));
    }
}
