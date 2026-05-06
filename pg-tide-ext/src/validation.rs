//! Shared identifier validation for pg_tide.
//!
//! All dynamic SQL identifier paths (outbox names, inbox names, schema names)
//! must pass through `validate_identifier()` before use in format strings or
//! dynamic DDL/DML.
//!
//! Identifiers are always double-quoted in SQL (`"name"`), so most printable
//! ASCII characters are safe. The only characters that can break out of
//! double-quoting are the double-quote itself (`"`) and the null byte (`\0`).
//!
//! Rules:
//! - Non-empty
//! - ≤ 63 bytes (PostgreSQL's `NAMEDATALEN - 1`)
//! - No double-quote characters (`"`)
//! - No null bytes (`\0`)

use crate::error::PgTideError;

/// Validate a PostgreSQL identifier (outbox name, inbox name, schema, etc.).
///
/// Identifiers are always used as double-quoted SQL identifiers (`"name"`),
/// so the only characters that must be rejected are those that can break out
/// of double-quoting: `"` and null bytes.
///
/// # Errors
/// Returns `PgTideError::InvalidArgument` if the name is empty, too long, or
/// contains a double-quote or null byte.
pub fn validate_identifier(name: &str) -> Result<(), PgTideError> {
    if name.is_empty() {
        return Err(PgTideError::InvalidArgument(
            "identifier must not be empty".to_string(),
        ));
    }
    if name.len() > 63 {
        return Err(PgTideError::InvalidArgument(format!(
            "identifier '{}' exceeds 63 bytes",
            name
        )));
    }
    for c in name.chars() {
        if c == '"' || c == '\0' {
            return Err(PgTideError::InvalidArgument(format!(
                "identifier '{}' contains invalid character '{}'",
                name, c
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_identifiers() {
        assert!(validate_identifier("orders").is_ok());
        assert!(validate_identifier("my_outbox_1").is_ok());
        assert!(validate_identifier("_private").is_ok());
        assert!(validate_identifier("A").is_ok());
        // Hyphens are fine because identifiers are always double-quoted in SQL.
        assert!(validate_identifier("my-outbox").is_ok());
        assert!(validate_identifier("dedup-inbox").is_ok());
        assert!(validate_identifier("smoke-inbox").is_ok());
        // Dots are fine in double-quoted contexts.
        assert!(validate_identifier("some.thing").is_ok());
        // Exactly 63 bytes.
        assert!(validate_identifier(&"a".repeat(63)).is_ok());
    }

    #[test]
    fn invalid_empty() {
        assert!(validate_identifier("").is_err());
    }

    #[test]
    fn invalid_too_long() {
        assert!(validate_identifier(&"a".repeat(64)).is_err());
    }

    #[test]
    fn invalid_contains_double_quote() {
        // Double-quotes break out of SQL double-quoting.
        assert!(validate_identifier("bad\"name").is_err());
    }

    #[test]
    fn invalid_contains_null_byte() {
        assert!(validate_identifier("bad\0name").is_err());
    }

    // These were previously rejected but are now allowed because identifiers
    // are always double-quoted in SQL.
    #[test]
    fn previously_invalid_now_valid() {
        assert!(validate_identifier("1bad").is_ok());
        assert!(validate_identifier("bad;name").is_ok());
        assert!(validate_identifier("bad'name").is_ok());
    }
}
