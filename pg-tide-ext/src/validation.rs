//! Shared identifier validation for pg_tide.
//!
//! All dynamic SQL identifier paths (outbox names, inbox names, schema names)
//! must pass through `validate_identifier()` before use in format strings or
//! dynamic DDL/DML.
//!
//! Rules:
//! - Non-empty
//! - ≤ 63 bytes (PostgreSQL's `NAMEDATALEN - 1`)
//! - First character: ASCII letter (`a-z`, `A-Z`) or underscore (`_`)
//! - Remaining characters: ASCII letter, digit (`0-9`), or underscore

use crate::error::PgTideError;

/// Validate a PostgreSQL identifier (outbox name, inbox name, schema, etc.).
///
/// # Errors
/// Returns `PgTideError::InvalidArgument` if the name is empty, too long, or
/// contains characters that are not safe PostgreSQL unquoted identifiers.
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
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c == '_' || c.is_ascii_alphabetic() => {}
        _ => {
            return Err(PgTideError::InvalidArgument(format!(
                "identifier '{}' must start with a letter or underscore",
                name
            )))
        }
    }
    for c in chars {
        if !(c == '_' || c.is_ascii_alphanumeric()) {
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
    fn invalid_starts_with_digit() {
        assert!(validate_identifier("1bad").is_err());
    }

    #[test]
    fn invalid_contains_dot() {
        assert!(validate_identifier("bad.name").is_err());
    }

    #[test]
    fn invalid_contains_semicolon() {
        assert!(validate_identifier("bad;name").is_err());
    }

    #[test]
    fn invalid_contains_quote() {
        assert!(validate_identifier("bad'name").is_err());
    }
}
