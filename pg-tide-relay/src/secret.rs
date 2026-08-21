//! Strict, redacted secret references and safe local file loading.
use crate::error::RelayError;
use std::fmt;
use std::io::Read;
use std::path::Path;

pub const MAX_SECRET_FILE_BYTES: u64 = 1024 * 1024;

#[derive(Clone, PartialEq, Eq)]
pub struct SecretString(String);
impl SecretString {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
    pub fn expose(&self) -> &str {
        &self.0
    }
}
impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretString(REDACTED)")
    }
}
impl fmt::Display for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SecretRef {
    Env(String),
    File(String),
}

pub fn parse_reference(value: &str) -> Result<SecretRef, RelayError> {
    if let Some(name) = value
        .strip_prefix("${env:")
        .or_else(|| value.strip_prefix("${ENV:"))
        .and_then(|s| s.strip_suffix('}'))
    {
        if valid_name(name) {
            return Ok(SecretRef::Env(name.to_string()));
        }
    }
    if let Some(path) = value
        .strip_prefix("${file:")
        .and_then(|s| s.strip_suffix('}'))
    {
        if Path::new(path).is_absolute() && !path.contains('\0') {
            return Ok(SecretRef::File(path.to_string()));
        }
    }
    Err(RelayError::InvalidSecretToken(value.to_string()))
}

pub fn resolve_reference(reference: &SecretRef) -> Result<SecretString, RelayError> {
    match reference {
        SecretRef::Env(name) => {
            std::env::var(name)
                .map(SecretString::new)
                .map_err(|_| RelayError::SecretNotFound {
                    token: format!("${{env:{name}}}"),
                })
        }
        SecretRef::File(path) => load_file(Path::new(path)),
    }
}

pub fn resolve(value: &str) -> Result<SecretString, RelayError> {
    resolve_reference(&parse_reference(value)?)
}

pub fn load_file(path: &Path) -> Result<SecretString, RelayError> {
    use std::fs::OpenOptions;
    let mut opts = OpenOptions::new();
    opts.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.custom_flags(libc::O_NOFOLLOW);
    }
    let file = opts.open(path).map_err(|e| read_err(path, e.to_string()))?;
    let meta = file.metadata().map_err(|e| read_err(path, e.to_string()))?;
    if !meta.is_file() || meta.len() > MAX_SECRET_FILE_BYTES {
        return Err(read_err(
            path,
            "not a regular file or exceeds size limit".to_string(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // SAFETY: geteuid has no preconditions and only reads the process identity.
        let effective_uid = unsafe { libc::geteuid() };
        if meta.uid() != effective_uid || (meta.mode() & 0o077) != 0 {
            return Err(read_err(
                path,
                "file ownership or permissions are unsafe".to_string(),
            ));
        }
    }
    let mut bytes = Vec::with_capacity(meta.len() as usize);
    std::io::Read::take(&mut (&file), MAX_SECRET_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| read_err(path, e.to_string()))?;
    if bytes.len() as u64 > MAX_SECRET_FILE_BYTES {
        return Err(read_err(path, "file exceeds size limit".to_string()));
    }
    String::from_utf8(bytes)
        .map(|s| SecretString::new(s.trim_end_matches(['\r', '\n'])))
        .map_err(|_| read_err(path, "file is not UTF-8".to_string()))
}
fn valid_name(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}
fn read_err(path: &Path, reason: String) -> RelayError {
    RelayError::SecretReadError {
        path: path.display().to_string(),
        reason,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn strict_references() {
        assert!(
            matches!(parse_reference("${env:PG_TIDE_KEY}"), Ok(SecretRef::Env(name)) if name == "PG_TIDE_KEY")
        );
        assert!(parse_reference("${env:bad-name}").is_err());
        assert!(parse_reference("${file:}").is_err());
        assert!(parse_reference("${file:relative-secret}").is_err());
        assert!(parse_reference("plain").is_err());
    }
    #[test]
    fn secret_is_redacted() {
        let s = SecretString::new("canary");
        assert_eq!(s.to_string(), "[REDACTED]");
        assert!(!format!("{:?}", s).contains("canary"));
    }

    #[cfg(unix)]
    #[test]
    fn file_loader_uses_an_absolute_nofollow_path() {
        use std::os::unix::fs::symlink;
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let secret_path = directory.path().join("secret");
        let link_path = directory.path().join("link");
        std::fs::write(&secret_path, "canary\n").unwrap();
        std::fs::set_permissions(&secret_path, std::fs::Permissions::from_mode(0o600)).unwrap();
        symlink(&secret_path, &link_path).unwrap();
        assert_eq!(load_file(&secret_path).unwrap().expose(), "canary");
        assert!(load_file(&link_path).is_err());
    }
}
