//! Server configuration. One JSON file, checked on the open handle before it is read.
//!
//! Errors name the path and the mode, never file content, so a token cannot leak
//! through an error message.

use serde_json::{json, Value};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

/// Shortest accepted token, in bytes.
pub const MIN_TOKEN_BYTES: usize = 32;
/// Longest accepted token, in bytes. Its `Authorization` header fits the 16 KiB head.
pub const MAX_TOKEN_BYTES: usize = 1024;
/// Longest tenant name, in UTF-8 bytes. The engine's limit.
pub const MAX_TENANT_BYTES: usize = 255;
const MAX_FILE_BYTES: u64 = 1 << 20;
const FIELDS: &str = "data_dir, listen, unix_socket, unix_socket_group, tenants";

/// A tenant's bearer token. `Debug` never shows it.
#[derive(Clone, Eq, PartialEq)]
pub struct Token(String);

impl Token {
    /// Accepts 32 to 1024 visible ASCII bytes, which fit an `Authorization` header as is.
    pub fn new(text: String) -> Result<Self, &'static str> {
        if !(MIN_TOKEN_BYTES..=MAX_TOKEN_BYTES).contains(&text.len()) {
            return Err("token must be 32 to 1024 bytes");
        }
        if !text.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err("token must be visible ASCII without spaces");
        }
        Ok(Self(text))
    }

    /// The secret itself. Send it only in an `Authorization` header.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Token(<redacted>)")
    }
}

/// One tenant and its token.
#[derive(Clone, Debug)]
pub struct Tenant {
    /// Tenant name, 1 to 255 UTF-8 bytes.
    pub name: String,
    /// The token that selects this tenant.
    pub token: Token,
}

/// The TCP listen setting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Listen {
    /// Bind no TCP listener.
    None,
    /// Bind this address.
    Tcp(SocketAddr),
}

impl Listen {
    /// Parses `none` or an address such as `127.0.0.1:<port>`.
    pub fn parse(text: &str) -> Result<Self, String> {
        if text == "none" {
            return Ok(Self::None);
        }
        let error = "listen must be an address such as 127.0.0.1:<port>, or \"none\"";
        text.parse().map(Self::Tcp).map_err(|_| error.to_owned())
    }
}

/// A checked configuration file. Flags override its fields.
#[derive(Clone, Debug, Default)]
pub struct Config {
    /// Data directory.
    pub data_dir: Option<PathBuf>,
    /// TCP listen address, or none.
    pub listen: Option<Listen>,
    /// Unix socket path.
    pub unix_socket: Option<PathBuf>,
    /// Group for a 0660 Unix socket, as a name or a numeric gid.
    pub unix_socket_group: Option<String>,
    /// At least one tenant. Tokens are unique.
    pub tenants: Vec<Tenant>,
}

impl Config {
    /// Opens, checks and parses a configuration file.
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = read_private(path, "config")?;
        parse(&text).map_err(|message| format!("config {}: {message}", path.display()))
    }

    /// Finds a tenant by name.
    pub fn tenant(&self, name: &str) -> Option<&Tenant> {
        self.tenants.iter().find(|tenant| tenant.name == name)
    }
}

/// Reads a token file with the same checks as the configuration file.
/// Surrounding whitespace, such as a trailing newline, is ignored.
pub fn read_token_file(path: &Path) -> Result<Token, String> {
    let text = read_private(path, "token file")?;
    Token::new(text.trim().to_owned())
        .map_err(|message| format!("token file {}: {message}", path.display()))
}

/// Writes a new mode 0600 configuration with one tenant and a random token.
/// Never overwrites an existing file. Returns the token so the caller can print it once.
pub fn init(path: &Path, tenant: &str) -> Result<Token, String> {
    let fail = |message: String| format!("config {}: {message}", path.display());
    check_tenant_name(tenant).map_err(|message| fail(message.into()))?;
    let token = crate::random_hex(MIN_TOKEN_BYTES)
        .map_err(|error| fail(format!("cannot read /dev/urandom: {error}")))
        .and_then(|hex| Token::new(hex).map_err(|message| fail(message.into())))?;
    let tenants = json!([{"name": tenant, "token": token.expose()}]);
    let mut text = serde_json::to_string_pretty(&json!({ "tenants": tenants }))
        .map_err(|_| fail("cannot encode the configuration".into()))?;
    text.push('\n');
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|error| fail(format!("cannot create: {error}")))?;
    if let Err(error) = file.write_all(text.as_bytes()).and_then(|()| file.sync_all()) {
        drop(file);
        let _ = std::fs::remove_file(path);
        return Err(fail(format!("cannot write: {error}")));
    }
    Ok(token)
}

/// Opens with `O_NONBLOCK`, so a FIFO cannot block the open, then refuses anything but a
/// regular file, checked with `fstat` on the handle. Read or write through that handle.
pub fn open_regular(options: &mut OpenOptions, path: &Path) -> std::io::Result<File> {
    let file = options.custom_flags(rustix::fs::OFlags::NONBLOCK.bits() as i32).open(path)?;
    if !file.metadata()?.is_file() {
        return Err(std::io::Error::other("must be a regular file"));
    }
    Ok(file)
}

/// Opens once with [`open_regular`] and [`check_private`], then reads the same handle.
fn read_private(path: &Path, what: &str) -> Result<String, String> {
    let fail = |message: String| format!("{what} {}: {message}", path.display());
    let file = open_regular(OpenOptions::new().read(true), path)
        .map_err(|error| fail(format!("cannot open: {error}")))?;
    check_private(&file, path, what)?;
    let mut text = String::new();
    file.take(MAX_FILE_BYTES + 1)
        .read_to_string(&mut text)
        .map_err(|error| fail(format!("cannot read: {error}")))?;
    if text.len() as u64 > MAX_FILE_BYTES {
        return Err(fail(format!("larger than {MAX_FILE_BYTES} bytes")));
    }
    Ok(text)
}

/// Checks an open file with `fstat`: a regular file owned by the effective uid, with no
/// group or other bits. The error names the path and the mode, never content.
pub fn check_private(file: &File, path: &Path, what: &str) -> Result<(), String> {
    let fail = |message: String| format!("{what} {}: {message}", path.display());
    let meta = file.metadata().map_err(|error| fail(format!("cannot stat: {error}")))?;
    let euid = rustix::process::geteuid().as_raw();
    let mode = meta.mode() & 0o7777;
    if !meta.file_type().is_file() || meta.uid() != euid || mode & 0o077 != 0 {
        return Err(fail(format!(
            "must be a regular file owned by uid {euid} with mode 0600 or stricter, \
             found mode {mode:04o} owned by uid {}",
            meta.uid()
        )));
    }
    Ok(())
}

fn parse(text: &str) -> Result<Config, String> {
    let value: Value = serde_json::from_str(text).map_err(|error| {
        format!("invalid JSON at line {} column {}", error.line(), error.column())
    })?;
    let Value::Object(fields) = value else {
        return Err("expected a JSON object".into());
    };
    let mut config = Config::default();
    for (key, value) in fields {
        match key.as_str() {
            "data_dir" => config.data_dir = Some(non_empty(value, "data_dir")?.into()),
            "unix_socket" => config.unix_socket = Some(non_empty(value, "unix_socket")?.into()),
            "unix_socket_group" => {
                config.unix_socket_group = Some(non_empty(value, "unix_socket_group")?)
            }
            "listen" => config.listen = Some(Listen::parse(&non_empty(value, "listen")?)?),
            "tenants" => config.tenants = tenants(value)?,
            _ => return Err(format!("unknown top-level field, expected one of: {FIELDS}")),
        }
    }
    if config.tenants.is_empty() {
        return Err("tenants must name at least one tenant".into());
    }
    Ok(config)
}

fn non_empty(value: Value, field: &str) -> Result<String, String> {
    match value {
        Value::String(text) if !text.is_empty() => Ok(text),
        _ => Err(format!("{field} must be a non-empty string")),
    }
}

fn tenants(value: Value) -> Result<Vec<Tenant>, String> {
    let shape = "tenants must be an array of {\"name\": ..., \"token\": ...} objects";
    let Value::Array(entries) = value else {
        return Err(shape.into());
    };
    let mut tenants: Vec<Tenant> = Vec::with_capacity(entries.len());
    for (index, entry) in entries.into_iter().enumerate() {
        let fail = |message: &str| format!("tenants[{index}]: {message}");
        let Value::Object(mut fields) = entry else {
            return Err(fail(shape));
        };
        let (Some(Value::String(name)), Some(Value::String(token)), true) =
            (fields.remove("name"), fields.remove("token"), fields.is_empty())
        else {
            return Err(fail("needs exactly a string \"name\" and a string \"token\""));
        };
        check_tenant_name(&name).map_err(fail)?;
        let token = Token::new(token).map_err(fail)?;
        if tenants.iter().any(|tenant| tenant.name == name) {
            return Err(fail("two tenants share a name"));
        }
        if tenants.iter().any(|tenant| tenant.token == token) {
            return Err(fail("two tenants share a token"));
        }
        tenants.push(Tenant { name, token });
    }
    Ok(tenants)
}

fn check_tenant_name(name: &str) -> Result<(), &'static str> {
    if name.is_empty() || name.len() > MAX_TENANT_BYTES {
        return Err("tenant names must be 1 to 255 UTF-8 bytes");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    #[test]
    fn debug_never_shows_tokens() {
        let parse = |token: &str| {
            super::parse(&format!(r#"{{"tenants": [{{"name": "a", "token": "{token}"}}]}}"#))
        };
        let secret = "secret-token-0123456789abcdefghijkl";
        assert!(!format!("{:?}", parse(secret).unwrap()).contains(secret), "Debug redacts tokens");
        for (bytes, ok) in [(31, false), (32, true), (1024, true), (1025, false)] {
            let token = "t".repeat(bytes);
            let error = parse(&token).err().unwrap_or_default();
            assert_eq!(error.is_empty(), ok, "{bytes}-byte token: {error}");
            let named = error.contains("32 to 1024 bytes") && !error.contains(&token);
            assert!(ok || named, "{bytes}: the error names the rule, not the token: {error}");
        }
    }
}
