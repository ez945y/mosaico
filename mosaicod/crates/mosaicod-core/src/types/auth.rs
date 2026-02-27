use crc32fast::Hasher;
use std::{ops::BitOr, str::FromStr};

#[derive(thiserror::Error, Debug)]
pub enum ApiKeyError {
    #[error("the api key is incomplete")]
    IncompleteApiKey,

    #[error("bad header")]
    BadHeader,

    #[error("bad payload")]
    BadPayload,

    #[error("bad checksum")]
    BadChecksum,

    #[error("checksum mismatch")]
    ChecksumMismatch,
}

type Payload = [u8; ApiKey::PAYLOAD_LENGTH];
type Checksum = [u8; ApiKey::CHECKSUM_LENGTH];

/// Mosaico API Key.
#[derive(PartialEq, Debug)]
pub struct ApiKey {
    payload: Payload,
    checksum: Checksum,
}

fn compute_checksum(payload: &Payload) -> Checksum {
    let mut hasher = Hasher::new();
    hasher.update(payload);
    let hash = hasher.finalize();

    format!("{:04x}", hash % 0xFFFF)
        .as_bytes()
        .try_into()
        .unwrap()
}

impl ApiKey {
    /// Header included in the token
    pub const HEADER: &str = "msco";

    /// Number of characters used to generate the key payload
    const PAYLOAD_LENGTH: usize = 32;
    const CHECKSUM_LENGTH: usize = 4;

    const SEPARATOR: &str = "_";

    /// Generates a new random API key
    pub fn new() -> Self {
        // Use of `.unwrap()` since we are creating a string of known size with alphanumeric chars
        let payload: Payload = crate::random::alphanumeric(ApiKey::PAYLOAD_LENGTH)
            .to_lowercase()
            .as_bytes()
            .try_into()
            .unwrap();

        Self {
            checksum: compute_checksum(&payload),
            payload: payload,
        }
    }
}

impl FromStr for ApiKey {
    type Err = ApiKeyError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(ApiKey::SEPARATOR).collect();

        if parts.len() != 3 {
            return Err(ApiKeyError::IncompleteApiKey);
        }

        let (header, payload, checksum) = (parts[0], parts[1], parts[2]);

        if header != ApiKey::HEADER {
            return Err(ApiKeyError::BadHeader);
        }

        let payload_size_ok = payload.chars().count() == ApiKey::PAYLOAD_LENGTH;
        let checksum_size_ok = checksum.chars().count() == ApiKey::CHECKSUM_LENGTH;

        let payload_is_alphanumeric: bool = payload
            .chars()
            .all(|c| c.is_ascii_digit() || (c.is_ascii_alphabetic() && c.is_lowercase()));

        let checksum_is_alphanumeric: bool = checksum
            .chars()
            .all(|c| c.is_ascii_digit() || (c.is_ascii_alphabetic() && c.is_lowercase()));

        if !(payload_size_ok && payload_is_alphanumeric) {
            return Err(ApiKeyError::BadPayload);
        }

        if !(checksum_size_ok && checksum_is_alphanumeric) {
            return Err(ApiKeyError::BadChecksum);
        }

        // We use `.unwrap()` since we have already checked above
        let payload: Payload = payload.as_bytes().try_into().unwrap();
        let checksum: Checksum = checksum.as_bytes().try_into().unwrap();

        if checksum != compute_checksum(&payload) {
            return Err(ApiKeyError::ChecksumMismatch);
        }

        Ok(Self { payload, checksum })
    }
}

impl std::fmt::Display for ApiKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{header}{separator}{payload}{separator}{checksum}",
            header = ApiKey::HEADER,
            payload = std::str::from_utf8(&self.payload).unwrap(),
            checksum = std::str::from_utf8(&self.checksum).unwrap(),
            separator = ApiKey::SEPARATOR,
        )
    }
}

#[derive(PartialEq)]
pub struct Permission(u8);

impl Permission {
    pub const READ: Self = Self(0b0000_0001);
    pub const WRITE: Self = Self(0b0000_0010);
    pub const DELETE: Self = Self(0b0000_0100);
    pub const MANAGE: Self = Self(0b0000_1000);

    /// Creates a new permission scope from a set of permissions.
    ///
    /// # Example
    /// ```
    /// use mosaicod_core::types::Permission;
    ///
    /// fn main(){
    ///     let perm = Permission::new(Permission::READ | Permission::WRITE);
    /// }
    /// ```
    pub fn new(perm: Permission) -> Self {
        Self(perm.0)
    }

    /// Adds new permissions
    ///
    /// # Example
    /// ```
    /// use mosaicod_core::types::Permission;
    ///
    /// fn main(){
    ///     let mut perm = Permission::default();
    ///     assert!(!perm.has(Permission::MANAGE));
    ///     perm = perm.add(Permission::MANAGE);
    ///     assert!(perm.has(Permission::MANAGE));
    /// }
    /// ```
    pub fn add(&self, permission: Permission) -> Permission {
        Self(self.0 | permission.0)
    }

    /// Removes permissions
    ///
    /// # Example
    /// ```
    /// use mosaicod_core::types::Permission;
    ///
    /// fn main(){
    ///     let perm = Permission::new(Permission::WRITE | Permission::READ);
    ///     let perm = perm.remove(Permission::WRITE);
    ///     assert!(!perm.has(Permission::WRITE));
    /// }
    /// ```
    pub fn remove(&self, permission: Permission) -> Permission {
        Self(self.0 & !permission.0)
    }

    /// Checks if the current permission has the `target` permissions
    ///
    /// # Example
    /// ```
    /// use mosaicod_core::types::Permission;
    ///
    /// fn main(){
    ///     let perm = Permission::new(Permission::READ | Permission::WRITE);
    ///     assert!(perm.has(Permission::READ));
    ///     assert!(perm.has(Permission::WRITE));
    ///     assert!(!perm.has(Permission::MANAGE));
    /// }
    /// ```
    pub fn has(&self, target: Permission) -> bool {
        target.0 & self.0 == target.0
    }

    /// Check if the current permission is empty (i.e. has no permissions set)
    ///
    /// # Example
    /// ```
    /// use mosaicod_core::types::Permission;
    ///
    /// fn main(){
    ///     let perm = Permission::default();
    ///     assert!(perm.is_empty());
    /// }
    /// ```
    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }
}

impl Default for Permission {
    /// Returns an empty permission
    fn default() -> Self {
        Self(0)
    }
}

impl BitOr for Permission {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self::Output {
        Self(rhs.0 | self.0)
    }
}

/// Represent an api key scope. The scope is composed of:
/// * an API Key like `msco:0938n8b37r378brf`
/// * the associated permissions (like: read, write, ..)
/// * a description to keep track of the purpose of the key
pub struct AuthScope {
    key: ApiKey,

    /// Permissions associated with the scope
    pub permission: Permission,

    /// Description to keep track of the purpose of the key
    pub description: String,
}

impl AuthScope {
    /// Create a new API key scope
    ///
    /// # Example
    /// ```
    /// use mosaicod_core::types::{ApiKeyScope, Permission};
    ///
    /// fn main(){
    ///     // Single permission
    ///     let scope = ApiKeyScope::new(Permission::READ, "dummy key".to_owned());
    ///
    ///     // Multiple permissions
    ///     let scope = ApiKeyScope::new(
    ///         Permission::READ | Permission::WRITE,
    ///         "dummy key".to_owned(),
    ///     );
    /// }
    pub fn new(permission: Permission, description: String) -> Self {
        Self {
            key: ApiKey::new(),
            permission,
            description,
        }
    }

    /// Get the scope api key
    pub fn key(&self) -> &ApiKey {
        &self.key
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permissions() {
        let perm = Permission::new(Permission::READ | Permission::WRITE);

        assert!(perm.has(Permission::READ | Permission::WRITE));
        assert!(perm.has(Permission::READ));
        assert!(perm.has(Permission::WRITE));

        let perm = Permission::new(Permission::MANAGE);
        assert!(perm.has(Permission::MANAGE));
        assert!(!perm.has(Permission::READ));
        assert!(!perm.has(Permission::WRITE));
        assert!(!perm.has(Permission::DELETE));

        let mut perm = Permission::new(Permission::READ | Permission::WRITE);
        perm = perm.add(Permission::MANAGE);
        assert!(perm.has(Permission::READ | Permission::WRITE | Permission::MANAGE),);
    }

    #[test]
    fn api_key_create_and_parse() {
        let key = ApiKey::new();
        dbg!(&key.to_string());

        let key_str = key.to_string();

        let _: ApiKey = key_str.parse().expect("Error parsing API key");
    }

    #[test]
    fn bad_apy_key() {
        let res: Result<ApiKey, ApiKeyError> =
            "mosaico_gm8osbmxriljmgkyeb7aybirba4jeysw_e2c2".parse();
        assert!(matches!(res, Err(ApiKeyError::BadHeader)));

        // Removed char in payload
        let res: Result<ApiKey, ApiKeyError> = "msco_m8osbmxriljmgkyeb7aybirba4jeysw_e2c2".parse();
        assert!(matches!(res, Err(ApiKeyError::BadPayload)));

        // e -> E in checksum
        let res: Result<ApiKey, ApiKeyError> = "msco_gm8osbmxriljmgkyeb7aybirba4jeysw_E2c2".parse();
        dbg!(&res);
        assert!(matches!(res, Err(ApiKeyError::BadChecksum)));

        // Removed char from checksum
        let res: Result<ApiKey, ApiKeyError> = "msco_gm8osbmxriljmgkyeb7aybirba4jeysw_e2c".parse();
        dbg!(&res);
        assert!(matches!(res, Err(ApiKeyError::BadChecksum)));

        // Changed checksum
        let res: Result<ApiKey, ApiKeyError> = "msco_gm8osbmxriljmgkyeb7aybirba4jeysw_e2c3".parse();
        assert!(matches!(res, Err(ApiKeyError::ChecksumMismatch)));
    }
}
