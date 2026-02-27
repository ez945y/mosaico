use mosaicod_core::types;

struct AuthScopeRecord {
    /// Unique identifier of the auth scope
    pub auth_scope_id: i32,

    pub(crate) api_key: String,

    pub(crate) permissions: u8,

    /// Auth scope description
    pub description: String,

    /// UNIX timestamp in milliseconds since the creation
    pub(crate) creation_unix_timestamp: i64,

    /// UNIX timestamp in milliseconds of the expiration date
    pub(crate) expiration_unix_timestyamp: i64,
}

impl AuthScopeRecord {
    pub fn creation_timestamp(&self) -> types::Timestamp {
        self.creation_unix_timestamp.into()
    }

    pub fn expiration_timestamp(&self) -> types::Timestamp {
        self.expiration_unix_timestyamp.into()
    }

    pub fn permission(&self) -> types::Permission {
        self.permissions.into()
    }

    pub fn api_key(&self) -> Result<types::ApiKey, types::ApiKeyError> {
        self.api_key.parse()
    }
}
