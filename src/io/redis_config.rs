use std::collections::BTreeMap;

use crate::io::RetryPolicy;
use anyhow::anyhow;

use crate::core::{JsonStreamValue, Value, VarName};

/// Source-owned Redis knowledge configuration.
///
/// The `keys` catalog maps checker input variables to exact Redis keys. The
/// source never derives a key implicitly from a variable name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RedisKnowledgeConfig {
    pub host: String,
    pub port: Option<u16>,
    pub database: u32,
    pub publish_initial: bool,
    pub keys: BTreeMap<VarName, String>,
    pub retry: RetryPolicy,
}

impl RedisKnowledgeConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.host.trim().is_empty(),
            "Redis knowledge host cannot be empty"
        );
        anyhow::ensure!(
            !self.host.contains(['@', '/', '?', '#']),
            "Redis knowledge host must be a hostname or address, not a credential-bearing URL"
        );
        anyhow::ensure!(
            !self.keys.is_empty(),
            "Redis knowledge source must declare at least one key mapping"
        );

        let mut keys = BTreeMap::<&str, &VarName>::new();
        for (variable, key) in &self.keys {
            let variable_name = variable.name();
            anyhow::ensure!(
                !variable_name.trim().is_empty(),
                "Redis knowledge model variable name cannot be empty"
            );
            anyhow::ensure!(
                !key.trim().is_empty(),
                "Redis knowledge key for `{variable}` cannot be empty"
            );
            if let Some(previous) = keys.insert(key, variable) {
                anyhow::bail!(
                    "Redis knowledge key `{key}` is mapped to both `{previous}` and `{variable}`"
                );
            }
        }
        Ok(())
    }
}

/// Construct the exact Redis keyspace-notification channel for a selected key.
/// Keyspace notifications are invalidations; the key's current value is read
/// separately after a notification arrives.
pub fn redis_keyspace_channel(database: u32, key: &str) -> String {
    format!("__keyspace@{database}__:{key}")
}

/// Decode one selected Redis key state.
///
/// `None` is deliberately handled before decoding so a missing key becomes
/// `Value::NoVal`, while a stored JSON `null` follows the existing `Value`
/// decoder and becomes `Value::Unit`.
pub fn decode_redis_knowledge_value(key: &str, payload: Option<&[u8]>) -> anyhow::Result<Value> {
    let Some(payload) = payload else {
        return Ok(Value::NoVal);
    };

    match Value::decode_json(payload) {
        Ok(value) => Ok(value),
        Err(json_error) => match std::str::from_utf8(payload) {
            Ok(text) => Ok(Value::Str(text.into())),
            Err(utf8_error) => Err(anyhow!(utf8_error).context(format!(
                "Redis knowledge key `{key}` contains invalid UTF-8 after JSON5 decoding failed: {json_error}"
            ))),
        },
    }
}
