use std::{collections::BTreeMap, num::NonZeroU32, time::Duration};

use anyhow::anyhow;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::core::{JsonStreamValue, Value, VarName};

const DEFAULT_INITIAL_DELAY: Duration = Duration::from_millis(250);
const DEFAULT_MAX_DELAY: Duration = Duration::from_secs(5);

/// Retry settings for a long-lived Redis knowledge-state source.
///
/// `max_attempts` counts the initial attempt. `None` means retry forever. The
/// delays are capped exponential backoff delays and must both be non-zero, with
/// `max_delay` at least as large as `initial_delay`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RedisKnowledgeRetry {
    pub max_attempts: Option<NonZeroU32>,
    pub initial_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RedisKnowledgeRetry {
    fn default() -> Self {
        Self {
            max_attempts: None,
            initial_delay: DEFAULT_INITIAL_DELAY,
            max_delay: DEFAULT_MAX_DELAY,
        }
    }
}

impl RedisKnowledgeRetry {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.initial_delay.is_zero(),
            "Redis knowledge retry `initial_delay` must be greater than zero"
        );
        anyhow::ensure!(
            !self.max_delay.is_zero(),
            "Redis knowledge retry `max_delay` must be greater than zero"
        );
        anyhow::ensure!(
            self.max_delay >= self.initial_delay,
            "Redis knowledge retry `max_delay` must be at least `initial_delay`"
        );
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RedisKnowledgeRetryWire {
    #[serde(default)]
    max_attempts: Option<NonZeroU32>,
    #[serde(default = "default_initial_delay_ms")]
    initial_delay_ms: u64,
    #[serde(default = "default_max_delay_ms")]
    max_delay_ms: u64,
}

fn default_initial_delay_ms() -> u64 {
    DEFAULT_INITIAL_DELAY.as_millis() as u64
}

fn default_max_delay_ms() -> u64 {
    DEFAULT_MAX_DELAY.as_millis() as u64
}

impl<'de> Deserialize<'de> for RedisKnowledgeRetry {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = RedisKnowledgeRetryWire::deserialize(deserializer)?;
        let retry = Self {
            max_attempts: wire.max_attempts,
            initial_delay: Duration::from_millis(wire.initial_delay_ms),
            max_delay: Duration::from_millis(wire.max_delay_ms),
        };
        retry.validate().map_err(serde::de::Error::custom)?;
        Ok(retry)
    }
}

impl Serialize for RedisKnowledgeRetry {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Wire<'a> {
            max_attempts: &'a Option<NonZeroU32>,
            initial_delay_ms: u64,
            max_delay_ms: u64,
        }

        Wire {
            max_attempts: &self.max_attempts,
            initial_delay_ms: self.initial_delay.as_millis() as u64,
            max_delay_ms: self.max_delay.as_millis() as u64,
        }
        .serialize(serializer)
    }
}

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
    pub retry: RedisKnowledgeRetry,
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
        self.retry.validate()?;
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
