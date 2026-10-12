#[cfg(feature = "redis")]
mod input_stream;
#[cfg(feature = "redis")]
mod knowledge;

pub use super::redis_config::{
    RedisKnowledgeConfig, decode_redis_knowledge_value, redis_keyspace_channel,
};
#[cfg(feature = "redis")]
pub use input_stream::input_stream;
#[cfg(feature = "redis")]
pub(crate) use input_stream::{RedisInputControl, RedisInputItem, open_owned_input_stream_items};
#[cfg(feature = "redis")]
pub(crate) use knowledge::{RedisKnowledgeInputControl, open_value_redis_knowledge};

#[cfg(feature = "redis")]
pub(crate) fn validate_input_format(format: &crate::core::FormatId) -> anyhow::Result<()> {
    anyhow::ensure!(
        matches!(format.as_str(), "json" | "json5"),
        "Redis input format `{format}` is unsupported; expected `json` or `json5`"
    );
    Ok(())
}
