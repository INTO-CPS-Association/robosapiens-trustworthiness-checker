mod input_stream;
mod knowledge;

pub use super::redis_config::{
    RedisKnowledgeConfig, decode_redis_knowledge_value, redis_keyspace_channel,
};
pub use input_stream::input_stream;
pub(crate) use input_stream::{RedisInputControl, RedisInputItem, open_owned_input_stream_items};
pub(crate) use knowledge::{RedisKnowledgeInputControl, open_value_redis_knowledge};

pub(crate) fn validate_input_format(format: &crate::core::FormatId) -> anyhow::Result<()> {
    anyhow::ensure!(
        matches!(format.as_str(), "json" | "json5"),
        "Redis input format `{format}` is unsupported; expected `json` or `json5`"
    );
    Ok(())
}
