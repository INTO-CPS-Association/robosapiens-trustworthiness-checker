use std::collections::BTreeMap;

use crate::VarName;

pub type TopicMapping = BTreeMap<VarName, String>;

pub mod builders;
pub mod cli;
pub mod file;
pub mod map;
pub mod mqtt;
pub mod replay_history;
#[cfg(feature = "ros")]
pub mod ros;
pub mod testing;
pub use self::builders::InputProviderBuilder;
pub mod redis;
