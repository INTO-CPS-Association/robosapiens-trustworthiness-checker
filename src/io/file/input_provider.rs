use crate::core::{InputProvider, OutputStream, Value, VarName};
use crate::io::replay_history::{ReplayHistory, ReplaySnapshot};
pub use crate::lang::untimed_input::UntimedInputFileData;
use async_trait::async_trait;
use futures::{StreamExt, stream};
use std::collections::{BTreeMap, BTreeSet};

/// Returns an iterator over the values for a given key in the UntimedInputFileData.
/// None if no keys are present.
fn input_file_data_iter(
    data: UntimedInputFileData,
    key: VarName,
) -> Box<dyn Iterator<Item = Value> + 'static> {
    let max_index = data.keys().max();
    if let Some(max_key) = max_index {
        Box::new((0..=*max_key).map(move |time| {
            data.get(&time)
                .and_then(|data_for_time| data_for_time.get(&key).cloned())
                .unwrap_or(Value::NoVal)
        }))
    } else {
        Box::new(std::iter::empty())
    }
}

/// Configurable file-backed input provider with pluggable replay-history strategy.
///
/// `ReplayHistory::StoreAll` stores a deterministic replay snapshot,
/// while `ReplayHistory::Disabled` disables history recording entirely.
#[derive(Clone, Debug)]
pub struct FileInputProvider {
    data: UntimedInputFileData,
    replay_history: ReplayHistory,
}

impl FileInputProvider {
    /// Construct a provider with replay history disabled by default.
    pub fn new(data: UntimedInputFileData) -> Self {
        Self::with_replay_history(data, ReplayHistory::disabled())
    }

    /// Construct a provider with an explicit replay-history strategy.
    pub fn with_replay_history(data: UntimedInputFileData, replay_history: ReplayHistory) -> Self {
        let provider = Self {
            data,
            replay_history: replay_history.clone(),
        };

        // Seed/refresh active history snapshot from the full input data once.
        // Null strategy intentionally does nothing.
        if replay_history.is_store_all() {
            replay_history.clear();
            for (_t, row) in provider.data.iter() {
                replay_history.record_row(row.clone());
            }
        }

        provider
    }

    /// Construct a provider with seeded replay snapshot and additional file data.
    ///
    /// Useful when integrating with distributed runtime replay seeding.
    pub fn with_seeded_history(
        data: UntimedInputFileData,
        seeded_snapshot: ReplaySnapshot,
    ) -> Self {
        let replay_history = ReplayHistory::store_all_with_snapshot(seeded_snapshot);
        // Append rows from file data after seeded history.
        for (_t, row) in data.iter() {
            replay_history.record_row(row.clone());
        }

        Self {
            data,
            replay_history,
        }
    }

    pub fn data(&self) -> &UntimedInputFileData {
        &self.data
    }
}

#[async_trait(?Send)]
impl InputProvider for FileInputProvider {
    type Val = Value;

    fn var_stream(&mut self, var: &VarName) -> Option<OutputStream<Value>> {
        let iter = input_file_data_iter(self.data.clone(), var.clone());
        Some(Box::pin(stream::iter(iter)))
    }

    async fn control_stream(&mut self) -> OutputStream<anyhow::Result<()>> {
        let max_index = self.data.keys().max();
        if let Some(max_key) = max_index {
            stream::repeat_with(|| Ok(()))
                .take(*max_key + 1)
                .boxed_local()
        } else {
            stream::empty().boxed_local()
        }
    }

    fn replay_history(&self) -> Option<BTreeMap<usize, BTreeMap<VarName, Value>>> {
        self.replay_history.snapshot()
    }

    fn replay_history_handle(&self) -> Option<ReplayHistory> {
        Some(self.replay_history.clone())
    }
}

/// Backward-compatible InputProvider implementation for the legacy data type.
///
/// This preserves existing call sites that pass `UntimedInputFileData` directly,
/// while defaulting to disabled replay-history behavior.
#[async_trait(?Send)]
impl InputProvider for UntimedInputFileData {
    type Val = Value;

    fn var_stream(&mut self, var: &VarName) -> Option<OutputStream<Value>> {
        FileInputProvider::new(self.clone()).var_stream(var)
    }

    async fn control_stream(&mut self) -> OutputStream<anyhow::Result<()>> {
        FileInputProvider::new(self.clone()).control_stream().await
    }

    fn replay_history(&self) -> Option<BTreeMap<usize, BTreeMap<VarName, Value>>> {
        FileInputProvider::new(self.clone()).replay_history()
    }

    fn replay_history_handle(&self) -> Option<ReplayHistory> {
        FileInputProvider::new(self.clone()).replay_history_handle()
    }
}

pub fn replay_history_for_vars(
    data: &UntimedInputFileData,
    vars: &[VarName],
) -> UntimedInputFileData {
    let var_set = vars.iter().cloned().collect::<BTreeSet<_>>();
    data.iter()
        .map(|(t, row)| {
            let filtered: BTreeMap<VarName, Value> = row
                .iter()
                .filter(|(name, _)| var_set.contains(*name))
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect();
            (*t, filtered)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::async_test;
    use futures::StreamExt;
    use macro_rules_attribute::apply;
    use std::collections::BTreeMap;

    use super::*;
    use crate::core::Value;
    use test_log::test;

    fn sample_data() -> UntimedInputFileData {
        let mut data: UntimedInputFileData = BTreeMap::new();
        data.insert(0, {
            let mut map = BTreeMap::new();
            map.insert("x".into(), Value::Int(1));
            map.insert("y".into(), Value::Int(10));
            map
        });
        data.insert(1, {
            let mut map = BTreeMap::new();
            map.insert("x".into(), Value::Int(2));
            map.insert("y".into(), Value::Int(11));
            map
        });
        data
    }

    #[test]
    fn default_history_is_disabled() {
        let provider = FileInputProvider::new(sample_data());
        assert!(provider.replay_history().is_none());
    }

    #[test]
    fn store_all_history_records_snapshot() {
        let provider =
            FileInputProvider::with_replay_history(sample_data(), ReplayHistory::store_all());
        let snap = provider.replay_history().unwrap();
        assert_eq!(snap.len(), 2);
        assert_eq!(snap.get(&0).unwrap().get(&"x".into()), Some(&Value::Int(1)));
        assert_eq!(
            snap.get(&1).unwrap().get(&"y".into()),
            Some(&Value::Int(11))
        );
    }

    #[test]
    fn null_history_returns_none() {
        let provider =
            FileInputProvider::with_replay_history(sample_data(), ReplayHistory::disabled());
        assert!(provider.replay_history().is_none());
    }

    #[apply(async_test)]
    async fn stream_output_matches_data() {
        let mut provider = FileInputProvider::new(sample_data());
        let x_stream = provider.var_stream(&"x".into()).unwrap();
        let vals = x_stream.collect::<Vec<_>>().await;
        assert_eq!(vals, vec![Value::Int(1), Value::Int(2)]);
    }

    #[test]
    fn replay_history_for_vars_filters() {
        let data = sample_data();
        let filtered = replay_history_for_vars(&data, &["x".into()]);
        assert_eq!(filtered.get(&0).unwrap().len(), 1);
        assert!(filtered.get(&0).unwrap().contains_key(&"x".into()));
    }
}
