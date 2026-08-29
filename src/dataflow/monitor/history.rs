use super::super::ReconfigurationMapping;
use super::super::environment::EnvironmentSlot;
use super::DataflowMonitor;
use crate::core::Value;

impl DataflowMonitor {
    pub(super) fn recompute_history_requirements(&mut self) {
        let static_requirements = self.program.history_requirements();
        for (index, depth) in self.effective_history_depths.iter_mut().enumerate() {
            *depth = static_requirements.depth(EnvironmentSlot::new(index));
        }

        let execution = &self.execution;
        let effective_depths = &mut self.effective_history_depths;
        execution.for_each_active_body_history_requirement(&mut |requirement| {
            if let Some(depth) = effective_depths.get_mut(requirement.slot().index()) {
                *depth = (*depth).max(requirement.depth());
            }
        });

        for index in 0..self.effective_history_depths.len() {
            let depth = self.effective_history_depths[index];
            match (self.history_bindings[index], depth) {
                (Some(history_id), 0) => {
                    drop(self.history_store.take(history_id));
                    self.history_bindings[index] = None;
                }
                (Some(history_id), depth) => {
                    self.history_store.set_required_depth(history_id, depth);
                }
                (None, depth) if depth > 0 => {
                    let history_id = self.history_store.allocate(depth);
                    self.history_bindings[index] = Some(history_id);
                }
                (None, _) => {}
            }
        }
    }

    pub(super) fn commit_histories(&mut self) {
        if self.history_store.is_empty() {
            return;
        }
        for slot in 0..self.history_bindings.len() {
            let Some(history_id) = self.history_bindings[slot] else {
                continue;
            };
            let value = std::mem::replace(&mut self.environment_values[slot], Value::NoVal);
            self.history_store.commit(history_id, value);
        }
    }

    pub(super) fn transfer_histories_from(
        &mut self,
        source: &mut DataflowMonitor,
        mapping: &ReconfigurationMapping,
    ) -> Vec<crate::VarName> {
        let mut retained = Vec::new();
        for target_index in 0..self.history_bindings.len() {
            let target_slot = EnvironmentSlot::new(target_index);
            let Some(target_id) = self.history_bindings[target_index] else {
                continue;
            };
            if self.history_store[target_id].required_depth() == 0 {
                continue;
            }
            let Some(source_slot) = mapping
                .environment(target_slot)
                .and_then(|mapping| mapping.source())
            else {
                continue;
            };
            let Some(source_id) = source
                .history_bindings
                .get(source_slot.index())
                .copied()
                .flatten()
            else {
                continue;
            };
            let source_is_useful = source
                .history_store
                .get(source_id)
                .is_some_and(|history| history.required_depth() > 0);
            if !source_is_useful {
                continue;
            }

            self.history_store
                .replace_from(target_id, &mut source.history_store, source_id);
            source.history_bindings[source_slot.index()] = None;
            if let Some(variable) = self.program.environment_layout().variable(target_slot) {
                retained.push(variable.clone());
            }
        }
        retained
    }
}
