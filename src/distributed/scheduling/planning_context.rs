use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use event_listener::Event;
use futures::StreamExt;
use smol::LocalExecutor;

use crate::{OutputStream, Value, VarName};

#[derive(Debug, Clone, Default)]
pub struct PlanningContextSnapshot {
    pub version: usize,
    pub step: Option<usize>,
    pub latest_bindings: BTreeMap<VarName, Value>,
    pub history: BTreeMap<usize, BTreeMap<VarName, Value>>,
}

#[derive(Debug, Clone, Default)]
pub struct PlanningContext {
    inner: Rc<RefCell<PlanningContextState>>,
}

#[derive(Debug)]
struct PlanningContextState {
    latest_bindings: BTreeMap<VarName, Value>,
    history: BTreeMap<usize, BTreeMap<VarName, Value>>,
    step: Option<usize>,
    next_step: usize,
    version: usize,
    record_history: bool,
    change_event: Rc<Event>,
}

impl Default for PlanningContextState {
    fn default() -> Self {
        Self {
            latest_bindings: BTreeMap::new(),
            history: BTreeMap::new(),
            step: None,
            next_step: 0,
            version: 0,
            record_history: false,
            change_event: Rc::new(Event::new()),
        }
    }
}

impl PlanningContext {
    pub fn new(record_history: bool) -> Self {
        let state = PlanningContextState {
            change_event: Rc::new(Event::new()),
            record_history,
            ..PlanningContextState::default()
        };
        Self {
            inner: Rc::new(RefCell::new(state)),
        }
    }

    pub fn from_history(history: BTreeMap<usize, BTreeMap<VarName, Value>>) -> Self {
        let step = history.keys().max().copied();
        let next_step = step.map(|step| step.saturating_add(1)).unwrap_or(0);
        let mut latest_bindings = BTreeMap::new();
        for row in history.values() {
            for (var, value) in row {
                if !matches!(value, Value::NoVal | Value::Deferred) {
                    latest_bindings.insert(var.clone(), value.clone());
                }
            }
        }

        Self {
            inner: Rc::new(RefCell::new(PlanningContextState {
                latest_bindings,
                history,
                step,
                next_step,
                version: next_step,
                record_history: true,
                change_event: Rc::new(Event::new()),
            })),
        }
    }

    pub fn snapshot(&self) -> PlanningContextSnapshot {
        let inner = self.inner.borrow();
        PlanningContextSnapshot {
            version: inner.version,
            step: inner.step,
            latest_bindings: inner.latest_bindings.clone(),
            history: inner.history.clone(),
        }
    }

    pub fn version(&self) -> usize {
        self.inner.borrow().version
    }

    pub fn record_value(&self, var: VarName, value: Value) {
        self.record_batch([(var, value)]);
    }

    pub fn record_batch(&self, values: impl IntoIterator<Item = (VarName, Value)>) {
        let values = values.into_iter().collect::<BTreeMap<_, _>>();
        if values.is_empty() {
            return;
        }

        let mut inner = self.inner.borrow_mut();
        let step = inner.next_step;

        if inner.record_history {
            inner.history.insert(step, values.clone());
        }

        let mut has_value = false;
        for (var, value) in values {
            if !matches!(value, Value::NoVal | Value::Deferred) {
                inner.latest_bindings.insert(var, value);
                has_value = true;
            }
        }

        if has_value {
            inner.step = Some(step);
            inner.next_step = inner.next_step.saturating_add(1);
        } else if inner.record_history {
            inner.step = Some(step);
            inner.next_step = inner.next_step.saturating_add(1);
        }
        inner.version = inner.version.saturating_add(1);
        inner.change_event.notify(usize::MAX);
    }

    pub async fn wait_for_update_since(&self, version: usize) {
        loop {
            let listener = {
                let inner = self.inner.borrow();
                if inner.version != version {
                    return;
                }
                inner.change_event.listen()
            };

            listener.await;
        }
    }
}

pub fn spawn_planning_context_recorder(
    executor: Rc<LocalExecutor<'static>>,
    context: PlanningContext,
    mut batches: OutputStream<Vec<(VarName, Value)>>,
) -> smol::Task<()> {
    executor.spawn(async move {
        while let Some(batch) = batches.next().await {
            context.record_batch(batch);
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_batch_preserves_one_history_step_per_batch() {
        let context = PlanningContext::new(true);

        context.record_batch([
            (VarName::new("x"), Value::Int(1)),
            (VarName::new("y"), Value::Int(2)),
        ]);

        let snapshot = context.snapshot();
        assert_eq!(snapshot.version, 1);
        assert_eq!(snapshot.step, Some(0));
        assert_eq!(snapshot.history.len(), 1);
        assert_eq!(
            snapshot.history.get(&0),
            Some(&BTreeMap::from([
                (VarName::new("x"), Value::Int(1)),
                (VarName::new("y"), Value::Int(2)),
            ]))
        );
        assert_eq!(
            snapshot.latest_bindings.get(&VarName::new("x")),
            Some(&Value::Int(1))
        );
        assert_eq!(
            snapshot.latest_bindings.get(&VarName::new("y")),
            Some(&Value::Int(2))
        );
    }
}
