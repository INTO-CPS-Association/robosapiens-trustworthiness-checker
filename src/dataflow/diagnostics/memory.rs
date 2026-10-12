//! Structured memory traversal of dataflow owners.
//!
//! Each owner is charged its fixed storage from capacities and layouts, plus the estimated payload
//! of the values it retains (see [`crate::diagnostics::ValueSizer`]). Components are aggregated per
//! stream and category rather than per node, so a snapshot stays small for large specifications.
//!
//! | category | contents |
//! |:---------|:---------|
//! | `scratch` | per-node value scratch of a stream's canonical evaluator, overwritten every tick |
//! | `state` | retained operator state: delay rings, lifted operands, latches, retained branch values |
//! | `call_state` | persistent function-call, match-arm and branch evaluators nested in a stream |
//! | `dynamic_body` | the active `dynamic`/`defer` body evaluator and its projected environment |
//! | `history` | shared named-variable history rings |
//! | `environment` | the monitor's environment row and retained row |
//! | `program` | immutable graphs, plans and caches; shared, not per session |
//! | `accelerator` | quickened region registers and native artifacts, visible size only |

use std::mem::size_of;

use crate::core::Value;
use crate::dataflow::execution::evaluator::Evaluator;
use crate::dataflow::execution::evaluator_state::{EvaluatorState, NodeState};
use crate::diagnostics::{
    MemoryBudget, MemoryComponent, MemoryEstimate, MemorySnapshot, PayloadEstimate, SnapshotKind,
    ValueSizer,
};

/// Why a snapshot was taken.
#[derive(Clone, Debug)]
pub(in crate::dataflow) enum SnapshotPoint {
    Periodic,
    Final,
    BeforeReconfiguration,
    Explicit(String),
}

impl SnapshotPoint {
    pub(in crate::dataflow) fn kind(&self) -> SnapshotKind {
        match self {
            Self::Periodic => SnapshotKind::Periodic,
            Self::Final => SnapshotKind::Final,
            Self::BeforeReconfiguration => SnapshotKind::BeforeReconfiguration,
            Self::Explicit(_) => SnapshotKind::Explicit,
        }
    }

    pub(in crate::dataflow) fn label(&self) -> String {
        match self {
            Self::Periodic => "periodic".into(),
            Self::Final => "final".into(),
            Self::BeforeReconfiguration => "before_reconfiguration".into(),
            Self::Explicit(label) => label.clone(),
        }
    }
}

/// Deepest nesting of call, branch and body evaluators followed.
const MAX_NESTING: usize = 16;

#[derive(Default)]
struct StreamTotals {
    scratch: MemoryEstimate,
    state: MemoryEstimate,
    call_state: MemoryEstimate,
    dynamic_body: MemoryEstimate,
    nesting_truncated: bool,
    /// Evaluators or nodes not entered because the snapshot's work budget was spent.
    budget_skipped: bool,
    opaque_callables: u64,
    /// Function capture lists, whose contents are shared and mutable and so not traversed.
    opaque_captures: u64,
}

/// One snapshot under construction.
pub(in crate::dataflow) struct MemoryVisitor<'a> {
    snapshot: MemorySnapshot,
    sizer: ValueSizer<'a>,
}

impl<'a> MemoryVisitor<'a> {
    pub(in crate::dataflow) fn new(
        point: &SnapshotPoint,
        tick: u64,
        definition_version: u32,
        budget: MemoryBudget,
    ) -> Self {
        let mut snapshot = MemorySnapshot::new(point.label(), tick, budget);
        snapshot.kind = point.kind();
        snapshot.definition_version = Some(definition_version);
        Self {
            snapshot,
            sizer: ValueSizer::new(budget),
        }
    }

    pub(in crate::dataflow) fn finish(mut self) -> MemorySnapshot {
        self.snapshot.work = self.sizer.work();
        self.snapshot.budget_exhausted = self.sizer.exhausted();
        self.snapshot.depth_truncated = self.sizer.depth_truncated();
        self.snapshot.total.payload.truncated |=
            self.snapshot.budget_exhausted || self.snapshot.depth_truncated;
        self.snapshot
    }

    pub(in crate::dataflow) fn push(
        &mut self,
        path: impl Into<String>,
        category: &'static str,
        stream: Option<&str>,
        estimate: MemoryEstimate,
    ) {
        self.snapshot.push(MemoryComponent {
            path: path.into(),
            category,
            stream: stream.map(str::to_owned),
            estimate,
            opaque: false,
            note: None,
        });
    }

    pub(in crate::dataflow) fn push_opaque(
        &mut self,
        path: impl Into<String>,
        category: &'static str,
        stream: Option<&str>,
        estimate: MemoryEstimate,
        note: &'static str,
    ) {
        self.snapshot.push(MemoryComponent {
            path: path.into(),
            category,
            stream: stream.map(str::to_owned),
            estimate,
            opaque: true,
            note: Some(note),
        });
    }

    /// Charge a slice of value slots with `capacity` slots of fixed storage.
    pub(in crate::dataflow) fn slots(
        &mut self,
        capacity: usize,
        values: impl IntoIterator<Item = &'a Value>,
    ) -> MemoryEstimate {
        MemoryEstimate {
            fixed_bytes: (capacity * size_of::<Value>()) as u64,
            payload: self.sizer.values(values),
        }
    }

    /// Spend one unit to enter an owner outside the evaluators (a history ring, a row); `false`
    /// once the snapshot's work budget is spent.
    pub(in crate::dataflow) fn enter_owner(&mut self) -> bool {
        self.sizer.enter_owner()
    }

    fn payload(&mut self, values: impl IntoIterator<Item = &'a Value>) -> PayloadEstimate {
        self.sizer.values(values)
    }

    /// Charge one stream's canonical evaluator, aggregated by category.
    pub(in crate::dataflow) fn stream_evaluator(
        &mut self,
        stream: &str,
        evaluator: &'a Evaluator,
        native_owned: bool,
    ) {
        let prefix = format!("streams/{stream}");
        if !self.sizer.enter_owner() {
            self.push_opaque(
                prefix,
                "state",
                Some(stream),
                MemoryEstimate {
                    fixed_bytes: 0,
                    payload: PayloadEstimate {
                        truncated: true,
                        ..PayloadEstimate::default()
                    },
                },
                "the snapshot's work budget was spent before this stream was inspected",
            );
            return;
        }
        let mut totals = StreamTotals::default();
        self.evaluator_state(evaluator.canonical.as_ref(), 0, &mut totals, Target::Own);
        let note = native_owned.then_some(
            "a native tier owns this stream's state; the canonical copy shown may be stale",
        );
        for (name, category, estimate) in [
            ("scratch", "scratch", totals.scratch),
            ("state", "state", totals.state),
            ("call_state", "call_state", totals.call_state),
            ("dynamic_body", "dynamic_body", totals.dynamic_body),
        ] {
            if estimate == MemoryEstimate::default() {
                continue;
            }
            let mut estimate = estimate;
            estimate.payload.truncated |= totals.nesting_truncated || totals.budget_skipped;
            // Natively owned state is not materialized for observation, so the canonical copy is
            // reported as opaque rather than as an ordinary, fully inspected component.
            self.snapshot.push(MemoryComponent {
                path: format!("{prefix}/{name}"),
                category,
                stream: Some(stream.to_owned()),
                estimate,
                opaque: native_owned,
                note: note
                    .or(totals.budget_skipped.then_some(
                        "the snapshot's work budget was spent before every node was inspected",
                    ))
                    .or(totals.nesting_truncated.then_some(
                        "nested evaluators beyond the nesting bound were not traversed",
                    )),
            });
        }
        if totals.opaque_callables > 0 || totals.opaque_captures > 0 {
            let mut estimate = MemoryEstimate::default();
            estimate.payload.opaque_values = totals.opaque_callables + totals.opaque_captures;
            self.push_opaque(
                format!("{prefix}/callables"),
                "state",
                Some(stream),
                estimate,
                "host or language callables and the mutable capture lists of function values are \
                 opaque: their handles and capture slots are counted, their contents are not \
                 traversed",
            );
        }
    }

    fn evaluator_state(
        &mut self,
        state: &'a EvaluatorState,
        nesting: usize,
        totals: &mut StreamTotals,
        target: Target,
    ) {
        if nesting > MAX_NESTING {
            totals.nesting_truncated = true;
            return;
        }
        // Entering an evaluator costs a unit even when it holds no value.
        if !self.sizer.enter_owner() {
            totals.budget_skipped = true;
            return;
        }
        let scratch = MemoryEstimate {
            fixed_bytes: (size_of::<EvaluatorState>()
                + state.node_values.capacity() * size_of::<Value>())
                as u64,
            payload: self.payload(state.node_values.iter()),
        };
        let fixed_states =
            MemoryEstimate::fixed(state.node_states.capacity() * size_of::<NodeState>());
        match target {
            Target::Own => {
                totals.scratch.merge(&scratch);
                totals.state.merge(&fixed_states);
            }
            Target::Call => {
                totals.call_state.merge(&scratch);
                totals.call_state.merge(&fixed_states);
            }
            Target::Body => {
                totals.dynamic_body.merge(&scratch);
                totals.dynamic_body.merge(&fixed_states);
            }
        }
        for node in &state.node_states {
            // One unit per node state, so a long run of scalar or empty states is bounded.
            if !self.sizer.enter_owner() {
                totals.budget_skipped = true;
                break;
            }
            self.node_state(node, nesting, totals, target);
        }
    }

    fn charge(&self, totals: &mut StreamTotals, target: Target, estimate: MemoryEstimate) {
        match target {
            Target::Own => totals.state.merge(&estimate),
            Target::Call => totals.call_state.merge(&estimate),
            Target::Body => totals.dynamic_body.merge(&estimate),
        }
    }

    fn node_state(
        &mut self,
        node: &'a NodeState,
        nesting: usize,
        totals: &mut StreamTotals,
        target: Target,
    ) {
        let estimate = match node {
            NodeState::UnaryLift { last_input }
            | NodeState::Default { last_input }
            | NodeState::IsDefined { last_input }
            | NodeState::When { last_input, .. } => MemoryEstimate {
                fixed_bytes: 0,
                payload: self.payload(last_input.iter()),
            },
            NodeState::Latch { last_value } => MemoryEstimate {
                fixed_bytes: 0,
                payload: self.payload(last_value.iter()),
            },
            NodeState::BinaryLift {
                last_left,
                last_right,
            } => MemoryEstimate {
                fixed_bytes: 0,
                payload: self.payload(last_left.iter().chain(last_right.iter())),
            },
            NodeState::Update {
                last_base,
                last_update,
                ..
            } => MemoryEstimate {
                fixed_bytes: 0,
                payload: self.payload(last_base.iter().chain(last_update.iter())),
            },
            NodeState::OperandLift { last_operands } => MemoryEstimate {
                fixed_bytes: (last_operands.capacity() * size_of::<Option<Value>>()) as u64,
                payload: self.payload(last_operands.iter().flatten()),
            },
            NodeState::Delay(delay) => {
                let (capacity, values) = delay.retained();
                MemoryEstimate {
                    fixed_bytes: (capacity * size_of::<Value>()) as u64,
                    payload: self.payload(values),
                }
            }
            NodeState::ScalarDelay(delay) => {
                let (capacity, _) = delay.retained();
                MemoryEstimate::fixed(
                    capacity * size_of::<crate::dataflow::execution::quickening::ScalarValue>(),
                )
            }
            NodeState::ScalarDefault { .. } | NodeState::Init { .. } => MemoryEstimate::default(),
            NodeState::CallLift {
                last_function,
                last_arguments,
                active_function,
                callable,
            } => {
                totals.opaque_callables +=
                    u64::from(active_function.is_some()) + u64::from(callable.is_some());
                MemoryEstimate {
                    fixed_bytes: (last_arguments.capacity() * size_of::<Option<Value>>()) as u64,
                    payload: self
                        .payload(last_function.iter().chain(last_arguments.iter().flatten())),
                }
            }
            NodeState::Function { function, captures } => {
                totals.opaque_callables += u64::from(function.is_some());
                totals.opaque_captures += 1;
                // The capture list is shared with the function values built from it and is
                // rewritten while they run, so its contents are not traversed. A short, safe
                // borrow reads only how many slots it holds; one that is already borrowed
                // contributes no slot count and the estimate is marked truncated.
                let (slots, truncated) = match captures.try_borrow() {
                    Ok(captures) => (captures.capacity(), false),
                    Err(_) => (0, true),
                };
                MemoryEstimate {
                    fixed_bytes: (slots * size_of::<Value>()) as u64,
                    payload: PayloadEstimate {
                        truncated,
                        ..PayloadEstimate::default()
                    },
                }
            }
            NodeState::PersistentCall {
                evaluator,
                environment_values,
                last_arguments,
            } => {
                let row = MemoryEstimate {
                    fixed_bytes: (environment_values.capacity() * size_of::<Value>()
                        + last_arguments.capacity() * size_of::<Option<Value>>())
                        as u64,
                    payload: self.payload(
                        environment_values
                            .iter()
                            .chain(last_arguments.iter().flatten()),
                    ),
                };
                totals.call_state.merge(&row);
                self.evaluator_state(
                    evaluator.canonical.as_ref(),
                    nesting + 1,
                    totals,
                    Target::Call,
                );
                MemoryEstimate::default()
            }
            NodeState::Reconfigurable(expression) => {
                let own = MemoryEstimate {
                    fixed_bytes: (size_of::<
                        crate::dataflow::execution::evaluator_state::ReconfigurableExpressionState,
                    >() + expression.template_cache.capacity() * size_of::<usize>())
                        as u64,
                    payload: self.payload(
                        expression
                            .last_source_value
                            .iter()
                            .chain(expression.last_defer_result.iter()),
                    ),
                };
                let body_row = MemoryEstimate {
                    fixed_bytes: (expression.environment_values.capacity() * size_of::<Value>())
                        as u64,
                    payload: self.payload(expression.environment_values.iter()),
                };
                totals.dynamic_body.merge(&body_row);
                if let Some(active) = &expression.active_expression {
                    self.evaluator_state(
                        active.evaluator.canonical.as_ref(),
                        nesting + 1,
                        totals,
                        Target::Body,
                    );
                }
                own
            }
            NodeState::LazyIf(branches) => {
                let retained = MemoryEstimate {
                    fixed_bytes: 0,
                    payload: self.payload(
                        branches
                            .last_condition
                            .iter()
                            .chain(branches.last_then_value.iter())
                            .chain(branches.last_else_value.iter()),
                    ),
                };
                self.evaluator_state(&branches.then_state, nesting + 1, totals, Target::Call);
                self.evaluator_state(&branches.else_state, nesting + 1, totals, Target::Call);
                retained
            }
            NodeState::Match(arms) => {
                for arm in arms {
                    if !self.sizer.enter_owner() {
                        totals.budget_skipped = true;
                        break;
                    }
                    for call in arm.guard.iter().chain(std::iter::once(&arm.body)) {
                        let row = MemoryEstimate {
                            fixed_bytes: (call.environment_values.capacity() * size_of::<Value>())
                                as u64,
                            payload: self.payload(call.environment_values.iter()),
                        };
                        totals.call_state.merge(&row);
                        self.evaluator_state(
                            call.evaluator.canonical.as_ref(),
                            nesting + 1,
                            totals,
                            Target::Call,
                        );
                    }
                }
                MemoryEstimate::fixed(arms.capacity() * size_of::<usize>())
            }
        };
        self.charge(totals, target, estimate);
    }
}

#[derive(Clone, Copy)]
enum Target {
    Own,
    Call,
    Body,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::execution::evaluator_state::{EvaluatorState, NodeState};

    fn state_of_scalars(count: usize) -> EvaluatorState {
        EvaluatorState {
            node_values: Vec::new(),
            node_states: (0..count)
                .map(|index| {
                    if index % 2 == 0 {
                        NodeState::Init { started: false }
                    } else {
                        NodeState::ScalarDefault { last_input: None }
                    }
                })
                .collect(),
        }
    }

    fn visitor(max_work: usize) -> MemoryVisitor<'static> {
        MemoryVisitor::new(
            &SnapshotPoint::Periodic,
            0,
            0,
            MemoryBudget {
                max_depth: 32,
                max_work,
            },
        )
    }

    /// Node states that own no value still cost a unit each, so a huge graph of them cannot be
    /// walked for free under a small budget.
    #[test]
    fn many_empty_node_states_stay_inside_the_work_bound() {
        let state: &'static EvaluatorState = Box::leak(Box::new(state_of_scalars(500_000)));
        for max_work in [0, 1, 7, 50] {
            let mut visitor = visitor(max_work);
            let mut totals = StreamTotals::default();
            let started = std::time::Instant::now();
            visitor.evaluator_state(state, 0, &mut totals, Target::Own);
            assert!(visitor.sizer.work() <= max_work, "{max_work}");
            assert!(visitor.sizer.exhausted(), "{max_work}");
            assert!(totals.budget_skipped, "{max_work}");
            // The evaluator itself costs one unit, then one per node state entered.
            assert_eq!(visitor.sizer.work(), max_work, "{max_work}");
            assert!(started.elapsed() < std::time::Duration::from_secs(1));
            let snapshot = visitor.finish();
            assert!(snapshot.budget_exhausted && snapshot.total.payload.truncated);
        }
    }

    #[test]
    fn a_sufficient_budget_enters_every_node_state() {
        let state = Box::leak(Box::new(state_of_scalars(1_000)));
        let mut visitor = visitor(1_001);
        let mut totals = StreamTotals::default();
        visitor.evaluator_state(state, 0, &mut totals, Target::Own);
        assert_eq!(visitor.sizer.work(), 1_001);
        assert!(!visitor.sizer.exhausted() && !totals.budget_skipped);
    }
}
