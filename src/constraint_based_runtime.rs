use async_trait::async_trait;
use futures::future::join_all;
use futures::stream;
use futures::stream::BoxStream;
use futures::StreamExt;
use std::collections::BTreeMap;
use std::iter::zip;
use std::mem;

use crate::ast::LOLASpecification;
use crate::ast::SExpr;
use crate::constraint_solver::*;
use crate::core::InputProvider;
use crate::core::Monitor;
use crate::core::OutputHandler;
use crate::core::Specification;
use crate::core::Value;
use crate::core::VarName;
use crate::is_enum_variant;

#[derive(Default)]
pub struct ValStreamCollection(pub BTreeMap<VarName, BoxStream<'static, Value>>);

impl ValStreamCollection {
    fn into_stream(self) -> BoxStream<'static, BTreeMap<VarName, Value>> {
        Box::pin(futures::stream::unfold(self, |mut streams| async move {
            let mut res = BTreeMap::new();
            let nexts = streams.0.values_mut().map(|s| s.next());
            let next_vals = join_all(nexts).await;
            for (k, v) in zip(streams.0.keys(), next_vals) {
                match v {
                    Some(v) => {
                        res.insert(k.clone(), v);
                    }
                    None => {
                        return None;
                    }
                }
            }
            Some((res, streams))
        }))
    }
}

/* Get a stream of the values of a variable from a constraint store over time */
fn var_output_stream<'a>(
    var_name: VarName,
    constraints: BoxStream<'a, ConstraintStore>,
) -> BoxStream<'a, Value> {
    Box::pin(constraints.enumerate().map(move |(index, cs)| {
        if let Some(val) = cs.get_from_outputs_resolved(&var_name, &index) {
            // Return the value if it is resolved
            val.clone()
        } else {
            Value::Unknown
        }
    }))
}

#[derive(Debug, Default)]
pub struct SyncConstraintBasedRuntime {
    store: ConstraintStore,
    time: usize,
}

impl SyncConstraintBasedRuntime {
    fn receive_inputs(&mut self, inputs: &BTreeMap<VarName, Value>) {
        // Add new input values
        for (name, val) in inputs {
            self.store
                .input_streams
                .entry(name.clone())
                .or_insert(Vec::new())
                .push((self.time, val.clone()));
        }

        // Try to simplify the expressions in-place with fixed-point iteration
        let mut changed = true;
        while changed {
            changed = false;
            let mut new_exprs = BTreeMap::new();
            // Note: Intentionally does not borrow outputs_exprs here as it is needed for expr.simplify
            for (name, expr) in &self.store.output_exprs {
                if is_enum_variant!(expr, SExpr::<VarName>::Val(_)) {
                    new_exprs.insert(name.clone(), expr.clone());
                    continue;
                }

                match expr.simplify(self.time, &self.store) {
                    SimplifyResult::Resolved(v) => {
                        changed = true;
                        new_exprs.insert(name.clone(), SExpr::Val(v));
                    }
                    SimplifyResult::Unresolved(e) => {
                        new_exprs.insert(name.clone(), *e);
                    }
                }
            }
            self.store.output_exprs = new_exprs;
        }

        // Add unresolved version with absolute time of each output_expr
        for (name, expr) in &self.store.output_exprs {
            self.store
                .outputs_unresolved
                .entry(name.clone())
                .or_insert(Vec::new())
                .push((self.time, expr.to_absolute(self.time)));
        }
    }

    fn resolve_possible(&mut self) {
        // Fixed point iteration to resolve as many expressions as possible
        let mut changed = true;
        while changed {
            changed = false;
            let mut new_unresolved: SExprStream = BTreeMap::new();
            // Note: Intentionally does not borrow outputs_unresolved here as it is needed for expr.simplify
            for (name, map) in &self.store.outputs_unresolved {
                for (idx_time, expr) in map {
                    match expr.simplify(self.time, &self.store) {
                        SimplifyResult::Resolved(v) => {
                            changed = true;
                            self.store
                                .outputs_resolved
                                .entry(name.clone())
                                .or_insert(Vec::new())
                                .push((*idx_time, v.clone()));
                        }
                        SimplifyResult::Unresolved(e) => {
                            new_unresolved
                                .entry(name.clone())
                                .or_insert(Vec::new())
                                .push((*idx_time, *e));
                        }
                    }
                }
            }
            self.store.outputs_unresolved = new_unresolved;
        }
    }

    pub fn step(&mut self, inputs: &BTreeMap<VarName, Value>) {
        self.receive_inputs(inputs);
        self.resolve_possible();
        self.time += 1;
    }
}

pub struct ConstraintBasedMonitor {
    input_streams: ValStreamCollection,
    model: LOLASpecification,
    output_handler: Box<dyn OutputHandler<Value>>,
}

#[async_trait]
impl Monitor<LOLASpecification, Value> for ConstraintBasedMonitor {
    fn new(model: LOLASpecification, input: &mut dyn InputProvider<Value>, output: Box<dyn OutputHandler<Value>>) -> Self {
        let input_streams = model
            .input_vars()
            .iter()
            .map(move |var| {
                let stream = input.input_stream(var);
                (var.clone(), stream.unwrap())
            })
            .collect::<BTreeMap<_, _>>();
        let input_streams = ValStreamCollection(input_streams);

        ConstraintBasedMonitor {
            input_streams,
            model,
            output_handler: output,
        }
    }

    fn spec(&self) -> &LOLASpecification {
        &self.model
    }

    async fn run(mut self) {
        let outputs = self
            .model
            .output_vars()
            .into_iter()
            .map(|var| (var.clone(), self.output_stream(&var)))
            .collect();
        self.output_handler.provide_streams(outputs);
        self.output_handler.run().await;
    }
}

impl ConstraintBasedMonitor {
    fn stream_output_constraints(&mut self) -> BoxStream<'static, ConstraintStore> {
        let inputs_stream = mem::take(&mut self.input_streams).into_stream();
        let mut runtime_initial = SyncConstraintBasedRuntime::default();
        runtime_initial.store = model_constraints(self.model.clone());
        Box::pin(stream::unfold(
            (inputs_stream, runtime_initial),
            |(mut inputs_stream, mut runtime)| async move {
                // Add the new constraints to the constraint store
                let new_inputs = inputs_stream.next().await?;
                runtime.step(&new_inputs);

                // Keep unfolding
                Some((runtime.store.clone(), (inputs_stream, runtime)))
            },
        ))
    }

    fn output_stream(&mut self, var: &VarName) -> BoxStream<'static, Value> {
        var_output_stream(var.clone(), self.stream_output_constraints())
    }
}
