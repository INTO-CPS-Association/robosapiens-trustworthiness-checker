use futures::future::join_all;
use futures::stream;
use futures::stream::BoxStream;
use futures::StreamExt;
use std::collections::BTreeMap;
use std::iter::zip;
use std::mem;

use crate::ast::LOLASpecification;
use crate::constraint_solver::*;
use crate::core::IndexedVarName;
use crate::core::InputProvider;
use crate::core::Monitor;
use crate::core::Specification;
use crate::core::Value;
use crate::core::VarName;
use crate::ast::SExpr;
use crate::OutputStream;
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

fn inputs_to_constraints<'a>(
    inputs: BoxStream<'a, BTreeMap<VarName, Value>>,
) -> BoxStream<'a, SExprConstraintStore<IndexedVarName>> {
    Box::pin(inputs.enumerate().map(|(i, input)| {
        SExprConstraintStore {
            resolved: input
                .iter()
                .map(|(k, v)| (k.clone().to_indexed(i), (*v).clone()))
                .collect(),
            unresolved: Vec::new(),
            input_streams: BTreeMap::new(),
            output_exprs: BTreeMap::new(),
            outputs_resolved: BTreeMap::new(),
            outputs_unresolved: BTreeMap::new(),
        }
    }))
}

fn constraints_to_outputs<'a>(
    constraints: BoxStream<'a, SExprConstraintStore<IndexedVarName>>,
    output_vars: Vec<VarName>,
) -> BoxStream<'a, BTreeMap<VarName, Value>> {
    Box::pin(constraints.enumerate().map(move |(index, cs)| {
        let mut res = BTreeMap::new();

        for var in &output_vars {
            if let Some(val) = cs.get_from_outputs_resolved(&var, &index) {
                res.insert(var.clone(), val.clone());
            }
        }
        res
    }))
}

#[derive(Debug, Default)]
pub struct SyncConstraintBasedRuntime {
    store: SExprConstraintStore<IndexedVarName>,
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

        // Try to simplify the expressions in-place
        // NOTE: This is a naive implementation that doesn't implement it 100 % correctly
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
}

impl Monitor<LOLASpecification, Value> for ConstraintBasedMonitor {
    fn new(model: LOLASpecification, input: &mut dyn InputProvider<Value>) -> Self {
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
        }
    }

    fn spec(&self) -> &LOLASpecification {
        &self.model
    }

    fn monitor_outputs(&mut self) -> OutputStream<BTreeMap<VarName, Value>> {
        constraints_to_outputs(
            self.stream_output_constraints(),
            self.model.output_vars.clone(),
        )
    }
}

impl ConstraintBasedMonitor {
    fn stream_output_constraints(
        &mut self,
    ) -> BoxStream<'static, SExprConstraintStore<IndexedVarName>> {
        let inputs_initial = self.inputs_into_constraints();
        let mut runtime_initial = SyncConstraintBasedRuntime::default();
        runtime_initial.store = model_constraints(self.model.clone());
        Box::pin(stream::unfold(
            ( inputs_initial, runtime_initial),
            |(mut inputs, mut runtime)| async move {
                // Add the new contraints to the constraint store
                let tmp_store = inputs.next().await?;
                let new_inputs = tmp_store.resolved.iter().map(|(k, v)| {
                    let name = VarName(k.0.clone());
                    (name, v.clone())
                }).collect::<BTreeMap<VarName, Value>>();
                runtime.step(&new_inputs);

                // Keep unfolding
                Some((runtime.store.clone(), ( inputs, runtime)))
            },
        ))
    }

    fn inputs_into_constraints(
        &mut self,
    ) -> BoxStream<'static, SExprConstraintStore<IndexedVarName>> {
        let input_streams = mem::take(&mut self.input_streams);
        inputs_to_constraints(input_streams.into_stream())
    }

}
