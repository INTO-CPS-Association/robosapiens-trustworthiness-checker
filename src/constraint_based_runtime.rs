use futures::future::join_all;
use futures::stream;
use futures::stream::BoxStream;
use futures::stream::LocalBoxStream;
use futures::StreamExt;
use std::collections::BTreeMap;
use std::iter::zip;

use crate::constraint_solver::*;
use crate::core::IndexedVarName;
use crate::core::StreamData;
use crate::core::VarName;

pub struct ValStreamCollection(pub BTreeMap<VarName, BoxStream<'static, StreamData>>);

impl ValStreamCollection {
    fn stream(&mut self) -> BoxStream<'_, BTreeMap<VarName, StreamData>> {
        Box::pin(futures::stream::unfold(self, |streams| async move {
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
    inputs: BoxStream<'a, BTreeMap<VarName, StreamData>>,
) -> BoxStream<'a, SExprConstraintStore<IndexedVarName>> {
    Box::pin(inputs.enumerate().map(|(i, input)| {
        SExprConstraintStore {
            resolved: input
                .iter()
                .map(|(k, v)| (k.clone().to_indexed(i), (*v).clone()))
                .collect(),
            unresolved: Vec::new(),
        }
    }))
}

fn constraints_to_outputs<'a>(
    constraints: BoxStream<'a, SExprConstraintStore<IndexedVarName>>,
    output_vars: Vec<VarName>,
) -> BoxStream<'a, BTreeMap<VarName, StreamData>> {
    Box::pin(constraints.enumerate().map(move |(index, cs)| {
        let mut res = BTreeMap::new();
        for (k, v) in cs.resolved {
            for var in &output_vars {
                if k == var.to_indexed(index) {
                    res.insert(var.clone(), v.clone());
                }
            }
        }
        res
    }))
}

pub struct ConstraintBasedMonitor {
    input_streams: ValStreamCollection,
    constraints: SExprConstraintStore<VarName>,
    input_vars: Vec<VarName>,
    output_vars: Vec<VarName>,
    input_index: usize,
}

// unsafe impl Send for SExprConstraintStore<VarName> {}
// unsafe impl Sync for SExprConstraintStore<VarName> {}
// unsafe impl Send for SExprConstraintStore<IndexedVarName> {}
// unsafe impl Sync for SExprConstraintStore<IndexedVarName> {}

impl ConstraintBasedMonitor {
    pub fn new(
        input_vars: Vec<VarName>,
        output_vars: Vec<VarName>,
        constraints: SExprConstraintStore<VarName>,
        input_streams: ValStreamCollection,
    ) -> Self {
        ConstraintBasedMonitor {
            input_streams,
            constraints,
            input_vars,
            output_vars,
            input_index: 0,
        }
    }

    pub fn stream_constraints(&mut self) -> BoxStream<'_, SExprConstraintStore<IndexedVarName>> {
        inputs_to_constraints(self.input_streams.stream())
    }

    pub fn stream_outputs(&mut self) -> LocalBoxStream<'_, BTreeMap<VarName, StreamData>> {
        let outputs = self.output_vars.clone();
        let input_index = self.input_index.clone();
        Box::pin(self.stream_constraints().map(move |cs| {
            let mut res = BTreeMap::new();
            for k in &outputs {
                res.insert(
                    k.clone(),
                    cs.resolved
                        .iter()
                        .find_map(|(v, s)| {
                            if *v == k.to_indexed(input_index.try_into().unwrap()) {
                                Some(s.clone())
                            } else {
                                None
                            }
                        })
                        .unwrap_or_else(|| StreamData::Unknown)
                        .clone(),
                );
            }
            res
        }))
    }

    pub fn monitor_constraints(&mut self) -> BoxStream<'_, SExprConstraintStore<IndexedVarName>> {
        let initial_constraints = SExprConstraintStore::<IndexedVarName>::default();
        let overall_constraints = self.constraints.clone();
        let inputs_initial = self.stream_constraints();
        Box::pin(stream::unfold(
            (0, initial_constraints, inputs_initial, overall_constraints),
            |(index, mut constraints, mut inputs, cs)| async move {
                // Add the new contraints to the constraint store
                let new_constraints = inputs.next().await?;
                constraints.extend(new_constraints);

                // Add new output equations to the constraint store
                constraints.extend(to_indexed_constraints(
                    &cs.clone(),
                    // Potential panic from usize to isize conversion
                    index.try_into().unwrap(),
                ));

                // Solve the extended constraint system
                constraints.solve(index.clone());

                // Keep unfolding
                Some((constraints.clone(), (index + 1, constraints, inputs, cs)))
            },
        ))
    }

    pub fn monitor_outputs(&mut self) -> BoxStream<'_, BTreeMap<VarName, StreamData>> {
        let output_vars = self.output_vars.clone();
        constraints_to_outputs(self.monitor_constraints(), output_vars)
    }
}
