use futures::future::join_all;
use futures::stream;
use futures::stream::BoxStream;
use futures::StreamExt;
use std::collections::BTreeMap;
use std::iter::zip;
use std::mem;

use crate::ast::LOLASpecification;
use crate::constraint_solver::*;
use crate::core::ConcreteStreamData;
use crate::core::IndexedVarName;
use crate::core::InputProvider;
use crate::core::Monitor;
use crate::core::Specification;
use crate::core::VarName;
use crate::OutputStream;

#[derive(Default)]
pub struct ValStreamCollection(pub BTreeMap<VarName, BoxStream<'static, ConcreteStreamData>>);

impl ValStreamCollection {
    fn into_stream(self) -> BoxStream<'static, BTreeMap<VarName, ConcreteStreamData>> {
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
    inputs: BoxStream<'a, BTreeMap<VarName, ConcreteStreamData>>,
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
) -> BoxStream<'a, BTreeMap<VarName, ConcreteStreamData>> {
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
    model: LOLASpecification,
}

impl Monitor<LOLASpecification, ConcreteStreamData> for ConstraintBasedMonitor {
    fn new(model: LOLASpecification, mut input: impl InputProvider<ConcreteStreamData>) -> Self {
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

    fn monitor_outputs(&mut self) -> OutputStream<BTreeMap<VarName, ConcreteStreamData>> {
        constraints_to_outputs(
            self.stream_output_constraints(),
            self.model.output_vars.clone(),
        )
    }
}

impl ConstraintBasedMonitor {
    fn inputs_into_constraints(
        &mut self,
    ) -> BoxStream<'static, SExprConstraintStore<IndexedVarName>> {
        let input_streams = mem::take(&mut self.input_streams);
        inputs_to_constraints(input_streams.into_stream())
    }

    fn stream_output_constraints(
        &mut self,
    ) -> BoxStream<'static, SExprConstraintStore<IndexedVarName>> {
        let initial_constraints = SExprConstraintStore::<IndexedVarName>::default();
        let overall_constraints = model_constraints(self.model.clone());
        let inputs_initial = self.inputs_into_constraints();
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
}
