use crate::core::InputProvider;
use crate::core::Monitor;
use crate::core::OutputHandler;
use crate::core::Specification;
use crate::core::Value;
use crate::core::VarName;
use crate::is_enum_variant;
use crate::lang::dynamic_lola::ast::LOLASpecification;
use crate::lang::dynamic_lola::ast::SExpr;
use crate::runtime::constraints::solver::model_constraints;
use crate::runtime::constraints::solver::ConstraintStore;
use crate::runtime::constraints::solver::ConvertToAbsolute;
use crate::runtime::constraints::solver::SExprStream;
use crate::runtime::constraints::solver::Simplifiable;
use crate::runtime::constraints::solver::SimplifyResult;
use async_stream::stream;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use std::collections::BTreeMap;
use tokio::sync::broadcast;
use tracing::{debug, info};

#[derive(Default)]
pub struct ValStreamCollection(pub BTreeMap<VarName, BoxStream<'static, Value>>);

impl ValStreamCollection {
    fn into_stream(mut self) -> BoxStream<'static, BTreeMap<VarName, Value>> {
        Box::pin(stream!(loop {
            let mut res = BTreeMap::new();
            for (name, stream) in self.0.iter_mut() {
                match stream.next().await {
                    Some(val) => {
                        res.insert(name.clone(), val);
                    }
                    None => {
                        return;
                    }
                }
            }
            yield res;
        }))
    }
}

#[derive(Debug, Default)]
pub struct ConstraintBasedRuntime {
    store: ConstraintStore,
    time: usize,
}
impl SExpr<VarName> {
    // Traverses the sexpr and returns a map of its dependencies to other variables
    fn dependencies(&self) -> BTreeMap<VarName, usize> {
        fn dependencies_impl(
            sexpr: &SExpr<VarName>,
            steps: usize,
            map: &mut BTreeMap<VarName, usize>,
        ) {
            match sexpr {
                SExpr::Var(name) => {
                    map.entry(name.clone())
                        .and_modify(|existing_depth| *existing_depth = (*existing_depth).max(steps))
                        .or_insert(steps);
                }
                SExpr::SIndex(sexpr, idx, _) => {
                    dependencies_impl(sexpr, steps + idx.unsigned_abs(), map);
                }
                SExpr::If(iff, then, els) => {
                    dependencies_impl(iff, steps, map);
                    dependencies_impl(then, steps, map);
                    dependencies_impl(els, steps, map);
                }
                SExpr::Val(_) => {}
                SExpr::List(vec) => {
                    vec.iter()
                        .for_each(|sexpr| dependencies_impl(sexpr, steps, map));
                }
                SExpr::Eval(sexpr)
                | SExpr::Not(sexpr)
                | SExpr::LHead(sexpr)
                | SExpr::LTail(sexpr)
                | SExpr::Defer(sexpr) => dependencies_impl(sexpr, steps, map),
                SExpr::BinOp(sexpr1, sexpr2, _)
                | SExpr::Update(sexpr1, sexpr2)
                | SExpr::LIndex(sexpr1, sexpr2)
                | SExpr::LAppend(sexpr1, sexpr2)
                | SExpr::LConcat(sexpr1, sexpr2) => {
                    dependencies_impl(sexpr1, steps, map);
                    dependencies_impl(sexpr2, steps, map);
                }
            }
        }

        let mut map = BTreeMap::new();
        dependencies_impl(self, 0, &mut map);
        map
    }
}

impl ConstraintBasedRuntime {
    fn generate_dependencies(&mut self) -> BTreeMap<VarName, usize> {
        // Merge map prioritizing the smallest value during conflicts
        // (smallest value in this case means expressions going further back in history)
        fn merge_min(map1: &mut BTreeMap<VarName, usize>, map2: BTreeMap<VarName, usize>) {
            for (key, value) in map2 {
                map1.entry(key)
                    .and_modify(|existing| *existing = (*existing).max(value))
                    .or_insert(value);
            }
        }

        let mut dependencies: BTreeMap<VarName, usize> = BTreeMap::new();
        for (name, expr) in &self.store.output_exprs {
            let mut expr_deps = expr.dependencies();
            // Make sure it is not deleted immediately after being resolved,
            // by adding a dependency on itself to the longest dependency
            let max_dep = expr_deps.values().max().cloned().unwrap_or(0);
            expr_deps.insert(name.clone(), max_dep);
            // Merge with global dependencies
            merge_min(&mut dependencies, expr_deps);
        }
        dependencies
    }

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

    fn cleanup(&mut self) {
        // Remove unused input values and resolved outputs
        for collection in [
            &mut self.store.input_streams,
            &mut self.store.outputs_resolved,
        ] {
            // Go through each saved value and remove it if it is older than the current time,
            // keeping the longest dependency in mind
            for (name, values) in collection {
                let longest_dep = self
                    .store
                    .output_dependencies
                    .get(name)
                    .cloned()
                    .unwrap_or(0);
                // Modify the collection in place
                values.retain(|(time, _)| *time + longest_dep >= self.time);
            }
        }
    }

    pub fn step(&mut self, inputs: &BTreeMap<VarName, Value>) {
        info!("Runtime step at time: {}", self.time);
        self.receive_inputs(inputs);
        self.resolve_possible();
        self.cleanup();
        debug!("Store after clean: {:?}", self.store);
        self.time += 1;
    }
}

#[derive(Debug, Clone)]
pub enum ProducerMessage<T> {
    Data(T),
    Done,
}

struct InputProducer {
    sender: broadcast::Sender<ProducerMessage<BTreeMap<VarName, Value>>>,
}

impl InputProducer {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(10);
        Self { sender }
    }
    pub fn run(&self, stream_collection: ValStreamCollection) {
        let task_sender = self.sender.clone();
        tokio::spawn(async move {
            let mut inputs_stream = stream_collection.into_stream();
            while let Some(inputs) = inputs_stream.next().await {
                let data = ProducerMessage::Data(inputs);
                task_sender.send(data).unwrap();
            }
            task_sender.send(ProducerMessage::Done).unwrap();
        });
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ProducerMessage<BTreeMap<VarName, Value>>> {
        self.sender.subscribe()
    }
}

pub struct ConstraintBasedMonitor {
    input_producer: InputProducer,
    stream_collection: ValStreamCollection,
    model: LOLASpecification,
    output_handler: Box<dyn OutputHandler<Value>>,
    has_inputs: bool,
}

#[async_trait]
impl Monitor<LOLASpecification, Value> for ConstraintBasedMonitor {
    fn new(
        model: LOLASpecification,
        input: &mut dyn InputProvider<Value>,
        output: Box<dyn OutputHandler<Value>>,
    ) -> Self {
        let input_streams = model
            .input_vars()
            .iter()
            .map(move |var| {
                let stream = input.input_stream(var);
                (var.clone(), stream.unwrap())
            })
            .collect::<BTreeMap<_, _>>();
        let has_inputs = !input_streams.is_empty();
        let stream_collection = ValStreamCollection(input_streams);
        let input_producer = InputProducer::new();

        ConstraintBasedMonitor {
            input_producer,
            stream_collection,
            model,
            output_handler: output,
            has_inputs,
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
        if self.has_inputs {
            self.input_producer.run(self.stream_collection);
        }
        self.output_handler.run().await;
    }
}

impl ConstraintBasedMonitor {
    fn stream_output_constraints(&mut self) -> BoxStream<'static, ConstraintStore> {
        let input_receiver = self.input_producer.subscribe();
        let mut runtime_initial = ConstraintBasedRuntime::default();
        runtime_initial.store = model_constraints(self.model.clone());
        runtime_initial.store.output_dependencies = runtime_initial.generate_dependencies();
        let has_inputs = self.has_inputs.clone();
        Box::pin(stream!(
            let mut runtime = runtime_initial;
            if has_inputs {
                let mut input_receiver = input_receiver;
                while let Ok(inputs) = input_receiver.recv().await {
                    match inputs {
                        ProducerMessage::Data(inputs) => {
                        runtime.step(&inputs);
                        yield runtime.store.clone();
                        }
                        ProducerMessage::Done => {
                            break;
                        }
                    }
                }
            }
            else {
                loop {
                    runtime.step(&BTreeMap::new());
                    yield runtime.store.clone();
                }
            }
        ))
    }

    fn output_stream(&mut self, var: &VarName) -> BoxStream<'static, Value> {
        let var_name = var.clone();
        let mut constraints = self.stream_output_constraints();

        Box::pin(stream! {
            let mut index = 0;
            while let Some(cs) = constraints.next().await {
                if let Some(resolved) = cs.get_from_outputs_resolved(&var_name, &index).cloned(){
                    index += 1;
                    yield resolved;
                }
            }
        })
    }
}
