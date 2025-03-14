use crate::OutputStream;
use crate::core::InputProvider;
use crate::core::Monitor;
use crate::core::OutputHandler;
use crate::core::Specification;
use crate::core::Value;
use crate::core::VarName;
use crate::dep_manage::interface::DependencyManager;
use crate::is_enum_variant;
use crate::lang::dynamic_lola::ast::LOLASpecification;
use crate::lang::dynamic_lola::ast::SExpr;
use crate::runtime::constraints::solver::ConstraintStore;
use crate::runtime::constraints::solver::ConvertToAbsolute;
use crate::runtime::constraints::solver::SExprStream;
use crate::runtime::constraints::solver::Simplifiable;
use crate::runtime::constraints::solver::SimplifyResult;
use crate::runtime::constraints::solver::model_constraints;
use crate::stream_utils::oneshot_to_stream;
use async_stream::stream;
use async_trait::async_trait;
use core::panic;
use futures::StreamExt;
use futures::stream::BoxStream;
use std::collections::BTreeMap;
use std::mem;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;
use tracing::debug;
use tracing::info;

#[derive(Debug)]
pub struct ConstraintBasedRuntime {
    store: ConstraintStore,
    time: usize,
    dependencies: DependencyManager,
}

impl ConstraintBasedRuntime {
    pub fn new(dependencies: DependencyManager) -> Self {
        Self {
            store: ConstraintStore::default(),
            time: 0,
            dependencies,
        }
    }

    pub fn store_from_spec(&mut self, spec: LOLASpecification) {
        self.store = model_constraints(spec);
    }

    fn receive_inputs<'a, Iter>(&mut self, inputs: Iter)
    where
        Iter: Iterator<Item = (&'a VarName, &'a Value)>,
    {
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

                match expr.simplify(self.time, &self.store, name, &mut self.dependencies) {
                    SimplifyResult::Resolved(v) => {
                        changed = true;
                        new_exprs.insert(name.clone(), SExpr::Val(v));
                    }
                    SimplifyResult::Unresolved(e) => {
                        if *e != *expr {
                            changed = true;
                        }
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
                    match expr.simplify(self.time, &self.store, name, &mut self.dependencies) {
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

    // Remove unused input values and resolved outputs
    fn cleanup(&mut self) {
        let longest_times = self.dependencies.longest_time_dependencies();
        for collection in [
            &mut self.store.input_streams,
            &mut self.store.outputs_resolved,
        ] {
            // Go through each saved value and remove it if it is older than the current time,
            // keeping the longest dependency in mind
            for (name, values) in collection {
                let longest_dep = longest_times.get(name).cloned().unwrap_or(0);
                // Modify the collection in place
                values.retain(|(time, _)| {
                    longest_dep
                        .checked_add(*time)
                        // Overflow means that data for this var should always be kept - hence the true
                        .map_or(true, |t| t >= self.time)
                });
            }
        }
    }

    pub fn step<'a, Iter>(&mut self, inputs: Iter)
    where
        Iter: Iterator<Item = (&'a VarName, &'a Value)>,
    {
        info!("Runtime step at time: {}", self.time);
        self.receive_inputs(inputs);
        self.resolve_possible();
        self.time += 1;
    }
}

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

struct InputProducer {
    stream_rx: Option<oneshot::Receiver<BoxStream<'static, BTreeMap<VarName, Value>>>>,
    stream_tx: Option<oneshot::Sender<BoxStream<'static, BTreeMap<VarName, Value>>>>,
}

impl InputProducer {
    // TODO: TW comment here
    pub fn new() -> Self {
        let (stream_tx, stream_rx) = oneshot::channel();
        Self {
            stream_rx: Some(stream_rx),
            stream_tx: Some(stream_tx),
        }
    }

    pub fn run(&mut self, stream_collection: ValStreamCollection) {
        if let Some(stream_tx) = mem::take(&mut self.stream_tx) {
            let _ = stream_tx.send(stream_collection.into_stream());
        } else {
            panic!("InputProducer already run");
        }
    }

    pub fn subscribe(&mut self) -> BoxStream<'static, BTreeMap<VarName, Value>> {
        if let Some(stream_rx) = mem::take(&mut self.stream_rx) {
            oneshot_to_stream(stream_rx)
        } else {
            panic!("InputProducer already subscribed to");
        }
    }
}

pub struct ConstraintBasedMonitor {
    input_producer: InputProducer,
    stream_collection: ValStreamCollection,
    model: LOLASpecification,
    output_handler: Box<dyn OutputHandler<Val = Value>>,
    has_inputs: bool,
    dependencies: DependencyManager,
}

#[async_trait]
impl Monitor<LOLASpecification, Value> for ConstraintBasedMonitor {
    fn new(
        model: LOLASpecification,
        input: &mut dyn InputProvider<Val = Value>,
        output: Box<dyn OutputHandler<Val = Value>>,
        dependencies: DependencyManager,
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
            dependencies,
        }
    }

    fn spec(&self) -> &LOLASpecification {
        &self.model
    }

    async fn run(mut self) {
        let outputs = self.output_streams();
        self.output_handler.provide_streams(outputs);
        if self.has_inputs {
            self.input_producer.run(self.stream_collection);
        }
        self.output_handler.run().await;
    }
}

impl ConstraintBasedMonitor {
    fn output_streams(&mut self) -> BTreeMap<VarName, BoxStream<'static, Value>> {
        // TODO: TW split initial part into fn and comment on it
        let (output_senders, output_streams): (Vec<_>, Vec<_>) = self
            .model
            .output_vars()
            .iter()
            .map(|var| {
                let (sender, receiver) = mpsc::channel(10);
                let stream: OutputStream<Value> = Box::pin(ReceiverStream::new(receiver));
                ((var.clone(), sender), (var.clone(), stream))
            })
            .unzip();
        let output_senders = output_senders.into_iter().collect::<BTreeMap<_, _>>();
        let output_streams = output_streams.into_iter().collect::<BTreeMap<_, _>>();
        let mut var_indexes = self
            .model
            .output_vars()
            .into_iter()
            .zip(std::iter::repeat(0))
            .collect::<BTreeMap<_, _>>();

        let mut input_stream = self.input_producer.subscribe();
        let mut runtime_initial = ConstraintBasedRuntime::new(self.dependencies.clone());
        runtime_initial.store = model_constraints(self.model.clone());
        let has_inputs = self.has_inputs.clone();
        tokio::spawn(async move {
            let mut runtime = runtime_initial;
            if has_inputs {
                while let Some(inputs) = input_stream.next().await {
                    runtime.step(inputs.iter());
                    for (var, sender) in &output_senders {
                        let idx = var_indexes.get_mut(var).unwrap();
                        if let Some(val) = runtime.store.get_from_outputs_resolved(var, idx) {
                            let _ = sender.send(val.clone()).await;
                            *idx += 1;
                        }
                    }
                    debug!("Store before clean: {:#?}", runtime.store);
                    runtime.cleanup();
                    debug!("Store after clean: {:#?}", runtime.store);
                }
            } else {
                loop {
                    runtime.step(BTreeMap::new().iter());
                    for (var, sender) in &output_senders {
                        let idx = var_indexes.get_mut(var).unwrap();
                        if let Some(val) = runtime.store.get_from_outputs_resolved(var, idx) {
                            let _ = sender.send(val.clone()).await;
                            *idx += 1;
                        }
                    }
                    runtime.cleanup();
                }
            }
        });

        output_streams
    }
}
