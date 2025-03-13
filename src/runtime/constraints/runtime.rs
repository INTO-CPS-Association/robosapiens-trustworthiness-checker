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
use async_stream::stream;
use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use std::collections::BTreeMap;
use std::mem;
use tokio::sync::mpsc;
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

#[derive(Debug, Clone)]
pub enum ProducerMessage<T> {
    Data(T),
    Done,
}

struct InputProducer {
    mpsc_sender: Vec<mpsc::Sender<ProducerMessage<BTreeMap<VarName, Value>>>>,
}

impl InputProducer {
    const BUFFER_SIZE: usize = 1;

    pub fn new() -> Self {
        let mpsc_sender = Vec::new();
        Self { mpsc_sender }
    }

    pub fn run(&mut self, stream_collection: ValStreamCollection) {
        let task_sender = mem::take(&mut self.mpsc_sender);
        tokio::spawn(async move {
            if !task_sender.is_empty() {
                let mut inputs_stream = stream_collection.into_stream();
                while let Some(inputs) = inputs_stream.next().await {
                    let data = ProducerMessage::Data(inputs);
                    for sender in &task_sender {
                        let _ = sender.send(data.clone()).await;
                    }
                }
                for sender in &task_sender {
                    let _ = sender.send(ProducerMessage::Done).await;
                }
            }
        });
    }

    pub fn subscribe(&mut self) -> mpsc::Receiver<ProducerMessage<BTreeMap<VarName, Value>>> {
        let (sender, receiver) = mpsc::channel(Self::BUFFER_SIZE);
        self.mpsc_sender.push(sender);
        receiver
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
        let mut runtime_initial = ConstraintBasedRuntime::new(self.dependencies.clone());
        runtime_initial.store = model_constraints(self.model.clone());
        let has_inputs = self.has_inputs.clone();
        Box::pin(stream!(
            let mut runtime = runtime_initial;
            if has_inputs {
                let mut input_receiver = input_receiver;
                while let Some(inputs) = input_receiver.recv().await {
                    match inputs {
                        ProducerMessage::Data(inputs) => {
                            runtime.step(inputs.iter());
                            yield runtime.store.clone();
                            debug!("Store before clean: {:#?}", runtime.store);
                            runtime.cleanup();
                            debug!("Store after clean: {:#?}", runtime.store);
                        }
                        ProducerMessage::Done => {
                            break;
                        }
                    }
                }
            }
            else {
                loop {
                    runtime.step(BTreeMap::new().iter());
                    yield runtime.store.clone();
                    runtime.cleanup();
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
