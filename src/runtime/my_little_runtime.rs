use crate::{
    OutputStream, SExpr, VarName,
    core::{
        AbstractMonitorBuilder, InputProvider, Monitor, OutputHandler, Runnable, Specification,
        Value,
    },
    dep_manage::interface::DependencyManager,
    lang::dynamic_lola::{ast::LOLASpecification, lalr_parser},
    semantics::{
        AbstractContextBuilder, MonitoringSemantics, StreamContext, untimed_untyped_lola::semantics,
    },
    stream_utils,
};

use anyhow::anyhow;
use async_trait::async_trait;
use ecow::EcoVec;
use futures::{FutureExt, StreamExt, future::LocalBoxFuture};
use smol::LocalExecutor;
use std::{
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
    rc::Rc,
};
use tracing::{debug, error, info};
use unsync::spsc;

/// **************************
/// NOTE:
/// This is a prototype of a potential simplification for a runtime.
/// It currently still has some issues in terms of subcontexts but its overall
/// design may be useful for future runtimes.
/// **************************

pub struct LittleMonitorBuilder {
    executor: Option<Rc<LocalExecutor<'static>>>,
    model: Option<LOLASpecification>,
    input: Option<Box<dyn InputProvider<Val = Value>>>,
    output: Option<Box<dyn OutputHandler<Val = Value>>>,
    dependencies: Option<DependencyManager>,
}

impl AbstractMonitorBuilder<LOLASpecification, Value> for LittleMonitorBuilder {
    type Mon = LittleMonitor;

    fn new() -> Self {
        Self {
            executor: None,
            model: None,
            input: None,
            output: None,
            dependencies: None,
        }
    }

    fn executor(mut self, executor: Rc<LocalExecutor<'static>>) -> Self {
        self.executor = Some(executor);
        self
    }

    fn model(mut self, model: LOLASpecification) -> Self {
        self.model = Some(model);
        self
    }

    fn input(mut self, input: Box<dyn InputProvider<Val = Value>>) -> Self {
        self.input = Some(input);
        self
    }

    fn output(mut self, output: Box<dyn OutputHandler<Val = Value>>) -> Self {
        self.output = Some(output);
        self
    }

    fn dependencies(mut self, dependencies: DependencyManager) -> Self {
        self.dependencies = Some(dependencies);
        self
    }

    fn build(self) -> LittleMonitor {
        let executor = self.executor.unwrap();
        let model = self.model.unwrap();
        let mut input = self.input.unwrap();
        let output = self.output.unwrap();
        let dependencies = self.dependencies.unwrap();
        let input_streams = model
            .input_vars()
            .iter()
            .map(|var| {
                let stream = input.input_stream(var);
                stream.unwrap()
            })
            .collect::<Vec<_>>();
        let has_inputs = !input_streams.is_empty();

        LittleMonitor {
            _executor: executor,
            model,
            input_provider: input,
            output_handler: output,
            _has_inputs: has_inputs,
            _dependencies: dependencies,
        }
    }

    fn async_build(self: Box<Self>) -> LocalBoxFuture<'static, Self::Mon> {
        Box::pin(async move { (*self).build() })
    }

    fn mqtt_reconfig_provider(self, _provider: crate::io::mqtt::MQTTLocalityReceiver) -> Self {
        todo!()
    }
}

#[derive(Debug)]
enum StreamState {
    Pending,
    Finished,
}

struct ExprEvalutor {
    // Sender that forwards it to the VarManager
    sender: spsc::Sender<Value>,
    // Stream that evaluates the expression
    eval_stream: OutputStream<Value>,

    // Kept for debugging
    var_name: VarName,
    _expr: SExpr,
}

impl ExprEvalutor {
    fn new(
        var_name: VarName,
        expr: SExpr,
        sender: spsc::Sender<Value>,
        ctx: &LittleContext,
    ) -> Self {
        let eval_stream =
            semantics::UntimedLolaSemantics::<lalr_parser::LALRExprParser>::to_async_stream(
                expr.clone(),
                ctx,
            );
        Self {
            var_name,
            _expr: expr,
            sender,
            eval_stream,
        }
    }

    async fn eval_value(&mut self) -> anyhow::Result<StreamState> {
        debug!(?self.var_name, "ExprEvaluator eval_value: Waiting for next value.");
        if let Some(val) = self.eval_stream.next().await {
            info!(?self.var_name, ?val, "ExprEvaluator eval_value: Forwarding value to VarManager.");
            if let Err(e) = self.sender.send(val).await {
                return Err(anyhow!(
                    "ExprEvaluator eval_value for variable {}: Error sending value to VarManager: {}",
                    self.var_name,
                    e
                ));
            }
            // Stream not done yet
            Ok(StreamState::Pending)
        } else {
            debug!(
                "ExprEvaluator stream finished for variable {}",
                self.var_name
            );
            Ok(StreamState::Finished)
        }
    }
}

struct VarManager {
    // VarName this manages
    var_name: VarName,
    // Stream where Values are received
    value_stream: OutputStream<Value>,
    // Subscribers to this specific variable
    subscribers: Vec<spsc::Sender<Value>>,
    // Retained history of values (if needed)
    retained_history: VecDeque<Value>,
    samples_forwarded: usize,
    id: usize,
}

impl VarManager {
    fn new(var_name: VarName, value_stream: OutputStream<Value>) -> Self {
        let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        debug!(?var_name, "Creating VarManager {}", id);
        Self {
            var_name,
            value_stream,
            subscribers: Vec::new(),
            retained_history: VecDeque::new(),
            id,
            samples_forwarded: 0,
        }
    }

    fn new_from_receiver(var_name: VarName, receiver: spsc::Receiver<Value>) -> Self {
        let value_stream = stream_utils::channel_to_output_stream(receiver);
        Self::new(var_name, value_stream)
    }

    fn subscribe(&mut self, history_length: usize) -> OutputStream<Value> {
        let (mut tx, rx) = spsc::channel::<Value>(history_length + 128);
        // Compute how many Unknowns are needed - needed because history_length is longer than
        // retained_history.len()
        let missing = std::cmp::min(
            history_length.saturating_sub(self.retained_history.len()),
            self.samples_forwarded,
        );
        // Push them to the history
        std::iter::repeat(Value::Unknown)
            .take(missing)
            .for_each(|v| {
                self.retained_history.push_front(v);
            });
        info!(
            ?self.var_name,
            history_length,
            ?self.retained_history,
            "VarManager {} subscribe: Preparing subscription.",
            self.id,
        );

        // Pre-send requested history - available when requested
        self.retained_history.iter().skip(
            self.retained_history
            .len()
            .saturating_sub(history_length),
        ).for_each(|v| {
            // Should never fail as the capacity is always sufficient
            if let Err(e) = tx.try_send(v.clone()) {
            error!(
                ?self.var_name,
                ?e,
                "VarManager {} subscribe: Error sending retained history value to new subscriber.",
                self.id,
            );
            }
        });

        // Save subscriber for future values
        self.subscribers.push(tx);
        stream_utils::channel_to_output_stream(rx)
    }

    async fn forward_value(&mut self) -> anyhow::Result<StreamState> {
        info!(
            ?self.var_name,
            "VarManager {} forward_value: Waiting for next value.",
            self.id,
        );
        if let Some(val) = self.value_stream.next().await {
            info!(
                ?self.var_name,
                ?val,
                "VarManager {} forward_value: Forwarding value to {} subscribers.",
                self.id,
                self.subscribers.len()
            );
            // Retain in history if needed
            if self.retained_history.len() > 0 {
                self.retained_history.pop_front();
                self.retained_history.push_back(val.clone());
            }

            self.samples_forwarded += 1;
            let mut disconnected = vec![];
            for (idx, subscriber) in self.subscribers.iter_mut().enumerate() {
                if let Err(_) = subscriber.send(val.clone()).await {
                    // The only type of error is disconnection
                    info!(
                        "VarManager {} forward_value: Subscriber {} disconnected.",
                        self.id, idx
                    );
                    disconnected.push(idx);
                }
            }
            // Remove disconnected subscribers
            for idx in disconnected.into_iter().rev() {
                self.subscribers.remove(idx);
            }

            // Stream not done yet
            Ok(StreamState::Pending)
        } else {
            info!(?self.var_name, "VarManager {} stream finished", self.id);
            Ok(StreamState::Finished)
        }
    }
}

pub struct LittleMonitor {
    _executor: Rc<LocalExecutor<'static>>,
    model: LOLASpecification,
    input_provider: Box<dyn InputProvider<Val = Value>>,
    output_handler: Box<dyn OutputHandler<Val = Value>>,
    _has_inputs: bool,
    _dependencies: DependencyManager,
}

impl LittleMonitor {
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        model: LOLASpecification,
        input: Box<dyn InputProvider<Val = Value>>,
        output: Box<dyn OutputHandler<Val = Value>>,
        dependencies: DependencyManager,
    ) -> Self {
        LittleMonitorBuilder::new()
            .executor(executor)
            .model(model)
            .input(input)
            .output(output)
            .dependencies(dependencies)
            .build()
    }

    async fn eval_expr_evals(expr_evals: &mut Vec<ExprEvalutor>) -> anyhow::Result<StreamState> {
        let mut to_remove = vec![];
        for expr_eval in expr_evals.iter_mut() {
            match expr_eval.eval_value().await {
                Ok(StreamState::Pending) => {
                    info!(?expr_eval.var_name, "eval_expr_evals: ExprEvaluator pending");
                }
                Ok(StreamState::Finished) => {
                    info!(?expr_eval.var_name, "eval_expr_evals: ExprEvaluator finished");
                    to_remove.push(expr_eval.var_name.clone());
                }
                Err(e) => {
                    error!(?expr_eval.var_name, ?e, "eval_expr_evals: Error in ExprEvaluator");
                    return Err(anyhow!(
                        "Error in ExprEvaluator for variable {}: {}",
                        expr_eval.var_name,
                        e
                    ));
                }
            }
        }
        expr_evals.retain(|expr_eval| !to_remove.contains(&expr_eval.var_name));

        Ok(if expr_evals.is_empty() {
            StreamState::Finished
        } else {
            StreamState::Pending
        })
    }

    async fn work_task(
        mut ctx: LittleContext,
        mut expr_evals: Vec<ExprEvalutor>,
    ) -> anyhow::Result<()> {
        loop {
            info!("LittleMonitor work_task: Waiting for next tick...");
            let result =
                futures::join!(ctx.forward_values(), Self::eval_expr_evals(&mut expr_evals));

            // A bit verbose but it is nice for debugging...
            match result {
                (Ok(StreamState::Pending), Ok(StreamState::Pending)) => {
                    debug!(
                        "LittleMonitor work_task: Both forward_values and eval_expr_evals pending, continuing..."
                    );
                    continue;
                }
                (Ok(StreamState::Finished), Ok(StreamState::Finished)) => {
                    debug!(
                        "LittleMonitor work_task: Both forward_values and eval_expr_evals finished, ending work_task."
                    );
                    return Ok(());
                }
                (Ok(StreamState::Pending), Ok(StreamState::Finished)) => {
                    error!(
                        "LittleMonitor work_task: eval_expr_evals finished but forward_values pending"
                    );
                    return Err(anyhow!(
                        "eval_expr_evals finished but forward_values pending"
                    ));
                }
                (Ok(StreamState::Finished), Ok(StreamState::Pending)) => {
                    error!(
                        "LittleMonitor work_task: forward_values finished but eval_expr_evals pending"
                    );
                    return Err(anyhow!(
                        "forward_values finished but eval_expr_evals pending"
                    ));
                }
                (Ok(_), Err(e)) => {
                    error!(?e, "LittleMonitor work_task: Error in eval_expr_evals");
                    return Err(e);
                }
                (Err(e), Ok(_)) => {
                    error!(?e, "LittleMonitor work_task: Error in ctx.forward_values");
                    return Err(e);
                }
                (Err(e1), Err(e2)) => {
                    error!(
                        ?e1,
                        ?e2,
                        "LittleMonitor work_task: Errors in both ctx.forward_values and eval_expr_evals"
                    );
                    return Err(anyhow!("Errors in work_task: {}, {}", e1, e2));
                }
            }
        }
    }
}

impl Monitor<LOLASpecification, Value> for LittleMonitor {
    fn spec(&self) -> &LOLASpecification {
        &self.model
    }
}

#[async_trait(?Send)]
impl Runnable for LittleMonitor {
    async fn run_boxed(mut self: Box<Self>) -> anyhow::Result<()> {
        info!("Running LittleMonitor.");
        // Set up input streams
        let input_streams = self
            .model
            .input_vars()
            .iter()
            .map(|var| {
                let stream = self.input_provider.input_stream(var);
                (var.clone(), stream.unwrap())
            })
            .collect::<BTreeMap<VarName, OutputStream<Value>>>();

        // Prepare for ExprEvalutors and create VarManagers for output variables
        let (expr_eval_components, mut var_managers): (BTreeMap<_, _>, Vec<_>) = self
            .model
            .output_vars()
            .iter()
            .map(|var_name| {
                let (sender, receiver): (spsc::Sender<Value>, spsc::Receiver<Value>) =
                    spsc::channel(128);
                let var_manager = VarManager::new_from_receiver(var_name.clone(), receiver);
                let expr = self.model.var_expr(var_name).ok_or_else(|| {
                    anyhow!(
                        "No expression found for output variable {} when setting up Monitor",
                        var_name
                    )
                });
                ((var_name.clone(), (expr, sender)), var_manager)
            })
            .collect();

        // Give OutputHandler subscriptions to output variables
        let subscriptions = var_managers
            .iter_mut()
            .map(|vm| vm.subscribe(0))
            .collect::<Vec<OutputStream<Value>>>();
        self.output_handler.provide_streams(subscriptions);

        // Let VarManagers used by Context also include input streams
        // (so we can call var(x) on input variables)
        let var_managers = var_managers
            .into_iter()
            .chain(
                input_streams
                    .into_iter()
                    .map(|(var_name, stream)| VarManager::new(var_name, stream)),
            )
            .collect::<Vec<VarManager>>();

        // Create context
        let builder = LittleContextBuilder::new().var_managers(
            var_managers
                .into_iter()
                .map(|vm| (vm.var_name.clone(), vm))
                .collect(),
        );
        let context = builder.build();

        // Now that context is ready, create ExprEvalutors
        let expr_evals = expr_eval_components
            .into_iter()
            .map(|(var_name, (expr_res, sender))| {
                let expr = expr_res.unwrap();
                ExprEvalutor::new(var_name, expr, sender, &context)
            })
            .collect::<Vec<ExprEvalutor>>();

        // Little helper function that logs after a future has ended. Reduces lines of code...
        fn log_end<Fut, T>(fut: Fut, msg: &'static str) -> impl futures::Future<Output = T>
        where
            Fut: futures::Future<Output = T>,
        {
            fut.map(move |res| {
                info!("{}", msg);
                res
            })
            .fuse()
        }

        let input_fut = log_end(self.input_provider.run(), "input_provider.run() ended");
        let output_fut = log_end(self.output_handler.run(), "output_handler.run() ended");
        let work_fut = log_end(
            Box::pin(Self::work_task(context, expr_evals)),
            "LittleMonitor work_task ended",
        );

        let res = futures::join!(input_fut, output_fut, work_fut);
        if let Err(e) = res.0 {
            error!(?e, "Input provider had an error");
        }
        if let Err(e) = res.1 {
            error!(?e, "Output handler had an error");
        }
        if let Err(e) = res.2 {
            error!(?e, "Work task had an error");
        }

        Ok(())
    }
}

struct LittleContextBuilder {
    var_managers: Option<BTreeMap<VarName, VarManager>>,
    history_length: Option<usize>,
}

impl LittleContextBuilder {
    fn var_managers(self, var_managers: BTreeMap<VarName, VarManager>) -> Self {
        Self {
            var_managers: Some(var_managers),
            ..self
        }
    }
}

impl AbstractContextBuilder for LittleContextBuilder {
    type Val = Value;

    type Ctx = LittleContext;

    fn new() -> Self {
        Self {
            var_managers: None,
            history_length: None,
        }
    }

    fn executor(self, _executor: Rc<LocalExecutor<'static>>) -> Self {
        todo!()
    }

    fn var_names(self, _var_names: Vec<VarName>) -> Self {
        todo!()
    }

    fn history_length(self, history_length: usize) -> Self {
        Self {
            history_length: Some(history_length),
            ..self
        }
    }

    fn input_streams(self, _streams: Vec<OutputStream<Self::Val>>) -> Self {
        todo!()
    }

    fn partial_clone(&self) -> Self {
        todo!()
    }

    fn build(self) -> Self::Ctx {
        LittleContext::new(
            Rc::new(RefCell::new(self.var_managers.expect(
                "VarManagers must be set before building LittleContext",
            ))),
            self.history_length.unwrap_or(0),
        )
    }
}

static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

struct LittleContext {
    // Rc RefCell because of the StreamContext interface for Var...
    var_managers: Rc<RefCell<BTreeMap<VarName, VarManager>>>,
    // History length for new calls to var
    history_length: usize,
    // Unique identifier for this variable manager
    id: usize,
}

impl LittleContext {
    fn new(
        var_managers: Rc<RefCell<BTreeMap<VarName, VarManager>>>,
        history_length: usize,
    ) -> Self {
        let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        debug!("Creating LittleContext {}", id);
        Self {
            var_managers,
            history_length,
            id,
        }
    }

    async fn forward_values(&mut self) -> anyhow::Result<StreamState> {
        let mut managers = self.var_managers.borrow_mut();
        for (name, manager) in managers.iter_mut() {
            match manager.forward_value().await {
                Ok(StreamState::Pending) => {}
                Ok(StreamState::Finished) => {
                    info!(?name, "forward_values: VarManager finished");
                    // Need to clear them as early as possible, as this indicates
                    // subscribers should not expect more values
                    manager.subscribers.clear();
                }
                Err(e) => {
                    error!(?name, ?e, "forward_values: Error in VarManager");
                    return Err(e);
                }
            }
        }
        managers.retain(|_, manager| !manager.subscribers.is_empty());

        Ok(if managers.is_empty() {
            StreamState::Finished
        } else {
            StreamState::Pending
        })
    }

    fn subcontext_common(&self, vs: EcoVec<VarName>, history_length: usize) -> Self {
        let mut managers = self.var_managers.borrow_mut();
        let new_managers = managers
            .iter_mut()
            .filter_map(|(var_name, manager)| {
                if vs.contains(var_name) {
                    let stream = manager.subscribe(history_length);
                    Some((var_name.clone(), VarManager::new(var_name.clone(), stream)))
                } else {
                    None
                }
            })
            .collect::<BTreeMap<_, _>>();
        let builder = LittleContextBuilder::new()
            .var_managers(new_managers)
            .history_length(history_length);
        builder.build()
    }
}

#[async_trait(?Send)]
impl StreamContext<Value> for LittleContext {
    type Builder = LittleContextBuilder;

    fn var(&self, x: &VarName) -> Option<OutputStream<Value>> {
        let mut manager = self.var_managers.borrow_mut();
        let stream = manager.get_mut(x)?.subscribe(self.history_length);
        info!(
            self.id,
            ?x,
            "LittleContext::var: Created new output stream for variable"
        );
        Some(stream)
    }

    fn subcontext(&self, history_length: usize) -> Self {
        info!(
            self.id,
            ?history_length,
            "LittleContext::subcontext: Creating subcontext."
        );
        self.subcontext_common(
            self.var_managers
                .borrow()
                .keys()
                .cloned()
                .collect::<EcoVec<VarName>>(),
            history_length,
        )
    }

    fn restricted_subcontext(&self, vs: EcoVec<VarName>, history_length: usize) -> Self {
        info!(
            self.id,
            ?vs,
            ?history_length,
            "LittleContext::restricted_subcontext: Creating restricted subcontext."
        );
        self.subcontext_common(vs, history_length)
    }

    async fn tick(&mut self) {
        // Tick is just a less informative version of forward_values but
        // the DUPs interface dictates that we need this
        let _ = self.forward_values().await.unwrap();
    }

    async fn run(&mut self) {
        // unimplemented!("StreamContext::run")
    }

    fn is_clock_started(&self) -> bool {
        unimplemented!("StreamContext::is_clock_started")
    }

    fn clock(&self) -> usize {
        unimplemented!("StreamContext::clock")
    }

    fn cancellation_token(&self) -> crate::utils::cancellation_token::CancellationToken {
        unimplemented!("StreamContext::cancellation_token")
    }

    fn cancel(&self) {
        unimplemented!("StreamContext::cancel")
    }
}

#[cfg(test)]
mod tests {

    use crate::async_test;
    use crate::core::Runnable;
    use crate::dep_manage::interface::{DependencyKind, create_dependency_manager};
    use crate::io::testing::ManualOutputHandler;
    use crate::runtime::my_little_runtime::LittleMonitor;
    use futures::stream::StreamExt;
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::collections::BTreeMap;
    use std::rc::Rc;
    use tracing::warn;

    use crate::lola_fixtures::*;
    use crate::{Value, lola_specification};

    use tc_testutils::streams::with_timeout;

    #[apply(async_test)]
    async fn test_simple_add(executor: Rc<LocalExecutor<'static>>) {
        let spec_untyped = lola_specification(&mut spec_simple_add_monitor()).unwrap();

        let x = vec![0.into(), 1.into(), 2.into()];
        let y = vec![3.into(), 4.into(), 5.into()];
        let input_streams = BTreeMap::from([("x".into(), x), ("y".into(), y)]);
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let monitor = LittleMonitor {
            _executor: executor.clone(),
            model: spec_untyped.clone(),
            input_provider: Box::new(input_streams),
            _has_inputs: true,
            output_handler,
            _dependencies: create_dependency_manager(DependencyKind::Empty, spec_untyped.clone()),
        };

        executor.spawn(monitor.run()).detach();

        let outputs: Vec<(usize, Vec<Value>)> =
            with_timeout(outputs.enumerate().collect(), 1, "outputs")
                .await
                .unwrap();

        warn!(?outputs, "Outputs:");
        assert_eq!(outputs.len(), 3,);
        assert_eq!(
            outputs,
            vec![
                (0, vec![3.into()]),
                (1, vec![5.into()]),
                (2, vec![7.into()]),
            ],
        );
    }

    #[apply(async_test)]
    async fn test_dynamic(executor: Rc<LocalExecutor<'static>>) {
        let spec_untyped = lola_specification(&mut spec_dynamic()).unwrap();

        let x = vec![0.into(), 1.into(), 2.into()];
        // TODO: Use the real test values
        // let e = vec!["x + 1".into(), "x + 2".into(), "x + 3".into()];
        let e = vec!["x + 1".into(), Value::Unknown, Value::Unknown];
        let input_streams = BTreeMap::from([("x".into(), x), ("e".into(), e)]);
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec_untyped.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let monitor = LittleMonitor {
            _executor: executor.clone(),
            model: spec_untyped.clone(),
            input_provider: Box::new(input_streams),
            _has_inputs: true,
            output_handler,
            _dependencies: create_dependency_manager(DependencyKind::Empty, spec_untyped.clone()),
        };

        executor.spawn(monitor.run()).detach();

        let outputs: Vec<(usize, Vec<Value>)> =
            with_timeout(outputs.enumerate().collect(), 1, "outputs")
                .await
                .unwrap();

        warn!(?outputs, "Outputs:");
        assert_eq!(outputs.len(), 3,);
        assert_eq!(
            outputs,
            vec![
                (0, vec![1.into()]),
                (1, vec![2.into()]),
                (2, vec![3.into()]),
            ],
        );
    }
}
