use crate::{
    OutputStream, VarName,
    core::{DeferrableStreamData, InputProvider, OutputHandler, Runtime, Specification},
    lang::core::{DepGraph, DependencyGraphExpr, DependencyGraphSpec, DependencyResolver},
    runtime::RuntimeBuilder,
    semantics::{AbstractContextBuilder, AsyncConfig, MonitoringSemantics, StreamContext},
    stream_utils::{self},
    utils::cancellation_token::CancellationToken,
};

use anyhow::anyhow;
use async_trait::async_trait;
use ecow::EcoVec;
use futures::{
    FutureExt, StreamExt,
    future::{LocalBoxFuture, join_all},
};
use smol::LocalExecutor;
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, VecDeque},
    rc::Rc,
    time::Instant,
};
use tracing::{debug, error, info, warn};
use unsync::spsc;

const CHANNEL_SIZE: usize = 8;

pub struct SemiSyncRuntimeBuilder<AC, MS>
where
    AC: AsyncConfig,
    AC::Expr: DependencyGraphExpr,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
{
    executor: Option<Rc<LocalExecutor<'static>>>,
    model: Option<AC::Spec>,
    input: Option<Box<dyn InputProvider<Val = AC::Val>>>,
    output: Option<Box<dyn OutputHandler<Val = AC::Val>>>,
    starting_history: Option<BTreeMap<VarName, Vec<AC::Val>>>,
    _marker: std::marker::PhantomData<MS>,
}

impl<AC, MS> SemiSyncRuntimeBuilder<AC, MS>
where
    AC: AsyncConfig<Ctx = SemiSyncContext<AC>>,
    AC::Expr: DependencyGraphExpr,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
{
    pub fn starting_history(mut self, history: BTreeMap<VarName, Vec<AC::Val>>) -> Self {
        self.starting_history = Some(history);
        self
    }
}

impl<AC, MS> RuntimeBuilder<AC::Spec, AC::Val> for SemiSyncRuntimeBuilder<AC, MS>
where
    AC: AsyncConfig<Ctx = SemiSyncContext<AC>>,
    AC::Expr: DependencyGraphExpr,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
{
    type Runtime = SemiSyncRuntime<AC, MS>;

    fn new() -> Self {
        Self {
            executor: None,
            model: None,
            input: None,
            output: None,
            starting_history: None,
            _marker: std::marker::PhantomData,
        }
    }

    fn executor(mut self, executor: Rc<LocalExecutor<'static>>) -> Self {
        self.executor = Some(executor);
        self
    }

    fn model(mut self, model: AC::Spec) -> Self {
        self.model = Some(model);
        self
    }

    fn input(mut self, input: Box<dyn InputProvider<Val = AC::Val>>) -> Self {
        self.input = Some(input);
        self
    }

    fn output(mut self, output: Box<dyn OutputHandler<Val = AC::Val>>) -> Self {
        self.output = Some(output);
        self
    }

    fn build(self) -> LocalBoxFuture<'static, Self::Runtime> {
        Box::pin(async move {
            let executor = self.executor.unwrap();
            let model = self.model.unwrap();
            let input = self.input.unwrap();
            let output = self.output.unwrap();
            let starting_history = self.starting_history.unwrap_or_default();

            SemiSyncRuntime {
                _executor: executor,
                model,
                input_provider: input,
                output_handler: output,
                starting_history: starting_history,
                _marker: std::marker::PhantomData,
            }
        })
    }
}

#[derive(Debug, PartialEq)]
pub enum StreamState {
    Pending,
    Finished,
}

pub struct ExprEvalutor<AC, MS>
where
    AC: AsyncConfig,
    AC::Expr: DependencyGraphExpr,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
{
    // Sender that forwards it to the VarManager
    sender: spsc::Sender<AC::Val>,
    // Stream that evaluates the expression
    eval_stream: OutputStream<AC::Val>,
    var_name: VarName,

    _marker: std::marker::PhantomData<MS>,

    // Kept for debugging
    _expr: AC::Expr,
}

impl<AC, MS> ExprEvalutor<AC, MS>
where
    AC: AsyncConfig,
    AC::Expr: DependencyGraphExpr,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
{
    fn new(
        var_name: VarName,
        expr: AC::Expr,
        sender: spsc::Sender<AC::Val>,
        ctx: &AC::Ctx,
        starting_history: Vec<AC::Val>,
    ) -> Self {
        let hist_len = starting_history.len();
        let eval_stream = MS::to_async_stream(expr.clone(), ctx);
        let eval_stream: OutputStream<AC::Val> =
            Box::pin(futures::stream::iter(starting_history).chain(eval_stream.skip(hist_len)));
        Self {
            var_name,
            _expr: expr,
            sender,
            eval_stream,
            _marker: std::marker::PhantomData,
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
            // Drop streams and channels to propagate the news
            // TODO: Make this more clean
            self.eval_stream = Box::pin(futures::stream::empty());
            (self.sender, _) = spsc::channel::<AC::Val>(1);
            Ok(StreamState::Finished)
        }
    }
}

/// Manages retained history for a variable stream
/// Is essentially a circular buffer that can only increase in capacity, plus some more logic.
struct RetainedHistory<T: DeferrableStreamData> {
    history: VecDeque<T>,
    capacity: usize,
    samples_seen: usize,
}

impl<T: DeferrableStreamData> RetainedHistory<T> {
    fn new(capacity: usize) -> Self {
        Self {
            history: VecDeque::new(),
            capacity,
            samples_seen: 0,
        }
    }

    fn prune_old(&mut self) {
        while self.history.len() > self.capacity {
            self.history.pop_front();
        }
    }

    /// Adds a new value to the history, maintaining the capacity limit
    fn push(&mut self, value: T) {
        self.samples_seen += 1;

        if self.capacity == 0 {
            return;
        }

        self.history.push_back(value);
        self.prune_old();
    }

    /// Gets the last N values from history, padding with deferred values if needed
    /// NOTE: I was arguing whether this should be NoVal or Deferred but ended with Deferred.
    /// Reason: They are requesting N values but we don't have enough context. This is exactly
    /// what Deferred means.
    /// Also, the context where this becomes relevant is when we have a new subscriber that
    /// requests more history than what we currently store, which is only the case for DUPs that
    /// introduce time indices.
    fn get_last_n_with_pad(&self, n: usize) -> Vec<T> {
        let available = self.history.len();
        let to_send = match n {
            0 => return Vec::new(),
            // Case below means that we have received context from somewhere else - e.g., context_transfer
            n if available >= self.samples_seen => std::cmp::min(available, n),
            n => std::cmp::min(self.samples_seen, n),
        };

        let mut result = Vec::with_capacity(to_send);

        // Add deferred values if we need padding
        let padding_needed = to_send.saturating_sub(available);
        for _ in 0..padding_needed {
            result.push(T::deferred_value());
        }

        // Add actual history values
        let skip = available.saturating_sub(to_send - padding_needed);
        result.extend(self.history.iter().skip(skip).cloned());

        result
    }

    /// Increases the capacity
    fn increase_capacity(&mut self, new_capacity: usize) {
        if new_capacity <= self.capacity {
            return;
        }

        self.capacity = new_capacity;
    }

    fn get_all(&self) -> Vec<T> {
        self.history.iter().cloned().collect()
    }
}

struct VarManager<AC>
where
    AC: AsyncConfig,
    AC::Expr: DependencyGraphExpr,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
{
    // VarName this manages
    var_name: VarName,
    // Stream where Values are received
    value_stream: OutputStream<AC::Val>,
    // Subscribers to this specific variable
    subscribers: Vec<spsc::Sender<AC::Val>>,
    new_subscribers: Vec<(spsc::Sender<AC::Val>, usize)>, // (Sender, history_length)
    // Retained history of values (if needed)
    retained_history: RetainedHistory<AC::Val>,
    id: usize,
}

impl<AC> VarManager<AC>
where
    AC: AsyncConfig,
    AC::Expr: DependencyGraphExpr,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
{
    fn new(var_name: VarName, value_stream: OutputStream<AC::Val>) -> Self {
        let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        debug!(?var_name, "Creating VarManager {}", id);
        Self {
            var_name,
            value_stream,
            subscribers: Vec::new(),
            new_subscribers: Vec::new(),
            retained_history: RetainedHistory::new(0),
            id,
        }
    }

    fn new_from_receiver(var_name: VarName, receiver: spsc::Receiver<AC::Val>) -> Self {
        let value_stream = stream_utils::channel_to_output_stream(receiver);
        Self::new(var_name, value_stream)
    }

    fn subscribe(&mut self, history_length: usize) -> OutputStream<AC::Val> {
        let (tx, rx) = spsc::channel::<AC::Val>(history_length + CHANNEL_SIZE);

        // Increase capacity if the new subscriber needs more history
        self.retained_history.increase_capacity(history_length);

        info!(
            ?self.var_name,
            history_length,
            history_items = ?self.retained_history.history.len(),
            "VarManager {} subscribe: Preparing subscription.",
            self.id,
        );

        // Add to new subs - history will be sent next time we forward values
        self.new_subscribers.push((tx, history_length));

        stream_utils::channel_to_output_stream(rx)
    }

    /// Processes new subscribers and sends them the requested history
    fn process_new_subscribers(&mut self) {
        while let Some((mut tx, history_length)) = self.new_subscribers.pop() {
            let history_to_send = self.retained_history.get_last_n_with_pad(history_length);
            debug!(
                ?self.var_name,
                ?history_length,
                ?history_to_send,
                "VarManager {} process_new_subscribers: Sending retained history to new subscriber.",
                self.id,
            );

            for v in history_to_send {
                // Should never fail as the capacity is always sufficient
                if let Err(e) = tx.try_send(v) {
                    error!(
                        ?self.var_name,
                        ?e,
                        "VarManager {} process_new_subscribers: Error sending retained history value to new subscriber.",
                        self.id,
                    );
                    panic!(
                        "Error sending retained history value to new subscriber: {}",
                        e
                    );
                }
            }

            self.subscribers.push(tx);
        }
    }

    /// Forwards a value to all subscribers and removes disconnected ones
    async fn broadcast_to_subscribers(&mut self, val: AC::Val) {
        let mut disconnected = vec![];
        for (idx, subscriber) in self.subscribers.iter_mut().enumerate() {
            if subscriber.send(val.clone()).await.is_err() {
                // The only type of error is disconnection
                info!(
                    "VarManager {} broadcast_to_subscribers: Subscriber {} disconnected.",
                    self.id, idx
                );
                disconnected.push(idx);
            }
        }

        // Remove disconnected subscribers in reverse order to preserve indices
        for idx in disconnected.into_iter().rev() {
            self.subscribers.remove(idx);
        }
    }

    async fn forward_value(&mut self) -> anyhow::Result<StreamState> {
        info!(
            ?self.var_name,
            history_capacity = self.retained_history.capacity,
            "VarManager {} forward_value: Waiting for next value.",
            self.id,
        );

        // Process any new subscribers that need history
        self.process_new_subscribers();

        if let Some(val) = self.value_stream.next().await {
            info!(
                ?self.var_name,
                ?val,
                "VarManager {} forward_value: Forwarding value to {} subscribers.",
                self.id,
                self.subscribers.len()
            );

            // Add to retained history
            self.retained_history.push(val.clone());

            info!(
                ?self.var_name,
                "VarManager {} forward_value: Updated retained history.",
                self.id,
            );

            // Broadcast to all subscribers
            self.broadcast_to_subscribers(val).await;

            // Stream not done yet
            Ok(StreamState::Pending)
        } else {
            info!(?self.var_name, "VarManager {} stream finished", self.id);
            // Close the channels early to let receivers know we are done
            self.subscribers.clear();
            self.new_subscribers.clear();
            Ok(StreamState::Finished)
        }
    }

    // Gets the retained history for the VarManager
    fn get_retained_history(&self) -> Vec<AC::Val> {
        self.retained_history.get_all()
    }

    fn set_history_to_retain(&mut self, history_to_retain: usize) {
        self.retained_history.capacity = history_to_retain;
    }

    fn cancel(&mut self) {
        self.value_stream = Box::pin(futures::stream::empty());
        self.subscribers.clear();
        self.new_subscribers.clear();
    }
}

pub struct SemiSyncRuntime<AC, MS>
where
    AC: AsyncConfig,
    AC::Expr: DependencyGraphExpr,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
{
    _executor: Rc<LocalExecutor<'static>>,
    model: AC::Spec,
    input_provider: Box<dyn InputProvider<Val = AC::Val>>,
    output_handler: Box<dyn OutputHandler<Val = AC::Val>>,
    starting_history: BTreeMap<VarName, Vec<AC::Val>>,
    _marker: std::marker::PhantomData<MS>,
}

impl<AC, MS> SemiSyncRuntime<AC, MS>
where
    AC: AsyncConfig<Ctx = SemiSyncContext<AC>>,
    AC::Expr: DependencyGraphExpr,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
{
    pub async fn new(
        executor: Rc<LocalExecutor<'static>>,
        model: AC::Spec,
        input: Box<dyn InputProvider<Val = AC::Val>>,
        output: Box<dyn OutputHandler<Val = AC::Val>>,
    ) -> Self {
        SemiSyncRuntimeBuilder::new()
            .executor(executor)
            .model(model)
            .input(input)
            .output(output)
            .build()
            .await
    }

    fn setup_input_streams(
        model: &AC::Spec,
        input_provider: &mut dyn InputProvider<Val = AC::Val>,
    ) -> BTreeMap<VarName, OutputStream<AC::Val>> {
        model
            .input_vars()
            .iter()
            .map(|var| {
                let stream = input_provider.var_stream(var);
                (
                    var.clone(),
                    stream.expect(&format!(
                        "Input stream unavailable for input variable: {}",
                        var
                    )),
                )
            })
            .collect()
    }

    fn setup_computed_var_managers(
        model: &AC::Spec,
    ) -> anyhow::Result<(
        Vec<(VarName, AC::Expr, spsc::Sender<AC::Val>)>,
        Vec<VarManager<AC>>,
    )> {
        let stream_vars = model.stream_vars();
        stream_vars
            .iter()
            .map(|var_name| {
                let (sender, receiver): (spsc::Sender<AC::Val>, spsc::Receiver<AC::Val>) =
                    spsc::channel(CHANNEL_SIZE);
                let var_manager = VarManager::<AC>::new_from_receiver(var_name.clone(), receiver);
                let expr = model.var_expr(var_name).ok_or_else(|| {
                    anyhow!(
                        "No expression found for computed variable {} when setting up Monitor",
                        var_name
                    )
                })?;
                Ok(((var_name.clone(), expr, sender), var_manager))
            })
            .collect::<anyhow::Result<Vec<_>>>()
            .map(|entries| entries.into_iter().unzip())
    }

    fn build_context(
        var_managers: Vec<VarManager<AC>>,
        input_streams: BTreeMap<VarName, OutputStream<AC::Val>>,
        spec: AC::Spec,
    ) -> SemiSyncContext<AC> {
        let var_managers = var_managers
            .into_iter()
            .chain(
                input_streams
                    .into_iter()
                    .map(|(var_name, stream)| VarManager::<AC>::new(var_name, stream)),
            )
            .map(|vm| (vm.var_name.clone(), vm))
            .collect();

        SemiSyncContextBuilder::new()
            .var_managers(var_managers)
            .spec(spec)
            .build()
    }

    fn log_when_done<Fut, T>(fut: Fut, msg: &'static str) -> impl futures::Future<Output = T>
    where
        Fut: futures::Future<Output = T>,
    {
        fut.map(move |res| {
            info!("{}", msg);
            res
        })
        .fuse()
    }

    pub async fn input_task(
        input_provider: &mut dyn InputProvider<Val = AC::Val>,
    ) -> anyhow::Result<()> {
        let mut input_provider_stream = input_provider.control_stream().await;
        while let Some(res) = input_provider_stream.next().await {
            if res.is_err() {
                error!(
                    "SemiSyncRuntime: Input provider stream returned error: {:?}",
                    res
                );
                return res;
            }
        }
        Ok(())
    }

    fn log_task_errors(
        output_res: &anyhow::Result<()>,
        work_res: &anyhow::Result<()>,
        input_res: &anyhow::Result<()>,
    ) {
        if let Err(e) = output_res {
            error!(?e, "Output handler had an error");
        }
        if let Err(e) = work_res {
            error!(?e, "Work task had an error");
        }
        if let Err(e) = input_res {
            error!(?e, "Input task had an error");
        }
    }

    async fn eval_expr_evals(
        expr_evals: &mut Vec<ExprEvalutor<AC, MS>>,
        aux_vars: &BTreeSet<VarName>,
    ) -> anyhow::Result<StreamState> {
        let mut to_remove = vec![];
        let futures = expr_evals.iter_mut().map(|expr_eval| async {
            let name = expr_eval.var_name.clone();
            let res = expr_eval.eval_value().await;
            (name, res)
        });

        let results = join_all(futures).await;

        for (name, res) in results {
            match res {
                Ok(StreamState::Pending) => {
                    info!(?name, "eval_expr_evals: ExprEvaluator pending");
                }
                Ok(StreamState::Finished) => {
                    info!(?name, "eval_expr_evals: ExprEvaluator finished");
                    to_remove.push(name);
                }
                Err(e) => {
                    error!(?name, ?e, "eval_expr_evals: Error in ExprEvaluator");
                    return Err(anyhow!(
                        "Error in ExprEvaluator for variable {}: {}",
                        name,
                        e
                    ));
                }
            }
        }
        expr_evals.retain(|expr_eval| !to_remove.contains(&expr_eval.var_name));

        // If any non-aux eval'er is active:
        if expr_evals
            .iter()
            .any(|expr_eval| !aux_vars.contains(&expr_eval.var_name))
        {
            return Ok(StreamState::Pending);
        }
        // Else done
        info!("All ExprEvalutors finished");
        Ok(StreamState::Finished)
    }

    pub async fn step(
        ctx: &mut SemiSyncContext<AC>,
        expr_evals: &mut Vec<ExprEvalutor<AC, MS>>,
    ) -> anyhow::Result<StreamState> {
        info!("SemiSyncRuntime work_task: Waiting for next tick...");
        let step_started = Instant::now();
        let aux_vars = ctx.aux_vars.clone();
        let expr_evaluator_count = expr_evals.len();
        let non_aux_expr_evaluator_count = expr_evals
            .iter()
            .filter(|expr_eval| !aux_vars.contains(&expr_eval.var_name))
            .count();
        let result = futures::join!(
            async {
                let started = Instant::now();
                let result = ctx.forward_values().await;
                (result, started.elapsed().as_secs_f64() * 1000.0)
            },
            async {
                let started = Instant::now();
                let result = Self::eval_expr_evals(expr_evals, &aux_vars).await;
                (result, started.elapsed().as_secs_f64() * 1000.0)
            }
        );
        let ((forward_result, forward_values_duration_ms), (eval_result, eval_expr_duration_ms)) =
            result;
        let total_duration_ms = step_started.elapsed().as_secs_f64() * 1000.0;
        let forward_state = stream_state_name(forward_result.as_ref().ok());
        let eval_state = stream_state_name(eval_result.as_ref().ok());

        // ACSOS paper benchmark instrumentation: log additional timing information
        warn!(
            target: "benchmark",
            event = "benchmark_monitoring_step",
            context_id = ctx.id,
            expr_evaluator_count,
            non_aux_expr_evaluator_count,
            aux_var_count = aux_vars.len(),
            forward_state,
            eval_state,
            forward_values_duration_ms,
            eval_expr_duration_ms,
            duration_ms = total_duration_ms,
            "benchmark_monitoring_step"
        );

        // A bit verbose but it is nice for debugging...
        match (forward_result, eval_result) {
            (Ok(StreamState::Pending), Ok(StreamState::Pending)) => {
                debug!(
                    "SemiSyncRuntime work_task: Both forward_values and eval_expr_evals pending, continuing..."
                );
                Ok(StreamState::Pending)
            }
            (Ok(StreamState::Finished), Ok(StreamState::Finished)) => {
                debug!(
                    "SemiSyncRuntime work_task: Both forward_values and eval_expr_evals finished, ending work_task."
                );
                Ok(StreamState::Finished)
            }
            (Ok(StreamState::Pending), Ok(StreamState::Finished)) => {
                error!(
                    "SemiSyncRuntime work_task: eval_expr_evals finished but forward_values pending"
                );
                Err(anyhow!(
                    "eval_expr_evals finished but forward_values pending"
                ))
            }
            (Ok(StreamState::Finished), Ok(StreamState::Pending)) => {
                error!(
                    "SemiSyncRuntime work_task: forward_values finished but eval_expr_evals pending"
                );
                Err(anyhow!(
                    "forward_values finished but eval_expr_evals pending"
                ))
            }
            (Ok(_), Err(e)) => {
                error!(?e, "SemiSyncRuntime work_task: Error in eval_expr_evals");
                Err(e)
            }
            (Err(e), Ok(_)) => {
                error!(?e, "SemiSyncRuntime work_task: Error in ctx.forward_values");
                Err(e)
            }
            (Err(e1), Err(e2)) => {
                error!(
                    ?e1,
                    ?e2,
                    "SemiSyncRuntime work_task: Errors in both ctx.forward_values and eval_expr_evals"
                );
                Err(anyhow!("Errors in work_task: {}, {}", e1, e2))
            }
        }
    }

    pub async fn work_task(
        mut ctx: SemiSyncContext<AC>,
        mut expr_evals: Vec<ExprEvalutor<AC, MS>>,
    ) -> anyhow::Result<()> {
        let mut res = Self::step(&mut ctx, &mut expr_evals).await?;
        while res != StreamState::Finished {
            res = Self::step(&mut ctx, &mut expr_evals).await?;
        }
        Ok(())
    }

    pub async fn setup_runtime(
        self,
    ) -> anyhow::Result<(
        Box<dyn InputProvider<Val = AC::Val>>,
        Box<dyn OutputHandler<Val = AC::Val>>,
        SemiSyncContext<AC>,
        Vec<ExprEvalutor<AC, MS>>,
    )> {
        let SemiSyncRuntime {
            _executor: _,
            model,
            mut input_provider,
            mut output_handler,
            starting_history,
            _marker: _,
        } = self;
        // Assert: All history lengths are the same:
        let hist_len = starting_history
            .first_key_value()
            .map(|(_, hist)| hist.len())
            .unwrap_or(0);
        assert!(
            starting_history
                .iter()
                .all(|(_, hist)| hist.len() == hist_len),
            "All history lengths must be the same"
        );

        info!(
            "Setting up runtime with starting history: {:?}",
            starting_history
        );

        let mut input_streams = Self::setup_input_streams(&model, &mut *input_provider);
        let input_vars = model.input_vars().iter().cloned().collect::<BTreeSet<_>>();
        for var in input_vars.iter() {
            // NOTE: Using NoVal here, because this indicates the start of a new trace where no
            // values have previously been received on the stream.
            // Also, Deferred messes up signal semantics outputs. E.g., z = x + y and
            let hist = starting_history
                .get(var)
                .cloned()
                .unwrap_or_else(|| vec![AC::Val::no_val_value(); hist_len]);
            let stream = input_streams
                .remove(var)
                .expect(&format!("Input stream for variable {} not found", var));
            let prefixed: OutputStream<AC::Val> =
                Box::pin(futures::stream::iter(hist.clone()).chain(stream));
            input_streams.insert(var.clone(), prefixed);
        }

        let output_vars = model.output_vars();
        let (expr_eval_components, mut var_managers) = Self::setup_computed_var_managers(&model)?;
        let mut subscriptions: BTreeMap<VarName, OutputStream<AC::Val>> = var_managers
            .iter_mut()
            .filter_map(|vm| {
                output_vars.contains(&vm.var_name).then(|| {
                    let var_name = vm.var_name.clone();
                    let stream = vm.subscribe(0);
                    (var_name, stream)
                })
            })
            .collect();
        let mut context = Self::build_context(var_managers, input_streams, model.clone());
        let mut expr_evals = expr_eval_components
            .into_iter()
            .map(|(var_name, expr, sender)| {
                let hist = starting_history
                    .get(&var_name)
                    .cloned()
                    .unwrap_or_else(|| vec![AC::Val::no_val_value(); hist_len]);
                ExprEvalutor::new(var_name, expr, sender, &context, hist)
            })
            .collect();
        for _ in 0..hist_len {
            Self::step(&mut context, &mut expr_evals)
                .await
                .expect("Step failed when syncing starting history");
            for (_, sub) in subscriptions.iter_mut() {
                // Drain the subscription to sync it up with the context
                let _ = sub.next().await;
            }
        }
        info!("Finished syncing starting history of length {}", hist_len,);
        output_handler.provide_streams(subscriptions);
        Ok((input_provider, output_handler, context, expr_evals))
    }
}

fn stream_state_name(state: Option<&StreamState>) -> &'static str {
    match state {
        Some(StreamState::Pending) => "pending",
        Some(StreamState::Finished) => "finished",
        None => "error",
    }
}

#[async_trait(?Send)]
impl<AC, MS> Runtime for SemiSyncRuntime<AC, MS>
where
    AC: AsyncConfig<Ctx = SemiSyncContext<AC>>,
    AC::Expr: DependencyGraphExpr,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
{
    async fn run_boxed(mut self: Box<Self>) -> anyhow::Result<()> {
        debug!(?self.model, "Running SemiSyncMonitor based on model:");

        let (mut input_provider, mut output_handler, context, expr_evals) =
            Self::setup_runtime(*self).await?;

        let output_fut = Self::log_when_done(output_handler.run(), "output_handler.run() ended");
        let work_fut = Self::log_when_done(
            Self::work_task(context, expr_evals),
            "work_task.run() ended",
        );
        let input_fut = Self::log_when_done(
            Self::input_task(&mut *input_provider),
            "input_provider ended",
        );

        let (output_res, work_res, input_res) = futures::join!(output_fut, work_fut, input_fut);
        Self::log_task_errors(&output_res, &work_res, &input_res);

        match (output_res, work_res, input_res) {
            (Ok(_), Ok(_), Ok(_)) => Ok(()),
            (Err(e), _, _) => Err(anyhow!("OutputHandler failed with error: {}", e)),
            (_, Err(e), _) => Err(anyhow!("SemiSync work task failed with error: {}", e)),
            (_, _, Err(e)) => Err(anyhow!("InputProvider failed with error: {}", e)),
        }
    }
}

pub struct SemiSyncContextBuilder<AC>
where
    AC: AsyncConfig,
    AC::Expr: DependencyGraphExpr,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
{
    var_managers: Option<BTreeMap<VarName, VarManager<AC>>>,
    spec: Option<AC::Spec>,
    cancellation_token: Option<CancellationToken>,
}

impl<AC> SemiSyncContextBuilder<AC>
where
    AC: AsyncConfig,
    AC::Expr: DependencyGraphExpr,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
{
    fn var_managers(self, var_managers: BTreeMap<VarName, VarManager<AC>>) -> Self {
        Self {
            var_managers: Some(var_managers),
            ..self
        }
    }
    fn spec(self, spec: AC::Spec) -> Self {
        Self {
            spec: Some(spec),
            ..self
        }
    }

    fn cancellation_token(self, cancellation_token: CancellationToken) -> Self {
        Self {
            cancellation_token: Some(cancellation_token),
            ..self
        }
    }
}

impl<AC> AbstractContextBuilder for SemiSyncContextBuilder<AC>
where
    AC: AsyncConfig<Ctx = SemiSyncContext<AC>>,
    AC::Expr: DependencyGraphExpr,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
{
    type AC = AC;

    fn new() -> Self {
        Self {
            var_managers: None,
            spec: None,
            cancellation_token: None,
        }
    }

    fn executor(self, _executor: Rc<LocalExecutor<'static>>) -> Self {
        todo!()
    }

    fn var_names(self, _var_names: Vec<VarName>) -> Self {
        todo!()
    }

    fn history_length(self, _history_length: usize) -> Self {
        todo!()
    }

    fn input_streams(self, _streams: Vec<OutputStream<<Self::AC as AsyncConfig>::Val>>) -> Self {
        todo!()
    }

    fn partial_clone(&self) -> Self {
        todo!()
    }

    fn build(self) -> <Self::AC as AsyncConfig>::Ctx {
        let ctx = SemiSyncContext::new_with_token(
            Rc::new(RefCell::new(self.var_managers.expect(
                "VarManagers must be set before building SemiSyncContext",
            ))),
            self.spec
                .expect("Spec must be set before building SemiSyncContext"),
            self.cancellation_token
                .unwrap_or_else(CancellationToken::new),
        );
        info!(
            "SemiSyncContextBuilder: Built SemiSyncContext with id {:?} and VarManagers: {:?}",
            ctx.id,
            ctx.var_managers.borrow().keys()
        );
        ctx
    }
}

static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

pub struct SemiSyncContext<AC>
where
    AC: AsyncConfig,
    AC::Expr: DependencyGraphExpr,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
{
    // Rc RefCell because of the StreamContext interface for Var...
    var_managers: Rc<RefCell<BTreeMap<VarName, VarManager<AC>>>>,
    // Unique identifier for this variable manager
    id: usize,
    // The specification for this context
    spec: AC::Spec,
    // Dependencies from the specification
    deps: Box<dyn DependencyResolver<AC>>,
    aux_vars: BTreeSet<VarName>,
    cancellation_token: CancellationToken,
}

impl<AC> SemiSyncContext<AC>
where
    AC: AsyncConfig<Ctx = SemiSyncContext<AC>>,
    AC::Expr: DependencyGraphExpr,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
{
    fn new_with_token(
        var_managers: Rc<RefCell<BTreeMap<VarName, VarManager<AC>>>>,
        spec: AC::Spec,
        cancellation_token: CancellationToken,
    ) -> Self {
        let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        debug!(
            "Creating SemiSyncContext {:?} with vars: {:?}",
            id,
            var_managers.borrow().keys()
        );
        let deps = Box::new(DepGraph::resolver_from_spec(spec.clone()));
        var_managers.borrow_mut().iter_mut().for_each(|(name, vm)| {
            let dep = deps.longest_time_dependency(&name);
            debug!(
                "SemiSyncContext ID {:?}: Setting history to retain for variable {} to {} based on dependencies.",
                id,
                name,
                dep
            );
            vm.set_history_to_retain(dep as usize);
        });
        let aux_vars = spec.aux_vars();
        Self {
            var_managers,
            id,
            spec,
            deps,
            aux_vars,
            cancellation_token,
        }
    }

    async fn forward_values(&mut self) -> anyhow::Result<StreamState> {
        // Helper func to run the logic for awaiting manager.
        async fn forward_one<AC>(
            name: &VarName,
            manager: &mut VarManager<AC>,
        ) -> anyhow::Result<StreamState>
        where
            AC: AsyncConfig<Ctx = SemiSyncContext<AC>>,
            AC::Expr: DependencyGraphExpr,
            AC::Spec: DependencyGraphSpec,
            AC::Val: DeferrableStreamData,
        {
            match manager.forward_value().await {
                Ok(StreamState::Pending) => Ok(StreamState::Pending),
                Ok(StreamState::Finished) => {
                    info!(?name, "forward_values: VarManager finished");
                    manager.subscribers.clear();
                    Ok(StreamState::Finished)
                }
                Err(e) => {
                    error!(?name, ?e, "forward_values: Error in VarManager");
                    Err(e)
                }
            }
        }

        let mut managers = self.var_managers.borrow_mut();
        let mut to_remove = vec![];
        let futs = managers.iter_mut().map(|(name, manager)| async {
            let res = forward_one::<AC>(name, manager).await;
            (name.clone(), res)
        });
        let input_results = join_all(futs).await;
        for (name, res) in input_results {
            if res? == StreamState::Finished {
                to_remove.push(name);
            }
        }
        managers.retain(|name, _| !to_remove.contains(name));
        // If any non-aux manager is active:
        if managers.keys().any(|key| !self.aux_vars.contains(key)) {
            return Ok(StreamState::Pending);
        }
        // Else done
        info!(
            "SemiSyncContext ID: {:?}: All VarManagers finished",
            self.id
        );
        Ok(StreamState::Finished)
    }

    fn subcontext_common(&self, vs: EcoVec<VarName>) -> Self {
        let mut managers = self.var_managers.borrow_mut();
        let new_managers = managers
            .iter_mut()
            .filter_map(|(var_name, manager)| {
                if vs.contains(var_name) {
                    let history_length = self.deps.longest_time_dependency(var_name) as usize;
                    let stream = manager.subscribe(history_length);
                    Some((
                        var_name.clone(),
                        VarManager::<AC>::new(var_name.clone(), stream),
                    ))
                } else {
                    None
                }
            })
            .collect::<BTreeMap<_, _>>();
        debug!(
            "SemiSyncContext ID {:?}: Subcontext var_managers: {:?}",
            self.id,
            new_managers.keys()
        );
        let builder = SemiSyncContextBuilder::new()
            .var_managers(new_managers)
            .spec(self.spec.clone())
            .cancellation_token(self.cancellation_token.clone());
        builder.build()
    }

    // Gets the retained history for all the variables inside the context.
    pub fn get_retained_history(&self) -> BTreeMap<VarName, Vec<AC::Val>> {
        let managers = self.var_managers.borrow();
        managers
            .iter()
            .map(|(var_name, manager)| (var_name.clone(), manager.get_retained_history()))
            .collect()
    }
}

#[async_trait(?Send)]
impl<AC> StreamContext for SemiSyncContext<AC>
where
    AC: AsyncConfig<Ctx = SemiSyncContext<AC>>,
    AC::Expr: DependencyGraphExpr,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
{
    type AC = AC;
    type Builder = SemiSyncContextBuilder<AC>;

    fn var(&self, x: &VarName) -> Option<OutputStream<AC::Val>> {
        debug!(
            "SemiSyncContext ID {:?}: Requesting variable {}",
            self.id, x
        );
        let mut manager = self.var_managers.borrow_mut();
        let history_length = self.deps.longest_time_dependency(x) as usize;
        let stream = manager.get_mut(x)?.subscribe(history_length);
        info!(
            self.id,
            ?x,
            ?history_length,
            "SemiSyncContext::var: Created new output stream for variable"
        );
        Some(stream)
    }

    fn subcontext(&self, _history_length: usize) -> Self {
        info!(self.id, "SemiSyncContext::subcontext: Creating subcontext.");
        // Note: Must be in separate variable to avoid double borrow
        let vars = self
            .var_managers
            .borrow()
            .keys()
            .cloned()
            .collect::<EcoVec<VarName>>();
        self.subcontext_common(vars)
    }

    fn restricted_subcontext(&self, vs: EcoVec<VarName>, _history_length: usize) -> Self {
        info!(
            ?vs,
            "SemiSyncContext::restricted_subcontext: Creating restricted subcontext with parent id: {}",
            self.id
        );
        self.subcontext_common(vs)
    }

    fn subcontext_excluding(&self, excluded: &VarName, _history_length: usize) -> Self {
        let vars = self
            .var_managers
            .borrow()
            .keys()
            .filter(|var| *var != excluded)
            .cloned()
            .collect();
        self.subcontext_common(vars)
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

    fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    fn cancel(&self) {
        self.cancellation_token.cancel();
        for manager in self.var_managers.borrow_mut().values_mut() {
            manager.cancel();
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::async_test;
    use crate::core::{Runtime, Specification};
    use crate::io::map::MapInputProvider;
    use crate::io::testing::{ManualOutputHandler, NullOutputHandler};
    use crate::lang::dsrv::lalr_parser::LALRParser;
    use crate::lang::dsrv::type_checker::type_check;
    use crate::runtime::builder::{SemiSyncValueConfig, TypedSemiSyncValueConfig};
    use crate::runtime::semi_sync::SemiSyncRuntime;
    use crate::semantics::{TypedUntimedDsrvSemantics, UntimedDsrvSemantics};
    use crate::{Value, dsrv_specification};
    use crate::{VarName, dsrv_fixtures::*};
    use futures::stream::StreamExt;
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::collections::BTreeMap;
    use std::rc::Rc;

    use tc_testutils::streams::{with_timeout, with_timeout_res};

    type TestRuntime = SemiSyncRuntime<SemiSyncValueConfig, UntimedDsrvSemantics<LALRParser>>;
    type TestTypedRuntime =
        SemiSyncRuntime<TypedSemiSyncValueConfig, TypedUntimedDsrvSemantics<LALRParser>>;

    #[apply(async_test)]
    async fn test_simple_add(executor: Rc<LocalExecutor<'static>>) {
        let spec = dsrv_specification(&mut spec_simple_add_monitor()).unwrap();

        let x = vec![0.into(), 1.into(), 2.into()];
        let y = vec![3.into(), 4.into(), 5.into()];
        let input_streams =
            MapInputProvider::new(BTreeMap::from([("x".into(), x), ("y".into(), y)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let monitor = TestRuntime {
            _executor: executor.clone(),
            model: spec.clone(),
            input_provider: Box::new(input_streams),
            output_handler,
            starting_history: BTreeMap::new(),
            _marker: std::marker::PhantomData,
        };

        executor.spawn(monitor.run()).detach();

        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 1, "outputs")
                .await
                .unwrap();

        assert_eq!(outputs.len(), 3,);
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), 3.into())])),
                (1, BTreeMap::from([("z".into(), 5.into())])),
                (2, BTreeMap::from([("z".into(), 7.into())])),
            ],
        );
    }

    #[apply(async_test)]
    async fn test_typed_simple_add(executor: Rc<LocalExecutor<'static>>) {
        let spec = dsrv_specification(&mut spec_simple_add_monitor_typed()).unwrap();
        let spec = type_check(spec).expect("typed simple add spec should type check");

        let x = vec![0.into(), 1.into(), 2.into()];
        let y = vec![3.into(), 4.into(), 5.into()];
        let input_streams =
            MapInputProvider::new(BTreeMap::from([("x".into(), x), ("y".into(), y)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let monitor = TestTypedRuntime {
            _executor: executor.clone(),
            model: spec.clone(),
            input_provider: Box::new(input_streams),
            output_handler,
            starting_history: BTreeMap::new(),
            _marker: std::marker::PhantomData,
        };

        executor.spawn(monitor.run()).detach();

        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 1, "typed outputs")
                .await
                .unwrap();

        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), 3.into())])),
                (1, BTreeMap::from([("z".into(), 5.into())])),
                (2, BTreeMap::from([("z".into(), 7.into())])),
            ],
        );
    }

    #[apply(async_test)]
    async fn test_simple_add_null_handler(executor: Rc<LocalExecutor<'static>>) {
        // Testing that the monitor works with a NullOutputHandler
        // (to avoid previous regressions)
        let spec = dsrv_specification(&mut spec_simple_add_monitor()).unwrap();

        let x = vec![0.into(), 1.into(), 2.into()];
        let y = vec![3.into(), 4.into(), 5.into()];
        let input_streams =
            MapInputProvider::new(BTreeMap::from([("x".into(), x), ("y".into(), y)]));
        let output_handler = Box::new(NullOutputHandler::new(
            executor.clone(),
            spec.output_vars.clone(),
        ));
        let monitor = TestRuntime {
            _executor: executor.clone(),
            model: spec.clone(),
            input_provider: Box::new(input_streams),
            output_handler,
            starting_history: BTreeMap::new(),
            _marker: std::marker::PhantomData,
        };

        with_timeout_res(monitor.run(), 1, "monitor run")
            .await
            .unwrap();
    }

    #[apply(async_test)]
    async fn test_dependent_outputs(executor: Rc<LocalExecutor<'static>>) {
        // Tests that monitor correctly shuts down when there are multiple outputs that depend
        // on each other
        // (There was a bug where output stream cancellation did not propagate properly)
        let mut spec = "in x\nout a\nout b\na = x\nb = a + 1";
        let spec = dsrv_specification(&mut spec).unwrap();

        let x = vec![0.into(), 1.into(), 2.into()];
        let input_streams = MapInputProvider::new(BTreeMap::from([("x".into(), x)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let monitor = TestRuntime {
            _executor: executor.clone(),
            model: spec.clone(),
            input_provider: Box::new(input_streams),
            output_handler,
            starting_history: BTreeMap::new(),
            _marker: std::marker::PhantomData,
        };

        executor.spawn(monitor.run()).detach();

        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 1, "outputs")
                .await
                .unwrap();

        assert_eq!(outputs.len(), 3,);
        assert_eq!(
            outputs,
            vec![
                (
                    0,
                    BTreeMap::from([("a".into(), 0.into()), ("b".into(), 1.into())])
                ),
                (
                    1,
                    BTreeMap::from([("a".into(), 1.into()), ("b".into(), 2.into())])
                ),
                (
                    2,
                    BTreeMap::from([("a".into(), 2.into()), ("b".into(), 3.into())])
                ),
            ],
        );
    }

    #[apply(async_test)]
    async fn test_dynamic(executor: Rc<LocalExecutor<'static>>) {
        let spec = dsrv_specification(&mut spec_dynamic()).unwrap();

        let x = vec![0.into(), 1.into(), 2.into()];
        let e = vec!["x + 1".into(), "x + 2".into(), "x + 3".into()];
        let input_streams =
            MapInputProvider::new(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let monitor = TestRuntime {
            _executor: executor.clone(),
            model: spec.clone(),
            input_provider: Box::new(input_streams),
            output_handler,
            starting_history: BTreeMap::new(),
            _marker: std::marker::PhantomData,
        };

        executor.spawn(monitor.run()).detach();

        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 1, "outputs")
                .await
                .unwrap();

        assert_eq!(outputs.len(), 3,);
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), 1.into())])),
                (1, BTreeMap::from([("z".into(), 3.into())])),
                (2, BTreeMap::from([("z".into(), 5.into())])),
            ],
        );
    }

    #[apply(async_test)]
    async fn test_defer_single(executor: Rc<LocalExecutor<'static>>) {
        let spec = dsrv_specification(&mut spec_defer()).unwrap();

        let x = vec![0.into(), 1.into(), 2.into()];
        let e = vec!["x + 1".into(), Value::Deferred, Value::Deferred];
        let input_streams =
            MapInputProvider::new(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let monitor = TestRuntime {
            _executor: executor.clone(),
            model: spec.clone(),
            input_provider: Box::new(input_streams),
            output_handler,
            starting_history: BTreeMap::new(),
            _marker: std::marker::PhantomData,
        };

        executor.spawn(monitor.run()).detach();

        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 1, "outputs")
                .await
                .unwrap();

        assert_eq!(outputs.len(), 3,);
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), 1.into())])),
                (1, BTreeMap::from([("z".into(), 2.into())])),
                (2, BTreeMap::from([("z".into(), 3.into())])),
            ],
        );
    }

    #[apply(async_test)]
    async fn test_defer_multiple(executor: Rc<LocalExecutor<'static>>) {
        let spec = dsrv_specification(&mut spec_defer()).unwrap();

        let x = vec![0.into(), 1.into(), 2.into()];
        let e = vec!["x + 1".into(), "x + 2".into(), "x + 3".into()];
        let input_streams =
            MapInputProvider::new(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let monitor = TestRuntime {
            _executor: executor.clone(),
            model: spec.clone(),
            input_provider: Box::new(input_streams),
            output_handler,
            starting_history: BTreeMap::new(),
            _marker: std::marker::PhantomData,
        };

        executor.spawn(monitor.run()).detach();

        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 1, "outputs")
                .await
                .unwrap();

        assert_eq!(outputs.len(), 3,);
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), 1.into())])),
                (1, BTreeMap::from([("z".into(), 2.into())])),
                (2, BTreeMap::from([("z".into(), 3.into())])),
            ],
        );
    }

    #[apply(async_test)]
    async fn test_defer_delayed(executor: Rc<LocalExecutor<'static>>) {
        let spec = dsrv_specification(&mut spec_defer()).unwrap();

        let x = vec![0.into(), 1.into(), 2.into()];
        let e = vec![Value::Deferred, Value::Deferred, "x + 3".into()];
        let input_streams =
            MapInputProvider::new(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let monitor = TestRuntime {
            _executor: executor.clone(),
            model: spec.clone(),
            input_provider: Box::new(input_streams),
            output_handler,
            starting_history: BTreeMap::new(),
            _marker: std::marker::PhantomData,
        };

        executor.spawn(monitor.run()).detach();

        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 1, "outputs")
                .await
                .unwrap();

        assert_eq!(outputs.len(), 3,);
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), Value::Deferred)])),
                (1, BTreeMap::from([("z".into(), Value::Deferred)])),
                (2, BTreeMap::from([("z".into(), 5.into())])),
            ],
        );
    }

    #[apply(async_test)]
    async fn test_defer_sindex(executor: Rc<LocalExecutor<'static>>) {
        let spec = dsrv_specification(&mut spec_defer()).unwrap();

        let x = vec![0.into(), 1.into(), 2.into(), 3.into(), 4.into()];
        let e = vec![
            "x[2]".into(),
            Value::Deferred,
            Value::Deferred,
            Value::Deferred,
            Value::Deferred,
        ];
        let input_streams =
            MapInputProvider::new(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let monitor = TestRuntime {
            _executor: executor.clone(),
            model: spec.clone(),
            input_provider: Box::new(input_streams),
            output_handler,
            starting_history: BTreeMap::new(),
            _marker: std::marker::PhantomData,
        };

        executor.spawn(monitor.run()).detach();

        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 1, "outputs")
                .await
                .unwrap();

        assert_eq!(outputs.len(), 5,);
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), Value::Deferred)])),
                (1, BTreeMap::from([("z".into(), Value::Deferred)])),
                (2, BTreeMap::from([("z".into(), 0.into())])),
                (3, BTreeMap::from([("z".into(), 1.into())])),
                (4, BTreeMap::from([("z".into(), 2.into())])),
            ],
        );
    }

    #[apply(async_test)]
    async fn test_eval_order_regression(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        // This test is designed to catch a regression where the order of evaluation of outputs
        // was causing a deadlock

        // Naming is important here... Regression was caused by waiting for a before b
        let mut spec = "in x\nout a\naux b\nb = x\na = b";
        let spec = dsrv_specification(&mut spec).unwrap();

        let x = vec![0.into(), 1.into(), 2.into()];
        let input_streams = MapInputProvider::new(BTreeMap::from([("x".into(), x)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let monitor = TestRuntime {
            _executor: executor.clone(),
            model: spec.clone(),
            input_provider: Box::new(input_streams),
            output_handler,
            starting_history: BTreeMap::new(),
            _marker: std::marker::PhantomData,
        };

        executor.spawn(monitor.run()).detach();

        let outputs: Vec<(usize, BTreeMap<VarName, Value>)> =
            with_timeout(outputs.enumerate().collect(), 1, "outputs")
                .await
                .unwrap();

        assert_eq!(outputs.len(), 3,);
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("a".into(), 0.into())])),
                (1, BTreeMap::from([("a".into(), 1.into())])),
                (2, BTreeMap::from([("a".into(), 2.into())])),
            ],
        );
        Ok(())
    }

    // TODO: - MHK implement a similar test but for reconfig runtime when it is more testable.
    //
    // #[apply(async_test)]
    // async fn test_context_transfer(_executor: Rc<LocalExecutor<'static>>) {
    //     // Tests that context transfers work purely from a Context perspective (without actually
    //     // involving the monitor or outputs) with various time indices for the history.
    //
    //     // NOTE: The values used in these tests don't actually matter, as long as history for z
    //     // behaves as intended
    //
    //     // If we go > CHANNEL_SIZE we deadlock because context buffer is full. Not an issue when
    //     // using the full runtime because it progresses the subscribers one step after each runtime
    //     // step
    //     for time_index in 1..CHANNEL_SIZE {
    //         // Note: z has time index of `time_index` which means we should maintain history for it
    //         let spec_str = format!("in x\nout z\nz = default(z[{}], 0) + x", time_index);
    //         let spec = dsrv_specification(&mut spec_str.as_str()).unwrap();
    //         let x_stream: OutputStream<Value> = Box::pin(futures::stream::iter(
    //             (0..CHANNEL_SIZE).map(|x| (x as i64).into()),
    //         ));
    //         let z_stream: OutputStream<Value> = Box::pin(futures::stream::iter(
    //             (0..CHANNEL_SIZE).map(|z| (z as i64).into()),
    //         ));
    //         let stream_map = BTreeMap::from([
    //             (
    //                 "x".into(),
    //                 VarManager::<SemiSyncValueConfig>::new("x".into(), x_stream),
    //             ),
    //             (
    //                 "z".into(),
    //                 VarManager::<SemiSyncValueConfig>::new("z".into(), z_stream),
    //             ),
    //         ]);
    //         let mut context1 = SemiSyncContext::<SemiSyncValueConfig>::new(
    //             Rc::new(RefCell::new(stream_map)),
    //             spec.clone(),
    //         );
    //         let x_out = context1.var(&"x".into()).unwrap();
    //         let z_out = context1.var(&"z".into()).unwrap();
    //
    //         for _ in 0..CHANNEL_SIZE {
    //             with_timeout_res(context1.forward_values(), 1, "forward_values")
    //                 .await
    //                 .unwrap();
    //         }
    //         let x_vals: Vec<_> = with_timeout(x_out.take(CHANNEL_SIZE).collect(), 1, "x stream")
    //             .await
    //             .unwrap();
    //         let z_vals: Vec<_> = with_timeout(z_out.take(CHANNEL_SIZE).collect(), 1, "z stream")
    //             .await
    //             .unwrap();
    //         assert_eq!(
    //             x_vals,
    //             (0..CHANNEL_SIZE)
    //                 .map(|v| (v as i64).into())
    //                 .collect::<Vec<Value>>(),
    //             "Failed at time_index: {}",
    //             time_index
    //         );
    //         assert_eq!(
    //             z_vals,
    //             (0..CHANNEL_SIZE)
    //                 .map(|v| (v as i64).into())
    //                 .collect::<Vec<Value>>(),
    //             "Failed at time_index: {}",
    //             time_index
    //         );
    //         let x_stream: OutputStream<Value> = Box::pin(futures::stream::iter(
    //             (0..CHANNEL_SIZE).map(|x| (x as i64).into()),
    //         ));
    //         let z_stream: OutputStream<Value> = Box::pin(futures::stream::iter(
    //             (0..CHANNEL_SIZE).map(|z| (z as i64).into()),
    //         ));
    //         let stream_map = BTreeMap::from([
    //             (
    //                 "x".into(),
    //                 VarManager::<SemiSyncValueConfig>::new("x".into(), x_stream),
    //             ),
    //             (
    //                 "z".into(),
    //                 VarManager::<SemiSyncValueConfig>::new("z".into(), z_stream),
    //             ),
    //         ]);
    //         let mut context2 = SemiSyncContext::<SemiSyncValueConfig>::new(
    //             Rc::new(RefCell::new(stream_map)),
    //             spec,
    //         );
    //         context1.context_transfer(&mut context2);
    //         let history = context2.get_retained_history();
    //         // New context has `time_index` in hist for z and Deferred for x:
    //         let expected_z_hist: Vec<Value> = (CHANNEL_SIZE - time_index..CHANNEL_SIZE)
    //             .map(|v| (v as i64).into())
    //             .collect();
    //         // x history is deferred because we don't need those
    //         let expected_x_hist: Vec<Value> = (CHANNEL_SIZE - time_index..CHANNEL_SIZE)
    //             .map(|_| Value::NoVal)
    //             .collect();
    //         assert_eq!(
    //             history,
    //             BTreeMap::from([
    //                 ("x".into(), expected_x_hist.clone()),
    //                 ("z".into(), expected_z_hist.clone())
    //             ]),
    //             "Failed at time_index: {}",
    //             time_index
    //         );
    //         let x_out = context2.var(&"x".into()).unwrap();
    //         let z_out = context2.var(&"z".into()).unwrap();
    //         // Need to forward new values in order to also make VarManagers send history
    //         // (This could be changed if `var` was async)
    //         for _ in 0..CHANNEL_SIZE {
    //             context2.forward_values().await.unwrap();
    //         }
    //
    //         let x_vals: Vec<_> = with_timeout(
    //             x_out.take(CHANNEL_SIZE).collect(),
    //             1,
    //             "context transfer stream",
    //         )
    //         .await
    //         .unwrap();
    //         let z_vals: Vec<_> = with_timeout(z_out.take(CHANNEL_SIZE).collect(), 1, "z stream")
    //             .await
    //             .unwrap();
    //
    //         // z carries the history value, x does not
    //         assert_eq!(
    //             x_vals,
    //             (0..CHANNEL_SIZE)
    //                 .map(|v| (v as i64).into())
    //                 .collect::<Vec<Value>>(),
    //             "Failed at time_index: {}",
    //             time_index
    //         );
    //         // Note: The streams are now "out of sync". A runtime would need to forward the necessary amount
    //         // in order for the stream to not be yielding history but instead yield new values
    //         let expected_z = expected_z_hist
    //             .into_iter()
    //             .chain((0..CHANNEL_SIZE).map(|v| (v as i64).into()))
    //             .take(CHANNEL_SIZE)
    //             .collect::<Vec<Value>>();
    //         assert_eq!(z_vals, expected_z, "Failed at time_index: {}", time_index);
    //     }
    // }
}
