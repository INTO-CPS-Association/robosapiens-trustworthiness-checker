use crate::{
    InputEvent, InputStream, InputTickStream, OutputStream, VarName,
    core::{DeferrableStreamData, OutputHandler, Runtime, Specification},
    lang::core::{DepGraph, DependencyGraphExpr, DependencyGraphSpec, DependencyResolver},
    runtime::RuntimeBuilder,
    semantics::{AbstractContextBuilder, AsyncConfig, MonitoringSemantics, StreamContext},
    stream_utils::{self},
    utils::cancellation_token::CancellationToken,
};

use anyhow::anyhow;
use async_stream::stream;
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

fn lift_no_val<T: DeferrableStreamData>(mut input: OutputStream<T>) -> OutputStream<T> {
    Box::pin(stream! {
        let mut last = None;
        while let Some(current) = input.next().await {
            if current.is_no_val() {
                yield last.clone().unwrap_or(current);
            } else {
                last = Some(current.clone());
                yield current;
            }
        }
    })
}

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
    input: Option<InputStream<AC::Val>>,
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

    fn input(mut self, input: InputStream<AC::Val>) -> Self {
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
                input_stream: input,
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
    // Sender that forwards it to the managed variable
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
            info!(?self.var_name, ?val, "ExprEvaluator eval_value: Forwarding value to managed variable.");
            if let Err(e) = self.sender.send(val).await {
                return Err(anyhow!(
                    "ExprEvaluator eval_value for variable {}: Error sending value to managed variable: {}",
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

/// Shared subscription and retained-history behavior for both computed and external variables.
struct VarManager<T: DeferrableStreamData> {
    subscribers: Vec<spsc::Sender<T>>,
    new_subscribers: Vec<(spsc::Sender<T>, usize)>,
    retained_history: RetainedHistory<T>,
}

impl<T: DeferrableStreamData> VarManager<T> {
    fn new() -> Self {
        Self {
            subscribers: Vec::new(),
            new_subscribers: Vec::new(),
            retained_history: RetainedHistory::new(0),
        }
    }

    fn subscribe(&mut self, history_length: usize) -> OutputStream<T> {
        let (tx, rx) = spsc::channel(history_length + CHANNEL_SIZE);
        self.retained_history.increase_capacity(history_length);
        self.new_subscribers.push((tx, history_length));
        stream_utils::channel_to_output_stream(rx)
    }

    fn process_new_subscribers(&mut self) {
        while let Some((mut sender, history_length)) = self.new_subscribers.pop() {
            for value in self.retained_history.get_last_n_with_pad(history_length) {
                sender
                    .try_send(value)
                    .expect("variable subscription capacity must hold its requested history");
            }
            self.subscribers.push(sender);
        }
    }

    async fn publish(&mut self, value: T) {
        self.process_new_subscribers();
        self.retained_history.push(value.clone());

        let mut disconnected = Vec::new();
        for (index, subscriber) in self.subscribers.iter_mut().enumerate() {
            if subscriber.send(value.clone()).await.is_err() {
                disconnected.push(index);
            }
        }
        for index in disconnected.into_iter().rev() {
            self.subscribers.remove(index);
        }
    }

    fn finish(&mut self) {
        self.process_new_subscribers();
        self.clear_subscribers();
    }

    fn clear_subscribers(&mut self) {
        self.subscribers.clear();
        self.new_subscribers.clear();
    }

    fn set_history_to_retain(&mut self, history_to_retain: usize) {
        self.retained_history.capacity = history_to_retain;
    }

    fn retained_history(&self) -> Vec<T> {
        self.retained_history.get_all()
    }
}

enum VariableSource<T> {
    Computed(OutputStream<T>),
    External { pending_value: Option<T> },
}

/// A variable visible through a SemiSync context.
///
/// The source variant makes computed and external construction distinct, while
/// subscription fan-out and retained history remain shared.
struct ManagedVariable<T: DeferrableStreamData> {
    var_name: VarName,
    source: VariableSource<T>,
    manager: VarManager<T>,
    id: usize,
}

impl<T: DeferrableStreamData> ManagedVariable<T> {
    fn computed(var_name: VarName, value_stream: OutputStream<T>) -> Self {
        Self::new(var_name, VariableSource::Computed(value_stream))
    }

    fn computed_from_receiver(var_name: VarName, receiver: spsc::Receiver<T>) -> Self {
        Self::computed(var_name, stream_utils::channel_to_output_stream(receiver))
    }

    fn external(var_name: VarName) -> Self {
        Self::new(
            var_name,
            VariableSource::External {
                pending_value: None,
            },
        )
    }

    fn new(var_name: VarName, source: VariableSource<T>) -> Self {
        let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        debug!(?var_name, "Creating managed variable {}", id);
        Self {
            var_name,
            source,
            manager: VarManager::new(),
            id,
        }
    }

    fn is_external(&self) -> bool {
        matches!(self.source, VariableSource::External { .. })
    }

    fn has_pending_external_value(&self) -> bool {
        matches!(
            self.source,
            VariableSource::External {
                pending_value: Some(_)
            }
        )
    }

    fn set_external_value(&mut self, value: T) {
        let VariableSource::External { pending_value } = &mut self.source else {
            unreachable!("only external variables receive input ticks")
        };
        debug_assert!(pending_value.is_none());
        *pending_value = Some(value);
    }

    fn subscribe(&mut self, history_length: usize) -> OutputStream<T> {
        info!(
            ?self.var_name,
            history_length,
            history_items = ?self.manager.retained_history.history.len(),
            "Managed variable {} subscribe: Preparing subscription.",
            self.id,
        );
        self.manager.subscribe(history_length)
    }

    async fn forward_value(&mut self) -> StreamState {
        let next = match &mut self.source {
            VariableSource::Computed(stream) => stream.next().await,
            VariableSource::External { pending_value } => pending_value.take(),
        };
        if let Some(value) = next {
            self.manager.publish(value).await;
            StreamState::Pending
        } else {
            self.manager.finish();
            StreamState::Finished
        }
    }

    fn retained_history(&self) -> Vec<T> {
        self.manager.retained_history()
    }

    fn set_history_to_retain(&mut self, history_to_retain: usize) {
        self.manager.set_history_to_retain(history_to_retain);
    }

    fn cancel(&mut self) {
        match &mut self.source {
            VariableSource::Computed(stream) => {
                *stream = Box::pin(futures::stream::empty());
            }
            VariableSource::External { pending_value } => {
                *pending_value = None;
            }
        }
        self.manager.clear_subscribers();
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
    input_stream: InputStream<AC::Val>,
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
        input: InputStream<AC::Val>,
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

    fn setup_computed_variables(
        model: &AC::Spec,
    ) -> anyhow::Result<(
        Vec<(VarName, AC::Expr, spsc::Sender<AC::Val>)>,
        Vec<ManagedVariable<AC::Val>>,
    )> {
        let stream_vars = model.stream_vars();
        stream_vars
            .iter()
            .map(|var_name| {
                let (sender, receiver): (spsc::Sender<AC::Val>, spsc::Receiver<AC::Val>) =
                    spsc::channel(CHANNEL_SIZE);
                let variable = ManagedVariable::computed_from_receiver(var_name.clone(), receiver);
                let expr = model.var_expr(var_name).ok_or_else(|| {
                    anyhow!(
                        "No expression found for computed variable {} when setting up Monitor",
                        var_name
                    )
                })?;
                Ok(((var_name.clone(), expr, sender), variable))
            })
            .collect::<anyhow::Result<Vec<_>>>()
            .map(|entries| entries.into_iter().unzip())
    }

    fn build_context(
        variables: Vec<ManagedVariable<AC::Val>>,
        input_vars: &BTreeSet<VarName>,
        spec: AC::Spec,
    ) -> SemiSyncContext<AC> {
        let mut variables = variables
            .into_iter()
            .map(|variable| (variable.var_name.clone(), variable))
            .collect::<BTreeMap<_, _>>();
        variables.extend(
            input_vars
                .iter()
                .cloned()
                .map(|var| (var.clone(), ManagedVariable::external(var))),
        );

        SemiSyncContextBuilder::<AC>::new()
            .variables(variables)
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

    fn log_task_errors(output_res: &anyhow::Result<()>, work_res: &anyhow::Result<()>) {
        if let Err(e) = output_res {
            error!(?e, "Output handler had an error");
        }
        if let Err(e) = work_res {
            error!(?e, "Work task had an error");
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

    async fn initialize_runtime(
        self,
    ) -> anyhow::Result<(
        InputStream<AC::Val>,
        Box<dyn OutputHandler<Val = AC::Val>>,
        SemiSyncContext<AC>,
        Vec<ExprEvalutor<AC, MS>>,
    )> {
        let SemiSyncRuntime {
            _executor: _,
            model,
            input_stream,
            mut output_handler,
            starting_history,
            _marker: _,
        } = self;
        let input_vars = model.input_vars();
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

        let output_vars = model.output_vars();
        let (expr_eval_components, mut variables) = Self::setup_computed_variables(&model)?;
        let mut subscriptions: BTreeMap<VarName, OutputStream<AC::Val>> = variables
            .iter_mut()
            .filter_map(|vm| {
                output_vars.contains(&vm.var_name).then(|| {
                    let var_name = vm.var_name.clone();
                    let stream = vm.subscribe(0);
                    (var_name, stream)
                })
            })
            .collect();
        let mut context = Self::build_context(variables, &input_vars, model.clone());
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
        for index in 0..hist_len {
            let tick = input_vars
                .iter()
                .map(|var| {
                    InputEvent::new(
                        var.clone(),
                        starting_history
                            .get(var)
                            .and_then(|history| history.get(index))
                            .cloned()
                            .unwrap_or_else(AC::Val::no_val_value),
                    )
                })
                .collect();
            context.set_input_tick(tick)?;
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
        Ok((input_stream, output_handler, context, expr_evals))
    }

    pub(crate) async fn setup_runtime(
        self,
    ) -> anyhow::Result<(
        InputTickStream<AC::Val>,
        Box<dyn OutputHandler<Val = AC::Val>>,
        SemiSyncContext<AC>,
        Vec<ExprEvalutor<AC, MS>>,
    )> {
        let (input, output, context, expr_evals) = self.initialize_runtime().await?;
        Ok((crate::into_tick_stream(input), output, context, expr_evals))
    }

    pub(crate) async fn setup_runtime_without_input(
        self,
    ) -> anyhow::Result<(
        Box<dyn OutputHandler<Val = AC::Val>>,
        SemiSyncContext<AC>,
        Vec<ExprEvalutor<AC, MS>>,
    )> {
        let (_unused_input, output, context, expr_evals) = self.initialize_runtime().await?;
        Ok((output, context, expr_evals))
    }

    async fn process_input_ticks(
        mut ticks: InputTickStream<AC::Val>,
        mut context: SemiSyncContext<AC>,
        mut expr_evals: Vec<ExprEvalutor<AC, MS>>,
    ) -> anyhow::Result<()> {
        while Self::advance_input(&mut ticks, &mut context, &mut expr_evals).await? {}
        Self::work_task(context, expr_evals).await
    }

    pub(super) async fn advance_input(
        ticks: &mut InputTickStream<AC::Val>,
        context: &mut SemiSyncContext<AC>,
        expr_evals: &mut Vec<ExprEvalutor<AC, MS>>,
    ) -> anyhow::Result<bool> {
        let Some(tick) = ticks.next().await else {
            return Ok(false);
        };
        Self::advance_tick(tick?, context, expr_evals).await?;
        Ok(true)
    }

    pub(super) async fn advance_tick(
        tick: Vec<InputEvent<AC::Val>>,
        context: &mut SemiSyncContext<AC>,
        expr_evals: &mut Vec<ExprEvalutor<AC, MS>>,
    ) -> anyhow::Result<()> {
        context.set_input_tick(tick)?;
        Self::step(context, expr_evals).await?;
        Ok(())
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

        let (ticks, mut output_handler, context, expr_evals) = Self::setup_runtime(*self).await?;

        let output_fut = Self::log_when_done(output_handler.run(), "output_handler.run() ended");
        let work_fut = Self::log_when_done(
            Self::process_input_ticks(ticks, context, expr_evals),
            "work_task.run() ended",
        );
        let (output_res, work_res) = futures::join!(output_fut, work_fut);
        Self::log_task_errors(&output_res, &work_res);

        match (output_res, work_res) {
            (Ok(_), Ok(_)) => Ok(()),
            (Err(e), _) => Err(anyhow!("OutputHandler failed with error: {}", e)),
            (_, Err(e)) => Err(anyhow!("SemiSync work task failed with error: {}", e)),
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
    variables: Option<BTreeMap<VarName, ManagedVariable<AC::Val>>>,
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
    fn variables(self, variables: BTreeMap<VarName, ManagedVariable<AC::Val>>) -> Self {
        Self {
            variables: Some(variables),
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
            variables: None,
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
            Rc::new(RefCell::new(self.variables.expect(
                "Variables must be set before building SemiSyncContext",
            ))),
            self.spec
                .expect("Spec must be set before building SemiSyncContext"),
            self.cancellation_token
                .unwrap_or_else(CancellationToken::new),
        );
        info!(
            "SemiSyncContextBuilder: Built SemiSyncContext with id {:?} and variables: {:?}",
            ctx.id,
            ctx.variable_names()
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
    variables: Rc<RefCell<BTreeMap<VarName, ManagedVariable<AC::Val>>>>,
    // Unique identifier for this context
    id: usize,
    // The specification for this context
    spec: AC::Spec,
    // Dependencies from the specification
    deps: Box<dyn DependencyResolver<AC>>,
    aux_vars: BTreeSet<VarName>,
    cancellation_token: CancellationToken,
    uses_dependency_history: bool,
}

impl<AC> SemiSyncContext<AC>
where
    AC: AsyncConfig<Ctx = SemiSyncContext<AC>>,
    AC::Expr: DependencyGraphExpr,
    AC::Spec: DependencyGraphSpec,
    AC::Val: DeferrableStreamData,
{
    pub(super) fn set_input_tick(&mut self, tick: Vec<InputEvent<AC::Val>>) -> anyhow::Result<()> {
        let mut variables = self.variables.borrow_mut();
        anyhow::ensure!(
            variables
                .values()
                .filter(|variable| variable.is_external())
                .all(|variable| !variable.has_pending_external_value()),
            "external input was not consumed before the next step"
        );

        for InputEvent { var, .. } in &tick {
            let variable = variables.get(var).ok_or_else(|| {
                anyhow!("input stream emitted undeclared semi-sync variable `{var}`")
            })?;
            anyhow::ensure!(
                variable.is_external(),
                "input stream emitted undeclared semi-sync variable `{var}`"
            );
        }

        for InputEvent { var, value } in tick {
            variables
                .get_mut(&var)
                .expect("input variable was validated above")
                .set_external_value(value);
        }
        for variable in variables
            .values_mut()
            .filter(|variable| variable.is_external())
        {
            if !variable.has_pending_external_value() {
                variable.set_external_value(AC::Val::no_val_value());
            }
        }
        Ok(())
    }

    fn new_with_token(
        variables: Rc<RefCell<BTreeMap<VarName, ManagedVariable<AC::Val>>>>,
        spec: AC::Spec,
        cancellation_token: CancellationToken,
    ) -> Self {
        let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        debug!(
            "Creating SemiSyncContext {:?} with vars: {:?}",
            id,
            variables.borrow().keys()
        );
        let deps = Box::new(DepGraph::resolver_from_spec(spec.clone()));
        for (name, variable) in variables.borrow_mut().iter_mut() {
            variable.set_history_to_retain(deps.longest_time_dependency(name) as usize);
        }
        let aux_vars = spec.aux_vars();
        Self {
            variables,
            id,
            spec,
            deps,
            aux_vars,
            cancellation_token,
            uses_dependency_history: true,
        }
    }

    async fn forward_values(&mut self) -> anyhow::Result<StreamState> {
        let variables = Rc::clone(&self.variables);
        let mut variables = variables.borrow_mut();
        let results = join_all(
            variables
                .iter_mut()
                .map(|(name, variable)| async { (name.clone(), variable.forward_value().await) }),
        )
        .await;
        let finished = results
            .into_iter()
            .filter_map(|(name, state)| (state == StreamState::Finished).then_some(name))
            .collect::<BTreeSet<_>>();

        variables.retain(|name, _| !finished.contains(name));
        if variables.keys().any(|name| !self.aux_vars.contains(name)) {
            Ok(StreamState::Pending)
        } else {
            Ok(StreamState::Finished)
        }
    }

    fn subcontext_common(&self, vs: EcoVec<VarName>) -> Self {
        let new_variables = self
            .variables
            .borrow_mut()
            .iter_mut()
            .filter_map(|(name, variable)| {
                vs.contains(name).then(|| {
                    let history_length = self.deps.longest_time_dependency(name) as usize;
                    let stream = lift_no_val(variable.subscribe(history_length));
                    (
                        name.clone(),
                        ManagedVariable::computed(name.clone(), stream),
                    )
                })
            })
            .collect::<BTreeMap<_, _>>();
        debug!(
            "SemiSyncContext ID {:?}: Subcontext variables: {:?}",
            self.id,
            new_variables.keys()
        );
        let builder = SemiSyncContextBuilder::<AC>::new()
            .variables(new_variables)
            .spec(self.spec.clone())
            .cancellation_token(self.cancellation_token.clone());
        let mut context = builder.build();
        context.uses_dependency_history = false;
        context
    }

    // Gets the retained history for all the variables inside the context.
    pub fn get_retained_history(&self) -> BTreeMap<VarName, Vec<AC::Val>> {
        self.variables
            .borrow()
            .iter()
            .map(|(name, variable)| (name.clone(), variable.retained_history()))
            .collect()
    }

    fn variable_names(&self) -> EcoVec<VarName> {
        self.variables.borrow().keys().cloned().collect()
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
        let history_length = if self.uses_dependency_history {
            self.deps.longest_time_dependency(x) as usize
        } else {
            0
        };
        let stream = self
            .variables
            .borrow_mut()
            .get_mut(x)?
            .subscribe(history_length);
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
        let variables = self.variable_names();
        self.subcontext_common(variables)
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
        let variables = self
            .variables
            .borrow()
            .keys()
            .filter(|name| *name != excluded)
            .cloned()
            .collect();
        self.subcontext_common(variables)
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
        for variable in self.variables.borrow_mut().values_mut() {
            variable.cancel();
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::async_test;
    use crate::core::{Runtime, Specification};
    use crate::io::testing::{ManualOutputHandler, NullOutputHandler};
    use crate::io::{controlled, map};
    use crate::lang::dsrv::lalr_parser::LALRParser;
    use crate::lang::dsrv::type_checker::type_check;
    use crate::runtime::RuntimeBuilder;
    use crate::runtime::builder::{SemiSyncValueConfig, TypedSemiSyncValueConfig};
    use crate::runtime::semi_sync::{SemiSyncRuntime, SemiSyncRuntimeBuilder};
    use crate::semantics::{
        AbstractContextBuilder, StreamContext, TypedUntimedDsrvSemantics, UntimedDsrvSemantics,
    };
    use crate::{InputBatch, InputEvent, InputStream, Value, dsrv_specification};
    use crate::{VarName, dsrv_fixtures::*};
    use futures::{FutureExt, stream::StreamExt};
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::collections::BTreeMap;
    use std::rc::Rc;

    use tc_testutils::streams::{with_timeout, with_timeout_res};

    type TestRuntime = SemiSyncRuntime<SemiSyncValueConfig, UntimedDsrvSemantics<LALRParser>>;
    type TestTypedRuntime =
        SemiSyncRuntime<TypedSemiSyncValueConfig, TypedUntimedDsrvSemantics<LALRParser>>;

    struct CompatibilityCase {
        name: &'static str,
        specification: &'static str,
        inputs: BTreeMap<VarName, Vec<Value>>,
        expected: Vec<BTreeMap<VarName, Value>>,
    }

    fn int_values(values: &[i64]) -> Vec<Value> {
        values.iter().copied().map(Value::Int).collect()
    }

    fn output<const N: usize>(values: [(&'static str, Value); N]) -> BTreeMap<VarName, Value> {
        values
            .into_iter()
            .map(|(name, value)| (name.into(), value))
            .collect()
    }

    fn external_input_context() -> super::SemiSyncContext<SemiSyncValueConfig> {
        let mut source = "in x\nout z\nz = x";
        let spec = dsrv_specification(&mut source).unwrap();
        super::SemiSyncContextBuilder::<SemiSyncValueConfig>::new()
            .variables(BTreeMap::from([(
                "x".into(),
                super::ManagedVariable::external("x".into()),
            )]))
            .spec(spec)
            .build()
    }

    fn input_tick(events: Vec<InputEvent<Value>>) -> Vec<InputEvent<Value>> {
        events
    }

    async fn assert_compatibility_case(
        executor: Rc<LocalExecutor<'static>>,
        case: CompatibilityCase,
    ) {
        let mut source = case.specification;
        let spec = dsrv_specification(&mut source).unwrap();
        let (input, controller) = controlled(map::input_stream(case.inputs));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars.clone(),
        ));
        let mut outputs = output_handler.get_output();
        let monitor: TestRuntime = SemiSyncRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec)
            .input(input)
            .output(output_handler)
            .build()
            .await;
        let monitor_task = executor.spawn(monitor.run());

        assert!(
            outputs.next().now_or_never().is_none(),
            "{} emitted output before its first controlled input",
            case.name
        );

        let expected_len = case.expected.len();
        for (tick, expected) in case.expected.into_iter().enumerate() {
            let advance_name = format!("{} input tick {tick}", case.name);
            with_timeout_res(controller.advance(), 1, &advance_name)
                .await
                .unwrap();

            let output_name = format!("{} output tick {tick}", case.name);
            let actual = with_timeout(outputs.next(), 1, &output_name)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("{} output ended at tick {tick}", case.name));
            assert_eq!(actual, expected, "{} output tick {tick}", case.name);

            if tick + 1 < expected_len {
                assert!(
                    outputs.next().now_or_never().is_none(),
                    "{} emitted tick {} before its input was released",
                    case.name,
                    tick + 1
                );
            }
        }

        let end_name = format!("{} output completion", case.name);
        assert_eq!(
            with_timeout(outputs.next(), 1, &end_name).await.unwrap(),
            None,
            "{} emitted more output rows than expected",
            case.name
        );
        let monitor_name = format!("{} monitor completion", case.name);
        with_timeout_res(monitor_task, 1, &monitor_name)
            .await
            .unwrap();
    }

    #[test]
    fn semi_sync_context_rejects_a_second_input_tick_without_replacing_the_first() {
        smol::block_on(async {
            let mut context = external_input_context();
            context
                .set_input_tick(input_tick(vec![InputEvent::new("x".into(), Value::Int(1))]))
                .unwrap();

            let error = context
                .set_input_tick(input_tick(vec![InputEvent::new("x".into(), Value::Int(2))]))
                .unwrap_err();
            assert_eq!(
                error.to_string(),
                "external input was not consumed before the next step"
            );

            let mut values = context.var(&"x".into()).unwrap();
            assert_eq!(
                context.forward_values().await.unwrap(),
                super::StreamState::Pending
            );
            assert_eq!(values.next().await, Some(Value::Int(1)));
        });
    }

    #[test]
    fn semi_sync_context_rejects_undeclared_input_before_updating() {
        let mut context = external_input_context();

        let error = context
            .set_input_tick(input_tick(vec![InputEvent::new(
                "unknown".into(),
                Value::Int(3),
            )]))
            .unwrap_err();
        assert!(error.to_string().contains("undeclared semi-sync variable"));

        context
            .set_input_tick(input_tick(vec![InputEvent::new("x".into(), Value::Int(3))]))
            .unwrap();
    }

    #[test]
    fn semi_sync_context_replays_external_history_and_closes_subscriptions() {
        smol::block_on(async {
            let mut context = external_input_context();
            context
                .variables
                .borrow_mut()
                .get_mut(&"x".into())
                .unwrap()
                .manager
                .set_history_to_retain(1);

            context
                .set_input_tick(input_tick(vec![InputEvent::new("x".into(), Value::Int(1))]))
                .unwrap();
            assert_eq!(
                context.forward_values().await.unwrap(),
                super::StreamState::Pending
            );

            let mut values = context
                .variables
                .borrow_mut()
                .get_mut(&"x".into())
                .unwrap()
                .subscribe(1);
            context
                .set_input_tick(input_tick(vec![InputEvent::new("x".into(), Value::Int(2))]))
                .unwrap();
            assert_eq!(
                context.forward_values().await.unwrap(),
                super::StreamState::Pending
            );
            assert_eq!(values.next().await, Some(Value::Int(1)));
            assert_eq!(values.next().await, Some(Value::Int(2)));

            assert_eq!(
                context.forward_values().await.unwrap(),
                super::StreamState::Finished
            );
            assert_eq!(values.next().await, None);
        });
    }

    #[apply(async_test)]
    async fn main_semisync_interactions_preserve_ordered_output_traces(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let cases = vec![
            CompatibilityCase {
                name: "external passthrough",
                specification: "in x\nout z\nz = x",
                inputs: BTreeMap::from([("x".into(), int_values(&[1, 2, 3]))]),
                expected: vec![
                    output([("z", Value::Int(1))]),
                    output([("z", Value::Int(2))]),
                    output([("z", Value::Int(3))]),
                ],
            },
            CompatibilityCase {
                name: "external fan-in",
                specification: "in x\nin y\nout z\nz = x + y",
                inputs: BTreeMap::from([
                    ("x".into(), int_values(&[1, 2, 3])),
                    ("y".into(), int_values(&[10, 20, 30])),
                ]),
                expected: vec![
                    output([("z", Value::Int(11))]),
                    output([("z", Value::Int(22))]),
                    output([("z", Value::Int(33))]),
                ],
            },
            CompatibilityCase {
                name: "computed fan-out",
                specification: "in x\nout a\nout b\na = x + 1\nb = x * 2",
                inputs: BTreeMap::from([("x".into(), int_values(&[1, 2, 3]))]),
                expected: vec![
                    output([("a", Value::Int(2)), ("b", Value::Int(2))]),
                    output([("a", Value::Int(3)), ("b", Value::Int(4))]),
                    output([("a", Value::Int(4)), ("b", Value::Int(6))]),
                ],
            },
            CompatibilityCase {
                name: "reverse-declared computed chain",
                specification: "in x\nout c\nout b\nout a\nc = b + 1\nb = a + 1\na = x + 1",
                inputs: BTreeMap::from([("x".into(), int_values(&[1, 2, 3]))]),
                expected: vec![
                    output([
                        ("a", Value::Int(2)),
                        ("b", Value::Int(3)),
                        ("c", Value::Int(4)),
                    ]),
                    output([
                        ("a", Value::Int(3)),
                        ("b", Value::Int(4)),
                        ("c", Value::Int(5)),
                    ]),
                    output([
                        ("a", Value::Int(4)),
                        ("b", Value::Int(5)),
                        ("c", Value::Int(6)),
                    ]),
                ],
            },
            CompatibilityCase {
                name: "diamond dependency",
                specification: "in x\nout a\nout b\nout z\na = x + 1\nb = x * 2\nz = a + b",
                inputs: BTreeMap::from([("x".into(), int_values(&[1, 2, 3]))]),
                expected: vec![
                    output([
                        ("a", Value::Int(2)),
                        ("b", Value::Int(2)),
                        ("z", Value::Int(4)),
                    ]),
                    output([
                        ("a", Value::Int(3)),
                        ("b", Value::Int(4)),
                        ("z", Value::Int(7)),
                    ]),
                    output([
                        ("a", Value::Int(4)),
                        ("b", Value::Int(6)),
                        ("z", Value::Int(10)),
                    ]),
                ],
            },
            CompatibilityCase {
                name: "independent branches recombine",
                specification: "in x\nin y\nout a\nout b\nout z\na = x + 1\nb = y * 2\nz = a + b",
                inputs: BTreeMap::from([
                    ("x".into(), int_values(&[1, 2, 3])),
                    ("y".into(), int_values(&[10, 20, 30])),
                ]),
                expected: vec![
                    output([
                        ("a", Value::Int(2)),
                        ("b", Value::Int(20)),
                        ("z", Value::Int(22)),
                    ]),
                    output([
                        ("a", Value::Int(3)),
                        ("b", Value::Int(40)),
                        ("z", Value::Int(43)),
                    ]),
                    output([
                        ("a", Value::Int(4)),
                        ("b", Value::Int(60)),
                        ("z", Value::Int(64)),
                    ]),
                ],
            },
            CompatibilityCase {
                name: "auxiliary computed chain",
                specification: "in x\nout z\naux a\naux b\na = x + 1\nb = a * 2\nz = b + 1",
                inputs: BTreeMap::from([("x".into(), int_values(&[1, 2, 3]))]),
                expected: vec![
                    output([("z", Value::Int(5))]),
                    output([("z", Value::Int(7))]),
                    output([("z", Value::Int(9))]),
                ],
            },
            CompatibilityCase {
                name: "external history",
                specification: "in x\nout z\nz = x + default(x[1], 0)",
                inputs: BTreeMap::from([("x".into(), int_values(&[1, 2, 3]))]),
                expected: vec![
                    output([("z", Value::Int(1))]),
                    output([("z", Value::Int(3))]),
                    output([("z", Value::Int(5))]),
                ],
            },
            CompatibilityCase {
                name: "recursive computed history",
                specification: "in x\nout z\nz = x + default(z[1], 0)",
                inputs: BTreeMap::from([("x".into(), int_values(&[1, 2, 3]))]),
                expected: vec![
                    output([("z", Value::Int(1))]),
                    output([("z", Value::Int(3))]),
                    output([("z", Value::Int(6))]),
                ],
            },
            CompatibilityCase {
                name: "conditional external selection",
                specification: "in x\nin y\nin flag\nout z\nz = if flag then x else y",
                inputs: BTreeMap::from([
                    (
                        "flag".into(),
                        vec![Value::Bool(true), Value::Bool(false), Value::Bool(true)],
                    ),
                    ("x".into(), int_values(&[1, 2, 3])),
                    ("y".into(), int_values(&[10, 20, 30])),
                ]),
                expected: vec![
                    output([("z", Value::Int(1))]),
                    output([("z", Value::Int(20))]),
                    output([("z", Value::Int(3))]),
                ],
            },
            CompatibilityCase {
                name: "explicit no-value propagation",
                specification: "in x\nout z\nz = x",
                inputs: BTreeMap::from([(
                    "x".into(),
                    vec![Value::Int(1), Value::NoVal, Value::Int(3)],
                )]),
                expected: vec![
                    output([("z", Value::Int(1))]),
                    output([("z", Value::NoVal)]),
                    output([("z", Value::Int(3))]),
                ],
            },
            CompatibilityCase {
                name: "dynamic external expression",
                specification: "in x\nin e\nout z\nz = dynamic(e)",
                inputs: BTreeMap::from([
                    (
                        "e".into(),
                        vec!["x + 1".into(), "x + 2".into(), "x + 3".into()],
                    ),
                    ("x".into(), int_values(&[0, 1, 2])),
                ]),
                expected: vec![
                    output([("z", Value::Int(1))]),
                    output([("z", Value::Int(3))]),
                    output([("z", Value::Int(5))]),
                ],
            },
            CompatibilityCase {
                name: "four-way external fan-in",
                specification: "in a\nin b\nin c\nin d\nout z\nz = a + b + c + d",
                inputs: BTreeMap::from([
                    ("a".into(), int_values(&[1, 2, 3])),
                    ("b".into(), int_values(&[10, 20, 30])),
                    ("c".into(), int_values(&[100, 200, 300])),
                    ("d".into(), int_values(&[1000, 2000, 3000])),
                ]),
                expected: vec![
                    output([("z", Value::Int(1111))]),
                    output([("z", Value::Int(2222))]),
                    output([("z", Value::Int(3333))]),
                ],
            },
            CompatibilityCase {
                name: "computed history after external transformation",
                specification: "in x\nout a\nout z\na = x + 1\nz = a + default(a[1], 0)",
                inputs: BTreeMap::from([("x".into(), int_values(&[1, 2, 3]))]),
                expected: vec![
                    output([("a", Value::Int(2)), ("z", Value::Int(2))]),
                    output([("a", Value::Int(3)), ("z", Value::Int(5))]),
                    output([("a", Value::Int(4)), ("z", Value::Int(7))]),
                ],
            },
            CompatibilityCase {
                name: "multi-stage cross dependency",
                specification: "in x\nin y\nout a\nout b\nout z\na = x + y\nb = a + x\nz = b + y",
                inputs: BTreeMap::from([
                    ("x".into(), int_values(&[1, 2, 3])),
                    ("y".into(), int_values(&[10, 20, 30])),
                ]),
                expected: vec![
                    output([
                        ("a", Value::Int(11)),
                        ("b", Value::Int(12)),
                        ("z", Value::Int(22)),
                    ]),
                    output([
                        ("a", Value::Int(22)),
                        ("b", Value::Int(24)),
                        ("z", Value::Int(44)),
                    ]),
                    output([
                        ("a", Value::Int(33)),
                        ("b", Value::Int(36)),
                        ("z", Value::Int(66)),
                    ]),
                ],
            },
        ];

        for case in cases {
            assert_compatibility_case(executor.clone(), case).await;
        }
    }

    #[apply(async_test)]
    async fn sparse_event_ticks_preserve_exact_output_order(executor: Rc<LocalExecutor<'static>>) {
        let mut source = "in x\nin y\nout ox\nout oy\nox = x\noy = y";
        let spec = dsrv_specification(&mut source).unwrap();
        let input: InputStream<Value> =
            Box::pin(futures::stream::iter([Ok(InputBatch::events(vec![
                InputEvent::new("x".into(), Value::Int(1)),
                InputEvent::new("y".into(), Value::Int(10)),
            ]))]));
        let (input, controller) = controlled(input);
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars.clone(),
        ));
        let mut outputs = output_handler.get_output();
        let monitor: TestRuntime = SemiSyncRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec)
            .input(input)
            .output(output_handler)
            .build()
            .await;
        let monitor_task = executor.spawn(monitor.run());

        controller.advance().await.unwrap();
        assert_eq!(
            outputs.next().await.unwrap(),
            output([("ox", Value::Int(1)), ("oy", Value::NoVal)])
        );
        controller.advance().await.unwrap();
        assert_eq!(
            outputs.next().await.unwrap(),
            output([("ox", Value::NoVal), ("oy", Value::Int(10))])
        );
        assert_eq!(outputs.next().await, None);
        monitor_task.await.unwrap();
    }

    #[apply(async_test)]
    async fn test_simple_add(executor: Rc<LocalExecutor<'static>>) {
        let spec = dsrv_specification(&mut spec_simple_add_monitor()).unwrap();

        let x = vec![0.into(), 1.into(), 2.into()];
        let y = vec![3.into(), 4.into(), 5.into()];
        let input_stream = map::input_stream(BTreeMap::from([("x".into(), x), ("y".into(), y)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let monitor: TestRuntime = SemiSyncRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec.clone())
            .input(input_stream)
            .output(output_handler)
            .build()
            .await;

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
        let input_stream = map::input_stream(BTreeMap::from([("x".into(), x), ("y".into(), y)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let monitor: TestTypedRuntime = SemiSyncRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec.clone())
            .input(input_stream)
            .output(output_handler)
            .build()
            .await;

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
        let input_stream = map::input_stream(BTreeMap::from([("x".into(), x), ("y".into(), y)]));
        let output_handler = Box::new(NullOutputHandler::new(
            executor.clone(),
            spec.output_vars.clone(),
        ));
        let monitor: TestRuntime = SemiSyncRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec.clone())
            .input(input_stream)
            .output(output_handler)
            .build()
            .await;

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
        let input_stream = map::input_stream(BTreeMap::from([("x".into(), x)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let monitor: TestRuntime = SemiSyncRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec.clone())
            .input(input_stream)
            .output(output_handler)
            .build()
            .await;

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
        let input_stream = map::input_stream(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let monitor: TestRuntime = SemiSyncRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec.clone())
            .input(input_stream)
            .output(output_handler)
            .build()
            .await;

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

    #[ignore = "semi-sync deadlocks when a specification has multiple DUP outputs"]
    #[apply(async_test)]
    async fn multiple_runtime_compiled_outputs_complete_each_tick(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let mut source = "in x: Int\nin y: Int\nin left_source: Str\nin right_source: Str\n\
                          out left: Int\nout right: Int\n\
                          left = dynamic(left_source: Int)\n\
                          right = defer(right_source: Int)";
        let spec = dsrv_specification(&mut source).unwrap();
        let input_stream = map::input_stream(BTreeMap::from([
            ("x".into(), vec![Value::Int(1)]),
            ("y".into(), vec![Value::Int(10)]),
            ("left_source".into(), vec![Value::Str("x".into())]),
            ("right_source".into(), vec![Value::Deferred]),
        ]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();
        let monitor: TestRuntime = SemiSyncRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec.clone())
            .input(input_stream)
            .output(output_handler)
            .build()
            .await;

        executor.spawn(monitor.run()).detach();

        let rows = with_timeout(outputs.take(1).collect::<Vec<_>>(), 1, "outputs")
            .await
            .expect("semi-sync should complete the tick");
        assert_eq!(
            rows,
            vec![BTreeMap::from([
                (VarName::new("left"), Value::Int(1)),
                (VarName::new("right"), Value::Deferred),
            ])]
        );
    }

    #[apply(async_test)]
    async fn dynamic_deferred_property_keeps_installed_history_on_global_timeline(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let mut spec_source = "in x: Int\nin e: Str\nout z: Int\nz = dynamic(e: Int)";
        let spec = dsrv_specification(&mut spec_source).unwrap();
        let input_stream = map::input_stream(BTreeMap::from([
            ("x".into(), vec![1.into(), 2.into(), 3.into()]),
            (
                "e".into(),
                vec!["x[1]".into(), Value::Deferred, "x[1]".into()],
            ),
        ]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();
        let monitor: TestRuntime = SemiSyncRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec.clone())
            .input(input_stream)
            .output(output_handler)
            .build()
            .await;

        executor.spawn(monitor.run()).detach();

        let outputs = with_timeout(outputs.enumerate().collect::<Vec<_>>(), 1, "outputs")
            .await
            .unwrap();
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), Value::Deferred)])),
                (1, BTreeMap::from([("z".into(), Value::Deferred)])),
                (2, BTreeMap::from([("z".into(), Value::Int(2))])),
            ]
        );
    }

    #[apply(async_test)]
    async fn typed_dynamic_deferred_property_keeps_installed_history_on_global_timeline(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let mut spec_source = "in x: Int\nin e: Str\nout z: Int\nz = dynamic(e: Int)";
        let spec = dsrv_specification(&mut spec_source).unwrap();
        let spec = type_check(spec).expect("dynamic regression spec should type check");
        let input_stream = map::input_stream(BTreeMap::from([
            ("x".into(), vec![1.into(), 2.into(), 3.into()]),
            (
                "e".into(),
                vec!["x[1]".into(), Value::Deferred, "x[1]".into()],
            ),
        ]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();
        let monitor: TestTypedRuntime = SemiSyncRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec.clone())
            .input(input_stream)
            .output(output_handler)
            .build()
            .await;

        executor.spawn(monitor.run()).detach();

        let outputs = with_timeout(outputs.enumerate().collect::<Vec<_>>(), 1, "typed outputs")
            .await
            .unwrap();
        assert_eq!(
            outputs,
            vec![
                (0, BTreeMap::from([("z".into(), Value::Deferred)])),
                (1, BTreeMap::from([("z".into(), Value::Deferred)])),
                (2, BTreeMap::from([("z".into(), Value::Int(2))])),
            ]
        );
    }

    #[apply(async_test)]
    async fn test_defer_single(executor: Rc<LocalExecutor<'static>>) {
        let spec = dsrv_specification(&mut spec_defer()).unwrap();

        let x = vec![0.into(), 1.into(), 2.into()];
        let e = vec!["x + 1".into(), Value::Deferred, Value::Deferred];
        let input_stream = map::input_stream(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let monitor: TestRuntime = SemiSyncRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec.clone())
            .input(input_stream)
            .output(output_handler)
            .build()
            .await;

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
        let input_stream = map::input_stream(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let monitor: TestRuntime = SemiSyncRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec.clone())
            .input(input_stream)
            .output(output_handler)
            .build()
            .await;

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
        let input_stream = map::input_stream(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let monitor: TestRuntime = SemiSyncRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec.clone())
            .input(input_stream)
            .output(output_handler)
            .build()
            .await;

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
        let input_stream = map::input_stream(BTreeMap::from([("x".into(), x), ("e".into(), e)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let monitor: TestRuntime = SemiSyncRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec.clone())
            .input(input_stream)
            .output(output_handler)
            .build()
            .await;

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
        let input_stream = map::input_stream(BTreeMap::from([("x".into(), x)]));
        let mut output_handler = Box::new(ManualOutputHandler::new(
            executor.clone(),
            spec.output_vars(),
        ));
        let outputs = output_handler.get_output();

        let monitor: TestRuntime = SemiSyncRuntimeBuilder::new()
            .executor(executor.clone())
            .model(spec.clone())
            .input(input_stream)
            .output(output_handler)
            .build()
            .await;

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
