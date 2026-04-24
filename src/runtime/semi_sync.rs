use crate::{
    OutputStream, SExpr, VarName,
    core::{
        AbstractMonitorBuilder, DeferrableStreamData, InputProvider, Monitor, OutputHandler,
        Runnable, Specification,
    },
    lang::core::{DepGraph, DependencyResolver},
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
};
use tracing::{debug, error, info};
use unsync::spsc;

pub struct SemiSyncMonitorBuilder<AC, MS>
where
    AC: AsyncConfig<Expr = SExpr>,
    MS: MonitoringSemantics<AC>,
{
    executor: Option<Rc<LocalExecutor<'static>>>,
    model: Option<AC::Spec>,
    input: Option<Box<dyn InputProvider<Val = AC::Val>>>,
    output: Option<Box<dyn OutputHandler<Val = AC::Val>>>,
    _marker: std::marker::PhantomData<MS>,
}

impl<AC, MS> AbstractMonitorBuilder<AC::Spec, AC::Val> for SemiSyncMonitorBuilder<AC, MS>
where
    AC: AsyncConfig<Expr = SExpr, Ctx = SemiSyncContext<AC>>,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
{
    type Mon = SemiSyncMonitor<AC, MS>;

    fn new() -> Self {
        Self {
            executor: None,
            model: None,
            input: None,
            output: None,
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

    fn build(self) -> SemiSyncMonitor<AC, MS> {
        let executor = self.executor.unwrap();
        let model = self.model.unwrap();
        let input = self.input.unwrap();
        let output = self.output.unwrap();

        SemiSyncMonitor {
            _executor: executor,
            model,
            input_provider: input,
            output_handler: output,
            _marker: std::marker::PhantomData,
        }
    }

    fn var_msg_types(self, _var_msg_types: BTreeMap<VarName, String>) -> Self {
        self
    }

    fn async_build(self: Box<Self>) -> LocalBoxFuture<'static, Self::Mon> {
        Box::pin(async move { (*self).build() })
    }
}

#[derive(Debug, PartialEq)]
pub enum StreamState {
    Pending,
    Finished,
}

pub struct ExprEvalutor<AC, MS>
where
    AC: AsyncConfig<Expr = SExpr>,
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
    AC: AsyncConfig<Expr = SExpr>,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
{
    fn new(
        var_name: VarName,
        expr: AC::Expr,
        sender: spsc::Sender<AC::Val>,
        ctx: &AC::Ctx,
    ) -> Self {
        let eval_stream = MS::to_async_stream(expr.clone(), ctx);
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

    /// Increases the capacity, padding with deferred values if needed
    fn increase_capacity(&mut self, new_capacity: usize) {
        if new_capacity <= self.capacity {
            return;
        }

        let padding_needed = std::cmp::min(
            new_capacity.saturating_sub(self.capacity),
            self.samples_seen.saturating_sub(self.history.len()),
        );

        for _ in 0..padding_needed {
            self.history.push_front(T::deferred_value());
        }

        self.capacity = new_capacity;
    }

    fn get_all(&self) -> Vec<T> {
        self.history.iter().cloned().collect()
    }

    fn set_all(&mut self, values: impl IntoIterator<Item = T>) {
        self.history = values.into_iter().collect();
        self.prune_old();
    }
}

// TODO: Fix that we have the boolean parameter input to define an ordering. Should not be defined
// inside the VarManager but somewhere with logic in the context.
struct VarManager<AC>
where
    AC: AsyncConfig<Expr = SExpr>,
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
    AC: AsyncConfig<Expr = SExpr>,
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
        let (tx, rx) = spsc::channel::<AC::Val>(history_length + 8);

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

    fn set_retained_history(&mut self, history: impl IntoIterator<Item = AC::Val>) {
        self.retained_history.set_all(history);
    }

    fn set_history_to_retain(&mut self, history_to_retain: usize) {
        self.retained_history.capacity = history_to_retain;
    }
}

pub struct SemiSyncMonitor<AC, MS>
where
    AC: AsyncConfig<Expr = SExpr>,
    MS: MonitoringSemantics<AC>,
{
    _executor: Rc<LocalExecutor<'static>>,
    model: AC::Spec,
    input_provider: Box<dyn InputProvider<Val = AC::Val>>,
    output_handler: Box<dyn OutputHandler<Val = AC::Val>>,
    _marker: std::marker::PhantomData<MS>,
}

impl<AC, MS> SemiSyncMonitor<AC, MS>
where
    AC: AsyncConfig<Expr = SExpr, Ctx = SemiSyncContext<AC>>,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
{
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        model: AC::Spec,
        input: Box<dyn InputProvider<Val = AC::Val>>,
        output: Box<dyn OutputHandler<Val = AC::Val>>,
    ) -> Self {
        SemiSyncMonitorBuilder::new()
            .executor(executor)
            .model(model)
            .input(input)
            .output(output)
            .build()
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

    fn setup_output_var_managers(
        model: &AC::Spec,
    ) -> anyhow::Result<(
        Vec<(VarName, AC::Expr, spsc::Sender<AC::Val>)>,
        Vec<VarManager<AC>>,
    )> {
        model
            .output_vars()
            .iter()
            .map(|var_name| {
                let (sender, receiver): (spsc::Sender<AC::Val>, spsc::Receiver<AC::Val>) =
                    spsc::channel(128);
                let var_manager = VarManager::<AC>::new_from_receiver(var_name.clone(), receiver);
                let expr = model.var_expr(var_name).ok_or_else(|| {
                    anyhow!(
                        "No expression found for output variable {} when setting up Monitor",
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
                    "SemiSyncMonitor: Input provider stream returned error: {:?}",
                    res
                );
                return res;
            }
        }
        Ok(())
    }

    fn log_task_errors(
        output_res: anyhow::Result<()>,
        work_res: anyhow::Result<()>,
        input_res: anyhow::Result<()>,
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
        info!("SemiSyncMonitor work_task: Waiting for next tick...");
        let aux_vars = ctx.aux_vars.clone();
        let result = futures::join!(
            ctx.forward_values(),
            Self::eval_expr_evals(expr_evals, &aux_vars)
        );

        // A bit verbose but it is nice for debugging...
        match result {
            (Ok(StreamState::Pending), Ok(StreamState::Pending)) => {
                debug!(
                    "SemiSyncMonitor work_task: Both forward_values and eval_expr_evals pending, continuing..."
                );
                Ok(StreamState::Pending)
            }
            (Ok(StreamState::Finished), Ok(StreamState::Finished)) => {
                debug!(
                    "SemiSyncMonitor work_task: Both forward_values and eval_expr_evals finished, ending work_task."
                );
                Ok(StreamState::Finished)
            }
            (Ok(StreamState::Pending), Ok(StreamState::Finished)) => {
                error!(
                    "SemiSyncMonitor work_task: eval_expr_evals finished but forward_values pending"
                );
                Err(anyhow!(
                    "eval_expr_evals finished but forward_values pending"
                ))
            }
            (Ok(StreamState::Finished), Ok(StreamState::Pending)) => {
                error!(
                    "SemiSyncMonitor work_task: forward_values finished but eval_expr_evals pending"
                );
                Err(anyhow!(
                    "forward_values finished but eval_expr_evals pending"
                ))
            }
            (Ok(_), Err(e)) => {
                error!(?e, "SemiSyncMonitor work_task: Error in eval_expr_evals");
                Err(e)
            }
            (Err(e), Ok(_)) => {
                error!(?e, "SemiSyncMonitor work_task: Error in ctx.forward_values");
                Err(e)
            }
            (Err(e1), Err(e2)) => {
                error!(
                    ?e1,
                    ?e2,
                    "SemiSyncMonitor work_task: Errors in both ctx.forward_values and eval_expr_evals"
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
        let SemiSyncMonitor {
            _executor: _,
            model,
            mut input_provider,
            mut output_handler,
            _marker: _,
        } = self;

        let input_streams = Self::setup_input_streams(&model, &mut *input_provider);
        let (expr_eval_components, mut var_managers) = Self::setup_output_var_managers(&model)?;
        let subscriptions = var_managers
            .iter_mut()
            .map(|vm| vm.subscribe(0))
            .collect::<Vec<OutputStream<AC::Val>>>();
        output_handler.provide_streams(subscriptions);
        let context = Self::build_context(var_managers, input_streams, model.clone());
        let expr_evals = expr_eval_components
            .into_iter()
            .map(|(var_name, expr, sender)| ExprEvalutor::new(var_name, expr, sender, &context))
            .collect();

        Ok((input_provider, output_handler, context, expr_evals))
    }
}

impl<AC, MS> Monitor<AC::Spec, AC::Val> for SemiSyncMonitor<AC, MS>
where
    AC: AsyncConfig<Expr = SExpr, Ctx = SemiSyncContext<AC>>,
    AC::Val: DeferrableStreamData,
    MS: MonitoringSemantics<AC>,
{
    fn spec(&self) -> &AC::Spec {
        &self.model
    }
}

#[async_trait(?Send)]
impl<AC, MS> Runnable for SemiSyncMonitor<AC, MS>
where
    AC: AsyncConfig<Expr = SExpr, Ctx = SemiSyncContext<AC>>,
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
        Self::log_task_errors(output_res, work_res, input_res);

        Ok(())
    }
}

pub struct SemiSyncContextBuilder<AC>
where
    AC: AsyncConfig<Expr = SExpr>,
    AC::Val: DeferrableStreamData,
{
    var_managers: Option<BTreeMap<VarName, VarManager<AC>>>,
    spec: Option<AC::Spec>,
}

impl<AC> SemiSyncContextBuilder<AC>
where
    AC: AsyncConfig<Expr = SExpr>,
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
}

impl<AC> AbstractContextBuilder for SemiSyncContextBuilder<AC>
where
    AC: AsyncConfig<Expr = SExpr, Ctx = SemiSyncContext<AC>>,
    AC::Val: DeferrableStreamData,
{
    type AC = AC;

    fn new() -> Self {
        Self {
            var_managers: None,
            spec: None,
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
        let ctx = SemiSyncContext::new(
            Rc::new(RefCell::new(self.var_managers.expect(
                "VarManagers must be set before building SemiSyncContext",
            ))),
            self.spec
                .expect("Spec must be set before building SemiSyncContext"),
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
    AC: AsyncConfig<Expr = SExpr>,
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
}

impl<AC> SemiSyncContext<AC>
where
    AC: AsyncConfig<Expr = SExpr, Ctx = SemiSyncContext<AC>>,
    AC::Val: DeferrableStreamData,
{
    fn new(var_managers: Rc<RefCell<BTreeMap<VarName, VarManager<AC>>>>, spec: AC::Spec) -> Self {
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
        }
    }

    async fn forward_values(&mut self) -> anyhow::Result<StreamState> {
        // Helper func to run the logic for awaiting manager.
        async fn forward_one<AC>(
            name: &VarName,
            manager: &mut VarManager<AC>,
        ) -> anyhow::Result<StreamState>
        where
            AC: AsyncConfig<Expr = SExpr, Ctx = SemiSyncContext<AC>>,
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
            .spec(self.spec.clone());
        builder.build()
    }

    // Gets the retained history for all the variables inside the context.
    fn get_retained_history(&self) -> BTreeMap<VarName, Vec<AC::Val>> {
        let managers = self.var_managers.borrow();

        let history: BTreeMap<_, Vec<AC::Val>> = managers
            .iter()
            .map(|(var_name, manager)| (var_name.clone(), manager.get_retained_history()))
            .collect();
        history
    }

    // Sets the retained history for the variables in the context based on the provided history.
    fn set_retained_history(&mut self, history: BTreeMap<VarName, Vec<AC::Val>>) {
        let mut managers = self.var_managers.borrow_mut();
        for (var_name, history) in history.into_iter() {
            if let Some(manager) = managers.get_mut(&var_name) {
                manager.set_retained_history(history);
            } else {
                info!(
                    "SemiSyncContext ID {:?}: Tried to set retained history for variable {} but it is not in the context. Most likely due to different specs.",
                    self.id, var_name
                );
            }
        }
    }

    // Transfers the retained history from this context to the VarManagers of the `other` context.
    // New subscribers will receive that history if relevant.
    pub fn context_transfer(&self, other: &mut Self) {
        let history = self.get_retained_history();
        info!(
            "SemiSyncContext ID {:?}: Transferring context to SemiSyncContext ID {:?} with history: {:?}",
            self.id,
            other.id,
            history
                .keys()
                .map(|k| (k, history[k].len()))
                .collect::<BTreeMap<_, _>>()
        );
        other.set_retained_history(history);
    }
}

#[async_trait(?Send)]
impl<AC> StreamContext for SemiSyncContext<AC>
where
    AC: AsyncConfig<Expr = SExpr, Ctx = SemiSyncContext<AC>>,
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
        unimplemented!("StreamContext::cancellation_token")
    }

    fn cancel(&self) {
        unimplemented!("StreamContext::cancel")
    }
}

#[cfg(test)]
mod tests {

    use crate::core::Runnable;
    use crate::dsrv_fixtures::*;
    use crate::io::map::MapInputProvider;
    use crate::io::testing::{ManualOutputHandler, NullOutputHandler};
    use crate::lang::dsrv::lalr_parser::LALRParser;
    use crate::runtime::builder::SemiSyncValueConfig;
    use crate::runtime::semi_sync::{SemiSyncContext, SemiSyncMonitor, VarManager};
    use crate::semantics::{StreamContext, UntimedDsrvSemantics};
    use crate::{OutputStream, async_test};
    use crate::{Value, dsrv_specification};
    use futures::stream::StreamExt;
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::rc::Rc;

    use tc_testutils::streams::{with_timeout, with_timeout_res};

    type TestMonitor = SemiSyncMonitor<SemiSyncValueConfig, UntimedDsrvSemantics<LALRParser>>;

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

        let monitor = TestMonitor {
            _executor: executor.clone(),
            model: spec.clone(),
            input_provider: Box::new(input_streams),
            output_handler,
            _marker: std::marker::PhantomData,
        };

        executor.spawn(monitor.run()).detach();

        let outputs: Vec<(usize, Vec<Value>)> =
            with_timeout(outputs.enumerate().collect(), 1, "outputs")
                .await
                .unwrap();

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
        let monitor = TestMonitor {
            _executor: executor.clone(),
            model: spec.clone(),
            input_provider: Box::new(input_streams),
            output_handler,
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
            spec.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let monitor = TestMonitor {
            _executor: executor.clone(),
            model: spec.clone(),
            input_provider: Box::new(input_streams),
            output_handler,
            _marker: std::marker::PhantomData,
        };

        executor.spawn(monitor.run()).detach();

        let outputs: Vec<(usize, Vec<Value>)> =
            with_timeout(outputs.enumerate().collect(), 1, "outputs")
                .await
                .unwrap();

        assert_eq!(outputs.len(), 3,);
        assert_eq!(
            outputs,
            vec![
                (0, vec![0.into(), 1.into()]),
                (1, vec![1.into(), 2.into()]),
                (2, vec![2.into(), 3.into()]),
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

        let monitor = TestMonitor {
            _executor: executor.clone(),
            model: spec.clone(),
            input_provider: Box::new(input_streams),
            output_handler,
            _marker: std::marker::PhantomData,
        };

        executor.spawn(monitor.run()).detach();

        let outputs: Vec<(usize, Vec<Value>)> =
            with_timeout(outputs.enumerate().collect(), 1, "outputs")
                .await
                .unwrap();

        assert_eq!(outputs.len(), 3,);
        assert_eq!(
            outputs,
            vec![
                (0, vec![1.into()]),
                (1, vec![3.into()]),
                (2, vec![5.into()]),
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

        let monitor = TestMonitor {
            _executor: executor.clone(),
            model: spec.clone(),
            input_provider: Box::new(input_streams),
            output_handler,
            _marker: std::marker::PhantomData,
        };

        executor.spawn(monitor.run()).detach();

        let outputs: Vec<(usize, Vec<Value>)> =
            with_timeout(outputs.enumerate().collect(), 1, "outputs")
                .await
                .unwrap();

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

        let monitor = TestMonitor {
            _executor: executor.clone(),
            model: spec.clone(),
            input_provider: Box::new(input_streams),
            output_handler,
            _marker: std::marker::PhantomData,
        };

        executor.spawn(monitor.run()).detach();

        let outputs: Vec<(usize, Vec<Value>)> =
            with_timeout(outputs.enumerate().collect(), 1, "outputs")
                .await
                .unwrap();

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

        let monitor = TestMonitor {
            _executor: executor.clone(),
            model: spec.clone(),
            input_provider: Box::new(input_streams),
            output_handler,
            _marker: std::marker::PhantomData,
        };

        executor.spawn(monitor.run()).detach();

        let outputs: Vec<(usize, Vec<Value>)> =
            with_timeout(outputs.enumerate().collect(), 1, "outputs")
                .await
                .unwrap();

        assert_eq!(outputs.len(), 3,);
        assert_eq!(
            outputs,
            vec![
                (0, vec![Value::Deferred]),
                (1, vec![Value::Deferred]),
                (2, vec![5.into()]),
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

        let monitor = TestMonitor {
            _executor: executor.clone(),
            model: spec.clone(),
            input_provider: Box::new(input_streams),
            output_handler,
            _marker: std::marker::PhantomData,
        };

        executor.spawn(monitor.run()).detach();

        let outputs: Vec<(usize, Vec<Value>)> =
            with_timeout(outputs.enumerate().collect(), 1, "outputs")
                .await
                .unwrap();

        assert_eq!(outputs.len(), 5,);
        assert_eq!(
            outputs,
            vec![
                (0, vec![Value::Deferred]),
                (1, vec![Value::Deferred]),
                (2, vec![0.into()]),
                (3, vec![1.into()]),
                (4, vec![2.into()]),
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
            spec.output_vars.clone(),
        ));
        let outputs = output_handler.get_output();

        let monitor = TestMonitor {
            _executor: executor.clone(),
            model: spec.clone(),
            input_provider: Box::new(input_streams),
            output_handler,
            _marker: std::marker::PhantomData,
        };

        executor.spawn(monitor.run()).detach();

        let outputs: Vec<(usize, Vec<Value>)> =
            with_timeout(outputs.enumerate().collect(), 1, "outputs")
                .await
                .unwrap();

        assert_eq!(outputs.len(), 3,);
        assert_eq!(
            outputs,
            vec![
                (0, vec![0.into(), 0.into()]),
                (1, vec![1.into(), 1.into()]),
                (2, vec![2.into(), 2.into()]),
            ],
        );
        Ok(())
    }

    #[apply(async_test)]
    async fn test_context_transfer(_executor: Rc<LocalExecutor<'static>>) {
        // Tests that context transfers work purely from a Context perspective (without actually
        // involving the monitor or outputs)

        // Note: The values used in these tests don't actually matter.

        // Note: z has time index of 1 which means we should maintain history for it
        let spec = dsrv_specification(&mut spec_acc_monitor()).unwrap();
        let x_stream: OutputStream<Value> =
            Box::pin(futures::stream::iter((0..3).map(|x| x.into())));
        let z_stream: OutputStream<Value> =
            Box::pin(futures::stream::iter((0..3).map(|z| z.into())));
        let stream_map = BTreeMap::from([
            (
                "x".into(),
                VarManager::<SemiSyncValueConfig>::new("x".into(), x_stream),
            ),
            (
                "z".into(),
                VarManager::<SemiSyncValueConfig>::new("z".into(), z_stream),
            ),
        ]);
        let mut context1 = SemiSyncContext::<SemiSyncValueConfig>::new(
            Rc::new(RefCell::new(stream_map)),
            spec.clone(),
        );
        let x_out = context1.var(&"x".into()).unwrap();
        let z_out = context1.var(&"z".into()).unwrap();

        context1.forward_values().await.unwrap();
        context1.forward_values().await.unwrap();
        context1.forward_values().await.unwrap();
        let x_vals: Vec<_> = with_timeout(x_out.take(3).collect(), 1, "x stream")
            .await
            .unwrap();
        let z_vals: Vec<_> = with_timeout(z_out.take(3).collect(), 1, "z stream")
            .await
            .unwrap();
        assert_eq!(x_vals, vec![0.into(), 1.into(), 2.into()]);
        assert_eq!(z_vals, vec![0.into(), 1.into(), 2.into()]);
        let x_stream: OutputStream<Value> =
            Box::pin(futures::stream::iter((0..3).map(|x| x.into())));
        let z_stream: OutputStream<Value> =
            Box::pin(futures::stream::iter((0..3).map(|z| z.into())));
        let stream_map = BTreeMap::from([
            (
                "x".into(),
                VarManager::<SemiSyncValueConfig>::new("x".into(), x_stream),
            ),
            (
                "z".into(),
                VarManager::<SemiSyncValueConfig>::new("z".into(), z_stream),
            ),
        ]);
        let mut context2 =
            SemiSyncContext::<SemiSyncValueConfig>::new(Rc::new(RefCell::new(stream_map)), spec);
        context1.context_transfer(&mut context2);
        let history = context2.get_retained_history();
        // New context has one in hist for z and 0 for x:
        assert_eq!(
            history,
            BTreeMap::from([("x".into(), vec![]), ("z".into(), vec![2.into()])])
        );
        let x_out = context2.var(&"x".into()).unwrap();
        let z_out = context2.var(&"z".into()).unwrap();
        // Need to forward a new values in order to also make VarManagers send history
        // (This could be changed if var was async)
        context2.forward_values().await.unwrap();
        context2.forward_values().await.unwrap();
        context2.forward_values().await.unwrap();

        let x_vals: Vec<_> = with_timeout(x_out.take(3).collect(), 1, "context transfer stream")
            .await
            .unwrap();
        let z_vals: Vec<_> = with_timeout(z_out.take(4).collect(), 1, "z stream")
            .await
            .unwrap();

        // z carries the history value, x does not
        assert_eq!(x_vals, vec![0.into(), 1.into(), 2.into()]);
        // Note: The streams are now "out of sync". A runtime would still need to forward the necessary amount
        assert_eq!(z_vals, vec![2.into(), 0.into(), 1.into(), 2.into()]);
    }
}
