use crate::{
    OutputStream, SExpr, VarName,
    core::{
        AbstractMonitorBuilder, DeferrableStreamData, InputProvider, Monitor, OutputHandler,
        Runnable, Specification,
    },
    semantics::{AbstractContextBuilder, AsyncConfig, MonitoringSemantics, StreamContext},
    stream_utils::{self},
    utils::cancellation_token::CancellationToken,
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
use tracing::{debug, error, info, warn};
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

    _marker: std::marker::PhantomData<MS>,

    // Kept for debugging
    var_name: VarName,
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
    retained_history: VecDeque<AC::Val>,
    samples_forwarded: usize,
    id: usize,
    input: bool,
}

impl<AC> VarManager<AC>
where
    AC: AsyncConfig<Expr = SExpr>,
    AC::Val: DeferrableStreamData,
{
    fn new(var_name: VarName, value_stream: OutputStream<AC::Val>, input: bool) -> Self {
        let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        debug!(?var_name, "Creating VarManager {}", id);
        Self {
            var_name,
            value_stream,
            subscribers: Vec::new(),
            new_subscribers: Vec::new(),
            retained_history: VecDeque::new(),
            id,
            samples_forwarded: 0,
            input,
        }
    }

    fn new_from_receiver(
        var_name: VarName,
        receiver: spsc::Receiver<AC::Val>,
        input: bool,
    ) -> Self {
        let value_stream = stream_utils::channel_to_output_stream(receiver);
        Self::new(var_name, value_stream, input)
    }

    fn subscribe(&mut self, history_length: usize) -> OutputStream<AC::Val> {
        let history_length = if history_length > 0 {
            warn!(
                "Subtracting one from history_length (val = {}) to circumvent bug with the other async runtime. Will be fixed in the future but requires changing the combinators.",
                history_length - 1,
            );
            history_length - 1
        } else {
            0
        };

        let (tx, rx) = spsc::channel::<AC::Val>(history_length + 8);
        // Compute how many Deferreds are needed - needed because new history_len is longer than
        // retained_history.len()
        let missing = std::cmp::min(
            history_length.saturating_sub(self.retained_history.len()),
            self.samples_forwarded,
        );
        let defer_v = <AC::Val as DeferrableStreamData>::deferred_value();
        // Push them to the history
        std::iter::repeat(defer_v).take(missing).for_each(|v| {
            self.retained_history.push_front(v);
        });
        info!(
            ?self.var_name,
            history_length,
            ?self.retained_history,
            "VarManager {} subscribe: Preparing subscription.",
            self.id,
        );

        // Add to new subs - history will be sent next time we forward values
        self.new_subscribers.push((tx, history_length));

        stream_utils::channel_to_output_stream(rx)
    }

    async fn forward_value(&mut self) -> anyhow::Result<StreamState> {
        info!(
            ?self.var_name,
            "VarManager {} forward_value: Waiting for next value.",
            self.id,
        );

        // Forward the requested history to subscribers
        while let Some((mut tx, history_length)) = self.new_subscribers.pop() {
            let to_send = std::cmp::min(self.samples_forwarded, history_length);
            self.retained_history.iter().skip(
                self.retained_history
                    .len()
                    .saturating_sub(to_send),
            ).for_each(|v| {
                    // Should never fail as the capacity is always sufficient
                    if let Err(e) = tx.try_send(v.clone()) {
                        error!(
                            ?self.var_name,
                            ?e,
                            "VarManager {} subscribe: Error sending retained history value to new subscriber.",
                            self.id,
                        );
                        panic!("Error sending retained history value to new subscriber: {}", e);
                    }
                });
            // Now add to subscribers
            self.subscribers.push(tx);
        }

        if let Some(val) = self.value_stream.next().await {
            info!(
                ?self.var_name,
                ?val,
                "VarManager {} forward_value: Forwarding value to {} subscribers.",
                self.id,
                self.subscribers.len()
            );
            self.samples_forwarded += 1;

            // Retain in history if needed
            if self.retained_history.len() > 0 {
                self.retained_history.pop_front();
                self.retained_history.push_back(val.clone());
                info!(
                    ?self.var_name,
                    ?self.retained_history,
                    "VarManager {} forward_value: Updated retained history.",
                    self.id,
                );
            }

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
            // TODO: Make this more clean
            // Close the channels early to let receivers know we are done
            self.subscribers = vec![];
            self.new_subscribers = vec![];
            Ok(StreamState::Finished)
        }
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
                let var_manager =
                    VarManager::<AC>::new_from_receiver(var_name.clone(), receiver, false);
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
    ) -> SemiSyncContext<AC> {
        let var_managers = var_managers
            .into_iter()
            .chain(
                input_streams
                    .into_iter()
                    .map(|(var_name, stream)| VarManager::<AC>::new(var_name, stream, true)),
            )
            .map(|vm| (vm.var_name.clone(), vm))
            .collect();

        SemiSyncContextBuilder::new()
            .var_managers(var_managers)
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
    ) -> anyhow::Result<StreamState> {
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

    pub async fn step(
        ctx: &mut SemiSyncContext<AC>,
        expr_evals: &mut Vec<ExprEvalutor<AC, MS>>,
    ) -> anyhow::Result<StreamState> {
        info!("SemiSyncMonitor work_task: Waiting for next tick...");
        let result = futures::join!(ctx.forward_values(), Self::eval_expr_evals(expr_evals));

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
        let context = Self::build_context(var_managers, input_streams);
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
    history_length: Option<usize>,
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
            self.history_length.unwrap_or(0),
        );
        info!(
            "SemiSyncContextBuilder: Built SemiSyncContext with id {:?} and VarManagers: {:?}",
            ctx.id,
            ctx.var_managers.borrow().keys()
        );
        return ctx;
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
    // History length for new calls to var
    history_length: usize,
    // Unique identifier for this variable manager
    id: usize,
}

impl<AC> SemiSyncContext<AC>
where
    AC: AsyncConfig<Expr = SExpr, Ctx = SemiSyncContext<AC>>,
    AC::Val: DeferrableStreamData,
{
    fn new(
        var_managers: Rc<RefCell<BTreeMap<VarName, VarManager<AC>>>>,
        history_length: usize,
    ) -> Self {
        let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        debug!(
            "Creating SemiSyncContext {:?} with vars: {:?}",
            id,
            var_managers.borrow().keys()
        );
        Self {
            var_managers,
            history_length,
            id,
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
        // Forward inputs first to avoid deadlocks
        for (name, manager) in managers.iter_mut() {
            if manager.input {
                let state = forward_one::<AC>(name, manager).await?;
                if state == StreamState::Finished {
                    to_remove.push(name.clone());
                }
            }
        }
        // Forward rest
        for (name, manager) in managers.iter_mut() {
            if !manager.input {
                let state = forward_one::<AC>(name, manager).await?;
                if state == StreamState::Finished {
                    to_remove.push(name.clone());
                }
            }
        }
        managers.retain(|name, _| !to_remove.contains(name));
        Ok(if managers.is_empty() {
            warn!(
                "SemiSyncContext ID: {:?}: All VarManagers finished",
                self.id
            );
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
                    let input = manager.input;
                    Some((
                        var_name.clone(),
                        VarManager::<AC>::new(var_name.clone(), stream, input),
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
            .history_length(history_length);
        builder.build()
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
        debug!(
            "SemiSyncContext ID {:?}: VarManagers available {:?}",
            self.id,
            manager.keys()
        );
        let stream = manager.get_mut(x)?.subscribe(self.history_length);
        info!(
            self.id,
            ?x,
            "SemiSyncContext::var: Created new output stream for variable"
        );
        Some(stream)
    }

    fn subcontext(&self, history_length: usize) -> Self {
        info!(
            self.id,
            ?history_length,
            "SemiSyncContext::subcontext: Creating subcontext."
        );
        // Note: Must be in separate variable to avoid double borrow
        let vars = self
            .var_managers
            .borrow()
            .keys()
            .cloned()
            .collect::<EcoVec<VarName>>();
        self.subcontext_common(vars, history_length)
    }

    fn restricted_subcontext(&self, vs: EcoVec<VarName>, history_length: usize) -> Self {
        info!(
            ?vs,
            ?history_length,
            "SemiSyncContext::restricted_subcontext: Creating restricted subcontext with parent id: {}",
            self.id
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

    fn cancellation_token(&self) -> CancellationToken {
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
    use crate::dsrv_fixtures::*;
    use crate::io::map::MapInputProvider;
    use crate::io::testing::{ManualOutputHandler, NullOutputHandler};
    use crate::lang::dsrv::lalr_parser::LALRParser;
    use crate::runtime::builder::SemiSyncValueConfig;
    use crate::runtime::semi_sync::SemiSyncMonitor;
    use crate::semantics::UntimedDsrvSemantics;
    use crate::{Value, dsrv_specification};
    use futures::stream::StreamExt;
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
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
}
