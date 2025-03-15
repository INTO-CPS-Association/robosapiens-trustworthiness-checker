use core::panic;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::mem;
use std::rc::Rc;

use async_stream::stream;
use async_trait::async_trait;
use async_unsync::bounded;
use async_unsync::oneshot;
use async_unsync::semaphore;
use futures::StreamExt;
use futures::future::LocalBoxFuture;
use futures::future::join_all;
use smol::LocalExecutor;
use strum_macros::Display;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tokio_util::sync::DropGuard;
use tracing::Level;
use tracing::debug;
use tracing::info;
use tracing::instrument;
use tracing::warn;

use crate::core::InputProvider;
use crate::core::Monitor;
use crate::core::MonitoringSemantics;
use crate::core::OutputHandler;
use crate::core::Specification;
use crate::core::SyncStreamContext;
use crate::core::{OutputStream, StreamContext, StreamData, VarName};
use crate::dep_manage::interface::DependencyManager;
use crate::stream_utils::{drop_guard_stream, oneshot_to_stream};

/// Track the stage of the context determining the lifecycle of all variables
/// managed by it
#[derive(Debug, Display, Clone, PartialEq, Eq)]
enum ContextStage {
    /// Only waiting for new subscriptions - no values can be received
    Gathering,
    /// Can grant new subs to the variable or provide values in a time
    /// synchronised manner
    Open,
    /// No new subs can be granted, but values can still be provided (not
    /// necessarily time synchronised)
    Closed,
}

/// An actor which manages access to and retention of a stream variable
/// throughout its lifecycle by tracking the subscribers to the variable
/// and creating independent output streams to forward new data to each
/// subscriber.
///
/// This actor goes through three stages of lifecycle determined by the
/// `ContextStage` enum:
/// 1. Gathering: In this stage, the actor waits for all subscribers to request
///    output streams.
/// 2. Open: In this stage, the actor forwards data from the input stream to all
///    subscribers but can also still grant new subscriptions.
/// 3. Closed: In this stage, the actor stops granting new subscriptions but
///    continues to forward data to all subscribers. Moreover, the actor may
///    stop running or directly forward the input stream if there is only one
///    subscriber.
struct VarManager<V: StreamData> {
    executor: Rc<LocalExecutor<'static>>,
    var: VarName,
    input_stream: Rc<RefCell<Option<OutputStream<V>>>>,
    context_stage: watch::Receiver<ContextStage>,
    current_context_stage: ContextStage,
    outstanding_sub_requests: Rc<RefCell<usize>>,
    var_semaphore: Rc<semaphore::Semaphore>,
    subscribers: Rc<RefCell<Vec<bounded::Sender<V>>>>,
    clock: Rc<RefCell<usize>>,
}

impl<V: StreamData> VarManager<V> {
    fn new(
        executor: Rc<LocalExecutor<'static>>,
        var: VarName,
        input_stream: OutputStream<V>,
        context_stage: watch::Receiver<ContextStage>,
    ) -> Self {
        let current_context_stage = ContextStage::Gathering;
        let var_semaphore = Rc::new(semaphore::Semaphore::new(1));
        let clock = Rc::new(RefCell::new(0));
        let subscribers = Rc::new(RefCell::new(vec![]));
        let outstanding_sub_requests = Rc::new(RefCell::new(0));
        Self {
            executor,
            var,
            input_stream: Rc::new(RefCell::new(Some(input_stream))),
            var_semaphore,
            context_stage,
            current_context_stage,
            outstanding_sub_requests,
            subscribers,
            clock,
        }
    }

    fn subscribe(&mut self) -> OutputStream<V> {
        let semaphore = self.var_semaphore.clone();
        let input_stream_ref = self.input_stream.clone();
        let subscribers_ref = self.subscribers.clone();
        let mut current_context_stage = self.current_context_stage.clone();
        let mut context_stage = self.context_stage.clone();
        let (output_tx, output_rx) = oneshot::channel().into_split();
        let outstanding_sub_requests_ref = self.outstanding_sub_requests.clone();
        *self.outstanding_sub_requests.borrow_mut() += 1;
        semaphore.remove_permits(1);
        let (tx, mut rx) = bounded::channel(10).into_split();
        subscribers_ref.borrow_mut().push(tx);

        self.executor
            .spawn(async move {
                if current_context_stage == ContextStage::Closed {
                    panic!("Cannot subscribe to a variable in the closed stage");
                }

                if current_context_stage == ContextStage::Gathering {
                    debug!("Waiting for context stage to move to open or closed");
                    context_stage
                        .wait_for(|stage| *stage != ContextStage::Gathering)
                        .await
                        .unwrap();
                    current_context_stage = context_stage.borrow().clone();
                    debug!("done waiting with current stage {}", current_context_stage);
                }

                let res = if current_context_stage == ContextStage::Closed
                    && subscribers_ref.borrow().len() == 1
                {
                    // Take the stream out of the RefCell by replacing it with None
                    let mut input_ref = input_stream_ref.borrow_mut();
                    let stream = input_ref.take().unwrap();
                    output_tx.send(stream).unwrap();
                } else if current_context_stage == ContextStage::Open
                    || current_context_stage == ContextStage::Closed
                {
                    debug!("Sending stream to subscriber");
                    output_tx
                        .send(Box::pin(stream! {
                            while let Some(data) = rx.recv().await {
                                yield data;
                            }
                        }) as OutputStream<V>)
                        .unwrap();
                    debug!("done sending stream to subscriber");
                } else {
                    unreachable!()
                };

                if *outstanding_sub_requests_ref.borrow() == 1 {
                    debug!("Adding permit back to semaphore");
                    semaphore.add_permits(1);
                };
                *outstanding_sub_requests_ref.borrow_mut() -= 1;

                res
            })
            .detach();

        oneshot_to_stream(output_rx)
    }

    fn tick(&self) -> LocalBoxFuture<'static, bool> {
        let semaphore = self.var_semaphore.clone();
        let input_stream_ref = self.input_stream.clone();
        let subscribers_ref = self.subscribers.clone();
        let clock_ref = self.clock.clone();
        let var = self.var.clone();

        Box::pin(async move {
            debug!(?var, "Waiting for permit");
            let _permit = semaphore.acquire().await.unwrap();
            debug!(?var, "Acquired permit");

            debug!(?var, "Distributing single");

            let mut binding = input_stream_ref.borrow_mut();
            let input_stream = match binding.as_mut() {
                Some(stream) => stream,
                None => {
                    debug!("Input stream is none; stopping distribution");
                    return false;
                }
            };

            match input_stream.next().await {
                Some(data) => {
                    info!(?data, "Distributing data");
                    *clock_ref.borrow_mut() += 1;
                    let mut to_delete = vec![];
                    for (i, child_sender) in subscribers_ref.borrow().iter().enumerate() {
                        if let Err(_) = child_sender.send(data.clone()).await {
                            info!("Stopping distributing to receiver since it has been dropped");
                            to_delete.push(i);
                        }
                    }
                    for i in to_delete {
                        subscribers_ref.borrow_mut().remove(i);
                    }
                    info!("Distributed data");
                }
                None => {
                    *clock_ref.borrow_mut() += 1;
                    info!("Stopped distributing data due to end of input stream");
                    return false;
                }
            }

            true
        })
    }

    fn run(self) -> LocalBoxFuture<'static, ()> {
        Box::pin(async move { while self.tick().await {} })
    }
}

/// Create a wrapper around an input stream which stores a history buffer of
/// data of length history_length for retrospective monitoring
fn store_history<V: StreamData>(
    executor: Rc<LocalExecutor<'static>>,
    var: VarName,
    history_length: usize,
    mut input_stream: OutputStream<V>,
) -> OutputStream<V> {
    if history_length == 0 {
        return input_stream;
    }

    let (send, mut recv) = bounded::channel(history_length).into_split();

    executor
        .spawn(async move {
            while let Some(data) = input_stream.next().await {
                debug!(
                    ?var,
                    ?data,
                    ?history_length,
                    "monitored history data for history"
                );
                if let Err(_) = send.send(data).await {
                    debug!(
                        ?var,
                        ?history_length,
                        "Failed to send data due to no receivers; shutting down"
                    );
                    return;
                }
            }
            debug!("store_history out of input data");
        })
        .detach();

    Box::pin(stream! {
        while let Some(data) = recv.recv().await {
            yield data;
            debug!("store_history yielded data");
        }
        debug!("store_history finished history data");
    })
}

/// A context which consumes data for a set of variables and makes
/// it available when evaluating a deferred expression
//
/// This is implemented in the background using a combination of
/// manage_var and store history actors
struct Context<Val: StreamData> {
    /// The executor which is used to run background tasks
    executor: Rc<LocalExecutor<'static>>,
    /// The variables which are available in the context
    vars: Vec<VarName>,
    /// The amount of history stored for retrospective monitoring
    /// of each variable (0 means no history)
    #[allow(dead_code)]
    history_length: usize,
    /// The current stage of the context
    ctx_stage: watch::Sender<ContextStage>,
    /// Current clock
    clock: usize,
    /// Variable manangers
    var_managers: Rc<RefCell<BTreeMap<VarName, VarManager<Val>>>>,
    /// The cancellation token used to cancel all background tasks
    cancellation_token: CancellationToken,
}

impl<Val: StreamData> Context<Val> {
    fn new(
        executor: Rc<LocalExecutor<'static>>,
        input_streams: BTreeMap<VarName, OutputStream<Val>>,
        history_length: usize,
        cancellation_token: CancellationToken,
    ) -> Self {
        let mut vars = Vec::new();
        let ctx_stage = watch::channel(ContextStage::Gathering);
        let clock: usize = 0;
        // TODO: push the mutability to the API of contexts
        let var_managers = Rc::new(RefCell::new(BTreeMap::new()));

        for (var, input_stream) in input_streams.into_iter() {
            vars.push(var.clone());
            let input_stream =
                store_history(executor.clone(), var.clone(), history_length, input_stream);
            var_managers.borrow_mut().insert(
                var.clone(),
                VarManager::new(
                    executor.clone(),
                    var.clone(),
                    input_stream,
                    ctx_stage.1.clone(),
                ),
            );
        }

        Context {
            executor,
            vars,
            history_length,
            clock,
            var_managers,
            ctx_stage: ctx_stage.0,
            cancellation_token,
        }
    }
}

impl<Val: StreamData> StreamContext<Val> for Context<Val> {
    fn var(&self, var: &VarName) -> Option<OutputStream<Val>> {
        if self.is_clock_started() {
            panic!("Cannot request a stream after the clock has started");
        }

        let mut var_managers = self.var_managers.borrow_mut();
        let var_manager = var_managers.get_mut(var)?;

        Some(var_manager.subscribe())
    }

    fn subcontext(&self, history_length: usize) -> Box<dyn SyncStreamContext<Val>> {
        let input_streams = self
            .vars
            .iter()
            .map(|var| (var.clone(), self.var(var).unwrap()))
            .collect();

        // Recursively create a new context based on ourself
        Box::new(Context::new(
            self.executor.clone(),
            input_streams,
            history_length,
            self.cancellation_token.clone(),
        ))
    }
}

#[async_trait(?Send)]
impl<Val: StreamData> SyncStreamContext<Val> for Context<Val> {
    async fn advance_clock(&mut self) {
        if self.ctx_stage.borrow().clone() == ContextStage::Gathering {
            self.ctx_stage.send(ContextStage::Open).unwrap();
        }
        join_all(
            self.var_managers
                .borrow_mut()
                .iter_mut()
                .map(|(_, var_manager)| var_manager.tick()),
        )
        .await;
        self.clock += 1;
    }

    async fn lazy_advance_clock(&mut self) {
        if self.ctx_stage.borrow().clone() == ContextStage::Gathering {
            self.ctx_stage.send(ContextStage::Open).unwrap();
        }
        for (_, var_manager) in self.var_managers.borrow_mut().iter_mut() {
            self.executor.spawn(var_manager.tick()).detach();
        }
        self.clock += 1;
    }

    fn clock(&self) -> usize {
        self.clock
    }

    async fn start_auto_clock(&mut self) {
        if !self.is_clock_started() {
            self.ctx_stage
                .send(ContextStage::Closed)
                .expect("Failed to send closed stage to context");
            let mut var_managers = self.var_managers.borrow_mut();
            for (_, var_manager) in mem::take(&mut *var_managers).into_iter() {
                self.executor.spawn(var_manager.run()).detach();
            }
            self.clock = usize::MAX;
        }
    }

    fn is_clock_started(&self) -> bool {
        self.clock() == usize::MAX
    }

    fn upcast(&self) -> &dyn StreamContext<Val> {
        self
    }
}

/// A Monitor instance implementing the Async Runtime.
///
/// This runtime uses async actors to keep track of dependencies between
/// channels and to distribute data between them, pass data around via async
/// streams, and automatically perform garbage collection of the data contained
/// in the streams.
///
///  - The Expr type parameter is the type of the expressions in the model.
///  - The Val type parameter is the type of the values used in the channels.
///  - The S type parameter is the monitoring semantics used to evaluate the
///    expressions as streams.
///  - The M type parameter is the model/specification being monitored.
pub struct AsyncMonitorRunner<Expr, Val, S, M>
where
    Val: StreamData,
    S: MonitoringSemantics<Expr, Val>,
    M: Specification<Expr = Expr>,
    Expr: Sync + Send,
{
    #[allow(dead_code)]
    executor: Rc<LocalExecutor<'static>>,
    model: M,
    output_handler: Box<dyn OutputHandler<Val = Val>>,
    output_streams: BTreeMap<VarName, OutputStream<Val>>,
    #[allow(dead_code)]
    // This is used for RAII to cancel background tasks when the async var
    // exchange is dropped
    cancellation_guard: Rc<DropGuard>,
    expr_t: PhantomData<Expr>,
    semantics_t: PhantomData<S>,
}

#[async_trait(?Send)]
impl<Expr: Sync + Send, Val, S, M> Monitor<M, Val> for AsyncMonitorRunner<Expr, Val, S, M>
where
    Val: StreamData,
    S: MonitoringSemantics<Expr, Val>,
    M: Specification<Expr = Expr>,
{
    fn new(
        executor: Rc<LocalExecutor<'static>>,
        model: M,
        input_streams: &mut dyn InputProvider<Val = Val>,
        output: Box<dyn OutputHandler<Val = Val>>,
        _dependencies: DependencyManager,
    ) -> Self {
        let cancellation_token = CancellationToken::new();
        let cancellation_guard = Rc::new(cancellation_token.clone().drop_guard());

        let input_vars = model.input_vars().clone();
        let output_vars = model.output_vars().clone();

        let input_streams = input_vars.iter().map(|var| {
            let stream = input_streams.input_stream(var).unwrap();
            (var.clone(), stream)
        });

        // Create deferred streams based on each of the output variables
        let output_oneshots: Vec<_> = output_vars
            .iter()
            .cloned()
            .map(|_| oneshot::channel::<OutputStream<Val>>().into_split())
            .collect();
        let (output_txs, output_rxs): (Vec<_>, Vec<_>) = output_oneshots.into_iter().unzip();
        let output_txs: BTreeMap<_, _> = output_vars
            .iter()
            .cloned()
            .zip(output_txs.into_iter())
            .collect();
        let output_streams = output_rxs.into_iter().map(oneshot_to_stream);
        let output_streams = output_vars.iter().cloned().zip(output_streams.into_iter());

        // Combine the input and output streams into a single map
        let streams = input_streams.chain(output_streams.into_iter()).collect();

        let mut context = Context::new(executor.clone(), streams, 0, cancellation_token.clone());

        // Create a map of the output variables to their streams
        // based on using the context
        let output_streams = model
            .output_vars()
            .iter()
            .map(|var| {
                (
                    var.clone(),
                    // Add a guard to the stream to cancel background
                    // tasks whenever all the outputs are dropped
                    drop_guard_stream(
                        context.var(var).expect(
                            format!("Failed to find expression for var {}", var.0.as_str())
                                .as_str(),
                        ),
                        cancellation_guard.clone(),
                    ),
                )
            })
            .collect();

        // Send outputs computed based on the context to the
        // output handler
        for (var, tx) in output_txs {
            let expr = model
                .var_expr(&var)
                .expect(format!("Failed to find expression for var {}", var.0.as_str()).as_str());
            let stream = S::to_async_stream(expr, &context);
            if let Err(_) = tx.send(stream) {
                warn!(?var, "Failed to send stream for var to requester");
            }
        }

        executor
            .spawn(async move {
                context.start_auto_clock().await;
            })
            .detach();

        Self {
            executor,
            model,
            output_streams,
            semantics_t: PhantomData,
            cancellation_guard,
            expr_t: PhantomData,
            output_handler: output,
        }
    }

    fn spec(&self) -> &M {
        &self.model
    }

    #[instrument(name="Running async Monitor", level=Level::INFO, skip(self))]
    async fn run(mut self) {
        self.output_handler.provide_streams(self.output_streams);
        self.output_handler.run().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use macro_rules_attribute::apply;
    use smol_macros::test as smol_test;
    use test_log::test;

    #[test(apply(smol_test))]
    async fn test_manage_var_gathering(executor: Rc<LocalExecutor<'static>>) {
        let input_stream = Box::pin(stream! {
            yield 1;
            yield 2;
            yield 3;
        });

        let (stage_tx, stage_rx) = watch::channel(ContextStage::Gathering);

        let mut manager = VarManager::new(
            executor.clone(),
            VarName("test".to_string()),
            input_stream,
            stage_rx,
        );

        info!("subscribing 1");
        let sub1 = manager.subscribe();
        info!("subscribing 2");
        let sub2 = manager.subscribe();

        info!("Closing");
        stage_tx.send(ContextStage::Closed).unwrap();

        info!("running manager");
        executor.spawn(manager.run()).detach();

        let output1 = sub1.collect::<Vec<_>>().await;
        let output2 = sub2.collect::<Vec<_>>().await;

        assert_eq!(output1, vec![1, 2, 3]);
        assert_eq!(output2, vec![1, 2, 3]);
    }

    #[test(apply(smol_test))]
    async fn test_manage_open_and_ticking(executor: Rc<LocalExecutor<'static>>) {
        let input_stream = Box::pin(stream! {
            yield 1;
            yield 2;
            yield 3;
            yield 4;
        });

        let (stage_tx, stage_rx) = watch::channel(ContextStage::Gathering);

        let mut manager = VarManager::new(
            executor.clone(),
            VarName("test".to_string()),
            input_stream,
            stage_rx,
        );

        info!("Moving to open and ticking 1");
        stage_tx.send(ContextStage::Open).unwrap();
        manager.tick().await;

        info!("subscribing 1");
        let mut sub1 = manager.subscribe();
        info!("ticking 2");
        manager.tick().await;

        info!("checking output 1");
        let sub1_output = sub1.next().await.unwrap();
        assert_eq!(sub1_output, 2);

        info!("subscribing 2");
        let sub2 = manager.subscribe();
        info!("ticking 3");
        manager.tick().await;
        info!("ticking 4");
        manager.tick().await;

        info!("checking output 2");
        let output1 = sub1.take(2).collect::<Vec<_>>().await;
        let output2 = sub2.take(2).collect::<Vec<_>>().await;

        assert_eq!(output1, vec![3, 4]);
        assert_eq!(output2, vec![3, 4]);
    }
}
