use core::panic;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt::{Debug, Display};
use std::future::ready;
use std::iter::once;
use std::marker::PhantomData;
use std::mem;
use std::pin::Pin;
use std::rc::Rc;

use crate::utils::cancellation_token::CancellationToken;
use anyhow::anyhow;
use async_cell::unsync::AsyncCell;
use async_stream::stream;
use async_trait::async_trait;
use async_unsync::bounded;
use async_unsync::oneshot;
use async_unsync::semaphore;
use futures::future::LocalBoxFuture;
use futures::future::join_all;
use futures::{FutureExt, StreamExt, join, select};
use smol::LocalExecutor;
use strum_macros::Display;
use tracing::debug;
use tracing::info;
use tracing::instrument;
use tracing::warn;
use tracing::{Level, error};

use crate::core::AbstractMonitorBuilder;
use crate::core::InputProvider;
use crate::core::Monitor;
use crate::core::OutputHandler;
use crate::core::Runnable;
use crate::core::Specification;
use crate::core::{OutputStream, StreamData, VarName};
use crate::semantics::{AbstractContextBuilder, AsyncConfig, MonitoringSemantics, StreamContext};
use crate::stream_utils::{drop_guard_stream, oneshot_to_stream};

/// Track the stage of a variable's lifecycle
#[derive(Debug, Display, Clone, PartialEq, Eq)]
enum VarStage {
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
/// This actor goes through three stages of the hidden internal lifecycle
/// determined by the `ContextStage` enum:
/// 1. Gathering: In this initial stage, the actor waits for all subscribers
///    to request output streams.
/// 2. Open: In this stage, the actor forwards data from the input stream to all
///    subscribers but can also still grant new subscriptions. This stage
///    starts when tick is called manually and ends if run is called.
/// 3. Closed: In this stage, the actor stops granting new subscriptions but
///    continues to forward data to all subscribers.
///    run is called.
/// This lifecycle allows us to optimize how we distribute data based on the
/// total number of subscribers to a variable in the case where all
/// subscriptions occur before the first tick. In particular, we can directly
/// forward the input stream if there is only a single subscriber or stop
/// distributing data if there are no subscribers.
///
/// The actor also maintains a clock which is incremented each time a new value
/// is distributed to the subscribers.
///
/// The data inside the var manager needs to be contained in Rc<RefCell<...>>s
/// since the async tasks spawned by the actor need may outlive the var manager
/// itself. The semaphore var_semaphore is used to control access to the
/// variable, ensure in particular that:
///  - only one time tick can happen at once
///  - we can't tick when there are outstanding subscription requests
pub struct VarManager<V: StreamData> {
    /// The executor which is used to run background tasks
    executor: Rc<LocalExecutor<'static>>,
    /// The variable name which this manager is responsible for
    var: VarName,
    /// The input stream which is feeding data into the variable
    input_stream: Rc<RefCell<Option<OutputStream<V>>>>,
    /// The current stage of the variable's lifetime
    var_stage: Rc<AsyncCell<VarStage>>,
    /// The number of outstanding unfulfilled subscription requests
    outstanding_sub_requests: Rc<RefCell<usize>>,
    /// The semaphore used to control access to the variable
    /// (only one time tick can happen at once, and we can't tick when there
    /// are outstanding subscription requests)
    var_semaphore: Rc<semaphore::Semaphore>,
    /// A sender to give messages to each subscriber to the variable
    subscribers: Rc<RefCell<Vec<bounded::Sender<V>>>>,
    /// The current clock value of the variable
    clock: Rc<RefCell<usize>>,
    /// Unique identifier for this variable manager
    id: usize,
    /// Cancellation token to stop the VarManager when output streams are dropped
    cancellation_token: CancellationToken,
}

static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

impl<V: StreamData> VarManager<V> {
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        var: VarName,
        input_stream: OutputStream<V>,
        cancellation_token: CancellationToken,
    ) -> Self {
        let var_stage = AsyncCell::new_with(VarStage::Gathering).into_shared();
        let var_semaphore = Rc::new(semaphore::Semaphore::new(1));
        let clock = Rc::new(RefCell::new(0));
        let subscribers = Rc::new(RefCell::new(vec![]));
        let outstanding_sub_requests = Rc::new(RefCell::new(0));
        let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self {
            executor,
            var,
            input_stream: Rc::new(RefCell::new(Some(input_stream))),
            var_semaphore,
            var_stage,
            outstanding_sub_requests,
            subscribers,
            clock,
            id,
            cancellation_token,
        }
    }

    /// Subscribe to the variable and return a stream of its output
    pub fn subscribe(&mut self) -> OutputStream<V> {
        // Make owned copies of references to variables owned by the struct
        // so that these are not borrowed when the async block is spawned
        let semaphore = self.var_semaphore.clone();
        let input_stream_ref = self.input_stream.clone();
        let subscribers_ref = self.subscribers.clone();
        let var_stage_ref = self.var_stage.clone();
        let outstanding_sub_requests_ref = self.outstanding_sub_requests.clone();
        let var = self.var.clone();
        let id = self.id;

        debug!(?var, "VarManager {id}: Subscribing to variable");

        // Create a oneshot channel to send the output stream to the subscriber
        let (output_tx, output_rx) = oneshot::channel().into_split();

        // Prepare an inner channel which will be used to return the output
        let (tx, mut rx) = bounded::channel(1000).into_split();
        subscribers_ref.borrow_mut().push(tx);

        // Increment the number of outstanding subscription requests and remove
        // a permit from the semaphore to prevent any output on the variable
        // until the subscription is complete
        *self.outstanding_sub_requests.borrow_mut() += 1;
        semaphore.remove_permits(1);

        // This should return immediately since var_stage is never None
        // (it is initialized to Gathering in the constructor)
        let mut current_var_stage = smol::future::block_on(var_stage_ref.get_shared());

        // Spawn a background async task to handle the subscription request
        self.executor
            .spawn(async move {
                if current_var_stage == VarStage::Closed {
                    panic!("VarManager {id}: Cannot subscribe to a variable in the closed stage");
                }

                while current_var_stage == VarStage::Gathering {
                    debug!(
                        ?var,
                        "VarManager {id}: Waiting for context stage to move to open or closed"
                    );
                    smol::future::yield_now().await;
                    current_var_stage = var_stage_ref.get_shared().await;
                    debug!(
                        ?var,
                        "VarManager {id}: var stage changed {}", current_var_stage
                    );
                }

                if current_var_stage == VarStage::Closed
                    && subscribers_ref.borrow().len() == 1
                {
                    debug!("VarManager {id}: Directly sending stream to single subscriber");
                    // Take the stream out of the RefCell by replacing it with None
                    let mut input_ref = input_stream_ref.borrow_mut();
                    let stream = input_ref.take().unwrap();
                    output_tx.send(stream).expect(format!("VarManager {}: Tried to send output for variable {}", id, var).as_str());
                    subscribers_ref.borrow_mut().pop();
                } else if current_var_stage == VarStage::Open
                    || current_var_stage == VarStage::Closed
                {
                    debug!("VarManager {id}: Sending stream to subscriber");
                    output_tx
                        .send(Box::pin(stream! {
                            let id = id;
                            loop {
                                debug!("VarManager {id}: Waiting to forward to subs");
                                if let Some(data) = rx.recv().await {
                                    debug!("VarManager {id}: Forwarding {:?} to subs", data);
                                    yield data;
                                }
                                else {
                                    debug!("VarManager {id}: Stream ended - no more to forward to subs");
                                    return;
                                }
                            }
                        }) as OutputStream<V>).expect(&format!("VarManager {var} with id {id}: Failed to send stream to subscriber - receiver dropped"));
                    debug!("VarManager {id}: done sending stream to subscriber");
                } else {
                    unreachable!()
                };

                if *outstanding_sub_requests_ref.borrow() == 1 {
                    debug!("VarManager {id}: Adding permit back to semaphore");
                    semaphore.add_permits(1);
                };
                *outstanding_sub_requests_ref.borrow_mut() -= 1;
            })
            .detach();

        // Return a lazy stream which will be filled start producing data
        // once the subscription is complete
        oneshot_to_stream(output_rx)
    }

    /// Distribute the next value from the input stream to all subscribers.
    /// The future will be completed once all of the data has been
    /// sent to all subscribers (i.e. placed in their input buffers) but will
    /// not wait until they have processed it.
    pub fn tick(&self) -> LocalBoxFuture<'static, bool> {
        // Make owned copies of references to variables owned by the struct
        // so that these are not borrowed when the async block is returned
        let semaphore = self.var_semaphore.clone();
        let input_stream_ref = self.input_stream.clone();
        let subscribers_ref = self.subscribers.clone();
        let clock_ref = self.clock.clone();
        let var = self.var.clone();
        let var_stage = self.var_stage.clone();
        let cancellation_token = self.cancellation_token.clone();
        let id = self.id;

        debug!("VarManager {}: Starting tick for variable '{}'", id, var);

        // Return a future which will actually do the distribution
        Box::pin(async move {
            // Check if cancellation was requested
            if cancellation_token.is_cancelled().await {
                debug!("VarManager {id}: Cancellation requested, stopping tick");
                return false;
            }

            // Move to the open stage if we are in the gathering stage and we
            // have been ticked
            if var_stage.get().await == VarStage::Gathering {
                debug!("VarManager {id}: Moving to open stage from tick");
                var_stage.set(VarStage::Open);
            }

            debug!(?var, "VarManager {id}: Waiting for permit");
            let _permit = select! {
                permit = semaphore.acquire().fuse() => permit.unwrap(),
                _ = cancellation_token.cancelled().fuse() => {
                    debug!("VarManager {id}: Cancellation requested during semaphore acquisition");
                    return false;
                }
            };
            debug!(?var, "VarManager {id}: Acquired permit");

            debug!(?var, "VarManager {id}: Distributing single");

            let mut binding = input_stream_ref.borrow_mut();
            let input_stream = match binding.as_mut() {
                Some(stream) => stream,
                None => {
                    debug!("VarManager {id}: Input stream is none; stopping distribution");
                    return false;
                }
            };

            if var_stage.get().await == VarStage::Closed && subscribers_ref.borrow().is_empty() {
                debug!("VarManager {id}: No subscribers; stopping distribution");
                return false;
            }

            // Use select! to race input_stream.next() against cancellation
            debug!(
                "VarManager {id}: Waiting for next value for variable '{}'",
                var
            );
            let next_result = select! {
                next_item = input_stream.next().fuse() => {
                    debug!("VarManager {id}: Received next item for '{}': {:?}", var, next_item);
                    next_item
                },
                _ = cancellation_token.cancelled().fuse() => {
                    debug!("VarManager {id}: Cancellation requested during input stream read");
                    return false;
                }
            };

            match next_result {
                Some(data) => {
                    debug!(?data, "VarManager {id}: Distributing data");
                    *clock_ref.borrow_mut() += 1;
                    let mut to_delete = vec![];

                    for (i, child_sender) in subscribers_ref.borrow().iter().enumerate() {
                        // Use select! to race send operation against cancellation
                        let send_result = select! {
                            result = child_sender.send(data.clone()).fuse() => result,
                            _ = cancellation_token.cancelled().fuse() => {
                                debug!("VarManager {id}: Cancellation requested during send");
                                return false;
                            }
                        };

                        if send_result.is_err() {
                            info!(
                                "VarManager {id}: Stopping distributing to receiver since it has been dropped"
                            );
                            to_delete.push(i);
                        }
                    }
                    for i in to_delete.iter().rev() {
                        subscribers_ref.borrow_mut().remove(*i);
                    }
                    debug!("VarManager {id}: Distributed data");
                }
                None => {
                    *clock_ref.borrow_mut() += 1;
                    info!("VarManager {id}: Stopped distributing data due to end of input stream");
                    // Remove references to subscribers to let rx's know that streams have ended
                    *subscribers_ref.borrow_mut() = vec![];
                    return false;
                }
            }

            true
        })
    }

    /// Continuously distribute data to all subscribers until the input stream
    /// is exhausted
    pub fn run(self) -> LocalBoxFuture<'static, ()> {
        // Move to the closed stage since the variable is now running
        debug!("VarManager {}: Moving to closed stage from run", self.id);
        self.var_stage.set(VarStage::Closed);

        let cancellation_token = self.cancellation_token.clone();
        let id = self.id;

        // Return a future which will run the tick function until it returns
        // false (indicating the input stream is exhausted or there are no
        // subscribers) or cancellation is requested
        Box::pin(async move {
            debug!("VarManager {id}: Starting run loop with cancellation token");
            let mut should_continue = true;
            while should_continue {
                should_continue = select! {
                    _ = cancellation_token.cancelled().fuse() => {
                        debug!("VarManager {id}: Cancellation requested, stopping run loop");
                        false  // Exit the loop
                    }
                    tick_result = self.tick().fuse() => {
                        if tick_result {
                            true  // Continue the loop
                        } else {
                            debug!("VarManager {id}: Tick returned false, stopping run loop");
                            false  // Exit the loop
                        }
                    }
                };
            }
            debug!(
                "VarManager {id}: Run loop ended, cancellation: {}",
                cancellation_token.is_cancelled().await
            );
        })
    }

    /// Get the var name
    pub fn var_name(&self) -> VarName {
        self.var.clone()
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

#[derive(Debug)]
pub struct ContextId {
    id: usize,
    parent_id: Option<usize>,
}

impl Display for ContextId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.parent_id {
            Some(id) => write!(f, "(id: {}, parent: {})", self.id, id),
            None => write!(f, "(id: {}, parent: None)", self.id),
        }
    }
}

/// A context which consumes data for a set of variables and makes
/// it available when evaluating a deferred expression
//
/// This is implemented in the background using a combination of
/// manage_var and store history actors
pub struct Context<AC: AsyncConfig> {
    /// The executor which is used to run background tasks
    pub executor: Rc<LocalExecutor<'static>>,
    /// The variables which are available in the context
    var_names: Vec<VarName>,
    /// The amount of history stored for retrospective monitoring
    /// of each variable (0 means no history)
    #[allow(dead_code)]
    history_length: usize,
    /// Current clock
    clock: usize,
    /// Variable manangers
    var_managers: Rc<RefCell<BTreeMap<VarName, VarManager<AC::Val>>>>,
    /// Cancellation token to stop all var managers when output streams are dropped
    cancellation_token: CancellationToken,
    /// Identifier - used for log messages
    id: ContextId,
    /// Nested context --- Lets us construct a constext which wraps another context. Currently only
    /// used to recursively start parent in distributed monitoring, and not used for subcontexts
    /// (which start and stop independently from the parent)
    nested: Option<Box<Context<AC>>>,
    /// The builder used to construct us
    builder: ContextBuilder<AC>,
}

/// Concrete builder for Context<Val>
// Builders come with a bit of boilerplate, but the abstract builder
// allow us to change the type of context with is being constructed
// without having to duplicate the code of the constructor,
// giving us a lot of flexibility in extending the async runtime
// and should make tests a little easier
// (e.g. for the distributed runtime)
// We could look into a crate that derives this in the future.
pub struct ContextBuilder<AC: AsyncConfig> {
    executor: Option<Rc<LocalExecutor<'static>>>,
    var_names: Option<Vec<VarName>>,
    input_streams: Option<Vec<OutputStream<AC::Val>>>,
    history_length: Option<usize>,
    nested: Option<Box<Context<AC>>>,
    id: Option<ContextId>,
}

impl<AC> AbstractContextBuilder for ContextBuilder<AC>
where
    AC: AsyncConfig<Ctx = Context<AC>>,
{
    type AC = AC;

    fn new() -> Self {
        ContextBuilder {
            executor: None,
            var_names: None,
            input_streams: None,
            history_length: None,
            nested: None,
            id: None,
        }
    }

    fn executor(mut self, executor: Rc<LocalExecutor<'static>>) -> Self {
        self.executor = Some(executor);
        self
    }

    fn var_names(mut self, var_names: Vec<VarName>) -> Self {
        self.var_names = Some(var_names);
        self
    }

    fn history_length(mut self, history_length: usize) -> Self {
        self.history_length = Some(history_length);
        self
    }

    fn input_streams(mut self, streams: Vec<OutputStream<AC::Val>>) -> Self {
        self.input_streams = Some(streams);
        self
    }

    fn partial_clone(&self) -> Self {
        let mut res = ContextBuilder::new();

        if let Some(ex) = &self.executor {
            res = res.executor(ex.clone())
        };

        if let Some(var_names) = &self.var_names {
            res = res.var_names(var_names.clone())
        }

        if let Some(history_length) = self.history_length {
            res = res.history_length(history_length)
        }

        res
    }

    fn build(self) -> AC::Ctx {
        let builder = self.partial_clone();
        let executor = self.executor.expect("Executor not supplied");
        let var_names = self.var_names.expect("Var names not supplied");
        let input_streams = self.input_streams.expect("Input streams not supplied");
        let history_length = self.history_length.unwrap_or(0);
        let nested = self.nested;
        assert_eq!(var_names.len(), input_streams.len());

        let clock: usize = 0;
        // TODO: push the mutability to the API of contexts
        let var_managers = Rc::new(RefCell::new(BTreeMap::new()));
        let cancellation_token = CancellationToken::new();

        for (var, input_stream) in var_names.iter().zip(input_streams.into_iter()) {
            let input_stream =
                store_history(executor.clone(), var.clone(), history_length, input_stream);
            var_managers.borrow_mut().insert(
                var.clone(),
                VarManager::new(
                    executor.clone(),
                    var.clone(),
                    input_stream,
                    cancellation_token.clone(),
                ),
            );
        }

        let id = self.id.unwrap_or_else(|| {
            let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ContextId {
                id,
                parent_id: None,
            }
        });

        let manager_ids: Vec<_> = var_managers
            .borrow_mut()
            .iter()
            .map(|(var, manager)| (var.to_string(), manager.id))
            .collect();

        debug!(
            "ContextBuilder: Building Context with id {id}, and var_managers with ids: {:?}",
            manager_ids
        );

        Context {
            executor,
            var_names,
            history_length,
            clock,
            var_managers,
            cancellation_token,
            nested,
            builder,
            id,
        }
    }
}

impl<AC: AsyncConfig> ContextBuilder<AC> {
    pub fn id(mut self, id: usize, parent_id: Option<usize>) -> Self {
        let id = ContextId { id, parent_id };
        self.id = Some(id);
        self
    }

    pub fn nested(mut self, nested: Context<AC>) -> Self {
        self.nested = Some(Box::new(nested));
        self
    }

    pub fn maybe_nested(mut self, nested: Option<Context<AC>>) -> Self {
        self.nested = nested.map(Box::new);
        self
    }
}

impl<AC> Context<AC>
where
    AC: AsyncConfig<Ctx = Context<AC>>,
{
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        var_names: Vec<VarName>,
        input_streams: Vec<OutputStream<AC::Val>>,
        history_length: usize,
    ) -> Self {
        <Self as StreamContext>::Builder::new()
            .executor(executor)
            .var_names(var_names)
            .input_streams(input_streams)
            .history_length(history_length)
            .build()
    }

    pub fn var_names(&self) -> &Vec<VarName> {
        &self.var_names
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    pub fn cancel(&self) {
        debug!("Context[id={}]: Cancelling all var managers", self.id);
        self.cancellation_token.cancel();
    }
}

#[async_trait(?Send)]
impl<AC> StreamContext for Context<AC>
where
    AC: AsyncConfig<Ctx = Context<AC>>,
{
    type AC = AC;
    type Builder = ContextBuilder<AC>;

    fn var(&self, var: &VarName) -> Option<OutputStream<AC::Val>> {
        debug!(
            "Context[id={}]: Requesting stream for variable '{}'",
            self.id, var
        );

        if self.is_clock_started() {
            debug!(
                "Context[id={}]: Clock already started, can't request stream for '{}'",
                self.id, var
            );
            panic!(
                "Context {}: Cannot request a stream after the clock has started",
                self.id
            );
        }

        debug!(
            "Context[id={}]: Looking for var manager for '{}'",
            self.id, var
        );
        let mut var_managers = self.var_managers.borrow_mut();
        let var_manager = match var_managers.get_mut(var) {
            Some(vm) => {
                debug!("Context[id={}]: Found var manager for '{}'", self.id, var);
                vm
            }
            None => {
                debug!(
                    "Context[id={}]: No var manager found for '{}'. Available: {:?}",
                    self.id,
                    var,
                    var_managers.keys().collect::<Vec<_>>()
                );
                return None;
            }
        };

        debug!(
            "Context[id={}]: Subscribing to stream for '{}'",
            self.id, var
        );
        let stream = var_manager.subscribe();
        debug!(
            "Context[id={}]: Successfully subscribed to stream for '{}'",
            self.id, var
        );

        Some(stream)
    }

    fn subcontext(&self, history_length: usize) -> Self {
        let input_streams: Vec<_> = self
            .var_names
            .iter()
            .map(|var| self.var(var).unwrap())
            .collect();

        let id_num = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let parent_id = self.id.id;

        // Recursively create a new context based on ourself
        self.builder
            .partial_clone()
            .input_streams(input_streams)
            .history_length(history_length)
            .id(id_num, Some(parent_id))
            .build()
    }

    fn restricted_subcontext(&self, vs: ecow::EcoVec<VarName>, history_length: usize) -> Self {
        let vs = vs.into_iter().collect::<Vec<_>>();
        let input_streams: Vec<_> = self
            .var_names
            .iter()
            .filter_map(|var| {
                if vs.contains(var) {
                    self.var(var)
                } else {
                    None
                }
            })
            .collect();

        // Recursively create a new context based on ourself
        self.builder
            .partial_clone()
            .var_names(vs)
            .input_streams(input_streams)
            .history_length(history_length)
            .build()
    }

    async fn tick(&mut self) {
        debug!(
            "Context[id={}]: Ticking context with {} var managers",
            self.id,
            self.var_managers.borrow().len()
        );

        let nested_tick = match self.nested.as_mut() {
            Some(nested) => nested.tick(),
            None => Box::pin(ready(())),
        };

        let var_names = self
            .var_managers
            .borrow()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        debug!(
            "Context[id={}]: Ticking variables: {:?}",
            self.id, var_names
        );

        join_all(
            once(nested_tick).chain(self.var_managers.borrow_mut().iter_mut().map(
                |(var_name, var_manager)| {
                    let var_name = var_name.clone();
                    Box::pin(async move {
                        debug!("Context: Ticking var_manager for '{}'", var_name);
                        var_manager.tick().await;
                        debug!("Context: Ticked var_manager for '{}'", var_name);
                    }) as Pin<Box<dyn Future<Output = ()>>>
                },
            )),
        )
        .await;

        self.clock += 1;
        debug!(
            "Context[id={}]: Tick complete, clock now {}",
            self.id, self.clock
        );
    }

    fn clock(&self) -> usize {
        self.clock
    }

    async fn run(&mut self) {
        if !self.is_clock_started() {
            debug!(
                "Run for Context[id={}] with {} var managers",
                self.id,
                self.var_managers.borrow().len()
            );

            if let Some(nested) = self.nested.as_mut() {
                debug!("Context[id={}]: Running nested context", self.id);
                nested.run().await;
                debug!("Context[id={}]: Nested context run complete", self.id);
            }

            let var_names = self
                .var_managers
                .borrow()
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            debug!(
                "Context[id={}]: Running var managers for: {:?}",
                self.id, var_names
            );

            let mut var_managers = self.var_managers.borrow_mut();
            for (var_name, mut var_manager) in mem::take(&mut *var_managers).into_iter() {
                // TODO: TW - I added this as a temporary fix because it was blocking my work.
                // The issue is that the Async RT does not support specifications where an input
                // variable is unused. The InputProvider expects the RT to consume the streams it
                // sends, but the Async RT only does so if the VarManager has at least one
                // subscriber. This causes the InputProvider to crash.
                // It is one of those bugs that have always been lurking but the recent changes for
                // the reconfiguration paper made it crash correctly instead of just hanging.
                // ...
                // PS. the bug is not visible with files. It has been seen with ROS2 and I believe
                // it should replicate with MQTT and Redis.

                // Spawn noop task for every var to avoid bug:
                let stream = var_manager.subscribe();
                let name = var_name.clone();
                self.executor
                    .spawn(async move {
                        debug!(
                            "Context[id={}]: Starting noop task for '{}'",
                            var_manager.id, name
                        );
                        stream.for_each(|_| async {}).await;
                        debug!(
                            "Context[id={}]: Noop task for '{}' completed",
                            var_manager.id, name
                        );
                    })
                    .detach();
                // Actually make it run:
                debug!(
                    "Context[id={}]: Spawning run for var_manager '{}'",
                    self.id, var_name
                );
                self.executor.spawn(var_manager.run()).detach();
            }

            self.clock = usize::MAX;
            debug!("Context[id={}]: Run complete, clock set to MAX", self.id);
        } else {
            debug!(
                "Context[id={}]: Run called but clock already started",
                self.id
            );
        }
    }

    fn is_clock_started(&self) -> bool {
        self.clock() == usize::MAX
    }

    fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    fn cancel(&self) {
        debug!(
            "Context[id={}]: Cancelling all var managers via StreamContext trait",
            self.id
        );
        self.cancellation_token.cancel();
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
pub struct AsyncMonitorRunner<AC, S, M>
where
    AC: AsyncConfig,
    S: MonitoringSemantics<AC>,
    M: Specification<Expr = AC::Expr>,
{
    #[allow(dead_code)]
    pub executor: Rc<LocalExecutor<'static>>,
    model: M,
    input_provider: Box<dyn InputProvider<Val = AC::Val>>,
    output_handler: Box<dyn OutputHandler<Val = AC::Val>>,
    output_streams: Vec<OutputStream<AC::Val>>,
    cancellation_token: CancellationToken,
    #[allow(dead_code)]
    semantics_t: PhantomData<S>,
}

pub struct AsyncMonitorBuilder<
    M: Specification<Expr = AC::Expr>,
    AC: AsyncConfig,
    S: MonitoringSemantics<AC>,
> {
    pub(super) executor: Option<Rc<LocalExecutor<'static>>>,
    pub(crate) model: Option<M>,
    pub(super) input: Option<Box<dyn InputProvider<Val = AC::Val>>>,
    pub(super) output: Option<Box<dyn OutputHandler<Val = AC::Val>>>,
    pub(super) context_builder: Option<<<AC as AsyncConfig>::Ctx as StreamContext>::Builder>,
    semantics_t: PhantomData<S>,
}

impl<M: Specification<Expr = AC::Expr>, AC: AsyncConfig, S: MonitoringSemantics<AC>>
    AsyncMonitorBuilder<M, AC, S>
{
    pub fn partial_clone(&self) -> Self {
        Self {
            executor: self.executor.clone(),
            model: self.model.clone(),
            input: None,
            output: None,
            context_builder: self
                .context_builder
                .as_ref()
                .map(|builder| builder.partial_clone()),
            semantics_t: PhantomData,
        }
    }
}

pub trait AbstractAsyncMonitorBuilder<M, AC: AsyncConfig>:
    AbstractMonitorBuilder<M, AC::Val>
{
    fn context_builder(self, context_builder: <AC::Ctx as StreamContext>::Builder) -> Self;
}

impl<M: Specification<Expr = AC::Expr>, S: MonitoringSemantics<AC>, AC: AsyncConfig>
    AbstractAsyncMonitorBuilder<M, AC> for AsyncMonitorBuilder<M, AC, S>
{
    fn context_builder(self, context_builder: <AC::Ctx as StreamContext>::Builder) -> Self {
        Self {
            context_builder: Some(context_builder),
            ..self
        }
    }
}

impl<M: Specification<Expr = AC::Expr>, S: MonitoringSemantics<AC>, AC: AsyncConfig>
    AbstractMonitorBuilder<M, AC::Val> for AsyncMonitorBuilder<M, AC, S>
{
    type Mon = AsyncMonitorRunner<AC, S, M>;

    fn new() -> Self {
        AsyncMonitorBuilder {
            executor: None,
            model: None,
            input: None,
            output: None,
            context_builder: None,
            semantics_t: PhantomData,
        }
    }

    fn executor(self, executor: Rc<LocalExecutor<'static>>) -> Self {
        Self {
            executor: Some(executor),
            ..self
        }
    }

    fn model(self, model: M) -> Self {
        Self {
            model: Some(model),
            ..self
        }
    }

    fn input(self, input: Box<dyn InputProvider<Val = AC::Val>>) -> Self {
        Self {
            input: Some(input),
            ..self
        }
    }

    fn output(self, output: Box<dyn OutputHandler<Val = AC::Val>>) -> Self {
        Self {
            output: Some(output),
            ..self
        }
    }

    fn build(self) -> Self::Mon {
        debug!("AsyncMonitorBuilder: Starting build process");
        let executor = self.executor.expect("Executor not supplied");
        let model = self.model.expect("Model not supplied");

        debug!(
            "AsyncMonitorBuilder: Model variables: input={:?}, output={:?}",
            model.input_vars(),
            model.output_vars()
        );

        let mut input_provider = self.input.expect("Input streams not supplied");

        let output_handler = self.output.expect("Output handler not supplied");
        debug!(
            "AsyncMonitorBuilder: Output handler variables: {:?}",
            output_handler.var_names()
        );

        let context_builder = self
            .context_builder
            .unwrap_or_else(<AC::Ctx as StreamContext>::Builder::new);
        debug!("AsyncMonitorBuilder: Context builder initialized");

        let input_vars = model.input_vars().clone();
        let output_vars = model.output_vars().clone();
        let var_names: Vec<VarName> = input_vars
            .iter()
            .chain(output_vars.iter())
            .cloned()
            .collect();

        let input_streams = input_vars.iter().map(|var| {
            input_provider
                .var_stream(var)
                .expect(format!("Input stream not found for {}", var).as_str())
        });

        // Create deferred streams based on each of the output variables
        let output_oneshots: Vec<_> = output_vars
            .iter()
            .cloned()
            .map(|_| oneshot::channel::<OutputStream<AC::Val>>().into_split())
            .collect();
        let (output_txs, output_rxs): (Vec<_>, Vec<_>) = output_oneshots.into_iter().unzip();
        let output_txs: BTreeMap<_, _> = output_vars.iter().cloned().zip(output_txs).collect();
        let output_streams = output_rxs.into_iter().map(oneshot_to_stream);

        // Combine the input and output streams into a single map
        let streams: Vec<OutputStream<AC::Val>> = input_streams.chain(output_streams).collect();

        let mut context = context_builder
            .executor(executor.clone())
            .var_names(var_names)
            .input_streams(streams)
            .build();

        // Get cancellation token from context and create drop guard
        let cancellation_token = context.cancellation_token();
        let drop_guard = Rc::new(cancellation_token.clone().drop_guard());
        debug!("AsyncMonitorBuilder: Created drop guard for cancellation token");

        // Create a map of the output variables to their streams
        // based on using the context
        let output_streams = model
            .output_vars()
            .iter()
            .map(|var| {
                let stream = context.var(var).unwrap_or_else(|| {
                    panic!("Failed to find expression for var {}", var.name().as_str())
                });
                // Wrap stream with drop guard so cancellation happens when the last output stream
                // is dropped
                let guarded_stream = drop_guard_stream(stream, drop_guard.clone());
                debug!(
                    "AsyncMonitorBuilder: Wrapped output stream for var {} with drop guard",
                    var
                );
                guarded_stream
            })
            .collect();

        // Send outputs computed based on the context to the
        // output handler
        for (var, tx) in output_txs {
            let expr = model.var_expr(&var).unwrap_or_else(|| {
                panic!("Failed to find expression for var {}", var.name().as_str())
            });
            let stream = S::to_async_stream(expr, &context);
            if tx.send(stream).is_err() {
                warn!(?var, "Failed to send stream for var to requester");
            }
        }

        executor
            .spawn(async move {
                context.run().await;
            })
            .detach();

        debug!("AsyncMonitorBuilder: Returning runner with cancellation token");
        let runner = AsyncMonitorRunner {
            executor,
            model,
            input_provider,
            output_handler,
            output_streams,
            cancellation_token,
            semantics_t: PhantomData,
        };
        debug!("AsyncMonitorBuilder: Build process complete, runner created");
        runner
    }

    fn async_build(self: Box<Self>) -> LocalBoxFuture<'static, Self::Mon> {
        Box::pin(async move { self.build() })
    }
}

impl<AC, S, M> AsyncMonitorRunner<AC, S, M>
where
    AC: AsyncConfig,
    S: MonitoringSemantics<AC>,
    M: Specification<Expr = AC::Expr>,
{
    pub fn new(
        executor: Rc<LocalExecutor<'static>>,
        model: M,
        input: Box<dyn InputProvider<Val = AC::Val>>,
        output: Box<dyn OutputHandler<Val = AC::Val>>,
    ) -> Self {
        AsyncMonitorBuilder::new()
            .executor(executor)
            .model(model)
            .input(input)
            .output(output)
            .build()
    }
}

#[async_trait(?Send)]
impl<AC, S, M> Monitor<M, AC::Val> for AsyncMonitorRunner<AC, S, M>
where
    AC: AsyncConfig,
    S: MonitoringSemantics<AC>,
    M: Specification<Expr = AC::Expr>,
{
    fn spec(&self) -> &M {
        &self.model
    }
}

#[async_trait(?Send)]
impl<AC, S, M> Runnable for AsyncMonitorRunner<AC, S, M>
where
    AC: AsyncConfig,
    S: MonitoringSemantics<AC>,
    M: Specification<Expr = AC::Expr>,
{
    #[instrument(name="Running async Monitor", level=Level::INFO, skip(self))]
    async fn run_boxed(mut self: Box<Self>) -> anyhow::Result<()> {
        debug!("AsyncMonitorRunner: Starting monitor execution");
        self.output_handler.provide_streams(self.output_streams);

        debug!("AsyncMonitorRunner: Creating futures for input and output handlers");
        let output_fut = self.output_handler.run().fuse();

        // Wrap input provider's run with cancellation support
        let cancellation_token = self.cancellation_token.clone();
        let mut input_provider_stream = self.input_provider.control_stream().await;
        let input_provider_future = Box::pin(async move {
            while let Some(res) = input_provider_stream.next().await {
                if res.is_err() {
                    error!(
                        "AsyncMonitorRunner: Input provider stream returned error: {:?}",
                        res
                    );
                    return res;
                } else {
                    debug!("AsyncMonitorRunner: Received Ok message from input provider");
                }
            }
            debug!("AsyncMonitorRunner: Input provider control_stream ended");
            Ok(())
        });

        let input_fut = Box::pin(async move {
            futures::select! {
                result = input_provider_future.fuse() => result,
                _ = cancellation_token.cancelled().fuse() => {
                    debug!("AsyncMonitorRunner: Input provider cancelled");
                    Ok(())
                }
            }
        })
        .fuse();

        let (output_res, input_res) = join!(output_fut, input_fut);
        let result = match (output_res, input_res) {
            (Ok(_), Ok(_)) => Ok(()),
            (Err(e1), Ok(_)) => Err(anyhow!("OutputHandler failed with error: {}", e1)),
            (Ok(_), Err(e2)) => Err(anyhow!("InputProvider failed with error: {}", e2)),
            (Err(e1), Err(e2)) => Err(anyhow!(
                "Both OutputHandler and InputProvider failed: output={:?}, input={:?}",
                e1,
                e2
            )),
        };

        self.cancellation_token.cancel();
        debug!(?result, "AsyncMonitorRunner: Monitor execution completed");
        result
    }
}

#[cfg(test)]
mod tests {
    use crate::{Value, lola_fixtures::TestConfig};

    use super::*;

    use crate::async_test;
    use futures::stream;
    use macro_rules_attribute::apply;

    #[apply(async_test)]
    async fn test_manage_var_gathering(executor: Rc<LocalExecutor<'static>>) {
        let input_stream = Box::pin(stream! {
            yield 1;
            yield 2;
            yield 3;
        });

        let cancellation_token = CancellationToken::new();
        let mut manager = VarManager::new(
            executor.clone(),
            "test".into(),
            input_stream,
            cancellation_token,
        );

        info!("subscribing 1");
        let sub1 = manager.subscribe();
        info!("subscribing 2");
        let sub2 = manager.subscribe();

        info!("running manager");
        executor.spawn(manager.run()).detach();

        let output1 = sub1.collect::<Vec<_>>().await;
        let output2 = sub2.collect::<Vec<_>>().await;

        assert_eq!(output1, vec![1, 2, 3]);
        assert_eq!(output2, vec![1, 2, 3]);
    }

    #[apply(async_test)]
    async fn test_manage_tick_then_run(executor: Rc<LocalExecutor<'static>>) {
        let input_stream = Box::pin(stream! {
            yield 1;
            yield 2;
            yield 3;
            yield 4;
        });

        let cancellation_token = CancellationToken::new();
        let mut manager = VarManager::new(
            executor.clone(),
            "test".into(),
            input_stream,
            cancellation_token,
        );

        info!("ticking 1");
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

    #[apply(async_test)]
    async fn test_subctx_regression_727dc01(executor: Rc<LocalExecutor<'static>>) {
        fn mock_indirection<AC>(ctx: &AC::Ctx, x: VarName) -> OutputStream<AC::Val>
        where
            AC: AsyncConfig<Val = Value>,
        {
            let mut subcontext = ctx.subcontext(10);
            Box::pin(stream! {
                let mut var_stream = subcontext.var(&x).unwrap();
                subcontext.tick().await;
                while let Some(current) = var_stream.next().await {
                    yield current;
                    subcontext.tick().await;
                }
            })
        }

        let x = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let mut ctx = Context::<TestConfig>::new(executor.clone(), vec!["x".into()], vec![x], 10);
        let var_stream = mock_indirection::<TestConfig>(&ctx, "x".into());
        ctx.run().await;
        let exp: Vec<Value> = vec![1.into(), 2.into(), 3.into()];
        // If this hangs then we have regressed - previously it meant that subcontexts cannot
        // figure out when streams end
        let res: Vec<_> = var_stream.collect().await;
        assert_eq!(exp, res);
    }
}
