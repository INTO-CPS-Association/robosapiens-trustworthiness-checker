//! A local, sink-based ROS output backend.
//!
//! ROS publishers are synchronous at the `r2r` API boundary, but ROS still
//! needs a node to be spun while publishers are alive.  The opened sink retains
//! one local ROS session so publisher bindings can be reconciled without
//! recreating the context or node.  The spinner belongs to the opened sink and
//! is joined by `poll_close`; it is never detached.

use std::{
    cell::RefCell,
    collections::BTreeMap,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

use futures::{Future, FutureExt, Sink, StreamExt};
use smol::LocalExecutor;
use uuid::Uuid;

use crate::{
    core::{
        OutputBatch, OutputBinding, OutputError, OutputInterface, OutputSink, OutputWriter,
        StreamData, Value, VarName,
    },
    io::ros::{
        ValuePublisher, create_value_publisher,
        ros_topic_stream_mapping::{RosMsgType, ros_output_route_mapping},
    },
    utils::cancellation_token::CancellationToken,
};

/// The type-erased publisher held by an opened ROS sink.
///
/// ROS message types are heterogeneous, so one publisher object is retained per
/// output route.  The value-specific conversion remains in the existing ROS
/// publisher helpers.
pub(crate) trait RosPublisher<V: StreamData>: 'static {
    fn publish(&self, value: &V) -> Result<(), OutputError>;
}

pub(crate) type InterfaceValidator = fn(&OutputInterface) -> Result<(), OutputError>;
pub(crate) type PublisherFactory<V> =
    fn(&mut r2r::Node, &OutputBinding) -> Result<Box<dyn RosPublisher<V>>, OutputError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PublisherReconciliationChange {
    Add,
    Remove,
    Rebind,
}

type PublisherReconciliationPlan = BTreeMap<VarName, PublisherReconciliationChange>;
type PublisherOwner<V> = Rc<dyn RosPublisher<V>>;

/// A publisher owner is reusable only when it still describes the same output
/// binding.  Comparing the route metadata rather than the parsed ROS type also
/// keeps the factory contract unchanged for the Value and MSTLO backends.
fn publisher_routes_are_compatible(old: &OutputBinding, candidate: &OutputBinding) -> bool {
    !old.role().is_auxiliary()
        && !candidate.role().is_auxiliary()
        && old.variable() == candidate.variable()
        && old.route() == candidate.route()
}

/// Compute the minimal publisher-owner changes needed for a candidate
/// interface.  Auxiliary routes are deliberately absent from the plan because
/// they never own a ROS publisher.
fn publisher_reconciliation_plan(
    active: &OutputInterface,
    candidate: &OutputInterface,
) -> PublisherReconciliationPlan {
    let mut plan = BTreeMap::new();

    for old_route in active.bindings() {
        if old_route.role().is_auxiliary() {
            continue;
        }

        match candidate.binding(old_route.variable()) {
            Some(candidate_route) if !candidate_route.role().is_auxiliary() => {
                if !publisher_routes_are_compatible(old_route, candidate_route) {
                    plan.insert(
                        old_route.variable().clone(),
                        PublisherReconciliationChange::Rebind,
                    );
                }
            }
            _ => {
                plan.insert(
                    old_route.variable().clone(),
                    PublisherReconciliationChange::Remove,
                );
            }
        }
    }

    for candidate_route in candidate.bindings() {
        if candidate_route.role().is_auxiliary() {
            continue;
        }

        match active.binding(candidate_route.variable()) {
            Some(old_route) if !old_route.role().is_auxiliary() => {}
            _ => {
                plan.insert(
                    candidate_route.variable().clone(),
                    PublisherReconciliationChange::Add,
                );
            }
        }
    }

    plan
}

struct RosSessionState<V: StreamData> {
    interface: OutputInterface,
    publishers: BTreeMap<VarName, PublisherOwner<V>>,
}

/// The retained local ROS owner.  The node lives in the session rather than in
/// the spinner, so publisher creation and spinning use the same node across
/// every interface reconfiguration.
struct RosSession<V: StreamData> {
    node: RefCell<r2r::Node>,
    state: RosSessionState<V>,
}

fn create_publishers<V: StreamData>(
    node: &mut r2r::Node,
    interface: &OutputInterface,
    publisher_factory: PublisherFactory<V>,
) -> Result<BTreeMap<VarName, PublisherOwner<V>>, OutputError> {
    let mut publishers = BTreeMap::new();
    for route in interface
        .bindings()
        .iter()
        .filter(|route| !route.role().is_auxiliary())
    {
        let publisher = (publisher_factory)(node, route)?;
        let publisher: PublisherOwner<V> = Rc::from(publisher);
        publishers.insert(route.variable().clone(), publisher);
    }
    Ok(publishers)
}

/// Assemble the candidate owner map without touching the active map.  This is
/// intentionally node-independent: all node work has already succeeded before
/// this function is called, and compatible owners are cloned as `Rc`s instead
/// of being recreated.
fn reconcile_publisher_owners<V: StreamData>(
    active: &OutputInterface,
    candidate: &OutputInterface,
    active_publishers: &BTreeMap<VarName, PublisherOwner<V>>,
    created_publishers: &BTreeMap<VarName, PublisherOwner<V>>,
) -> Result<BTreeMap<VarName, PublisherOwner<V>>, OutputError> {
    let mut candidate_publishers = BTreeMap::new();

    for route in candidate
        .bindings()
        .iter()
        .filter(|route| !route.role().is_auxiliary())
    {
        let publisher = match active.binding(route.variable()) {
            Some(active_route) if publisher_routes_are_compatible(active_route, route) => {
                active_publishers.get(route.variable()).cloned()
            }
            _ => created_publishers.get(route.variable()).cloned(),
        }
        .ok_or_else(|| {
            OutputError::backend(format!(
                "ROS publisher reconciliation did not produce an owner for `{}`",
                route.variable()
            ))
        })?;
        candidate_publishers.insert(route.variable().clone(), publisher);
    }

    Ok(candidate_publishers)
}

fn validate_candidate_interface(
    interface: &OutputInterface,
    validate_interface: InterfaceValidator,
) -> Result<(), OutputError> {
    // OutputInterface is validated by its constructor, but retain this check at
    // the backend boundary because route metadata is ROS-specific and is not
    // part of core validation.
    OutputInterface::validate_bindings(interface.bindings())?;
    validate_interface(interface)
}

fn reconfigure_session<V: StreamData>(
    session: &Rc<RefCell<RosSession<V>>>,
    candidate: OutputInterface,
    validate_interface: InterfaceValidator,
    publisher_factory: PublisherFactory<V>,
) -> Result<(), OutputError> {
    // Validate everything before borrowing or mutating the live session.  A
    // failed candidate therefore cannot replace the active interface or cause
    // a partial publisher-map update.
    validate_candidate_interface(&candidate, validate_interface)?;

    let mut session = session.borrow_mut();
    let plan = publisher_reconciliation_plan(&session.state.interface, &candidate);
    if plan.is_empty() && session.state.interface == candidate {
        return Ok(());
    }

    // Resolve all routes before touching the node.  These lookups are an
    // internal invariant of the plan, but returning an error keeps a malformed
    // plan transactional rather than panicking.
    let routes_to_create = plan
        .iter()
        .filter_map(|(variable, change)| {
            matches!(
                change,
                PublisherReconciliationChange::Add | PublisherReconciliationChange::Rebind
            )
            .then_some(variable)
        })
        .map(|variable| {
            candidate.binding(variable).ok_or_else(|| {
                OutputError::backend(format!(
                    "ROS publisher reconciliation has no candidate route for `{variable}`"
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    // New/rebound publishers are created while the active session state is
    // still untouched.  If a factory call fails, dropping this temporary map
    // leaves the active interface and all compatible owners in place.
    let mut created_publishers = BTreeMap::new();
    {
        let mut node = session.node.borrow_mut();
        for route in routes_to_create {
            let publisher = (publisher_factory)(&mut node, route)?;
            let publisher: PublisherOwner<V> = Rc::from(publisher);
            created_publishers.insert(route.variable().clone(), publisher);
        }
    }

    let candidate_publishers = reconcile_publisher_owners(
        &session.state.interface,
        &candidate,
        &session.state.publishers,
        &created_publishers,
    )?;

    // This single state assignment is the commit point.  The handle only
    // reports success after both the candidate interface and its complete
    // publisher map are installed together.
    session.state = RosSessionState {
        interface: candidate,
        publishers: candidate_publishers,
    };
    Ok(())
}

pub(crate) async fn open<V: StreamData>(
    executor: Rc<LocalExecutor<'static>>,
    node_name: String,
    interface: OutputInterface,
    validate_interface: InterfaceValidator,
    publisher_factory: PublisherFactory<V>,
) -> Result<OutputWriter<V>, OutputError> {
    validate_candidate_interface(&interface, validate_interface)?;

    let context = r2r::Context::create().map_err(|error| {
        OutputError::backend(format!("failed to create ROS context: {error:?}"))
    })?;
    let node_name = format!("{}_{}", node_name, Uuid::new_v4().simple());
    let mut node = r2r::Node::create(context, &node_name, "").map_err(|error| {
        OutputError::backend(format!(
            "failed to create ROS node `{node_name}`: {error:?}"
        ))
    })?;
    let publishers = create_publishers(&mut node, &interface, publisher_factory)?;

    let session = Rc::new(RefCell::new(RosSession {
        node: RefCell::new(node),
        state: RosSessionState {
            interface,
            publishers,
        },
    }));

    let cancellation = CancellationToken::new();
    let cancellation_for_spinner = cancellation.clone();
    let session_for_spinner = Rc::clone(&session);
    let spinner = executor.spawn(async move {
        let mut spin_ticks = smol::Timer::interval(crate::io::ros::ROS_SPIN_INTERVAL);
        let mut cancelled = cancellation_for_spinner.cancelled().fuse();
        loop {
            futures::select_biased! {
                _ = cancelled => break,
                _ = spin_ticks.next().fuse() => {
                    // `spin_once` is synchronous.  The node borrow ends
                    // before the spinner awaits its next tick, leaving the
                    // node available for a reconfiguration factory call.
                    let session = session_for_spinner.borrow();
                    session
                        .node
                        .borrow_mut()
                        .spin_once(crate::io::ros::ROS_SPIN_TIMEOUT);
                },
            }
        }
    });

    let mut sink = RosSink::new(session, cancellation, Some(spinner));
    sink.rebinding = Some((validate_interface, publisher_factory));
    Ok(OutputWriter::from_output_sink(sink))
}

/// Validate the common ROS metadata carried by output routes and apply a
/// value-specific message-type check.
pub(crate) fn validate_ros_interface(
    interface: &OutputInterface,
    validate_message_type: fn(&RosMsgType) -> Result<(), OutputError>,
) -> Result<(), OutputError> {
    for route in interface.bindings() {
        if route.role().is_auxiliary() {
            // Auxiliary values are consumed but never need a ROS publisher.
            continue;
        }

        let (_, message_type) = ros_output_route_mapping(route)?;
        validate_message_type(&message_type).map_err(|error| {
            OutputError::invalid(format!("ROS output route `{}`: {error}", route.variable()))
        })?;
    }
    Ok(())
}

/// Validate the dynamic `Value` route set.
pub(crate) fn validate_value_interface(interface: &OutputInterface) -> Result<(), OutputError> {
    validate_ros_interface(interface, |message_type| {
        if matches!(message_type, RosMsgType::MstloTimedValue) {
            return Err(OutputError::invalid(
                "MstloTimedValue requires a typed MSTLO ROS output backend",
            ));
        }
        Ok(())
    })
}

struct DynamicValuePublisher {
    topic: String,
    publisher: Box<dyn ValuePublisher>,
}

impl RosPublisher<Value> for DynamicValuePublisher {
    fn publish(&self, value: &Value) -> Result<(), OutputError> {
        self.publisher.publish_value(value).map_err(|error| {
            OutputError::backend(format!(
                "failed to publish dynamic ROS value on `{}`: {error}",
                self.topic
            ))
        })
    }
}

/// Create a type-erased publisher for a dynamic [`Value`] route using the
/// existing scalar conversions and JSON/string fallback.
pub(crate) fn create_value_ros_publisher(
    node: &mut r2r::Node,
    route: &OutputBinding,
) -> Result<Box<dyn RosPublisher<Value>>, OutputError> {
    let (topic, message_type) = ros_output_route_mapping(route)?;
    let publisher = create_value_publisher(node, topic, &message_type).map_err(|error| {
        OutputError::backend(format!(
            "failed to create ROS publisher for `{}` on `{topic}`: {error}",
            route.variable()
        ))
    })?;
    Ok(Box::new(DynamicValuePublisher {
        topic: topic.to_owned(),
        publisher,
    }))
}

struct RosSink<V: StreamData> {
    session: Rc<RefCell<RosSession<V>>>,
    cancellation: CancellationToken,
    spinner: Option<smol::Task<()>>,
    ready: bool,
    close_started: bool,
    closed: bool,
    failure: Option<OutputError>,
    rebinding: Option<(InterfaceValidator, PublisherFactory<V>)>,
}

impl<V: StreamData> RosSink<V> {
    fn new(
        session: Rc<RefCell<RosSession<V>>>,
        cancellation: CancellationToken,
        spinner: Option<smol::Task<()>>,
    ) -> Self {
        Self {
            session,
            cancellation,
            spinner,
            ready: false,
            close_started: false,
            closed: false,
            failure: None,
            rebinding: None,
        }
    }

    fn state_error(&self) -> Option<OutputError> {
        self.failure
            .clone()
            .or_else(|| self.closed.then_some(OutputError::closed()))
    }

    fn fail(&mut self, error: OutputError) -> OutputError {
        self.cancellation.cancel();
        self.failure = Some(error.clone());
        error
    }

    fn publish_batch(&self, batch: &OutputBatch<V>) -> Result<(), OutputError> {
        // Publishing is synchronous at the ROS API boundary.  Keep this borrow
        // entirely within this method; no RefCell borrow reaches an await in
        // the sink or in the reconfiguration handle.
        let session = self.session.borrow();
        session.state.interface.validate_batch(batch)?;

        for tick in batch.ticks() {
            for update in tick.updates() {
                let route = session
                    .state
                    .interface
                    .binding(update.variable)
                    .ok_or_else(|| {
                        OutputError::invalid(format!(
                            "output update variable `{}` has no ROS route",
                            update.variable
                        ))
                    })?;
                if route.role().is_auxiliary() || update.value.is_no_val() {
                    continue;
                }

                let publisher = session
                    .state
                    .publishers
                    .get(update.variable)
                    .ok_or_else(|| {
                        OutputError::backend(format!(
                            "ROS output publisher is missing for `{}`",
                            update.variable
                        ))
                    })?;
                publisher.publish(update.value)?;
            }
        }
        Ok(())
    }
}

impl<V: StreamData> OutputSink<V> for RosSink<V> {
    fn poll_rebind(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        interface: &OutputInterface,
    ) -> Poll<Result<(), OutputError>> {
        let this = self.get_mut();
        if let Some(error) = this.state_error() {
            return Poll::Ready(Err(error));
        }
        let Some((validate, create)) = this.rebinding else {
            return Poll::Ready(Err(OutputError::invalid(
                "ROS owner cannot change bindings",
            )));
        };
        Poll::Ready(reconfigure_session(
            &this.session,
            interface.clone(),
            validate,
            create,
        ))
    }
}

impl<V: StreamData> Sink<OutputBatch<V>> for RosSink<V> {
    type Error = OutputError;

    fn poll_ready(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if let Some(error) = this.state_error() {
            return Poll::Ready(Err(error));
        }
        this.ready = true;
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, batch: OutputBatch<V>) -> Result<(), Self::Error> {
        let this = self.get_mut();
        if let Some(error) = this.state_error() {
            return Err(error);
        }
        if !this.ready {
            return Err(this.fail(OutputError::backend(
                "ROS output sink was not ready for start_send",
            )));
        }
        this.ready = false;

        if let Err(error) = this.publish_batch(&batch) {
            return Err(this.fail(error));
        }
        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if let Some(error) = this.state_error() {
            return Poll::Ready(Err(error));
        }
        this.ready = true;
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if this.closed {
            return Poll::Ready(Err(OutputError::closed()));
        }

        if !this.close_started {
            this.close_started = true;
            this.cancellation.cancel();
        }

        let spinner_finished = match this.spinner.as_mut() {
            Some(spinner) => match Pin::new(spinner).poll(context) {
                Poll::Pending => false,
                Poll::Ready(()) => true,
            },
            None => true,
        };
        if !spinner_finished {
            return Poll::Pending;
        }

        this.spinner = None;
        this.closed = true;
        match this.failure.clone() {
            Some(error) => Poll::Ready(Err(error)),
            None => Poll::Ready(Ok(())),
        }
    }
}

impl<V: StreamData> Drop for RosSink<V> {
    fn drop(&mut self) {
        // Dropping a task cancels it, but notify the spinner first so a task
        // currently waiting on the cancellation future can observe termination.
        self.cancellation.cancel();
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;
    use crate::core::{OutputBinding, OutputRole};

    fn var(name: &str) -> VarName {
        VarName::new(name)
    }

    fn route(name: &str, topic: &str, message_type: &str, role: OutputRole) -> OutputBinding {
        OutputBinding::new(
            var(name),
            Some(
                crate::core::Route::new(topic, Some(crate::core::FormatId::new(message_type)))
                    .unwrap(),
            ),
            role,
        )
    }

    fn interface(routes: impl IntoIterator<Item = OutputBinding>) -> OutputInterface {
        OutputInterface::from_bindings(routes).expect("test routes should be valid")
    }

    #[test]
    fn reconciliation_plan_is_empty_for_unchanged_bindings() {
        let active = interface([
            route("x", "/x", "Int32", OutputRole::Output),
            route("aux", "/aux", "Int32", OutputRole::Auxiliary),
        ]);
        // Deliberately change route order: publisher ownership is keyed by
        // variable, not by the position of a route in the interface.
        let candidate = interface([
            route("aux", "/different", "String", OutputRole::Auxiliary),
            route("x", "/x", "Int32", OutputRole::Output),
        ]);

        assert_eq!(
            publisher_reconciliation_plan(&active, &candidate),
            BTreeMap::new()
        );
    }

    #[test]
    fn reconciliation_plan_is_exact_for_add_and_remove() {
        let active = interface([
            route("kept", "/kept", "Int32", OutputRole::Output),
            route("removed", "/removed", "Int32", OutputRole::Output),
        ]);
        let candidate = interface([
            route("kept", "/kept", "Int32", OutputRole::Output),
            route("added", "/added", "Int32", OutputRole::Output),
        ]);

        assert_eq!(
            publisher_reconciliation_plan(&active, &candidate),
            BTreeMap::from([
                (var("added"), PublisherReconciliationChange::Add,),
                (var("removed"), PublisherReconciliationChange::Remove,),
            ])
        );
    }

    #[test]
    fn reconciliation_plan_marks_topic_and_message_changes_as_rebinds() {
        let active = interface([
            route("topic_changed", "/old", "Int32", OutputRole::Output),
            route("type_changed", "/same", "Int32", OutputRole::Output),
        ]);
        let candidate = interface([
            route("topic_changed", "/new", "Int32", OutputRole::Output),
            route("type_changed", "/same", "Float64", OutputRole::Output),
        ]);

        assert_eq!(
            publisher_reconciliation_plan(&active, &candidate),
            BTreeMap::from([
                (var("topic_changed"), PublisherReconciliationChange::Rebind,),
                (var("type_changed"), PublisherReconciliationChange::Rebind,),
            ])
        );
    }

    #[test]
    fn auxiliary_routes_do_not_create_publisher_owners() {
        let active = interface([route("x", "/x", "Int32", OutputRole::Auxiliary)]);
        let candidate = interface([
            route("x", "/x", "Int32", OutputRole::Auxiliary),
            route("y", "/y", "Int32", OutputRole::Output),
        ]);
        let y_publishes = Rc::new(Cell::new(0));
        let created = BTreeMap::from([(var("y"), fake_owner(Rc::clone(&y_publishes)))]);

        assert_eq!(
            publisher_reconciliation_plan(&active, &candidate),
            BTreeMap::from([(var("y"), PublisherReconciliationChange::Add)])
        );
        let publishers =
            reconcile_publisher_owners::<Value>(&active, &candidate, &BTreeMap::new(), &created)
                .unwrap();
        assert_eq!(publishers.len(), 1);
        assert!(publishers.contains_key(&var("y")));
    }

    #[test]
    fn reconciliation_reuses_compatible_owner_and_rebinds_changed_owner() {
        let active = interface([
            route("kept", "/kept", "Int32", OutputRole::Output),
            route("rebound", "/old", "Int32", OutputRole::Output),
        ]);
        let candidate = interface([
            route("kept", "/kept", "Int32", OutputRole::Output),
            route("rebound", "/new", "Int32", OutputRole::Output),
        ]);
        let kept_publishes = Rc::new(Cell::new(0));
        let old_rebound_publishes = Rc::new(Cell::new(0));
        let new_rebound_publishes = Rc::new(Cell::new(0));
        let active_publishers = BTreeMap::from([
            (var("kept"), fake_owner(Rc::clone(&kept_publishes))),
            (
                var("rebound"),
                fake_owner(Rc::clone(&old_rebound_publishes)),
            ),
        ]);
        let created_publishers = BTreeMap::from([(
            var("rebound"),
            fake_owner(Rc::clone(&new_rebound_publishes)),
        )]);

        let publishers = reconcile_publisher_owners(
            &active,
            &candidate,
            &active_publishers,
            &created_publishers,
        )
        .unwrap();

        assert!(Rc::ptr_eq(
            publishers.get(&var("kept")).unwrap(),
            active_publishers.get(&var("kept")).unwrap()
        ));
        assert!(Rc::ptr_eq(
            publishers.get(&var("rebound")).unwrap(),
            created_publishers.get(&var("rebound")).unwrap()
        ));
        publishers
            .get(&var("kept"))
            .unwrap()
            .publish(&Value::Int(1))
            .unwrap();
        publishers
            .get(&var("rebound"))
            .unwrap()
            .publish(&Value::Int(2))
            .unwrap();
        assert_eq!(kept_publishes.get(), 1);
        assert_eq!(old_rebound_publishes.get(), 0);
        assert_eq!(new_rebound_publishes.get(), 1);
    }

    struct FakePublisher {
        publishes: Rc<Cell<usize>>,
    }

    impl RosPublisher<Value> for FakePublisher {
        fn publish(&self, _value: &Value) -> Result<(), OutputError> {
            self.publishes.set(self.publishes.get() + 1);
            Ok(())
        }
    }

    fn fake_owner(publishes: Rc<Cell<usize>>) -> PublisherOwner<Value> {
        Rc::new(FakePublisher { publishes })
    }
}
