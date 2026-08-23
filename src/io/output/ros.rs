//! A local, sink-based ROS output backend.
//!
//! ROS publishers are synchronous at the `r2r` API boundary, but ROS still
//! needs a node to be spun while publishers are alive.  The spinner belongs to
//! the opened sink and is joined by `poll_close`; it is never detached.

use std::{
    collections::BTreeMap,
    marker::PhantomData,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

use async_trait::async_trait;
use futures::{Future, FutureExt, Sink, StreamExt};
use smol::LocalExecutor;
use uuid::Uuid;

use crate::{
    core::{
        OutputBackend, OutputBatch, OutputError, OutputInterface, OutputRoute, OutputWriter,
        StreamData, Value, VarName,
    },
    io::ros::{
        ValuePublisher, create_value_publisher,
        ros_topic_stream_mapping::{RosMsgType, ros_output_route_mapping},
    },
    utils::cancellation_token::CancellationToken,
};

/// The type-erased publisher held by a [`RosOutputBackend`] sink.
///
/// ROS message types are heterogeneous, so one publisher object is retained per
/// output route.  The value-specific conversion remains in the existing ROS
/// publisher helpers.
pub(crate) trait RosPublisher<V: StreamData>: 'static {
    fn publish(&self, value: &V) -> Result<(), OutputError>;
}

pub(crate) type InterfaceValidator = fn(&OutputInterface) -> Result<(), OutputError>;
pub(crate) type PublisherFactory<V: StreamData> =
    fn(&mut r2r::Node, &OutputRoute) -> Result<Box<dyn RosPublisher<V>>, OutputError>;

/// A reusable local ROS output backend.
///
/// The backend stores only node configuration and type-specific factory
/// functions.  Every call to [`OutputBackend::open`] receives its own fixed
/// interface, publishers, node, and joined spinner task.
pub struct RosOutputBackend<V: StreamData> {
    executor: Rc<LocalExecutor<'static>>,
    node_name: String,
    validate_interface: InterfaceValidator,
    publisher_factory: PublisherFactory<V>,
    _value: PhantomData<fn() -> V>,
}

impl<V: StreamData> RosOutputBackend<V> {
    pub(crate) fn new(
        executor: Rc<LocalExecutor<'static>>,
        node_name: String,
        validate_interface: InterfaceValidator,
        publisher_factory: PublisherFactory<V>,
    ) -> Self {
        Self {
            executor,
            node_name,
            validate_interface,
            publisher_factory,
            _value: PhantomData,
        }
    }
}

#[async_trait(?Send)]
impl<V: StreamData> OutputBackend for RosOutputBackend<V> {
    type Val = V;

    async fn open(
        &self,
        interface: OutputInterface,
    ) -> Result<OutputWriter<Self::Val>, OutputError> {
        // OutputInterface is already validated by its constructor, but retain
        // this check at the backend boundary because route metadata is ROS
        // specific and is deliberately not part of core validation.
        OutputInterface::validate_routes(interface.routes())?;
        (self.validate_interface)(&interface)?;

        let has_outputs = interface
            .routes()
            .iter()
            .any(|route| !route.role.is_auxiliary());
        if !has_outputs {
            return Ok(OutputWriter::from_sink(RosSink::new(
                interface,
                BTreeMap::new(),
                CancellationToken::new(),
                None,
            )));
        }

        let context = r2r::Context::create().map_err(|error| {
            OutputError::backend(format!("failed to create ROS context: {error:?}"))
        })?;
        let node_name = format!("{}_{}", self.node_name, Uuid::new_v4().simple());
        let mut node = r2r::Node::create(context, &node_name, "").map_err(|error| {
            OutputError::backend(format!(
                "failed to create ROS node `{node_name}`: {error:?}"
            ))
        })?;

        let mut publishers = BTreeMap::new();
        for route in interface
            .routes()
            .iter()
            .filter(|route| !route.role.is_auxiliary())
        {
            let publisher = (self.publisher_factory)(&mut node, route)?;
            publishers.insert(route.variable.clone(), publisher);
        }

        let cancellation = CancellationToken::new();
        let cancellation_for_spinner = cancellation.clone();
        let spinner = self.executor.spawn(async move {
            let mut spin_ticks = smol::Timer::interval(crate::io::ros::ROS_SPIN_INTERVAL);
            let mut cancelled = cancellation_for_spinner.cancelled().fuse();
            loop {
                futures::select_biased! {
                    _ = cancelled => break,
                    _ = spin_ticks.next().fuse() => node.spin_once(crate::io::ros::ROS_SPIN_TIMEOUT),
                }
            }
        });

        Ok(OutputWriter::from_sink(RosSink::new(
            interface,
            publishers,
            cancellation,
            Some(spinner),
        )))
    }
}

/// Validate the common ROS metadata carried by output routes and apply a
/// value-specific message-type check.
pub(crate) fn validate_ros_interface(
    interface: &OutputInterface,
    validate_message_type: fn(&RosMsgType) -> Result<(), OutputError>,
) -> Result<(), OutputError> {
    for route in interface.routes() {
        if route.role.is_auxiliary() {
            // Auxiliary values are consumed but never need a ROS publisher.
            continue;
        }

        let (_, message_type) = ros_output_route_mapping(route)?;
        validate_message_type(&message_type).map_err(|error| {
            OutputError::invalid(format!("ROS output route `{}`: {error}", route.variable))
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
    route: &OutputRoute,
) -> Result<Box<dyn RosPublisher<Value>>, OutputError> {
    let (topic, message_type) = ros_output_route_mapping(route)?;
    let publisher = create_value_publisher(node, topic, &message_type).map_err(|error| {
        OutputError::backend(format!(
            "failed to create ROS publisher for `{}` on `{topic}`: {error}",
            route.variable
        ))
    })?;
    Ok(Box::new(DynamicValuePublisher {
        topic: topic.to_owned(),
        publisher,
    }))
}

struct RosSink<V: StreamData> {
    interface: OutputInterface,
    publishers: BTreeMap<VarName, Box<dyn RosPublisher<V>>>,
    cancellation: CancellationToken,
    spinner: Option<smol::Task<()>>,
    ready: bool,
    close_started: bool,
    closed: bool,
    failure: Option<OutputError>,
}

impl<V: StreamData> RosSink<V> {
    fn new(
        interface: OutputInterface,
        publishers: BTreeMap<VarName, Box<dyn RosPublisher<V>>>,
        cancellation: CancellationToken,
        spinner: Option<smol::Task<()>>,
    ) -> Self {
        Self {
            interface,
            publishers,
            cancellation,
            spinner,
            ready: false,
            close_started: false,
            closed: false,
            failure: None,
        }
    }

    fn state_error(&self) -> Option<OutputError> {
        self.failure
            .clone()
            .or_else(|| self.closed.then_some(OutputError::Closed))
    }

    fn fail(&mut self, error: OutputError) -> OutputError {
        self.cancellation.cancel();
        self.failure = Some(error.clone());
        error
    }

    fn publish_batch(&self, batch: &OutputBatch<V>) -> Result<(), OutputError> {
        self.interface.validate_batch(batch)?;

        for tick in batch.ticks() {
            for update in tick.updates() {
                let route = self.interface.route(update.variable).ok_or_else(|| {
                    OutputError::invalid(format!(
                        "output update variable `{}` has no ROS route",
                        update.variable
                    ))
                })?;
                if route.role.is_auxiliary() || update.value.is_no_val() {
                    continue;
                }

                let publisher = self.publishers.get(update.variable).ok_or_else(|| {
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
            return Poll::Ready(Err(OutputError::Closed));
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
