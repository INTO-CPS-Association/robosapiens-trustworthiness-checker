use std::{
    borrow::Borrow,
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    rc::Rc,
    task::Poll,
    time::Duration,
};

use futures::Sink;
use smol::LocalExecutor;

use crate::io::config::OutputConfiguration;
use crate::io::{PipelineGeneration, SessionId, SessionRevision, ShutdownDeadline};
use crate::{
    VarName,
    core::{
        FormatId, JsonStreamValue, OutputBatch, OutputBinding, OutputError, OutputInterface,
        OutputRole, OutputWriter, RosStreamValue, Route,
    },
};

use super::delivery::DeliveryCancellation;
use super::{Delivery, DeliveryPolicy, DestinationId, OutputBackendConfig};

const JSON_CODEC: &str = "json";

/// Which part of the model output set a destination consumes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutputDestinationSelection {
    /// A single destination may consume the complete model output set.
    All,
    /// A disjoint primary partition of the model output set.
    Partition(BTreeSet<VarName>),
    /// Mirror the listed model outputs in addition to their primary partition.
    Mirror(BTreeSet<VarName>),
    /// Mirror every model output. This is used by configuration files' `mirror`
    /// flag, where an omitted variable list means all outputs.
    MirrorAll,
}

impl OutputDestinationSelection {
    fn is_primary_capable(&self) -> bool {
        matches!(self, Self::All | Self::Partition(_))
    }
}

/// One configured destination and its local routing/stage policy.
#[derive(Clone, Debug)]
pub struct OutputDestination<V = crate::Value> {
    id: DestinationId,
    backend: OutputBackendConfig<V>,
    routes: Option<BTreeMap<VarName, Route>>,
    selection: OutputDestinationSelection,
    delivery: DeliveryPolicy,
}

impl<V> OutputDestination<V> {
    pub fn new(id: impl Into<DestinationId>, backend: OutputBackendConfig<V>) -> Self {
        Self {
            id: id.into(),
            backend,
            routes: None,
            selection: OutputDestinationSelection::All,
            delivery: DeliveryPolicy::direct(),
        }
    }

    pub fn id(&self) -> &DestinationId {
        &self.id
    }

    pub fn backend(&self) -> &OutputBackendConfig<V> {
        &self.backend
    }

    pub fn routes(&self) -> Option<&BTreeMap<VarName, Route>> {
        self.routes.as_ref()
    }

    pub fn selection(&self) -> &OutputDestinationSelection {
        &self.selection
    }

    pub const fn delivery(&self) -> DeliveryPolicy {
        self.delivery
    }

    pub fn with_routes<I, K, R>(mut self, routes: I) -> Self
    where
        I: IntoIterator<Item = (K, R)>,
        K: Borrow<VarName>,
        R: Borrow<Route>,
    {
        self.routes = Some(
            routes
                .into_iter()
                .map(|(variable, route)| (variable.borrow().clone(), route.borrow().clone()))
                .collect(),
        );
        self
    }

    pub fn with_route_catalog(mut self, routes: BTreeMap<VarName, Route>) -> Self {
        self.routes = Some(routes);
        self
    }

    pub fn partition<I, T>(mut self, variables: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Borrow<VarName>,
    {
        self.selection = OutputDestinationSelection::Partition(
            variables
                .into_iter()
                .map(|variable| variable.borrow().clone())
                .collect(),
        );
        self
    }

    pub fn mirror<I, T>(mut self, variables: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Borrow<VarName>,
    {
        self.selection = OutputDestinationSelection::Mirror(
            variables
                .into_iter()
                .map(|variable| variable.borrow().clone())
                .collect(),
        );
        self
    }

    pub fn mirror_all(mut self) -> Self {
        self.selection = OutputDestinationSelection::MirrorAll;
        self
    }

    pub(crate) fn set_mirror_all(&mut self) {
        self.selection = OutputDestinationSelection::MirrorAll;
    }

    pub fn all(mut self) -> Self {
        self.selection = OutputDestinationSelection::All;
        self
    }

    pub fn with_delivery(mut self, delivery: DeliveryPolicy) -> Self {
        self.delivery = delivery;
        self
    }
}

/// A deterministic, unopened registry of local output destinations.
#[derive(Clone, Debug, Default)]
pub struct OutputDestinations<V = crate::Value> {
    destinations: BTreeMap<DestinationId, OutputDestination<V>>,
    default: Option<DestinationId>,
}

impl<V> IntoIterator for OutputDestination<V> {
    type Item = Self;
    type IntoIter = std::iter::Once<Self>;

    fn into_iter(self) -> Self::IntoIter {
        std::iter::once(self)
    }
}

impl<V> TryFrom<OutputDestination<V>> for OutputDestinations<V> {
    type Error = anyhow::Error;

    fn try_from(destination: OutputDestination<V>) -> Result<Self, Self::Error> {
        Self::single(destination)
    }
}

impl<V> TryFrom<Vec<OutputDestination<V>>> for OutputDestinations<V> {
    type Error = anyhow::Error;

    fn try_from(destinations: Vec<OutputDestination<V>>) -> Result<Self, Self::Error> {
        Self::new(destinations)
    }
}

impl<V> OutputDestinations<V> {
    /// Construct a destination registry while preserving its ID invariant.
    ///
    /// This constructor is fallible because a `BTreeMap` would otherwise hide
    /// duplicate IDs by overwriting the earlier destination.
    pub fn new<I>(destinations: I) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = OutputDestination<V>>,
    {
        let mut result = Self {
            destinations: BTreeMap::new(),
            default: None,
        };
        for destination in destinations {
            anyhow::ensure!(
                !destination.id.is_empty(),
                "output destination ID cannot be empty"
            );
            let id = destination.id.clone();
            anyhow::ensure!(
                result.destinations.insert(id, destination).is_none(),
                "output destination ID is declared more than once"
            );
        }
        Ok(result)
    }

    pub fn single(destination: OutputDestination<V>) -> anyhow::Result<Self> {
        let id = destination.id.clone();
        anyhow::ensure!(!id.is_empty(), "output destination ID cannot be empty");
        Ok(Self {
            destinations: BTreeMap::from([(id.clone(), destination)]),
            default: Some(id),
        })
    }

    pub fn try_new<I>(destinations: I) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = OutputDestination<V>>,
    {
        Self::new(destinations)
    }

    pub fn destinations(&self) -> &BTreeMap<DestinationId, OutputDestination<V>> {
        &self.destinations
    }

    pub fn default(&self) -> Option<&DestinationId> {
        self.default.as_ref()
    }

    pub fn with_default(mut self, id: impl Into<DestinationId>) -> anyhow::Result<Self> {
        let id = id.into();
        anyhow::ensure!(
            self.destinations.contains_key(&id),
            "output default destination `{id}` is not registered"
        );
        self.default = Some(id);
        Ok(self)
    }

    fn validate_ids(&self) -> anyhow::Result<()> {
        for destination in self.destinations.values() {
            anyhow::ensure!(
                !destination.id.is_empty(),
                "output destination ID cannot be empty"
            );
        }
        if let Some(default) = &self.default {
            anyhow::ensure!(
                self.destinations.contains_key(default),
                "output default destination `{default}` is not registered"
            );
        }
        Ok(())
    }

    fn ordered(&self) -> Vec<&OutputDestination<V>> {
        self.destinations.values().collect()
    }

    fn get(&self, id: &DestinationId) -> Option<&OutputDestination<V>> {
        self.destinations.get(id)
    }
}

/// A branching pipeline whose resolution is pure and whose opening phase
/// exposes one [`OutputWriter`] to the runtime.
///
/// The example separates resource-free planning from opening and from the
/// writer's admission and completion boundaries:
///
/// ```
/// use trustworthiness_checker::{OutputBatch, Value, VarName};
/// use trustworthiness_checker::io::output::{
///     OutputBackendConfig, OutputDestination, OutputPipeline,
/// };
///
/// # fn main() -> anyhow::Result<()> {
/// smol::block_on(async {
///     let destination = OutputDestination::<Value>::new(
///         "local-null",
///         OutputBackendConfig::null(),
///     );
///     let pipeline = OutputPipeline::from_destination(destination)?;
///     let resolved = pipeline.resolve(
///         [VarName::new("alert"), VarName::new("total"), VarName::new("scaled")],
///         std::iter::empty::<VarName>(),
///         None,
///     )?;
///     let mut writer = pipeline.open(resolved).await?;
///
///     writer
///         .send(OutputBatch::update("total", Value::Int(8)))
///         .await?;
///     writer.flush().await?;
///     writer.close().await?;
///     Ok(())
/// })
/// # }
/// ```
///
/// [`OutputWriter::send`] admits a complete batch after readiness;
/// [`OutputWriter::flush`] is the downstream completion barrier. A transport
/// backend's completion contract does not necessarily imply remote persistence.
#[derive(Clone, Debug)]
pub struct OutputPipeline<V = crate::Value> {
    destinations: OutputDestinations<V>,
    executor: Option<Rc<LocalExecutor<'static>>>,
    shutdown_timeout: Option<Duration>,
    generation: PipelineGeneration,
}

impl<V> OutputPipeline<V> {
    pub fn new(destinations: OutputDestinations<V>) -> Self {
        Self {
            destinations,
            executor: None,
            shutdown_timeout: None,
            generation: PipelineGeneration::new(),
        }
    }

    /// Attach the local executor used by worker-backed output stages. The
    /// pipeline remains resource-free: this only records where an opened stage
    /// may run its owned task.
    pub fn with_executor(mut self, executor: Rc<LocalExecutor<'static>>) -> Self {
        self.executor = Some(executor);
        self
    }

    pub fn with_shutdown_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    pub(crate) fn shutdown_timeout(&self) -> Option<Duration> {
        self.shutdown_timeout
    }

    pub fn from_destination(destination: OutputDestination<V>) -> anyhow::Result<Self> {
        Ok(Self::new(OutputDestinations::single(destination)?))
    }

    pub fn destinations(&self) -> &OutputDestinations<V> {
        &self.destinations
    }

    pub fn with_destination_delivery(
        mut self,
        destination: impl Into<DestinationId>,
        delivery: DeliveryPolicy,
    ) -> anyhow::Result<Self> {
        let destination = destination.into();
        let configured = self
            .destinations
            .destinations
            .get_mut(&destination)
            .ok_or_else(|| {
                anyhow::anyhow!("output destination `{destination}` is not configured")
            })?;
        configured.delivery = delivery;
        self.generation = PipelineGeneration::new();
        Ok(self)
    }

    /// Resolve model output variables, auxiliary variables, local route
    /// catalogs, and an optional request-specific monitor override.
    ///
    /// This method performs no backend construction or I/O. Its result is
    /// deterministic: variables, bindings, and destinations are ordered by
    /// their stable identifiers.
    pub fn resolve<I, A>(
        &self,
        model_outputs: I,
        auxiliary: A,
        output_configuration: Option<&OutputConfiguration>,
    ) -> anyhow::Result<ResolvedOutput>
    where
        I: IntoIterator,
        I::Item: Borrow<VarName>,
        A: IntoIterator,
        A::Item: Borrow<VarName>,
    {
        self.destinations.validate_ids()?;
        anyhow::ensure!(
            !self.destinations.destinations.is_empty(),
            "output pipeline requires at least one destination"
        );

        let model_outputs = collect_names(model_outputs, "model output")?;
        let auxiliary = collect_names(auxiliary, "auxiliary output")?;
        let overlap = model_outputs
            .intersection(&auxiliary)
            .cloned()
            .collect::<Vec<_>>();
        anyhow::ensure!(
            overlap.is_empty(),
            "variables cannot be both model outputs and auxiliary outputs: {overlap:?}"
        );

        validate_output_configuration(
            output_configuration,
            &model_outputs,
            &auxiliary,
            &self.destinations,
        )?;

        let ordered = self.destinations.ordered();
        let (deliveries, primary) = resolve_deliveries(
            &ordered,
            self.destinations.default(),
            &model_outputs,
            &auxiliary,
            output_configuration,
        )?;
        let mut resolved_destinations = Vec::with_capacity(ordered.len());

        for destination in ordered {
            destination.backend.validate_local()?;
            let output_variables = deliveries.get(&destination.id).cloned().unwrap_or_default();
            let destination_auxiliary = auxiliary_deliveries(
                destination,
                &auxiliary,
                output_configuration,
                self.destinations.default(),
                &self.destinations,
            )?;
            let variables = output_variables
                .iter()
                .chain(destination_auxiliary.iter())
                .cloned()
                .collect::<BTreeSet<_>>();
            let output_routes = output_routes_for_destination(
                output_configuration,
                &destination.id,
                self.destinations.destinations.len(),
                self.destinations.default(),
            )?;
            let bindings = resolve_destination_bindings(
                destination,
                &destination_auxiliary,
                &variables,
                output_routes,
            )?;
            validate_backend_routes(destination, &bindings)?;
            let interface = OutputInterface::from_bindings(bindings.iter().cloned())
                .map_err(anyhow::Error::from)?;

            let resolved_destination = ResolvedDestination {
                id: destination.id.clone(),
                bindings: bindings.into_boxed_slice(),
                interface,
                delivery: destination.delivery,
                primary: primary
                    .values()
                    .any(|primary_id| primary_id == &destination.id),
            };
            resolved_destinations.push(resolved_destination);
        }

        let resolved = ResolvedOutput {
            model_outputs: model_outputs
                .into_iter()
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            auxiliary: auxiliary.into_iter().collect::<Vec<_>>().into_boxed_slice(),
            destinations: resolved_destinations.into_boxed_slice(),
            generation: self.generation,
        };
        Ok(resolved)
    }

    /// Plan the smallest transition between two pure resolutions.
    ///
    /// Destination IDs are durable owner identities: the pipeline opens that
    /// registry once, and requests only change the resolved bindings and
    /// interfaces of those owners. A changed owner receives one
    /// `Reconfigure`; unchanged owners are implicit. The returned plan performs
    /// no backend work.
    pub(crate) fn plan_reconfiguration(
        &self,
        active: &ResolvedOutput,
        candidate: ResolvedOutput,
        session: SessionId,
        expected_revision: SessionRevision,
    ) -> anyhow::Result<OutputPipelineReconfigurationPlan> {
        self.validate_resolution_for_reconfiguration(active, "active")?;
        self.validate_resolution_for_reconfiguration(&candidate, "candidate")?;

        let active_by_id = active
            .destinations
            .iter()
            .map(|destination| (&destination.id, destination))
            .collect::<BTreeMap<_, _>>();
        let mut changed = Vec::new();
        for candidate_destination in &candidate.destinations {
            let active_destination =
                active_by_id.get(&candidate_destination.id).ok_or_else(|| {
                    anyhow::anyhow!(
                        "candidate output destination `{}` is not an active pipeline owner",
                        candidate_destination.id
                    )
                })?;
            if *active_destination != candidate_destination {
                changed.push(candidate_destination.id.clone());
            }
        }

        Ok(OutputPipelineReconfigurationPlan {
            session,
            expected_revision,
            candidate,
            changed: changed.into_boxed_slice(),
        })
    }

    fn validate_resolution_for_reconfiguration(
        &self,
        resolved: &ResolvedOutput,
        side: &str,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            resolved.generation == self.generation,
            "{side} resolved output durable configuration does not match the pipeline"
        );
        anyhow::ensure!(
            resolved.destinations.len() == self.destinations.destinations.len(),
            "{side} resolved output destination count does not match the pipeline"
        );
        let mut ids = BTreeSet::new();
        for destination in &resolved.destinations {
            anyhow::ensure!(
                ids.insert(destination.id.clone()),
                "{side} resolved output destination `{}` is duplicated",
                destination.id
            );
            let configured = self.destinations.get(&destination.id).ok_or_else(|| {
                anyhow::anyhow!(
                    "{side} resolved output destination `{}` is not configured",
                    destination.id
                )
            })?;
            anyhow::ensure!(
                destination.delivery == configured.delivery,
                "{side} resolved output delivery policy for destination `{}` does not match the pipeline",
                destination.id
            );
            OutputInterface::validate_bindings(destination.interface.bindings())
                .map_err(anyhow::Error::from)?;
        }
        Ok(())
    }

    /// Resolve and open a writer in one operation. Resolution still completes
    /// before any destination is opened.
    pub async fn build<I, A>(
        &self,
        model_outputs: I,
        auxiliary: A,
        output_configuration: Option<&OutputConfiguration>,
    ) -> Result<OutputWriter<V>, OutputError>
    where
        I: IntoIterator,
        I::Item: Borrow<VarName>,
        A: IntoIterator,
        A::Item: Borrow<VarName>,
        V: JsonStreamValue + RosStreamValue,
    {
        let resolved = self
            .resolve(model_outputs, auxiliary, output_configuration)
            .map_err(OutputError::from)?;
        self.open(resolved).await
    }

    /// Open a resolved output and return its live writer without session state.
    /// A single destination keeps the direct fast path; sessions use the router
    /// because they may update destination bindings in place later.
    pub async fn open(&self, resolved: ResolvedOutput) -> Result<OutputWriter<V>, OutputError>
    where
        V: JsonStreamValue + RosStreamValue,
    {
        self.validate_resolved_for_open(&resolved)?;
        if resolved.destinations.len() == 1 {
            let opened = self.open_destination(&resolved.destinations[0]).await?;
            let mut writer = opened.writer;
            if let Some(executor) = &self.executor {
                writer.attach_drop_executor(Rc::clone(executor));
            }
            return Ok(writer.with_shutdown_timeout(self.shutdown_timeout));
        }
        self.open_session(resolved)
            .await
            .map(OutputPipelineSession::into_writer)
    }

    /// Open every resolved destination and retain the active resolution for live
    /// interface reconfiguration.
    pub async fn open_session(
        &self,
        resolved: ResolvedOutput,
    ) -> Result<OutputPipelineSession<V>, OutputError>
    where
        V: JsonStreamValue + RosStreamValue,
    {
        self.validate_resolved_for_open(&resolved)?;

        let mut opened = Vec::with_capacity(resolved.destinations.len());
        for resolved_destination in resolved.destinations.iter() {
            match self.open_destination(resolved_destination).await {
                Ok(destination) => opened.push(destination),
                Err(error) => {
                    let cleanup = close_opened(&mut opened).await;
                    return Err(with_cleanup(error, cleanup));
                }
            }
        }

        let router = OutputRouter::new(opened);
        let router_state = router.state();
        let mut writer = OutputWriter::from_output_sink(router);
        if let Some(executor) = &self.executor {
            writer.attach_drop_executor(Rc::clone(executor));
        }
        let writer = writer.with_shutdown_timeout(self.shutdown_timeout);
        Ok(OutputPipelineSession::new(
            self.clone(),
            resolved,
            writer,
            router_state,
        ))
    }

    fn validate_resolved_for_open(&self, resolved: &ResolvedOutput) -> Result<(), OutputError> {
        self.validate_resolution_for_reconfiguration(resolved, "resolved")
            .map_err(OutputError::from)?;
        self.destinations
            .validate_ids()
            .map_err(OutputError::from)?;
        for destination in self.destinations.destinations.values() {
            destination
                .backend
                .validate_local()
                .map_err(OutputError::from)?;
        }
        for destination in &resolved.destinations {
            if destination.delivery.queue.is_some() && self.executor.is_none() {
                return Err(OutputError::invalid(
                    "worker-backed output delivery requires a runtime local executor",
                ));
            }
        }
        Ok(())
    }

    async fn open_destination(
        &self,
        resolved: &ResolvedDestination,
    ) -> Result<OpenedDestination<V>, OutputError>
    where
        V: JsonStreamValue + RosStreamValue,
    {
        let destination = self.destinations.get(&resolved.id).ok_or_else(|| {
            OutputError::invalid(format!(
                "resolved output destination `{}` is not configured",
                resolved.id
            ))
        })?;
        let writer = destination.backend.open(resolved.interface.clone()).await?;
        let (writer, cancellation) =
            Delivery::new(writer, self.executor.clone(), resolved.delivery)?.into_parts();
        Ok(OpenedDestination {
            id: resolved.id.clone(),
            variables: variables_for_destination(resolved),
            cancellation,
            writer,
        })
    }
}

fn collect_names<I>(names: I, description: &str) -> anyhow::Result<BTreeSet<VarName>>
where
    I: IntoIterator,
    I::Item: Borrow<VarName>,
{
    let mut result = BTreeSet::new();
    for name in names {
        let name = name.borrow().clone();
        anyhow::ensure!(
            !name.name().trim().is_empty(),
            "{description} variable cannot be empty"
        );
        anyhow::ensure!(
            result.insert(name.clone()),
            "duplicate {description} variable `{name}`"
        );
    }
    Ok(result)
}

fn validate_output_configuration<V>(
    output_configuration: Option<&OutputConfiguration>,
    model_outputs: &BTreeSet<VarName>,
    auxiliary: &BTreeSet<VarName>,
    destinations: &OutputDestinations<V>,
) -> anyhow::Result<()> {
    let Some(config) = output_configuration else {
        return Ok(());
    };
    config.validate_structure()?;
    if let Some(destination) = &config.destination {
        anyhow::ensure!(
            destinations.destinations.contains_key(destination),
            "monitor output destination `{destination}` is not configured"
        );
    }
    if let Some(groups) = &config.destinations {
        for destination in groups.keys() {
            anyhow::ensure!(
                destinations.destinations.contains_key(destination),
                "monitor output destination `{destination}` is not configured"
            );
        }
        let grouped_outputs = groups
            .values()
            .flat_map(|routes| routes.keys())
            .filter(|variable| model_outputs.contains(*variable))
            .cloned()
            .collect::<BTreeSet<_>>();
        anyhow::ensure!(
            grouped_outputs == *model_outputs,
            "grouped monitor output bindings must cover the model outputs exactly"
        );
        for routes in groups.values() {
            validate_requested_routes(Some(routes), model_outputs, auxiliary, false)?;
        }
    }
    if let Some(routes) = &config.outputs {
        validate_requested_routes(Some(routes), model_outputs, auxiliary, true)?;
        if let Some(destination) = &config.destination {
            anyhow::ensure!(
                destinations.destinations.contains_key(destination),
                "monitor output destination `{destination}` is not configured"
            );
        } else if destinations.destinations.len() > 1 && destinations.default.is_none() {
            anyhow::bail!("flat monitor outputs require an explicit, sole, or default destination");
        }
    }
    Ok(())
}

fn output_routes_for_destination<'a>(
    output_configuration: Option<&'a OutputConfiguration>,
    destination: &DestinationId,
    destination_count: usize,
    default: Option<&DestinationId>,
) -> anyhow::Result<Option<&'a BTreeMap<VarName, Route>>> {
    let Some(config) = output_configuration else {
        return Ok(None);
    };
    if let Some(groups) = &config.destinations {
        return Ok(groups.get(destination));
    }
    let Some(routes) = config.outputs.as_ref() else {
        return Ok(None);
    };
    let target = config
        .destination
        .as_ref()
        .or_else(|| (destination_count == 1).then_some(destination))
        .or(default)
        .ok_or_else(|| anyhow::anyhow!("flat monitor outputs have no destination owner"))?;
    Ok((target == destination).then_some(routes))
}

fn resolve_deliveries<V>(
    destinations: &[&OutputDestination<V>],
    default: Option<&DestinationId>,
    model_outputs: &BTreeSet<VarName>,
    _auxiliary: &BTreeSet<VarName>,
    output_configuration: Option<&OutputConfiguration>,
) -> anyhow::Result<(
    BTreeMap<DestinationId, BTreeSet<VarName>>,
    BTreeMap<VarName, DestinationId>,
)> {
    let mut deliveries = destinations
        .iter()
        .map(|destination| (destination.id.clone(), BTreeSet::new()))
        .collect::<BTreeMap<_, _>>();
    let mut primary = BTreeMap::new();

    if let Some(config) = output_configuration {
        if let Some(groups) = &config.destinations {
            for (destination, routes) in groups {
                let selected = deliveries
                    .get_mut(destination)
                    .expect("monitor destination was validated");
                selected.extend(
                    routes
                        .keys()
                        .filter(|variable| model_outputs.contains(*variable))
                        .cloned(),
                );
            }
            for variable in model_outputs {
                let owners = destinations
                    .iter()
                    .filter(|destination| {
                        destination.selection.is_primary_capable()
                            && deliveries
                                .get(destination.id.as_str())
                                .is_some_and(|selected| selected.contains(variable))
                    })
                    .map(|destination| destination.id.clone())
                    .collect::<Vec<_>>();
                let Some(owner) = owners
                    .iter()
                    .find(|owner| default.is_some_and(|default| default == *owner))
                    .or_else(|| owners.first())
                else {
                    anyhow::bail!("model output `{variable}` has no grouped destination")
                };
                primary.insert(variable.clone(), owner.clone());
            }
            return Ok((deliveries, primary));
        }

        if let Some(routes) = &config.outputs {
            let target = config
                .destination
                .as_ref()
                .or_else(|| (destinations.len() == 1).then_some(&destinations[0].id))
                .or(default)
                .ok_or_else(|| anyhow::anyhow!("flat monitor outputs have no destination owner"))?;
            let target_destination = destinations
                .iter()
                .find(|destination| &destination.id == target)
                .ok_or_else(|| anyhow::anyhow!("flat monitor output destination is unknown"))?;
            anyhow::ensure!(
                target_destination.selection.is_primary_capable(),
                "flat monitor output destination `{target}` is not primary-capable"
            );
            let selected = deliveries
                .get_mut(target)
                .expect("monitor destination was validated");
            selected.extend(model_outputs.iter().cloned());
            for variable in model_outputs {
                primary.insert(variable.clone(), target.clone());
            }
            let _ = routes;
            return Ok((deliveries, primary));
        }
    }

    let mut explicit_partitions = BTreeSet::new();
    for destination in destinations {
        if let OutputDestinationSelection::Partition(selected) = &destination.selection {
            ensure_subset(selected, model_outputs, &destination.id)?;
            for variable in selected {
                anyhow::ensure!(
                    explicit_partitions.insert(variable.clone()),
                    "model output `{variable}` is assigned to more than one primary partition"
                );
                primary.insert(variable.clone(), destination.id.clone());
                deliveries
                    .get_mut(&destination.id)
                    .expect("destination is registered")
                    .insert(variable.clone());
            }
        }
    }

    // A local route catalog can add delivery fan-out, but it can establish a
    // primary owner only on an All or Partition destination. Mirror catalogs
    // are deliberately ignored here; their primary must already exist.
    for variable in model_outputs {
        if primary.contains_key(variable) {
            continue;
        }
        let owners = destinations
            .iter()
            .filter(|destination| {
                destination.selection.is_primary_capable()
                    && destination
                        .routes
                        .as_ref()
                        .is_some_and(|routes| routes.contains_key(variable))
            })
            .map(|destination| destination.id.clone())
            .collect::<Vec<_>>();
        if !owners.is_empty() {
            let owner = owners
                .iter()
                .find(|owner| default.is_some_and(|default| default == *owner))
                .or_else(|| owners.first())
                .expect("non-empty catalog owners");
            primary.insert(variable.clone(), owner.clone());
            for destination in owners {
                deliveries
                    .get_mut(&destination)
                    .expect("destination is registered")
                    .insert(variable.clone());
            }
        }
    }

    let all_primary_count = destinations
        .iter()
        .filter(|destination| matches!(&destination.selection, OutputDestinationSelection::All))
        .count();
    for variable in model_outputs {
        if primary.contains_key(variable) {
            continue;
        }
        let candidate = if destinations.len() == 1 {
            destinations[0]
                .selection
                .is_primary_capable()
                .then_some(destinations[0].id.clone())
        } else if let Some(default) = default {
            destinations
                .iter()
                .find(|destination| &destination.id == default)
                .filter(|destination| {
                    matches!(destination.selection, OutputDestinationSelection::All)
                        && !destination.backend.requires_format()
                })
                .map(|destination| destination.id.clone())
        } else if all_primary_count == 1 {
            destinations
                .iter()
                .find(|destination| {
                    matches!(destination.selection, OutputDestinationSelection::All)
                })
                .map(|destination| destination.id.clone())
        } else {
            None
        };
        let Some(candidate) = candidate else {
            anyhow::bail!("model output `{variable}` has no destination owner")
        };
        primary.insert(variable.clone(), candidate.clone());
        deliveries
            .get_mut(&candidate)
            .expect("destination is registered")
            .insert(variable.clone());
    }

    for destination in destinations {
        let mirrors = match &destination.selection {
            OutputDestinationSelection::Mirror(variables) => variables,
            OutputDestinationSelection::MirrorAll => model_outputs,
            OutputDestinationSelection::All | OutputDestinationSelection::Partition(_) => continue,
        };
        ensure_subset(mirrors, model_outputs, &destination.id)?;
        for variable in mirrors {
            anyhow::ensure!(
                primary.contains_key(variable),
                "mirrored output `{variable}` has no primary destination"
            );
            deliveries
                .get_mut(&destination.id)
                .expect("destination is registered")
                .insert(variable.clone());
        }
    }
    Ok((deliveries, primary))
}

fn auxiliary_deliveries<V>(
    destination: &OutputDestination<V>,
    auxiliary: &BTreeSet<VarName>,
    output_configuration: Option<&OutputConfiguration>,
    default: Option<&DestinationId>,
    destinations: &OutputDestinations<V>,
) -> anyhow::Result<BTreeSet<VarName>> {
    if auxiliary.is_empty() {
        return Ok(BTreeSet::new());
    }
    if let Some(config) = output_configuration {
        if let Some(groups) = &config.destinations {
            return Ok(groups
                .get(&destination.id)
                .into_iter()
                .flat_map(|routes| routes.keys())
                .filter(|variable| auxiliary.contains(*variable))
                .cloned()
                .collect());
        }
        if let Some(routes) = &config.outputs {
            let target = config
                .destination
                .as_ref()
                .or_else(|| (destinations.destinations.len() == 1).then_some(&destination.id))
                .or(default);
            if target.is_some_and(|target| target == &destination.id) {
                return Ok(routes
                    .keys()
                    .filter(|variable| auxiliary.contains(*variable))
                    .cloned()
                    .collect());
            }
        }
    }
    let owner = default.or_else(|| destinations.destinations.keys().next());
    Ok(if owner.is_some_and(|owner| owner == &destination.id) {
        auxiliary.clone()
    } else {
        BTreeSet::new()
    })
}

fn validate_requested_routes(
    routes: Option<&BTreeMap<VarName, Route>>,
    model_outputs: &BTreeSet<VarName>,
    auxiliary: &BTreeSet<VarName>,
    require_model_coverage: bool,
) -> anyhow::Result<()> {
    let Some(routes) = routes else {
        return Ok(());
    };
    for (variable, route) in routes {
        anyhow::ensure!(
            model_outputs.contains(variable) || auxiliary.contains(variable),
            "monitor output route is declared for unknown variable `{variable}`"
        );
        Route::new(route.address(), route.format().cloned())?;
    }
    if require_model_coverage {
        let missing = model_outputs
            .difference(&routes.keys().cloned().collect())
            .cloned()
            .collect::<Vec<_>>();
        anyhow::ensure!(
            missing.is_empty(),
            "monitor output routes are missing model outputs: {missing:?}"
        );
    }
    Ok(())
}

fn ensure_subset(
    selected: &BTreeSet<VarName>,
    model_outputs: &BTreeSet<VarName>,
    id: &DestinationId,
) -> anyhow::Result<()> {
    let extra = selected
        .difference(model_outputs)
        .cloned()
        .collect::<Vec<_>>();
    anyhow::ensure!(
        extra.is_empty(),
        "destination `{id}` selects variables that are not model outputs: {extra:?}"
    );
    Ok(())
}

fn resolve_destination_bindings<V>(
    destination: &OutputDestination<V>,
    auxiliary: &BTreeSet<VarName>,
    variables: &BTreeSet<VarName>,
    requested_routes: Option<&BTreeMap<VarName, Route>>,
) -> anyhow::Result<Vec<OutputBinding>> {
    let mut bindings = Vec::with_capacity(variables.len());
    for variable in variables {
        // A requested route is an override, not a replacement for
        // the durable catalog. The latter may contain routes for a previous
        // the resolved model outputs; entries outside them are ignored, while
        // newly added variables continue to the backend's default route.
        let requested = requested_routes
            .and_then(|routes| routes.get(variable))
            .or_else(|| {
                destination
                    .routes
                    .as_ref()
                    .and_then(|routes| routes.get(variable))
            });
        let route = match requested {
            Some(route) => normalize_route(&destination.backend, variable, route)?,
            None if auxiliary.contains(variable) => None,
            None => default_route(&destination.backend, variable)?,
        };
        let role = if auxiliary.contains(variable) {
            OutputRole::Auxiliary
        } else {
            OutputRole::Output
        };
        bindings.push(OutputBinding::new(variable.clone(), route, role));
    }
    Ok(bindings)
}

fn validate_backend_routes<V>(
    destination: &OutputDestination<V>,
    bindings: &[OutputBinding],
) -> anyhow::Result<()> {
    let route_kind = match destination.backend.kind() {
        super::OutputBackendKind::Mqtt => "MQTT topic",
        super::OutputBackendKind::Redis => "Redis channel",
        _ => return Ok(()),
    };
    // VarName ordering is based on interned identity, so sort by its displayed
    // name to keep the reported collision independent of construction order.
    let mut output_bindings = bindings
        .iter()
        .filter(|binding| binding.role() == OutputRole::Output)
        .collect::<Vec<_>>();
    output_bindings.sort_by(|left, right| left.variable().name().cmp(&right.variable().name()));

    let mut routes = BTreeMap::<&str, &VarName>::new();
    for binding in output_bindings {
        let Some(route) = binding.route() else {
            continue;
        };
        if let Some(previous_variable) = routes.insert(route.address(), binding.variable()) {
            anyhow::bail!(
                "output destination `{}` has duplicate {route_kind} `{}` for variables `{}` and `{}`",
                destination.id,
                route.address(),
                previous_variable,
                binding.variable(),
            );
        }
    }
    Ok(())
}

fn default_route<V>(
    backend: &OutputBackendConfig<V>,
    variable: &VarName,
) -> anyhow::Result<Option<Route>> {
    if backend.requires_format() {
        anyhow::bail!("backend requires an explicit route format for output `{variable}`")
    }
    if matches!(
        backend.kind(),
        super::OutputBackendKind::Mqtt | super::OutputBackendKind::Redis
    ) {
        return Route::new(
            variable.name().into_boxed_str(),
            Some(FormatId::new(JSON_CODEC)),
        )
        .map(Some);
    }
    Ok(None)
}

fn normalize_route<V>(
    backend: &OutputBackendConfig<V>,
    variable: &VarName,
    route: &Route,
) -> anyhow::Result<Option<Route>> {
    let route = Route::new(route.address(), route.format().cloned())?;
    match backend.kind() {
        super::OutputBackendKind::Mqtt | super::OutputBackendKind::Redis => {
            if let Some(codec) = route.format() {
                anyhow::ensure!(
                    backend.supports_format(codec.as_str()),
                    "unsupported output route format `{}` for `{variable}`",
                    codec
                );
            }
            Route::new(route.address(), Some(FormatId::new(JSON_CODEC))).map(Some)
        }
        super::OutputBackendKind::Ros => {
            anyhow::ensure!(
                route.format().is_some(),
                "ROS output `{variable}` requires a route format"
            );
            Ok(Some(route))
        }
        #[cfg(any(test, feature = "test-support"))]
        super::OutputBackendKind::Test => Ok(Some(route)),
        _ => {
            anyhow::ensure!(
                route.format().is_none(),
                "backend `{}` does not support an output route format for `{variable}`",
                backend.kind_name()
            );
            Ok(Some(route))
        }
    }
}

fn variables_for_destination(resolved: &ResolvedDestination) -> BTreeSet<VarName> {
    resolved
        .bindings
        .iter()
        .map(|binding| binding.variable().clone())
        .collect()
}

struct OpenedDestination<V> {
    id: DestinationId,
    variables: BTreeSet<VarName>,
    cancellation: Option<DeliveryCancellation>,
    writer: OutputWriter<V>,
}

async fn close_opened<V>(opened: &mut [OpenedDestination<V>]) -> Option<OutputError> {
    let mut error = None;
    for destination in opened {
        if let Err(close_error) = destination.writer.close().await {
            super::remember_error(&mut error, close_error);
        }
    }
    error
}

fn with_cleanup(primary: OutputError, cleanup: Option<OutputError>) -> OutputError {
    match cleanup {
        Some(cleanup) => super::combine_errors(primary, cleanup),
        None => primary,
    }
}

/// A resolved destination with a fixed backend interface and delivery policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedDestination {
    id: DestinationId,
    bindings: Box<[OutputBinding]>,
    interface: OutputInterface,
    delivery: DeliveryPolicy,
    primary: bool,
}

impl ResolvedDestination {
    pub fn id(&self) -> &DestinationId {
        &self.id
    }

    pub fn bindings(&self) -> &[OutputBinding] {
        &self.bindings
    }

    pub fn interface(&self) -> &OutputInterface {
        &self.interface
    }

    pub const fn delivery(&self) -> DeliveryPolicy {
        self.delivery
    }

    pub fn is_primary(&self) -> bool {
        self.primary
    }
}

/// Complete deterministic output resolution for one active monitor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedOutput {
    model_outputs: Box<[VarName]>,
    auxiliary: Box<[VarName]>,
    destinations: Box<[ResolvedDestination]>,
    generation: PipelineGeneration,
}

impl ResolvedOutput {
    pub fn model_outputs(&self) -> &[VarName] {
        &self.model_outputs
    }

    pub fn auxiliary(&self) -> &[VarName] {
        &self.auxiliary
    }

    pub fn destinations(&self) -> &[ResolvedDestination] {
        &self.destinations
    }

    pub fn destination(&self, id: &DestinationId) -> Option<&ResolvedDestination> {
        self.destinations
            .iter()
            .find(|destination| &destination.id == id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OutputPipelineReconfigurationPlan {
    session: SessionId,
    expected_revision: SessionRevision,
    candidate: ResolvedOutput,
    changed: Box<[DestinationId]>,
}

impl OutputPipelineReconfigurationPlan {
    pub(crate) fn candidate(&self) -> &ResolvedOutput {
        &self.candidate
    }

    pub(crate) fn changed_destinations(&self) -> &[DestinationId] {
        &self.changed
    }

    pub(crate) fn is_changed(&self) -> bool {
        !self.changed.is_empty()
    }

    pub(crate) fn session(&self) -> SessionId {
        self.session
    }

    pub(crate) fn expected_revision(&self) -> SessionRevision {
        self.expected_revision
    }
}

/// An opened output pipeline together with its pure resolution and the live
/// router state used to update fixed destination bindings.
pub struct OutputPipelineSession<V> {
    pipeline: OutputPipeline<V>,
    resolved: ResolvedOutput,
    writer: OutputWriter<V>,
    router_state: Rc<RefCell<OutputRouterState<V>>>,
    sticky_error: Option<OutputError>,
    session: SessionId,
    revision: SessionRevision,
    pending_revision: Option<SessionRevision>,
}

impl<V> OutputPipelineSession<V> {
    fn new(
        pipeline: OutputPipeline<V>,
        resolved: ResolvedOutput,
        writer: OutputWriter<V>,
        router_state: Rc<RefCell<OutputRouterState<V>>>,
    ) -> Self {
        Self {
            pipeline,
            resolved,
            writer,
            router_state,
            sticky_error: None,
            session: SessionId::new(),
            revision: SessionRevision::initial(),
            pending_revision: None,
        }
    }

    pub fn resolved(&self) -> &ResolvedOutput {
        &self.resolved
    }

    pub(crate) fn session_id(&self) -> SessionId {
        self.session
    }

    pub(crate) fn revision(&self) -> SessionRevision {
        self.revision
    }

    pub(crate) fn commit_revision(&mut self, expected: SessionRevision) -> Result<(), OutputError> {
        if self.revision != expected || self.pending_revision != Some(expected) {
            return Err(self.fail(OutputError::invalid(
                "output session revision changed before coordinated commit",
            )));
        }
        self.revision = self.revision.next();
        self.pending_revision = None;
        Ok(())
    }

    pub fn writer(&self) -> &OutputWriter<V> {
        &self.writer
    }

    pub fn error(&self) -> Option<&OutputError> {
        self.sticky_error.as_ref().or_else(|| self.writer.error())
    }

    pub fn writer_mut(&mut self) -> &mut OutputWriter<V> {
        &mut self.writer
    }

    pub fn into_writer(self) -> OutputWriter<V> {
        self.writer
    }

    pub async fn send(&mut self, batch: OutputBatch<V>) -> Result<(), OutputError> {
        if let Some(error) = &self.sticky_error {
            return Err(error.clone());
        }
        self.writer.send(batch).await
    }

    pub async fn flush(&mut self) -> Result<(), OutputError> {
        if let Some(error) = &self.sticky_error {
            return Err(error.clone());
        }
        self.writer.flush().await
    }

    pub async fn close(&mut self) -> Result<(), OutputError> {
        let deadline = self.writer.shutdown_deadline();
        self.close_with_deadline(deadline).await
    }

    pub async fn close_with_deadline(
        &mut self,
        deadline: ShutdownDeadline,
    ) -> Result<(), OutputError> {
        let close = self.writer.close_with_deadline(deadline).await;
        match (self.sticky_error.clone(), close) {
            (Some(primary), Ok(())) => Err(primary),
            (Some(primary), Err(cleanup)) => Err(super::combine_errors(primary, cleanup)),
            (None, result) => result,
        }
    }

    /// Apply a pure pipeline plan at the caller's ordered barrier. Only the
    /// affected existing owners are flushed and updated. An interface update
    /// must be supported by the opened owner; there is no hidden replacement
    /// fallback.
    pub(crate) async fn apply_reconfiguration(
        &mut self,
        plan: OutputPipelineReconfigurationPlan,
    ) -> Result<(), OutputError>
    where
        V: JsonStreamValue + RosStreamValue,
    {
        if let Some(error) = &self.sticky_error {
            return Err(error.clone());
        }
        if plan.session() != self.session
            || plan.expected_revision() != self.revision
            || self.pending_revision.is_some()
        {
            return Err(self.fail(OutputError::invalid(
                "stale output pipeline reconfiguration plan does not match the active session revision",
            )));
        }
        if let Err(error) = self
            .pipeline
            .validate_resolution_for_reconfiguration(plan.candidate(), "candidate")
        {
            return Err(self.fail(OutputError::from(error)));
        }

        for destination in plan.changed_destinations() {
            let target = plan
                .candidate()
                .destinations
                .iter()
                .find(|target| target.id() == destination)
                .ok_or_else(|| {
                    self.fail(OutputError::invalid(format!(
                        "changed output destination `{destination}` is absent from the candidate resolution",
                    )))
                })?;
            if let Err(error) = OutputRouterState::rebind_destination(
                &self.router_state,
                destination,
                target.interface.clone(),
            )
            .await
            {
                return Err(self.fail(error));
            }
            let variables = variables_for_destination(target);
            let update = {
                let mut state = RefCell::borrow_mut(self.router_state.as_ref());
                state.replace_variables(destination, variables)
            };
            if let Err(error) = update {
                return Err(self.fail(error));
            }
        }

        self.resolved = plan.candidate().clone();
        self.pending_revision = Some(plan.expected_revision());
        Ok(())
    }

    fn fail(&mut self, error: OutputError) -> OutputError {
        if self.sticky_error.is_none() {
            self.sticky_error = Some(error);
        }
        self.sticky_error
            .as_ref()
            .expect("output session retains a failure")
            .clone()
    }
}

struct OutputRouterSelection {
    variables: BTreeSet<VarName>,
}

impl OutputRouterSelection {
    fn new<V>(destinations: &BTreeMap<DestinationId, OpenedDestination<V>>) -> Self {
        let mut selection = Self {
            variables: BTreeSet::new(),
        };
        selection.rebuild(destinations);
        selection
    }

    fn rebuild<V>(&mut self, destinations: &BTreeMap<DestinationId, OpenedDestination<V>>) {
        self.variables.clear();
        for destination in destinations.values() {
            self.variables.extend(destination.variables.iter().cloned());
        }
    }
}

struct OutputRouterState<V> {
    destinations: BTreeMap<DestinationId, OpenedDestination<V>>,
    selection: OutputRouterSelection,
    ready: bool,
    failure: Option<OutputError>,
    closing: bool,
    closed: bool,
    close_done: BTreeSet<DestinationId>,
}

impl<V> OutputRouterState<V> {
    fn cancel_delivery(&self) {
        for destination in self.destinations.values() {
            if let Some(cancellation) = &destination.cancellation {
                cancellation.cancel();
            }
        }
    }

    fn new(destinations: Vec<OpenedDestination<V>>) -> Self {
        let destinations = destinations
            .into_iter()
            .map(|destination| (destination.id.clone(), destination))
            .collect::<BTreeMap<_, _>>();
        let selection = OutputRouterSelection::new(&destinations);
        Self {
            destinations,
            selection,
            ready: false,
            failure: None,
            closing: false,
            closed: false,
            close_done: BTreeSet::new(),
        }
    }

    fn state_error(&self) -> Option<OutputError> {
        if let Some(error) = &self.failure {
            return Some(error.clone());
        }
        if self.closing || self.closed {
            return Some(OutputError::closed());
        }
        None
    }

    fn remember(&mut self, error: OutputError) -> OutputError {
        super::remember_error(&mut self.failure, error);
        self.failure
            .as_ref()
            .expect("router retains an error")
            .clone()
    }

    fn replace_variables(
        &mut self,
        id: &DestinationId,
        variables: BTreeSet<VarName>,
    ) -> Result<(), OutputError> {
        let Some(destination) = self.destinations.get_mut(id) else {
            return Err(OutputError::invalid(format!(
                "output destination `{id}` is not active"
            )));
        };
        destination.variables = variables;
        self.selection.rebuild(&self.destinations);
        self.ready = false;
        Ok(())
    }
}

struct OutputRouter<V> {
    state: Rc<RefCell<OutputRouterState<V>>>,
}

impl<V> OutputRouter<V> {
    fn new(destinations: Vec<OpenedDestination<V>>) -> Self {
        Self {
            state: Rc::new(RefCell::new(OutputRouterState::new(destinations))),
        }
    }

    fn state(&self) -> Rc<RefCell<OutputRouterState<V>>> {
        Rc::clone(&self.state)
    }
}

impl<V: Clone + 'static> crate::core::OutputSink<V> for OutputRouter<V> {
    fn abort(self: Pin<&mut Self>) {
        let mut state = self.state.borrow_mut();
        state.cancel_delivery();
        for destination in state.destinations.values_mut() {
            destination.writer.abort();
        }
        state.closing = true;
        state.closed = true;
    }
}

impl<V: 'static> OutputRouterState<V> {
    async fn rebind_destination(
        state: &Rc<RefCell<Self>>,
        id: &DestinationId,
        interface: OutputInterface,
    ) -> Result<(), OutputError> {
        futures::future::poll_fn(|cx| {
            let mut state = state.borrow_mut();
            if let Some(error) = state.state_error() {
                return Poll::Ready(Err(error));
            }
            let result = {
                let Some(destination) = state.destinations.get_mut(id) else {
                    return Poll::Ready(Err(OutputError::invalid(format!(
                        "output destination `{id}` is not active"
                    ))));
                };
                // The writer retains the operation state if this future is
                // cancelled; keep its owner in the router throughout.
                let rebind = destination.writer.rebind(interface.clone());
                futures::pin_mut!(rebind);
                rebind.poll(cx)
            };
            match result {
                Poll::Ready(Err(error)) => Poll::Ready(Err(state.remember(error))),
                other => other,
            }
        })
        .await
    }
}

/// Fan out one logical batch under global backpressure. The router's mutable
/// state is shared with its owning pipeline session so fixed destination owners
/// can receive new bindings without replacing their live writers.
impl<V: Clone + 'static> Sink<OutputBatch<V>> for OutputRouter<V> {
    type Error = OutputError;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        let mut state = this.state.borrow_mut();
        if let Some(error) = state.state_error() {
            return std::task::Poll::Ready(Err(error));
        }
        let mut pending = false;
        let mut error = None;
        for destination in state.destinations.values_mut() {
            match std::pin::Pin::new(&mut destination.writer).poll_ready(context) {
                std::task::Poll::Pending => pending = true,
                std::task::Poll::Ready(Ok(())) => {}
                std::task::Poll::Ready(Err(destination_error)) => {
                    super::remember_error(&mut error, destination_error);
                }
            }
        }
        if let Some(error) = error {
            return std::task::Poll::Ready(Err(state.remember(error)));
        }
        if pending {
            std::task::Poll::Pending
        } else {
            state.ready = true;
            std::task::Poll::Ready(Ok(()))
        }
    }

    fn start_send(
        self: std::pin::Pin<&mut Self>,
        batch: OutputBatch<V>,
    ) -> Result<(), Self::Error> {
        let this = self.get_mut();
        let mut state = this.state.borrow_mut();
        if let Some(error) = state.state_error() {
            return Err(error);
        }
        if !state.ready {
            return Err(state.remember(OutputError::backend(
                "output router was not ready for start_send",
            )));
        }
        state.ready = false;
        if let Some(variable) = batch
            .updates()
            .find(|update| !state.selection.variables.contains(update.variable))
            .map(|update| update.variable.clone())
        {
            return Err(state.remember(OutputError::invalid(format!(
                "output update variable `{variable}` has no resolved destination"
            ))));
        }
        if state.destinations.len() == 1 {
            let destination = state
                .destinations
                .values_mut()
                .next()
                .expect("destination count checked above");
            if let Err(error) = std::pin::Pin::new(&mut destination.writer).start_send(batch) {
                return Err(state.remember(error));
            }
            return Ok(());
        }
        for destination in state.destinations.values_mut() {
            let selected = match batch.select_variables_cloned(&destination.variables) {
                Ok(selected) => selected,
                Err(error) => return Err(state.remember(error)),
            };
            if selected.is_empty() {
                continue;
            }
            if let Err(error) = std::pin::Pin::new(&mut destination.writer).start_send(selected) {
                return Err(state.remember(error));
            }
        }
        Ok(())
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        let mut state = this.state.borrow_mut();
        if state.closed {
            return std::task::Poll::Ready(Err(OutputError::closed()));
        }
        let mut pending = false;
        let mut error = None;
        for destination in state.destinations.values_mut() {
            match std::pin::Pin::new(&mut destination.writer).poll_flush(context) {
                std::task::Poll::Pending => pending = true,
                std::task::Poll::Ready(Ok(())) => {}
                std::task::Poll::Ready(Err(destination_error)) => {
                    super::remember_error(&mut error, destination_error);
                }
            }
        }
        if let Some(error) = error {
            return std::task::Poll::Ready(Err(state.remember(error)));
        }
        if pending {
            std::task::Poll::Pending
        } else {
            std::task::Poll::Ready(Ok(()))
        }
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        let mut state = this.state.borrow_mut();
        if state.closed {
            return std::task::Poll::Ready(Err(state
                .failure
                .clone()
                .unwrap_or_else(OutputError::closed)));
        }
        state.closing = true;
        let mut pending = false;
        let mut cleanup_error = None;
        let destination_ids = state.destinations.keys().cloned().collect::<Vec<_>>();
        for id in destination_ids {
            if state.close_done.contains(&id) {
                continue;
            }
            let result = {
                let destination = state
                    .destinations
                    .get_mut(&id)
                    .expect("destination ID came from the active registry");
                std::pin::Pin::new(&mut destination.writer).poll_close(context)
            };
            match result {
                std::task::Poll::Pending => pending = true,
                std::task::Poll::Ready(Ok(())) => {
                    state.close_done.insert(id);
                }
                std::task::Poll::Ready(Err(error)) => {
                    state.close_done.insert(id);
                    super::remember_error(&mut cleanup_error, error);
                }
            }
        }
        if pending {
            if let Some(error) = cleanup_error {
                super::remember_error(&mut state.failure, error);
            }
            return std::task::Poll::Pending;
        }
        if let Some(error) = cleanup_error {
            super::remember_error(&mut state.failure, error);
        }
        state.closed = true;
        state.closing = false;
        match state.failure.clone() {
            Some(error) => std::task::Poll::Ready(Err(error)),
            None => std::task::Poll::Ready(Ok(())),
        }
    }
}

trait BackendKindName {
    fn kind_name(&self) -> &'static str;
}

impl<V> BackendKindName for OutputBackendConfig<V> {
    fn kind_name(&self) -> &'static str {
        match self.kind() {
            super::OutputBackendKind::Stdout => "stdout",
            super::OutputBackendKind::Null => "null",
            super::OutputBackendKind::LimitedNull => "limited-null",
            super::OutputBackendKind::Channel => "channel",
            super::OutputBackendKind::Mqtt => "mqtt",
            super::OutputBackendKind::Redis => "redis",
            super::OutputBackendKind::Ros => "ros",
            #[cfg(any(test, feature = "test-support"))]
            super::OutputBackendKind::Test => "test",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::{Cell, RefCell},
        num::NonZeroUsize,
        pin::Pin,
        rc::Rc,
        task::{Context, Poll},
    };

    use async_trait::async_trait;
    use futures::Sink;

    use super::*;
    use crate::core::{OutputSink, OutputUpdate, VarName};
    use crate::io::output::TestOutputOpener;
    use crate::io::output::{CoalescingLimits, QueueLimits};

    #[derive(Clone)]
    struct RecordingBackend {
        opened: Rc<Cell<usize>>,
        closed: Rc<Cell<usize>>,
        batches: Rc<RefCell<Vec<OutputBatch<crate::Value>>>>,
        fail_open: bool,
        fail_close: bool,
    }

    #[derive(Clone)]
    struct ReconfigurableRecordingBackend {
        opened: Rc<Cell<usize>>,
        closed: Rc<Cell<usize>>,
        flushes: Rc<Cell<usize>>,
        interface_updates: Rc<Cell<usize>>,
        batches: Rc<RefCell<Vec<OutputBatch<crate::Value>>>>,
        fail_interface_update: bool,
    }

    impl ReconfigurableRecordingBackend {
        fn new() -> Self {
            Self {
                opened: Rc::new(Cell::new(0)),
                closed: Rc::new(Cell::new(0)),
                flushes: Rc::new(Cell::new(0)),
                interface_updates: Rc::new(Cell::new(0)),
                batches: Rc::new(RefCell::new(Vec::new())),
                fail_interface_update: false,
            }
        }
    }

    struct ReconfigurableRecordingSink {
        interface: OutputInterface,
        interface_updates: Rc<Cell<usize>>,
        fail_interface_update: bool,
        closed: Rc<Cell<usize>>,
        flushes: Rc<Cell<usize>>,
        batches: Rc<RefCell<Vec<OutputBatch<crate::Value>>>>,
        ready: bool,
        is_closed: bool,
    }

    struct RecordingSink {
        closed: Rc<Cell<usize>>,
        batches: Rc<RefCell<Vec<OutputBatch<crate::Value>>>>,
        ready: bool,
        fail_close: bool,
    }

    struct GenericRecordingSink<V> {
        closed: Rc<Cell<usize>>,
        batches: Rc<RefCell<Vec<OutputBatch<V>>>>,
        ready: bool,
    }

    struct GateSink {
        gate: Rc<Cell<bool>>,
        ready: bool,
    }

    struct CloneCountingValue {
        clones: Rc<Cell<usize>>,
        value: i64,
    }

    impl Clone for CloneCountingValue {
        fn clone(&self) -> Self {
            self.clones.set(self.clones.get() + 1);
            Self {
                clones: Rc::clone(&self.clones),
                value: self.value,
            }
        }
    }

    impl<V> Sink<OutputBatch<V>> for GenericRecordingSink<V> {
        type Error = OutputError;

        fn poll_ready(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            self.ready = true;
            Poll::Ready(Ok(()))
        }

        fn start_send(mut self: Pin<&mut Self>, batch: OutputBatch<V>) -> Result<(), Self::Error> {
            if !self.ready {
                return Err(OutputError::backend("generic recording sink was not ready"));
            }
            self.ready = false;
            self.batches.borrow_mut().push(batch);
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            self.closed.set(self.closed.get() + 1);
            Poll::Ready(Ok(()))
        }
    }

    impl Sink<OutputBatch<crate::Value>> for GateSink {
        type Error = OutputError;

        fn poll_ready(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            if !self.gate.get() {
                return Poll::Pending;
            }
            self.ready = true;
            Poll::Ready(Ok(()))
        }

        fn start_send(
            mut self: Pin<&mut Self>,
            _batch: OutputBatch<crate::Value>,
        ) -> Result<(), Self::Error> {
            if !self.ready {
                return Err(OutputError::backend("gate sink was not ready"));
            }
            self.ready = false;
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    impl Sink<OutputBatch<crate::Value>> for ReconfigurableRecordingSink {
        type Error = OutputError;

        fn poll_ready(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            if self.is_closed {
                return Poll::Ready(Err(OutputError::closed()));
            }
            self.ready = true;
            Poll::Ready(Ok(()))
        }

        fn start_send(
            mut self: Pin<&mut Self>,
            batch: OutputBatch<crate::Value>,
        ) -> Result<(), Self::Error> {
            if self.is_closed {
                return Err(OutputError::closed());
            }
            if !self.ready {
                return Err(OutputError::backend(
                    "reconfigurable recording sink was not ready",
                ));
            }
            self.interface.validate_batch(&batch)?;
            self.ready = false;
            self.batches.borrow_mut().push(batch);
            Ok(())
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            if self.is_closed {
                return Poll::Ready(Err(OutputError::closed()));
            }
            self.flushes.set(self.flushes.get() + 1);
            self.ready = true;
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            if self.is_closed {
                return Poll::Ready(Err(OutputError::closed()));
            }
            self.is_closed = true;
            self.ready = true;
            self.closed.set(self.closed.get() + 1);
            Poll::Ready(Ok(()))
        }
    }

    impl OutputSink<crate::Value> for ReconfigurableRecordingSink {
        fn poll_rebind(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            interface: &OutputInterface,
        ) -> Poll<Result<(), OutputError>> {
            if self.fail_interface_update {
                return Poll::Ready(Err(OutputError::backend(
                    "intentional interface update failure",
                )));
            }
            self.interface = interface.clone();
            self.interface_updates.set(self.interface_updates.get() + 1);
            Poll::Ready(Ok(()))
        }
    }

    #[async_trait(?Send)]
    impl TestOutputOpener<crate::Value> for ReconfigurableRecordingBackend {
        async fn open(
            &self,
            interface: OutputInterface,
        ) -> Result<OutputWriter<crate::Value>, OutputError> {
            self.opened.set(self.opened.get() + 1);
            Ok(OutputWriter::from_output_sink(
                ReconfigurableRecordingSink {
                    interface,
                    interface_updates: Rc::clone(&self.interface_updates),
                    fail_interface_update: self.fail_interface_update,
                    closed: Rc::clone(&self.closed),
                    flushes: Rc::clone(&self.flushes),
                    batches: Rc::clone(&self.batches),
                    ready: false,
                    is_closed: false,
                },
            ))
        }
    }

    impl Sink<OutputBatch<crate::Value>> for RecordingSink {
        type Error = OutputError;

        fn poll_ready(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            self.ready = true;
            Poll::Ready(Ok(()))
        }

        fn start_send(
            mut self: Pin<&mut Self>,
            batch: OutputBatch<crate::Value>,
        ) -> Result<(), Self::Error> {
            if !self.ready {
                return Err(OutputError::backend("recording sink was not ready"));
            }
            self.ready = false;
            self.batches.borrow_mut().push(batch);
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            self.closed.set(self.closed.get() + 1);
            if self.fail_close {
                Poll::Ready(Err(OutputError::backend("intentional close failure")))
            } else {
                Poll::Ready(Ok(()))
            }
        }
    }

    #[async_trait(?Send)]
    impl TestOutputOpener<crate::Value> for RecordingBackend {
        async fn open(
            &self,
            _interface: OutputInterface,
        ) -> Result<OutputWriter<crate::Value>, OutputError> {
            if self.fail_open {
                return Err(OutputError::backend("intentional open failure"));
            }
            self.opened.set(self.opened.get() + 1);
            Ok(OutputWriter::from_sink(RecordingSink {
                closed: Rc::clone(&self.closed),
                batches: Rc::clone(&self.batches),
                ready: false,
                fail_close: self.fail_close,
            }))
        }
    }

    fn route(name: &str) -> Route {
        Route::new(name.to_owned().into_boxed_str(), None).unwrap()
    }

    fn value(name: &str, value: i64) -> OutputUpdate<crate::Value> {
        OutputUpdate::new(VarName::new(name), crate::Value::Int(value))
    }

    fn pipeline(
        destinations: Vec<OutputDestination<crate::Value>>,
    ) -> OutputPipeline<crate::Value> {
        OutputPipeline::new(OutputDestinations::new(destinations).unwrap())
    }

    #[test]
    fn durable_routes_fallback_for_changed_output_interfaces() {
        let destination = OutputDestination::new("out", OutputBackendConfig::null())
            .with_routes([(VarName::new("x"), route("/configured/x"))]);
        let resolved = pipeline(vec![destination])
            .resolve(
                [VarName::new("x"), VarName::new("y")],
                std::iter::empty::<VarName>(),
                None,
            )
            .unwrap();
        let bindings = resolved.destinations()[0].bindings();
        assert_eq!(bindings.len(), 2);
        assert_eq!(bindings[0].variable(), &VarName::new("x"));
        assert_eq!(bindings[0].route().unwrap().address(), "/configured/x");
        assert_eq!(bindings[1].variable(), &VarName::new("y"));
        assert!(bindings[1].route().is_none());
        assert_eq!(resolved.destinations()[0].interface().len(), 2);
        assert_eq!(
            resolved.destinations()[0]
                .interface()
                .binding(&VarName::new("x"))
                .unwrap()
                .route()
                .map(Route::address),
            Some("/configured/x")
        );
    }

    #[test]
    fn resolution_is_sorted_and_partitions_without_losing_auxiliary_routes() {
        let first = OutputDestination::new("first", OutputBackendConfig::null())
            .partition([VarName::new("x")])
            .with_routes([(VarName::new("x"), route("/x"))]);
        let second = OutputDestination::new("second", OutputBackendConfig::null())
            .partition([VarName::new("y")])
            .with_routes([(VarName::new("y"), route("/y"))]);
        let resolved = pipeline(vec![second, first])
            .resolve(
                [VarName::new("x"), VarName::new("y")],
                [VarName::new("debug")],
                None,
            )
            .unwrap();

        assert_eq!(
            resolved
                .destinations()
                .iter()
                .map(|destination| destination.id.as_str())
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
        assert_eq!(resolved.destinations()[0].bindings.len(), 2);
        assert_eq!(
            resolved.destinations()[0].bindings[1].role(),
            OutputRole::Auxiliary
        );
        assert!(resolved.destinations()[0].bindings[1].route().is_none());
    }

    #[test]
    fn compact_mqtt_routes_get_the_canonical_json_codec() {
        let destination = OutputDestination::new(
            "mqtt",
            OutputBackendConfig::Mqtt {
                host: "broker".into(),
                port: Some(1883),
                retry: crate::io::RetryPolicy::output_default(),
            },
        )
        .with_routes([(VarName::new("x"), route("topic"))]);
        let resolved = pipeline(vec![destination])
            .resolve([VarName::new("x")], std::iter::empty::<VarName>(), None)
            .unwrap();
        assert_eq!(
            resolved.destinations()[0].bindings()[0]
                .route()
                .unwrap()
                .format()
                .unwrap()
                .as_str(),
            "json"
        );
    }

    #[test]
    fn duplicate_mqtt_topics_are_rejected_during_resolution() {
        let destination =
            OutputDestination::new("telemetry", OutputBackendConfig::mqtt("broker", None))
                .with_routes([
                    (VarName::new("b"), route("shared/topic")),
                    (VarName::new("a"), route("shared/topic")),
                ]);
        let error = pipeline(vec![destination])
            .resolve(
                [VarName::new("b"), VarName::new("a")],
                std::iter::empty::<VarName>(),
                None,
            )
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "output destination `telemetry` has duplicate MQTT topic `shared/topic` for variables `a` and `b`"
        );
    }

    #[test]
    fn duplicate_redis_channels_are_rejected_during_resolution() {
        let destination =
            OutputDestination::new("events", OutputBackendConfig::redis("redis", None))
                .with_routes([
                    (VarName::new("b"), route("shared/channel")),
                    (VarName::new("a"), route("shared/channel")),
                ]);
        let error = pipeline(vec![destination])
            .resolve(
                [VarName::new("b"), VarName::new("a")],
                std::iter::empty::<VarName>(),
                None,
            )
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "output destination `events` has duplicate Redis channel `shared/channel` for variables `a` and `b`"
        );
    }

    #[test]
    fn repeated_mqtt_topics_across_destinations_are_allowed() {
        let first = OutputDestination::new("first", OutputBackendConfig::mqtt("broker", None))
            .partition([VarName::new("x")])
            .with_routes([(VarName::new("x"), route("shared/topic"))]);
        let second = OutputDestination::new("second", OutputBackendConfig::mqtt("broker", None))
            .partition([VarName::new("y")])
            .with_routes([(VarName::new("y"), route("shared/topic"))]);

        let resolved = pipeline(vec![second, first])
            .resolve(
                [VarName::new("y"), VarName::new("x")],
                std::iter::empty::<VarName>(),
                None,
            )
            .unwrap();
        assert_eq!(resolved.destinations().len(), 2);
    }

    #[test]
    fn router_preserves_tick_boundaries_for_each_partition() {
        smol::block_on(async {
            let left_batches: Rc<RefCell<Vec<OutputBatch<crate::Value>>>> =
                Rc::new(RefCell::new(Vec::new()));
            let right_batches: Rc<RefCell<Vec<OutputBatch<crate::Value>>>> =
                Rc::new(RefCell::new(Vec::new()));
            let left = OutputBackendConfig::test(RecordingBackend {
                opened: Rc::new(Cell::new(0)),
                closed: Rc::new(Cell::new(0)),
                batches: Rc::clone(&left_batches),
                fail_open: false,
                fail_close: false,
            });
            let right = OutputBackendConfig::test(RecordingBackend {
                opened: Rc::new(Cell::new(0)),
                closed: Rc::new(Cell::new(0)),
                batches: Rc::clone(&right_batches),
                fail_open: false,
                fail_close: false,
            });
            let pipeline = pipeline(vec![
                OutputDestination::new("left", left).partition([VarName::new("x")]),
                OutputDestination::new("right", right).partition([VarName::new("y")]),
            ]);
            let resolved = pipeline
                .resolve(
                    [VarName::new("x"), VarName::new("y")],
                    std::iter::empty::<VarName>(),
                    None,
                )
                .unwrap();
            let mut writer = pipeline.open(resolved).await.unwrap();
            writer
                .feed(
                    OutputBatch::from_ticks(vec![
                        vec![value("x", 1), value("y", 10)],
                        vec![value("x", 2), value("y", 20)],
                    ])
                    .unwrap(),
                )
                .await
                .unwrap();
            writer.close().await.unwrap();

            let left_batches = std::cell::RefCell::borrow(left_batches.as_ref());
            let right_batches = std::cell::RefCell::borrow(right_batches.as_ref());
            assert_eq!(left_batches[0].tick_count(), 2);
            assert_eq!(right_batches[0].tick_count(), 2);
            assert_eq!(
                left_batches[0].ticks().next().unwrap().to_updates()[0].value,
                crate::Value::Int(1)
            );
        });
    }

    #[test]
    fn partial_open_closes_every_previous_destination() {
        smol::block_on(async {
            let closed = Rc::new(Cell::new(0));
            let first = OutputBackendConfig::test(RecordingBackend {
                opened: Rc::new(Cell::new(0)),
                closed: Rc::clone(&closed),
                batches: Rc::new(RefCell::new(Vec::new())),
                fail_open: false,
                fail_close: false,
            });
            let second = OutputBackendConfig::test(RecordingBackend {
                opened: Rc::new(Cell::new(0)),
                closed: Rc::clone(&closed),
                batches: Rc::new(RefCell::new(Vec::new())),
                fail_open: true,
                fail_close: false,
            });
            let pipeline = pipeline(vec![
                OutputDestination::new("first", first).partition([VarName::new("x")]),
                OutputDestination::new("second", second).mirror_all(),
            ]);
            let resolved = pipeline
                .resolve([VarName::new("x")], std::iter::empty::<VarName>(), None)
                .unwrap();
            let error = match pipeline.open(resolved).await {
                Ok(_) => panic!("second destination should fail to open"),
                Err(error) => error,
            };
            assert!(error.to_string().contains("intentional open failure"));
            assert_eq!(closed.get(), 1);
        });
    }

    #[test]
    fn destination_delivery_is_part_of_the_resolved_plan() {
        let coalesce = CoalescingLimits::new(NonZeroUsize::new(2), None, None).unwrap();
        let delivery = DeliveryPolicy::coalesce(coalesce);
        let destination =
            OutputDestination::<crate::Value>::new("only", OutputBackendConfig::null())
                .with_delivery(delivery);
        let pipeline = OutputPipeline::from_destination(destination).unwrap();
        let resolved = pipeline
            .resolve([VarName::new("x")], std::iter::empty::<VarName>(), None)
            .unwrap();
        assert_eq!(resolved.destinations()[0].delivery(), delivery);
    }

    #[test]
    fn explicit_flat_bindings_and_route_free_local_bindings_resolve() {
        let only = OutputDestination::<crate::Value>::new("only", OutputBackendConfig::null());
        let config = OutputConfiguration {
            outputs: Some(BTreeMap::from([
                (VarName::new("x"), route("/x")),
                (VarName::new("y"), route("/y")),
            ])),
            destination: Some("only".into()),
            destinations: None,
        };
        let resolved = OutputPipeline::from_destination(only)
            .unwrap()
            .resolve(
                [VarName::new("x"), VarName::new("y")],
                std::iter::empty::<VarName>(),
                Some(&config),
            )
            .unwrap();
        assert_eq!(resolved.destinations()[0].bindings().len(), 2);

        let console =
            OutputDestination::<crate::Value>::new("console", OutputBackendConfig::stdout())
                .partition([VarName::new("x")]);
        let local = OutputPipeline::from_destination(console)
            .unwrap()
            .resolve([VarName::new("x")], std::iter::empty::<VarName>(), None)
            .unwrap();
        assert!(local.destinations()[0].bindings()[0].route().is_none());
    }

    #[test]
    fn grouped_bindings_make_cross_destination_mirroring_explicit() {
        let config = OutputConfiguration {
            outputs: None,
            destination: None,
            destinations: Some(BTreeMap::from([
                (
                    "first".into(),
                    BTreeMap::from([(VarName::new("x"), route("/first/x"))]),
                ),
                (
                    "second".into(),
                    BTreeMap::from([
                        (VarName::new("x"), route("/second/x")),
                        (VarName::new("y"), route("/second/y")),
                    ]),
                ),
            ])),
        };
        let pipeline = pipeline(vec![
            OutputDestination::new("first", OutputBackendConfig::null()),
            OutputDestination::new("second", OutputBackendConfig::null()),
        ]);
        let resolved = pipeline
            .resolve(
                [VarName::new("x"), VarName::new("y")],
                std::iter::empty::<VarName>(),
                Some(&config),
            )
            .unwrap();
        assert_eq!(resolved.destinations()[0].bindings().len(), 1);
        assert_eq!(resolved.destinations()[1].bindings().len(), 2);
        assert_eq!(
            resolved,
            pipeline
                .resolve(
                    [VarName::new("x"), VarName::new("y")],
                    std::iter::empty::<VarName>(),
                    Some(&config),
                )
                .unwrap()
        );
    }

    #[test]
    fn router_does_not_emit_empty_ticks_to_unselected_destinations() {
        smol::block_on(async {
            let left_batches = Rc::new(RefCell::new(Vec::new()));
            let right_batches = Rc::new(RefCell::new(Vec::new()));
            let left = OutputBackendConfig::test(RecordingBackend {
                opened: Rc::new(Cell::new(0)),
                closed: Rc::new(Cell::new(0)),
                batches: Rc::clone(&left_batches),
                fail_open: false,
                fail_close: false,
            });
            let right = OutputBackendConfig::test(RecordingBackend {
                opened: Rc::new(Cell::new(0)),
                closed: Rc::new(Cell::new(0)),
                batches: Rc::clone(&right_batches),
                fail_open: false,
                fail_close: false,
            });
            let pipeline = pipeline(vec![
                OutputDestination::new("left", left).partition([VarName::new("x")]),
                OutputDestination::new("right", right).partition([VarName::new("y")]),
            ]);
            let resolved = pipeline
                .resolve(
                    [VarName::new("x"), VarName::new("y")],
                    std::iter::empty::<VarName>(),
                    None,
                )
                .unwrap();
            let mut writer = pipeline.open(resolved).await.unwrap();
            writer
                .send(OutputBatch::update(VarName::new("x"), crate::Value::Int(1)))
                .await
                .unwrap();
            writer.close().await.unwrap();
            assert_eq!(std::cell::RefCell::borrow(left_batches.as_ref()).len(), 1);
            assert!(std::cell::RefCell::borrow(right_batches.as_ref()).is_empty());
        });
    }

    #[test]
    fn mirrors_cannot_claim_primary_ownership_from_catalogs() {
        let mirror = OutputDestination::new("mirror", OutputBackendConfig::null())
            .with_routes([(VarName::new("x"), route("/mirror/x"))])
            .mirror_all();
        let error = pipeline(vec![mirror.clone()])
            .resolve([VarName::new("x")], std::iter::empty::<VarName>(), None)
            .unwrap_err();
        assert!(error.to_string().contains("no destination owner"));

        let listed_mirror = OutputDestination::new("listed", OutputBackendConfig::null())
            .with_routes([(VarName::new("x"), route("/listed/x"))])
            .mirror([VarName::new("x")]);
        assert!(
            pipeline(vec![listed_mirror])
                .resolve([VarName::new("x")], std::iter::empty::<VarName>(), None)
                .is_err()
        );

        let output_configuration = OutputConfiguration {
            outputs: Some(BTreeMap::from([(VarName::new("x"), route("/monitor/x"))])),
            destination: Some("mirror".into()),
            destinations: None,
        };
        assert!(
            pipeline(vec![
                OutputDestination::new("mirror", OutputBackendConfig::null(),).mirror_all()
            ])
            .resolve(
                [VarName::new("x")],
                std::iter::empty::<VarName>(),
                Some(&output_configuration),
            )
            .is_err()
        );

        let primary = OutputDestination::new("primary", OutputBackendConfig::null());
        let resolved = pipeline(vec![mirror, primary])
            .resolve([VarName::new("x")], std::iter::empty::<VarName>(), None)
            .unwrap();
        assert!(
            !resolved
                .destination(&"mirror".to_owned())
                .unwrap()
                .is_primary()
        );
        assert!(
            resolved
                .destination(&"primary".to_owned())
                .unwrap()
                .is_primary()
        );
        assert_eq!(
            resolved
                .destination(&"mirror".to_owned())
                .unwrap()
                .bindings()[0]
                .route()
                .unwrap()
                .address(),
            "/mirror/x"
        );
    }

    #[test]
    fn destination_registry_construction_rejects_invalid_ids() {
        let duplicate =
            OutputDestination::<crate::Value>::new("duplicate", OutputBackendConfig::null());
        let error = OutputDestinations::new([duplicate.clone(), duplicate]).unwrap_err();
        assert!(error.to_string().contains("declared more than once"));

        let error = OutputDestinations::new([OutputDestination::<crate::Value>::new(
            "",
            OutputBackendConfig::null(),
        )])
        .unwrap_err();
        assert!(error.to_string().contains("cannot be empty"));

        let error = OutputDestinations::single(OutputDestination::<crate::Value>::new(
            "",
            OutputBackendConfig::null(),
        ))
        .unwrap_err();
        assert!(error.to_string().contains("cannot be empty"));

        let error = <OutputDestinations<crate::Value> as TryFrom<Vec<_>>>::try_from(vec![
            OutputDestination::new("duplicate", OutputBackendConfig::null()),
            OutputDestination::new("duplicate", OutputBackendConfig::null()),
        ])
        .unwrap_err();
        assert!(error.to_string().contains("declared more than once"));

        assert!(
            OutputPipeline::from_destination(OutputDestination::<crate::Value>::new(
                "",
                OutputBackendConfig::null(),
            ))
            .is_err()
        );
    }

    #[test]
    fn partial_open_combines_destination_cleanup_errors() {
        smol::block_on(async {
            let closed = Rc::new(Cell::new(0));
            let first = OutputBackendConfig::test(RecordingBackend {
                opened: Rc::new(Cell::new(0)),
                closed: Rc::clone(&closed),
                batches: Rc::new(RefCell::new(Vec::new())),
                fail_open: false,
                fail_close: true,
            });
            let second = OutputBackendConfig::test(RecordingBackend {
                opened: Rc::new(Cell::new(0)),
                closed: Rc::clone(&closed),
                batches: Rc::new(RefCell::new(Vec::new())),
                fail_open: true,
                fail_close: false,
            });
            let pipeline = pipeline(vec![
                OutputDestination::new("first", first).partition([VarName::new("x")]),
                OutputDestination::new("second", second).mirror_all(),
            ]);
            let resolved = pipeline
                .resolve([VarName::new("x")], std::iter::empty::<VarName>(), None)
                .unwrap();
            let error = match pipeline.open(resolved).await {
                Ok(_) => panic!("second destination should fail to open"),
                Err(error) => error,
            };
            assert!(error.to_string().contains("intentional open failure"));
            assert!(error.to_string().contains("intentional close failure"));
            assert_eq!(closed.get(), 1);
        });
    }

    #[test]
    fn resolved_output_is_bound_to_its_pipeline_configuration() {
        smol::block_on(async {
            let opened_elsewhere = Rc::new(Cell::new(0));
            let first = pipeline(vec![
                OutputDestination::new(
                    "out",
                    OutputBackendConfig::test(RecordingBackend {
                        opened: Rc::new(Cell::new(0)),
                        closed: Rc::new(Cell::new(0)),
                        batches: Rc::new(RefCell::new(Vec::new())),
                        fail_open: false,
                        fail_close: false,
                    }),
                )
                .with_routes([(VarName::new("x"), route("/first"))]),
            ]);
            let second = pipeline(vec![
                OutputDestination::new(
                    "out",
                    OutputBackendConfig::test(RecordingBackend {
                        opened: Rc::clone(&opened_elsewhere),
                        closed: Rc::new(Cell::new(0)),
                        batches: Rc::new(RefCell::new(Vec::new())),
                        fail_open: false,
                        fail_close: false,
                    }),
                )
                .with_routes([(VarName::new("x"), route("/second"))]),
            ]);
            let resolved = first
                .resolve([VarName::new("x")], std::iter::empty::<VarName>(), None)
                .unwrap();
            let error = match second.open(resolved).await {
                Ok(_) => panic!("a resolution from another pipeline must be rejected"),
                Err(error) => error,
            };
            assert!(error.to_string().contains("durable configuration"));
            assert_eq!(opened_elsewhere.get(), 0);
        });

        smol::block_on(async {
            let pipeline = pipeline(vec![OutputDestination::new(
                "out",
                OutputBackendConfig::null(),
            )]);
            let first = pipeline
                .resolve([VarName::new("x")], std::iter::empty::<VarName>(), None)
                .unwrap();
            let second = pipeline
                .resolve([VarName::new("y")], [VarName::new("debug")], None)
                .unwrap();
            assert_ne!(first, second);
            pipeline.open(first).await.unwrap().close().await.unwrap();
            pipeline.open(second).await.unwrap().close().await.unwrap();
        });
    }

    #[test]
    fn delivery_worker_preserves_interface_reconfiguration() {
        smol::block_on(async {
            let interface = OutputInterface::outputs([VarName::new("x")]).unwrap();
            let backend = ReconfigurableRecordingBackend::new();
            let executor = Rc::new(LocalExecutor::new());
            let writer = backend.open(interface).await.unwrap();
            let policy = DeliveryPolicy::coalesce(
                CoalescingLimits::new(NonZeroUsize::new(2), None, None).unwrap(),
            )
            .with_queue(QueueLimits::new(NonZeroUsize::new(1).unwrap(), None));
            let mut writer = Delivery::new(writer, Some(Rc::clone(&executor)), policy)
                .unwrap()
                .into_writer();
            executor
                .run(async {
                    writer
                        .rebind(OutputInterface::outputs([VarName::new("x")]).unwrap())
                        .await
                        .unwrap();
                    writer.close().await.unwrap()
                })
                .await;
        });
    }

    #[test]
    fn reconfiguration_updates_only_changed_destination_in_place() {
        smol::block_on(async {
            let first_backend = ReconfigurableRecordingBackend::new();
            let second_backend = ReconfigurableRecordingBackend::new();
            let pipeline = pipeline(vec![
                OutputDestination::new("first", OutputBackendConfig::test(first_backend.clone()))
                    .partition([VarName::new("x")])
                    .with_routes([(VarName::new("x"), route("/old"))]),
                OutputDestination::new("second", OutputBackendConfig::test(second_backend.clone()))
                    .partition([VarName::new("y")])
                    .with_routes([(VarName::new("y"), route("/same"))]),
            ]);
            let active = pipeline
                .resolve(
                    [VarName::new("x"), VarName::new("y")],
                    std::iter::empty::<VarName>(),
                    None,
                )
                .unwrap();
            let candidate_configuration = OutputConfiguration {
                outputs: None,
                destination: None,
                destinations: Some(BTreeMap::from([
                    (
                        "first".into(),
                        BTreeMap::from([(VarName::new("x"), route("/new"))]),
                    ),
                    (
                        "second".into(),
                        BTreeMap::from([(VarName::new("y"), route("/same"))]),
                    ),
                ])),
            };
            let candidate = pipeline
                .resolve(
                    [VarName::new("x"), VarName::new("y")],
                    std::iter::empty::<VarName>(),
                    Some(&candidate_configuration),
                )
                .unwrap();
            let mut session = pipeline.open_session(active).await.unwrap();
            let plan = pipeline
                .plan_reconfiguration(
                    session.resolved(),
                    candidate,
                    session.session_id(),
                    session.revision(),
                )
                .unwrap();
            assert_eq!(plan.changed_destinations().len(), 1);
            assert_eq!(plan.session(), session.session_id());
            assert_eq!(plan.expected_revision(), SessionRevision::initial());
            session.apply_reconfiguration(plan).await.unwrap();
            assert_eq!(session.revision(), SessionRevision::initial());
            session.commit_revision(SessionRevision::initial()).unwrap();
            assert_eq!(session.revision(), SessionRevision::initial().next());

            assert_eq!(first_backend.opened.get(), 1);
            assert_eq!(first_backend.closed.get(), 0);
            assert_eq!(first_backend.flushes.get(), 1);
            assert_eq!(first_backend.interface_updates.get(), 1);
            assert_eq!(second_backend.opened.get(), 1);
            assert_eq!(second_backend.closed.get(), 0);
            assert_eq!(second_backend.flushes.get(), 0);
            assert_eq!(second_backend.interface_updates.get(), 0);

            session.close().await.unwrap();
            assert_eq!(first_backend.closed.get(), 1);
            assert_eq!(second_backend.closed.get(), 1);
        });
    }

    #[test]
    fn flat_monitor_bindings_override_unused_local_catalogs() {
        let selected = OutputDestination::new("selected", OutputBackendConfig::null())
            .with_routes([(VarName::new("x"), route("/local/x"))]);
        let unused = OutputDestination::new("unused", OutputBackendConfig::null())
            .with_routes([(VarName::new("x"), route("/stale/x"))]);
        let config = OutputConfiguration {
            outputs: Some(BTreeMap::from([(VarName::new("x"), route("/monitor/x"))])),
            destination: Some("selected".into()),
            destinations: None,
        };
        let resolved = pipeline(vec![unused, selected])
            .resolve(
                [VarName::new("x")],
                std::iter::empty::<VarName>(),
                Some(&config),
            )
            .unwrap();
        assert!(
            resolved
                .destination(&"unused".to_owned())
                .unwrap()
                .bindings()
                .is_empty()
        );
        assert_eq!(
            resolved
                .destination(&"selected".to_owned())
                .unwrap()
                .bindings()[0]
                .route()
                .unwrap()
                .address(),
            "/monitor/x"
        );
    }

    #[test]
    fn grouped_monitor_bindings_override_unused_local_catalogs() {
        let unused = OutputDestination::new("unused", OutputBackendConfig::null())
            .with_routes([(VarName::new("x"), route("/stale/x"))]);
        let first = OutputDestination::new("first", OutputBackendConfig::null())
            .with_routes([(VarName::new("x"), route("/local/x"))]);
        let second = OutputDestination::new("second", OutputBackendConfig::null())
            .with_routes([(VarName::new("y"), route("/local/y"))]);
        let config = OutputConfiguration {
            outputs: None,
            destination: None,
            destinations: Some(BTreeMap::from([
                (
                    "first".into(),
                    BTreeMap::from([(VarName::new("x"), route("/monitor/x"))]),
                ),
                (
                    "second".into(),
                    BTreeMap::from([(VarName::new("y"), route("/monitor/y"))]),
                ),
            ])),
        };
        let resolved = pipeline(vec![unused, first, second])
            .resolve(
                [VarName::new("x"), VarName::new("y")],
                std::iter::empty::<VarName>(),
                Some(&config),
            )
            .unwrap();
        assert!(
            resolved
                .destination(&"unused".to_owned())
                .unwrap()
                .bindings()
                .is_empty()
        );
        assert_eq!(
            resolved
                .destination(&"first".to_owned())
                .unwrap()
                .bindings()[0]
                .route()
                .unwrap()
                .address(),
            "/monitor/x"
        );
        assert_eq!(
            resolved
                .destination(&"second".to_owned())
                .unwrap()
                .bindings()[0]
                .route()
                .unwrap()
                .address(),
            "/monitor/y"
        );
    }

    #[test]
    fn router_poll_ready_applies_global_backpressure() {
        let blocked = Rc::new(Cell::new(false));
        let already_ready = Rc::new(Cell::new(true));
        let mut router = OutputRouter::new(vec![
            OpenedDestination {
                id: "left".into(),
                variables: BTreeSet::from([VarName::new("x")]),
                cancellation: None,
                writer: OutputWriter::from_sink(GateSink {
                    gate: Rc::clone(&already_ready),
                    ready: false,
                }),
            },
            OpenedDestination {
                id: "right".into(),
                variables: BTreeSet::new(),
                cancellation: None,
                writer: OutputWriter::from_sink(GateSink {
                    gate: Rc::clone(&blocked),
                    ready: false,
                }),
            },
        ]);
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(matches!(
            Pin::new(&mut router).poll_ready(&mut context),
            Poll::Pending
        ));
        blocked.set(true);
        assert!(matches!(
            Pin::new(&mut router).poll_ready(&mut context),
            Poll::Ready(Ok(()))
        ));
    }

    #[test]
    fn single_destination_router_moves_the_original_batch() {
        smol::block_on(async {
            let clones = Rc::new(Cell::new(0));
            let batches: Rc<RefCell<Vec<OutputBatch<CloneCountingValue>>>> =
                Rc::new(RefCell::new(Vec::new()));
            let destination = OutputWriter::from_sink(GenericRecordingSink {
                closed: Rc::new(Cell::new(0)),
                batches: Rc::clone(&batches),
                ready: false,
            });
            let mut writer = OutputWriter::from_sink(OutputRouter::new(vec![OpenedDestination {
                id: "only".into(),
                variables: BTreeSet::from([VarName::new("x")]),
                cancellation: None,
                writer: destination,
            }]));
            let batch = OutputBatch::tick(vec![OutputUpdate::new(
                VarName::new("x"),
                CloneCountingValue {
                    clones: Rc::clone(&clones),
                    value: 1,
                },
            )])
            .unwrap();
            writer.send(batch).await.unwrap();
            assert_eq!(clones.get(), 0);
            writer.close().await.unwrap();
            assert_eq!(RefCell::borrow(batches.as_ref()).len(), 1);
        });
    }

    #[test]
    fn router_clones_each_value_once_per_destination_delivery() {
        smol::block_on(async {
            let clones = Rc::new(Cell::new(0));
            let left_batches: Rc<RefCell<Vec<OutputBatch<CloneCountingValue>>>> =
                Rc::new(RefCell::new(Vec::new()));
            let right_batches: Rc<RefCell<Vec<OutputBatch<CloneCountingValue>>>> =
                Rc::new(RefCell::new(Vec::new()));
            let left = OutputWriter::from_sink(GenericRecordingSink {
                closed: Rc::new(Cell::new(0)),
                batches: Rc::clone(&left_batches),
                ready: false,
            });
            let right = OutputWriter::from_sink(GenericRecordingSink {
                closed: Rc::new(Cell::new(0)),
                batches: Rc::clone(&right_batches),
                ready: false,
            });
            let mut writer = OutputWriter::from_sink(OutputRouter::new(vec![
                OpenedDestination {
                    id: "left".into(),
                    variables: BTreeSet::from([VarName::new("x")]),
                    cancellation: None,
                    writer: left,
                },
                OpenedDestination {
                    id: "right".into(),
                    variables: BTreeSet::from([VarName::new("x")]),
                    cancellation: None,
                    writer: right,
                },
            ]));
            let batch = OutputBatch::tick(vec![OutputUpdate::new(
                VarName::new("x"),
                CloneCountingValue {
                    clones: Rc::clone(&clones),
                    value: 1,
                },
            )])
            .unwrap();
            writer.send(batch).await.unwrap();
            assert_eq!(clones.get(), 2);
            writer.close().await.unwrap();
            assert_eq!(RefCell::borrow(left_batches.as_ref()).len(), 1);
            assert_eq!(RefCell::borrow(right_batches.as_ref()).len(), 1);
        });
    }
}
