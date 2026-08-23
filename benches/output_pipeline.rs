#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    hint::black_box,
    rc::Rc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use smol::LocalExecutor;
use trustworthiness_checker::core::{OutputBackend, OutputError, OutputInterface};
use trustworthiness_checker::io::{
    OutputBackendBuilder, OutputBackendConfig, OutputDestination, OutputDestinations,
    OutputPipeline, OutputStage, ResolvedOutput,
};
use trustworthiness_checker::{OutputBatch, OutputUpdate, OutputWriter, Value, VarName};

#[derive(Clone, Copy, Debug)]
struct SyntheticCost {
    /// Deterministic arithmetic iterations once per backend physical batch.
    per_batch_work: usize,
    /// Deterministic arithmetic iterations once per update in that batch.
    per_update_work: usize,
    /// One bounded async delay once per backend physical batch.
    delay: Duration,
}

impl SyntheticCost {
    fn new(delay: Duration, per_batch_work: usize, per_update_work: usize) -> Self {
        Self {
            per_batch_work,
            per_update_work,
            delay,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct DestinationStats {
    /// The key is an ID assigned when the backend starts observing a physical
    /// batch. Values retain the logical tick IDs carried by that physical batch.
    physical_logical_ids: BTreeMap<usize, Vec<usize>>,
    completed_physical_batches: usize,
    started_ticks: usize,
    started_updates: usize,
    completed_ticks: usize,
    completed_updates: usize,
    started_ids: BTreeSet<usize>,
    completed_ids: BTreeSet<usize>,
    backend_start_latency: Vec<Duration>,
    backend_completion_latency: Vec<Duration>,
}

#[derive(Clone, Debug, Default)]
struct BenchStats {
    submitted_physical_batches: usize,
    submitted_ticks: usize,
    submitted_updates: usize,
    admitted_physical_batches: usize,
    admitted_ticks: usize,
    admitted_updates: usize,
    /// These timestamps are per logical input tick, not per physical batch.
    submitted_at: BTreeMap<usize, Instant>,
    admitted_at: BTreeMap<usize, Instant>,
    /// A per-tick copy of the physical feed/admission wait. This is a producer
    /// backpressure proxy; it is not an observation of an internal queue.
    admission_wait: Vec<Duration>,
    destinations: BTreeMap<String, DestinationStats>,
}

impl BenchStats {
    fn register_destination(&mut self, id: &str) {
        self.destinations.entry(id.to_owned()).or_default();
    }

    fn record_submission(&mut self, batch: &OutputBatch<Value>, started: Instant) -> Vec<usize> {
        let ids = logical_ids(batch);
        assert_eq!(
            ids.len(),
            batch.tick_count(),
            "every submitted logical tick must carry exactly one logical ID"
        );
        self.submitted_physical_batches += 1;
        self.submitted_ticks += batch.tick_count();
        self.submitted_updates += batch.update_count();
        for id in &ids {
            assert!(
                self.submitted_at.insert(*id, started).is_none(),
                "logical tick {id} was submitted more than once"
            );
        }
        ids
    }

    fn record_admission(&mut self, ids: &[usize], updates: usize, started: Instant) {
        let admitted = Instant::now();
        self.admitted_physical_batches += 1;
        self.admitted_ticks += ids.len();
        self.admitted_updates += updates;
        for id in ids {
            assert!(
                self.submitted_at.contains_key(id),
                "logical tick {id} was admitted without a submission"
            );
            assert!(
                self.admitted_at.insert(*id, admitted).is_none(),
                "logical tick {id} was admitted more than once"
            );
            self.admission_wait
                .push(admitted.saturating_duration_since(started));
        }
    }

    fn record_backend_start(
        &mut self,
        destination: &str,
        batch: &OutputBatch<Value>,
        ids: &[usize],
        started: Instant,
    ) {
        assert_eq!(
            ids.len(),
            batch.tick_count(),
            "backend physical batch tick count must match its logical IDs"
        );
        let latencies =
            ids.iter()
                .map(|id| {
                    let submitted =
                        self.submitted_at.get(id).copied().unwrap_or_else(|| {
                            panic!("backend observed unknown logical tick {id}")
                        });
                    started.saturating_duration_since(submitted)
                })
                .collect::<Vec<_>>();

        let stats = self
            .destinations
            .get_mut(destination)
            .unwrap_or_else(|| panic!("backend used unregistered destination {destination}"));
        let physical_id = stats.physical_logical_ids.len();
        assert!(
            stats
                .physical_logical_ids
                .insert(physical_id, ids.to_vec())
                .is_none()
        );
        stats.started_ticks += batch.tick_count();
        stats.started_updates += batch.update_count();
        for (id, latency) in ids.iter().zip(latencies) {
            assert!(
                stats.started_ids.insert(*id),
                "destination {destination} started logical tick {id} more than once"
            );
            stats.backend_start_latency.push(latency);
        }
    }

    fn record_backend_completion(
        &mut self,
        destination: &str,
        batch: &OutputBatch<Value>,
        ids: &[usize],
        completed: Instant,
    ) {
        let latencies =
            ids.iter()
                .map(|id| {
                    let submitted =
                        self.submitted_at.get(id).copied().unwrap_or_else(|| {
                            panic!("backend completed unknown logical tick {id}")
                        });
                    completed.saturating_duration_since(submitted)
                })
                .collect::<Vec<_>>();
        let stats = self
            .destinations
            .get_mut(destination)
            .unwrap_or_else(|| panic!("backend used unregistered destination {destination}"));
        assert!(
            stats.completed_physical_batches < stats.physical_logical_ids.len(),
            "a backend physical batch completed before it started"
        );
        stats.completed_physical_batches += 1;
        stats.completed_ticks += batch.tick_count();
        stats.completed_updates += batch.update_count();
        for (id, latency) in ids.iter().zip(latencies) {
            assert!(
                stats.started_ids.contains(id),
                "destination {destination} completed logical tick {id} before starting it"
            );
            assert!(
                stats.completed_ids.insert(*id),
                "destination {destination} completed logical tick {id} more than once"
            );
            stats.backend_completion_latency.push(latency);
        }
    }
}

#[derive(Clone)]
struct SyntheticBackend {
    stats: Rc<RefCell<BenchStats>>,
    destination: String,
    cost: SyntheticCost,
    fail: bool,
}

impl SyntheticBackend {
    fn new(
        stats: Rc<RefCell<BenchStats>>,
        destination: impl Into<String>,
        cost: SyntheticCost,
    ) -> Self {
        Self {
            stats,
            destination: destination.into(),
            cost,
            fail: false,
        }
    }

    fn failing(stats: Rc<RefCell<BenchStats>>, destination: impl Into<String>) -> Self {
        Self {
            stats,
            destination: destination.into(),
            cost: SyntheticCost::new(Duration::ZERO, 0, 0),
            fail: true,
        }
    }
}

#[async_trait(?Send)]
impl OutputBackend for SyntheticBackend {
    type Val = Value;

    async fn open(
        &self,
        interface: OutputInterface,
    ) -> Result<OutputWriter<Self::Val>, OutputError> {
        let stats = Rc::clone(&self.stats);
        let destination = self.destination.clone();
        let cost = self.cost;
        let fail = self.fail;
        let interface = Rc::new(interface);
        Ok(OutputWriter::from_sink(
            trustworthiness_checker::io::output::local_batch_sink(
                move |batch: OutputBatch<Value>| {
                    let stats = Rc::clone(&stats);
                    let destination = destination.clone();
                    let interface = Rc::clone(&interface);
                    async move {
                        let ids = logical_ids(&batch);
                        let backend_started = Instant::now();
                        stats.borrow_mut().record_backend_start(
                            &destination,
                            &batch,
                            &ids,
                            backend_started,
                        );

                        interface.validate_batch(&batch)?;
                        synthetic_cpu_work(cost.per_batch_work, 0);
                        if cost.per_update_work > 0 {
                            for (index, _) in batch.updates().enumerate() {
                                synthetic_cpu_work(cost.per_update_work, index as u64);
                            }
                        }
                        if fail {
                            return Err(OutputError::backend("synthetic destination failure"));
                        }
                        if !cost.delay.is_zero() {
                            smol::Timer::after(cost.delay).await;
                        }

                        stats.borrow_mut().record_backend_completion(
                            &destination,
                            &batch,
                            &ids,
                            Instant::now(),
                        );
                        Ok(())
                    }
                },
            ),
        ))
    }
}

fn synthetic_cpu_work(units: usize, seed: u64) {
    let mut accumulator = seed;
    for unit in 0..units {
        accumulator = accumulator
            .wrapping_mul(31)
            .wrapping_add((unit as u64).wrapping_add(seed));
    }
    black_box(accumulator);
}

#[derive(Clone, Debug, Default)]
struct DeliveryExpectation {
    logical_ids: BTreeSet<usize>,
    updates: usize,
}

#[derive(Clone, Debug)]
struct CaseExpectations {
    input_physical_batches: usize,
    input_ticks: usize,
    input_updates: usize,
    input_ids: BTreeSet<usize>,
    destinations: BTreeMap<String, DeliveryExpectation>,
}

struct PreparedCase {
    writer: OutputWriter<Value>,
    executor: Option<Rc<LocalExecutor<'static>>>,
    batches: Vec<OutputBatch<Value>>,
    stats: Rc<RefCell<BenchStats>>,
    expectations: CaseExpectations,
}

#[derive(Clone, Debug, Default)]
struct LatencySummary {
    p50: Option<Duration>,
    p95: Option<Duration>,
    p99: Option<Duration>,
    max: Option<Duration>,
}

#[derive(Clone, Debug, Default)]
struct DestinationResult {
    physical_batches: usize,
    completed_physical_batches: usize,
    logical_ticks: usize,
    updates: usize,
    backend_start: LatencySummary,
    backend_completion: LatencySummary,
}

#[derive(Clone, Debug, Default)]
struct BenchResult {
    elapsed: Duration,
    input_physical_batches: usize,
    input_ticks: usize,
    input_updates: usize,
    admitted_physical_batches: usize,
    admission_wait: LatencySummary,
    destinations: BTreeMap<String, DestinationResult>,
    flush_latency: Duration,
    close_latency: Duration,
}

fn quantile(values: &[Duration], numerator: usize, denominator: usize) -> Option<Duration> {
    if values.is_empty() {
        return None;
    }
    let mut values = values.to_vec();
    values.sort_unstable();
    let index = ((values.len() - 1) * numerator) / denominator;
    values.get(index).copied()
}

fn latency_summary(values: &[Duration]) -> LatencySummary {
    LatencySummary {
        p50: quantile(values, 50, 100),
        p95: quantile(values, 95, 100),
        p99: quantile(values, 99, 100),
        max: values.iter().copied().max(),
    }
}

fn finish_result(
    started: Instant,
    stats: Rc<RefCell<BenchStats>>,
    expectations: &CaseExpectations,
    flush_latency: Duration,
    close_latency: Duration,
) -> BenchResult {
    let stats = stats.borrow();
    assert_run_invariants(&stats, expectations);
    let destinations = stats
        .destinations
        .iter()
        .map(|(id, destination)| {
            (
                id.clone(),
                DestinationResult {
                    physical_batches: destination.physical_logical_ids.len(),
                    completed_physical_batches: destination.completed_physical_batches,
                    logical_ticks: destination.completed_ticks,
                    updates: destination.completed_updates,
                    backend_start: latency_summary(&destination.backend_start_latency),
                    backend_completion: latency_summary(&destination.backend_completion_latency),
                },
            )
        })
        .collect();
    BenchResult {
        elapsed: started.elapsed(),
        input_physical_batches: stats.submitted_physical_batches,
        input_ticks: stats.submitted_ticks,
        input_updates: stats.submitted_updates,
        admitted_physical_batches: stats.admitted_physical_batches,
        admission_wait: latency_summary(&stats.admission_wait),
        destinations,
        flush_latency,
        close_latency,
    }
}

fn assert_run_invariants(stats: &BenchStats, expectations: &CaseExpectations) {
    assert_eq!(
        stats.submitted_physical_batches, expectations.input_physical_batches,
        "submitted physical batch count changed during execution"
    );
    assert_eq!(
        stats.admitted_physical_batches,
        expectations.input_physical_batches
    );
    assert_eq!(stats.submitted_ticks, expectations.input_ticks);
    assert_eq!(stats.admitted_ticks, expectations.input_ticks);
    assert_eq!(stats.submitted_updates, expectations.input_updates);
    assert_eq!(stats.admitted_updates, expectations.input_updates);
    assert_eq!(
        stats.submitted_at.keys().copied().collect::<BTreeSet<_>>(),
        expectations.input_ids
    );
    assert_eq!(
        stats.admitted_at.keys().copied().collect::<BTreeSet<_>>(),
        expectations.input_ids
    );
    assert_eq!(stats.admission_wait.len(), expectations.input_ticks);

    assert_eq!(
        stats.destinations.keys().collect::<BTreeSet<_>>(),
        expectations.destinations.keys().collect::<BTreeSet<_>>(),
        "backend destination accounting must match resolved destinations"
    );
    for (id, expected) in &expectations.destinations {
        let actual = stats
            .destinations
            .get(id)
            .unwrap_or_else(|| panic!("missing destination statistics for {id}"));
        assert_eq!(
            actual.started_ids, expected.logical_ids,
            "started IDs for {id}"
        );
        assert_eq!(
            actual.completed_ids, expected.logical_ids,
            "completed IDs for {id}"
        );
        assert_eq!(actual.started_ticks, expected.logical_ids.len());
        assert_eq!(actual.completed_ticks, expected.logical_ids.len());
        assert_eq!(actual.started_updates, expected.updates);
        assert_eq!(actual.completed_updates, expected.updates);
        assert_eq!(
            actual.completed_physical_batches,
            actual.physical_logical_ids.len(),
            "every started physical batch for {id} must complete"
        );
        assert_eq!(
            actual.backend_start_latency.len(),
            expected.logical_ids.len(),
            "backend start latency must be sampled per logical tick for {id}"
        );
        assert_eq!(
            actual.backend_completion_latency.len(),
            expected.logical_ids.len(),
            "backend completion latency must be sampled per logical tick for {id}"
        );
        let physical_ids = actual
            .physical_logical_ids
            .values()
            .flat_map(|ids| ids.iter().copied())
            .collect::<BTreeSet<_>>();
        assert_eq!(physical_ids, expected.logical_ids);
    }
}

fn consume_latency_summary(summary: LatencySummary) {
    black_box((summary.p50, summary.p95, summary.p99, summary.max));
}

fn consume_result(result: BenchResult) -> Duration {
    let BenchResult {
        elapsed,
        input_physical_batches,
        input_ticks,
        input_updates,
        admitted_physical_batches,
        admission_wait,
        destinations,
        flush_latency,
        close_latency,
    } = result;
    black_box((
        input_physical_batches,
        input_ticks,
        input_updates,
        admitted_physical_batches,
        flush_latency,
        close_latency,
    ));
    consume_latency_summary(admission_wait);
    for (id, destination) in destinations {
        let DestinationResult {
            physical_batches,
            completed_physical_batches,
            logical_ticks,
            updates,
            backend_start,
            backend_completion,
        } = destination;
        black_box((
            id,
            physical_batches,
            completed_physical_batches,
            logical_ticks,
            updates,
        ));
        consume_latency_summary(backend_start);
        consume_latency_summary(backend_completion);
    }
    elapsed
}

fn run_prepared(prepared: PreparedCase) -> Duration {
    let started = Instant::now();
    let PreparedCase {
        writer,
        executor,
        batches,
        stats,
        expectations,
    } = prepared;
    let operation_stats = Rc::clone(&stats);
    let operation = async move {
        let mut writer = writer;
        for batch in batches {
            let submission_started = Instant::now();
            let (ids, updates) = {
                let mut stats = operation_stats.borrow_mut();
                let ids = stats.record_submission(&batch, submission_started);
                (ids, batch.update_count())
            };
            writer.feed(batch).await?;
            operation_stats
                .borrow_mut()
                .record_admission(&ids, updates, submission_started);
        }
        let flush_started = Instant::now();
        writer.flush().await?;
        let flush_latency = flush_started.elapsed();
        let close_started = Instant::now();
        writer.close().await?;
        let close_latency = close_started.elapsed();
        Ok::<_, OutputError>((flush_latency, close_latency))
    };
    let (flush_latency, close_latency) = match executor {
        Some(executor) => {
            smol::block_on(executor.run(operation)).expect("benchmark pipeline failed")
        }
        None => smol::block_on(operation).expect("benchmark direct writer failed"),
    };
    consume_result(finish_result(
        started,
        stats,
        &expectations,
        flush_latency,
        close_latency,
    ))
}

fn run_failure_case(prepared: PreparedCase) -> Duration {
    let started = Instant::now();
    let PreparedCase {
        writer,
        executor,
        batches,
        stats,
        ..
    } = prepared;
    let batch = batches
        .into_iter()
        .next()
        .expect("failure benchmark has one batch");
    let operation = async move {
        let mut writer = writer;
        let submission_started = Instant::now();
        let (ids, updates) = {
            let mut stats = stats.borrow_mut();
            let ids = stats.record_submission(&batch, submission_started);
            (ids, batch.update_count())
        };
        if writer.feed(batch).await.is_ok() {
            stats
                .borrow_mut()
                .record_admission(&ids, updates, submission_started);
        }
        let close_started = Instant::now();
        let _ = writer.close().await;
        close_started.elapsed()
    };
    let close_latency = match executor {
        Some(executor) => smol::block_on(executor.run(operation)),
        None => smol::block_on(operation),
    };
    black_box(close_latency);
    started.elapsed()
}

fn value(index: usize) -> Value {
    Value::Int(index as i64)
}

fn logical_id(value: &Value) -> usize {
    match value {
        Value::Int(id) if *id >= 0 => {
            usize::try_from(*id).expect("synthetic logical ID must fit in usize")
        }
        _ => panic!("synthetic benchmark value must carry a non-negative integer ID"),
    }
}

fn logical_ids(batch: &OutputBatch<Value>) -> Vec<usize> {
    batch
        .ticks()
        .map(|tick| {
            let mut id = None;
            for update in tick.updates() {
                let update_id = logical_id(update.value);
                if let Some(previous) = id {
                    assert_eq!(
                        previous, update_id,
                        "all updates in one logical tick must carry the same ID"
                    );
                } else {
                    id = Some(update_id);
                }
            }
            id.expect("a non-empty logical tick must carry an ID")
        })
        .collect()
}

fn input_counts(batches: &[OutputBatch<Value>]) -> (usize, usize, BTreeSet<usize>) {
    let mut ticks = 0;
    let mut updates = 0;
    let mut ids = BTreeSet::new();
    for batch in batches {
        let batch_ids = logical_ids(batch);
        assert_eq!(batch_ids.len(), batch.tick_count());
        ticks += batch.tick_count();
        updates += batch.update_count();
        for id in batch_ids {
            assert!(ids.insert(id), "logical tick {id} occurs more than once");
        }
    }
    (ticks, updates, ids)
}

fn delivery_for_variables(
    batches: &[OutputBatch<Value>],
    variables: &BTreeSet<VarName>,
) -> DeliveryExpectation {
    let mut delivery = DeliveryExpectation::default();
    for batch in batches {
        let ids = logical_ids(batch);
        for (id, tick) in ids.into_iter().zip(batch.ticks()) {
            let selected_updates = tick
                .updates()
                .filter(|update| variables.contains(update.variable))
                .count();
            if selected_updates > 0 {
                assert!(delivery.logical_ids.insert(id));
                delivery.updates += selected_updates;
            }
        }
    }
    delivery
}

fn expectations_for_all(batches: &[OutputBatch<Value>], destination: &str) -> CaseExpectations {
    let (input_ticks, input_updates, input_ids) = input_counts(batches);
    let variables = batches
        .iter()
        .flat_map(|batch| batch.updates().map(|update| update.variable.clone()))
        .collect::<BTreeSet<_>>();
    let delivery = delivery_for_variables(batches, &variables);
    let expectations = CaseExpectations {
        input_physical_batches: batches.len(),
        input_ticks,
        input_updates,
        input_ids,
        destinations: BTreeMap::from([(destination.to_owned(), delivery)]),
    };
    assert_case_setup(batches, &expectations);
    expectations
}

fn expectations_for_resolved(
    resolved: &ResolvedOutput,
    batches: &[OutputBatch<Value>],
) -> CaseExpectations {
    let (input_ticks, input_updates, input_ids) = input_counts(batches);
    let destinations = resolved
        .destinations()
        .iter()
        .map(|destination| {
            let variables = destination
                .bindings()
                .iter()
                .map(|binding| binding.variable().clone())
                .collect::<BTreeSet<_>>();
            (
                destination.id().clone(),
                delivery_for_variables(batches, &variables),
            )
        })
        .collect();
    let expectations = CaseExpectations {
        input_physical_batches: batches.len(),
        input_ticks,
        input_updates,
        input_ids,
        destinations,
    };
    assert_case_setup(batches, &expectations);
    expectations
}

fn assert_case_setup(batches: &[OutputBatch<Value>], expectations: &CaseExpectations) {
    assert_eq!(batches.len(), expectations.input_physical_batches);
    let (ticks, updates, ids) = input_counts(batches);
    assert_eq!(ticks, expectations.input_ticks);
    assert_eq!(updates, expectations.input_updates);
    assert_eq!(ids, expectations.input_ids);
    let requested_ids = (0..expectations.input_ticks).collect::<BTreeSet<_>>();
    assert_eq!(
        ids, requested_ids,
        "logical IDs must cover the requested tick range"
    );
}

fn prepare_pipeline_case(
    width: usize,
    batches: Vec<OutputBatch<Value>>,
    builder: OutputBackendBuilder<Value>,
    executor: Rc<LocalExecutor<'static>>,
    stats: Rc<RefCell<BenchStats>>,
) -> PreparedCase {
    let model_outputs = names(width);
    let resolved = builder
        .resolve(model_outputs.clone(), std::iter::empty::<VarName>(), None)
        .expect("benchmark output pipeline should resolve");
    let expectations = expectations_for_resolved(&resolved, &batches);
    for destination in expectations.destinations.keys() {
        stats.borrow_mut().register_destination(destination);
    }
    let writer = smol::block_on(builder.build(model_outputs, std::iter::empty::<VarName>(), None))
        .expect("benchmark output pipeline should open");
    PreparedCase {
        writer,
        executor: Some(executor),
        batches,
        stats,
        expectations,
    }
}

fn prepare_direct(
    width: usize,
    batches: Vec<OutputBatch<Value>>,
    cost: SyntheticCost,
) -> PreparedCase {
    let stats = Rc::new(RefCell::new(BenchStats::default()));
    stats.borrow_mut().register_destination("direct");
    let backend = SyntheticBackend::new(Rc::clone(&stats), "direct", cost);
    let interface =
        OutputInterface::outputs(names(width)).expect("direct interface should resolve");
    let writer = smol::block_on(backend.open(interface)).expect("direct writer should open");
    let expectations = expectations_for_all(&batches, "direct");
    PreparedCase {
        writer,
        executor: None,
        batches,
        stats,
        expectations,
    }
}

fn custom_destination(
    id: &str,
    variables: &[VarName],
    stats: &Rc<RefCell<BenchStats>>,
    cost: SyntheticCost,
) -> OutputDestination<Value> {
    let backend = OutputBackendConfig::custom(SyntheticBackend::new(Rc::clone(stats), id, cost));
    OutputDestination::new(id, backend).partition(variables.iter())
}

fn one_destination_builder_case(
    width: usize,
    batches: Vec<OutputBatch<Value>>,
    stages: Vec<OutputStage>,
    cost: SyntheticCost,
) -> PreparedCase {
    let stats = Rc::new(RefCell::new(BenchStats::default()));
    let destination = OutputDestination::new(
        "default",
        OutputBackendConfig::custom(SyntheticBackend::new(Rc::clone(&stats), "default", cost)),
    )
    .all();
    let pipeline = OutputPipeline::from_destination(destination)
        .expect("benchmark default output destination should construct a pipeline");
    let executor = Rc::new(LocalExecutor::new());
    let builder = OutputBackendBuilder::from_pipeline(pipeline)
        .executor(Rc::clone(&executor))
        .with_shared_stages(stages);
    prepare_pipeline_case(width, batches, builder, executor, stats)
}

fn names(width: usize) -> Vec<VarName> {
    (0..width)
        .map(|index| VarName::new(&format!("v{index}")))
        .collect()
}

fn mixed_update_count(width: usize, tick: usize) -> usize {
    match tick % 3 {
        0 => 1,
        1 => width,
        _ => width.min(2),
    }
}

fn expected_shape_updates(width: usize, ticks: usize, shape: &str) -> usize {
    match shape {
        "singleton" => ticks,
        "simultaneous" | "packed" => ticks * width,
        "mixed" => (0..ticks).map(|tick| mixed_update_count(width, tick)).sum(),
        _ => unreachable!("unknown output benchmark shape"),
    }
}

fn mixed_tick(variables: &[VarName], tick: usize) -> OutputBatch<Value> {
    match tick % 3 {
        0 => OutputBatch::update(variables[tick % variables.len()].clone(), value(tick)),
        1 => OutputBatch::tick(
            variables
                .iter()
                .cloned()
                .map(|variable| OutputUpdate::new(variable, value(tick)))
                .collect(),
        )
        .expect("mixed simultaneous tick should be valid"),
        _ => {
            let layout = variables[..variables.len().min(2)].to_vec();
            OutputBatch::packed_rows(layout.clone(), layout.iter().map(|_| value(tick)))
                .expect("mixed packed tick should be valid")
        }
    }
}

fn shape_batches(
    width: usize,
    logical_ticks: usize,
    physical_ticks: usize,
    shape: &str,
) -> Vec<OutputBatch<Value>> {
    assert!(width > 0, "benchmark width must be positive");
    assert!(
        physical_ticks > 0,
        "physical tick grouping must be positive"
    );
    let variables = names(width);
    let mut result = Vec::new();
    let mut start = 0;
    while start < logical_ticks {
        let count = physical_ticks.min(logical_ticks - start);
        let batch = match shape {
            "singleton" => OutputBatch::from_ticks(
                (start..start + count)
                    .map(|tick| {
                        vec![OutputUpdate::new(
                            variables[tick % width].clone(),
                            value(tick),
                        )]
                    })
                    .collect(),
            )
            .expect("singleton shape should be valid"),
            "simultaneous" => OutputBatch::from_ticks(
                (start..start + count)
                    .map(|tick| {
                        variables
                            .iter()
                            .cloned()
                            .map(|variable| OutputUpdate::new(variable, value(tick)))
                            .collect()
                    })
                    .collect(),
            )
            .expect("simultaneous shape should be valid"),
            "packed" => OutputBatch::packed_rows(
                variables.clone(),
                (start..start + count).flat_map(|tick| (0..width).map(move |_| value(tick))),
            )
            .expect("packed shape should be valid"),
            "mixed" => (start..start + count).fold(OutputBatch::empty(), |batch, tick| {
                batch
                    .concat(mixed_tick(&variables, tick))
                    .expect("mixed shape should concatenate")
            }),
            _ => unreachable!("unknown output benchmark shape"),
        };
        assert_eq!(
            batch.tick_count(),
            count,
            "one physical batch must contain the requested logical tick count"
        );
        assert!(
            batch.tick_count() <= physical_ticks,
            "physical tick grouping must be an upper bound, not a shape-dependent multiplier"
        );
        result.push(batch);
        start += count;
    }

    let requested_updates = expected_shape_updates(width, logical_ticks, shape);
    let (actual_ticks, actual_updates, actual_ids) = input_counts(&result);
    assert_eq!(
        actual_ticks, logical_ticks,
        "shape changed logical tick count"
    );
    assert_eq!(
        actual_updates, requested_updates,
        "shape changed update count"
    );
    assert_eq!(
        actual_ids,
        (0..logical_ticks).collect::<BTreeSet<_>>(),
        "shape must carry one unique logical ID per requested tick"
    );
    result
}

fn builder_from_destinations(
    destinations: Vec<OutputDestination<Value>>,
    executor: Rc<LocalExecutor<'static>>,
) -> OutputBackendBuilder<Value> {
    let registry = OutputDestinations::new(destinations)
        .expect("benchmark destination IDs should be non-empty and unique");
    let pipeline = OutputPipeline::new(registry);
    OutputBackendBuilder::from_pipeline(pipeline).executor(executor)
}

fn prepare_failure_case(width: usize) -> PreparedCase {
    let stats = Rc::new(RefCell::new(BenchStats::default()));
    let variables = names(width);
    let split = width / 2;
    let failing = OutputDestination::new(
        "a-failing",
        OutputBackendConfig::custom(SyntheticBackend::failing(Rc::clone(&stats), "a-failing")),
    )
    .partition(variables[..split].iter());
    let successful = custom_destination(
        "b-successful",
        &variables[split..],
        &stats,
        SyntheticCost::new(Duration::from_micros(10), 0, 0),
    );
    let executor = Rc::new(LocalExecutor::new());
    let builder = builder_from_destinations(vec![failing, successful], Rc::clone(&executor));
    prepare_pipeline_case(
        width,
        shape_batches(width, 1, 1, "simultaneous"),
        builder,
        executor,
        stats,
    )
}

fn bench_failure_cleanup(c: &mut Criterion) {
    let mut group = c.benchmark_group("output/shutdown_failure_cleanup");
    group.sample_size(10);
    group.bench_function("one_failing_of_two_destinations", |benchmark| {
        benchmark.iter_batched(
            || prepare_failure_case(32),
            |prepared| black_box(run_failure_case(prepared)),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_direct_and_fast_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("output/direct_vs_pipeline");
    group.sample_size(10);
    for shape in ["singleton", "simultaneous", "packed", "mixed"] {
        for width in [1_usize, 8, 32, 128] {
            let batches = shape_batches(width, 64, 16, shape);
            let updates = batches.iter().map(OutputBatch::update_count).sum::<usize>();
            group.throughput(Throughput::Elements(updates as u64));
            group.bench_with_input(
                BenchmarkId::new(format!("direct/{shape}/width_{width}"), updates),
                &batches,
                |benchmark, batches| {
                    benchmark.iter_batched(
                        || {
                            prepare_direct(
                                width,
                                batches.clone(),
                                SyntheticCost::new(Duration::ZERO, 0, 0),
                            )
                        },
                        |prepared| black_box(run_prepared(prepared)),
                        BatchSize::SmallInput,
                    );
                },
            );
            group.bench_with_input(
                BenchmarkId::new(format!("pipeline_no_stage/{shape}/width_{width}"), updates),
                &batches,
                |benchmark, batches| {
                    benchmark.iter_batched(
                        || {
                            one_destination_builder_case(
                                width,
                                batches.clone(),
                                Vec::new(),
                                SyntheticCost::new(Duration::ZERO, 0, 0),
                            )
                        },
                        |prepared| black_box(run_prepared(prepared)),
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }
    group.finish();
}

fn bench_stage_orders_and_backpressure(c: &mut Criterion) {
    let mut group = c.benchmark_group("output/stages");
    group.sample_size(10);
    let batches = shape_batches(32, 256, 1, "singleton");
    group.throughput(Throughput::Elements(256));
    for capacity in [1_usize, 8, 64, 256] {
        for (name, stages) in [
            ("buffer", vec![OutputStage::buffer(capacity).unwrap()]),
            (
                "buffer_then_coalesce",
                vec![
                    OutputStage::buffer(capacity).unwrap(),
                    OutputStage::coalesce(64, None).unwrap(),
                ],
            ),
            (
                "coalesce_then_buffer",
                vec![
                    OutputStage::coalesce(64, None).unwrap(),
                    OutputStage::buffer(capacity).unwrap(),
                ],
            ),
        ] {
            group.bench_with_input(
                BenchmarkId::new(name, capacity),
                &batches,
                |benchmark, batches| {
                    benchmark.iter_batched(
                        || {
                            one_destination_builder_case(
                                32,
                                batches.clone(),
                                stages.clone(),
                                SyntheticCost::new(Duration::from_micros(50), 8, 0),
                            )
                        },
                        |prepared| black_box(run_prepared(prepared)),
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }
    group.bench_function("coalesce_only/tick_limit_64", |benchmark| {
        benchmark.iter_batched(
            || {
                one_destination_builder_case(
                    32,
                    batches.clone(),
                    vec![OutputStage::coalesce(64, None).unwrap()],
                    SyntheticCost::new(Duration::from_micros(50), 8, 0),
                )
            },
            |prepared| black_box(run_prepared(prepared)),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn routed_destinations(
    count: usize,
    width: usize,
    mirror: bool,
    stats: &Rc<RefCell<BenchStats>>,
) -> Vec<OutputDestination<Value>> {
    assert!(count > 0 && count <= width);
    let variables = names(width);
    let chunk = width.div_ceil(count);
    let mut destinations = Vec::with_capacity(count);
    for destination_index in 0..count {
        let id = format!("d{destination_index}");
        if mirror && destination_index == 0 {
            destinations.push(
                OutputDestination::new(
                    id,
                    OutputBackendConfig::custom(SyntheticBackend::new(
                        Rc::clone(stats),
                        "d0",
                        SyntheticCost::new(Duration::ZERO, 0, 0),
                    )),
                )
                .all(),
            );
            continue;
        }
        let start = destination_index * chunk;
        let end = (start + chunk).min(width);
        assert!(
            start < end,
            "every routed destination must receive variables"
        );
        let selected = &variables[start..end];
        let backend = OutputBackendConfig::custom(SyntheticBackend::new(
            Rc::clone(stats),
            id.clone(),
            SyntheticCost::new(Duration::ZERO, 0, 0),
        ));
        let destination = if mirror {
            OutputDestination::new(id, backend).mirror(selected.iter())
        } else {
            OutputDestination::new(id, backend).partition(selected.iter())
        };
        destinations.push(destination);
    }
    assert_eq!(destinations.len(), count);
    destinations
}

fn bench_routing(c: &mut Criterion) {
    let mut group = c.benchmark_group("output/routing");
    group.sample_size(10);
    let width = 32;
    let batches = shape_batches(width, 128, 16, "packed");
    group.throughput(Throughput::Elements(128 * width as u64));
    for destination_count in [2_usize, 4] {
        for mirror in [false, true] {
            group.bench_with_input(
                BenchmarkId::new(
                    if mirror {
                        "partial_mirror"
                    } else {
                        "partition"
                    },
                    destination_count,
                ),
                &batches,
                |benchmark, batches| {
                    benchmark.iter_batched(
                        || {
                            let stats = Rc::new(RefCell::new(BenchStats::default()));
                            let executor = Rc::new(LocalExecutor::new());
                            let destinations =
                                routed_destinations(destination_count, width, mirror, &stats);
                            let builder =
                                builder_from_destinations(destinations, Rc::clone(&executor));
                            prepare_pipeline_case(width, batches.clone(), builder, executor, stats)
                        },
                        |prepared| black_box(run_prepared(prepared)),
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }
    group.finish();
}

fn bench_coalescing_and_native_batching(c: &mut Criterion) {
    let mut group = c.benchmark_group("output/coalescing");
    group.sample_size(10);
    let singleton = shape_batches(32, 256, 1, "singleton");
    group.throughput(Throughput::Elements(256));
    for limit in [4_usize, 16, 64, 256] {
        group.bench_with_input(
            BenchmarkId::new("tick_limit", limit),
            &singleton,
            |benchmark, batches| {
                benchmark.iter_batched(
                    || {
                        one_destination_builder_case(
                            32,
                            batches.clone(),
                            vec![OutputStage::coalesce(limit, None).unwrap()],
                            SyntheticCost::new(Duration::from_micros(50), 8, 0),
                        )
                    },
                    |prepared| black_box(run_prepared(prepared)),
                    BatchSize::SmallInput,
                );
            },
        );
    }
    for limit in [32_usize, 256, 4096] {
        group.bench_with_input(
            BenchmarkId::new("update_limit", limit),
            &singleton,
            |benchmark, batches| {
                benchmark.iter_batched(
                    || {
                        one_destination_builder_case(
                            32,
                            batches.clone(),
                            vec![
                                OutputStage::coalesce_with_limits(None, None, Some(limit)).unwrap(),
                            ],
                            SyntheticCost::new(Duration::from_micros(50), 8, 0),
                        )
                    },
                    |prepared| black_box(run_prepared(prepared)),
                    BatchSize::SmallInput,
                );
            },
        );
    }
    for delay in [Duration::from_millis(1), Duration::from_millis(5)] {
        group.bench_with_input(
            BenchmarkId::new("timed_delay_ms", delay.as_millis()),
            &singleton,
            |benchmark, batches| {
                benchmark.iter_batched(
                    || {
                        one_destination_builder_case(
                            32,
                            batches.clone(),
                            vec![
                                OutputStage::coalesce_with_limits(Some(delay), None, None).unwrap(),
                            ],
                            SyntheticCost::new(Duration::from_micros(50), 8, 0),
                        )
                    },
                    |prepared| black_box(run_prepared(prepared)),
                    BatchSize::SmallInput,
                );
            },
        );
    }

    let native = shape_batches(32, 256, 64, "packed");
    group.bench_function("producer_native_packed", |benchmark| {
        benchmark.iter_batched(
            || {
                one_destination_builder_case(
                    32,
                    native.clone(),
                    Vec::new(),
                    SyntheticCost::new(Duration::from_micros(50), 8, 0),
                )
            },
            |prepared| black_box(run_prepared(prepared)),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("singleton_plus_generic_coalesce", |benchmark| {
        benchmark.iter_batched(
            || {
                one_destination_builder_case(
                    32,
                    singleton.clone(),
                    vec![OutputStage::coalesce(64, None).unwrap()],
                    SyntheticCost::new(Duration::from_micros(50), 8, 0),
                )
            },
            |prepared| black_box(run_prepared(prepared)),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn output_pipeline_benches(c: &mut Criterion) {
    bench_direct_and_fast_path(c);
    bench_stage_orders_and_backpressure(c);
    bench_coalescing_and_native_batching(c);
    bench_routing(c);
    bench_failure_cleanup(c);
}

criterion_group!(benches, output_pipeline_benches);
criterion_main!(benches);
