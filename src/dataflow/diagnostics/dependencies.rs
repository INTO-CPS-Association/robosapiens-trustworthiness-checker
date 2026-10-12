//! Machine-readable dependency metadata and dependency-aware aggregation.
//!
//! Edges are read from the bound graphs of the active definition: a *same-tick* edge is a direct
//! read of another variable's current value, a *historical* edge a read through `x[k]`, and a
//! *dynamic* edge a dependency of the currently installed `dynamic`/`defer` body. Function and
//! match-arm programs contribute their captured variables as same-tick edges.
//!
//! For every output the aggregation walks its upstream closure over all three kinds of edge and
//! splits it into streams no other output reaches (*unique upstream*) and streams other outputs
//! also reach (*shared upstream*, listed with those outputs and never divided among them). These
//! are diagnostic aggregates: removing an output would not necessarily save its unique figures,
//! because evaluation order, fusion and retention change with the specification.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::dataflow::DataflowProgram;
use crate::dataflow::environment::EnvironmentSlot;
use crate::dataflow::ir::{BoundEvaluationGraph, BoundRef, StreamOp};

#[derive(Clone, Debug, Default, Serialize)]
pub(in crate::dataflow) struct StreamDependencies {
    pub(in crate::dataflow) stream: String,
    pub(in crate::dataflow) output: bool,
    pub(in crate::dataflow) same_tick: Vec<String>,
    pub(in crate::dataflow) historical: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(in crate::dataflow) dynamic: Vec<String>,
}

/// Per-stream cost inputs to the aggregation, and the aggregates built from them.
#[derive(Clone, Copy, Debug, Serialize)]
pub(in crate::dataflow) struct Cost {
    /// Total elapsed nanoseconds of graph steps evaluating this stream alone.
    pub(in crate::dataflow) step_ns: u64,
    /// Estimated bytes charged to this stream by the latest snapshot of the current definition,
    /// or `None` when no such snapshot exists. Charges add up across streams without double
    /// counting, so a payload shared between streams is charged to the first stream reaching it
    /// (unless `map_identity_uncertain`).
    pub(in crate::dataflow) bytes: Option<u64>,
    /// The charge plus the shells of shared payload roots charged to another stream. A partial
    /// figure: the children of a shared root are not re-walked, so it is not a reachable total.
    /// It overlaps between streams, so it exists only for one stream (`local`). Every upstream
    /// aggregate leaves it `null`: summing overlapping figures would count shared roots once per
    /// stream, and de-duplicating them would need a recursive walk the snapshot does not do.
    pub(in crate::dataflow) bytes_with_shared_roots: Option<u64>,
    /// Some part of the stream could not be inspected (for example native-owned state), so the
    /// byte figures cover only what was visible.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub(in crate::dataflow) opaque: bool,
    /// A budget cut some part of the stream's traversal short.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub(in crate::dataflow) truncated: bool,
    /// A map root was charged without being compared against every earlier same-sized root (the
    /// comparison window holds only the most recent roots), so a cloned handle may have been
    /// charged again and the charges are not guaranteed globally unique.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub(in crate::dataflow) map_identity_uncertain: bool,
}

impl Default for Cost {
    fn default() -> Self {
        Self {
            step_ns: 0,
            bytes: Some(0),
            bytes_with_shared_roots: Some(0),
            opaque: false,
            truncated: false,
            map_identity_uncertain: false,
        }
    }
}

impl Cost {
    /// Memory figures marked unavailable because no snapshot of the definition exists.
    pub(in crate::dataflow) fn without_memory(step_ns: u64) -> Self {
        Self {
            step_ns,
            bytes: None,
            bytes_with_shared_roots: None,
            ..Self::default()
        }
    }

    /// An empty upstream aggregate: it sums `step_ns` and `bytes` but never the overlapping
    /// `bytes_with_shared_roots`, which stays unavailable.
    fn aggregate() -> Self {
        Self {
            bytes_with_shared_roots: None,
            ..Self::default()
        }
    }

    fn add(&mut self, other: Cost) {
        self.step_ns += other.step_ns;
        self.bytes = self
            .bytes
            .zip(other.bytes)
            .map(|(left, right)| left + right);
        self.opaque |= other.opaque;
        self.truncated |= other.truncated;
        self.map_identity_uncertain |= other.map_identity_uncertain;
    }
}

#[derive(Clone, Debug, Serialize)]
pub(in crate::dataflow) struct OutputAggregate {
    pub(in crate::dataflow) output: String,
    /// The output stream's own cost, the only place `bytes_with_shared_roots` is reported. It is
    /// not repeated in the upstream totals.
    pub(in crate::dataflow) local: Cost,
    /// Streams upstream of the output, excluding the output itself, that no other output reaches.
    pub(in crate::dataflow) unique_upstream: Cost,
    pub(in crate::dataflow) unique_streams: Vec<String>,
    /// Upstream streams, excluding the output itself, that other outputs also reach.
    pub(in crate::dataflow) shared_upstream: Cost,
    pub(in crate::dataflow) shared_streams: Vec<SharedStream>,
    /// Closure streams counted in the totals but not listed, beyond the listing limit.
    pub(in crate::dataflow) unlisted_streams: usize,
    /// Fused regions whose members all lie in this output's unique closure.
    pub(in crate::dataflow) unique_region_ns: u64,
    /// Fused regions with at least one member shared with another output, or outside the closure.
    pub(in crate::dataflow) shared_region_ns: u64,
    pub(in crate::dataflow) inputs: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub(in crate::dataflow) struct SharedStream {
    pub(in crate::dataflow) stream: String,
    pub(in crate::dataflow) also_reached_by: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub(in crate::dataflow) struct DependencyReport {
    pub(in crate::dataflow) definition_version: u32,
    /// Label of the snapshot supplying byte costs, or `None` when no snapshot of this definition
    /// version is retained, in which case every byte figure is `null` (unavailable). A snapshot of
    /// an earlier version is never used: stream names can be reused with another meaning.
    pub(in crate::dataflow) memory_snapshot: Option<String>,
    /// Completed ticks at the supplying snapshot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(in crate::dataflow) memory_snapshot_tick: Option<u64>,
    pub(in crate::dataflow) streams: Vec<StreamDependencies>,
    pub(in crate::dataflow) outputs: Vec<OutputAggregate>,
    /// Outputs beyond `max_dependency_outputs`, which receive no aggregate. With any omitted,
    /// uniqueness is relative to the aggregated outputs only.
    pub(in crate::dataflow) omitted_outputs: usize,
    pub(in crate::dataflow) note: &'static str,
}

#[derive(Default)]
struct Edges {
    same_tick: BTreeSet<usize>,
    historical: BTreeSet<usize>,
    /// The stream reads its own earlier values through a recursive delay.
    own_history: bool,
}

fn collect(graph: &BoundEvaluationGraph, edges: &mut Edges) {
    fn add(edges: &mut Edges, reference: &BoundRef, historical: bool) {
        if let BoundRef::External(slot) = reference {
            if historical {
                edges.historical.insert(slot.index());
            } else {
                edges.same_tick.insert(slot.index());
            }
        }
    }
    add(edges, &graph.output, false);
    for op in &graph.nodes {
        match op {
            StreamOp::Delay { input, offset } => add(edges, input, *offset > 0),
            StreamOp::RecursiveDelay { .. } => edges.own_history = true,
            _ => op.for_each_operand(|operand| add(edges, operand, false)),
        }
        match op {
            StreamOp::If {
                then_branch,
                else_branch,
                ..
            } => {
                collect(then_branch, edges);
                collect(else_branch, edges);
            }
            StreamOp::Function { func }
            | StreamOp::DirectApply { func, .. }
            | StreamOp::RecursiveApply { func, .. } => {
                edges
                    .same_tick
                    .extend(func.capture_slots.iter().map(|slot| slot.index()));
            }
            StreamOp::Match { arms, .. } => {
                for arm in arms {
                    for function in arm.guard.iter().chain(std::iter::once(&arm.body)) {
                        edges
                            .same_tick
                            .extend(function.capture_slots.iter().map(|slot| slot.index()));
                    }
                }
            }
            _ => {}
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(in crate::dataflow) fn build(
    program: &DataflowProgram,
    definition_version: u32,
    memory_snapshot: Option<(String, u64)>,
    dynamic_slots: impl Fn(usize) -> Vec<EnvironmentSlot>,
    costs: &BTreeMap<usize, Cost>,
    regions: &[(Vec<usize>, u64)],
    limits: &crate::diagnostics::RecorderLimits,
) -> DependencyReport {
    let input_count = program.input_vars().len();
    let slot_name = |slot: usize| -> String {
        if slot < input_count {
            program.input_vars()[slot].name().to_string()
        } else {
            program
                .stream_vars()
                .get(slot - input_count)
                .map_or_else(|| format!("slot {slot}"), |var| var.name().to_string())
        }
    };
    let names = |slots: &BTreeSet<usize>| slots.iter().map(|slot| slot_name(*slot)).collect();
    let stream_count = program.stream_programs().len();
    let mut upstream = vec![BTreeSet::<usize>::new(); stream_count];
    let mut streams = Vec::with_capacity(stream_count);
    for (index, stream) in program.stream_programs().iter().enumerate() {
        let mut edges = Edges::default();
        collect(&stream.graph, &mut edges);
        let own = input_count + index;
        edges.same_tick.remove(&own);
        if edges.own_history {
            edges.historical.insert(own);
        }
        let dynamic = dynamic_slots(index)
            .into_iter()
            .map(|slot| slot.index())
            .filter(|slot| *slot != own)
            .collect::<BTreeSet<_>>();
        upstream[index].extend(edges.same_tick.iter().copied());
        upstream[index].extend(
            edges
                .historical
                .iter()
                .filter(|slot| **slot != own)
                .copied(),
        );
        upstream[index].extend(dynamic.iter().copied());
        streams.push(StreamDependencies {
            stream: slot_name(own),
            output: program
                .output_vars()
                .contains(&program.stream_vars()[index]),
            same_tick: names(&edges.same_tick),
            historical: names(&edges.historical),
            dynamic: names(&dynamic),
        });
    }

    // Transitive upstream closure of each aggregated output, over stream slots only. Closures are
    // computed for at most `max_dependency_outputs` outputs, so the closure sets hold at most
    // `max_dependency_outputs * streams` entries, and each output lists at most
    // `max_listed_closure_streams` streams; the rest are counted in `unlisted_streams` and
    // `omitted_outputs`.
    let all_outputs = program
        .output_vars()
        .iter()
        .filter_map(|var| {
            program
                .stream_vars()
                .iter()
                .position(|stream| stream == var)
        })
        .collect::<Vec<_>>();
    let output_streams = &all_outputs[..all_outputs.len().min(limits.max_dependency_outputs)];
    let closure = |root: usize| -> (BTreeSet<usize>, BTreeSet<usize>) {
        let mut seen = BTreeSet::from([root]);
        let mut inputs = BTreeSet::new();
        let mut stack = vec![root];
        while let Some(stream) = stack.pop() {
            for &slot in &upstream[stream] {
                if slot < input_count {
                    inputs.insert(slot);
                } else if seen.insert(slot - input_count) {
                    stack.push(slot - input_count);
                }
            }
        }
        (seen, inputs)
    };
    let closures = output_streams
        .iter()
        .map(|&output| (output, closure(output)))
        .collect::<Vec<_>>();
    let mut reached_by = BTreeMap::<usize, Vec<usize>>::new();
    for (output, (members, _)) in &closures {
        for member in members {
            reached_by.entry(*member).or_default().push(*output);
        }
    }

    let listed = limits.max_listed_closure_streams;
    let stream_name = |stream: usize| slot_name(input_count + stream);
    let outputs = closures
        .iter()
        .map(|(output, (members, inputs))| {
            let mut aggregate = OutputAggregate {
                output: stream_name(*output),
                local: costs.get(output).copied().unwrap_or_default(),
                unique_upstream: Cost::aggregate(),
                unique_streams: Vec::new(),
                shared_upstream: Cost::aggregate(),
                shared_streams: Vec::new(),
                unlisted_streams: 0,
                unique_region_ns: 0,
                shared_region_ns: 0,
                inputs: inputs.iter().map(|slot| slot_name(*slot)).collect(),
            };
            for member in members {
                if member == output {
                    continue;
                }
                let cost = costs.get(member).copied().unwrap_or_default();
                let owners = &reached_by[member];
                let room = aggregate.unique_streams.len() + aggregate.shared_streams.len() < listed;
                if owners.len() == 1 {
                    aggregate.unique_upstream.add(cost);
                    if room {
                        aggregate.unique_streams.push(stream_name(*member));
                    } else {
                        aggregate.unlisted_streams += 1;
                    }
                } else {
                    aggregate.shared_upstream.add(cost);
                    if room {
                        aggregate.shared_streams.push(SharedStream {
                            stream: stream_name(*member),
                            also_reached_by: owners
                                .iter()
                                .filter(|owner| *owner != output)
                                .take(listed)
                                .map(|owner| stream_name(*owner))
                                .collect(),
                        });
                    } else {
                        aggregate.unlisted_streams += 1;
                    }
                }
            }
            for (region_members, nanos) in regions {
                if !region_members.iter().any(|member| members.contains(member)) {
                    continue;
                }
                let unique = region_members
                    .iter()
                    .all(|member| members.contains(member) && reached_by[member].len() == 1);
                if unique {
                    aggregate.unique_region_ns += nanos;
                } else {
                    aggregate.shared_region_ns += nanos;
                }
            }
            aggregate
        })
        .collect();

    DependencyReport {
        definition_version,
        memory_snapshot_tick: memory_snapshot.as_ref().map(|(_, tick)| *tick),
        memory_snapshot: memory_snapshot.map(|(label, _)| label),
        streams,
        outputs,
        omitted_outputs: all_outputs.len() - output_streams.len(),
        note: "Aggregates over each output's upstream closure, with the output's own cost in \
               local only. Shared streams are listed, not divided, so shared figures recur under \
               every output reaching them. Upstream byte totals sum charged bytes only; \
               bytes_with_shared_roots is null in every upstream aggregate because it overlaps \
               between streams and a de-duplicated figure would need a recursive walk. Byte \
               figures flagged map_identity_uncertain may charge a cloned map more than once. \
               These are diagnostic totals, not a prediction of time or memory saved by \
               removing an output. Region time covers fused regions touching \
               the closure, including the output's own region.",
    }
}
