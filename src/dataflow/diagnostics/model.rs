//! Versioned model metadata: which streams a definition has, and where they came from.
//!
//! A [`DefinitionRecord`] is captured when diagnostics are enabled and again whenever the monitor
//! installs another definition, so stream indices and plan identities recorded under one version
//! never collide with those of another. Source attribution is attached by [`super::sites`].

use serde::Serialize;

use super::sites::{SourceResolver, StreamSites};
use crate::dataflow::DataflowProgram;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::dataflow) enum StreamRole {
    Output,
    Auxiliary,
}

#[derive(Clone, Debug, Serialize)]
pub(in crate::dataflow) struct StreamRecord {
    pub(in crate::dataflow) id: usize,
    pub(in crate::dataflow) name: String,
    pub(in crate::dataflow) role: StreamRole,
    /// Environment slot of the stream's value.
    pub(in crate::dataflow) slot: usize,
    pub(in crate::dataflow) state_key: String,
    #[serde(flatten)]
    pub(in crate::dataflow) sites: StreamSites,
}

#[derive(Clone, Debug, Serialize)]
pub(in crate::dataflow) struct DefinitionRecord {
    pub(in crate::dataflow) version: u32,
    pub(in crate::dataflow) installed_at_tick: u64,
    pub(in crate::dataflow) definition_key: String,
    /// Graph nodes the bounded capture visited, nested graphs included. Counted while the capture
    /// walks them, never by a separate scan of the program, so with `node_count_is_lower_bound`
    /// it is the number visited, not the size of the program.
    pub(in crate::dataflow) node_count: usize,
    /// The node budget was reached and completeness is not established: the program has at least
    /// `node_count` nodes, and possibly exactly that many.
    pub(in crate::dataflow) node_count_is_lower_bound: bool,
    pub(in crate::dataflow) inputs: Vec<String>,
    pub(in crate::dataflow) streams: Vec<StreamRecord>,
}

impl DefinitionRecord {
    pub(in crate::dataflow) fn capture(
        program: &DataflowProgram,
        limits: &crate::diagnostics::RecorderLimits,
    ) -> Self {
        let inputs = program
            .input_vars()
            .iter()
            .map(|var| var.name().to_string())
            .collect::<Vec<_>>();
        let outputs = program.output_vars();
        let mut resolver =
            SourceResolver::new(program.debug_sources(), limits.max_source_scan_bytes);
        let mut remaining = limits.max_model_nodes;
        let streams: Vec<StreamRecord> = program
            .stream_vars()
            .iter()
            .zip(program.stream_programs())
            .enumerate()
            .map(|(id, (var, stream))| StreamRecord {
                id,
                name: var.name().to_string(),
                role: if outputs.contains(var) {
                    StreamRole::Output
                } else {
                    StreamRole::Auxiliary
                },
                slot: inputs.len() + id,
                state_key: stream.state_key().to_string(),
                sites: StreamSites::capture(&stream.graph, &mut resolver, &mut remaining),
            })
            .collect();
        Self {
            version: 0,
            installed_at_tick: 0,
            definition_key: program.definition_key().to_string(),
            node_count: streams.iter().map(|stream| stream.sites.node_count).sum(),
            node_count_is_lower_bound: streams.iter().any(|stream| stream.sites.nodes_truncated),
            inputs,
            streams,
        }
    }
}
