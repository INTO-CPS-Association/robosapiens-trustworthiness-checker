use std::collections::BTreeMap;

use crate::VarName;
use crate::distributed::distribution_graphs::{LabelledConcDistributionGraph, NodeName};
use crate::lang::distribution_constraints::ast::{
    DistConstraint, DistConstraintBody, DistConstraintType,
};
use petgraph::algo::all_simple_paths;
use petgraph::prelude::*;

pub fn gen_paths(
    node_name: NodeName,
    conc_dist_graph: &LabelledConcDistributionGraph,
) -> impl Iterator<Item = Vec<NodeIndex>> {
    let dist_graph = &conc_dist_graph.dist_graph;
    let monitor = dist_graph.central_monitor;
    let graph = &dist_graph.graph;
    let node = graph
        .node_indices()
        .find(|i| graph[*i] == node_name)
        .unwrap();

    const MIN_NODES: usize = 0;
    const MAX_NODES: Option<usize> = None;
    all_simple_paths(graph, node, monitor, MIN_NODES, MAX_NODES)
}

// Checks a path against a constraint
pub fn check_path_constraint(
    path: &Vec<NodeIndex>,
    node_labels: &BTreeMap<NodeIndex, Vec<VarName>>,
    constraint: &DistConstraint,
) -> bool {
    let typ = &constraint.0;
    let body = &constraint.1;
    match typ {
        DistConstraintType::CanRun => match body {
            // Check if var_name is within the node_labels relevant to the path
            DistConstraintBody::Monitor(var_name) => path.iter().any(|i| {
                node_labels
                    .get(i)
                    .is_some_and(|vec| vec.iter().any(|name| name == var_name))
            }),
            // Check if var_name is within the node_labels relevant to the path
            DistConstraintBody::Source(var_name) => path.iter().any(|i| {
                node_labels
                    .get(i)
                    .is_some_and(|vec| vec.iter().any(|name| name == var_name))
            }),

            DistConstraintBody::Dist(_) => todo!(),
            DistConstraintBody::WeightedDist(_, _) => {
                todo!()
            }
            DistConstraintBody::Sum(_) => {
                todo!()
            }
            _ => todo!(),
        },
        DistConstraintType::LocalityScore => todo!(),
        DistConstraintType::Redundancy => todo!(),
    }
}

pub fn check(
    node_name: NodeName,
    conc_dist_graph: &LabelledConcDistributionGraph,
    constraints: Vec<DistConstraint>,
) -> Vec<bool> {
    let node_labels = &conc_dist_graph.node_labels;
    gen_paths(node_name, conc_dist_graph)
        .map(|path| {
            constraints
                .iter()
                .map(|constraint| check_path_constraint(&path, node_labels, &constraint))
                .all(|b| b)
        })
        .collect()
}
