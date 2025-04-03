use std::collections::BTreeMap;

use crate::VarName;
use crate::distributed::distribution_graphs::{LabelledDistributionGraph, NodeName};
use crate::lang::distribution_constraints::ast::{
    DistConstraint, DistConstraintBody, DistConstraintType,
};
use contracts::ensures;
use ecow::EcoVec;
use petgraph::algo::all_simple_paths;
use petgraph::prelude::*;

// use petgraph::visit
#[allow(unused)]
type Path = EcoVec<NodeIndex>;

#[allow(unused)]
struct PathGenerator {
    active_paths: EcoVec<Path>,
    next_paths: EcoVec<Path>,
    active_neighbours: EcoVec<NodeIndex>,
}

impl PathGenerator {
    #[ensures(!ret.active_paths.is_empty() || ret.active_neighbours.is_empty())]
    fn new(origin: NodeIndex) -> Self {
        let active_paths = EcoVec::from([EcoVec::new()]);
        let active_neighbours = EcoVec::from([origin]);
        let next_paths = EcoVec::new();
        PathGenerator {
            active_paths,
            next_paths,
            active_neighbours,
        }
    }
}

// #[invariant(!self.active_paths.is_empty() || self.active_neighbours.is_empty())]
// impl<R: GraphRef> Walker<R> for PathGenerator {
//     type Item = Path;

//     fn walk_next(&mut self, context: R) -> Option<Self::Item> {
//         if let Some(next_neighbour) = self.active_neighbours.pop() {
//             let mut path = self.active_paths.first().unwrap().clone();
//             path.push(next_neighbour);
//             self.next_paths.push(path.clone());
//             Some(path)
//         } else if let Some(active_path) = self.active_paths.pop() {
//             let mut path = active_path.clone();
//             let mut neighbours = context.neighbours(active_path.last().unwrap());
//             while let Some(neighbour) = neighbours.next() {
//                 if !active_path.contains(&neighbour) {
//                     self.active_neighbours.push(neighbour);
//                     path.push(neighbour);
//                     self.next_paths.push(path.clone());
//                 }
//             }
//             self.active_paths.extend(self.next_paths.drain(..));
//             self.walk_next(context)

//         } else {
//             None
//         }
//     }
// }

// pub fn paths(
//     node_name: NodeName,
//     dist_graph: &LabelledDistributionGraph,
// ) -> Result<impl Iterator<Item = Vec<NodeIndex>>, ()> {
//     let root = dist_graph
//         .dist_graph
//         .graph
//         .node_indices()
//         .find(|i| dist_graph.dist_graph.graph[*i] == node_name)
//         .ok_or(())?;
// }

pub fn gen_paths(
    node_name: NodeName,
    conc_dist_graph: &LabelledDistributionGraph,
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
    conc_dist_graph: &LabelledDistributionGraph,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::distribution_graphs::{
        GenericDistributionGraph, LabelledDistributionGraph,
    };
    use std::collections::BTreeMap;

    #[test]
    fn test_dist_graph_paths() {
        let mut graph = Graph::<NodeName, _>::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        let c = graph.add_node("C".into());
        let d = graph.add_node("D".into());
        let e = graph.add_node("E".into());
        let f = graph.add_node("F".into());

        graph.extend_with_edges(&[(a, b), (b, c), (c, d), (d, e), (e, f)]);

        #[allow(unused)]
        let var_names = vec!["x".to_string(), "y".to_string(), "z".to_string()];

        let dist_graph = GenericDistributionGraph {
            graph,
            central_monitor: f,
        };

        let labelled_dist_graph = LabelledDistributionGraph {
            var_names: vec!["x".into(), "y".into(), "z".into()],
            dist_graph: dist_graph.clone(),
            node_labels: BTreeMap::from([
                (a, vec!["x".into()]),
                (b, vec!["y".into()]),
                (c, vec!["z".into()]),
                (d, vec!["x".into(), "y".into()]),
                (e, vec!["z".into()]),
                (f, vec!["y".into()]),
            ]),
        };

        let paths = gen_paths("A".into(), &labelled_dist_graph);
        for path in paths {
            println!("{:?}", path);
        }
    }
}
