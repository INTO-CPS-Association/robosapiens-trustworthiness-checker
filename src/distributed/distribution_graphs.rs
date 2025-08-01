/* - Should we be using hierarchical graphs?
 * - What about the subnodes of a node (i.e. the different ROS nodes inside
 *   of an individual robot)?
 */

use std::{collections::BTreeMap, fmt::Display, iter::once, rc::Rc};
use thiserror::Error;

use petgraph::{dot::Dot, prelude::*};
use serde::{Deserialize, Serialize};
use smol::{
    fs::{self, File},
    io::AsyncWriteExt,
    process::Command,
};
use tracing::{debug, info};

use crate::{OutputStream, VarName, core::StreamData};

pub type LabelledDistGraphStream = OutputStream<Rc<LabelledDistributionGraph>>;
pub type DistGraphStream = OutputStream<Rc<DistributionGraph>>;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug, Default, Ord, PartialOrd)]
pub struct NodeName(String);

impl NodeName {
    pub fn new(name: impl Into<String>) -> Self {
        NodeName(name.into())
    }
}

impl Display for NodeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0)
    }
}

impl From<&str> for NodeName {
    fn from(s: &str) -> Self {
        NodeName(s.into())
    }
}
impl From<String> for NodeName {
    fn from(s: String) -> Self {
        NodeName(s)
    }
}

impl From<NodeName> for String {
    fn from(node_name: NodeName) -> Self {
        node_name.0
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
struct NodeLabel {
    monitors: Vec<VarName>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct GenericDistributionGraph<Weight> {
    pub central_monitor: NodeIndex,
    pub graph: DiGraph<NodeName, Weight>,
}

pub type DistributionGraph = GenericDistributionGraph<u64>;

impl<W> GenericDistributionGraph<W> {
    pub fn get_node_index_by_name(&self, name: &NodeName) -> Option<NodeIndex> {
        self.graph
            .node_indices()
            .find(|&node| self.graph[node] == *name)
    }

    pub fn locations(&self) -> Vec<NodeName> {
        self.graph
            .node_indices()
            .map(|node| self.graph[node].clone())
            .collect()
    }
}

/* From: https://github.com/petgraph/petgraph/issues/199 */
fn graph_eq<N, E, Ty, Ix>(
    a: &petgraph::Graph<N, E, Ty, Ix>,
    b: &petgraph::Graph<N, E, Ty, Ix>,
) -> bool
where
    N: PartialEq,
    E: PartialEq,
    Ty: petgraph::EdgeType,
    Ix: petgraph::graph::IndexType + PartialEq,
{
    let a_ns = a.raw_nodes().iter().map(|n| &n.weight);
    let b_ns = b.raw_nodes().iter().map(|n| &n.weight);
    let a_es = a
        .raw_edges()
        .iter()
        .map(|e| (e.source(), e.target(), &e.weight));
    let b_es = b
        .raw_edges()
        .iter()
        .map(|e| (e.source(), e.target(), &e.weight));
    a_ns.eq(b_ns) && a_es.eq(b_es)
}

impl<W: PartialEq> PartialEq for GenericDistributionGraph<W> {
    fn eq(&self, other: &Self) -> bool {
        self.central_monitor == other.central_monitor && graph_eq(&self.graph, &other.graph)
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct GenericLabelledDistributionGraph<W> {
    pub dist_graph: Rc<GenericDistributionGraph<W>>,
    pub var_names: Vec<VarName>,
    pub node_labels: BTreeMap<NodeIndex, Vec<VarName>>,
}

pub type LabelledDistributionGraph = GenericLabelledDistributionGraph<u64>;

impl<W> GenericLabelledDistributionGraph<W> {
    pub fn monitors_at_node(&self, node: NodeIndex) -> Option<&Vec<VarName>> {
        self.node_labels.get(&node)
    }

    pub fn get_node_index_by_name(&self, name: &NodeName) -> Option<NodeIndex> {
        self.dist_graph.get_node_index_by_name(name)
    }
}

impl<W: StreamData> StreamData for GenericLabelledDistributionGraph<W> {}
impl<W: StreamData> StreamData for Rc<GenericLabelledDistributionGraph<W>> {}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum TaggedVarOrNodeName {
    NodeName(NodeName),
    VarName(VarName),
}

pub trait Distance<N, M, W> {
    fn dist(self: &Self, n: N, m: M) -> Option<W>;
}

impl Distance<NodeIndex, NodeIndex, u64> for GenericLabelledDistributionGraph<u64> {
    fn dist(self: &Self, n: NodeIndex, m: NodeIndex) -> Option<u64> {
        let dist = petgraph::algo::astar(
            &self.dist_graph.graph,
            n,
            |o| o == m,
            |e| *e.weight(),
            |_| 0,
        );
        dist.map(|(cost, _)| cost)
    }
}

impl Distance<NodeName, NodeName, u64> for GenericLabelledDistributionGraph<u64> {
    fn dist(self: &Self, n: NodeName, m: NodeName) -> Option<u64> {
        let n_index = self.dist_graph.get_node_index_by_name(&n).unwrap();
        let m_index = self.dist_graph.get_node_index_by_name(&m).unwrap();
        self.dist(n_index, m_index)
    }
}

impl Distance<NodeIndex, VarName, u64> for GenericLabelledDistributionGraph<u64> {
    fn dist(self: &Self, n: NodeIndex, m: VarName) -> Option<u64> {
        let dist = petgraph::algo::astar(
            &self.dist_graph.graph,
            n,
            |o| {
                self.node_labels
                    .get(&o)
                    .map(|r| r.contains(&m))
                    .unwrap_or(false)
            },
            |e| *e.weight(),
            |_| 0,
        );
        dist.map(|(cost, _)| cost)
    }
}

impl Distance<NodeName, VarName, u64> for GenericLabelledDistributionGraph<u64> {
    fn dist(self: &Self, n: NodeName, m: VarName) -> Option<u64> {
        let n_index = self.dist_graph.get_node_index_by_name(&n).unwrap();
        self.dist(n_index, m)
    }
}

impl Distance<VarName, NodeIndex, u64> for GenericLabelledDistributionGraph<u64> {
    fn dist(self: &Self, n: VarName, m: NodeIndex) -> Option<u64> {
        let n_indicies = self
            .node_labels
            .iter()
            .filter(|(_, v)| v.contains(&n))
            .map(|(k, _)| *k);
        n_indicies
            .map(|n_index| self.dist(n_index, m.clone()))
            .fold(None, |acc, dist| {
                if let Some(d) = dist {
                    if let Some(a) = acc {
                        Some(std::cmp::min(a, d))
                    } else {
                        Some(d)
                    }
                } else {
                    acc
                }
            })
    }
}

impl Distance<VarName, NodeName, u64> for GenericLabelledDistributionGraph<u64> {
    fn dist(self: &Self, n: VarName, m: NodeName) -> Option<u64> {
        let m_index = self.dist_graph.get_node_index_by_name(&m).unwrap();
        self.dist(n, m_index)
    }
}

impl Distance<VarName, VarName, u64> for GenericLabelledDistributionGraph<u64> {
    fn dist(self: &Self, n: VarName, m: VarName) -> Option<u64> {
        let n_indicies = self
            .node_labels
            .iter()
            .filter(|(_, v)| v.contains(&n))
            .map(|(k, _)| *k);
        n_indicies
            .map(|n_index| self.dist(n_index, m.clone()))
            .fold(None, |acc, dist| {
                if let Some(d) = dist {
                    if let Some(a) = acc {
                        Some(std::cmp::min(a, d))
                    } else {
                        Some(d)
                    }
                } else {
                    acc
                }
            })
    }
}

impl Distance<TaggedVarOrNodeName, TaggedVarOrNodeName, u64>
    for GenericLabelledDistributionGraph<u64>
{
    fn dist(self: &Self, n: TaggedVarOrNodeName, m: TaggedVarOrNodeName) -> Option<u64> {
        match (n, m) {
            (TaggedVarOrNodeName::NodeName(n), TaggedVarOrNodeName::NodeName(m)) => self.dist(n, m),
            (TaggedVarOrNodeName::NodeName(n), TaggedVarOrNodeName::VarName(m)) => self.dist(n, m),
            (TaggedVarOrNodeName::VarName(n), TaggedVarOrNodeName::NodeName(m)) => self.dist(n, m),
            (TaggedVarOrNodeName::VarName(n), TaggedVarOrNodeName::VarName(m)) => self.dist(n, m),
        }
    }
}

#[derive(Error, Debug)]
pub enum GraphPlottingError {
    FileCreationError(std::io::Error, String),
    RenameError(std::io::Error, String),
    DotError(String, Option<i32>, String),
    WriteError(std::io::Error, String),
    IOError(std::io::Error, String),
}

impl Display for GraphPlottingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(match self {
            GraphPlottingError::FileCreationError(err, path) => {
                write!(f, "Error creating file {}: {}", path, err)?
            }
            GraphPlottingError::RenameError(err, path) => {
                write!(f, "Error renaming file {}: {}", path, err)?
            }
            GraphPlottingError::DotError(err, status, path) => write!(
                f,
                "Error generating dot file {}: {} (status: {})",
                path,
                err,
                match status {
                    Some(status) => status.to_string(),
                    None => "n/a".to_string(),
                }
            )?,
            GraphPlottingError::WriteError(err, path) => {
                write!(f, "Error writing to file {}: {}", path, err)?
            }
            GraphPlottingError::IOError(err, path) => write!(
                f,
                "I/O error occurred while processing file {}: {}",
                path, err
            )?,
        })
    }
}

pub fn plottable_graph(input: Rc<LabelledDistributionGraph>) -> DiGraph<String, u64> {
    let node_map = |node: NodeIndex, node_name: &NodeName| {
        let label_str: String =
            input
                .node_labels
                .get(&node)
                .unwrap()
                .iter()
                .fold(String::new(), |acc, label| {
                    if acc.is_empty() {
                        format!("{}", label)
                    } else {
                        format!("{}, {}", acc, label)
                    }
                });
        Some(format!("{} {{{}}}", node_name, label_str))
    };

    let edge_map = |_: EdgeIndex, weight: &u64| Some(*weight);

    input.dist_graph.graph.filter_map(node_map, edge_map)
}

pub async fn graph_to_dot(
    input: Rc<LabelledDistributionGraph>,
    file_path: &str,
) -> Result<(), GraphPlottingError> {
    let graph = plottable_graph(input);
    let dot = Dot::new(&graph);
    let mut file = File::create(file_path)
        .await
        .map_err(|e| GraphPlottingError::FileCreationError(e, file_path.to_string()))?;
    let dot_string = dot.to_string();
    let bytes_to_write = dot_string.as_bytes();
    file.write_all(bytes_to_write)
        .await
        .map_err(|e| GraphPlottingError::WriteError(e, file_path.to_string()))?;
    debug!(
        "Wrote dot file {} with countents {}",
        file_path,
        dot.to_string()
    );
    file.sync_all()
        .await
        .map_err(|e| GraphPlottingError::IOError(e, file_path.to_string()))?;
    Ok(())
}

pub async fn graph_to_png(
    input: Rc<LabelledDistributionGraph>,
    file_path: &str,
) -> Result<(), GraphPlottingError> {
    let dot_path = "/tmp/graph_visualization.dot";
    let png_path = "/tmp/graph_visualization.png";
    graph_to_dot(input, dot_path)
        .await
        .expect(format!("Failed to Write dot file {}", dot_path).as_str());
    let mut command = Command::new("dot");
    command.arg("-Tpng").arg(dot_path).arg("-o").arg(png_path);
    let status = (match command.status().await {
        Ok(status) => match status.code() {
            Some(code) if code == 0 => Ok(code),
            Some(code) => Err(GraphPlottingError::DotError(
                format!("dot command exited with code {}", code),
                Some(code),
                dot_path.to_string(),
            )),
            None => Err(GraphPlottingError::DotError(
                "dot command terminated by signal".to_string(),
                None,
                dot_path.to_string(),
            )),
        },
        Err(err) => {
            return Err(GraphPlottingError::DotError(
                format!("Failed to execute dot command with error: {}", err),
                None,
                dot_path.to_string(),
            ));
        }
    })?;
    info!("Command status: {:?}", status);
    fs::rename(png_path, file_path)
        .await
        .map_err(|e| GraphPlottingError::RenameError(e, dot_path.to_string()))?;
    Ok(())
}

enum VarAssignmentsIter {
    Done,
    Base(std::option::IntoIter<BTreeMap<VarName, NodeName>>),
    Rec {
        rest_iter: Box<VarAssignmentsIter>,
        locations: Vec<NodeName>,
        first: VarName,
        current_assignment: Option<BTreeMap<VarName, NodeName>>,
        locs_iter: std::vec::IntoIter<NodeName>,
    },
}

impl VarAssignmentsIter {
    pub fn new(locations: Vec<NodeName>, var_names: Vec<VarName>) -> Self {
        match var_names.as_slice() {
            [] => VarAssignmentsIter::Base(Some(BTreeMap::<VarName, NodeName>::new()).into_iter()),
            [first, rest @ ..] => {
                let rest_iter = Box::new(VarAssignmentsIter::new(locations.clone(), rest.to_vec()));
                VarAssignmentsIter::Rec {
                    rest_iter,
                    locations: locations.clone(),
                    first: first.clone(),
                    current_assignment: None,
                    locs_iter: Vec::new().into_iter(),
                }
            }
        }
    }
}

impl Iterator for VarAssignmentsIter {
    type Item = BTreeMap<VarName, NodeName>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            VarAssignmentsIter::Done => None,
            VarAssignmentsIter::Base(iter) => iter.next(),
            VarAssignmentsIter::Rec {
                rest_iter,
                locations,
                first,
                current_assignment,
                locs_iter,
            } => loop {
                if let Some(assignment) = current_assignment.take() {
                    if let Some(loc) = locs_iter.next() {
                        let mut new_assignment = assignment.clone();
                        new_assignment.insert(first.clone(), loc);
                        *current_assignment = Some(assignment);
                        return Some(new_assignment);
                    }
                }
                match rest_iter.next() {
                    Some(assignment) => {
                        *current_assignment = Some(assignment);
                        *locs_iter = locations.clone().into_iter();
                    }
                    None => {
                        *self = VarAssignmentsIter::Done;
                        return None;
                    }
                }
            },
        }
    }
}

fn possible_var_assignments(
    locations: Vec<NodeName>,
    var_names: Vec<VarName>,
) -> VarAssignmentsIter {
    VarAssignmentsIter::new(locations, var_names)
}

pub struct PossibleLabelledDistGraphs {
    base_graph: Rc<DistributionGraph>,
    central_var_names: Vec<VarName>,
    var_names: Vec<VarName>,
    locations: Vec<NodeName>,
    assignments_iter: VarAssignmentsIter,
}

impl PossibleLabelledDistGraphs {
    pub fn new(
        base_graph: Rc<DistributionGraph>,
        central_var_names: Vec<VarName>,
        var_names: Vec<VarName>,
    ) -> Self {
        let locations = base_graph
            .graph
            .node_indices()
            .map(|idx| base_graph.graph[idx].clone())
            .collect::<Vec<_>>();
        let assignments_iter = possible_var_assignments(locations.clone(), var_names.clone());
        info!(
            ?var_names,
            ?central_var_names,
            "Iterating over potential graphs"
        );
        Self {
            base_graph,
            central_var_names,
            var_names,
            locations,
            assignments_iter,
        }
    }
}

impl Iterator for PossibleLabelledDistGraphs {
    type Item = LabelledDistributionGraph;

    fn next(&mut self) -> Option<Self::Item> {
        self.assignments_iter.next().map(|assignment| {
            let node_labels = self
                .locations
                .iter()
                .map(|loc| {
                    (
                        self.base_graph.get_node_index_by_name(loc).unwrap(),
                        self.var_names
                            .iter()
                            .filter(|var| assignment[var] == *loc)
                            .cloned()
                            .collect(),
                    )
                })
                .chain(once((
                    self.base_graph.central_monitor,
                    self.central_var_names.clone(),
                )))
                .collect();
            GenericLabelledDistributionGraph {
                dist_graph: self.base_graph.clone(),
                var_names: self.var_names.clone(),
                node_labels,
            }
        })
    }
}

/// Returns an iterator over all possible labellings of a distribution graph,
/// assigning each variable in `var_names` to a node in the graph.
pub fn possible_labelled_dist_graphs(
    base_graph: Rc<DistributionGraph>,
    central_var_names: Vec<VarName>,
    var_names: Vec<VarName>,
) -> PossibleLabelledDistGraphs {
    PossibleLabelledDistGraphs::new(base_graph, central_var_names, var_names)
}

#[cfg(test)]
pub mod generation {
    use super::*;
    use proptest::prelude::*;

    impl Arbitrary for NodeName {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_args: ()) -> Self::Strategy {
            "[a-z]{1,10}".prop_map(NodeName).boxed()
        }
    }

    pub fn arb_conc_distribution_graph() -> impl Strategy<Value = DistributionGraph> {
        (
            any::<NodeName>(),
            prop::collection::hash_set("[a-z]", 1..=10),
        )
            .prop_map(|(central_monitor, nodes)| {
                let mut graph: Graph<NodeName, u64> = DiGraph::new();
                let central_monitor = graph.add_node(central_monitor);
                for node in nodes {
                    graph.add_node(node.into());
                }
                let mut edges = vec![];
                for node in graph.node_indices() {
                    if node != central_monitor {
                        edges.push((central_monitor, node, 0));
                    }
                }
                let graph_clone = graph.clone();
                edges.extend(
                    graph
                        .node_indices()
                        .filter(|&node| node != central_monitor)
                        .flat_map(move |node| {
                            graph_clone
                                .node_indices()
                                .filter(move |&other| other != node && other != central_monitor)
                                .map(move |other| (node, other, 1))
                        }),
                );
                for (source, target, label) in edges {
                    graph.add_edge(source, target, label);
                }
                DistributionGraph {
                    central_monitor,
                    graph,
                }
            })
    }

    pub fn arb_labelled_conc_distribution_graph() -> impl Strategy<Value = LabelledDistributionGraph>
    {
        (
            arb_conc_distribution_graph(),
            prop::collection::hash_set("[a-z]", 1..=10),
        )
            .prop_map(|(dist_graph, var_names)| {
                let mut node_labels = BTreeMap::new();
                for node in dist_graph.graph.node_indices() {
                    node_labels.insert(
                        node,
                        var_names.clone().into_iter().map(|x| x.into()).collect(),
                    );
                }
                GenericLabelledDistributionGraph {
                    dist_graph: Rc::new(dist_graph),
                    var_names: var_names.into_iter().map(|x| x.into()).collect(),
                    node_labels,
                }
            })
    }
}

pub type Pos = (f64, f64, f64);

fn position_distance(x: &Pos, y: &Pos) -> f64 {
    ((x.0 - y.0).powi(2) + (x.1 - y.1).powi(2) + (x.2 - y.2).powi(2)).sqrt()
}

// #[requires(locations.contains(&central_monitor))]
pub fn dist_graph_from_positions(
    central_monitor: NodeName,
    locations: Vec<NodeName>,
    positions: Vec<Pos>,
) -> DistributionGraph {
    let mut graph = DiGraph::new();
    let mut node_indices = BTreeMap::new();
    for location in locations.iter() {
        let node_index = graph.add_node(location.clone());
        node_indices.insert(location.clone(), node_index);
    }
    for i in 0..locations.len() {
        for j in i + 1..locations.len() {
            let dist = position_distance(&positions[i], &positions[j]) as u64;
            graph.add_edge(
                node_indices[&locations[i]],
                node_indices[&locations[j]],
                dist,
            );
            graph.add_edge(
                node_indices[&locations[j]],
                node_indices[&locations[i]],
                dist,
            );
        }
    }
    // Add edges from central monitor to all nodes if the central monitor is not already
    // a node with a defined position
    let central_monitor = match node_indices.get(&central_monitor) {
        Some(central_monitor) => *central_monitor,
        None => {
            let central_monitor = graph.add_node(central_monitor.clone());
            for node_index in node_indices.values() {
                graph.add_edge(*node_index, central_monitor, 0);
                graph.add_edge(central_monitor, *node_index, 0);
            }
            central_monitor
        }
    };
    DistributionGraph {
        central_monitor,
        graph,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::{prop_assert_eq, proptest};
    use tracing::info;

    #[test]
    fn test_serialize_deserialize() {
        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        let c = graph.add_node("C".into());
        graph.add_edge(a, b, 0);
        graph.add_edge(b, c, 0);
        let dist_graph = Rc::new(DistributionGraph {
            central_monitor: a,
            graph,
        });
        let labelled_graph = LabelledDistributionGraph {
            dist_graph,
            var_names: vec!["a".into(), "b".into(), "c".into()],
            node_labels: BTreeMap::new(),
        };
        let serialized = serde_json::to_string(&labelled_graph).unwrap();
        let deserialized: LabelledDistributionGraph = serde_json::from_str(&serialized).unwrap();
        assert_eq!(labelled_graph, deserialized);
    }

    #[test]
    fn test_deserialize() {
        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        let c = graph.add_node("C".into());
        graph.add_edge(a, b, 0);
        graph.add_edge(b, c, 1);
        let dist_graph = Rc::new(DistributionGraph {
            central_monitor: a,
            graph,
        });
        let labelled_dist_graph = LabelledDistributionGraph {
            dist_graph,
            var_names: vec!["a".into(), "b".into(), "c".into()],
            node_labels: [(2.into(), vec!["a".into(), "b".into()])]
                .into_iter()
                .collect(),
        };
        let dist_graph_serialized = r#"{
            "dist_graph": {
                "central_monitor": 0,
                "graph": {
                    "nodes": [
                        "A",
                        "B",
                        "C"
                    ],
                    "edge_property": "directed",
                    "edges": [
                        [0, 1, 0],
                        [1, 2, 1]
                    ]
                }
            },
            "var_names": ["a", "b", "c"],
            "node_labels": {"2": ["a", "b"]}
        }"#;
        assert_eq!(
            serde_json::from_str::<LabelledDistributionGraph>(dist_graph_serialized).unwrap(),
            labelled_dist_graph
        );
    }

    #[test]
    fn test_graph_from_locations() {
        let central_monitor = NodeName::new("A");
        let locations = vec![NodeName::new("A"), NodeName::new("B")];
        let positions = vec![(0.0, 0.0, 0.0), (2.0, 2.0, 0.0)];
        let mut graph = DiGraph::new();
        graph.add_node(NodeName::new("A"));
        graph.add_node(NodeName::new("B"));
        graph.add_edge(NodeIndex::new(0), NodeIndex::new(1), 2);
        graph.add_edge(NodeIndex::new(1), NodeIndex::new(0), 2);
        let dist_graph_expected = Rc::new(DistributionGraph {
            central_monitor: NodeIndex::new(0),
            graph,
        });
        let dist_graph =
            dist_graph_from_positions(central_monitor.clone(), locations.clone(), positions);
        assert_eq!(
            dist_graph.central_monitor,
            dist_graph_expected.central_monitor
        );
        info!("Got graph: {:?}", dist_graph.graph);
        assert!(graph_eq(&dist_graph.graph, &dist_graph_expected.graph));
    }

    #[test]
    fn test_monitors_at_node() {
        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        let c = graph.add_node("B".into());
        graph.add_edge(a, b, 0);
        graph.add_edge(b, c, 1);
        let dist_graph = Rc::new(DistributionGraph {
            central_monitor: a,
            graph,
        });
        let labelled_dist_graph = LabelledDistributionGraph {
            dist_graph,
            var_names: vec!["a".into(), "b".into(), "c".into()],
            node_labels: [(2.into(), vec!["a".into(), "b".into()])]
                .into_iter()
                .collect(),
        };
        assert_eq!(
            labelled_dist_graph.monitors_at_node(2.into()),
            Some(&vec!["a".into(), "b".into()])
        );
        assert_eq!(labelled_dist_graph.monitors_at_node(1.into()), None);
    }

    #[test]
    fn test_possible_labellings() {
        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        let c = graph.add_node("B".into());
        graph.add_edge(a, b, 0);
        graph.add_edge(b, c, 1);
        let dist_graph = Rc::new(DistributionGraph {
            central_monitor: a,
            graph,
        });
        let var_names = vec!["x".into(), "y".into()];
        let possible_labelled_dist_graphs: Vec<_> =
            possible_labelled_dist_graphs(dist_graph.clone(), vec![], var_names).collect();
        assert_eq!(possible_labelled_dist_graphs.len(), 9);
    }

    proptest! {
        #[test]
        fn test_prop_get_node_index_by_name_prop(node_index in 0usize..10usize, dist_graph in generation::arb_conc_distribution_graph()) {
            if dist_graph.graph.node_indices().any(|node| node.index() == node_index) {
                let node_name_ref = &dist_graph.graph[NodeIndex::new(node_index)];
                let indexed_node_index = dist_graph.get_node_index_by_name(node_name_ref).unwrap();
                prop_assert_eq!(dist_graph.graph[indexed_node_index].clone(), node_name_ref.clone());
            }
        }

        #[test]
        fn test_prop_get_node_index_by_name_labelled_prop(node_index in 0usize..10usize, dist_graph in generation::arb_conc_distribution_graph()) {
            if dist_graph.graph.node_indices().any(|node| node.index() == node_index) {
                let node_name_ref = &dist_graph.graph[NodeIndex::new(node_index)];
                let indexed_node_index = dist_graph.get_node_index_by_name(node_name_ref).unwrap();
                prop_assert_eq!(dist_graph.graph[indexed_node_index].clone(), node_name_ref.clone());
            }
        }

        #[test]
        fn test_prop_monitors_at_node(node_index in 0usize..10usize, labelled_dist_graph in generation::arb_labelled_conc_distribution_graph()) {
            if labelled_dist_graph.dist_graph.graph.node_indices().any(|node| node.index() == node_index) {
                let node_name_ref = &labelled_dist_graph.dist_graph.graph[NodeIndex::new(node_index)];
                let indexed_node_index = labelled_dist_graph.get_node_index_by_name(node_name_ref).unwrap();
                prop_assert_eq!(labelled_dist_graph.monitors_at_node(indexed_node_index), Some(&labelled_dist_graph.node_labels[&indexed_node_index]));
            }
        }

        #[test]
        fn test_prop_dist(node_index in 0usize..10usize, labelled_dist_graph in generation::arb_labelled_conc_distribution_graph()) {
            if let Some(node) =  labelled_dist_graph.dist_graph.graph.node_indices().find(|node| node.index() == node_index) {
                prop_assert_eq!(labelled_dist_graph.dist(node, node), Some(0));
            }
        }

        #[test]
        fn test_prop_dist_name(node_index in 0usize..10usize, labelled_dist_graph in generation::arb_labelled_conc_distribution_graph()) {
            if let Some(node) =  labelled_dist_graph.dist_graph.graph.node_indices().find(|node| node.index() == node_index) {
                let node_name = labelled_dist_graph.dist_graph.graph[node].clone();
                prop_assert_eq!(labelled_dist_graph.dist(node_name.clone(), node_name), Some(0));
            }
        }

        #[test]
        fn test_prop_dist_label(node_index in 0usize..10usize, labelled_dist_graph in generation::arb_labelled_conc_distribution_graph()) {
            if let Some(node) = labelled_dist_graph.dist_graph.graph.node_indices().find(|node| node.index() == node_index) {
                if let Some(Some(node_label)) = labelled_dist_graph.monitors_at_node(node).map(|x| x.get(0).cloned()) {
                    prop_assert_eq!(labelled_dist_graph.dist(node_label.clone(), node_label), Some(0));
                }
            }
        }
    }
}
