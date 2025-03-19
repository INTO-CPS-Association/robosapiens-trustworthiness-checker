/* - Should we be using hierarchical graphs?
 * - What about the subnodes of a node (i.e. the different ROS nodes inside
 *   of an individual robot)?
 */

use std::{collections::BTreeMap, fmt::Display};

use petgraph::prelude::*;
use serde::{Deserialize, Serialize};

use crate::VarName;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct NodeName(String);

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

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub enum EdgeLabel {
    Internal,
    Physical { dist: f64, latency: f64 },
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
struct NodeLabel {
    monitors: Vec<VarName>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ConcDistributionGraph {
    pub central_monitor: NodeIndex,
    pub graph: DiGraph<NodeName, EdgeLabel>,
}

impl ConcDistributionGraph {
    pub fn get_node_index_by_name(&self, name: &NodeName) -> Option<NodeIndex> {
        self.graph
            .node_indices()
            .find(|&node| self.graph[node] == *name)
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

impl PartialEq for ConcDistributionGraph {
    fn eq(&self, other: &Self) -> bool {
        self.central_monitor == other.central_monitor && graph_eq(&self.graph, &other.graph)
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct LabelledConcDistributionGraph {
    pub dist_graph: ConcDistributionGraph,
    pub var_names: Vec<VarName>,
    pub node_labels: BTreeMap<NodeIndex, Vec<VarName>>,
}

impl LabelledConcDistributionGraph {
    pub fn monitors_at_node(&self, node: NodeIndex) -> Option<&Vec<VarName>> {
        self.node_labels.get(&node)
    }

    pub fn get_node_index_by_name(&self, name: &NodeName) -> Option<NodeIndex> {
        self.dist_graph.get_node_index_by_name(name)
    }
}

#[cfg(test)]
pub mod generation {
    use super::*;
    use proptest::prelude::*;

    impl Arbitrary for NodeName {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_args: ()) -> Self::Strategy {
            "[a-z]{1,10}".prop_map(|s| NodeName(s)).boxed()
        }
    }

    pub fn arb_conc_distribution_graph() -> impl Strategy<Value = ConcDistributionGraph> {
        (
            any::<NodeName>(),
            prop::collection::hash_set("[a-z]", 1..=10),
        )
            .prop_map(|(central_monitor, nodes)| {
                let mut graph = DiGraph::new();
                let central_monitor = graph.add_node(central_monitor);
                for node in nodes {
                    graph.add_node(node.into());
                }
                let mut edges = vec![];
                for node in graph.node_indices() {
                    if node != central_monitor {
                        edges.push((central_monitor, node, EdgeLabel::Internal));
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
                                .map(move |other| {
                                    (
                                        node,
                                        other,
                                        EdgeLabel::Physical {
                                            dist: 1.0,
                                            latency: 2.0,
                                        },
                                    )
                                })
                        }),
                );
                for (source, target, label) in edges {
                    graph.add_edge(source, target, label);
                }
                ConcDistributionGraph {
                    central_monitor,
                    graph,
                }
            })
    }

    pub fn arb_labelled_conc_distribution_graph()
    -> impl Strategy<Value = LabelledConcDistributionGraph> {
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
                LabelledConcDistributionGraph {
                    dist_graph,
                    var_names: var_names.into_iter().map(|x| x.into()).collect(),
                    node_labels,
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::{prop_assert_eq, proptest};

    #[test]
    fn test_serialize_deserialize() {
        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        let c = graph.add_node("C".into());
        graph.add_edge(a, b, EdgeLabel::Internal);
        graph.add_edge(
            b,
            c,
            EdgeLabel::Physical {
                dist: 1.0,
                latency: 2.0,
            },
        );
        let dist_graph = ConcDistributionGraph {
            central_monitor: a,
            graph,
        };
        let labelled_graph = LabelledConcDistributionGraph {
            dist_graph,
            var_names: vec!["a".into(), "b".into(), "c".into()],
            node_labels: BTreeMap::new(),
        };
        let serialized = serde_json::to_string(&labelled_graph).unwrap();
        let deserialized: LabelledConcDistributionGraph =
            serde_json::from_str(&serialized).unwrap();
        assert_eq!(labelled_graph, deserialized);
    }

    #[test]
    fn test_deserialize() {
        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        let c = graph.add_node("C".into());
        graph.add_edge(a, b, EdgeLabel::Internal);
        graph.add_edge(
            b,
            c,
            EdgeLabel::Physical {
                dist: 1.0,
                latency: 2.0,
            },
        );
        let dist_graph = ConcDistributionGraph {
            central_monitor: a,
            graph,
        };
        let labelled_dist_graph = LabelledConcDistributionGraph {
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
                        [0, 1, {"Internal": null}],
                        [1, 2, {"Physical": {"dist": 1.0, "latency": 2.0}}]
                    ]
                }
            },
            "var_names": ["a", "b", "c"],
            "node_labels": {"2": ["a", "b"]}
        }"#;
        assert_eq!(
            serde_json::from_str::<LabelledConcDistributionGraph>(dist_graph_serialized).unwrap(),
            labelled_dist_graph
        );
    }

    #[test]
    fn test_monitors_at_node() {
        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        let c = graph.add_node("B".into());
        graph.add_edge(a, b, EdgeLabel::Internal);
        graph.add_edge(
            b,
            c,
            EdgeLabel::Physical {
                dist: 1.0,
                latency: 2.0,
            },
        );
        let dist_graph = ConcDistributionGraph {
            central_monitor: a,
            graph,
        };
        let labelled_dist_graph = LabelledConcDistributionGraph {
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

    proptest! {
        #[test]
        fn test_prop_get_node_index_by_name_prop(node_index in 0usize..10usize, dist_graph in generation::arb_conc_distribution_graph()) {
            if let Some(_) = dist_graph.graph.node_indices().find(|&node| node.index() == node_index) {
                let node_name_ref = &dist_graph.graph[NodeIndex::new(node_index)];
                let indexed_node_index = dist_graph.get_node_index_by_name(node_name_ref).unwrap();
                prop_assert_eq!(dist_graph.graph[indexed_node_index].clone(), node_name_ref.clone());
            }
        }

        #[test]
        fn test_prop_get_node_index_by_name_labelled_prop(node_index in 0usize..10usize, dist_graph in generation::arb_conc_distribution_graph()) {
            if let Some(_) = dist_graph.graph.node_indices().find(|&node| node.index() == node_index) {
                let node_name_ref = &dist_graph.graph[NodeIndex::new(node_index)];
                let indexed_node_index = dist_graph.get_node_index_by_name(node_name_ref).unwrap();
                prop_assert_eq!(dist_graph.graph[indexed_node_index].clone(), node_name_ref.clone());
            }
        }

        #[test]
        fn test_prop_monitors_at_node(node_index in 0usize..10usize, labelled_dist_graph in generation::arb_labelled_conc_distribution_graph()) {
            if let Some(_) = labelled_dist_graph.dist_graph.graph.node_indices().find(|&node| node.index() == node_index) {
                let node_name_ref = &labelled_dist_graph.dist_graph.graph[NodeIndex::new(node_index)];
                let indexed_node_index = labelled_dist_graph.get_node_index_by_name(node_name_ref).unwrap();
                prop_assert_eq!(labelled_dist_graph.monitors_at_node(indexed_node_index), Some(&labelled_dist_graph.node_labels[&indexed_node_index]));
            }
        }
    }
}
